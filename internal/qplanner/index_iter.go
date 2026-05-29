package qplanner

import (
	"bytes"
	"fmt"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
)

// IndexIter iterates over an index namespace using bounds.
// key = indexFields + docId for both unique and non-unique indexes.
type IndexIter struct {
	Source   *CursorSource
	IdxInfo  *IndexInfo
	cursor   *btree.Cursor
	Bounds   query.Bounds
	boundIdx int
	Reverse  bool
	started  bool

	// PointLookup mirrors CBOIndex.PointLookup: true iff every original bound
	// was an equality (Start == End) before AdjustBoundsForNonUnique widened
	// the End. CountEntries uses it to gate the single-field canonical-key
	// probe — the probe is a valid multi-key detector only when the indexed
	// value sits at byte position 0 of the key (single-field PointLookup).
	PointLookup bool

	// pendingCurrent is set by skipOffset after it positions the cursor
	// directly on the first row to emit. When true, the next Next() call
	// returns the entry at the current cursor position WITHOUT advancing
	// first (it then clears the flag). This makes the handoff from a
	// cursor-level offset skip to normal iteration off-by-one-safe across
	// page boundaries: skipOffset leaves the cursor on the target entry
	// rather than one-before it.
	pendingCurrent bool
}

func (it *IndexIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	perf := perfCountersEnabled()
	var start time.Time
	if perf {
		start = time.Now()
		qpPerf.indexNextCalls.Add(1)
	}
	defer func() {
		if perf {
			qpPerf.indexNextNs.Add(uint64(time.Since(start).Nanoseconds()))
			if docId != nil {
				qpPerf.indexYields.Add(1)
			}
		}
	}()

	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}

	// No bounds: full index scan
	if len(it.Bounds) == 0 {
		return it.nextNoBounds()
	}

	for {
		if it.boundIdx >= len(it.Bounds) {
			return nil, nil, false, nil
		}

		b := it.Bounds[it.boundIdx]

		if !it.started {
			it.started = true
			if it.Reverse {
				if len(b.End) > 0 {
					if err = it.cursor.Seek(b.End); err != nil {
						return nil, nil, false, err
					}
					if !it.cursor.Valid() {
						if err = it.cursor.Last(); err != nil {
							return nil, nil, false, err
						}
					} else {
						// Check if we need to back up (Seek finds >=)
						k, kerr := it.cursor.Key()
						if kerr != nil {
							return nil, nil, false, kerr
						}
						cmp := bytes.Compare(k, b.End)
						if cmp > 0 || (cmp == 0 && !b.EndInclude) {
							if err = it.cursor.Previous(); err != nil {
								return nil, nil, false, err
							}
						}
					}
				} else {
					if err = it.cursor.Last(); err != nil {
						return nil, nil, false, err
					}
				}
			} else {
				if len(b.Start) > 0 {
					if err = it.cursor.Seek(b.Start); err != nil {
						return nil, nil, false, err
					}
					if it.cursor.Valid() && !b.StartInclude {
						k, kerr := it.cursor.Key()
						if kerr != nil {
							return nil, nil, false, kerr
						}
						if bytes.Equal(k, b.Start) {
							if err = it.cursor.Next(); err != nil {
								return nil, nil, false, err
							}
						}
					}
				} else {
					if err = it.cursor.First(); err != nil {
						return nil, nil, false, err
					}
				}
			}
		} else {
			if it.Reverse {
				if err = it.cursor.Previous(); err != nil {
					return nil, nil, false, err
				}
			} else {
				if err = it.cursor.Next(); err != nil {
					return nil, nil, false, err
				}
			}
		}

		if !it.cursor.Valid() {
			it.boundIdx++
			it.started = false
			continue
		}

		k, err := it.cursor.Key()
		if err != nil {
			return nil, nil, false, err
		}

		// Check bounds
		if !it.Reverse && len(b.End) > 0 {
			cmp := bytes.Compare(k, b.End)
			if cmp > 0 || (cmp == 0 && !b.EndInclude) {
				it.boundIdx++
				it.started = false
				continue
			}
		}
		if it.Reverse && len(b.Start) > 0 {
			cmp := bytes.Compare(k, b.Start)
			if cmp < 0 || (cmp == 0 && !b.StartInclude) {
				it.boundIdx++
				it.started = false
				continue
			}
		}

		return it.extractResult(k)
	}
}

func (it *IndexIter) nextNoBounds() (key []byte, docId []byte, multiKey bool, err error) {
	switch {
	case it.pendingCurrent:
		// skipOffset positioned the cursor directly on the entry to emit;
		// return it without advancing. Clear the flag so the following
		// call advances normally.
		it.pendingCurrent = false
	case !it.started:
		it.started = true
		if it.Reverse {
			if err = it.cursor.Last(); err != nil {
				return nil, nil, false, err
			}
		} else {
			if err = it.cursor.First(); err != nil {
				return nil, nil, false, err
			}
		}
	default:
		if it.Reverse {
			if err = it.cursor.Previous(); err != nil {
				return nil, nil, false, err
			}
		} else {
			if err = it.cursor.Next(); err != nil {
				return nil, nil, false, err
			}
		}
	}
	if !it.cursor.Valid() {
		return nil, nil, false, nil
	}
	k, err := it.cursor.Key()
	if err != nil {
		return nil, nil, false, err
	}
	return it.extractResult(k)
}

// skipOffset advances the index cursor past up to n logical result rows
// WITHOUT fetching/parsing the skipped documents, implementing the
// offsetSkipper contract. See offsetSkipper for the full contract.
//
// Scope (correctness): only the unbounded full-index scan is fast-skipped,
// and only across entries the index records as scalar (multiKey==false —
// the doc's single entry in this index). On the first multi-key/legacy
// entry it stops and returns the unskipped remainder, leaving the cursor
// on that entry so the normal Next() path (FetchIter → CanonicalKeyDedup →
// LimitIter, or consumer-side DocDedup) resolves the offset correctly.
//
// For bounded scans it skips nothing (returns n): bounded index scans
// either carry a residual filter (which blocks the delegation chain
// upstream) or would require per-bound logical-row accounting that the
// safe fetch-then-skip path already handles correctly.
func (it *IndexIter) skipOffset(n int) (remaining int, err error) {
	if n <= 0 {
		return 0, nil
	}
	// Only the unbounded full-index scan is fast-skipped. With bounds we
	// fall back to the safe fetch-then-skip path (return the full offset).
	if len(it.Bounds) != 0 {
		return n, nil
	}
	// skipOffset is only ever invoked before iteration starts (LimitIter
	// calls it on its first Next, before pulling any row). Guard against a
	// mid-stream call to avoid corrupting cursor position.
	if it.started {
		return n, nil
	}
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}
	it.started = true

	// Position at the first entry in scan order.
	if it.Reverse {
		if err = it.cursor.Last(); err != nil {
			return n, err
		}
	} else {
		if err = it.cursor.First(); err != nil {
			return n, err
		}
	}

	skipped := 0
	for skipped < n && it.cursor.Valid() {
		val, verr := it.cursor.Value()
		if verr != nil {
			return n - skipped, verr
		}
		if EntryValueIsMultiKey(val) {
			// A doc with >1 entries in this index (array/legacy). Entry
			// count no longer equals logical-row count: stop here and let
			// the dedup-aware path skip the remaining rows. Leave the
			// cursor on this entry; the next Next() emits it.
			it.pendingCurrent = true
			return n - skipped, nil
		}
		// Scalar entry == exactly one distinct doc == one logical row.
		skipped++
		if it.Reverse {
			if err = it.cursor.Previous(); err != nil {
				return n - skipped, err
			}
		} else {
			if err = it.cursor.Next(); err != nil {
				return n - skipped, err
			}
		}
	}

	// The next Next() should emit the entry at the current position (the
	// first un-skipped row), not advance past it. If the cursor ran out of
	// entries (skipped < n), the flag is harmless: Next() sees an invalid
	// cursor and returns end-of-stream.
	it.pendingCurrent = true
	return n - skipped, nil
}

// arrayPrefix is the anyenc type tag for array-typed values. writeValues
// emits a key with this leading byte exactly when a doc has an array-typed
// value for an indexed field (the per-element keys of a nested array, and the
// whole-value-as-tuple key of any multi-key doc). So a key with this prefix
// exists in a single-field index iff that index has ever held a multi-key
// (array) entry. See docs/specs/2026-05-28-i04-i05-fix-option-d-canonical-key-probe.md.
var arrayPrefix = []byte{byte(anyenc.TypeArray)}

// indexProbeAnyMultiKey reports whether the index namespace has any entry whose
// key begins with the array type tag — i.e. whether any indexed doc had an
// array-typed value. It is a single btree Seek, snapshot-consistent with the
// caller's read tx.
//
// PRECONDITION: only valid for single-field indexes. For a compound index the
// array field's value is not at byte position 0 of the key, so its 0x06 byte
// sits mid-key and Seek(arrayPrefix) would miss it (false negative). Callers
// must route compound/non-PointLookup shapes elsewhere before probing.
func indexProbeAnyMultiKey(cs *CursorSource) (bool, error) {
	c := cs.NewCursor()
	defer c.Close()
	if err := c.Seek(arrayPrefix); err != nil {
		return false, err
	}
	if !c.Valid() {
		return false, nil
	}
	k, err := c.Key()
	if err != nil {
		return false, err
	}
	return len(k) > 0 && k[0] == byte(anyenc.TypeArray), nil
}

// CountEntries counts distinct documents matching this index iterator's
// bounds. Two strategies based on bound count:
//
//	len(Bounds) <= 1: use cursor.CountUntil — page-batch count without
//	                  visiting individual cells. Within-doc dedup in
//	                  insertKeys guarantees ≤1 entry per distinct value
//	                  per doc, so the entry count equals the doc count.
//	                  Fast: no per-entry walk, no value reads.
//
//	len(Bounds) >  1: walk each entry, read the value byte, stream-count
//	                  scalar entries, and dedup multi-key (or legacy)
//	                  entries through a lazy seen-set. A doc with array
//	                  [v1,v2] would otherwise be counted twice when
//	                  $in:[v1,v2] crosses both bounds — the seen-set
//	                  reduces it to one. The seen-set is allocated on
//	                  the first multi-key entry and skipped entirely for
//	                  fully-scalar streams.
func (it *IndexIter) CountEntries() (int, error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}

	if len(it.Bounds) <= 1 {
		return it.countEntriesBatch()
	}
	return it.countEntriesWithDedup()
}

// countEntriesBatch is the original page-batch fast path, used for
// single-bound (or unbounded — len(Bounds)==0 returns 0) counts.
func (it *IndexIter) countEntriesBatch() (int, error) {
	total := 0
	for _, b := range it.Bounds {
		if len(b.Start) > 0 {
			if err := it.cursor.Seek(b.Start); err != nil {
				return 0, err
			}
			if it.cursor.Valid() && !b.StartInclude {
				k, kerr := it.cursor.Key()
				if kerr != nil {
					return 0, kerr
				}
				if bytes.Equal(k, b.Start) {
					if err := it.cursor.Next(); err != nil {
						return 0, err
					}
				}
			}
		} else {
			if err := it.cursor.First(); err != nil {
				return 0, err
			}
		}

		if !it.cursor.Valid() {
			continue
		}

		n, err := it.cursor.CountUntil(b.End, b.EndInclude)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

// countEntriesWithDedup walks each bound. If the bound's first entry is
// scalar AND no multi-key entry has been seen across previous bounds,
// the bound is counted via the page-batch fast path (cursor.CountUntil)
// — same as the single-bound case. As soon as a multi-key (or legacy)
// entry appears anywhere, we fall back to the per-entry walk for the
// remainder of the iteration so docId-level dedup is applied.
//
// Rationale: in practice most indexes are entirely scalar OR entirely
// multi-key for a given query (the field is either array-typed or not).
// Mixed indexes are rare. The peek-then-batch shortcut means the common
// scalar-only case pays only one extra value-read per bound (vs the
// pure-batch path), recovering most of the alpha.2 SimpleIndex/In speed
// while preserving correctness for multi-key.
func (it *IndexIter) countEntriesWithDedup() (int, error) {
	var seen map[string]struct{}
	total := 0
	stickyMulti := false // once any multi-key seen, never re-engage batch fast path

	for _, b := range it.Bounds {
		if err := it.seekBoundStart(b); err != nil {
			return 0, err
		}
		if !it.cursor.Valid() {
			continue
		}

		// Peek the first entry's value. If it's scalar AND we haven't yet
		// seen a multi-key entry in this iteration, use batch counting:
		// the within-doc dedup invariant still holds (each doc has ≤1
		// entry per distinct value across the whole index, which means
		// ≤1 entry in any value-range covered by a single bound).
		if !stickyMulti {
			val, verr := it.cursor.Value()
			if verr != nil {
				return 0, verr
			}
			if !EntryValueIsMultiKey(val) {
				n, err := it.cursor.CountUntil(b.End, b.EndInclude)
				if err != nil {
					return 0, err
				}
				total += n
				continue
			}
			stickyMulti = true
		}

		// Per-entry walk with seen-set dedup.
		for it.cursor.Valid() {
			k, kerr := it.cursor.Key()
			if kerr != nil {
				return 0, kerr
			}
			if len(b.End) > 0 {
				cmp := bytes.Compare(k, b.End)
				if cmp > 0 || (cmp == 0 && !b.EndInclude) {
					break
				}
			}
			val, verr := it.cursor.Value()
			if verr != nil {
				return 0, verr
			}
			if EntryValueIsMultiKey(val) {
				if seen == nil {
					seen = make(map[string]struct{}, 64)
				}
				docId := extractDocId(k, len(it.IdxInfo.FieldNames))
				if _, dup := seen[string(docId)]; dup {
					if err := it.cursor.Next(); err != nil {
						return 0, err
					}
					continue
				}
				seen[string(docId)] = struct{}{}
			}
			total++
			if err := it.cursor.Next(); err != nil {
				return 0, err
			}
		}
	}
	return total, nil
}

// seekBoundStart positions the cursor at the first entry of the given
// bound, accounting for StartInclude. Shared between
// countEntriesWithDedup and the existing countEntriesBatch.
func (it *IndexIter) seekBoundStart(b query.Bound) error {
	if len(b.Start) > 0 {
		if err := it.cursor.Seek(b.Start); err != nil {
			return err
		}
		if it.cursor.Valid() && !b.StartInclude {
			k, kerr := it.cursor.Key()
			if kerr != nil {
				return kerr
			}
			if bytes.Equal(k, b.Start) {
				if err := it.cursor.Next(); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return it.cursor.First()
}

// Close releases the underlying cursor resources.
func (it *IndexIter) Close() {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
}

func (it *IndexIter) extractResult(k []byte) (key []byte, docId []byte, multiKey bool, err error) {
	// Both unique and non-unique: key = indexFields + docId.
	//
	// multiKey is read from the entry's value byte (set by insertKeys).
	// Legacy entries with empty values are treated as multi-key for
	// safety; see IndexEntryFlagMultiKey + EntryValueIsMultiKey.
	val, verr := it.cursor.Value()
	if verr != nil {
		return nil, nil, false, verr
	}
	docID := extractDocId(k, len(it.IdxInfo.FieldNames))
	return k, docID, EntryValueIsMultiKey(val), nil
}

func (it *IndexIter) String() string {
	s := fmt.Sprintf("IndexScan(%s)", it.IdxInfo.Name)
	if it.Reverse {
		s += "(reverse)"
	}
	if len(it.Bounds) > 0 {
		s += fmt.Sprintf("[bounds=%s]", it.Bounds.String())
	}
	return s
}

// extractDocId extracts the docId portion from a non-unique index key.
// The key is a tuple of (field1, field2, ..., docId).
func extractDocId(key anyenc.Tuple, numFields int) []byte {
	offset, err := key.OffsetAfter(numFields)
	if err != nil {
		// Corrupt tuple shouldn't crash planner path; keep previous behavior
		// and let downstream key lookups fail naturally.
		return key
	}
	if offset < len(key) {
		return key[offset:]
	}
	return key
}
