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

	// indexHasMultiKey caches the canonical-key probe result for the lifetime
	// of this iterator (set lazily on the first CountEntries dispatch that
	// needs it). Per-IndexIter; never shared across goroutines.
	indexHasMultiKeyProbed bool
	indexHasMultiKey       bool

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

		// boundIdx counts consumed intervals; the list is ascending-sorted, so
		// a reverse scan consumes it from the top — otherwise global descending
		// order breaks across intervals and Limit keeps the wrong rows (BUG-13).
		bi := it.boundIdx
		if it.Reverse {
			bi = len(it.Bounds) - 1 - it.boundIdx
		}
		b := it.Bounds[bi]

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

// arrayPrefixInverted is the array tag for a REVERSE-flagged single-field index.
// Such an index stores the whole-array key bitwise-inverted (index.go
// writeValues), so its leading byte is ^TypeArray (0xF9) instead of 0x06.
var arrayPrefixInverted = []byte{^byte(anyenc.TypeArray)}

// arrayProbePrefix returns the leading byte the probe must seek to detect a
// multi-key (array) entry: the inverted array tag for a reverse single-field
// index, the plain array tag otherwise.
func arrayProbePrefix(reverse bool) []byte {
	if reverse {
		return arrayPrefixInverted
	}
	return arrayPrefix
}

// indexProbeAnyMultiKey reports whether the index namespace has any entry whose
// key begins with the array type tag — i.e. whether any indexed doc had an
// array-typed value. It is a single btree Seek, snapshot-consistent with the
// caller's read tx. fieldReverse selects the inverted array tag for a reverse
// single-field index.
//
// PRECONDITION: only valid for single-field indexes. For a compound index the
// array field's value is not at byte position 0 of the key, so its array tag
// sits mid-key and the Seek would miss it (false negative). Callers must route
// compound/non-PointLookup shapes elsewhere before probing.
func indexProbeAnyMultiKey(cs *CursorSource, fieldReverse bool) (bool, error) {
	c := cs.NewCursor()
	defer c.Close()
	prefix := arrayProbePrefix(fieldReverse)
	if err := c.Seek(prefix); err != nil {
		return false, err
	}
	if !c.Valid() {
		return false, nil
	}
	k, err := c.Key()
	if err != nil {
		return false, err
	}
	return len(k) > 0 && k[0] == prefix[0], nil
}

// CountEntries counts distinct documents matching this index iterator's
// bounds via a 4-branch dispatch:
//
//	Branch 1 (len(Bounds) <= 1): page-batch CountUntil. No cross-bound dedup
//	  concern — within-doc dedup in insertKeys guarantees ≤1 entry per distinct
//	  value per doc, so entry count == doc count. Fast: no per-entry walk.
//
//	Branch 2 (compound / non-PointLookup): pooled seen-set, skip-scalar mode.
//	  The canonical-key probe is only a valid multi-key detector for single-field
//	  indexes (the array tag is at byte 0 only there), so any other shape skips
//	  the probe and dedups by docId. Reading the per-entry value byte lets scalar
//	  entries be counted without touching the map — a scalar compound index
//	  queried on its prefix costs just the walk.
//
//	Branch 3 (single-field PointLookup, probe says no multi-key): page-batch
//	  CountUntil summed across bounds. With no array-valued doc, every doc has
//	  exactly one entry and matches at most one bound, so the sum is the
//	  distinct-doc count — no dedup needed.
//
//	Branch 4 (single-field PointLookup, probe says multi-key): pooled seen-set,
//	  blind mode (every entry interned). Such an index is array-dense, so the
//	  value byte is not consulted. An array doc whose values straddle several
//	  $in bounds is counted once.
//
// This is the I-05 fix: there is no peek-then-batch shortcut to misclassify a
// scalar-first bound that also holds multi-key entries.
func (it *IndexIter) CountEntries() (int, error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}

	// Branch 1.
	if len(it.Bounds) <= 1 {
		return it.countEntriesBatch()
	}

	// Branch 2: the probe doesn't apply to compound / non-PointLookup shapes;
	// skip-scalar mode counts scalar entries without deduping them.
	if !it.PointLookup || len(it.IdxInfo.FieldNames) != 1 {
		return it.countEntriesViaSeenSet(true)
	}

	// Branches 3 & 4: single-field PointLookup — the probe decides.
	hasMultiKey, err := it.probeMultiKey()
	if err != nil {
		return 0, err
	}
	if !hasMultiKey {
		return it.countEntriesBatch() // Branch 3
	}
	return it.countEntriesViaSeenSet(false) // Branch 4: blind dedup
}

// probeMultiKey runs the canonical-key probe lazily and caches the result for
// this iterator. True iff the index holds a multi-key entry — a doc with an
// array-typed value writes a canonical 0x06 whole-array key (writeValues has
// done this since before the value-byte feature, so legacy multi-key data is
// detected too). The caller must have established that this is a single-field
// index (see indexProbeAnyMultiKey's precondition); CountEntries enforces that
// by routing compound/non-PointLookup to the seen-set path (Branch 2) first.
func (it *IndexIter) probeMultiKey() (bool, error) {
	if it.indexHasMultiKeyProbed {
		return it.indexHasMultiKey, nil
	}
	// CountEntries only reaches here for single-field indexes (it gates on
	// len(FieldNames)==1), so Reverse[0] is the array field's direction.
	fieldReverse := len(it.IdxInfo.Reverse) > 0 && it.IdxInfo.Reverse[0]
	has, err := indexProbeAnyMultiKey(it.Source, fieldReverse)
	if err != nil {
		return false, err
	}
	it.indexHasMultiKey = has
	it.indexHasMultiKeyProbed = true
	return has, nil
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

// seekBoundStart positions the cursor at the first entry of the given
// bound, accounting for StartInclude. Shared by countEntriesBatch and
// countEntriesViaSeenSet.
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
