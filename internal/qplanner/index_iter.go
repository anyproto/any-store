package qplanner

import (
	"bytes"
	"fmt"
	"time"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
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
	if !it.started {
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
		return nil, nil, false, nil
	}
	k, err := it.cursor.Key()
	if err != nil {
		return nil, nil, false, err
	}
	return it.extractResult(k)
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

// countEntriesWithDedup walks each entry per-bound, reads the per-entry
// value byte, and applies docId-level dedup only for multi-key entries.
// See CountEntries for the strategy split rationale.
func (it *IndexIter) countEntriesWithDedup() (int, error) {
	var seen map[string]struct{}
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

		for it.cursor.Valid() {
			k, kerr := it.cursor.Key()
			if kerr != nil {
				return 0, kerr
			}
			// End-of-bound check.
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
