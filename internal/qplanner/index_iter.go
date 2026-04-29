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

// CountEntries counts all entries within bounds using batch page-level counting.
// This is much faster than calling Next() repeatedly when only a count is needed.
func (it *IndexIter) CountEntries() (int, error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}

	total := 0
	for _, b := range it.Bounds {
		// Seek to start
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

		// Use batch counting
		n, err := it.cursor.CountUntil(b.End, b.EndInclude)
		if err != nil {
			return 0, err
		}
		total += n
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
	// multiKey: until the per-entry value byte lands in a follow-up
	// commit, return true conservatively so consumers continue to dedup.
	// Same correctness as before this commit; perf will improve once the
	// bit is wired up.
	docID := extractDocId(k, len(it.IdxInfo.FieldNames))
	return k, docID, true, nil
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
