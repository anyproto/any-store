package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
)

// IndexIter iterates over an index namespace using bounds.
// For non-unique indexes, key = indexFields + docId; for unique indexes, value = docId.
type IndexIter struct {
	Source  *CursorSource
	IdxInfo *IndexInfo
	Bounds  query.Bounds
	Reverse bool

	cursor   *btree.Cursor
	boundIdx int
	started  bool
}

func (it *IndexIter) Next() (key []byte, docId []byte, err error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}

	// No bounds: full index scan
	if len(it.Bounds) == 0 {
		return it.nextNoBounds()
	}

	for {
		if it.boundIdx >= len(it.Bounds) {
			return nil, nil, nil
		}

		b := it.Bounds[it.boundIdx]

		if !it.started {
			it.started = true
			if it.Reverse {
				if len(b.End) > 0 {
					if err = it.cursor.Seek(b.End); err != nil {
						return nil, nil, err
					}
					if !it.cursor.Valid() {
						if err = it.cursor.Last(); err != nil {
							return nil, nil, err
						}
					} else {
						// Check if we need to back up (Seek finds >=)
						k, kerr := it.cursor.Key()
						if kerr != nil {
							return nil, nil, kerr
						}
						cmp := bytes.Compare(k, b.End)
						if cmp > 0 || (cmp == 0 && !b.EndInclude) {
							if err = it.cursor.Previous(); err != nil {
								return nil, nil, err
							}
						}
					}
				} else {
					if err = it.cursor.Last(); err != nil {
						return nil, nil, err
					}
				}
			} else {
				if len(b.Start) > 0 {
					if err = it.cursor.Seek(b.Start); err != nil {
						return nil, nil, err
					}
					if it.cursor.Valid() && !b.StartInclude {
						k, kerr := it.cursor.Key()
						if kerr != nil {
							return nil, nil, kerr
						}
						if bytes.Equal(k, b.Start) {
							if err = it.cursor.Next(); err != nil {
								return nil, nil, err
							}
						}
					}
				} else {
					if err = it.cursor.First(); err != nil {
						return nil, nil, err
					}
				}
			}
		} else {
			if it.Reverse {
				if err = it.cursor.Previous(); err != nil {
					return nil, nil, err
				}
			} else {
				if err = it.cursor.Next(); err != nil {
					return nil, nil, err
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
			return nil, nil, err
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

func (it *IndexIter) nextNoBounds() (key []byte, docId []byte, err error) {
	if !it.started {
		it.started = true
		if it.Reverse {
			if err = it.cursor.Last(); err != nil {
				return nil, nil, err
			}
		} else {
			if err = it.cursor.First(); err != nil {
				return nil, nil, err
			}
		}
	} else {
		if it.Reverse {
			if err = it.cursor.Previous(); err != nil {
				return nil, nil, err
			}
		} else {
			if err = it.cursor.Next(); err != nil {
				return nil, nil, err
			}
		}
	}
	if !it.cursor.Valid() {
		return nil, nil, nil
	}
	k, err := it.cursor.Key()
	if err != nil {
		return nil, nil, err
	}
	return it.extractResult(k)
}

// Close releases the underlying cursor resources.
func (it *IndexIter) Close() {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
}

func (it *IndexIter) extractResult(k []byte) (key []byte, docId []byte, err error) {
	if it.IdxInfo.Unique {
		val, verr := it.cursor.Value()
		if verr != nil {
			return nil, nil, verr
		}
		return k, val, nil
	}
	// Non-unique: key = indexFields + docId, extract docId
	docID := extractDocId(k, len(it.IdxInfo.FieldNames))
	return k, docID, nil
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
	var offset int
	var count int
	_ = key.ReadBytes(func(b []byte) error {
		count++
		if count <= numFields {
			offset += len(b)
			return nil
		}
		return errStopIter
	})
	if offset < len(key) {
		return key[offset:]
	}
	return key
}

var errStopIter = fmt.Errorf("stop")
