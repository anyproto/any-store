package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// FullScanIter scans the data namespace, applies a filter in memory,
// and optionally applies id-bounds to skip ranges.
type FullScanIter struct {
	Filter   query.Filter
	Source   *CursorSource
	Buf      *syncpool.DocBuffer
	Plan     *Plan // set by setPlanRef for doc value caching
	cursor   *btree.Cursor
	IDBounds query.Bounds
	boundIdx int
	Offset   int // batch-skip offset (only when Filter == nil)
	Reverse  bool
	started  bool
}

func (it *FullScanIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
		if it.Reverse && len(it.IDBounds) > 0 {
			it.boundIdx = len(it.IDBounds) - 1
		}
	}

	if len(it.IDBounds) == 0 {
		return it.nextNoBounds()
	}
	return it.nextWithBounds()
}

func (it *FullScanIter) nextNoBounds() (key []byte, docId []byte, multiKey bool, err error) {
	for {
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
			// Batch-skip offset entries at the cursor level.
			// Offset is only set by the planner when there's no meaningful filter.
			if it.Offset > 0 && it.cursor.Valid() {
				if it.Reverse {
					err = it.cursor.SkipBackward(it.Offset)
				} else {
					err = it.cursor.Skip(it.Offset)
				}
				it.Offset = 0
				if err != nil {
					return nil, nil, false, err
				}
				if !it.cursor.Valid() {
					return nil, nil, false, nil
				}
				// Fall through to return the entry at the new position.
				k, err := it.cursor.Key()
				if err != nil {
					return nil, nil, false, err
				}
				// FullScan walks the data namespace; docId is the primary key — unique by construction.
			return k, k, false, nil
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

		if ok, ferr := it.checkFilter(); ferr != nil {
			return nil, nil, false, ferr
		} else if !ok {
			continue
		}

		return k, k, false, nil
	}
}

func (it *FullScanIter) nextWithBounds() (key []byte, docId []byte, multiKey bool, err error) {
	for {
		if it.Reverse {
			if it.boundIdx < 0 {
				return nil, nil, false, nil
			}
		} else {
			if it.boundIdx >= len(it.IDBounds) {
				return nil, nil, false, nil
			}
		}

		b := it.IDBounds[it.boundIdx]

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
						// Seek finds >=, so if past End or at End without include, back up
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
			it.advanceBound()
			continue
		}

		k, err := it.cursor.Key()
		if err != nil {
			return nil, nil, false, err
		}

		// Check if past current bound
		if !it.Reverse && len(b.End) > 0 {
			cmp := bytes.Compare(k, b.End)
			if cmp > 0 || (cmp == 0 && !b.EndInclude) {
				it.advanceBound()
				continue
			}
		}
		if it.Reverse && len(b.Start) > 0 {
			cmp := bytes.Compare(k, b.Start)
			if cmp < 0 || (cmp == 0 && !b.StartInclude) {
				it.advanceBound()
				continue
			}
		}

		if ok, ferr := it.checkFilter(); ferr != nil {
			return nil, nil, false, ferr
		} else if !ok {
			continue
		}

		return k, k, false, nil
	}
}

func (it *FullScanIter) advanceBound() {
	if it.Reverse {
		it.boundIdx--
	} else {
		it.boundIdx++
	}
	it.started = false
}

func (it *FullScanIter) checkFilter() (ok bool, err error) {
	if it.Filter == nil {
		return true, nil
	}
	it.Buf.DocBuf, err = it.cursor.AppendValue(it.Buf.DocBuf[:0])
	if err != nil {
		return false, err
	}
	doc, err := it.Buf.Parser.ParseOwned(it.Buf.DocBuf)
	if err != nil {
		return false, err
	}
	if !it.Filter.Ok(doc, it.Buf) {
		return false, nil
	}
	if it.Plan != nil {
		it.Plan.DocParsed = doc
	}
	return true, nil
}

// Close releases the underlying cursor resources.
func (it *FullScanIter) Close() {
	if it.cursor != nil {
		it.cursor.Close()
		it.cursor = nil
	}
}

func (it *FullScanIter) String() string {
	s := "FullScan"
	if it.Reverse {
		s += "(reverse)"
	}
	if len(it.IDBounds) > 0 {
		s += fmt.Sprintf("[idBounds=%s]", it.IDBounds.String())
	}
	if it.Offset > 0 {
		s += fmt.Sprintf("[skip=%d]", it.Offset)
	}
	if it.Filter != nil {
		s += "(filtered)"
	}
	return s
}

// DocValue fetches and parses the document value at the current cursor position.
func (it *FullScanIter) DocValue() (*anyenc.Value, error) {
	var err error
	it.Buf.DocBuf, err = it.cursor.AppendValue(it.Buf.DocBuf[:0])
	if err != nil {
		return nil, err
	}
	return it.Buf.Parser.ParseOwned(it.Buf.DocBuf)
}

// RawValue returns the raw bytes of the value at the current cursor position.
func (it *FullScanIter) RawValue() ([]byte, error) {
	var err error
	it.Buf.DocBuf, err = it.cursor.AppendValue(it.Buf.DocBuf[:0])
	if err != nil {
		return nil, err
	}
	return it.Buf.DocBuf, nil
}
