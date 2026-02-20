package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// FullScanIter scans the data namespace, applies a filter in memory,
// and optionally applies id-bounds to skip ranges.
type FullScanIter struct {
	Source   *CursorSource
	Filter   query.Filter
	IDBounds query.Bounds
	Buf      *syncpool.DocBuffer
	Reverse  bool
	Plan     *Plan // set by setPlanRef for doc value caching

	cursor   *btree.Cursor
	boundIdx int
	started  bool
}

func (it *FullScanIter) Next() (key []byte, docId []byte, err error) {
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

func (it *FullScanIter) nextNoBounds() (key []byte, docId []byte, err error) {
	for {
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

		if ok, ferr := it.checkFilter(); ferr != nil {
			return nil, nil, ferr
		} else if !ok {
			continue
		}

		return k, k, nil
	}
}

func (it *FullScanIter) nextWithBounds() (key []byte, docId []byte, err error) {
	for {
		if it.Reverse {
			if it.boundIdx < 0 {
				return nil, nil, nil
			}
		} else {
			if it.boundIdx >= len(it.IDBounds) {
				return nil, nil, nil
			}
		}

		b := it.IDBounds[it.boundIdx]

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
						// Seek finds >=, so if past End or at End without include, back up
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
			it.advanceBound()
			continue
		}

		k, err := it.cursor.Key()
		if err != nil {
			return nil, nil, err
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
			return nil, nil, ferr
		} else if !ok {
			continue
		}

		return k, k, nil
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
	val, err := it.cursor.Value()
	if err != nil {
		return false, err
	}
	doc, err := it.Buf.Parser.Parse(val)
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
	if it.Filter != nil {
		s += "(filtered)"
	}
	return s
}

// DocValue fetches and parses the document value at the current cursor position.
func (it *FullScanIter) DocValue() (*anyenc.Value, error) {
	val, err := it.cursor.Value()
	if err != nil {
		return nil, err
	}
	return it.Buf.Parser.Parse(val)
}

// RawValue returns the raw bytes of the value at the current cursor position.
func (it *FullScanIter) RawValue() ([]byte, error) {
	return it.cursor.Value()
}
