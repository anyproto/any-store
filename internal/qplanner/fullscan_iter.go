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

	cursor  *btree.Cursor
	started bool
}

func (it *FullScanIter) Next() (key []byte, docId []byte, err error) {
	if it.cursor == nil {
		it.cursor = it.Source.NewCursor()
	}

	for {
		if !it.started {
			it.started = true
			if it.Reverse {
				if len(it.IDBounds) == 1 && len(it.IDBounds[0].End) > 0 {
					// Seek to end bound and go backwards
					if err = it.cursor.Seek(it.IDBounds[0].End); err != nil {
						return nil, nil, err
					}
					if !it.cursor.Valid() {
						if err = it.cursor.Last(); err != nil {
							return nil, nil, err
						}
					}
				} else {
					if err = it.cursor.Last(); err != nil {
						return nil, nil, err
					}
				}
			} else {
				if len(it.IDBounds) == 1 && len(it.IDBounds[0].Start) > 0 {
					if err = it.cursor.Seek(it.IDBounds[0].Start); err != nil {
						return nil, nil, err
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
			return nil, nil, nil
		}

		k, err := it.cursor.Key()
		if err != nil {
			return nil, nil, err
		}

		// Check id bounds
		if len(it.IDBounds) > 0 && !inBounds(k, it.IDBounds) {
			// If we're past bounds in the forward direction, we might be done
			if !it.Reverse && len(it.IDBounds) == 1 && len(it.IDBounds[0].End) > 0 {
				cmp := bytes.Compare(k, it.IDBounds[0].End)
				if cmp > 0 || (cmp == 0 && !it.IDBounds[0].EndInclude) {
					return nil, nil, nil
				}
			}
			if it.Reverse && len(it.IDBounds) == 1 && len(it.IDBounds[0].Start) > 0 {
				cmp := bytes.Compare(k, it.IDBounds[0].Start)
				if cmp < 0 || (cmp == 0 && !it.IDBounds[0].StartInclude) {
					return nil, nil, nil
				}
			}
			continue
		}

		// Apply filter
		if it.Filter != nil {
			val, verr := it.cursor.Value()
			if verr != nil {
				return nil, nil, verr
			}
			doc, perr := it.Buf.Parser.Parse(val)
			if perr != nil {
				return nil, nil, perr
			}
			if !it.Filter.Ok(doc, it.Buf) {
				continue
			}
			if it.Plan != nil {
				it.Plan.DocParsed = doc
			}
		}

		return k, k, nil
	}
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

// inBounds checks if key falls within any of the given bounds.
func inBounds(key []byte, bounds query.Bounds) bool {
	for _, b := range bounds {
		if inBound(key, b) {
			return true
		}
	}
	return false
}

func inBound(key []byte, b query.Bound) bool {
	if len(b.Start) > 0 {
		cmp := bytes.Compare(key, b.Start)
		if cmp < 0 || (cmp == 0 && !b.StartInclude) {
			return false
		}
	}
	if len(b.End) > 0 {
		cmp := bytes.Compare(key, b.End)
		if cmp > 0 || (cmp == 0 && !b.EndInclude) {
			return false
		}
	}
	return true
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
