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

	// ProjectFields, when non-nil, is the set of top-level field roots the
	// filter (and any sort fed from the same cached doc) references. checkFilter
	// then decodes ONLY those fields to test the filter (anyenc OP_Column
	// analogue), skipping the rest of the record. nil => full ParseOwned.
	// It points into projectBuf when the field set fits, so configuring
	// projection costs no heap allocation per query.
	ProjectFields []string
	projectBuf    [4]string

	// EmitFull is true when a matched row's cached doc is emitted directly to
	// the consumer (planIterator.Doc reads Plan.DocParsed with no intervening
	// stage that re-parses). In that case a matched doc must be re-parsed in
	// FULL before caching, since a projected doc would be missing fields the
	// consumer expects. When false (CountOnly, or a SortIter above re-parses on
	// emit), the projected doc is cached as-is.
	EmitFull bool
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
	// No filter: nothing to test. But when a downstream stage consumes the
	// scanned doc from the cursor (a SortIter that extracts the sort key from
	// Plan.DocParsed), decode the projected sort-key fields straight from the
	// cursor value and cache them — this lets the SortIter skip a per-row
	// point-lookup re-fetch + re-parse of the full document. ProjectFields is
	// only set in this no-filter branch when such a sort sits above (see
	// buildFullScanChain), and EmitFull is false there (the sort re-parses the
	// survivors in full on emit, so caching a projected doc is safe).
	if it.Filter == nil {
		if it.ProjectFields == nil || it.Plan == nil {
			return true, nil
		}
		it.Buf.DocBuf, err = it.cursor.AppendValue(it.Buf.DocBuf[:0])
		if err != nil {
			return false, err
		}
		doc, perr := it.Buf.Parser.ParseProjected(it.Buf.DocBuf, it.ProjectFields)
		if perr != nil {
			return false, perr
		}
		it.Plan.DocParsed = doc
		return true, nil
	}

	it.Buf.DocBuf, err = it.cursor.AppendValue(it.Buf.DocBuf[:0])
	if err != nil {
		return false, err
	}

	// Lazy/projected decode: when the filter's (and any cache-sharing sort's)
	// referenced field roots are statically known, decode only those fields to
	// evaluate the filter and skip the rest of the record. Mirrors SQLite's
	// OP_Column, which never materializes columns the query doesn't touch.
	if it.ProjectFields != nil {
		doc, perr := it.Buf.Parser.ParseProjected(it.Buf.DocBuf, it.ProjectFields)
		if perr != nil {
			return false, perr
		}
		if !it.Filter.Ok(doc, it.Buf) {
			return false, nil
		}
		if it.Plan != nil {
			if it.EmitFull {
				// The matched doc will be emitted whole straight from the cache
				// (no stage re-parses it), so it must be the COMPLETE document,
				// not the projected subset. Re-parse in full from the same raw
				// bytes before caching.
				full, ferr := it.Buf.Parser.ParseOwned(it.Buf.DocBuf)
				if ferr != nil {
					return false, ferr
				}
				it.Plan.DocParsed = full
			} else {
				it.Plan.DocParsed = doc
			}
		}
		return true, nil
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
