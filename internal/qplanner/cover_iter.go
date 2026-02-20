package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/query"
)

// CoverIter handles fixed-point lookups where Start == End in bounds.
// Instead of scanning, it does direct point lookups in the index btree.
type CoverIter struct {
	Source  *CursorSource
	IdxInfo *IndexInfo
	Bounds  query.Bounds

	idx int
}

func (it *CoverIter) Next() (key []byte, docId []byte, err error) {
	for it.idx < len(it.Bounds) {
		b := it.Bounds[it.idx]
		it.idx++

		if len(b.Start) == 0 || !bytes.Equal(b.Start, b.End) {
			continue
		}

		if it.IdxInfo.Unique {
			val, gerr := it.Source.Get(b.Start)
			if gerr != nil {
				continue // key not found
			}
			return b.Start, val, nil
		}

		// For non-unique, we need to scan a small range
		// This shouldn't happen for CoverIter (only used for fixed bounds)
		val, gerr := it.Source.Get(b.Start)
		if gerr != nil {
			continue
		}
		return b.Start, val, nil
	}
	return nil, nil, nil
}

// Close releases resources (CoverIter has no cursor to close).
func (it *CoverIter) Close() {}

func (it *CoverIter) String() string {
	return fmt.Sprintf("CoverLookup(%s)[%d bounds]", it.IdxInfo.Name, len(it.Bounds))
}
