package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/query"
)

// CoverIter handles fixed-point lookups where Start == End in bounds.
// Uses single-shot btree SeekKey + prefix comparison to find matching entries.
type CoverIter struct {
	Source  *CursorSource
	IdxInfo *IndexInfo
	Bounds  query.Bounds

	idx    int
	keyBuf []byte // reusable buffer for SeekKey results
}

func (it *CoverIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	for it.idx < len(it.Bounds) {
		b := it.Bounds[it.idx]
		it.idx++

		if len(b.Start) == 0 {
			continue
		}

		var kerr error
		it.keyBuf, kerr = it.Source.AppendSeekKey(b.Start, it.keyBuf[:0])
		if kerr != nil {
			continue // key not found
		}
		if !bytes.HasPrefix(it.keyBuf, b.Start) {
			continue
		}
		docID := extractDocId(it.keyBuf, len(it.IdxInfo.FieldNames))
		// Unique-index point lookup: at most one entry per doc — never a duplicate.
		return it.keyBuf, docID, false, nil
	}
	return nil, nil, false, nil
}

// Close releases resources (CoverIter uses single-shot lookups, no cursor to close).
func (it *CoverIter) Close() {}

func (it *CoverIter) String() string {
	return fmt.Sprintf("CoverLookup(%s)[%d bounds]", it.IdxInfo.Name, len(it.Bounds))
}
