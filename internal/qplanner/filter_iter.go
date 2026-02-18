package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// FilterIter takes docIds from an upstream iterator, fetches the document
// from the data namespace, and applies a filter predicate.
// It caches the last fetched document value in Plan.DocValue to avoid double-fetch.
type FilterIter struct {
	Source Iterator
	Data   *CursorSource
	Filter query.Filter
	Buf    *syncpool.DocBuffer
	Plan   *Plan // set by BuildPlan to cache fetched doc values
}

func (it *FilterIter) Next() (key []byte, docId []byte, err error) {
	for {
		key, docId, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, err
		}

		val, gerr := it.Data.Get(docId)
		if gerr != nil {
			// doc may have been deleted from data but still in index; skip
			continue
		}

		// Cache the fetched value to avoid re-fetching in planIterator.Doc()
		if it.Plan != nil {
			it.Plan.DocValue = append(it.Plan.DocValue[:0], val...)
		}

		if it.Filter == nil {
			return key, docId, nil
		}

		doc, perr := it.Buf.Parser.Parse(val)
		if perr != nil {
			return nil, nil, perr
		}
		if it.Filter.Ok(doc, it.Buf) {
			return key, docId, nil
		}
	}
}

func (it *FilterIter) String() string {
	return fmt.Sprintf("%s -> Filter", it.Source)
}
