package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// FilterIter takes docIds from an upstream iterator, fetches the document
// from the data namespace, and applies a filter predicate.
// It reuses upstream cached DocParsed when available (e.g. from FetchIter),
// and caches its own parsed result in Plan.DocParsed to avoid double-fetch.
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

		var doc *anyenc.Value
		if it.Plan != nil && it.Plan.DocParsed != nil {
			// Reuse parsed value from upstream FetchIter
			doc = it.Plan.DocParsed
		} else {
			var gerr error
			it.Buf.DocBuf, gerr = it.Data.AppendValue(docId, it.Buf.DocBuf[:0])
			if gerr != nil {
				// doc may have been deleted from data but still in index; skip
				continue
			}

			var perr error
			doc, perr = it.Buf.Parser.Parse(it.Buf.DocBuf)
			if perr != nil {
				return nil, nil, perr
			}
			if it.Plan != nil {
				it.Plan.DocParsed = doc
			}
		}

		if it.Filter == nil || it.Filter.Ok(doc, it.Buf) {
			return key, docId, nil
		}
		// Reset cached value on rejection — next iteration will get a new one
		if it.Plan != nil {
			it.Plan.DocParsed = nil
		}
	}
}

// Close releases resources by closing the source iterator.
func (it *FilterIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *FilterIter) String() string {
	return fmt.Sprintf("%s -> Filter", it.Source)
}
