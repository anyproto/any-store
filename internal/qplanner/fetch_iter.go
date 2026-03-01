package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/syncpool"
)

// FetchIter wraps an index-yielding iterator (which produces docIds)
// and performs point-lookups in the data namespace to fetch full documents.
// It parses and caches the result in Plan.DocParsed to avoid double-fetch/double-parse.
type FetchIter struct {
	Source Iterator
	Data   *CursorSource
	Buf    *syncpool.DocBuffer
	Plan   *Plan // set by BuildPlan for doc value caching
}

func (it *FetchIter) Next() (key []byte, docId []byte, err error) {
	for {
		key, docId, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, err
		}

		// Cursor-free point lookup: avoids Cursor struct allocation and stack growth.
		it.Buf.DocBuf, err = it.Data.AppendValue(docId, it.Buf.DocBuf[:0])
		if err != nil {
			if err == btree.ErrKeyNotFound {
				// doc may have been deleted from data but still in index; skip
				continue
			}
			return nil, nil, err
		}

		// Parse and cache the value to avoid re-fetching and re-parsing later
		if it.Plan != nil {
			doc, perr := it.Buf.Parser.Parse(it.Buf.DocBuf)
			if perr != nil {
				return nil, nil, perr
			}
			it.Plan.DocParsed = doc
		}

		return key, docId, nil
	}
}

// Close releases resources by closing the source iterator.
func (it *FetchIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *FetchIter) String() string {
	return fmt.Sprintf("%s -> Fetch", it.Source)
}
