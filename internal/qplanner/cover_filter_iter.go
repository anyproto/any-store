package qplanner

import (
	"fmt"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// CoverFilterIter uses a secondary index to filter docIds from an upstream iterator.
// For each docId, it fetches the document, extracts the index field values,
// and checks if they fall within the index bounds.
// This avoids a full data scan when a secondary index can narrow results.
type CoverFilterIter struct {
	Source  Iterator
	Data    *CursorSource
	IdxInfo *IndexInfo
	Index   *CursorSource
	Bounds  query.Bounds
	Buf     *syncpool.DocBuffer
}

func (it *CoverFilterIter) Next() (key []byte, docId []byte, err error) {
	for {
		key, docId, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, err
		}

		// Fetch the document to extract index field values
		val, gerr := it.Data.Get(docId)
		if gerr != nil {
			continue
		}

		doc, perr := it.Buf.Parser.Parse(val)
		if perr != nil {
			return nil, nil, perr
		}

		// Build the index key for this document
		var indexKey anyenc.Tuple
		for i := range it.IdxInfo.FieldNames {
			indexKey = it.IdxInfo.AppendIndexKey(indexKey, doc, i)
		}

		// Check if the index key falls within any of the bounds
		if matchesBounds(indexKey, it.Bounds) {
			return key, docId, nil
		}
	}
}

// matchesBounds checks if a key falls within any of the given bounds.
func matchesBounds(key anyenc.Tuple, bounds query.Bounds) bool {
	for _, b := range bounds {
		if matchesBound(key, b) {
			return true
		}
	}
	return false
}

func matchesBound(key []byte, b query.Bound) bool {
	if len(b.Start) > 0 {
		cmp := compareTuplePrefix(key, b.Start)
		if cmp < 0 || (cmp == 0 && !b.StartInclude) {
			return false
		}
	}
	if len(b.End) > 0 {
		cmp := compareTuplePrefix(key, b.End)
		if cmp > 0 || (cmp == 0 && !b.EndInclude) {
			return false
		}
	}
	return true
}

// compareTuplePrefix compares key against bound, treating bound as a prefix if shorter.
func compareTuplePrefix(key, bound []byte) int {
	minLen := len(key)
	if len(bound) < minLen {
		minLen = len(bound)
	}
	for i := range minLen {
		if key[i] < bound[i] {
			return -1
		}
		if key[i] > bound[i] {
			return 1
		}
	}
	if len(key) < len(bound) {
		return -1
	}
	return 0
}

// Close releases resources by closing the source iterator.
func (it *CoverFilterIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *CoverFilterIter) String() string {
	return fmt.Sprintf("%s -> CoverFilter(%s)[bounds=%s]", it.Source, it.IdxInfo.Name, it.Bounds.String())
}
