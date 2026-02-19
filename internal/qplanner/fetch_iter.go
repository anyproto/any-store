package qplanner

import "fmt"

// FetchIter wraps an index-yielding iterator (which produces docIds)
// and performs point-lookups in the data namespace to fetch full documents.
// It caches the fetched value in Plan.DocValue to avoid double-fetch.
type FetchIter struct {
	Source Iterator
	Data   *CursorSource
	Plan   *Plan // set by BuildPlan for doc value caching
}

func (it *FetchIter) Next() (key []byte, docId []byte, err error) {
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

		// Cache the fetched value to avoid re-fetching later
		if it.Plan != nil {
			it.Plan.DocValue = append(it.Plan.DocValue[:0], val...)
		}

		return key, docId, nil
	}
}

func (it *FetchIter) String() string {
	return fmt.Sprintf("%s -> Fetch", it.Source)
}
