package qplanner

import (
	"bytes"
	"fmt"

	"github.com/anyproto/any-store/anyenc"
)

// IndexFieldFilter specifies a single field to check from the compound index key.
type IndexFieldFilter struct {
	FieldIdx   int    // 0-based position in the index tuple
	MatchValue []byte // encoded value to match (pre-inverted for reverse fields)
}

// IndexFilterIter filters entries from an index scan by checking field values
// directly from the compound index key tuple, without fetching the document.
// This avoids expensive document fetches for non-matching entries.
type IndexFilterIter struct {
	Source  Iterator
	Filters []IndexFieldFilter
}

func (it *IndexFilterIter) Next() (key []byte, docId []byte, err error) {
	for {
		key, docId, err = it.Source.Next()
		if err != nil || docId == nil {
			return nil, nil, err
		}
		if it.matchesKey(anyenc.Tuple(key)) {
			return key, docId, nil
		}
	}
}

func (it *IndexFilterIter) matchesKey(key anyenc.Tuple) bool {
	for i := range it.Filters {
		f := &it.Filters[i]
		fieldBytes, err := key.FieldBytes(f.FieldIdx)
		if err != nil {
			return false
		}
		if !bytes.Equal(fieldBytes, f.MatchValue) {
			return false
		}
	}
	return true
}

func (it *IndexFilterIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *IndexFilterIter) String() string {
	if len(it.Filters) == 1 {
		return fmt.Sprintf("%s -> IndexFilter(field=%d)", it.Source, it.Filters[0].FieldIdx)
	}
	fields := make([]int, len(it.Filters))
	for i, f := range it.Filters {
		fields[i] = f.FieldIdx
	}
	return fmt.Sprintf("%s -> IndexFilter(fields=%v)", it.Source, fields)
}
