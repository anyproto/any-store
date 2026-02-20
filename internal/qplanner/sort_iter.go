package qplanner

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// SortIter collects all results from the source iterator, fetches documents,
// computes sort keys, sorts in memory, then yields results in sorted order.
type SortIter struct {
	Source    Iterator
	Data      *CursorSource
	Sorter    query.Sort
	Buf       *syncpool.DocBuffer
	PreSorted bool // hint that upstream data is partially sorted (helps quicksort)

	entries []sortEntry
	idx     int
	inited  bool
}

type sortEntry struct {
	docId   []byte
	sortKey []byte
}

func (it *SortIter) Next() (key []byte, docId []byte, err error) {
	if !it.inited {
		it.inited = true
		if err := it.collectAndSort(); err != nil {
			return nil, nil, err
		}
	}

	if it.idx >= len(it.entries) {
		return nil, nil, nil
	}

	e := it.entries[it.idx]
	it.idx++
	return e.docId, e.docId, nil
}

func (it *SortIter) collectAndSort() error {
	for {
		_, docId, err := it.Source.Next()
		if err != nil {
			return err
		}
		if docId == nil {
			break
		}

		val, gerr := it.Data.Get(docId)
		if gerr != nil {
			continue
		}

		doc, perr := it.Buf.Parser.Parse(val)
		if perr != nil {
			return perr
		}

		sk := it.Sorter.AppendKey(nil, doc)
		it.entries = append(it.entries, sortEntry{
			docId:   append([]byte(nil), docId...),
			sortKey: sk,
		})
	}

	sort.SliceStable(it.entries, func(a, b int) bool {
		return bytes.Compare(it.entries[a].sortKey, it.entries[b].sortKey) < 0
	})

	return nil
}

// Close releases resources by closing the source iterator.
func (it *SortIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *SortIter) String() string {
	return fmt.Sprintf("%s -> Sort", it.Source)
}
