package qplanner

import (
	"fmt"
	"sort"

	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// SortIter collects all results from the source iterator, fetches documents,
// computes sort keys, sorts in memory, then yields results in sorted order.
type SortIter struct {
	Source Iterator
	Data   *CursorSource
	Sorter query.Sort
	Buf    *syncpool.DocBuffer

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
		ka, kb := it.entries[a].sortKey, it.entries[b].sortKey
		if len(ka) < len(kb) {
			return true
		}
		if len(ka) > len(kb) {
			return false
		}
		for i := range ka {
			if ka[i] < kb[i] {
				return true
			}
			if ka[i] > kb[i] {
				return false
			}
		}
		return false
	})

	return nil
}

func (it *SortIter) String() string {
	return fmt.Sprintf("Sort -> %s", it.Source)
}
