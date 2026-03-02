package qplanner

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// SortIter collects all results from the source iterator, fetches documents,
// computes sort keys, sorts in memory, then yields results in sorted order.
type SortIter struct {
	Source          Iterator
	Data            *CursorSource
	Sorter          query.Sort
	Buf             *syncpool.DocBuffer
	Plan            *Plan
	PartiallySorted bool // leading index fields match sort order; pdqsort benefits automatically

	arena []byte
	entries    []sortEntry
	idx        int
	inited     bool
}

type sortEntry struct {
	off    uint32 // offset into arena
	keyLen uint16 // total length (sort key + docId suffix)
	docLen uint16 // docId length (trailing portion)
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
	docId = it.arena[e.off+uint32(e.keyLen)-uint32(e.docLen) : e.off+uint32(e.keyLen)]

	// Clear DocParsed so planIterator.Doc() does a lazy fetch by docId.
	// collectAndSort() leaves DocParsed pointing at the last collected doc,
	// which is NOT the doc for this sorted entry.
	if it.Plan != nil {
		it.Plan.DocParsed = nil
	}

	return docId, docId, nil
}

// growArena ensures the arena has at least need bytes of free capacity,
// growing in tiered steps to avoid frequent small reallocations
// and excessive doubling at large sizes.
func (it *SortIter) growArena(need int) {
	if cap(it.arena)-len(it.arena) >= need {
		return
	}
	grow := 100 << 10 // 100KB
	switch c := cap(it.arena); {
	case c < 1<<10:
		grow = 1 << 10 // 1KB
	case c < 10<<10:
		grow = 10 << 10 // 10KB
	}
	if grow < need {
		grow = need
	}
	it.arena = slices.Grow(it.arena, grow)
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

		// Prefer already-parsed doc from upstream (FullScanIter/FetchIter/FilterIter)
		doc := it.Plan.DocParsed
		if doc == nil {
			// Cursor-free point lookup: avoids Cursor allocation
			var verr error
			it.Buf.DocBuf, verr = it.Data.AppendValue(docId, it.Buf.DocBuf[:0])
			if verr != nil {
				continue
			}
			var perr error
			doc, perr = it.Buf.Parser.Parse(it.Buf.DocBuf)
			if perr != nil {
				return perr
			}
		}

		it.growArena(256)
		off := uint32(len(it.arena))
		it.arena = it.Sorter.AppendKey(it.arena, doc)
		it.arena = append(it.arena, docId...)
		keyLen := uint16(len(it.arena) - int(off))
		it.entries = append(it.entries, sortEntry{
			off: off, keyLen: keyLen, docLen: uint16(len(docId)),
		})
	}

	slices.SortFunc(it.entries, func(a, b sortEntry) int {
		ak := it.arena[a.off : a.off+uint32(a.keyLen)]
		bk := it.arena[b.off : b.off+uint32(b.keyLen)]
		return bytes.Compare(ak, bk)
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
