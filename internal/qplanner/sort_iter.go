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
// When TopK > 0, uses a max-heap of size TopK to keep only the smallest K entries,
// reducing sort from O(N log N) to O(N log K) and memory from O(N) to O(K) entries.
type SortIter struct {
	Source  Iterator
	Data    *CursorSource
	Sorter  query.Sort
	Buf     *syncpool.DocBuffer
	Plan    *Plan
	TopK    int // if > 0, keep only the top K entries (limit + offset)
	arena   []byte
	entries []sortEntry
	idx     int
	PartiallySorted bool // leading index fields match sort order; pdqsort benefits automatically
	inited          bool
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
	cmp := func(a, b sortEntry) int {
		ak := it.arena[a.off : a.off+uint32(a.keyLen)]
		bk := it.arena[b.off : b.off+uint32(b.keyLen)]
		return bytes.Compare(ak, bk)
	}

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
		e := sortEntry{off: off, keyLen: keyLen, docLen: uint16(len(docId))}

		if it.TopK > 0 {
			// Max-heap of size TopK: keep only the smallest K entries.
			if len(it.entries) < it.TopK {
				it.entries = append(it.entries, e)
				it.heapUp(len(it.entries) - 1)
			} else if cmp(e, it.entries[0]) < 0 {
				// New entry is smaller than the heap max — replace root.
				it.entries[0] = e
				it.heapDown(0)
			}
		} else {
			it.entries = append(it.entries, e)
		}
	}

	slices.SortFunc(it.entries, cmp)
	return nil
}

// heapUp restores the max-heap property by moving entries[i] up.
func (it *SortIter) heapUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if it.heapLess(p, i) {
			it.entries[p], it.entries[i] = it.entries[i], it.entries[p]
			i = p
		} else {
			break
		}
	}
}

// heapDown restores the max-heap property by moving entries[i] down.
func (it *SortIter) heapDown(i int) {
	n := len(it.entries)
	for {
		largest := i
		l, r := 2*i+1, 2*i+2
		if l < n && it.heapLess(largest, l) {
			largest = l
		}
		if r < n && it.heapLess(largest, r) {
			largest = r
		}
		if largest == i {
			break
		}
		it.entries[i], it.entries[largest] = it.entries[largest], it.entries[i]
		i = largest
	}
}

// heapLess returns true if entries[i] < entries[j] (for max-heap, parent should be largest).
func (it *SortIter) heapLess(i, j int) bool {
	a, b := it.entries[i], it.entries[j]
	ak := it.arena[a.off : a.off+uint32(a.keyLen)]
	bk := it.arena[b.off : b.off+uint32(b.keyLen)]
	return bytes.Compare(ak, bk) < 0
}

// Close releases resources by closing the source iterator.
func (it *SortIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
}

func (it *SortIter) String() string {
	if it.TopK > 0 {
		return fmt.Sprintf("%s -> TopK(%d)", it.Source, it.TopK)
	}
	return fmt.Sprintf("%s -> Sort", it.Source)
}
