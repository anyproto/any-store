package qplanner

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
)

// SortIter collects all results from the source iterator, fetches documents,
// computes sort keys, sorts in memory, then yields results in sorted order.
// When TopK > 0, uses a max-heap of size TopK to keep only the smallest K entries,
// reducing sort from O(N log N) to O(N log K) and memory from O(N) to O(K) entries.
//
// Arena bounding: a row's packed sort-key + docId is built in the arena's spare
// tail capacity and kept ONLY when the row enters/stays in the top-K heap
// (matching SQLite's pushOntoSorter in select.c, which inserts a record into the
// sorter AFTER the LIMIT eviction so "loser" rows are never materialized;
// vdbesort.c notes the sorter holds <= LIMIT+OFFSET records). A losing row
// simply truncates the tail back — zero retained bytes, zero allocation. When a
// smaller row evicts the heap root and the root's vacated slot is exactly the
// new row's size, the row overwrites that slot in place (reuse, no growth);
// otherwise the row stays at the tail and the root's bytes become dead, later
// reclaimed by a compaction guard that rebuilds the arena from the live entries
// when fragmentation grows large. This keeps the arena's high-water mark O(K)
// without any per-eviction memmove. With TopK <= 0 (full sort) every row is
// materialized, as before.
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
	// liveBytes is the sum of keyLen over the entries currently in the heap.
	// Used by the compaction guard to detect when arena waste (dead bytes left
	// by evicted entries whose slot could not be reused in place) is large.
	liveBytes int
	order     []int // reusable scratch: live-entry indices sorted by arena offset (compaction)
	PartiallySorted bool // leading index fields match sort order; pdqsort benefits automatically
	inited          bool
	// rawSorter is Sorter's RawSort fast path, resolved once in collectAndSort.
	// When the source yields no pre-parsed document, the sort key is built by
	// seeking the sort fields' raw fragments instead of parsing the whole
	// document (see appendSortKey). rawFallbacks counts consecutive documents
	// the raw path could not handle (e.g. an array container on the sort
	// path); after a few the fast path is disabled for the rest of this sort —
	// field shapes are homogeneous within a collection, so early fallbacks
	// predict wasted walks on every remaining document.
	rawSorter    query.RawSort
	rawFallbacks int
}

// sortRawFallbackMax disables the raw sort-key path after this many
// consecutive unhandled documents.
const sortRawFallbackMax = 8

type sortEntry struct {
	off      uint32 // offset into arena
	keyLen   uint16 // total length (sort key + docId suffix)
	docLen   uint16 // docId length (trailing portion)
	multiKey uint8  // 1 = upstream marked this entry multiKey; 0 = unique. Forwarded as-is on emit so consumer-side DocDedup can skip the seen-set for unique streams.
	_        [3]uint8 // explicit padding so the struct size stays predictable across archs
}

func (it *SortIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if !it.inited {
		it.inited = true
		if err := it.collectAndSort(); err != nil {
			return nil, nil, false, err
		}
	}

	if it.idx >= len(it.entries) {
		return nil, nil, false, nil
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

	return docId, docId, e.multiKey == 1, nil
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

// appendSortKey appends the row's sort key for doc (pre-parsed upstream
// document, may be nil) to dst. With no pre-parsed document it prefers the
// RawSort fast path over raw (the decoded document bytes): the sort fields'
// fragments are seeked and encoded without parsing the rest of the document —
// the full parse just to read the sort fields dominated the unindexed-sort
// profile. When the raw path cannot handle the document (array container on a
// sort path) it parses raw — the already-decoded bytes, so the s2 decode is
// not repeated — and takes the exact old path.
func (it *SortIter) appendSortKey(dst []byte, doc *anyenc.Value, raw []byte) ([]byte, error) {
	if doc == nil {
		if raw != nil && it.rawSorter != nil {
			if k, handled := it.rawSorter.AppendKeyRaw(dst, raw, it.Buf); handled {
				it.rawFallbacks = 0
				return k, nil
			}
			it.rawFallbacks++
			if it.rawFallbacks >= sortRawFallbackMax {
				it.rawSorter = nil
			}
		}
		src := it.Buf.DocBuf
		if raw != nil {
			src = raw
		}
		var perr error
		if doc, perr = it.Buf.Parser.ParseOwned(src); perr != nil {
			return dst, perr
		}
	}
	return it.Sorter.AppendKey(dst, doc), nil
}

func (it *SortIter) collectAndSort() error {
	cmpEntry := func(a, b sortEntry) int {
		ak := it.arena[a.off : a.off+uint32(a.keyLen)]
		bk := it.arena[b.off : b.off+uint32(b.keyLen)]
		return bytes.Compare(ak, bk)
	}

	if rs, isRaw := it.Sorter.(query.RawSort); isRaw {
		it.rawSorter = rs
	}

	for {
		_, docId, mk, err := it.Source.Next()
		if err != nil {
			return err
		}
		if docId == nil {
			break
		}

		// Prefer already-parsed doc from upstream (FullScanIter/FetchIter/FilterIter)
		doc := it.Plan.DocParsed
		var raw []byte
		if doc == nil {
			// Cursor-free point lookup: avoids Cursor allocation
			var verr error
			it.Buf.DocBuf, verr = it.Data.AppendValue(docId, it.Buf.DocBuf[:0])
			if verr != nil {
				continue
			}
			if it.rawSorter != nil {
				if d, derr := it.Buf.Parser.DecodedDoc(it.Buf.DocBuf); derr == nil {
					raw = d
				}
			}
		}

		var mkByte uint8
		if mk {
			mkByte = 1
		}

		if it.TopK <= 0 {
			// Full sort: materialize every row (behavior unchanged).
			it.growArena(256)
			off := uint32(len(it.arena))
			if it.arena, err = it.appendSortKey(it.arena, doc, raw); err != nil {
				return err
			}
			it.arena = append(it.arena, docId...)
			keyLen := uint16(len(it.arena) - int(off))
			it.entries = append(it.entries, sortEntry{
				off: off, keyLen: keyLen, docLen: uint16(len(docId)), multiKey: mkByte,
			})
			continue
		}

		// Max-heap of size TopK: keep only the smallest K entries. Build the
		// candidate's packed key + docId at the arena TAIL (in spare capacity)
		// WITHOUT committing it, so we can decide membership before deciding to
		// keep the bytes. A "loser" row truncates the tail back and costs no
		// retained memory and no allocation — mirroring SQLite's pushOntoSorter,
		// which only inserts a record into the sorter AFTER it survives the LIMIT
		// eviction (select.c), so the sorter holds <= LIMIT+OFFSET records.
		base := uint32(len(it.arena))
		it.growArena(256)
		if it.arena, err = it.appendSortKey(it.arena, doc, raw); err != nil {
			return err
		}
		it.arena = append(it.arena, docId...)
		candLen := uint16(len(it.arena) - int(base))
		candKey := it.arena[base:] // view of the candidate at the tail

		switch {
		case len(it.entries) < it.TopK:
			// Heap not yet full: admit unconditionally, keeping the tail bytes.
			it.entries = append(it.entries, sortEntry{
				off: base, keyLen: candLen, docLen: uint16(len(docId)), multiKey: mkByte,
			})
			it.liveBytes += int(candLen)
			it.heapUp(len(it.entries) - 1)
		case bytes.Compare(candKey, it.arena[it.entries[0].off:it.entries[0].off+uint32(it.entries[0].keyLen)]) < 0:
			// Candidate is smaller than the current heap max (the root): the root
			// loses. If the root's vacated slot is exactly the candidate's size,
			// overwrite it in place and drop the tail copy (reuse, no growth);
			// the slot cannot overlap any live entry, so surviving entries stay
			// byte-exact. Otherwise keep the candidate at the tail and let the
			// root's bytes become dead, reclaimed by the next compaction. We
			// deliberately do NOT memmove-compact the arena on every eviction —
			// that would be O(N*K) copy churn.
			root := it.entries[0]
			it.liveBytes += int(candLen) - int(root.keyLen)
			off := base
			if root.keyLen == candLen {
				copy(it.arena[root.off:root.off+uint32(candLen)], candKey)
				it.arena = it.arena[:base] // uncommit tail; reuse root's slot
				off = root.off
			}
			it.entries[0] = sortEntry{
				off: off, keyLen: candLen, docLen: uint16(len(docId)), multiKey: mkByte,
			}
			it.heapDown(0)
			it.maybeCompact()
		default:
			// Loser: never materialized — discard the tail bytes.
			it.arena = it.arena[:base]
		}
	}

	slices.SortFunc(it.entries, cmpEntry)
	return nil
}

// maybeCompact rebuilds the arena from the live heap entries when fragmentation
// (freed-but-unreused bytes) grows beyond live data, guaranteeing the arena
// high-water mark stays O(K * keylen) even for adversarial key-length streams
// (e.g. strictly decreasing keys of all-distinct lengths, where exact-size
// reuse never hits). Compaction is O(K log K) and can only run after at least
// liveBytes worth of appends have accumulated, so the amortized cost is O(1)
// per row; it does NOT run per eviction.
//
// In-place safety: we visit live entries in ascending arena-offset order and
// pack them toward offset 0. For the i-th entry in that order, the destination
// dst (the sum of the lengths of all earlier entries) satisfies dst <= off,
// because those earlier entries occupy disjoint regions that all lie below off.
// Thus each copy moves bytes to a position at or before their source, and
// destinations are strictly increasing and disjoint — so no entry's source is
// clobbered before it is copied. it.entries stays in heap order; only each
// entry's .off field is rewritten, so the heap invariant is preserved.
func (it *SortIter) maybeCompact() {
	const minWaste = 64 << 10 // don't bother compacting tiny arenas
	if len(it.arena) <= it.liveBytes*2 || len(it.arena)-it.liveBytes < minWaste {
		return
	}
	it.order = it.order[:0]
	for i := range it.entries {
		it.order = append(it.order, i)
	}
	slices.SortFunc(it.order, func(a, b int) int {
		return cmp.Compare(it.entries[a].off, it.entries[b].off)
	})
	dst := uint32(0)
	for _, i := range it.order {
		e := &it.entries[i]
		copy(it.arena[dst:dst+uint32(e.keyLen)], it.arena[e.off:e.off+uint32(e.keyLen)])
		e.off = dst
		dst += uint32(e.keyLen)
	}
	it.arena = it.arena[:dst]
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
