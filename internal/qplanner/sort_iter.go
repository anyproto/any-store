package qplanner

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"math"
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
// tail capacity and kept ONLY when the row enters/stays in the top-K heap, as
// SQLite's pushOntoSorter decides before it inserts a record: "inserted into
// the sorter if either (a) there are currently less than LIMIT+OFFSET items or
// (b) the new record is smaller than the largest record currently in the
// sorter", and otherwise the row is skipped (select.c:833-846). A losing row
// simply truncates the tail back — zero retained bytes, zero allocation. When a
// smaller row evicts the heap root and the root's vacated slot is exactly the
// new row's size, the row overwrites that slot in place (reuse, no growth);
// otherwise the row stays at the tail and the root's bytes become dead, later
// reclaimed by a compaction guard that rebuilds the arena from the live entries
// when fragmentation grows large. This keeps the arena's high-water mark O(K)
// without any per-eviction memmove. With TopK <= 0 (full sort) every row is
// materialized.
//
// The entries address the arena with 32-bit offsets, so what the sort keeps
// must fit sortArenaLimit bytes; the limit applies to a row when it is kept,
// after slot reuse and compaction, never to a candidate that loses. A sort
// whose kept rows pass it returns errSortTooLarge.
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
	liveBytes       int
	order           []int // reusable scratch: live-entry indices sorted by arena offset (compaction)
	PartiallySorted bool  // leading index fields match sort order; pdqsort benefits automatically
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

// sortEntry locates one row's packed sort key + docId in the arena. The
// lengths are 32-bit: a sort key is the whole marshaled field value (a long
// string, or an object or array sorted whole), so it has no bound below the
// arena's own, and a narrower length would wrap and slice the docId out of
// the wrong bytes. The struct stays 12 bytes — the entries are what the sort
// swaps — by keeping the multiKey flag in the top bit of the docId length.
type sortEntry struct {
	off    uint32 // offset into arena
	keyLen uint32 // total length (sort key + docId suffix)
	// docMK is the docId length (the trailing portion of the key) in the low
	// 31 bits and the multiKey flag in the top bit: set = upstream marked this
	// entry multiKey; clear = unique. The flag is forwarded as-is on emit so
	// consumer-side DocDedup can skip the seen-set for unique streams.
	docMK uint32
}

const sortEntryMultiKey = 1 << 31

func (e sortEntry) docLen() uint32 { return e.docMK &^ sortEntryMultiKey }
func (e sortEntry) multiKey() bool { return e.docMK&sortEntryMultiKey != 0 }

// errSortTooLarge is returned when the packed sort keys of the rows one sort
// keeps do not fit the 32-bit offsets of its entries.
var errSortTooLarge = errors.New("any-store: the sort keys of one in-memory sort exceed 4 GiB")

// sortArenaLimit is the most arena bytes a sort keeps: the end of every kept
// entry must be a uint32. A variable so tests can reach it without
// allocating 4 GiB. On a 32-bit platform memory runs out before a slice
// reaches it.
var sortArenaLimit uint64 = math.MaxUint32

// fitsSortArena reports whether an arena of n bytes can be addressed by the
// entries.
func fitsSortArena(n int) bool { return uint64(n) <= sortArenaLimit }

func (it *SortIter) Next() (key []byte, docId []byte, multiKey bool, err error) {
	if !it.inited {
		it.inited = true
		if err := it.collectAndSort(); err != nil {
			it.release()
			return nil, nil, false, err
		}
	}

	if it.idx >= len(it.entries) {
		return nil, nil, false, nil
	}

	e := it.entries[it.idx]
	it.idx++
	docId = it.arena[e.off+e.keyLen-e.docLen() : e.off+e.keyLen]

	// Clear DocParsed so planIterator.Doc() does a lazy fetch by docId.
	// collectAndSort() leaves DocParsed pointing at the last collected doc,
	// which is NOT the doc for this sorted entry. DocRaw likewise.
	if it.Plan != nil {
		it.Plan.DocParsed = nil
		it.Plan.DocRaw = nil
	}

	return docId, docId, e.multiKey(), nil
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
		ak := it.arena[a.off : a.off+a.keyLen]
		bk := it.arena[b.off : b.off+b.keyLen]
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
			// Decoded bytes handed over by a RawForSort FullScanIter: the
			// scan cursor already read the row, so no point lookup is needed.
			if it.Plan.DocRaw != nil {
				raw = it.Plan.DocRaw
				it.Plan.DocRaw = nil
			} else {
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
		}

		// docId length + multiKey flag, as sortEntry.docMK packs them. A docId
		// is a b-tree key and far below 2 GiB, so the flag bit is free.
		docMK := uint32(len(docId))
		if mk {
			docMK |= sortEntryMultiKey
		}

		if it.TopK <= 0 {
			// Full sort: materialize every row.
			it.growArena(256)
			off := len(it.arena)
			if it.arena, err = it.appendSortKey(it.arena, doc, raw); err != nil {
				return err
			}
			it.arena = append(it.arena, docId...)
			if !fitsSortArena(len(it.arena)) {
				return errSortTooLarge
			}
			keyLen := uint32(len(it.arena) - off)
			it.entries = append(it.entries, sortEntry{off: uint32(off), keyLen: keyLen, docMK: docMK})
			continue
		}

		// Max-heap of size TopK: keep only the smallest K entries. Build the
		// candidate's packed key + docId at the arena TAIL (in spare capacity)
		// WITHOUT committing it, so we can decide membership before deciding to
		// keep the bytes. A "loser" row truncates the tail back and keeps no
		// entry, though a loser longer than the spare capacity grows the
		// arena's backing array, and that capacity stays until the arena is
		// released. SQLite's pushOntoSorter also keeps no record of a row that
		// would not stay among LIMIT+OFFSET records (select.c:833-846). When
		// the loop's order satisfies no prefix of the ORDER BY (nOBSat==0), it
		// compares the row's key registers with the largest kept entry before
		// it builds a record at all (OP_IdxLE at select.c:852, makeSorterRecord
		// at :857-858); when it satisfies a prefix, the record is built first
		// (:789-797). The candidate's offset and length are native ints until
		// it is kept: a loser past sortArenaLimit is no error.
		base := len(it.arena)
		it.growArena(256)
		if it.arena, err = it.appendSortKey(it.arena, doc, raw); err != nil {
			return err
		}
		it.arena = append(it.arena, docId...)
		candLen := len(it.arena) - base
		candKey := it.arena[base:] // view of the candidate at the tail

		switch {
		case len(it.entries) < it.TopK:
			// Heap not yet full: admit unconditionally, keeping the tail bytes.
			// Nothing has been evicted yet, so the arena holds only live
			// entries: past the limit, the kept rows do not fit.
			if !fitsSortArena(len(it.arena)) {
				return errSortTooLarge
			}
			it.entries = append(it.entries, sortEntry{off: uint32(base), keyLen: uint32(candLen), docMK: docMK})
			it.liveBytes += candLen
			it.heapUp(len(it.entries) - 1)
		case bytes.Compare(candKey, it.arena[it.entries[0].off:it.entries[0].off+it.entries[0].keyLen]) < 0:
			// Candidate is smaller than the current heap max (the root): the root
			// loses. If the root's vacated slot is exactly the candidate's size,
			// overwrite it in place and drop the tail copy (reuse, no growth);
			// the slot cannot overlap any live entry, so surviving entries stay
			// byte-exact. Otherwise keep the candidate at the tail and let the
			// root's bytes become dead, reclaimed by the next compaction. We
			// deliberately do NOT memmove-compact the arena on every eviction —
			// that would be O(N*K) copy churn.
			root := it.entries[0]
			it.liveBytes += candLen - int(root.keyLen)
			var off int
			switch {
			case int(root.keyLen) == candLen:
				copy(it.arena[root.off:int(root.off)+candLen], candKey)
				it.arena = it.arena[:base] // uncommit tail; reuse root's slot
				off = int(root.off)
			case fitsSortArena(len(it.arena)):
				off = base
			default:
				// The tail is past the limit: pack the live entries, the
				// root's bytes now dead, and move the candidate after them.
				if off, err = it.admitAfterCompact(base, candLen); err != nil {
					return err
				}
			}
			it.entries[0] = sortEntry{off: uint32(off), keyLen: uint32(candLen), docMK: docMK}
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
	if sortNeedsCompact(len(it.arena), it.liveBytes) {
		it.compact()
	}
}

// sortNeedsCompact reports whether an arena of arenaLen bytes holding
// liveBytes of live entries has more dead bytes than live ones, and enough of
// them to be worth a pass. Written without liveBytes*2, which overflows a
// 32-bit int from 1 GiB of live keys on.
func sortNeedsCompact(arenaLen, liveBytes int) bool {
	const minWaste = 64 << 10 // don't bother compacting tiny arenas
	waste := arenaLen - liveBytes
	return waste > liveBytes && waste >= minWaste
}

// compact packs the live entries toward offset 0 (see maybeCompact) and
// returns the packed length; the arena is cut to it.
func (it *SortIter) compact() int {
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
		copy(it.arena[dst:dst+e.keyLen], it.arena[e.off:e.off+e.keyLen])
		e.off = dst
		dst += e.keyLen
	}
	it.arena = it.arena[:dst]
	return int(dst)
}

// admitAfterCompact keeps the candidate built at arena[base:base+candLen]
// when the arena's tail is past sortArenaLimit: it packs the live entries,
// with the evicted root (entries[0], about to be overwritten) counted as
// dead, and moves the candidate after them. Every live entry lies below base,
// so packing never reaches the candidate's bytes. Returns the candidate's
// new offset, or errSortTooLarge when the kept rows do not fit even packed.
func (it *SortIter) admitAfterCompact(base, candLen int) (int, error) {
	cand := it.arena[base : base+candLen]
	it.entries[0].keyLen = 0 // the evicted root's bytes are dead
	dst := it.compact()
	if !fitsSortArena(dst + candLen) {
		return 0, errSortTooLarge
	}
	it.arena = append(it.arena, cand...) // within capacity; copy handles the overlap
	return dst, nil
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
	ak := it.arena[a.off : a.off+a.keyLen]
	bk := it.arena[b.off : b.off+b.keyLen]
	return bytes.Compare(ak, bk) < 0
}

// release drops the arena and the entries: after an error, or once the
// iterator is closed, nothing reads them, and a caller that keeps the closed
// iterator must not keep a sort's memory alive with it.
func (it *SortIter) release() {
	it.arena, it.entries, it.order = nil, nil, nil
	it.liveBytes, it.idx = 0, 0
}

// Close closes the source iterator and releases the sort's memory.
func (it *SortIter) Close() {
	if it.Source != nil {
		it.Source.Close()
	}
	it.release()
}

func (it *SortIter) String() string {
	if it.TopK > 0 {
		return fmt.Sprintf("%s -> TopK(%d)", it.Source, it.TopK)
	}
	return fmt.Sprintf("%s -> Sort", it.Source)
}
