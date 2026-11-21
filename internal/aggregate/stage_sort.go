package aggregate

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strconv"

	"github.com/anyproto/any-store/v2/anyenc"
)

// SortStage is the blocking in-pipeline $sort over synthesized documents
// (stored-document sorts are pushed down to the access plan instead).
//
// It mirrors qplanner.SortIter's packed-arena design, but each entry carries
// the full marshaled document after its sort-key tuple — rows here no longer
// correspond to stored docIds. With TopK > 0 (a following $skip/$limit was
// folded in by Build) a max-heap keeps only the smallest K entries: a losing
// row is built in the arena's spare tail and truncated away — zero retained
// bytes — and a fragmentation guard re-packs the arena when dead bytes from
// evictions exceed live data, keeping the high-water mark O(K).
//
// Stability: the full sort uses slices.SortStableFunc over insertion-ordered
// entries; the top-K path appends a row sequence number to each key, which
// both totals the order (deterministic heap eviction: of equal keys the
// earliest rows survive) and makes the final sort stable by construction.
//
// The entries address the arena with 32-bit offsets, so the rows the stage
// keeps must fit sortArenaLimit bytes. As in qplanner.SortIter, the limit
// applies to a row when it is kept, after slot reuse and compaction, never to
// a top-K candidate that loses; rows past it are errSortTooLarge. MemoryLimit
// bounds the arena first unless it is disabled. Close releases the arena.
type SortStage struct {
	Src  Stage
	Spec SortSpec
	TopK int

	arena     []byte
	entries   []sortEntry
	liveBytes int
	order     []int // compaction scratch
	seq       uint32

	built bool
	p     anyenc.Parser
	idx   int
}

type sortEntry struct {
	off    uint32
	keyLen uint32 // sort-key tuple (+ seq suffix in TopK mode)
	docLen uint32 // marshaled document length (follows the key)
}

const sortEntryOverhead = 12 // approximate per-entry slab cost for MemAccount

// errSortTooLarge is returned when the rows one $sort keeps do not fit the
// 32-bit offsets of its entries.
var errSortTooLarge = errors.New("any-store: the rows of one in-pipeline $sort exceed 4 GiB")

// sortArenaLimit is the most arena bytes a $sort keeps: the end of every kept
// entry must be a uint32. A variable so tests can reach it without
// allocating 4 GiB.
var sortArenaLimit uint64 = math.MaxUint32

// fitsSortArena reports whether an arena of n bytes can be addressed by the
// entries.
func fitsSortArena(n int) bool { return uint64(n) <= sortArenaLimit }

func (s *SortStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if !s.built {
		// An error leaves the collected rows in place: a $facet feed pauses
		// the chain with errFacetPause after every row, and the next Next
		// resumes collecting where it stopped. Close releases them.
		if err := s.collectAndSort(ctx); err != nil {
			return nil, err
		}
		s.built = true
	}
	if s.idx >= len(s.entries) {
		return nil, nil
	}
	// Row owner on the output side: one reset per emitted row; the emitted
	// document itself is owned by the stage parser (invalidated next emit).
	ctx.RowArena.Reset()
	e := s.entries[s.idx]
	s.idx++
	start := e.off + e.keyLen
	return s.p.Parse(s.arena[start : start+e.docLen])
}

func (s *SortStage) collectAndSort(ctx *Ctx) error {
	cmpEntry := func(a, b sortEntry) int {
		return bytes.Compare(
			s.arena[a.off:a.off+a.keyLen],
			s.arena[b.off:b.off+b.keyLen],
		)
	}

	for rows := 1; ; rows++ {
		v, err := s.Src.Next(ctx)
		if err != nil {
			return err
		}
		if v == nil {
			break
		}
		if rows%ctxCheckEvery == 0 {
			if cerr := ctx.Context.Err(); cerr != nil {
				return cerr
			}
		}

		// Build the candidate (key tuple [+seq] + marshaled doc) in the
		// arena's spare tail without committing it. Its offset and lengths
		// are native ints until it is kept: a loser past sortArenaLimit is no
		// error.
		prevLen := len(s.arena)
		base := prevLen
		s.growArena(256)
		s.arena = s.Spec.Sort.AppendKey(s.arena, v)
		if s.TopK > 0 {
			s.arena = binary.BigEndian.AppendUint32(s.arena, s.seq)
			s.seq++
		}
		keyLen := len(s.arena) - base
		s.arena = v.MarshalTo(s.arena)
		total := len(s.arena) - base
		candBytes := s.arena[base:]

		switch {
		case s.TopK <= 0 || len(s.entries) < s.TopK:
			// Full sort, or heap not yet full: admit unconditionally. Nothing
			// has been evicted, so the arena holds only kept rows: past the
			// limit, they do not fit.
			if !fitsSortArena(len(s.arena)) {
				return sortTooLarge(ctx, len(s.arena)-prevLen)
			}
			s.entries = append(s.entries, sortEntry{off: uint32(base), keyLen: uint32(keyLen), docLen: uint32(total - keyLen)})
			s.liveBytes += total
			if s.TopK > 0 {
				s.heapUp(len(s.entries) - 1)
			}
		case s.heapKeyLess(candBytes[:keyLen], 0):
			// Candidate beats the heap max (root): the root loses. Reuse its
			// slot in place when sizes match exactly; otherwise its bytes go
			// dead until the compaction guard runs. A kept entry ends within
			// sortArenaLimit, so its two lengths add without wrapping.
			root := s.entries[0]
			rootTotal := int(root.keyLen + root.docLen)
			s.liveBytes += total - rootTotal
			var off int
			switch {
			case rootTotal == total:
				copy(s.arena[root.off:int(root.off)+total], candBytes)
				s.arena = s.arena[:base]
				off = int(root.off)
			case fitsSortArena(len(s.arena)):
				off = base
			default:
				// The tail is past the limit: pack the live entries, the
				// root's bytes now dead, and move the candidate after them.
				if off, err = s.admitAfterCompact(base, total); err != nil {
					return sortTooLarge(ctx, len(s.arena)+total-prevLen)
				}
			}
			s.entries[0] = sortEntry{off: uint32(off), keyLen: uint32(keyLen), docLen: uint32(total - keyLen)}
			s.heapDown(0)
			s.maybeCompact()
		default:
			// Loser: never materialized.
			s.arena = s.arena[:base]
		}

		if err = ctx.Mem.Add(len(s.arena) - prevLen + sortEntryOverhead); err != nil {
			return err
		}
	}

	if s.TopK > 0 {
		// Keys carry the seq suffix: total order, no ties — a plain sort is
		// already stable with respect to the input.
		slices.SortFunc(s.entries, cmpEntry)
	} else {
		slices.SortStableFunc(s.entries, cmpEntry)
	}
	return nil
}

// sortTooLarge is the error for a row that does not fit the arena once kept,
// growing it by grown bytes. The row is charged first, as if kept, so a row
// that also crosses MemoryLimit reports ErrMemoryLimitExceeded, the error
// callers match, as it would without the arena limit.
func sortTooLarge(ctx *Ctx, grown int) error {
	if err := ctx.Mem.Add(grown + sortEntryOverhead); err != nil {
		return err
	}
	return errSortTooLarge
}

// growArena ensures spare capacity, growing in tiered steps (SortIter's policy).
func (s *SortStage) growArena(need int) {
	if cap(s.arena)-len(s.arena) >= need {
		return
	}
	grow := 100 << 10
	switch c := cap(s.arena); {
	case c < 1<<10:
		grow = 1 << 10
	case c < 10<<10:
		grow = 10 << 10
	}
	if grow < need {
		grow = need
	}
	s.arena = slices.Grow(s.arena, grow)
}

// maybeCompact re-packs live entries toward offset 0 when dead bytes exceed
// live data (see qplanner.SortIter.maybeCompact for the in-place safety
// argument: sources are visited in ascending offset order, destinations are
// disjoint and never ahead of sources).
func (s *SortStage) maybeCompact() {
	if sortNeedsCompact(len(s.arena), s.liveBytes) {
		s.compact()
	}
}

// sortNeedsCompact reports whether an arena of arenaLen bytes holding
// liveBytes of live entries has more dead bytes than live ones, and enough of
// them to be worth a pass. Written without liveBytes*2, which overflows a
// 32-bit int from 1 GiB of live rows on.
func sortNeedsCompact(arenaLen, liveBytes int) bool {
	const minWaste = 64 << 10
	waste := arenaLen - liveBytes
	return waste > liveBytes && waste >= minWaste
}

// compact packs the live entries toward offset 0 and returns the packed
// length; the arena is cut to it.
func (s *SortStage) compact() int {
	s.order = s.order[:0]
	for i := range s.entries {
		s.order = append(s.order, i)
	}
	slices.SortFunc(s.order, func(a, b int) int {
		return cmp.Compare(s.entries[a].off, s.entries[b].off)
	})
	dst := uint32(0)
	for _, i := range s.order {
		e := &s.entries[i]
		total := e.keyLen + e.docLen
		copy(s.arena[dst:dst+total], s.arena[e.off:e.off+total])
		e.off = dst
		dst += total
	}
	s.arena = s.arena[:dst]
	return int(dst)
}

// admitAfterCompact keeps the candidate built at arena[base:base+total] when
// the arena's tail is past sortArenaLimit: it packs the live entries, with
// the evicted root (entries[0], about to be overwritten) counted as dead, and
// moves the candidate after them. Every live entry lies below base, so
// packing never reaches the candidate's bytes. Returns the candidate's new
// offset, or errSortTooLarge when the kept rows do not fit even packed.
func (s *SortStage) admitAfterCompact(base, total int) (int, error) {
	cand := s.arena[base : base+total]
	s.entries[0].keyLen, s.entries[0].docLen = 0, 0 // the evicted root's bytes are dead
	dst := s.compact()
	if !fitsSortArena(dst + total) {
		return 0, errSortTooLarge
	}
	s.arena = append(s.arena, cand...) // within capacity; copy handles the overlap
	return dst, nil
}

// heapKeyLess reports whether key sorts before entries[i]'s key (max-heap of
// the smallest TopK entries).
func (s *SortStage) heapKeyLess(key []byte, i int) bool {
	e := s.entries[i]
	return bytes.Compare(key, s.arena[e.off:e.off+e.keyLen]) < 0
}

func (s *SortStage) heapLess(i, j int) bool {
	a := s.entries[i]
	return s.heapKeyLess(s.arena[a.off:a.off+a.keyLen], j)
}

func (s *SortStage) heapUp(i int) {
	for i > 0 {
		p := (i - 1) / 2
		if s.heapLess(p, i) {
			s.entries[p], s.entries[i] = s.entries[i], s.entries[p]
			i = p
		} else {
			break
		}
	}
}

func (s *SortStage) heapDown(i int) {
	n := len(s.entries)
	for {
		largest := i
		l, r := 2*i+1, 2*i+2
		if l < n && s.heapLess(largest, l) {
			largest = l
		}
		if r < n && s.heapLess(largest, r) {
			largest = r
		}
		if largest == i {
			break
		}
		s.entries[i], s.entries[largest] = s.entries[largest], s.entries[i]
		i = largest
	}
}

// release drops the arena and the entries once the stage is closed: nothing
// reads them after that, and a caller that keeps the closed pipeline must not
// keep a sort's memory alive with it.
func (s *SortStage) release() {
	s.arena, s.entries, s.order = nil, nil, nil
	s.liveBytes, s.idx = 0, 0
}

func (s *SortStage) Close() {
	s.Src.Close()
	s.release()
}

func (s *SortStage) String() string {
	str := SortSpec{Fields: s.Spec.Fields}.String()
	if s.TopK > 0 {
		return str + " (topK " + strconv.Itoa(s.TopK) + ")"
	}
	return str
}
