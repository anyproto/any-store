package aggregate

import (
	"bytes"
	"cmp"
	"encoding/binary"
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
type SortStage struct {
	Src  Stage
	Spec SortSpec
	TopK int

	arena     []byte
	entries   []sortEntry
	liveBytes int
	order     []int // compaction scratch
	seq       uint32

	p     anyenc.Parser
	built bool
	idx   int
}

type sortEntry struct {
	off    uint32
	keyLen uint32 // sort-key tuple (+ seq suffix in TopK mode)
	docLen uint32 // marshaled document length (follows the key)
}

const sortEntryOverhead = 12 // approximate per-entry slab cost for MemAccount

func (s *SortStage) Next(ctx *Ctx) (*anyenc.Value, error) {
	if !s.built {
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
		// arena's spare tail without committing it.
		prevLen := len(s.arena)
		base := uint32(prevLen)
		s.growArena(256)
		s.arena = s.Spec.Sort.AppendKey(s.arena, v)
		if s.TopK > 0 {
			s.arena = binary.BigEndian.AppendUint32(s.arena, s.seq)
			s.seq++
		}
		keyLen := uint32(len(s.arena)) - base
		s.arena = v.MarshalTo(s.arena)
		docLen := uint32(len(s.arena)) - base - keyLen
		total := keyLen + docLen
		cand := sortEntry{off: base, keyLen: keyLen, docLen: docLen}
		candBytes := s.arena[base:]

		switch {
		case s.TopK <= 0 || len(s.entries) < s.TopK:
			// Full sort, or heap not yet full: admit unconditionally.
			s.entries = append(s.entries, cand)
			s.liveBytes += int(total)
			if s.TopK > 0 {
				s.heapUp(len(s.entries) - 1)
			}
		case s.heapKeyLess(candBytes[:keyLen], 0):
			// Candidate beats the heap max (root): the root loses. Reuse its
			// slot in place when sizes match exactly; otherwise its bytes go
			// dead until the compaction guard runs.
			root := s.entries[0]
			rootTotal := root.keyLen + root.docLen
			s.liveBytes += int(total) - int(rootTotal)
			if rootTotal == total {
				copy(s.arena[root.off:root.off+total], candBytes)
				s.arena = s.arena[:base]
				cand.off = root.off
			}
			s.entries[0] = cand
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
	const minWaste = 64 << 10
	if len(s.arena) <= s.liveBytes*2 || len(s.arena)-s.liveBytes < minWaste {
		return
	}
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

func (s *SortStage) Close() { s.Src.Close() }

func (s *SortStage) String() string {
	str := SortSpec{Fields: s.Spec.Fields}.String()
	if s.TopK > 0 {
		return str + " (topK " + strconv.Itoa(s.TopK) + ")"
	}
	return str
}
