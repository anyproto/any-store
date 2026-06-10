package vindex

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// buildSem bounds the total number of concurrent index-build / compaction workers
// across the whole process. Every parallel build draws workers from one shared
// budget, so many simultaneous builds cannot oversubscribe the CPU regardless of
// how many indexes build at once. Default budget is GOMAXPROCS.
type buildSem struct{ ch chan struct{} }

var curBuildSem atomic.Pointer[buildSem]

func init() {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	curBuildSem.Store(&buildSem{ch: make(chan struct{}, n)})
}

// SetBuildConcurrency sets the process-wide budget of concurrent build/compaction
// workers. n < 1 is treated as 1. In-flight builds keep the budget they captured.
func SetBuildConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	curBuildSem.Store(&buildSem{ch: make(chan struct{}, n)})
}

// BulkBuildParallel is BulkBuild with a concurrent in-RAM graph construction
// phase, then the same single-threaded bulk flush. This is the technique hnswlib
// and Lance use: many workers insert into one shared in-RAM graph guarded by
// per-node locks. It trades the byte-identical determinism of BulkBuild for
// near-linear build speedup (the graph-construction CPU, not the btree writes, is
// the dominant build cost). The resulting graph is a valid HNSW of equivalent
// recall, not byte-identical (it depends on insertion interleaving).
//
// threads <= 0 uses GOMAXPROCS. Levels are drawn sequentially from the index RNG
// (deterministic), so only the connection order is nondeterministic.
func BulkBuildParallel(wtx *btree.WriteTx, prefix string, p Params, seed int64, ids [][]byte, vecs [][]float32, threads int) (*Index, error) {
	if p.Dim <= 0 {
		return nil, fmt.Errorf("vindex: dim must be > 0")
	}
	if len(ids) != len(vecs) {
		return nil, fmt.Errorf("vindex: ids/vecs length mismatch: %d != %d", len(ids), len(vecs))
	}
	p.withDefaults()
	names := nsNames(prefix)
	var ns [5]*btree.Namespace
	for i, name := range names {
		n, err := ensureNS(wtx, name)
		if err != nil {
			return nil, err
		}
		ns[i] = n
	}
	ix := newIndex(ns, p.Dim, p.M, 2*p.M, p.EfConstruction, p.EfSearch, p.Ml, p.Metric, p.Quantization, seed)
	n := len(vecs)
	for i, v := range vecs {
		if len(v) != ix.dim {
			return nil, fmt.Errorf("vindex: vec %d dim mismatch: got %d want %d", i, len(v), ix.dim)
		}
	}

	pb := newPBuilder(ix, n)
	// Prepare vectors (full query + candidate representation) — single-threaded so
	// the level RNG draw order is deterministic and allocations are race-free.
	for i := 0; i < n; i++ {
		full := vecs[i]
		if ix.normalize {
			nv := make([]float32, ix.dim)
			full = normalizeInto(nv, vecs[i])
		} else {
			full = append([]float32(nil), vecs[i]...)
		}
		pb.full[i] = full
		lvl := ix.randomLevel()
		pb.levels[i] = lvl
		pb.links[i] = make([]uint32, pb.slotsFor(lvl))
		pb.cnt[i] = make([]uint16, lvl+1)
	}

	if n > 0 {
		pb.entry = 0
		pb.top = pb.levels[0]
		pb.build(threads)
		pb.repair(threads) // recover the recall the concurrent build cost
	}

	// Bulk flush (single-threaded), label order.
	mt := &meta{
		dim: ix.dim, metric: p.Metric, m: ix.m, m0: ix.m0,
		efC: ix.efC, efS: ix.efS, ml: ix.ml, quant: ix.quant,
		count: int64(n), nextLabel: uint32(n), deletedCount: 0,
		hasEntry: n > 0, entryLabel: pb.entry, topLayer: pb.top,
		l0Gen: uint64(n),
	}
	var kbuf, vbuf []byte
	var lb [4]byte
	nbrs := make([][]uint32, 0, 8)
	for label := 0; label < n; label++ {
		kbuf = labelKey(kbuf, uint32(label))
		if ix.quant == QuantNone {
			if err := wtx.Put(ix.vvec, kbuf, f32bytes(pb.full[label])); err != nil {
				return nil, err
			}
		} else {
			vbuf = encodeVec(vbuf[:0], pb.full[label], ix.quant)
			if err := wtx.Put(ix.vvec, kbuf, vbuf); err != nil {
				return nil, err
			}
		}
		if err := wtx.Put(ix.vlbl, kbuf, ids[label]); err != nil {
			return nil, err
		}
		lvl := pb.levels[label]
		nbrs = nbrs[:0]
		for lc := int32(0); lc <= lvl; lc++ {
			o := pb.off(lc)
			c := int(pb.cnt[label][lc])
			nbrs = append(nbrs, pb.links[label][o:o+c])
		}
		vbuf = encodeAdj(vbuf[:0], lvl, false, nbrs)
		if err := wtx.Put(ix.vadj, kbuf, vbuf); err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(lb[:], uint32(label))
		if err := wtx.Put(ix.vdoc, ids[label], lb[:]); err != nil {
			return nil, err
		}
	}
	if err := ix.writeMeta(wtx, mt); err != nil {
		return nil, err
	}
	return ix, nil
}

// pBuilder is the concurrent in-RAM HNSW. Adjacency is stored in per-node
// fixed-size slices (allocated once, never reassigned), each guarded by a per-node
// RWMutex so neighbour reads (traversal) and writes (connect) are race-free.
type pBuilder struct {
	ix     *Index
	m, m0  int
	full   [][]float32
	levels []int32

	links [][]uint32 // links[i] fixed; layer lc occupies [off(lc), off(lc)+m_lc)
	cnt   [][]uint16 // cnt[i][lc] = neighbour count at layer lc
	locks []sync.RWMutex

	repairEf int

	entryMu sync.Mutex
	entry   uint32
	top     int32
}

// repairEfTune, when >0, overrides the repair-pass ef (test/benchmark knob).
var repairEfTune int

func newPBuilder(ix *Index, n int) *pBuilder {
	return &pBuilder{
		ix: ix, m: ix.m, m0: ix.m0,
		full: make([][]float32, n),
		levels: make([]int32, n), links: make([][]uint32, n),
		cnt: make([][]uint16, n), locks: make([]sync.RWMutex, n),
		repairEf: repairEf(ix),
	}
}

// repairEf chooses the ef for the repair pass. The repair re-searches the COMPLETE
// graph, so a narrower beam than efConstruction still finds the few neighbours the
// concurrent build missed — at a fraction of the cost, which matters most on
// low-core machines where the repair caps the speedup.
func repairEf(ix *Index) int {
	if repairEfTune > 0 {
		return repairEfTune
	}
	// A beam just wide enough to reselect the layer-0 degree (m0). Measured: recall
	// is flat from here up to efConstruction, but build time isn't — so the narrow
	// beam keeps recall parity at a fraction of the repair cost (best low-core scaling).
	ef := ix.m0
	if ef < 32 {
		ef = 32
	}
	return ef
}

func (b *pBuilder) slotsFor(level int32) int { return b.m0 + int(level)*b.m }

// off returns the slot offset of layer lc within a node's links slice.
func (b *pBuilder) off(lc int32) int {
	if lc == 0 {
		return 0
	}
	return b.m0 + int(lc-1)*b.m
}

func (b *pBuilder) capAt(lc int32) int {
	if lc == 0 {
		return b.m0
	}
	return b.m
}

// neighborsInto copies node's layer-lc neighbours into dst under a read lock.
func (b *pBuilder) neighborsInto(node uint32, lc int32, dst []uint32) []uint32 {
	b.locks[node].RLock()
	if lc <= b.levels[node] {
		o := b.off(lc)
		c := int(b.cnt[node][lc])
		dst = append(dst[:0], b.links[node][o:o+c]...)
	} else {
		dst = dst[:0]
	}
	b.locks[node].RUnlock()
	return dst
}

// seedFraction of the nodes are inserted sequentially before going parallel, so
// the upper-layer routing structure and a dense base graph already exist when the
// workers start. Inserting into a near-empty graph concurrently otherwise makes
// poor neighbour choices on clustered data and measurably lowers recall. A floor
// keeps small builds well-seeded.
const (
	seedFraction = 16   // ~1/16 of nodes seeded sequentially
	seedFloor    = 2000 // but at least this many (capped at n)
)

func (b *pBuilder) build(threads int) {
	n := len(b.full)
	sem := curBuildSem.Load()
	if threads <= 0 {
		threads = cap(sem.ch)
	}
	if threads < 1 {
		threads = 1
	}
	seedN := n/seedFraction + 1
	if seedN < seedFloor {
		seedN = seedFloor
	}
	if seedN > n {
		seedN = n
	}
	sc := &pScratch{}
	for i := 1; i < seedN; i++ { // node 0 is the initial entry, already placed
		b.insert(uint32(i), sc)
	}
	if threads <= 1 || n-seedN < threads {
		for i := seedN; i < n; i++ {
			b.insert(uint32(i), sc)
		}
		return
	}
	var next int64 = int64(seedN)
	var wg sync.WaitGroup
	for w := 0; w < threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem.ch <- struct{}{} // bound active workers to the shared budget
			defer func() { <-sem.ch }()
			lsc := &pScratch{}
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if int(i) >= n {
					return
				}
				b.insert(uint32(i), lsc)
			}
		}()
	}
	wg.Wait()
}

// repair runs a second pass over the COMPLETE graph: each node re-searches and
// adds (bidirectionally, additively — never clobbering) the diverse neighbours it
// should have. During the concurrent build, two near-duplicate nodes inserted at
// the same time can each miss the other (neither was in the graph when the other
// searched); on the finished graph that search now finds them, recovering the
// recall the concurrency cost. Additive + per-node-locked, so it's race-free.
func (b *pBuilder) repair(threads int) {
	n := len(b.full)
	do := func(i uint32, sc *pScratch) {
		query := b.full[i]
		level := b.levels[i]
		b.entryMu.Lock()
		ep := b.entry
		top := b.top
		b.entryMu.Unlock()
		for lc := top; lc > level; lc-- {
			ep = b.greedy(query, ep, lc, sc)
		}
		start := top
		if level < start {
			start = level
		}
		for lc := start; lc >= 0; lc-- {
			found := b.searchLayer(query, ep, b.repairEf, lc, sc)
			found = b.selectHeuristic(found, b.capAt(lc), sc)
			for _, cd := range found {
				if cd.label == i {
					continue
				}
				b.addNeighbor(i, cd.label, lc)
				b.addNeighbor(cd.label, i, lc)
			}
			if len(found) > 0 {
				ep = found[0].label
			}
		}
	}
	sem := curBuildSem.Load()
	if threads <= 0 {
		threads = cap(sem.ch)
	}
	if threads <= 1 || n < threads*2 {
		sc := &pScratch{}
		for i := 0; i < n; i++ {
			do(uint32(i), sc)
		}
		return
	}
	var next int64
	var wg sync.WaitGroup
	for w := 0; w < threads; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem.ch <- struct{}{}
			defer func() { <-sem.ch }()
			sc := &pScratch{}
			for {
				i := atomic.AddInt64(&next, 1) - 1
				if int(i) >= n {
					return
				}
				do(uint32(i), sc)
			}
		}()
	}
	wg.Wait()
}

// pScratch is per-worker traversal state (heaps, visited set, buffers).
type pScratch struct {
	visited u32set
	cand    cheap
	res     cheap
	out     []candidate
	nbr     []uint32
	sel     []candidate
	naCand  []candidate
}

func (b *pBuilder) insert(label uint32, sc *pScratch) {
	query := b.full[label]
	level := b.levels[label]

	b.entryMu.Lock()
	ep := b.entry
	top := b.top
	b.entryMu.Unlock()

	for lc := top; lc > level; lc-- {
		ep = b.greedy(query, ep, lc, sc)
	}
	start := top
	if level < start {
		start = level
	}
	for lc := start; lc >= 0; lc-- {
		found := b.searchLayer(query, ep, b.ix.efC, lc, sc)
		found = b.selectHeuristic(found, b.capAt(lc), sc)
		// Set this node's own neighbours at lc (it isn't discoverable at lc until the
		// back-links below are created, so a plain write is safe).
		b.locks[label].Lock()
		o := b.off(lc)
		c := 0
		for _, cd := range found {
			b.links[label][o+c] = cd.label
			c++
		}
		b.cnt[label][lc] = uint16(c)
		b.locks[label].Unlock()
		var ep2 uint32
		have := false
		for _, cd := range found {
			b.addNeighbor(cd.label, label, lc)
			if !have {
				ep2, have = cd.label, true
			}
		}
		if have {
			ep = ep2
		}
	}
	if level > top {
		b.entryMu.Lock()
		if level > b.top {
			b.entry, b.top = label, level
		}
		b.entryMu.Unlock()
	}
}

func (b *pBuilder) greedy(query []float32, ep uint32, layer int32, sc *pScratch) uint32 {
	best := ep
	bestDist := b.ix.dist(query, b.full[best])
	for {
		improved := false
		sc.nbr = b.neighborsInto(best, layer, sc.nbr)
		for _, nb := range sc.nbr {
			if d := b.ix.dist(query, b.full[nb]); d < bestDist {
				bestDist, best, improved = d, nb, true
			}
		}
		if !improved {
			return best
		}
	}
}

func (b *pBuilder) searchLayer(query []float32, ep uint32, ef int, layer int32, sc *pScratch) []candidate {
	sc.visited.reset()
	sc.cand.reset(false)
	sc.res.reset(true)
	d0 := b.ix.dist(query, b.full[ep])
	sc.visited.visit(ep)
	sc.cand.push(candidate{d0, ep})
	sc.res.push(candidate{d0, ep})
	for sc.cand.len() > 0 {
		cur := sc.cand.pop()
		if sc.res.len() >= ef && cur.dist > sc.res.peek().dist {
			break
		}
		sc.nbr = b.neighborsInto(cur.label, layer, sc.nbr)
		for _, nb := range sc.nbr {
			if !sc.visited.visit(nb) {
				continue
			}
			d := b.ix.dist(query, b.full[nb])
			if sc.res.len() >= ef && d >= sc.res.peek().dist {
				continue
			}
			sc.cand.push(candidate{d, nb})
			sc.res.push(candidate{d, nb})
			if sc.res.len() > ef {
				sc.res.pop()
			}
		}
	}
	sc.out = sc.out[:0]
	for sc.res.len() > 0 {
		sc.out = append(sc.out, sc.res.pop())
	}
	for i, j := 0, len(sc.out)-1; i < j; i, j = i+1, j-1 {
		sc.out[i], sc.out[j] = sc.out[j], sc.out[i]
	}
	return sc.out
}

func (b *pBuilder) selectHeuristic(cands []candidate, m int, sc *pScratch) []candidate {
	if len(cands) <= m {
		return cands
	}
	sc.sel = sc.sel[:0]
	for ci := range cands {
		if len(sc.sel) >= m {
			break
		}
		c := cands[ci]
		cv := b.full[c.label]
		keep := true
		for r := 0; r < len(sc.sel); r++ {
			if b.ix.dist(cv, b.full[sc.sel[r].label]) < c.dist {
				keep = false
				break
			}
		}
		if keep {
			sc.sel = append(sc.sel, c)
		}
	}
	return sc.sel
}

// addNeighbor links newID into a's layer-lc list under a's write lock, pruning via
// the heuristic when full.
func (b *pBuilder) addNeighbor(a, newID uint32, lc int32) {
	if lc > b.levels[a] {
		return
	}
	capn := b.capAt(lc)
	o := b.off(lc)
	b.locks[a].Lock()
	defer b.locks[a].Unlock()
	c := int(b.cnt[a][lc])
	for i := 0; i < c; i++ {
		if b.links[a][o+i] == newID {
			return
		}
	}
	if c < capn {
		b.links[a][o+c] = newID
		b.cnt[a][lc] = uint16(c + 1)
		return
	}
	// Full: re-select capn diverse neighbours from {existing ∪ newID}.
	// Candidate and selection scratch live on the stack for the default
	// M/M0 sizes (the prune runs once per saturated edge — a heap slice
	// here was a measurable share of bulk-build allocations).
	av := b.full[a]
	var candArr, keptArr [maxStackNeighbors + 1]candidate
	cand := candArr[:0]
	if capn+1 > len(candArr) {
		cand = make([]candidate, 0, capn+1)
	}
	for i := 0; i < c; i++ {
		x := b.links[a][o+i]
		cand = append(cand, candidate{b.ix.dist(av, b.full[x]), x})
	}
	cand = append(cand, candidate{b.ix.dist(av, b.full[newID]), newID})
	insertionSortCands(cand)
	kept := keptArr[:0]
	if capn > len(keptArr) {
		kept = make([]candidate, 0, capn)
	}
	kept = b.selectHeuristicLocalInto(kept, cand, capn)
	for i, cd := range kept {
		b.links[a][o+i] = cd.label
	}
	b.cnt[a][lc] = uint16(len(kept))
}

// maxStackNeighbors bounds the stack-allocated neighbour scratch in
// addNeighbor: covers M up to 64 / M0 up to 64 (defaults are 16/32);
// larger configurations fall back to a heap slice.
const maxStackNeighbors = 64

// selectHeuristicLocalInto is the heuristic over a private candidate slice
// (used inside addNeighbor where no shared scratch is held); results are
// appended to dst.
func (b *pBuilder) selectHeuristicLocalInto(dst, cands []candidate, m int) []candidate {
	if len(cands) <= m {
		return append(dst, cands...)
	}
	for ci := range cands {
		if len(dst) >= m {
			break
		}
		c := cands[ci]
		cv := b.full[c.label]
		keep := true
		for r := 0; r < len(dst); r++ {
			if b.ix.dist(cv, b.full[dst[r].label]) < c.dist {
				keep = false
				break
			}
		}
		if keep {
			dst = append(dst, c)
		}
	}
	return dst
}
