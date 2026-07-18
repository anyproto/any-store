package vector

import (
	"math/rand"
	"sync"
)

// FlatHNSW is a struct-of-arrays / arena implementation of HNSW. Compared to
// the map-based HNSW it differs in three memory-conscious ways:
//
//  1. Vectors live in one contiguous []float32 slab (vectors), indexed by a
//     dense uint32 id. No per-vector slice header, no N pointers for the GC to
//     scan, sequential reads for the SIMD distance kernels.
//
//  2. Adjacency lists live in one flat []uint32 arena (links) plus a flat
//     []int32 count arena. Each node's links are appended once at insert time
//     (ids are dense and monotonic, so the arena grows append-only — exactly
//     the access pattern an arena is good at). No map[K]*node per node.
//
//  3. The per-query visited set and the two search heaps are pooled and reused
//     (see flatScratch / visitedList), so a steady-state search allocates
//     nothing.
//
// The same arena layout is what BtreeHNSW serialises to / from disk.
type FlatHNSW struct {
	M              int     // max neighbours per node on layers >= 1
	M0             int     // max neighbours per node on layer 0 (commonly 2*M)
	Ml             float64 // level generation factor
	EfSearch       int     // candidate-list size at query time
	EfConstruction int     // candidate-list size at insert time

	dim  int
	dist DistanceFunc
	rng  *rand.Rand

	mu      sync.RWMutex
	vectors []float32 // arena: vector for id at [id*dim : (id+1)*dim]
	keys    []uint64  // id -> caller key
	keyToID map[uint64]uint32
	level   []int32 // id -> top layer of the node

	// deleted[id] tombstones a node: it stays in the arenas and is still
	// traversed during graph navigation (it may be a vital waypoint), but is
	// excluded from search results and from neighbour selection. liveCount is
	// the number of non-deleted nodes. Tombstones are reclaimed by Compact.
	deleted   []bool
	liveCount int

	// Adjacency arena. For a node with top layer L the block holds M0 slots for
	// layer 0 followed by L blocks of M slots for layers 1..L. counts holds L+1
	// entries (one neighbour-count per layer).
	links    []uint32
	counts   []int32
	linkOff  []int32 // id -> start of node's block in links
	countOff []int32 // id -> start of node's block in counts

	entryID  uint32
	topLayer int32
	hasEntry bool

	// notifyDirty, when set, is called with the id of every node whose data or
	// adjacency changed. BtreeHNSW uses it to track which records to persist.
	notifyDirty func(id uint32)
	// notifyDelete, when set, is called with the id of every tombstoned node so
	// the persistence layer can record a (cheap) tombstone instead of rewriting
	// the whole node record.
	notifyDelete func(id uint32)

	scratch sync.Pool
}

// NewFlatHNSW creates an empty arena-backed HNSW. seed makes level generation
// deterministic; pass 0 for a fixed default.
func NewFlatHNSW(dim int, m Metric, seed int64) *FlatHNSW {
	M := 16
	return &FlatHNSW{
		M:              M,
		M0:             2 * M,
		Ml:             0.25,
		EfSearch:       20,
		EfConstruction: 200,
		dim:            dim,
		dist:           m.DistanceFor(),
		rng:            rand.New(rand.NewSource(seed)),
		keyToID:        make(map[uint64]uint32),
	}
}

// Len returns the number of live (non-deleted) vectors.
func (h *FlatHNSW) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.liveCount
}

// PhysicalLen returns the number of slots in the arenas, including tombstones.
// PhysicalLen-Len is the reclaimable space waiting for Compact.
func (h *FlatHNSW) PhysicalLen() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.keys)
}

func (h *FlatHNSW) vectorAt(id uint32) []float32 {
	return h.vectors[int(id)*h.dim : (int(id)+1)*h.dim]
}

func (h *FlatHNSW) randomLevel() int32 {
	var l int32
	for h.rng.Float64() < h.Ml {
		l++
		if l > 30 { // hard cap, matches typical HNSW guards
			break
		}
	}
	return l
}

// neighborSlots returns the [start, cap) of a node's link region for a layer.
func (h *FlatHNSW) neighborSlots(id uint32, layer int32) (start, capacity int) {
	base := int(h.linkOff[id])
	if layer == 0 {
		return base, h.M0
	}
	return base + h.M0 + int(layer-1)*h.M, h.M
}

func (h *FlatHNSW) neighbors(id uint32, layer int32) []uint32 {
	start, _ := h.neighborSlots(id, layer)
	cnt := int(h.counts[int(h.countOff[id])+int(layer)])
	return h.links[start : start+cnt]
}

// addNeighbor links newID into id's neighbour set on layer, pruning to the
// farthest-out if the set is full (the same "drop worst" rule coder/hnsw uses).
func (h *FlatHNSW) addNeighbor(id, newID uint32, layer int32) {
	start, capacity := h.neighborSlots(id, layer)
	cntIdx := int(h.countOff[id]) + int(layer)
	cnt := int(h.counts[cntIdx])

	// already linked?
	for i := 0; i < cnt; i++ {
		if h.links[start+i] == newID {
			return
		}
	}
	if cnt < capacity {
		h.links[start+cnt] = newID
		h.counts[cntIdx] = int32(cnt + 1)
		return
	}
	// full: replace the farthest existing neighbour if newID is closer.
	base := h.vectorAt(id)
	newDist := h.dist(base, h.vectorAt(newID))
	worstIdx, worstDist := -1, newDist
	for i := 0; i < cnt; i++ {
		d := h.dist(base, h.vectorAt(h.links[start+i]))
		if d > worstDist {
			worstDist, worstIdx = d, i
		}
	}
	if worstIdx >= 0 {
		h.links[start+worstIdx] = newID
	}
}

func (h *FlatHNSW) markDirty(id uint32) {
	if h.notifyDirty != nil {
		h.notifyDirty(id)
	}
}

// Add inserts a vector. If the key already exists the call is ignored; use
// Update to change an existing key's vector.
func (h *FlatHNSW) Add(key uint64, vec []float32) {
	if len(vec) != h.dim {
		panic("vector: dimension mismatch")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.addLocked(key, vec)
}

// addLocked is the lock-free insert core (h.mu must be held).
func (h *FlatHNSW) addLocked(key uint64, vec []float32) {
	if _, ok := h.keyToID[key]; ok {
		return
	}

	id := uint32(len(h.keys))
	L := h.randomLevel()

	h.keys = append(h.keys, key)
	h.keyToID[key] = id
	h.vectors = append(h.vectors, vec...)
	h.level = append(h.level, L)
	h.deleted = append(h.deleted, false)
	h.liveCount++

	// allocate adjacency block (append-only arena growth)
	h.linkOff = append(h.linkOff, int32(len(h.links)))
	blockLinks := h.M0 + int(L)*h.M
	h.links = append(h.links, make([]uint32, blockLinks)...)
	h.countOff = append(h.countOff, int32(len(h.counts)))
	h.counts = append(h.counts, make([]int32, int(L)+1)...)

	h.markDirty(id)

	if !h.hasEntry {
		h.entryID, h.topLayer, h.hasEntry = id, L, true
		return
	}

	sc := h.getScratch()
	defer h.putScratch(sc)

	// 1. descend from the top down to L+1 with a greedy ef=1 walk.
	ep := h.entryID
	for lc := h.topLayer; lc > L; lc-- {
		ep = h.greedyClosest(vec, ep, lc, sc)
	}

	// 2. for layers min(L, top)..0, search and connect.
	start := min(h.topLayer, L)
	for lc := start; lc >= 0; lc-- {
		found := h.searchLayer(vec, ep, h.EfConstruction, lc, sc)
		m := h.M
		if lc == 0 {
			m = h.M0
		}
		selected := selectClosest(found, m)
		for _, c := range selected {
			h.addNeighbor(id, c.id, lc)
			h.addNeighbor(c.id, id, lc)
			h.markDirty(c.id)
		}
		if len(found) > 0 {
			ep = found[0].id // closest, for the next layer down
		}
	}

	if L > h.topLayer {
		h.entryID, h.topLayer = id, L
	}
}

// appendRaw bulk-loads a single node directly into the arenas, bypassing the
// search/connect machinery. Nodes MUST be appended in ascending id order
// (0,1,2,…) so the dense-id invariant holds. neighborsPerLayer[l] holds the
// already-computed neighbour ids for layer l. Used by BtreeHNSW to reconstruct
// an index from its persisted records without re-running construction (which
// would pick different random levels). The caller is responsible for setting
// the entry point afterwards via setEntry.
func (h *FlatHNSW) appendRaw(key uint64, level int32, vec []float32, neighborsPerLayer [][]uint32) {
	id := uint32(len(h.keys))
	h.keys = append(h.keys, key)
	h.keyToID[key] = id
	h.vectors = append(h.vectors, vec...)
	h.level = append(h.level, level)
	h.deleted = append(h.deleted, false)
	h.liveCount++

	h.linkOff = append(h.linkOff, int32(len(h.links)))
	blockLinks := h.M0 + int(level)*h.M
	h.links = append(h.links, make([]uint32, blockLinks)...)
	h.countOff = append(h.countOff, int32(len(h.counts)))
	h.counts = append(h.counts, make([]int32, int(level)+1)...)

	for lc := int32(0); lc <= level; lc++ {
		nbs := neighborsPerLayer[lc]
		start, capacity := h.neighborSlots(id, lc)
		if len(nbs) > capacity {
			nbs = nbs[:capacity]
		}
		copy(h.links[start:start+len(nbs)], nbs)
		h.counts[int(h.countOff[id])+int(lc)] = int32(len(nbs))
	}
}

// appendTopo bulk-loads a node's TOPOLOGY only (key, level, per-layer
// neighbours) without a vector — the vector stays on disk for a paged index.
// Like appendRaw, nodes must arrive in ascending id order. The resulting
// FlatHNSW must only be used through a PagedHNSW (its vector slab is empty, so
// vectorAt / in-memory Search would be invalid).
func (h *FlatHNSW) appendTopo(key uint64, level int32, neighborsPerLayer [][]uint32) {
	id := uint32(len(h.keys))
	h.keys = append(h.keys, key)
	h.keyToID[key] = id
	h.level = append(h.level, level)
	h.deleted = append(h.deleted, false)
	h.liveCount++

	h.linkOff = append(h.linkOff, int32(len(h.links)))
	blockLinks := h.M0 + int(level)*h.M
	h.links = append(h.links, make([]uint32, blockLinks)...)
	h.countOff = append(h.countOff, int32(len(h.counts)))
	h.counts = append(h.counts, make([]int32, int(level)+1)...)

	for lc := int32(0); lc <= level; lc++ {
		nbs := neighborsPerLayer[lc]
		start, capacity := h.neighborSlots(id, lc)
		if len(nbs) > capacity {
			nbs = nbs[:capacity]
		}
		copy(h.links[start:start+len(nbs)], nbs)
		h.counts[int(h.countOff[id])+int(lc)] = int32(len(nbs))
	}
}

// setEntry restores the graph entry point (used after a bulk load).
func (h *FlatHNSW) setEntry(id uint32, top int32) {
	h.entryID, h.topLayer, h.hasEntry = id, top, true
}

// snapshot returns a node's persisted form: its key, level, vector copy, and
// per-layer neighbour ids. Used by BtreeHNSW to serialise a node.
func (h *FlatHNSW) snapshot(id uint32) (key uint64, level int32, vec []float32, neighborsPerLayer [][]uint32) {
	level = h.level[id]
	key = h.keys[id]
	vec = append([]float32(nil), h.vectorAt(id)...)
	neighborsPerLayer = make([][]uint32, level+1)
	for lc := int32(0); lc <= level; lc++ {
		neighborsPerLayer[lc] = append([]uint32(nil), h.neighbors(id, lc)...)
	}
	return
}

// greedyClosest walks one layer toward target, returning the closest node id.
func (h *FlatHNSW) greedyClosest(target []float32, ep uint32, layer int32, sc *flatScratch) uint32 {
	best := ep
	bestDist := h.dist(target, h.vectorAt(ep))
	for {
		improved := false
		for _, nb := range h.neighbors(best, layer) {
			d := h.dist(target, h.vectorAt(nb))
			if d < bestDist {
				bestDist, best, improved = d, nb, true
			}
		}
		if !improved {
			return best
		}
	}
}

// searchLayer is the standard two-heap HNSW layer search: a min-heap of nodes
// still to expand and a max-heap of the best ef results. Returns results sorted
// closest-first. Uses pooled scratch (heaps + visited) so it allocates nothing
// in steady state.
func (h *FlatHNSW) searchLayer(target []float32, ep uint32, ef int, layer int32, sc *flatScratch) []candidate {
	sc.vis.prepare(len(h.keys))
	sc.cand.reset(false) // min-heap (closest first)
	sc.res.reset(true)   // max-heap (farthest first)

	d0 := h.dist(target, h.vectorAt(ep))
	sc.vis.visit(ep)
	// Deleted nodes are still expanded (they may be the only route to a live
	// region) but never enter the result set.
	sc.cand.push(candidate{d0, ep})
	if !h.deleted[ep] {
		sc.res.push(candidate{d0, ep})
	}

	for sc.cand.len() > 0 {
		cur := sc.cand.pop()
		// Termination uses the result-set bound. While res is under-filled
		// (e.g. a neighbourhood thick with tombstones) we keep expanding, which
		// is exactly the extra work tombstones cost a query.
		if sc.res.len() >= ef && cur.dist > sc.res.peek().dist {
			break
		}
		for _, nb := range h.neighbors(cur.id, layer) {
			if !sc.vis.visit(nb) {
				continue
			}
			d := h.dist(target, h.vectorAt(nb))
			admit := sc.res.len() < ef || d < sc.res.peek().dist
			if !admit {
				continue
			}
			sc.cand.push(candidate{d, nb}) // route through, deleted or not
			if h.deleted[nb] {
				continue
			}
			sc.res.push(candidate{d, nb})
			if sc.res.len() > ef {
				sc.res.pop()
			}
		}
	}

	out := sc.out[:0]
	for sc.res.len() > 0 {
		out = append(out, sc.res.pop())
	}
	// res is a max-heap; reverse to get closest-first
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	sc.out = out
	return out
}

// selectClosest returns the m closest candidates (simple heuristic).
func selectClosest(c []candidate, m int) []candidate {
	if len(c) <= m {
		return c
	}
	// c is already closest-first from searchLayer
	return c[:m]
}

// Search returns the k nearest neighbours, closest-first.
func (h *FlatHNSW) Search(query []float32, k int) []SearchResult {
	if len(query) != h.dim {
		panic("vector: dimension mismatch")
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.hasEntry || k <= 0 {
		return nil
	}

	sc := h.getScratch()
	defer h.putScratch(sc)

	ep := h.entryID
	for lc := h.topLayer; lc > 0; lc-- {
		ep = h.greedyClosest(query, ep, lc, sc)
	}
	ef := max(h.EfSearch, k)
	found := h.searchLayer(query, ep, ef, 0, sc)
	k = min(k, len(found))
	out := make([]SearchResult, k)
	for i := 0; i < k; i++ {
		out[i] = SearchResult{Key: h.keys[found[i].id], Distance: found[i].dist}
	}
	return out
}

// MemBytes returns an approximate resident size of the index's backing arenas
// (vectors + adjacency + bookkeeping), excluding the keyToID map.
func (h *FlatHNSW) MemBytes() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return cap(h.vectors)*4 +
		cap(h.links)*4 +
		cap(h.counts)*4 +
		cap(h.keys)*8 +
		cap(h.level)*4 +
		cap(h.linkOff)*4 +
		cap(h.countOff)*4
}

// --- pooled per-search scratch ---------------------------------------------

type flatScratch struct {
	cand cheap
	res  cheap
	vis  visitedList
	out  []candidate
}

func (h *FlatHNSW) getScratch() *flatScratch {
	if s, ok := h.scratch.Get().(*flatScratch); ok {
		return s
	}
	return &flatScratch{}
}

func (h *FlatHNSW) putScratch(s *flatScratch) { h.scratch.Put(s) }
