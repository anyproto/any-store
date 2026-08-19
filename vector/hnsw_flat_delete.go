package vector

// This file implements deletion, update and reclamation for FlatHNSW, plus a
// hard-delete-with-repair variant kept purely so the benchmarks can quantify
// why the tombstone strategy is the one worth landing.
//
// Why tombstones (soft delete) are the default
// --------------------------------------------
// The arena layout makes a true in-place delete expensive on two fronts:
//
//   - No reverse index. To physically unlink a node we must find every node
//     that points AT it. Nothing in the layout records incoming edges, so the
//     only way is to scan the whole adjacency arena: O(total_links). See
//     deleteHardRepairLocked, whose cost the benchmarks measure.
//
//   - Variable-length adjacency blocks. A node with top layer L owns a block of
//     M0 + L*M slots at an insertion-order offset. A freed id therefore cannot
//     be reused by a new node of a different level, so id reuse can't be a
//     simple free-list — it needs Compact to rewrite the arenas.
//
// A tombstone is O(1): flip a bit, drop the key from the lookup map. The node
// stays as a navigation waypoint (deleted nodes are still traversed by
// searchLayer, just never returned). The cost is deferred to query time
// (slightly more nodes traversed) and to a periodic Compact that rebuilds the
// arenas keeping only live nodes.

// Delete tombstones the vector for key. It returns false if the key is absent
// or already deleted. O(1); reclaim the space later with Compact.
func (h *FlatHNSW) Delete(key uint64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.deleteLocked(key)
}

func (h *FlatHNSW) deleteLocked(key uint64) bool {
	id, ok := h.keyToID[key]
	if !ok || h.deleted[id] {
		return false
	}
	h.deleted[id] = true
	delete(h.keyToID, key)
	h.liveCount--
	if h.notifyDelete != nil {
		h.notifyDelete(id)
	}
	return true
}

// Update changes the vector stored for key. It is implemented as
// tombstone-old + insert-new: the old node keeps acting as a waypoint until
// Compact reclaims it, while the new node is linked into the graph at a fresh
// id using the new vector. This is the standard HNSW update because an in-place
// vector swap would leave both the node's own edges and every incoming edge
// computed against the stale vector (see UpdateInPlace for that variant and its
// caveats). Returns false if key does not exist.
func (h *FlatHNSW) Update(key uint64, vec []float32) bool {
	if len(vec) != h.dim {
		panic("vector: dimension mismatch")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.keyToID[key]; !ok {
		return false
	}
	h.deleteLocked(key)
	h.addLocked(key, vec)
	return true
}

// UpdateInPlace overwrites a node's vector without changing its id or re-running
// construction. It is cheap (no new node, no re-link of the whole graph) but
// lower quality: the node's outgoing edges and every incoming edge were chosen
// for the OLD vector and are not recomputed, so the node can end up poorly
// placed. Offered for benchmarking against Update; not recommended when the
// vector moves significantly. Returns false if key is absent or deleted.
func (h *FlatHNSW) UpdateInPlace(key uint64, vec []float32) bool {
	if len(vec) != h.dim {
		panic("vector: dimension mismatch")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	id, ok := h.keyToID[key]
	if !ok || h.deleted[id] {
		return false
	}
	copy(h.vectorAt(id), vec)
	h.markDirty(id)
	return true
}

// deleteHardRepairLocked physically unlinks a node: it scans the whole
// adjacency arena to drop every edge pointing at the node (the O(total_links)
// cost a reverse index would avoid), then tombstones it. Optionally re-links the
// orphaned neighbours to each other (coder/hnsw's "replenish"). This exists to
// measure the cost the tombstone path avoids; it is not on the default path.
func (h *FlatHNSW) deleteHardRepairLocked(key uint64, replenish bool) bool {
	id, ok := h.keyToID[key]
	if !ok || h.deleted[id] {
		return false
	}

	// Scan every node's per-layer neighbour list and swap-remove `id`.
	for other := uint32(0); other < uint32(len(h.keys)); other++ {
		if other == id || h.deleted[other] {
			continue
		}
		lvl := h.level[other]
		for lc := int32(0); lc <= lvl; lc++ {
			start, _ := h.neighborSlots(other, lc)
			cntIdx := int(h.countOff[other]) + int(lc)
			cnt := int(h.counts[cntIdx])
			for i := 0; i < cnt; i++ {
				if h.links[start+i] != id {
					continue
				}
				// swap-remove
				h.links[start+i] = h.links[start+cnt-1]
				cnt--
				h.counts[cntIdx] = int32(cnt)
				h.markDirty(other)
				if replenish {
					// connect this orphaned neighbour to one of id's neighbours
					h.replenishFrom(other, id, lc)
				}
				break
			}
		}
	}

	h.deleted[id] = true
	delete(h.keyToID, key)
	h.liveCount--
	if h.notifyDelete != nil {
		h.notifyDelete(id)
	}
	return true
}

// DeleteHardRepair is the locked wrapper around deleteHardRepairLocked.
func (h *FlatHNSW) DeleteHardRepair(key uint64, replenish bool) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.deleteHardRepairLocked(key, replenish)
}

// replenishFrom tries to restore degree for node `victim` (which just lost the
// edge to `removed`) by linking it to one of `removed`'s other live neighbours.
func (h *FlatHNSW) replenishFrom(victim, removed uint32, layer int32) {
	if layer > h.level[removed] {
		return
	}
	for _, cand := range h.neighbors(removed, layer) {
		if cand == victim || h.deleted[cand] {
			continue
		}
		h.addNeighbor(victim, cand, layer)
		h.addNeighbor(cand, victim, layer)
		h.markDirty(cand)
		return
	}
}

// DeletedFraction returns tombstones / physical size — the signal for when to
// Compact. Production systems compact somewhere around 0.1–0.2.
func (h *FlatHNSW) DeletedFraction() float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := len(h.keys)
	if n == 0 {
		return 0
	}
	return float64(n-h.liveCount) / float64(n)
}

// Compact rebuilds the arenas keeping only live nodes, assigning fresh dense
// ids and remapping every neighbour id. Edges that pointed at tombstoned nodes
// are dropped (leaving slightly thinner neighbourhoods — cheap, and usually a
// negligible recall hit until the deleted fraction is large). This reclaims all
// tombstone memory and removes tombstones from query navigation, restoring
// search speed. O(physical_nodes + total_links). Does NOT re-run construction;
// use Rebuild for that.
func (h *FlatHNSW) Compact() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.liveCount == len(h.keys) {
		return // nothing tombstoned
	}

	// old id -> new id (only for live nodes), in ascending old-id order.
	remap := make([]int32, len(h.keys))
	for i := range remap {
		remap[i] = -1
	}
	var newID uint32
	for old := uint32(0); old < uint32(len(h.keys)); old++ {
		if !h.deleted[old] {
			remap[old] = int32(newID)
			newID++
		}
	}

	n := int(newID)
	nv := &FlatHNSW{
		M: h.M, M0: h.M0, Ml: h.Ml, EfSearch: h.EfSearch, EfConstruction: h.EfConstruction,
		dim: h.dim, dist: h.dist, rng: h.rng,
		keyToID:      make(map[uint64]uint32, n),
		notifyDirty:  h.notifyDirty,
		notifyDelete: h.notifyDelete,
	}
	nv.vectors = make([]float32, 0, n*h.dim)
	nv.keys = make([]uint64, 0, n)
	nv.level = make([]int32, 0, n)
	nv.deleted = make([]bool, 0, n)

	bestEntry, bestLevel := uint32(0), int32(-1)
	for old := uint32(0); old < uint32(len(h.keys)); old++ {
		if h.deleted[old] {
			continue
		}
		nid := uint32(remap[old])
		lvl := h.level[old]

		nv.keys = append(nv.keys, h.keys[old])
		nv.keyToID[h.keys[old]] = nid
		nv.vectors = append(nv.vectors, h.vectorAt(old)...)
		nv.level = append(nv.level, lvl)
		nv.deleted = append(nv.deleted, false)
		nv.liveCount++

		nv.linkOff = append(nv.linkOff, int32(len(nv.links)))
		nv.links = append(nv.links, make([]uint32, h.M0+int(lvl)*h.M)...)
		nv.countOff = append(nv.countOff, int32(len(nv.counts)))
		nv.counts = append(nv.counts, make([]int32, int(lvl)+1)...)

		for lc := int32(0); lc <= lvl; lc++ {
			start, capacity := nv.neighborSlots(nid, lc)
			cntIdx := int(nv.countOff[nid]) + int(lc)
			w := 0
			for _, oldNbr := range h.neighbors(old, lc) {
				if r := remap[oldNbr]; r >= 0 && w < capacity {
					nv.links[start+w] = uint32(r)
					w++
				}
			}
			nv.counts[cntIdx] = int32(w)
		}

		if lvl > bestLevel {
			bestEntry, bestLevel = nid, lvl
		}
	}

	if n > 0 {
		nv.entryID, nv.topLayer, nv.hasEntry = bestEntry, bestLevel, true
	}

	// adopt the compacted arenas
	h.vectors, h.keys, h.keyToID, h.level, h.deleted = nv.vectors, nv.keys, nv.keyToID, nv.level, nv.deleted
	h.links, h.counts, h.linkOff, h.countOff = nv.links, nv.counts, nv.linkOff, nv.countOff
	h.entryID, h.topLayer, h.hasEntry = nv.entryID, nv.topLayer, nv.hasEntry
	h.liveCount = nv.liveCount
}

// Rebuild reconstructs the graph from scratch by re-inserting every live vector.
// Highest quality (full HNSW construction) and most expensive; use when many
// updates/deletes have degraded the graph beyond what Compact restores.
func (h *FlatHNSW) Rebuild() {
	h.mu.Lock()
	defer h.mu.Unlock()

	type kv struct {
		key uint64
		vec []float32
	}
	live := make([]kv, 0, h.liveCount)
	for id := uint32(0); id < uint32(len(h.keys)); id++ {
		if !h.deleted[id] {
			live = append(live, kv{h.keys[id], append([]float32(nil), h.vectorAt(id)...)})
		}
	}

	// reset state
	h.vectors = h.vectors[:0]
	h.keys = h.keys[:0]
	h.level = h.level[:0]
	h.deleted = h.deleted[:0]
	h.links = h.links[:0]
	h.counts = h.counts[:0]
	h.linkOff = h.linkOff[:0]
	h.countOff = h.countOff[:0]
	h.keyToID = make(map[uint64]uint32, len(live))
	h.liveCount = 0
	h.hasEntry = false
	h.topLayer = 0

	for _, e := range live {
		h.addLocked(e.key, e.vec)
	}
}
