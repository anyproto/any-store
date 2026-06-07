package vindex

// vecCache is the vector cache behind lever 1. Within a write BATCH (one
// WriteTx, many Insert calls) the graph traversal, neighbour-selection heuristic
// and back-linking read many of the SAME node vectors — above all the entry
// point and the upper-layer hubs, which every insert touches. Vectors are
// immutable (a label's vector never changes for an Index's lifetime; compaction
// reuses labels only via a fresh Index), so a cached vector stays valid for the
// whole batch and each is read from the btree at most once.
//
// The cache is owned by a pooled searcher and reused across inserts/batches with
// a bounded, configurable RAM ceiling:
//
//   - capVecs caps the number of cached vectors. Once full, further misses fall
//     back to an uncached btree read — so RAM is at most capVecs*dim*4 bytes,
//     independent of graph size. Hubs are touched first and stay resident; only
//     the long tail misses.
//   - Vectors live in an ARENA of fixed-size blocks (append-only, never moved),
//     so a []float32 handed out earlier in the batch stays valid as the cache
//     grows. reset() rewinds the write cursor; blocks are kept and refilled.
//   - A flat open-addressing probe table (linear probing, power-of-two, pre-sized
//     for capVecs) maps label -> arena slot. reset() bumps a generation tag
//     instead of clearing, so a new batch resets occupancy in O(1).
//
// All fields are pointer-free value slices, so the cache adds no GC scan cost.
type vecCache struct {
	dim     int
	capVecs int
	block   int         // vectors per arena block
	blocks  [][]float32 // arena; each len == block*dim
	cur     int         // current block index
	off     int         // vectors filled in the current block

	label []uint32 // probe table: label stored at a position
	slot  []int32  // probe table: arena slot at a position
	seen  []uint32 // probe table: position occupied iff seen[i]==gen
	gen   uint32
	mask  uint32
	count int // cached vectors this generation
}

const vecCacheBlockVecs = 256 // vectors per arena block

// reset prepares the cache for a new batch. Allocation-free once dim and capVecs
// are stable (the arena blocks and probe table are reused): it just rewinds the
// arena cursor and bumps the generation tag.
func (c *vecCache) reset(dim, capVecs int) {
	if c.dim != dim || c.capVecs != capVecs || c.label == nil {
		c.dim = dim
		c.capVecs = capVecs
		c.block = vecCacheBlockVecs
		c.blocks = nil
		// Probe table sized so capVecs entries stay under a ~0.67 load factor.
		n := 64
		for n < capVecs+capVecs/2 {
			n <<= 1
		}
		c.label = make([]uint32, n)
		c.slot = make([]int32, n)
		c.seen = make([]uint32, n)
		c.mask = uint32(n - 1)
		c.gen = 1
		c.cur, c.off, c.count = 0, 0, 0
		return
	}
	c.cur, c.off, c.count = 0, 0, 0
	c.gen++
	if c.gen == 0 { // wrapped: stale tags could false-hit, so clear once
		for i := range c.seen {
			c.seen[i] = 0
		}
		c.gen = 1
	}
}

// reserve looks up label. On a hit it returns (vec, true). On a miss with room it
// reserves a fresh arena slot, registers it, and returns (dst, false) — a
// dim-length slice the caller must fill with label's vector, stable for the rest
// of the batch. When the cache is full it returns (nil, false): the caller reads
// the vector uncached.
func (c *vecCache) reserve(label uint32) ([]float32, bool) {
	pos := hashU32(label) & c.mask
	for c.seen[pos] == c.gen {
		if c.label[pos] == label {
			return c.slotSlice(c.slot[pos]), true
		}
		pos = (pos + 1) & c.mask
	}
	if c.count >= c.capVecs { // full → uncached read
		return nil, false
	}
	if c.off == c.block { // current arena block full → advance
		c.cur++
		c.off = 0
	}
	if c.cur == len(c.blocks) {
		c.blocks = append(c.blocks, make([]float32, c.block*c.dim))
	}
	slot := int32(c.cur*c.block + c.off)
	dst := c.blocks[c.cur][c.off*c.dim : c.off*c.dim+c.dim]
	c.off++
	c.seen[pos] = c.gen
	c.label[pos] = label
	c.slot[pos] = slot
	c.count++
	return dst, false
}

// slotSlice resolves an arena slot to its dim-length backing slice.
func (c *vecCache) slotSlice(slot int32) []float32 {
	b := int(slot) / c.block
	o := (int(slot) % c.block) * c.dim
	return c.blocks[b][o : o+c.dim]
}

// hashU32 is the lowbias32 integer finalizer — a fast, well-distributed mix for
// dense-ish uint32 labels (avoids the clustering a plain identity hash would
// cause under linear probing).
func hashU32(x uint32) uint32 {
	x ^= x >> 16
	x *= 0x7feb352d
	x ^= x >> 15
	x *= 0x846ca68b
	x ^= x >> 16
	return x
}
