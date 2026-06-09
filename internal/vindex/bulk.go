package vindex

import (
	"encoding/binary"
	"fmt"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// BulkBuild creates a fresh index over (ids, vecs) by constructing the HNSW graph
// in RAM and serializing every record to the namespaces in ONE bulk pass, in label
// (insertion) order. This is the "build in memory, flush once" pattern every other
// engine uses (pgvector/Lance/DiskANN/usearch); it avoids the per-edge copy-on-write
// B-tree churn of Insert-per-document, the dominant cost of an Insert-built graph.
//
// The in-RAM build mirrors Insert's algorithm exactly — same level RNG draw order,
// same beam search, same neighbour-selection heuristic, same back-link pruning — so
// for a given (seed, order, vectors) it produces a graph byte-identical to the
// per-insert path. The serialized records use the same encoders (encodeVec /
// encodeAdj / meta), so search, incremental Insert, Delete and Compact all work
// against a bulk-built index unchanged.
//
// ids must be unique and len(ids)==len(vecs); each vec must have length p.Dim.
// It runs entirely inside wtx (atomic: on error the tx rolls back).
func BulkBuild(wtx *btree.WriteTx, prefix string, p Params, seed int64, ids [][]byte, vecs [][]float32) (*Index, error) {
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

	b := newRamBuilder(ix, n)
	// Build the graph node by node (label == index), exactly like Insert.
	//
	// Quantization fidelity: the per-insert path traverses against the *stored*
	// vectors, which for int8 are quantized-then-dequantized (lossy) — while the
	// node being inserted is compared as a full-precision query. We replicate that
	// asymmetry: b.full[i] (full, normalized) is used only as the query when
	// inserting i; b.vec[i] (= dequantized for int8, = full otherwise) is what every
	// candidate/neighbour distance uses. Without this, an int8 graph would differ.
	for i := 0; i < n; i++ {
		full := vecs[i]
		if ix.normalize {
			nv := make([]float32, ix.dim)
			full = normalizeInto(nv, vecs[i])
		} else {
			full = append([]float32(nil), vecs[i]...) // own copy (caller may reuse)
		}
		b.full[i] = full
		if ix.quant == QuantNone {
			b.vec[i] = full
		} else {
			enc := encodeVec(nil, full, ix.quant)
			dq := make([]float32, ix.dim)
			v, ok := decodeVecInto(enc, ix.dim, ix.quant, dq)
			if !ok {
				return nil, fmt.Errorf("vindex: dequantize failed for vec %d", i)
			}
			b.vec[i] = v
		}
		b.insert(uint32(i), full)
	}

	// Serialize in label order: dense, sequential keys → minimal page churn.
	mt := &meta{
		dim: ix.dim, metric: p.Metric, m: ix.m, m0: ix.m0,
		efC: ix.efC, efS: ix.efS, ml: ix.ml, quant: ix.quant,
		count: int64(n), nextLabel: uint32(n), deletedCount: 0,
		hasEntry: b.hasEntry, entryLabel: b.entry, topLayer: b.top,
		l0Gen: uint64(n), // per-insert ends at l0Gen==n; match so a hybrid mirror keys correctly
	}
	var kbuf, vbuf []byte
	var lb [4]byte
	for label := 0; label < n; label++ {
		kbuf = labelKey(kbuf, uint32(label))
		if ix.quant == QuantNone {
			if err := wtx.Put(ix.vvec, kbuf, f32bytes(b.full[label])); err != nil {
				return nil, err
			}
		} else {
			vbuf = encodeVec(vbuf[:0], b.full[label], ix.quant)
			if err := wtx.Put(ix.vvec, kbuf, vbuf); err != nil {
				return nil, err
			}
		}
		if err := wtx.Put(ix.vlbl, kbuf, ids[label]); err != nil {
			return nil, err
		}
		vbuf = encodeAdj(vbuf[:0], b.levels[label], false, b.nbrs[label])
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

// ramBuilder is the in-RAM HNSW used by BulkBuild. It holds the whole graph
// (vectors + per-layer adjacency) in plain slices and reuses the same heaps /
// visited set / candidate type as the btree searcher, so its traversal and
// selection are identical to Insert's.
type ramBuilder struct {
	ix     *Index
	full   [][]float32  // full-precision normalized vector per label (used only as a query)
	vec    [][]float32  // candidate/neighbour representation (dequantized for int8, else == full)
	levels []int32      // top layer per label
	nbrs   [][][]uint32 // label -> layer -> neighbour labels

	entry    uint32
	top      int32
	hasEntry bool

	visited u32set
	cand    cheap
	res     cheap
	out     []candidate
	sel     []candidate
	naCand  []candidate
}

func newRamBuilder(ix *Index, n int) *ramBuilder {
	return &ramBuilder{
		ix:     ix,
		full:   make([][]float32, n),
		vec:    make([][]float32, n),
		levels: make([]int32, n),
		nbrs:   make([][][]uint32, n),
	}
}

// insert adds label to the in-RAM graph using query (its full-precision vector) for
// traversal and b.vec for all candidate distances, mirroring Index.Insert step for
// step.
func (b *ramBuilder) insert(label uint32, query []float32) {
	level := b.ix.randomLevel()
	b.levels[label] = level
	b.nbrs[label] = make([][]uint32, level+1)

	if !b.hasEntry {
		b.hasEntry, b.entry, b.top = true, label, level
		return
	}

	ep := b.entry
	for lc := b.top; lc > level; lc-- {
		ep = b.greedyClosest(query, ep, lc)
	}
	start := b.top
	if level < start {
		start = level
	}
	for lc := start; lc >= 0; lc-- {
		found := b.searchLayer(query, ep, b.ix.efC, lc)
		found = b.selectHeuristic(found, b.ix.maxConn(lc))
		sel := make([]uint32, len(found)) // copy out before addNeighbor reuses b.sel
		for i, c := range found {
			sel[i] = c.label
		}
		b.nbrs[label][lc] = sel
		for _, nb := range sel {
			b.addNeighbor(nb, label, lc)
		}
		if len(sel) > 0 {
			ep = sel[0]
		}
	}
	if level > b.top {
		b.entry, b.top = label, level
	}
}

func (b *ramBuilder) neighborsAt(label uint32, layer int32) []uint32 {
	if layer > b.levels[label] {
		return nil
	}
	return b.nbrs[label][layer]
}

func (b *ramBuilder) greedyClosest(query []float32, ep uint32, layer int32) uint32 {
	best := ep
	bestDist := b.ix.dist(query, b.vec[best])
	for {
		improved := false
		for _, nb := range b.neighborsAt(best, layer) {
			if d := b.ix.dist(query, b.vec[nb]); d < bestDist {
				bestDist, best, improved = d, nb, true
			}
		}
		if !improved {
			return best
		}
	}
}

func (b *ramBuilder) searchLayer(query []float32, ep uint32, ef int, layer int32) []candidate {
	b.visited.reset()
	b.cand.reset(false)
	b.res.reset(true)

	d0 := b.ix.dist(query, b.vec[ep])
	b.visited.visit(ep)
	b.cand.push(candidate{d0, ep})
	b.res.push(candidate{d0, ep}) // no tombstones during build

	for b.cand.len() > 0 {
		cur := b.cand.pop()
		if b.res.len() >= ef && cur.dist > b.res.peek().dist {
			break
		}
		for _, nb := range b.neighborsAt(cur.label, layer) {
			if !b.visited.visit(nb) {
				continue
			}
			d := b.ix.dist(query, b.vec[nb])
			if b.res.len() >= ef && d >= b.res.peek().dist {
				continue
			}
			b.cand.push(candidate{d, nb})
			b.res.push(candidate{d, nb})
			if b.res.len() > ef {
				b.res.pop()
			}
		}
	}

	b.out = b.out[:0]
	for b.res.len() > 0 {
		b.out = append(b.out, b.res.pop())
	}
	for i, j := 0, len(b.out)-1; i < j; i, j = i+1, j-1 {
		b.out[i], b.out[j] = b.out[j], b.out[i]
	}
	return b.out
}

// selectHeuristic is Malkov's Algorithm 4 over the in-RAM vectors — identical to
// searcher.selectNeighborsHeuristic. cands must be closest-first.
func (b *ramBuilder) selectHeuristic(cands []candidate, m int) []candidate {
	if len(cands) <= m {
		return cands
	}
	b.sel = b.sel[:0]
	for ci := range cands {
		if len(b.sel) >= m {
			break
		}
		c := cands[ci]
		cv := b.vec[c.label]
		keep := true
		for r := 0; r < len(b.sel); r++ {
			if b.ix.dist(cv, b.vec[b.sel[r].label]) < c.dist {
				keep = false
				break
			}
		}
		if keep {
			b.sel = append(b.sel, c)
		}
	}
	return b.sel
}

// addNeighbor links b into a's neighbour list at layer, pruning via the heuristic
// when full — identical to Index.addNeighbor but over RAM.
func (b *ramBuilder) addNeighbor(a, newID uint32, layer int32) {
	if layer > b.levels[a] {
		return
	}
	list := b.nbrs[a][layer]
	for _, x := range list {
		if x == newID {
			return
		}
	}
	capn := b.ix.maxConn(layer)
	if len(list) < capn {
		b.nbrs[a][layer] = append(list, newID)
		return
	}
	av := b.vec[a]
	b.naCand = b.naCand[:0]
	for _, x := range list {
		b.naCand = append(b.naCand, candidate{b.ix.dist(av, b.vec[x]), x})
	}
	b.naCand = append(b.naCand, candidate{b.ix.dist(av, b.vec[newID]), newID})
	insertionSortCands(b.naCand)
	kept := b.selectHeuristic(b.naCand, capn)
	out := list[:0]
	for _, c := range kept {
		out = append(out, c.label)
	}
	b.nbrs[a][layer] = out
}
