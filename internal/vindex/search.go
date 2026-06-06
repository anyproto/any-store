package vindex

import (
	"fmt"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// Candidate is one ANN result: the document id and its distance to the query.
type Candidate struct {
	DocID    []byte
	Distance float32
}

// SearchCandidates runs the ANN beam search and returns up to ef live
// candidates with their distances. Unlike Search it does NOT rank/truncate to k
// — the query pipeline's sort+limit own the final ordering ("drop heap"). The
// returned order is whatever the layer search produced (closest-first); callers
// must not rely on it.
func (ix *Index) SearchCandidates(rtx *btree.ReadTx, query []float32, ef int) ([]Candidate, error) {
	if len(query) != ix.dim {
		return nil, fmt.Errorf("vindex: dim mismatch: got %d want %d", len(query), ix.dim)
	}
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return nil, err
	}
	if !mt.hasEntry {
		return nil, nil
	}
	if ef <= 0 {
		ef = ix.efS
	}
	s := ix.getSearcher(rtx, query)
	defer ix.putSearcher(s)
	s.checkDeleted = mt.deletedCount > 0
	if ix.hybrid {
		if s.l0, err = ix.layer0Mirror(rtx, mt); err != nil {
			return nil, err
		}
	}
	epn := mt.entryLabel
	for lc := mt.topLayer; lc > 0; lc-- {
		if epn, err = s.greedyClosest(epn, lc); err != nil {
			return nil, err
		}
	}
	found, err := s.searchLayer(epn, ef, 0)
	if err != nil {
		return nil, err
	}
	out := make([]Candidate, 0, len(found))
	var kbuf []byte
	for _, c := range found {
		docID, derr := rtx.Get(ix.vlbl, labelKey(kbuf, c.label))
		if derr != nil {
			return nil, derr
		}
		out = append(out, Candidate{DocID: append([]byte(nil), docID...), Distance: c.dist})
	}
	return out, nil
}

// EfSearch returns the index's default query-time candidate-list size.
func (ix *Index) EfSearch() int { return ix.efS }

// searcher carries the reusable per-operation buffers for traversing the
// btree-resident graph. One searcher serves a single Insert or Search call
// (not safe to share across goroutines). Vectors are read zero-copy into
// float32-backed scratch; adjacency into a reused byte buffer.
type searcher struct {
	ix    *Index
	rtx   *btree.ReadTx
	query []float32

	// checkDeleted gates the per-visited-node tombstone read. When the index has
	// no tombstones (the common case, and always during a fresh build) every node
	// is live, so we skip that extra adjacency read entirely.
	checkDeleted bool

	// l0 is the RAM layer-0 mirror for hybrid-mode READ searches; when set,
	// layer-0 neighbour and deleted lookups are served from it instead of the
	// btree. nil on the write path (Insert), which must see the in-progress graph.
	l0 *l0Mirror

	vbuf  []byte
	vf    []float32 // aliases vbuf — vector A (reused each read)
	vbuf2 []byte
	vf2   []float32 // aliases vbuf2 — vector B (held during pairwise prune)
	qbuf  []byte    // quantized-record read buffer for vf (int8 mode)
	qbuf2 []byte    // quantized-record read buffer for vf2 (int8 mode)

	adjbuf  []byte
	nbrs    []uint32
	keyBuf  []byte
	visited map[uint32]struct{}

	cand cheap
	res  cheap
	out  []candidate

	// write-path scratch reused by addNeighbor (insert connect phase, which runs
	// after searchLayer so these don't overlap with the read buffers above).
	naAdj []byte     // a's adjacency record read buffer
	naEnc []byte     // re-encoded adjacency write buffer
	naDec [][]uint32 // decoded per-layer neighbour lists (reused)

	// neighbour-selection-heuristic scratch.
	sel      []candidate // selected diverse neighbours (reused)
	naCand   []candidate // candidate set for an addNeighbor prune (reused)
	aVecBuf  []float32   // stable copy of the centre vector during a prune
	keptVecs []float32   // RAM cache of kept-neighbour vectors (m*dim, reused)
}

func (ix *Index) newSearcher(rtx *btree.ReadTx, query []float32) *searcher {
	vf := make([]float32, ix.dim)
	vf2 := make([]float32, ix.dim)
	return &searcher{
		ix:      ix,
		rtx:     rtx,
		query:   query,
		vf:      vf,
		vbuf:    f32bytes(vf),
		vf2:     vf2,
		vbuf2:   f32bytes(vf2),
		visited: make(map[uint32]struct{}, 256),
	}
}

// vecOf reads label's vector into the primary scratch (valid until the next
// vecOf call).
func (s *searcher) vecOf(label uint32) ([]float32, error) {
	s.keyBuf = labelKey(s.keyBuf, label)
	if s.ix.quant != QuantNone {
		s.qbuf, _ = s.rtx.AppendValue(s.ix.vvec, s.keyBuf, s.qbuf[:0])
		if v, ok := decodeVecInto(s.qbuf, s.ix.dim, s.ix.quant, s.vf); ok {
			return v, nil
		}
		return nil, fmt.Errorf("vindex: bad quantized vector record len %d", len(s.qbuf))
	}
	b, err := s.rtx.AppendValue(s.ix.vvec, s.keyBuf, s.vbuf[:0])
	if err != nil {
		return nil, err
	}
	if len(b) != s.ix.dim*4 {
		return nil, fmt.Errorf("vindex: bad vector record len %d", len(b))
	}
	if &b[0] == &s.vbuf[0] {
		return s.vf, nil
	}
	return bytesAsF32(b, s.ix.dim), nil
}

// vec2Of reads label's vector into the secondary scratch (held across a prune).
func (s *searcher) vec2Of(label uint32) ([]float32, error) {
	s.keyBuf = labelKey(s.keyBuf, label)
	if s.ix.quant != QuantNone {
		s.qbuf2, _ = s.rtx.AppendValue(s.ix.vvec, s.keyBuf, s.qbuf2[:0])
		if v, ok := decodeVecInto(s.qbuf2, s.ix.dim, s.ix.quant, s.vf2); ok {
			return v, nil
		}
		return nil, fmt.Errorf("vindex: bad quantized vector record len %d", len(s.qbuf2))
	}
	b, err := s.rtx.AppendValue(s.ix.vvec, s.keyBuf, s.vbuf2[:0])
	if err != nil {
		return nil, err
	}
	if len(b) != s.ix.dim*4 {
		return nil, fmt.Errorf("vindex: bad vector record len %d", len(b))
	}
	if &b[0] == &s.vbuf2[0] {
		return s.vf2, nil
	}
	return bytesAsF32(b, s.ix.dim), nil
}

func (s *searcher) adjBytesOf(label uint32) ([]byte, error) {
	s.keyBuf = labelKey(s.keyBuf, label)
	b, err := s.rtx.AppendValue(s.ix.vadj, s.keyBuf, s.adjbuf[:0])
	if err != nil {
		return nil, err
	}
	s.adjbuf = b
	return b, nil
}

func (s *searcher) isDeleted(label uint32) (bool, error) {
	b, err := s.adjBytesOf(label)
	if err != nil {
		return false, err
	}
	_, deleted, err := adjHeader(b)
	return deleted, err
}

// neighborsAt returns label's neighbours at layer, served from the RAM mirror
// when available (hybrid read path, layer 0) else from the btree.
func (s *searcher) neighborsAt(label uint32, layer int32) ([]uint32, error) {
	if s.l0 != nil && layer == 0 {
		return s.l0.neighbors(label), nil
	}
	adjB, err := s.adjBytesOf(label)
	if err != nil {
		return nil, err
	}
	s.nbrs, err = adjNeighbors(adjB, layer, s.nbrs)
	return s.nbrs, err
}

// isDeletedAt reports whether label is tombstoned, from the mirror on the hybrid
// layer-0 read path, else from the btree.
func (s *searcher) isDeletedAt(label uint32, layer int32) (bool, error) {
	if s.l0 != nil && layer == 0 {
		return s.l0.isDeleted(label), nil
	}
	return s.isDeleted(label)
}

// selectNeighborsHeuristic picks up to m diverse neighbours from cands (which
// must be sorted closest-first, with cand.dist = distance to the centre node)
// using Malkov's Algorithm 4: keep a candidate only if it is closer to the
// centre than to every already-kept neighbour. Plain "keep the m closest"
// instead fills a node's edges with same-cluster points and keeps no long-range
// bridges, fragmenting the graph on clustered (real-embedding) data so recall
// plateaus far below 1 regardless of ef. The result aliases s.sel (reused) —
// the caller must copy out the labels before the next heuristic call.
//
// Buffers: candidate vectors are read into the secondary scratch (vec2Of, vf2),
// kept-neighbour vectors into the primary scratch (vecOf, vf); the two never
// alias, and any centre vector the caller needs must live in its own buffer.
func (s *searcher) selectNeighborsHeuristic(cands []candidate, m int) ([]candidate, error) {
	if len(cands) <= m {
		return cands, nil
	}
	// Cache kept-neighbour vectors in a reused RAM slab (m*dim floats, bounded by
	// graph degree — not N). Each candidate is read from the btree once (vec2Of);
	// the inner "closer to a kept neighbour?" check then runs entirely in RAM,
	// instead of re-reading every kept vector from the btree per candidate. This
	// removes the dominant cost of the heuristic (it was ~64% of insert time, all
	// btree reads). Distances use the same SIMD kernel; results are identical.
	dim := s.ix.dim
	if cap(s.keptVecs) < m*dim {
		s.keptVecs = make([]float32, m*dim)
	}
	kept := s.keptVecs[:0]
	s.sel = s.sel[:0]
	for ci := range cands {
		if len(s.sel) >= m {
			break
		}
		c := cands[ci]
		cv, err := s.vec2Of(c.label)
		if err != nil {
			return nil, err
		}
		keep := true
		for r := 0; r < len(s.sel); r++ {
			rv := kept[r*dim : r*dim+dim]
			if s.ix.dist(cv, rv) < c.dist {
				keep = false
				break
			}
		}
		if keep {
			kept = append(kept, cv...) // copy into the slab (cv is reused scratch)
			s.sel = append(s.sel, c)
		}
	}
	return s.sel, nil
}

// insertionSortCands sorts candidates ascending by distance, in place and
// allocation-free. Used on the tiny addNeighbor candidate set (<= maxConn+1),
// where insertion sort beats a general sort and avoids reflection (sort.Slice).
func insertionSortCands(c []candidate) {
	for i := 1; i < len(c); i++ {
		x := c[i]
		j := i - 1
		for j >= 0 && c[j].dist > x.dist {
			c[j+1] = c[j]
			j--
		}
		c[j+1] = x
	}
}

// greedyClosest walks one layer toward the query, returning the closest label.
func (s *searcher) greedyClosest(ep uint32, layer int32) (uint32, error) {
	best := ep
	bv, err := s.vecOf(best)
	if err != nil {
		return 0, err
	}
	bestDist := s.ix.dist(s.query, bv)
	for {
		adjB, err := s.adjBytesOf(best)
		if err != nil {
			return 0, err
		}
		s.nbrs, err = adjNeighbors(adjB, layer, s.nbrs)
		if err != nil {
			return 0, err
		}
		improved := false
		for _, nb := range s.nbrs {
			nv, err := s.vecOf(nb)
			if err != nil {
				return 0, err
			}
			if d := s.ix.dist(s.query, nv); d < bestDist {
				bestDist, best, improved = d, nb, true
			}
		}
		if !improved {
			return best, nil
		}
	}
}

// searchLayer is the two-heap HNSW layer search. Deleted nodes are still
// expanded (they route) but never enter the result set. Returns results
// closest-first.
func (s *searcher) searchLayer(ep uint32, ef int, layer int32) ([]candidate, error) {
	clear(s.visited)
	s.cand.reset(false)
	s.res.reset(true)

	ev, err := s.vecOf(ep)
	if err != nil {
		return nil, err
	}
	d0 := s.ix.dist(s.query, ev)
	s.visited[ep] = struct{}{}
	s.cand.push(candidate{d0, ep})
	epDel := false
	if s.checkDeleted {
		if epDel, err = s.isDeletedAt(ep, layer); err != nil {
			return nil, err
		}
	}
	if !epDel {
		s.res.push(candidate{d0, ep})
	}

	for s.cand.len() > 0 {
		cur := s.cand.pop()
		if s.res.len() >= ef && cur.dist > s.res.peek().dist {
			break
		}
		// nbrs may alias the read-only mirror slice (hybrid layer 0) or the
		// reused s.nbrs buffer (btree); iterate it read-only either way.
		nbrs, nerr := s.neighborsAt(cur.label, layer)
		if nerr != nil {
			return nil, nerr
		}
		for _, nb := range nbrs {
			if _, seen := s.visited[nb]; seen {
				continue
			}
			s.visited[nb] = struct{}{}
			nv, err := s.vecOf(nb)
			if err != nil {
				return nil, err
			}
			d := s.ix.dist(s.query, nv)
			if s.res.len() >= ef && d >= s.res.peek().dist {
				continue
			}
			s.cand.push(candidate{d, nb})
			if s.checkDeleted {
				del, derr := s.isDeletedAt(nb, layer)
				if derr != nil {
					return nil, derr
				}
				if del {
					continue
				}
			}
			s.res.push(candidate{d, nb})
			if s.res.len() > ef {
				s.res.pop()
			}
		}
	}

	s.out = s.out[:0]
	for s.res.len() > 0 {
		s.out = append(s.out, s.res.pop())
	}
	for i, j := 0, len(s.out)-1; i < j; i, j = i+1, j-1 {
		s.out[i], s.out[j] = s.out[j], s.out[i]
	}
	return s.out, nil
}
