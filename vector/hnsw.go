package vector

import (
	"math"
	"math/rand"
	"slices"
)

// HNSW is a map-based, fully in-memory Hierarchical Navigable Small World
// graph. It is a close adaptation of github.com/coder/hnsw with the generic key
// fixed to uint64 and the distance function pluggable. Adjacency is stored as a
// map[uint64]*hnswNode per node, which makes deletes cheap but costs an
// allocation per node and chases pointers during search.
//
// This is the "pure in-memory, idiomatic Go" baseline that FlatHNSW is measured
// against.
type HNSW struct {
	// M is the maximum number of neighbours kept per node per layer.
	M int
	// Ml is the level-generation factor (layer i is ~Ml× the size of i-1).
	Ml float64
	// EfSearch is the size of the dynamic candidate list during search.
	EfSearch int

	dim  int
	dist DistanceFunc
	rng  *rand.Rand

	layers []*hnswLayer
}

type hnswNode struct {
	key       uint64
	vec       []float32
	neighbors map[uint64]*hnswNode
}

type hnswLayer struct {
	nodes map[uint64]*hnswNode
}

func (l *hnswLayer) entry() *hnswNode {
	for _, n := range l.nodes {
		return n
	}
	return nil
}

// NewHNSW builds an empty graph. seed makes level generation deterministic for
// reproducible benchmarks; pass 0 for a fixed default seed.
func NewHNSW(dim int, m Metric, seed int64) *HNSW {
	return &HNSW{
		M:        16,
		Ml:       0.25,
		EfSearch: 20,
		dim:      dim,
		dist:     m.DistanceFor(),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

// Len returns the number of vectors in the base layer.
func (h *HNSW) Len() int {
	if len(h.layers) == 0 {
		return 0
	}
	return len(h.layers[0].nodes)
}

func maxLevel(ml float64, n int) int {
	if n == 0 {
		return 1
	}
	return int(math.Round(math.Log(float64(n))/math.Log(1/ml))) + 1
}

func (h *HNSW) randomLevel() int {
	max := 1
	if len(h.layers) > 0 {
		max = maxLevel(h.Ml, len(h.layers[0].nodes))
	}
	for level := 0; level < max; level++ {
		if h.rng.Float64() > h.Ml {
			return level
		}
	}
	return max
}

// hnswCandidate is a (node, distance) pair used in the search frontier.
type hnswCandidate struct {
	node *hnswNode
	dist float32
}

// searchLayer is a greedy best-first search within a single layer starting from
// entry, returning up to ef closest candidates (sorted closest-first). Mirrors
// coder/hnsw's layerNode.search but with explicit slices instead of a heap —
// for the modest ef values used here a sorted slice is simpler and competitive.
func (h *HNSW) searchLayer(entry *hnswNode, target []float32, ef int) []hnswCandidate {
	visited := map[uint64]bool{entry.key: true}
	start := hnswCandidate{entry, h.dist(entry.vec, target)}
	// frontier: nodes still to expand (min-dist first). result: best ef seen.
	frontier := []hnswCandidate{start}
	result := []hnswCandidate{start}

	for len(frontier) > 0 {
		// pop closest from frontier
		bestIdx := 0
		for i := 1; i < len(frontier); i++ {
			if frontier[i].dist < frontier[bestIdx].dist {
				bestIdx = i
			}
		}
		cur := frontier[bestIdx]
		frontier = slices.Delete(frontier, bestIdx, bestIdx+1)

		// stop if the closest frontier node is worse than the worst result
		if len(result) >= ef && cur.dist > result[len(result)-1].dist {
			break
		}

		// iterate neighbours deterministically (sorted) for reproducibility
		neighborKeys := make([]uint64, 0, len(cur.node.neighbors))
		for k := range cur.node.neighbors {
			neighborKeys = append(neighborKeys, k)
		}
		slices.Sort(neighborKeys)
		for _, nk := range neighborKeys {
			if visited[nk] {
				continue
			}
			visited[nk] = true
			nb := cur.node.neighbors[nk]
			d := h.dist(nb.vec, target)
			if len(result) < ef || d < result[len(result)-1].dist {
				cand := hnswCandidate{nb, d}
				frontier = append(frontier, cand)
				result = insertSorted(result, cand, ef)
			}
		}
	}
	return result
}

// insertSorted inserts c into the closest-first slice res, capping length at ef.
func insertSorted(res []hnswCandidate, c hnswCandidate, ef int) []hnswCandidate {
	idx := sortSearch(len(res), func(i int) bool { return res[i].dist >= c.dist })
	res = slices.Insert(res, idx, c)
	if len(res) > ef {
		res = res[:ef]
	}
	return res
}

// sortSearch is sort.Search inlined to avoid importing sort here.
func sortSearch(n int, f func(int) bool) int {
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if !f(mid) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

func (n *hnswNode) addNeighbor(o *hnswNode, m int, dist DistanceFunc) {
	if n.neighbors == nil {
		n.neighbors = make(map[uint64]*hnswNode, m)
	}
	n.neighbors[o.key] = o
	if len(n.neighbors) <= m {
		return
	}
	// drop the farthest neighbour to keep the degree bounded
	var worst *hnswNode
	worstDist := float32(math.Inf(-1))
	for _, nb := range n.neighbors {
		if d := dist(nb.vec, n.vec); worst == nil || d > worstDist {
			worstDist, worst = d, nb
		}
	}
	delete(n.neighbors, worst.key)
	delete(worst.neighbors, n.key)
}

// Add inserts (or replaces) a vector.
func (h *HNSW) Add(key uint64, vec []float32) {
	if len(vec) != h.dim {
		panic("vector: dimension mismatch")
	}
	// Own the vector (the caller may reuse its slice). The flat index copies
	// into its arena for the same reason, so both pay this cost.
	vec = append([]float32(nil), vec...)

	insertLevel := h.randomLevel()
	for insertLevel >= len(h.layers) {
		h.layers = append(h.layers, &hnswLayer{nodes: map[uint64]*hnswNode{}})
	}

	var elevator *uint64
	for i := len(h.layers) - 1; i >= 0; i-- {
		layer := h.layers[i]
		newNode := &hnswNode{key: key, vec: vec}

		if layer.entry() == nil {
			layer.nodes[key] = newNode
			continue
		}

		searchPoint := layer.entry()
		if elevator != nil {
			if e, ok := layer.nodes[*elevator]; ok {
				searchPoint = e
			}
		}

		neighborhood := h.searchLayer(searchPoint, vec, h.EfSearch)
		e := neighborhood[0].node.key
		elevator = &e

		if insertLevel >= i {
			layer.nodes[key] = newNode
			for _, c := range neighborhood {
				if len(newNode.neighbors) >= h.M {
					break
				}
				c.node.addNeighbor(newNode, h.M, h.dist)
				newNode.addNeighbor(c.node, h.M, h.dist)
			}
		}
	}
}

// Search returns the k nearest neighbours, closest-first.
func (h *HNSW) Search(query []float32, k int) []SearchResult {
	if len(h.layers) == 0 || k <= 0 {
		return nil
	}
	var elevator *uint64
	for layer := len(h.layers) - 1; layer >= 0; layer-- {
		searchPoint := h.layers[layer].entry()
		if elevator != nil {
			if e, ok := h.layers[layer].nodes[*elevator]; ok {
				searchPoint = e
			}
		}
		if layer > 0 {
			nodes := h.searchLayer(searchPoint, query, h.EfSearch)
			e := nodes[0].node.key
			elevator = &e
			continue
		}
		ef := h.EfSearch
		if k > ef {
			ef = k
		}
		nodes := h.searchLayer(searchPoint, query, ef)
		if k > len(nodes) {
			k = len(nodes)
		}
		out := make([]SearchResult, k)
		for i := 0; i < k; i++ {
			out[i] = SearchResult{Key: nodes[i].node.key, Distance: nodes[i].dist}
		}
		return out
	}
	return nil
}
