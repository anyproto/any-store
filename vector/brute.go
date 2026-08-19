package vector

import (
	"cmp"
	"slices"
)

// SearchResult is one hit returned by a search: the caller's key and its
// distance to the query (smaller = closer).
type SearchResult struct {
	Key      uint64
	Distance float32
}

// Brute is an exact k-NN index: it keeps every vector in one contiguous slab
// and scans all of them per query. It is O(n·dim) per search but always returns
// the true nearest neighbours, so it doubles as the ground truth for measuring
// the recall of the approximate (HNSW) indexes.
//
// The contiguous slab (vectors stored back-to-back in a single []float32) is
// the same arena layout used by FlatHNSW — it keeps distance kernels reading
// sequential memory and avoids one allocation per vector.
type Brute struct {
	dim     int
	dist    DistanceFunc
	keys    []uint64
	vectors []float32 // len == len(keys)*dim, row-major
}

// NewBrute creates an exact index for dim-dimensional vectors under metric m.
func NewBrute(dim int, m Metric) *Brute {
	return &Brute{dim: dim, dist: m.DistanceFor()}
}

// Len returns the number of indexed vectors.
func (b *Brute) Len() int { return len(b.keys) }

// Add appends a vector. It does not deduplicate keys (the brute index is a
// benchmark/ground-truth tool, not a primary store).
func (b *Brute) Add(key uint64, vec []float32) {
	if len(vec) != b.dim {
		panic("vector: dimension mismatch")
	}
	b.keys = append(b.keys, key)
	b.vectors = append(b.vectors, vec...)
}

func (b *Brute) vectorAt(i int) []float32 {
	return b.vectors[i*b.dim : (i+1)*b.dim]
}

// Search returns the k nearest neighbours to query, ordered closest-first.
func (b *Brute) Search(query []float32, k int) []SearchResult {
	if k <= 0 || len(b.keys) == 0 {
		return nil
	}
	results := make([]SearchResult, len(b.keys))
	for i := range b.keys {
		results[i] = SearchResult{Key: b.keys[i], Distance: b.dist(query, b.vectorAt(i))}
	}
	slices.SortFunc(results, func(a, b SearchResult) int {
		return cmp.Compare(a.Distance, b.Distance)
	})
	if k > len(results) {
		k = len(results)
	}
	return results[:k]
}
