package vivf

import (
	"slices"

	"github.com/viterin/vek/vek32"
)

// normalize returns a unit-length copy of v (zero vector copied through).
func normalize(v []float32) []float32 {
	out := make([]float32, len(v))
	n := vek32.Norm(v)
	if n == 0 {
		copy(out, v)
		return out
	}
	return vek32.MulNumber_Into(out, v, 1/n)
}

// sqL2 is the scalar squared-L2 distance, used for the small PQ subspaces (dim
// 4–24) where a SIMD call's overhead outweighs its benefit.
func sqL2(a, b []float32) float32 {
	var s float32
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return s
}

// nearestSmall returns the index of the centroid closest (sqL2) to x among a PQ
// sub-codebook.
func nearestSmall(x []float32, cents [][]float32) int {
	best := 0
	bestD := sqL2(x, cents[0])
	for c := 1; c < len(cents); c++ {
		d := sqL2(x, cents[c])
		if d < bestD {
			bestD = d
			best = c
		}
	}
	return best
}

// topNCells returns the indices of the n coarse centroids nearest (L2) to q.
func topNCells(q []float32, coarse [][]float32, n int) []int {
	type cd struct {
		idx  int
		dist float32
	}
	cs := make([]cd, len(coarse))
	for i, c := range coarse {
		cs[i] = cd{i, vek32.Distance(q, c)}
	}
	slices.SortFunc(cs, func(a, b cd) int { return cmpF32(a.dist, b.dist) })
	if n > len(cs) {
		n = len(cs)
	}
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = cs[i].idx
	}
	return out
}

// partialSortByDist orders s ascending by key. (A full generic sort — not
// reflection-based — is fine here: candidate sets are a few thousand elements.)
func partialSortByDist[T any](s []T, key func(T) float32, n int) {
	if n <= 0 {
		return
	}
	slices.SortFunc(s, func(a, b T) int { return cmpF32(key(a), key(b)) })
}

func cmpF32(a, b float32) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
