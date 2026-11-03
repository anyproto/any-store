package vivf

import (
	"math"
	"math/rand"
	"runtime"

	"github.com/anyproto/any-store/v2/internal/buildpool"
	"github.com/anyproto/any-store/v2/internal/simd"
)

// kmeans runs Lloyd's algorithm on data, returning k centroids. Assignment (the
// O(n·k·dim) cost) is parallelised on the build budget; centroid recomputation
// is a cheap single-threaded reduction. Empty clusters are reseeded to a random
// point so k centroids are always populated. When kpp is set, centroids are
// seeded with k-means++ (D² sampling) instead of random points.
func kmeans(data [][]float32, k, iters int, seed int64, kpp bool) (cents [][]float32) {
	n := len(data)
	dim := len(data[0])
	rng := rand.New(rand.NewSource(seed))

	if kpp {
		cents = kmeansppInit(data, k, rng)
	} else {
		cents = make([][]float32, k)
		perm := rng.Perm(n) // k distinct random points (repeats only if n < k)
		for i := 0; i < k; i++ {
			c := make([]float32, dim)
			copy(c, data[perm[i%n]])
			cents[i] = c
		}
	}

	assign := make([]int32, n)
	for it := 0; it < iters; it++ {
		assignNearest(data, cents, assign)

		// Recompute centroids as the mean of their members.
		sums := make([][]float64, k)
		for i := range sums {
			sums[i] = make([]float64, dim)
		}
		counts := make([]int, k)
		for i, x := range data {
			a := assign[i]
			counts[a]++
			s := sums[a]
			for d, v := range x {
				s[d] += float64(v)
			}
		}
		for c := 0; c < k; c++ {
			if counts[c] == 0 {
				// Empty cluster: reseed to a random point to keep k live centroids.
				copy(cents[c], data[rng.Intn(n)])
				continue
			}
			inv := 1.0 / float64(counts[c])
			dst, s := cents[c], sums[c]
			for d := 0; d < dim; d++ {
				dst[d] = float32(s[d] * inv)
			}
		}
	}
	return cents
}

// kmeansppInit seeds k centroids with k-means++: each new centroid is sampled
// with probability proportional to its squared distance to the nearest centroid
// already chosen. The per-point d² update is parallelised on the build budget.
// It sums d² per chunk of ⌈n/GOMAXPROCS⌉ points, then the chunk sums in chunk
// order, as earlier releases do: the chunking is part of the result.
func kmeansppInit(data [][]float32, k int, rng *rand.Rand) [][]float32 {
	n := len(data)
	dim := len(data[0])
	cents := make([][]float32, 0, k)

	first := make([]float32, dim)
	copy(first, data[rng.Intn(n)])
	cents = append(cents, first)

	d2 := make([]float64, n)
	for i := range d2 {
		d2[i] = math.Inf(1)
	}
	size := perProcChunk(n)
	part := make([]float64, (n+size-1)/size)
	updateD2 := func(c []float32) (sum float64) {
		runChunks(n, size, func(t, lo, hi int) {
			var s float64
			for i := lo; i < hi; i++ {
				dist := float64(simd.Distance(data[i], c))
				dd := dist * dist
				if dd < d2[i] {
					d2[i] = dd
				}
				s += d2[i]
			}
			part[t] = s
		})
		for _, s := range part {
			sum += s
		}
		return sum
	}

	sum := updateD2(cents[0])
	for len(cents) < k {
		target := rng.Float64() * sum
		pick := n - 1
		var acc float64
		for i := 0; i < n; i++ {
			acc += d2[i]
			if acc >= target {
				pick = i
				break
			}
		}
		c := make([]float32, dim)
		copy(c, data[pick])
		cents = append(cents, c)
		sum = updateD2(c)
	}
	return cents
}

// assignNearest fills assign[i] with the index of the centroid closest (L2) to
// data[i], parallelised over the point range.
func assignNearest(data [][]float32, cents [][]float32, assign []int32) {
	parallelRange(len(data), func(lo, hi int) {
		for i := lo; i < hi; i++ {
			assign[i] = int32(nearest(data[i], cents))
		}
	})
}

// assignUnique returns each point's nearest centroid, as assignNearest, but
// -1 for a point whose nearest centroid is not unique (see nearestUnique).
func assignUnique(data [][]float32, cents [][]float32) []int32 {
	assign := make([]int32, len(data))
	parallelRange(len(data), func(lo, hi int) {
		for i := lo; i < hi; i++ {
			assign[i] = int32(nearestUnique(data[i], cents))
		}
	})
	return assign
}

// minParallelChunk is the fewest items parallelRange gives a worker, so a
// small phase runs on fewer goroutines, down to one, instead of one per item.
const minParallelChunk = 64

// parallelRange runs fn over [0, n) in chunks of ⌈n/GOMAXPROCS⌉ items, at
// least minParallelChunk, on the process-wide build budget (internal/buildpool),
// and returns once all are done. Each index is handled by exactly one call;
// it is for phases whose result is written per index, so the chunking does
// not show in it, nor do the budget and the scheduling.
func parallelRange(n int, fn func(lo, hi int)) {
	runChunks(n, max(perProcChunk(n), minParallelChunk), func(_, lo, hi int) { fn(lo, hi) })
}

// runChunks calls fn(t, lo, hi) for each chunk t = [lo, hi) of size items (the
// last one shorter) of [0, n), on the process-wide build budget.
func runChunks(n, size int, fn func(t, lo, hi int)) {
	if n <= 0 {
		return
	}
	buildpool.Current().Run((n+size-1)/size, func(t int) {
		fn(t, t*size, min((t+1)*size, n))
	})
}

// perProcChunk is ⌈n/GOMAXPROCS⌉: the chunk size that gives one chunk per
// GOMAXPROCS.
func perProcChunk(n int) int {
	procs := runtime.GOMAXPROCS(0)
	return max((n+procs-1)/procs, 1)
}

// nearest returns the index of the centroid closest (L2) to x.
func nearest(x []float32, cents [][]float32) int {
	best := 0
	bestD := simd.Distance(x, cents[0])
	for c := 1; c < len(cents); c++ {
		d := simd.Distance(x, cents[c])
		if d < bestD {
			bestD = d
			best = c
		}
	}
	return best
}

// nearestUnique is nearest for placement (assignUnique): the same index, or -1
// when that nearest is not unique — another centroid exactly as close, or a
// NaN distance, which orders against nothing.
func nearestUnique(x []float32, cents [][]float32) int {
	best := 0
	bestD := simd.Distance(x, cents[0])
	tie, nan := false, bestD != bestD
	for c := 1; c < len(cents); c++ {
		d := simd.Distance(x, cents[c])
		switch {
		case d < bestD:
			bestD, best, tie = d, c, false
		case d == bestD:
			tie = true
		case d != d:
			nan = true
		}
	}
	if tie || nan {
		return -1
	}
	return best
}
