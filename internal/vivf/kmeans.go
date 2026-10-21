package vivf

import (
	"math"
	"math/rand"
	"runtime"

	"github.com/anyproto/any-store/v2/internal/buildpool"
	"github.com/anyproto/any-store/v2/internal/simd"
)

// kmeans runs Lloyd's algorithm on data, returning k centroids and the final
// assignment: assign[i] is the centroid nearest data[i] among the returned
// centroids, or -1 when that is not unique (see nearestUnique and place), so a
// build that places each point in its one nearest cell reuses it instead of
// scoring every centroid again. Assignment (the O(n·k·dim) cost) is
// parallelised on the build budget; centroid recomputation is a cheap
// single-threaded reduction. Empty clusters are reseeded to a random point so k
// centroids are always populated. When kpp is set, centroids are seeded with
// k-means++ (D² sampling) instead of random points.
func kmeans(data [][]float32, k, iters int, seed int64, kpp bool) (cents [][]float32, assign []int32) {
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

	assign = make([]int32, n)
	for it := 0; it < iters; it++ {
		assignNearest(data, cents, assign, false)

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
	assignNearest(data, cents, assign, true)
	return cents, assign
}

// kmeansppInit seeds k centroids with k-means++: each new centroid is sampled
// with probability proportional to its squared distance to the nearest centroid
// already chosen. The per-point d² update is parallelised on the build budget;
// it sums d² per parallelRange chunk, then the chunk sums in chunk order.
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
	chunks, size := rangeChunks(n)
	part := make([]float64, chunks)
	updateD2 := func(c []float32) (sum float64) {
		buildpool.Current().Run(chunks, func(t int) {
			var s float64
			for i := t * size; i < min((t+1)*size, n); i++ {
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
// data[i], parallelised over the point range. With markTies, a point whose
// nearest centroid is not unique gets -1 instead (see nearestUnique).
func assignNearest(data [][]float32, cents [][]float32, assign []int32, markTies bool) {
	parallelRange(len(data), func(lo, hi int) {
		if markTies {
			for i := lo; i < hi; i++ {
				assign[i] = int32(nearestUnique(data[i], cents))
			}
			return
		}
		for i := lo; i < hi; i++ {
			assign[i] = int32(nearest(data[i], cents))
		}
	})
}

// parallelRange runs fn over [0, n) in rangeChunks chunks, on workers drawn
// from the process-wide build budget (internal/buildpool), and returns once
// all are done. Each index is handled by exactly one call, and the chunks
// depend only on n and GOMAXPROCS, so a result written per index, or summed
// per chunk and then in chunk order, does not depend on the budget or on
// scheduling.
func parallelRange(n int, fn func(lo, hi int)) {
	chunks, size := rangeChunks(n)
	buildpool.Current().Run(chunks, func(t int) {
		fn(t*size, min((t+1)*size, n))
	})
}

// rangeChunks splits n items into chunks of size items (the last one shorter):
// one per GOMAXPROCS, at least one item each.
func rangeChunks(n int) (chunks, size int) {
	if n <= 0 {
		return 0, 0
	}
	procs := runtime.GOMAXPROCS(0)
	size = (n + procs - 1) / procs
	return (n + size - 1) / size, size
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

// nearestUnique is nearest for the final assignment: the same index, or -1 when
// that nearest is not unique — another centroid exactly as close, or a NaN
// distance, which orders against nothing.
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
