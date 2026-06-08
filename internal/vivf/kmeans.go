// Package vivf is a Phase-0, in-RAM prototype of a btree-native IVF-PQ vector
// index (see vector/RESEARCH_IVFPQ_BTREE.md). It owns NO btree state — its sole
// purpose is to validate recall parity with the current HNSW index on real
// embeddings before any storage work. The algorithm here (coarse IVF quantizer +
// product quantization of residuals + asymmetric distance + exact re-rank) is the
// same one the eventual btree-resident index would use; only the container differs.
package vivf

import (
	"math"
	"math/rand"
	"runtime"
	"sync"

	"github.com/viterin/vek/vek32"
)

// kmeans runs Lloyd's algorithm on data, returning k centroids and the per-point
// assignment. Assignment (the O(n·k·dim) cost) is parallelised across cores;
// centroid recomputation is a cheap single-threaded reduction. Empty clusters are
// reseeded to a random point so k centroids are always populated. When kpp is set,
// centroids are seeded with k-means++ (D² sampling) instead of random points.
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
	assignNearest(data, cents, assign)
	return cents, assign
}

// kmeansppInit seeds k centroids with k-means++: each new centroid is sampled
// with probability proportional to its squared distance to the nearest centroid
// already chosen. The per-point d² update is parallelised across cores.
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
	updateD2 := func(c []float32) (sum float64) {
		nThreads := runtime.GOMAXPROCS(0)
		chunk := (n + nThreads - 1) / nThreads
		part := make([]float64, nThreads)
		var wg sync.WaitGroup
		for t := 0; t < nThreads; t++ {
			lo := t * chunk
			if lo >= n {
				break
			}
			hi := lo + chunk
			if hi > n {
				hi = n
			}
			wg.Add(1)
			go func(t, lo, hi int) {
				defer wg.Done()
				var s float64
				for i := lo; i < hi; i++ {
					dist := float64(vek32.Distance(data[i], c))
					dd := dist * dist
					if dd < d2[i] {
						d2[i] = dd
					}
					s += d2[i]
				}
				part[t] = s
			}(t, lo, hi)
		}
		wg.Wait()
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
	nThreads := runtime.GOMAXPROCS(0)
	n := len(data)
	if nThreads > n {
		nThreads = 1
	}
	chunk := (n + nThreads - 1) / nThreads
	var wg sync.WaitGroup
	for t := 0; t < nThreads; t++ {
		lo := t * chunk
		if lo >= n {
			break
		}
		hi := lo + chunk
		if hi > n {
			hi = n
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				assign[i] = int32(nearest(data[i], cents))
			}
		}(lo, hi)
	}
	wg.Wait()
}

// nearest returns the index of the centroid closest (L2) to x.
func nearest(x []float32, cents [][]float32) int {
	best := 0
	bestD := vek32.Distance(x, cents[0])
	for c := 1; c < len(cents); c++ {
		d := vek32.Distance(x, cents[c])
		if d < bestD {
			bestD = d
			best = c
		}
	}
	return best
}
