package vector

import (
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randVectors(n, dim int, seed int64) ([][]float32, []uint64) {
	rng := rand.New(rand.NewSource(seed))
	vecs := make([][]float32, n)
	keys := make([]uint64, n)
	for i := range vecs {
		v := make([]float32, dim)
		for d := range v {
			v[d] = rng.Float32()*2 - 1
		}
		vecs[i] = v
		keys[i] = uint64(i + 1)
	}
	return vecs, keys
}

// recall@k of approx against the exact ground-truth set.
func recallAt(approx, truth []SearchResult) float64 {
	if len(truth) == 0 {
		return 1
	}
	want := make(map[uint64]bool, len(truth))
	for _, r := range truth {
		want[r.Key] = true
	}
	hit := 0
	for _, r := range approx {
		if want[r.Key] {
			hit++
		}
	}
	return float64(hit) / float64(len(truth))
}

func TestDistanceSIMDMatchesScalar(t *testing.T) {
	t.Logf("SIMD acceleration active: %v (CPU features via vek)", SIMD())
	rng := rand.New(rand.NewSource(1))
	for _, dim := range []int{1, 3, 8, 16, 128, 1536} {
		a := make([]float32, dim)
		b := make([]float32, dim)
		for i := range a {
			a[i] = rng.Float32()
			b[i] = rng.Float32()
		}
		assert.InDelta(t, L2DistanceScalar(a, b), L2DistanceSIMD(a, b), 1e-3, "l2 dim=%d", dim)
		assert.InDelta(t, L2DistanceUnrolled(a, b), L2DistanceSIMD(a, b), 1e-3, "l2unroll dim=%d", dim)
		assert.InDelta(t, CosineDistanceScalar(a, b), CosineDistanceSIMD(a, b), 1e-3, "cos dim=%d", dim)
		assert.InDelta(t, DotDistanceScalar(a, b), DotDistanceSIMD(a, b), 1e-3, "dot dim=%d", dim)
	}
}

func TestBruteExact(t *testing.T) {
	vecs, keys := randVectors(200, 16, 7)
	b := NewBrute(16, L2)
	for i := range vecs {
		b.Add(keys[i], vecs[i])
	}
	// nearest neighbour of an indexed vector is itself (distance 0)
	res := b.Search(vecs[42], 1)
	require.Len(t, res, 1)
	assert.Equal(t, keys[42], res[0].Key)
	assert.InDelta(t, 0, res[0].Distance, 1e-4)
}

func TestHNSWRecall(t *testing.T) {
	const (
		n   = 2000
		dim = 64
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 99)
	queries, _ := randVectors(100, dim, 1234)

	for _, m := range []Metric{L2, Cosine} {
		brute := NewBrute(dim, m)
		mapG := NewHNSW(dim, m, 42)
		mapG.EfSearch = 64
		flat := NewFlatHNSW(dim, m, 42)
		flat.EfSearch = 64
		for i := range vecs {
			brute.Add(keys[i], vecs[i])
			mapG.Add(keys[i], vecs[i])
			flat.Add(keys[i], vecs[i])
		}
		require.Equal(t, n, flat.Len())

		var mapRecall, flatRecall float64
		for _, q := range queries {
			truth := brute.Search(q, k)
			mapRecall += recallAt(mapG.Search(q, k), truth)
			flatRecall += recallAt(flat.Search(q, k), truth)
		}
		mapRecall /= float64(len(queries))
		flatRecall /= float64(len(queries))
		t.Logf("metric=%s map-HNSW recall@%d=%.3f  flat-HNSW recall@%d=%.3f", m, k, mapRecall, k, flatRecall)
		assert.Greater(t, mapRecall, 0.80, "map recall too low for %s", m)
		assert.Greater(t, flatRecall, 0.85, "flat recall too low for %s", m)
	}
}

func TestFlatHNSWConcurrentSearch(t *testing.T) {
	const dim = 32
	vecs, keys := randVectors(1000, dim, 5)
	flat := NewFlatHNSW(dim, L2, 1)
	flat.EfSearch = 32
	for i := range vecs {
		flat.Add(keys[i], vecs[i])
	}
	queries, _ := randVectors(200, dim, 9)

	// Concurrent readers must not race on the pooled scratch / arenas.
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			for i := 0; i < len(queries); i++ {
				res := flat.Search(queries[(i+off)%len(queries)], 10)
				if len(res) == 0 {
					t.Errorf("empty result")
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

func TestFlatHNSWMatchesBruteTop1(t *testing.T) {
	const dim = 32
	vecs, keys := randVectors(1000, dim, 5)
	flat := NewFlatHNSW(dim, L2, 1)
	flat.EfSearch = 100
	brute := NewBrute(dim, L2)
	for i := range vecs {
		flat.Add(keys[i], vecs[i])
		brute.Add(keys[i], vecs[i])
	}
	// querying an indexed point should return itself as the top hit
	hits := 0
	for i := 0; i < 200; i++ {
		res := flat.Search(vecs[i], 1)
		if len(res) == 1 && res[0].Key == keys[i] && math.Abs(float64(res[0].Distance)) < 1e-3 {
			hits++
		}
	}
	assert.Greater(t, hits, 195, "self-query top-1 should almost always be exact")
}
