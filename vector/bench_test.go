package vector

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// ---------------------------------------------------------------------------
// Distance kernels: scalar vs hand-unrolled vs SIMD (vek/AVX).
// ---------------------------------------------------------------------------

func benchDistance(b *testing.B, dim int, fn DistanceFunc) {
	rng := rand.New(rand.NewSource(1))
	x := make([]float32, dim)
	y := make([]float32, dim)
	for i := range x {
		x[i], y[i] = rng.Float32(), rng.Float32()
	}
	b.ResetTimer()
	var sink float32
	for i := 0; i < b.N; i++ {
		sink += fn(x, y)
	}
	_ = sink
}

func BenchmarkDistance(b *testing.B) {
	for _, dim := range []int{128, 768, 1536} {
		b.Run(fmt.Sprintf("d%d/scalar", dim), func(b *testing.B) { benchDistance(b, dim, L2DistanceScalar) })
		b.Run(fmt.Sprintf("d%d/unrolled", dim), func(b *testing.B) { benchDistance(b, dim, L2DistanceUnrolled) })
		b.Run(fmt.Sprintf("d%d/simd", dim), func(b *testing.B) { benchDistance(b, dim, L2DistanceSIMD) })
	}
}

// ---------------------------------------------------------------------------
// Build throughput.
// ---------------------------------------------------------------------------

const (
	benchN   = 20000
	benchDim = 128
)

func benchData() ([][]float32, []uint64) { return randVectors(benchN, benchDim, 2024) }

func BenchmarkBuildMapHNSW(b *testing.B) {
	vecs, keys := benchData()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := NewHNSW(benchDim, L2, 1)
		g.EfSearch = 64
		for j := range vecs {
			g.Add(keys[j], vecs[j])
		}
	}
}

func BenchmarkBuildFlatHNSW(b *testing.B) {
	vecs, keys := benchData()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := NewFlatHNSW(benchDim, L2, 1)
		for j := range vecs {
			g.Add(keys[j], vecs[j])
		}
	}
}

func BenchmarkBuildBtreeHNSW(b *testing.B) {
	vecs, keys := benchData()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		if err != nil {
			b.Fatal(err)
		}
		idx, err := OpenBtreeHNSW(db, "emb", benchDim, L2)
		if err != nil {
			b.Fatal(err)
		}
		for j := range vecs {
			idx.Add(keys[j], vecs[j])
		}
		if err := idx.Flush(); err != nil {
			b.Fatal(err)
		}
		_ = db.Close()
	}
}

// ---------------------------------------------------------------------------
// Search latency. Each index is built once; only the query is timed.
// ---------------------------------------------------------------------------

func buildFlat(ef int) (*FlatHNSW, [][]float32) {
	vecs, keys := benchData()
	g := NewFlatHNSW(benchDim, L2, 1)
	g.EfSearch = ef
	for j := range vecs {
		g.Add(keys[j], vecs[j])
	}
	queries, _ := randVectors(1000, benchDim, 555)
	return g, queries
}

func BenchmarkSearchBrute(b *testing.B) {
	vecs, keys := benchData()
	g := NewBrute(benchDim, L2)
	for j := range vecs {
		g.Add(keys[j], vecs[j])
	}
	queries, _ := randVectors(1000, benchDim, 555)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Search(queries[i%len(queries)], 10)
	}
}

func BenchmarkSearchMapHNSW(b *testing.B) {
	vecs, keys := benchData()
	g := NewHNSW(benchDim, L2, 1)
	g.EfSearch = 64
	for j := range vecs {
		g.Add(keys[j], vecs[j])
	}
	queries, _ := randVectors(1000, benchDim, 555)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Search(queries[i%len(queries)], 10)
	}
}

func BenchmarkSearchFlatHNSW(b *testing.B) {
	g, queries := buildFlat(64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Search(queries[i%len(queries)], 10)
	}
}

// ---------------------------------------------------------------------------
// Memory footprint (reported, not a timing benchmark). Run with:
//
//	go test ./vector -run TestMemoryFootprint -v
//
// ---------------------------------------------------------------------------

func TestMemoryFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("skip memory report in -short")
	}
	rawVectors := benchN * benchDim * 4
	rawMiB := float64(rawVectors) / (1 << 20)
	t.Logf("dataset: %d vectors x %d dims = %.1f MiB raw float32 (both indexes own a copy of the data)", benchN, benchDim, rawMiB)

	heapMiB := func() float64 {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return float64(ms.HeapAlloc) / (1 << 20)
	}

	// measureOwned builds an index, frees the source dataset (the index owns its
	// own copy of every vector), measures the live heap, then frees the index and
	// measures again. The difference is the index's true resident footprint.
	// Deterministic across runs (no reliance on build-time allocation deltas).
	measureOwned := func(build func(vecs [][]float32, keys []uint64) func()) float64 {
		vecs, keys := benchData()
		release := build(vecs, keys) // release drops the last reference to the index
		vecs, keys = nil, nil
		_, _ = vecs, keys
		withIdx := heapMiB()
		release()
		empty := heapMiB()
		return withIdx - empty
	}

	mapHeap := measureOwned(func(vecs [][]float32, keys []uint64) func() {
		g := NewHNSW(benchDim, L2, 1)
		g.EfSearch = 64
		for j := range vecs {
			g.Add(keys[j], vecs[j])
		}
		return func() { runtime.KeepAlive(g); g = nil; _ = g }
	})
	t.Logf("%-14s footprint=%6.1f MiB  (%.2fx raw vectors)", "map-HNSW", mapHeap, mapHeap/rawMiB)

	var flatMem float64
	flatHeap := measureOwned(func(vecs [][]float32, keys []uint64) func() {
		g := NewFlatHNSW(benchDim, L2, 1)
		for j := range vecs {
			g.Add(keys[j], vecs[j])
		}
		flatMem = float64(g.MemBytes()) / (1 << 20)
		return func() { runtime.KeepAlive(g); g = nil; _ = g }
	})
	t.Logf("%-14s footprint=%6.1f MiB  (%.2fx raw vectors)  [arena MemBytes()=%.1f MiB]",
		"flat-HNSW", flatHeap, flatHeap/rawMiB, flatMem)
}
