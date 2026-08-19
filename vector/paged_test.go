package vector

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

func TestPagedMatchesMemory(t *testing.T) {
	const (
		n   = 2000
		dim = 48
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 17)
	queries, _ := randVectors(50, dim, 71)

	g := NewFlatHNSW(dim, L2, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}

	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	defer db.Close()
	p, err := BuildPagedFromFlat(g, db, "emb", L2)
	require.NoError(t, err)

	// Identical graph + identical vectors => identical results.
	for _, q := range queries {
		mem := g.Search(q, k)
		pag, err := p.Search(q, k)
		require.NoError(t, err)
		require.Equal(t, len(mem), len(pag))
		for j := range mem {
			require.Equal(t, mem[j].Key, pag[j].Key, "result mismatch")
			require.InDelta(t, mem[j].Distance, pag[j].Distance, 1e-4)
		}
	}
}

func TestPersistReloadParity(t *testing.T) {
	const (
		n   = 1500
		dim = 48
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 23)
	queries, _ := randVectors(40, dim, 91)

	g := NewFlatHNSW(dim, L2, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}

	path := filepath.Join(t.TempDir(), "p.db")
	// build + persist, then close
	{
		db, err := btree.Open(path, btree.Options{})
		require.NoError(t, err)
		require.NoError(t, PersistPaged(g, db, "emb", L2))
		require.NoError(t, db.Checkpoint(btree.CheckpointFull))
		require.NoError(t, db.Close())
	}
	// reopen cold and search — topology-only load must reproduce paged results
	db, err := btree.Open(path, btree.Options{})
	require.NoError(t, err)
	defer db.Close()
	require.True(t, PagedExists(db, "emb"))
	p, err := OpenPaged(db, "emb")
	require.NoError(t, err)
	require.Equal(t, n, p.PhysLen())

	for _, q := range queries {
		mem := g.Search(q, k)
		rel, err := p.Search(q, k)
		require.NoError(t, err)
		require.Equal(t, len(mem), len(rel))
		for j := range mem {
			require.Equal(t, mem[j].Key, rel[j].Key, "reloaded paged result mismatch")
		}
	}
}

// TestPagedVsMemory measures the latency and RAM trade of paging vectors from
// the btree (Option B) vs the in-memory arena (Option A), warm cache.
func TestPagedVsMemory(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const (
		n   = 20000
		dim = 128
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 2024)
	queries, _ := randVectors(2000, dim, 7)

	g := NewFlatHNSW(dim, L2, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}

	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	defer db.Close()
	p, err := BuildPagedFromFlat(g, db, "emb", L2)
	require.NoError(t, err)

	bench := func(search func(q []float32)) float64 {
		for i := 0; i < 200; i++ {
			search(queries[i%len(queries)])
		}
		const iters = 3000
		start := time.Now()
		for i := 0; i < iters; i++ {
			search(queries[i%len(queries)])
		}
		return float64(time.Since(start).Nanoseconds()) / iters
	}

	memNs := bench(func(q []float32) { g.Search(q, k) })

	getsBefore := p.gets
	var calls int
	pagNs := bench(func(q []float32) { p.Search(q, k); calls++ })
	getsPerQuery := float64(p.gets-getsBefore) / float64(calls)

	getsBefore = p.gets
	calls = 0
	hybNs := bench(func(q []float32) { p.SearchHybrid(q, k); calls++ })
	hybGets := float64(p.gets-getsBefore) / float64(calls)

	raw := float64(n*dim*4) / (1 << 20)
	t.Logf("dataset: %d x %dd (raw vectors %.1f MiB)", n, dim, raw)
	t.Logf("A  in-memory arena   : %8.0f ns/op   RAM=%5.1f MiB (full)                0 gets/q", memNs, float64(g.MemBytes())/(1<<20))
	t.Logf("B  paged vectors     : %8.0f ns/op   RAM=%5.1f MiB (topology only)   %4.0f gets/q  (%.1fx slower, %.0f%% less RAM)",
		pagNs, float64(p.TopologyBytes())/(1<<20), getsPerQuery, pagNs/memNs, 100*(1-float64(p.TopologyBytes())/float64(g.MemBytes())))
	t.Logf("B' hybrid route+rerank: %8.0f ns/op   RAM=routing-slab + topology     %4.0f gets/q  (%.1fx slower)",
		hybNs, hybGets, hybNs/memNs)
	t.Logf("   (hybrid routes in RAM, pages only the ef rerank set; a quantized routing slab makes its RAM ~25%% of A)")
}

// TestPagedCachePressure runs the paged index on a FILE-backed db whose page
// cache is far smaller than the vector data, so reads spill to disk — the regime
// where Option B earns its keep (data bigger than RAM) and pays for it.
func TestPagedCachePressure(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const (
		n   = 60000
		dim = 128
		k   = 10
	)
	vecs, keys := randVectors(n, dim, 5)
	queries, _ := randVectors(500, dim, 9)

	g := NewFlatHNSW(dim, L2, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}
	rawMiB := float64(n*dim*4) / (1 << 20)

	run := func(cachePages int) (float64, float64) {
		path := filepath.Join(t.TempDir(), "vec.db")
		db, err := btree.Open(path, btree.Options{CacheSize: cachePages})
		require.NoError(t, err)
		defer db.Close()
		p, err := BuildPagedFromFlat(g, db, "emb", L2)
		require.NoError(t, err)
		// warm
		for i := 0; i < 100; i++ {
			p.Search(queries[i%len(queries)], k)
		}
		before := p.gets
		start := time.Now()
		const iters = 1000
		for i := 0; i < iters; i++ {
			p.Search(queries[i%len(queries)], k)
		}
		ns := float64(time.Since(start).Nanoseconds()) / iters
		return ns, float64(p.gets-before) / iters
	}

	t.Logf("dataset %d x %dd = %.1f MiB raw vectors, page=4KiB", n, dim, rawMiB)
	for _, pages := range []int{2000, 8000, 40000} {
		ns, gets := run(pages)
		t.Logf("cache=%5d pages (%4.0f MiB): %9.0f ns/op  %.0f gets/query", pages, float64(pages*4)/1024, ns, gets)
	}
}
