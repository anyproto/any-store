package vector

import (
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// TestMDDataset is the realistic-scale capstone: 50–100k topic-clustered
// markdown-document embeddings (see mddata.go), run through the in-memory arena,
// the paged (Option B) index, and the route-in-RAM/rerank hybrid.
func TestMDDataset(t *testing.T) {
	if testing.Short() {
		t.Skip("realistic dataset is large; skipped in -short")
	}
	const (
		n   = 75000 // bump to 100000 for the top of the range
		dim = 768   // representative doc-embedding width (e5/bge-base class)
		k   = 10
	)

	t0 := time.Now()
	vecs, ids, _, avgBytes := GenMDDataset(n, dim, 2024)
	queries, _, _, _ := GenMDDataset(2000, dim, 7) // held-out, same distribution
	t.Logf("generated %d markdown docs (avg %.0f bytes/doc) + embedded dim=%d in %s",
		n, avgBytes, dim, time.Since(t0).Round(time.Millisecond))
	t.Logf("raw vectors: %d x %d x 4 = %.1f MiB; markdown corpus ~%.0f MiB",
		n, dim, float64(n*dim*4)/(1<<20), float64(n)*avgBytes/(1<<20))

	tb := time.Now()
	g := NewFlatHNSW(dim, Cosine, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(ids[i], vecs[i])
	}
	t.Logf("build flat HNSW (in-mem): %s  (%.0f docs/s)", time.Since(tb).Round(time.Millisecond), float64(n)/time.Since(tb).Seconds())

	const qRecall = 200
	brute := NewBrute(dim, Cosine)
	for i := range vecs {
		brute.Add(ids[i], vecs[i])
	}
	truth := make([][]SearchResult, qRecall)
	for i := 0; i < qRecall; i++ {
		truth[i] = brute.Search(queries[i], k)
	}
	for _, ef := range []int{64, 128, 256} {
		g.EfSearch = ef
		var recall float64
		s := time.Now()
		for i := 0; i < qRecall; i++ {
			recall += recallAt(g.Search(queries[i], k), truth[i])
		}
		us := float64(time.Since(s).Microseconds()) / qRecall
		t.Logf("recall@%d ef=%-3d: %.3f   (%.0f us/q in-mem)", k, ef, recall/qRecall, us)
	}
	g.EfSearch = 64

	timeSearch := func(fn func(q []float32)) float64 {
		for i := 0; i < 200; i++ {
			fn(queries[i%len(queries)])
		}
		const iters = 3000
		s := time.Now()
		for i := 0; i < iters; i++ {
			fn(queries[i%len(queries)])
		}
		return float64(time.Since(s).Nanoseconds()) / iters
	}
	memNs := timeSearch(func(q []float32) { g.Search(q, k) })

	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	p, err := BuildPagedFromFlat(g, db, "emb", Cosine)
	if err != nil {
		t.Fatal(err)
	}
	g0 := p.gets
	var c1 int
	pagNs := timeSearchN(func(q []float32) { p.Search(q, k); c1++ }, queries, 800)
	pagGets := float64(p.gets-g0) / float64(c1)

	g0 = p.gets
	var c2 int
	hybNs := timeSearchN(func(q []float32) { p.SearchHybrid(q, k); c2++ }, queries, 800)
	hybGets := float64(p.gets-g0) / float64(c2)

	fullMiB := float64(g.MemBytes()) / (1 << 20)
	topoMiB := float64(p.TopologyBytes()) / (1 << 20)
	t.Logf("--- search (k=%d, ef=64) ---", k)
	t.Logf("A in-memory arena : %8.1f us/q   RAM=%6.1f MiB (full)            0 gets/q", memNs/1000, fullMiB)
	t.Logf("B paged vectors   : %8.1f us/q   RAM=%6.1f MiB (topology only) %4.0f gets/q  (%.1fx)", pagNs/1000, topoMiB, pagGets, pagNs/memNs)
	t.Logf("B' hybrid         : %8.1f us/q   RAM=routing-slab + topology   %4.0f gets/q  (%.1fx)", hybNs/1000, hybGets, hybNs/memNs)
	t.Logf("doc-id mapping (DocFlatHNSW []byte ids) adds ~90 B/id => ~%.1f MiB at %d docs", 90*float64(n)/(1<<20), n)
}

func timeSearchN(fn func(q []float32), queries [][]float32, iters int) float64 {
	for i := 0; i < 100; i++ {
		fn(queries[i%len(queries)])
	}
	s := time.Now()
	for i := 0; i < iters; i++ {
		fn(queries[i%len(queries)])
	}
	return float64(time.Since(s).Nanoseconds()) / float64(iters)
}
