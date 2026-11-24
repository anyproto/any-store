package vector

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// The btree-backed modes of cmd/vectorbench: -paged measures the btree-paged
// index (B) and the route-in-RAM + paged-rerank hybrid (B′) next to the
// in-memory arena (A) over an in-memory btree, and -db runs them on a file:
// the first run builds and persists the index, a second run against the same
// path reopens it cold, loading only the topology. Their indexes take the
// module's internal btree types and are unexported, so the modes run from
// this package's tests, with the binary's flags under a vectorbench. prefix
// (cmd/vectorbench/README.md):
//
//	go test ./vector -run '^TestVectorbench$' -v -args -vectorbench.paged
//	go test ./vector -run '^TestVectorbench$' -v -args -vectorbench.db=$HOME/vb.db -vectorbench.cache=64
//
// `go test -c ./vector` builds them into a binary that runs where there is no
// Go toolchain, as cmd/vectorbench does.
var (
	vbPaged   = flag.Bool("vectorbench.paged", false, "measure the btree-paged and hybrid indexes over an in-memory btree")
	vbDB      = flag.String("vectorbench.db", "", "on-disk btree path: the first run builds and persists, a re-run reopens cold from disk")
	vbCache   = flag.Int("vectorbench.cache", 0, "btree page-cache size in pages for -vectorbench.db (0 = default 5000); a small value forces disk I/O")
	vbN       = flag.Int("vectorbench.n", 20000, "number of documents/vectors")
	vbDim     = flag.Int("vectorbench.dim", 768, "embedding dimension")
	vbEf      = flag.Int("vectorbench.ef", 64, "efSearch (HNSW candidate-list size)")
	vbK       = flag.Int("vectorbench.k", 10, "k nearest neighbours per query")
	vbQueries = flag.Int("vectorbench.queries", 200, "queries used for recall + latency")
	vbSeed    = flag.Int64("vectorbench.seed", 2024, "dataset RNG seed")
	vbMetric  = flag.String("vectorbench.metric", "cosine", "distance metric: cosine|l2|dot")
	vbRecall  = flag.Bool("vectorbench.recall", true, "measure recall vs exact brute force (O(n) per query)")
)

func TestVectorbench(t *testing.T) {
	if !*vbPaged && *vbDB == "" {
		t.Skip("cmd/vectorbench's btree modes: run with -vectorbench.paged or -vectorbench.db=<path>")
	}
	m := Cosine
	switch *vbMetric {
	case "l2", "euclidean":
		m = L2
	case "dot", "ip":
		m = Dot
	}
	t.Logf("config: n=%d dim=%d metric=%s ef=%d k=%d queries=%d SIMD=%v", *vbN, *vbDim, m, *vbEf, *vbK, *vbQueries, SIMD())
	if *vbDB != "" {
		vectorbenchDisk(t, m)
		return
	}
	vectorbenchPaged(t, m)
}

// vectorbenchBuild generates the dataset and the queries and builds the
// in-memory graph, measuring recall at ef when -vectorbench.recall is set.
func vectorbenchBuild(t *testing.T, m Metric) (g *FlatHNSW, qVecs [][]float32) {
	t0 := time.Now()
	vecs, ids, _, avgBytes := GenMDDataset(*vbN, *vbDim, *vbSeed)
	qVecs, _, _, _ = GenMDDataset(*vbQueries, *vbDim, *vbSeed+1)
	t.Logf("generated %d docs (avg %.0f B) in %s", *vbN, avgBytes, time.Since(t0).Round(time.Millisecond))

	tb := time.Now()
	g = NewFlatHNSW(*vbDim, m, 1)
	g.EfSearch = *vbEf
	for i := range vecs {
		g.Add(ids[i], vecs[i])
	}
	t.Logf("built HNSW in %s (%.0f docs/s)", time.Since(tb).Round(time.Millisecond), float64(*vbN)/time.Since(tb).Seconds())

	if *vbRecall {
		brute := NewBrute(*vbDim, m)
		for i := range vecs {
			brute.Add(ids[i], vecs[i])
		}
		var r float64
		for _, q := range qVecs {
			r += recallAt(g.Search(q, *vbK), brute.Search(q, *vbK))
		}
		t.Logf("recall@%d ef=%d: %.3f", *vbK, *vbEf, r/float64(len(qVecs)))
	}
	return g, qVecs
}

// vectorbenchPaged is the -paged mode: A, B and B′ over an in-memory btree,
// with btree reads per query.
func vectorbenchPaged(t *testing.T, m Metric) {
	g, qVecs := vectorbenchBuild(t, m)
	k := *vbK
	memNs := timeSearchN(func(q []float32) { g.Search(q, k) }, qVecs, 2000)
	t.Logf("A  in-memory arena     : %9.1f us/q   RAM=%7.1f MiB (full)             0 reads/q",
		memNs/1000, vbMiB(g.MemBytes()))

	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	defer db.Close()
	p, err := buildPagedFromFlat(g, db, "emb", m)
	require.NoError(t, err)

	gets0, calls := p.Gets(), 0
	pagNs := timeSearchN(vbTimed(t, "Search", &calls, func(q []float32) error { _, err := p.Search(q, k); return err }), qVecs, 2000)
	t.Logf("B  paged vectors       : %9.1f us/q   RAM=%7.1f MiB (topology only) %5.0f reads/q  (%.1fx)",
		pagNs/1000, vbMiB(p.TopologyBytes()), float64(p.Gets()-gets0)/float64(calls), pagNs/memNs)

	gets0, calls = p.Gets(), 0
	hybNs := timeSearchN(vbTimed(t, "SearchHybrid", &calls, func(q []float32) error { _, err := p.SearchHybrid(q, k); return err }), qVecs, 2000)
	t.Logf("B' hybrid route+rerank : %9.1f us/q   RAM=routing-slab + topology   %5.0f reads/q  (%.1fx)",
		hybNs/1000, float64(p.Gets()-gets0)/float64(calls), hybNs/memNs)
	t.Logf("(a quantized routing slab makes B' RAM ~25%% [int8] or ~3%% [binary] of A)")
}

// vectorbenchDisk is the -db mode on a file-backed btree. The first run
// builds the index and persists it (split :topo/:vec); a re-run against the
// same path reopens it cold, loading only the topology into RAM and paging
// the vectors from disk, so two runs measure build+warm and reload+cold.
func vectorbenchDisk(t *testing.T, m Metric) {
	path, k := *vbDB, *vbK
	opts := btree.Options{}
	if *vbCache > 0 {
		opts.CacheSize = *vbCache
	}
	db, err := btree.Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	exists, err := pagedExists(db, "emb")
	require.NoError(t, err)
	if exists {
		t.Logf("RELOAD run: reopening the on-disk index at %s (cold, cache=%d pages)", path, *vbCache)
		tl := time.Now()
		p, err := openPaged(db, "emb")
		require.NoError(t, err)
		t.Logf("reloaded %d nodes (topology only) in %s; topology RAM=%.1f MiB (vectors stay on disk)",
			p.PhysLen(), time.Since(tl).Round(time.Millisecond), vbMiB(p.TopologyBytes()))
		// The index has the dimension, metric and search depth of the run
		// that built it, whatever the flags of this one: the queries are
		// generated for its dimension, and the settings logged are its own.
		meta := vbPersistedMeta(t, db, "emb")
		t.Logf("index: dim=%d metric=%s ef=%d (as built; -vectorbench.dim, -metric and -ef do not apply to a reload)",
			meta.dim, meta.metric, meta.EfSearch)
		qVecs, _, _, _ := GenMDDataset(*vbQueries, meta.dim, *vbSeed+1)
		gets0, calls := p.Gets(), 0
		pagNs := timeSearchN(vbTimed(t, "Search", &calls, func(q []float32) error { _, err := p.Search(q, k); return err }), qVecs, 2000)
		t.Logf("B  paged vectors (cold disk): %8.1f us/q   %5.0f reads/q",
			pagNs/1000, float64(p.Gets()-gets0)/float64(calls))
		t.Logf("(cold = the btree page cache started empty in this process; run again for a warmer cache)")
		return
	}

	t.Logf("BUILD run: building and persisting to %s", path)
	g, qVecs := vectorbenchBuild(t, m)
	tp := time.Now()
	require.NoError(t, persistPaged(g, db, "emb", m))
	require.NoError(t, db.Checkpoint(btree.CheckpointFull))
	t.Logf("persisted (split :topo/:vec) in %s; on disk: %s", time.Since(tp).Round(time.Millisecond), vbFileSizes(path))

	memNs := timeSearchN(func(q []float32) { g.Search(q, k) }, qVecs, 2000)
	t.Logf("A  in-memory arena      : %8.1f us/q   RAM=%.1f MiB (full)", memNs/1000, vbMiB(g.MemBytes()))
	p, err := attachPaged(g, db, "emb", m)
	require.NoError(t, err)
	gets0, calls := p.Gets(), 0
	pagNs := timeSearchN(vbTimed(t, "Search", &calls, func(q []float32) error { _, err := p.Search(q, k); return err }), qVecs, 2000)
	t.Logf("B  paged vectors (warm) : %8.1f us/q   %5.0f reads/q  (%.1fx)",
		pagNs/1000, float64(p.Gets()-gets0)/float64(calls), pagNs/memNs)
	gets0, calls = p.Gets(), 0
	hybNs := timeSearchN(vbTimed(t, "SearchHybrid", &calls, func(q []float32) error { _, err := p.SearchHybrid(q, k); return err }), qVecs, 2000)
	t.Logf("B' hybrid route+rerank  : %8.1f us/q   %5.0f reads/q  (%.1fx)",
		hybNs/1000, float64(p.Gets()-gets0)/float64(calls), hybNs/memNs)
	t.Logf("run the same command again to measure the cold reload from disk (drop the OS cache first for a true cold read)")
}

// vbTimed wraps a search for timeSearchN: it counts the calls in *calls and
// fails the test at the first error, so a run whose searches fail reports
// no latency for them.
func vbTimed(t *testing.T, name string, calls *int, search func(q []float32) error) func(q []float32) {
	return func(q []float32) {
		*calls++
		if err := search(q); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// vbPersistedMeta reads the meta record of the persisted index name: the
// dimension, metric and search depth it was built with.
func vbPersistedMeta(t *testing.T, db *btree.DB, name string) indexMeta {
	ns, err := db.GetNamespace(name + ":pmeta")
	require.NoError(t, err)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	b, err := rtx.Get(ns, []byte(btreeMetaKey))
	require.NoError(t, err)
	meta, err := decodeMeta(b)
	require.NoError(t, err)
	return meta
}

func vbMiB(bytes int) float64 { return float64(bytes) / (1 << 20) }

func vbFileSizes(path string) string {
	var total int64
	var parts strings.Builder
	for _, suf := range []string{"", "-wal"} {
		if fi, err := os.Stat(path + suf); err == nil {
			total += fi.Size()
			if suf != "" {
				fmt.Fprintf(&parts, " +%s %.1fMiB", suf, float64(fi.Size())/(1<<20))
			}
		}
	}
	return fmt.Sprintf("%.1f MiB total%s", float64(total)/(1<<20), parts.String())
}
