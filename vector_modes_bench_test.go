package anystore

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestVectorModesCompare builds the same dataset under each index mode and
// reports build RPS, search RPS, recall@k (vs an exact oracle), on-disk size and
// resident heap — so the btree / hybrid / brute trade-offs are directly
// comparable, including RAM. It is a measurement, skipped in -short.
//
// Tunable: ASV_CMP_N / ASV_CMP_DIM / ASV_CMP_Q / ASV_CMP_K, ASV_RPS_DIR (real disk).
func TestVectorModesCompare(t *testing.T) {
	if testing.Short() {
		t.Skip("measurement test")
	}
	n := envIntDefault("ASV_CMP_N", 3000)
	dim := envIntDefault("ASV_CMP_DIM", 128)
	nq := envIntDefault("ASV_CMP_Q", 300)
	k := envIntDefault("ASV_CMP_K", 10)

	baseDir := os.Getenv("ASV_RPS_DIR")
	if baseDir == "" {
		baseDir = os.TempDir()
	}

	vecs := vrand(n, dim, 1)
	queries := vrand(nq, dim, 2)
	// exact oracle (L2) for recall.
	truth := make([]map[string]bool, nq)
	for i, q := range queries {
		truth[i] = bruteIDs(vecs, q, k)
	}

	type scenario struct {
		label string
		mode  VectorMode
		quant VectorQuantization
	}
	scenarios := []scenario{
		{"btree  f32", VectorModeBTree, VectorQuantNone},
		{"btree  int8", VectorModeBTree, VectorQuantInt8},
		{"hybrid f32", VectorModeHybrid, VectorQuantNone},
		{"hybrid int8", VectorModeHybrid, VectorQuantInt8},
		{"brute", VectorModeBruteForce, VectorQuantNone},
	}

	type result struct {
		label                       string
		buildRPS, searchRPS, recall float64
		diskMiB, heapMiB            float64
	}
	var results []result

	mib := func(b uint64) float64 { return float64(b) / (1 << 20) }
	heapInuse := func() uint64 {
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapInuse
	}

	for _, sc := range scenarios {
		dir, err := os.MkdirTemp(baseDir, "asvcmp")
		require.NoError(t, err)

		db, err := Open(ctx, filepath.Join(dir, "cmp.db"), nil)
		require.NoError(t, err)
		coll, err := db.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
			Name:   "emb",
			Kind:   IndexKindVector,
			Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, Mode: sc.mode, Quantization: sc.quant},
		}))

		// build (single batched tx)
		t0 := time.Now()
		tx, err := db.WriteTx(ctx)
		require.NoError(t, err)
		for i := 0; i < n; i++ {
			require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(vecDocJSON(i, vecs[i]))))
		}
		require.NoError(t, tx.Commit())
		buildRPS := float64(n) / time.Since(t0).Seconds()

		require.NoError(t, db.Flush(ctx, 0, FlushModeCheckpointTruncate))

		// warm (build the hybrid mirror) then measure search + recall
		_, err = coll.VectorSearch(ctx, "emb", queries[0], k, 0)
		require.NoError(t, err)
		hitSum := 0
		t1 := time.Now()
		for i, q := range queries {
			hh, err := coll.VectorSearch(ctx, "emb", q, k, 0)
			require.NoError(t, err)
			for _, h := range hh {
				if truth[i][string(h.DocId)] {
					hitSum++
				}
			}
		}
		searchRPS := float64(nq) / time.Since(t1).Seconds()
		recall := float64(hitSum) / float64(nq*k)

		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		disk := st.VectorIndexesSizeBytes

		heap := heapInuse() // db + mirror resident

		results = append(results, result{sc.label, buildRPS, searchRPS, recall, mib(uint64(disk)), mib(heap)})

		require.NoError(t, db.Close())
		_ = os.RemoveAll(dir)
	}

	t.Logf("=== mode comparison: N=%d dim=%d queries=%d k=%d ===", n, dim, nq, k)
	t.Logf("%-12s %12s %12s %8s %10s %10s", "mode", "build/s", "search/s", "recall", "indexMiB", "heapMiB")
	for _, r := range results {
		t.Logf("%-12s %12.0f %12.0f %8.3f %10.1f %10.1f", r.label, r.buildRPS, r.searchRPS, r.recall, r.diskMiB, r.heapMiB)
	}
}
