package vindex

import (
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

func bulkRecall(t *testing.T, ix *Index, rtx *btree.ReadTx, vecs, queries [][]float32, k int) float64 {
	t.Helper()
	hit, tot := 0, 0
	for _, q := range queries {
		truth := bruteTopK(vecs, q, k)
		hits, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
		for _, h := range hits {
			if truth[string(h.DocID)] {
				hit++
			}
		}
		tot += k
	}
	return float64(hit) / float64(tot)
}

// TestBulkBuildParallelEquivalence: the parallel build yields a valid graph whose
// recall matches the per-insert build (not byte-identical — it depends on insertion
// interleaving), and is fully functional (search + incremental insert/delete).
func TestBulkBuildParallelEquivalence(t *testing.T) {
	const (
		n   = 3000
		dim = 32
		k   = 10
	)
	const seed = int64(1)
	vecs := randVecs(n, dim, 11)
	queries := randVecs(60, dim, 99)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}
	params := Params{Dim: dim, Metric: L2, EfSearch: 64}

	// Per-insert reference recall.
	db1, ix1 := newTestIndex(t, dim, L2)
	insertAll(t, db1, ix1, vecs)
	rtx1, err := db1.BeginRead()
	require.NoError(t, err)
	defer rtx1.Rollback()
	refRecall := bulkRecall(t, ix1, rtx1, vecs, queries, k)

	// Parallel build.
	db2, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	wtx, err := db2.BeginWrite()
	require.NoError(t, err)
	ix2, err := BulkBuildParallel(wtx, "vix", params, seed, ids, vecs, 8)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	rtx2, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx2.Rollback()
	parRecall := bulkRecall(t, ix2, rtx2, vecs, queries, k)

	t.Logf("recall per-insert=%.3f parallel=%.3f", refRecall, parRecall)
	require.Greater(t, parRecall, 0.90, "parallel build recall too low")
	require.InDelta(t, refRecall, parRecall, 0.03, "parallel recall should match per-insert")

	// Incremental insert + delete after a parallel bulk build still work.
	more := randVecs(100, dim, 555)
	wtx, err = db2.BeginWrite()
	require.NoError(t, err)
	for i, v := range more {
		require.NoError(t, ix2.Insert(wtx, docID(n+i), v))
	}
	_, err = ix2.Delete(wtx, docID(0))
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	rtx3, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx3.Rollback()
	hits, err := ix2.Search(rtx3, more[0], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, docID(n), hits[0].DocID)
}

// TestBulkBuildParallelRace builds with many workers + then searches concurrently,
// to be run under -race.
func TestBulkBuildParallelRace(t *testing.T) {
	const (
		n   = 2500
		dim = 24
	)
	vecs := randVecs(n, dim, 5)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := BulkBuildParallel(wtx, "vix", Params{Dim: dim, Metric: Cosine, EfSearch: 64, Quantization: QuantInt8}, 1, ids, vecs, 8)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	// Concurrent reads on the committed index — each goroutine uses its OWN read tx
	// (the supported pattern; a ReadTx is not shared across goroutines).
	done := make(chan struct{})
	for w := 0; w < 4; w++ {
		go func(off int) {
			for i := 0; i < 50; i++ {
				rtx, err := db.BeginRead()
				if err != nil {
					break
				}
				_, _ = ix.Search(rtx, vecs[(i+off)%n], 10, 64)
				_ = rtx.Rollback()
			}
			done <- struct{}{}
		}(w * 7)
	}
	for w := 0; w < 4; w++ {
		<-done
	}
}

// TestBulkBuildParallelReopen verifies the on-disk format a parallel build writes
// is readable by a FRESH DB handle (== another process): build, close, reopen the
// file, open the index, and search correctly.
func TestBulkBuildParallelReopen(t *testing.T) {
	const (
		n   = 2000
		dim = 32
		k   = 10
	)
	vecs := randVecs(n, dim, 13)
	queries := randVecs(40, dim, 77)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}
	params := Params{Dim: dim, Metric: L2, EfSearch: 64}
	path := filepath.Join(t.TempDir(), "reopen.db")

	db, err := btree.Open(path, btree.Options{})
	require.NoError(t, err)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = BulkBuildParallel(wtx, "vix", params, 1, ids, vecs, 8)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	require.NoError(t, db.Close())

	// Fresh handle on the same file (simulates a different process opening it).
	db2, err := btree.Open(path, btree.Options{})
	require.NoError(t, err)
	defer db2.Close()
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix2, err := OpenTx(rtx, "vix", 1)
	require.NoError(t, err)
	// counts read back correctly
	live, deleted, total, err := ix2.Counts(rtx)
	require.NoError(t, err)
	require.Equal(t, n, live)
	require.Equal(t, 0, deleted)
	require.Equal(t, n, total)
	// recall sane + self-retrieval
	require.Greater(t, bulkRecall(t, ix2, rtx, vecs, queries, k), 0.90)
	for _, i := range []int{0, 999, n - 1} {
		hits, err := ix2.Search(rtx, vecs[i], 1, 64)
		require.NoError(t, err)
		require.Len(t, hits, 1)
		require.Equal(t, docID(i), hits[0].DocID)
	}
}

// TestBulkBuildParallelRecall768 checks parallel-build recall at a high dimension
// MATCHES the per-insert build on the same data. (Absolute recall on uniform-random
// 768-dim vectors is low by nature — the curse of dimensionality — so the property
// that matters is parity with the sequential builder, not an absolute number; real
// embeddings, validated in the consumer e2e, reach ~0.96.)
func TestBulkBuildParallelRecall768(t *testing.T) {
	if testing.Short() {
		t.Skip("slow high-dim build")
	}
	const (
		n   = 2500
		dim = 768
		k   = 10
	)
	vecs := randVecs(n, dim, 3)
	queries := randVecs(50, dim, 88)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}

	// per-insert reference
	db1, ix1 := newTestIndex(t, dim, Cosine)
	insertAll(t, db1, ix1, vecs)
	rtx1, err := db1.BeginRead()
	require.NoError(t, err)
	defer rtx1.Rollback()
	ref := bulkRecall(t, ix1, rtx1, vecs, queries, k)

	// parallel
	db2, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	wtx, err := db2.BeginWrite()
	require.NoError(t, err)
	ix2, err := BulkBuildParallel(wtx, "vix", Params{Dim: dim, Metric: Cosine, EfSearch: 64}, 1, ids, vecs, 0)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	rtx2, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx2.Rollback()
	par := bulkRecall(t, ix2, rtx2, vecs, queries, k)

	t.Logf("dim768 recall per-insert=%.3f parallel=%.3f", ref, par)
	require.InDelta(t, ref, par, 0.03, "parallel recall must match per-insert at dim 768")
}

// TestBulkBuildParallelEdge covers small / degenerate sizes (empty, single, fewer
// than the worker count) where the seed/parallel/repair paths must still be correct.
func TestBulkBuildParallelEdge(t *testing.T) {
	const dim = 8
	for _, n := range []int{0, 1, 2, 5, 50} {
		vecs := randVecs(n, dim, int64(n+1))
		ids := make([][]byte, n)
		for i := range ids {
			ids[i] = docID(i)
		}
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		require.NoError(t, err)
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		ix, err := BulkBuildParallel(wtx, "vix", Params{Dim: dim, Metric: L2, EfSearch: 64}, 1, ids, vecs, 8)
		require.NoError(t, err)
		require.NoError(t, wtx.Commit())
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		live, deleted, total, err := ix.Counts(rtx)
		require.NoError(t, err)
		require.Equalf(t, n, live, "n=%d live", n)
		require.Equal(t, 0, deleted)
		require.Equal(t, n, total)
		for i := 0; i < n; i++ { // each vector retrieves itself
			hits, err := ix.Search(rtx, vecs[i], 1, 64)
			require.NoError(t, err)
			require.Lenf(t, hits, 1, "n=%d i=%d", n, i)
			require.Equalf(t, docID(i), hits[0].DocID, "n=%d self-retrieval i=%d", n, i)
		}
		_ = rtx.Rollback()
		_ = db.Close()
	}
}

// TestBulkBuildParallelMem reports the RAM/alloc cost of the parallel build (which
// holds the whole graph in RAM) vs the per-insert path. Skipped in -short.
func TestBulkBuildParallelMem(t *testing.T) {
	if testing.Short() {
		t.Skip("mem")
	}
	const (
		n   = 20000
		dim = 128
	)
	vecs := randVecs(n, dim, 7)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}
	params := Params{Dim: dim, Metric: Cosine, EfSearch: 64, Quantization: QuantInt8}

	measure := func(name string, run func(db *btree.DB)) {
		db, err := btree.Open(filepath.Join(t.TempDir(), "m.db"), btree.Options{})
		require.NoError(t, err)
		runtime.GC()
		var m0 runtime.MemStats
		runtime.ReadMemStats(&m0)
		// peak HeapInuse sampler
		stop := make(chan struct{})
		var peak uint64
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					var ms runtime.MemStats
					runtime.ReadMemStats(&ms)
					if ms.HeapInuse > atomic.LoadUint64(&peak) {
						atomic.StoreUint64(&peak, ms.HeapInuse)
					}
					time.Sleep(5 * time.Millisecond)
				}
			}
		}()
		run(db)
		close(stop)
		var m1 runtime.MemStats
		runtime.ReadMemStats(&m1)
		const MiB = 1 << 20
		t.Logf("%-12s peakHeap=%4dMiB  allocated=%5dMiB  mallocs=%d",
			name, atomic.LoadUint64(&peak)/MiB, (m1.TotalAlloc-m0.TotalAlloc)/MiB, m1.Mallocs-m0.Mallocs)
		_ = db.Close()
	}

	measure("per-insert", func(db *btree.DB) {
		wtx, _ := db.BeginWrite()
		ix, _ := Create(wtx, "vix", params, 1)
		_ = wtx.Commit()
		wtx, _ = db.BeginWrite()
		for i := 0; i < n; i++ {
			require.NoError(t, ix.Insert(wtx, ids[i], vecs[i]))
			if (i+1)%2000 == 0 {
				require.NoError(t, wtx.Commit())
				wtx, _ = db.BeginWrite()
			}
		}
		require.NoError(t, wtx.Commit())
	})
	measure("parallel", func(db *btree.DB) {
		wtx, _ := db.BeginWrite()
		_, err := BulkBuildParallel(wtx, "vix", params, 1, ids, vecs, 0)
		require.NoError(t, err)
		require.NoError(t, wtx.Commit())
	})
}

// TestBulkBuildParallelTiming compares per-insert vs sequential bulk vs parallel
// bulk at scale, file-backed. Skipped in -short.
func TestBulkBuildParallelTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("timing")
	}
	const (
		n   = 20000
		dim = 128
	)
	const seed = int64(1)
	vecs := randVecs(n, dim, 7)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = docID(i)
	}
	params := Params{Dim: dim, Metric: Cosine, EfSearch: 64, Quantization: QuantInt8}

	// Per-insert (batched 2000/tx).
	db1, err := btree.Open(filepath.Join(t.TempDir(), "ins.db"), btree.Options{})
	require.NoError(t, err)
	wtx, err := db1.BeginWrite()
	require.NoError(t, err)
	ix1, err := Create(wtx, "vix", params, seed)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	tIns := time.Now()
	wtx, _ = db1.BeginWrite()
	for i := 0; i < n; i++ {
		require.NoError(t, ix1.Insert(wtx, ids[i], vecs[i]))
		if (i+1)%2000 == 0 {
			require.NoError(t, wtx.Commit())
			wtx, _ = db1.BeginWrite()
		}
	}
	require.NoError(t, wtx.Commit())
	insDur := time.Since(tIns)
	_ = db1.Close()

	// Parallel bulk.
	db2, err := btree.Open(filepath.Join(t.TempDir(), "par.db"), btree.Options{})
	require.NoError(t, err)
	tPar := time.Now()
	wtx, _ = db2.BeginWrite()
	_, err = BulkBuildParallel(wtx, "vix", params, seed, ids, vecs, 0)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	parDur := time.Since(tPar)
	_ = db2.Close()

	t.Logf("per-insert: %s (%.0f/s)   parallel-bulk: %s (%.0f/s)   speedup %.1fx",
		insDur.Round(time.Millisecond), float64(n)/insDur.Seconds(),
		parDur.Round(time.Millisecond), float64(n)/parDur.Seconds(),
		insDur.Seconds()/parDur.Seconds())
}
