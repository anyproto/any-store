package vindex

import (
	"sync"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// searchBoth runs the same query with the mirror off then on and asserts the
// results are byte-identical (same labels, distances, order). The hybrid mirror
// is derived from the same btree graph, so it must reproduce btree search
// exactly — recall is identical, not merely close.
func assertHybridEqualsBtree(t *testing.T, db *btree.DB, ix *Index, queries [][]float32, k int) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	for qi, q := range queries {
		ix.SetHybrid(false)
		base, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
		ix.SetHybrid(true)
		hyb, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
		ix.SetHybrid(false)

		require.Equal(t, len(base), len(hyb), "query %d result count", qi)
		for i := range base {
			require.Equalf(t, base[i].DocID, hyb[i].DocID, "query %d hit %d docID", qi, i)
			require.InDeltaf(t, base[i].Distance, hyb[i].Distance, 1e-6, "query %d hit %d dist", qi, i)
		}
	}
}

// TestVindexVectorTierEqualsBtree verifies the RAM vector tier returns identical
// results to reading vectors from the btree — for both float32 and int8 indexes
// (the tier caches the exact :vec bytes). Run against the same index with the
// cache off then on.
func TestVindexVectorTierEqualsBtree(t *testing.T) {
	const (
		n   = 1500
		dim = 32
		k   = 10
	)
	vecs := randVecs(n, dim, 7)
	queries := randVecs(40, dim, 99)

	searchAll := func(ix *Index, rtx *btree.ReadTx) [][]string {
		out := make([][]string, len(queries))
		for qi, q := range queries {
			hits, err := ix.Search(rtx, q, k, 64)
			require.NoError(t, err)
			ids := make([]string, len(hits))
			for i, h := range hits {
				ids[i] = string(h.DocID)
			}
			out[qi] = ids
		}
		return out
	}

	for _, quant := range []Quantization{QuantNone, QuantInt8} {
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		require.NoError(t, err)
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: L2, EfSearch: 64, Quantization: quant}, 1)
		require.NoError(t, err)
		require.NoError(t, wtx.Commit())
		insertAll(t, db, ix, vecs)

		rtx, err := db.BeginRead()
		require.NoError(t, err)

		ix.SetHybrid(true)
		ix.SetVectorCache(false)
		base := searchAll(ix, rtx)
		ix.SetVectorCache(true)
		cached := searchAll(ix, rtx)

		require.Equal(t, base, cached, "vector tier must match btree reads (quant=%v)", quant)

		_ = rtx.Rollback()
		_ = db.Close()
	}
}

func TestVindexHybridTruthEquivalence(t *testing.T) {
	const (
		n   = 1500
		dim = 32
		k   = 10
	)
	vecs := randVecs(n, dim, 7)
	queries := randVecs(60, dim, 99)

	db, ix := newTestIndex(t, dim, L2)
	insertAll(t, db, ix, vecs)

	// 1) base graph: hybrid == btree (also primes the mirror via a rebuild).
	assertHybridEqualsBtree(t, db, ix, queries, k)

	// 2) after deletes (tombstones change layer-0 deleted flags + bump l0Gen),
	//    a re-run must rebuild the mirror and still match exactly.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i += 7 {
		_, derr := ix.Delete(wtx, docID(i))
		require.NoError(t, derr)
	}
	require.NoError(t, wtx.Commit())
	assertHybridEqualsBtree(t, db, ix, queries, k)

	// 3) after more inserts (new nodes + back-links bump l0Gen again).
	more := randVecs(400, dim, 555)
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	for i, v := range more {
		require.NoError(t, ix.Insert(wtx, docID(n+i), v))
	}
	require.NoError(t, wtx.Commit())
	assertHybridEqualsBtree(t, db, ix, queries, k)
}

// TestVindexHybridConcurrent stresses the mirror's lock-free publish + dirty ring
// under concurrent readers and a writer (run with -race). Readers build/publish
// mirrors at various gens while the writer records changes; correctness is checked
// by each reader comparing its own hybrid result to a btree result on the same tx.
func TestVindexHybridConcurrent(t *testing.T) {
	const (
		n    = 500
		dim  = 16
		k    = 10
		iter = 60
	)
	vecs := randVecs(n, dim, 4)
	queries := randVecs(20, dim, 41)

	db, ix := newTestIndex(t, dim, L2)
	ix.SetHybrid(true)
	insertAll(t, db, ix, vecs)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	errCh := make(chan error, 8)

	// Writer: interleave insert batches and deletes, advancing l0Gen + the ring.
	wg.Add(1)
	go func() {
		defer wg.Done()
		next := n
		for i := 0; i < iter; i++ {
			select {
			case <-stop:
				return
			default:
			}
			wtx, err := db.BeginWrite()
			if err != nil {
				errCh <- err
				return
			}
			for j := 0; j < 10; j++ {
				if err := ix.Insert(wtx, docID(next), randVecs(1, dim, int64(next))[0]); err != nil {
					errCh <- err
					_ = wtx.Rollback()
					return
				}
				next++
			}
			if i%3 == 0 {
				_, _ = ix.Delete(wtx, docID(i))
			}
			if err := wtx.Commit(); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Readers: each on its own snapshot, asserting hybrid == btree.
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iter; i++ {
				rtx, err := db.BeginRead()
				if err != nil {
					errCh <- err
					return
				}
				q := queries[i%len(queries)]
				hyb, err := ix.Search(rtx, q, k, 64)
				if err != nil {
					errCh <- err
					_ = rtx.Rollback()
					return
				}
				_ = hyb
				_ = rtx.Rollback()
			}
		}()
	}

	wg.Wait()
	close(stop)
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

// searchHybridEqualsBtreeTx asserts hybrid==btree for all queries on an open rtx.
func searchHybridEqualsBtreeTx(t *testing.T, ix *Index, rtx *btree.ReadTx, queries [][]float32, k int) {
	t.Helper()
	for qi, q := range queries {
		ix.SetHybrid(false)
		base, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
		ix.SetHybrid(true)
		hyb, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
		require.Equalf(t, len(base), len(hyb), "query %d count", qi)
		for i := range base {
			require.Equalf(t, base[i].DocID, hyb[i].DocID, "query %d hit %d", qi, i)
		}
	}
}

// TestVindexHybridIncremental verifies the incremental overlay path: with hybrid
// on during writes (so the dirty ring records), a small batch of new inserts is
// folded into the mirror by re-reading only the changed labels — the shared CSR
// base is NOT rebuilt — and results still match btree exactly. This also proves
// the dirty-label capture (new node + its back-links) is complete: a missed
// back-link would leave the overlay serving stale adjacency and diverge from btree.
func TestVindexHybridIncremental(t *testing.T) {
	const (
		n   = 600
		dim = 24
		k   = 10
	)
	vecs := randVecs(n, dim, 11)
	queries := randVecs(40, dim, 222)

	db, ix := newTestIndex(t, dim, L2)
	ix.SetHybrid(true) // hybrid ON during writes → the dirty ring records changes
	insertAll(t, db, ix, vecs)

	// Prime: a hybrid search builds the full CSR base at the current gen.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	for _, q := range queries {
		_, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
	}
	require.NoError(t, rtx.Rollback())
	baseGen := ix.l0base.Load().gen
	require.NotZero(t, baseGen)

	// A small batch of new inserts (changed labels well under overlayFoldCap).
	more := randVecs(40, dim, 333)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	for i, v := range more {
		require.NoError(t, ix.Insert(wtx, docID(n+i), v))
	}
	require.NoError(t, wtx.Commit())

	rtx, err = db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	searchHybridEqualsBtreeTx(t, ix, rtx, queries, k)

	require.Equal(t, baseGen, ix.l0base.Load().gen,
		"base must NOT be rebuilt — the incremental overlay should have been used")
	require.Greater(t, ix.l0pub.Load().gen, baseGen,
		"published mirror gen must advance past the base via the overlay")
}

// TestVindexHybridFullRebuildFallback verifies the coverage-gap fallback: when
// the dirty ring can't cover the whole generation gap (a peer's writes, or — as
// modelled here — writes made while the ring wasn't recording), the mirror falls
// back to a full base rebuild and still matches btree exactly. l0Gen advances on
// every write regardless of hybrid mode, so writing with hybrid off leaves the
// ring missing those generations — the same hole a cross-process writer creates.
func TestVindexHybridFullRebuildFallback(t *testing.T) {
	const (
		n   = 400
		dim = 16
		k   = 10
	)
	vecs := randVecs(n, dim, 7)
	queries := randVecs(30, dim, 88)

	db, ix := newTestIndex(t, dim, L2)
	ix.SetHybrid(true)
	insertAll(t, db, ix, vecs)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	for _, q := range queries {
		_, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
	}
	require.NoError(t, rtx.Rollback())
	baseGen := ix.l0base.Load().gen

	// Writes with the ring NOT recording (hybrid off) → their generations are
	// absent from the ring, so coverage of (baseGen, newGen] must fail.
	ix.SetHybrid(false)
	more := randVecs(120, dim, 999)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	for i, v := range more {
		require.NoError(t, ix.Insert(wtx, docID(n+i), v))
	}
	require.NoError(t, wtx.Commit())
	ix.SetHybrid(true)

	rtx, err = db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	searchHybridEqualsBtreeTx(t, ix, rtx, queries, k)

	require.Greater(t, ix.l0base.Load().gen, baseGen,
		"base must be rebuilt when the dirty ring can't cover the generation gap")
}

// TestVindexHybridStaleSnapshot checks the snapshot-alignment contract: a search
// on an OLD read snapshot must use a mirror aligned to that snapshot, never the
// newer graph (which would dereference labels not yet visible). We hold an old
// read tx open across a writer commit, then search the old tx with hybrid on.
func TestVindexHybridStaleSnapshot(t *testing.T) {
	const (
		n   = 800
		dim = 16
		k   = 10
	)
	vecs := randVecs(n, dim, 3)
	queries := randVecs(40, dim, 71)

	db, ix := newTestIndex(t, dim, L2)
	insertAll(t, db, ix, vecs)

	// Prime + publish a mirror at the current gen via a hybrid search.
	ix.SetHybrid(true)
	rtxOld, err := db.BeginRead()
	require.NoError(t, err)
	defer rtxOld.Rollback()
	for _, q := range queries {
		_, err := ix.Search(rtxOld, q, k, 64)
		require.NoError(t, err)
	}

	// A concurrent writer adds nodes and advances l0Gen + the published mirror.
	more := randVecs(300, dim, 999)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	for i, v := range more {
		require.NoError(t, ix.Insert(wtx, docID(n+i), v))
	}
	require.NoError(t, wtx.Commit())
	// Trigger a publish of the newer mirror via a fresh-snapshot hybrid search.
	rtxNew, err := db.BeginRead()
	require.NoError(t, err)
	for _, q := range queries {
		_, err := ix.Search(rtxNew, q, k, 64)
		require.NoError(t, err)
	}
	require.NoError(t, rtxNew.Rollback())

	// Now search the OLD snapshot with hybrid on: it must rebuild a mirror at the
	// old snapshot's gen (the published one is newer) and return exactly what a
	// btree search on the old snapshot returns — no errors, no phantom labels.
	for qi, q := range queries {
		ix.SetHybrid(false)
		base, err := ix.Search(rtxOld, q, k, 64)
		require.NoError(t, err)
		ix.SetHybrid(true)
		hyb, err := ix.Search(rtxOld, q, k, 64)
		require.NoError(t, err)

		require.Equal(t, len(base), len(hyb), "query %d count", qi)
		for i := range base {
			require.Equalf(t, base[i].DocID, hyb[i].DocID, "query %d hit %d", qi, i)
		}
	}
	ix.SetHybrid(false)
}
