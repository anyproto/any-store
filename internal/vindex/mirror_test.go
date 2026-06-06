package vindex

import (
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
