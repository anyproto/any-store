package vindex

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newQuantIndex(t testing.TB, dim int, q Quantization) (*btree.DB, *Index) {
	t.Helper()
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: Cosine, EfSearch: 64, Quantization: q}, 1)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	return db, ix
}

func recallOf(t *testing.T, db *btree.DB, ix *Index, vecs, queries [][]float32, k int) float64 {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	var recall float64
	for _, q := range queries {
		truth := bruteTopK(vecs, q, k)
		hits, err := ix.Search(rtx, q, k, 128)
		require.NoError(t, err)
		hit := 0
		for _, h := range hits {
			if truth[string(h.DocID)] {
				hit++
			}
		}
		recall += float64(hit) / float64(k)
	}
	return recall / float64(len(queries))
}

func TestVindexQuantInt8(t *testing.T) {
	const (
		n   = 3000
		dim = 32
		k   = 10
	)
	vecs := randVecs(n, dim, 7)
	queries := randVecs(100, dim, 99)

	dbF, ixF := newQuantIndex(t, dim, QuantNone)
	dbQ, ixQ := newQuantIndex(t, dim, QuantInt8)
	for _, db := range []struct {
		db *btree.DB
		ix *Index
	}{{dbF, ixF}, {dbQ, ixQ}} {
		wtx, err := db.db.BeginWrite()
		require.NoError(t, err)
		for i, v := range vecs {
			require.NoError(t, db.ix.Insert(wtx, docID(i), v))
		}
		require.NoError(t, wtx.Commit())
	}

	rF := recallOf(t, dbF, ixF, vecs, queries, k)
	rQ := recallOf(t, dbQ, ixQ, vecs, queries, k)
	t.Logf("recall@%d  float32=%.3f  int8=%.3f", k, rF, rQ)
	// The quantization guarantee is that int8 recall tracks float32 recall; the
	// absolute value reflects the (random, hard) dataset, not the storage format.
	assert.Greater(t, rQ, 0.5, "int8 recall sanity")
	assert.InDelta(t, rF, rQ, 0.05, "int8 recall should track float32")

	// storage: int8 :vec ~4x smaller
	rtxF, _ := dbF.BeginRead()
	sF, err := ixF.Stats(rtxF)
	require.NoError(t, err)
	rtxF.Rollback()
	rtxQ, _ := dbQ.BeginRead()
	sQ, err := ixQ.Stats(rtxQ)
	require.NoError(t, err)
	rtxQ.Rollback()

	require.Equal(t, QuantInt8, sQ.Quantization)
	vecF := sF.Vec.TotalPages()
	vecQ := sQ.Vec.TotalPages()
	t.Logf("vec namespace pages: float32=%d  int8=%d  (%.1fx smaller)", vecF, vecQ, float64(vecF)/float64(vecQ))
	assert.Less(t, vecQ, vecF, "int8 vec storage must be smaller")
}

func TestVindexQuantReopen(t *testing.T) {
	const dim = 48
	dir := t.TempDir() + "/q.db"
	vecs := randVecs(600, dim, 3)
	queries := randVecs(20, dim, 8)

	db, err := btree.Open(dir, btree.Options{})
	require.NoError(t, err)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: Cosine, Quantization: QuantInt8}, 1)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	wtx, _ = db.BeginWrite()
	for i, v := range vecs {
		require.NoError(t, ix.Insert(wtx, docID(i), v))
	}
	require.NoError(t, wtx.Commit())

	pre := make([][]Hit, len(queries))
	rtx, _ := db.BeginRead()
	for i, q := range queries {
		pre[i], _ = ix.Search(rtx, q, 10, 64)
	}
	rtx.Rollback()
	require.NoError(t, db.Close())

	db2, err := btree.Open(dir, btree.Options{})
	require.NoError(t, err)
	defer db2.Close()
	ix2, err := Open(db2, "vix", 1)
	require.NoError(t, err)
	require.Equal(t, QuantInt8, ix2.quant) // quant restored from meta
	rtx2, _ := db2.BeginRead()
	defer rtx2.Rollback()
	for i, q := range queries {
		post, err := ix2.Search(rtx2, q, 10, 64)
		require.NoError(t, err)
		require.Equal(t, len(pre[i]), len(post))
		for j := range post {
			assert.Equal(t, pre[i][j].DocID, post[j].DocID, "result drift after reopen (int8)")
		}
	}
}
