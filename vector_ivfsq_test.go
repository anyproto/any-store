package anystore

import (
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeIVFSQIndex inserts vecs, then creates an IVF-SQ index (int8 full vectors per
// cell, scanned directly — no PQ). IVF-SQ favours Closure=1 + higher NProbe.
func makeIVFSQIndex(t *testing.T, coll Collection, vecs [][]float32, dim int) {
	t.Helper()
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb",
		Kind: IndexKindVector,
		Vector: &VectorParams{
			Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFSQ, NProbe: 32,
		},
	}))
}

// TestVectorMode_IVFSQ_EndToEnd drives the IVF-SQ index through Find(): self-
// retrieval, recall vs a brute oracle, _distance ordering, and update/delete.
func TestVectorMode_IVFSQ_EndToEnd(t *testing.T) {
	const (
		n   = 3000
		dim = 32
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 60, 7)
	makeIVFSQIndex(t, coll, vecs, dim)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "ivfsq", st.VectorIndexes[0].Mode)
	assert.Greater(t, st.VectorIndexes[0].SizeBytes, 0)

	// Self-query: a stored vector's nearest neighbour is itself, returned first.
	hits, err := vsearch(coll, "v", vecs[42], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	// int8 storage → a vector's distance to itself is the small quantization error,
	// not exactly 0.
	assert.InDelta(t, 0, hits[0].Distance, 5e-2)

	// Recall@k vs a brute-force L2 oracle (perturbed-neighbour queries).
	rng := rand.New(rand.NewSource(123))
	var recall float64
	const nq = 50
	for qi := 0; qi < nq; qi++ {
		src := vecs[rng.Intn(len(vecs))]
		q := make([]float32, dim)
		for d := range q {
			q[d] = src[d] + float32(rng.NormFloat64())*0.02
		}
		truth := bruteIDs(vecs, q, k)
		hh, err := vsearch(coll, "v", q, k, 0)
		require.NoError(t, err)
		hit := 0
		for _, h := range hh {
			if truth[string(h.DocId)] {
				hit++
			}
		}
		recall += float64(hit) / float64(k)
	}
	recall /= nq
	t.Logf("IVF-SQ recall@%d = %.3f", k, recall)
	assert.GreaterOrEqual(t, recall, 0.9, "IVF-SQ should reach high recall on clustered data")

	// _distance ordering through the pipeline.
	iter, err := coll.Find(vectorEqFilter(vecs[7])).Limit(5).Iter(ctx)
	require.NoError(t, err)
	var last float32 = -1
	for iter.Next() {
		d := float32(iter.Distance())
		require.GreaterOrEqual(t, d, last, "results must be _distance-ascending")
		last = d
	}
	require.NoError(t, iter.Close())

	// Update doc 5 to a far cluster, then find it there; then delete it.
	newVec := vecs[2900]
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(5, newVec))))
	hits, err = vsearch(coll, "v", newVec, 3, 0)
	require.NoError(t, err)
	found := false
	for _, h := range hits {
		if string(h.DocId) == string(idBytesOf(5)) {
			found = true
		}
	}
	assert.True(t, found, "updated vector must be findable at its new location")
	require.NoError(t, coll.DeleteId(ctx, 5))
	hits, err = vsearch(coll, "v", newVec, 5, 0)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, string(idBytesOf(5)), string(h.DocId), "deleted doc must not be returned")
	}
}

// TestVectorMode_IVFSQ_Persist verifies an IVF-SQ index round-trips through reopen.
func TestVectorMode_IVFSQ_Persist(t *testing.T) {
	const (
		n   = 1500
		dim = 16
	)
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 30, 5)
	makeIVFSQIndex(t, coll, vecs, dim)
	require.NoError(t, fx.Close())

	db2, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)
	st, err := coll2.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, "ivfsq", st.VectorIndexes[0].Mode)
	hits, err := vsearch(coll2, "v", vecs[100], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(100), hits[0].DocId)
}
