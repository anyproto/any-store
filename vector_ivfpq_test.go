package anystore

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// clusteredVecsAS draws n vectors from `centers` Gaussian clusters — realistic
// embedding-like structure (uniform-random vectors are near-equidistant in high
// dim and intrinsically hard for any ANN index).
func clusteredVecsAS(n, dim, centers int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	cs := make([][]float32, centers)
	for c := range cs {
		v := make([]float32, dim)
		for d := range v {
			v[d] = rng.Float32()*2 - 1
		}
		cs[c] = v
	}
	out := make([][]float32, n)
	for i := range out {
		base := cs[rng.Intn(centers)]
		v := make([]float32, dim)
		for d := range v {
			v[d] = base[d] + float32(rng.NormFloat64())*0.1
		}
		out[i] = v
	}
	return out
}

// makeIVFPQIndex inserts vecs, then creates an IVF-PQ index (which trains from the
// existing documents — the bulk-load pattern).
func makeIVFPQIndex(t *testing.T, coll Collection, vecs [][]float32, dim int) {
	t.Helper()
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb",
		Kind: IndexKindVector,
		Vector: &VectorParams{
			Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFPQ,
			Closure: 4, NProbe: 16,
		},
	}))
}

// TestVectorMode_IVFPQ_EndToEnd drives the btree-resident IVF-PQ index through the
// public Find() pipeline: self-retrieval, recall vs a brute oracle, _distance
// decoration, residual filtering, and update/delete.
func TestVectorMode_IVFPQ_EndToEnd(t *testing.T) {
	const (
		n   = 3000
		dim = 32
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 60, 7)
	makeIVFPQIndex(t, coll, vecs, dim)

	// Stats reports the new mode.
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "ivfpq", st.VectorIndexes[0].Mode)
	assert.Greater(t, st.VectorIndexes[0].SizeBytes, 0)

	// Self-query: a stored vector's nearest neighbour is itself (re-rank makes the
	// exact distance ~0), returned first.
	hits, err := vsearch(coll, "v", vecs[42], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	assert.InDelta(t, 0, hits[0].Distance, 1e-3)

	// Recall@k vs a brute-force L2 oracle. Queries are perturbed copies of random
	// base vectors (the realistic "find documents similar to this one" case), so the
	// true neighbours are well-defined near the source.
	rng := rand.New(rand.NewSource(123))
	queries := make([][]float32, 50)
	for i := range queries {
		src := vecs[rng.Intn(len(vecs))]
		q := make([]float32, dim)
		for d := range q {
			q[d] = src[d] + float32(rng.NormFloat64())*0.02
		}
		queries[i] = q
	}
	var recall float64
	for _, q := range queries {
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
	recall /= float64(len(queries))
	t.Logf("IVF-PQ recall@%d = %.3f", k, recall)
	assert.GreaterOrEqual(t, recall, 0.85, "IVF-PQ + re-rank should reach high recall on clustered data")

	// _distance decoration + ascending order through the pipeline.
	iter, err := coll.Find(vectorKnnFilter(vecs[7], 5)).Iter(ctx)
	require.NoError(t, err)
	var last float32 = -1
	got7 := false
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		if doc.Value().GetInt("id") == 7 {
			got7 = true
		}
		d := float32(iter.Distance())
		require.GreaterOrEqual(t, d, last, "results must be _distance-ascending")
		last = d
	}
	require.NoError(t, iter.Close())
	assert.True(t, got7, "self-query must return its own document")
}

// TestVectorMode_IVFPQ_FilterUpdateDelete covers a residual metadata filter plus
// update/delete maintenance on the IVF-PQ index.
func TestVectorMode_IVFPQ_FilterUpdateDelete(t *testing.T) {
	const (
		n   = 2000
		dim = 24
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 40, 13)
	makeIVFPQIndex(t, coll, vecs, dim)

	// Residual filter: vector clause + id predicate. Every returned doc must satisfy
	// the predicate.
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"id":{"$lt":100}}`, vknnJSON(vecs[5], 10, 0))).Iter(ctx)
	require.NoError(t, err)
	cnt := 0
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		require.Less(t, doc.Value().GetInt("id"), 100, "residual filter must hold")
		cnt++
	}
	require.NoError(t, iter.Close())
	assert.Positive(t, cnt)

	// Update doc 5's vector to be near doc 1900's cluster; querying with the new
	// vector must now return doc 5.
	newVec := vecs[1900]
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(5, newVec))))
	hits, err := vsearch(coll, "v", newVec, 3, 0)
	require.NoError(t, err)
	found := false
	for _, h := range hits {
		if string(h.DocId) == string(idBytesOf(5)) {
			found = true
		}
	}
	assert.True(t, found, "updated vector must be findable at its new location")

	// Delete doc 5; it must no longer be returned for that vector.
	require.NoError(t, coll.DeleteId(ctx, 5))
	hits, err = vsearch(coll, "v", newVec, 5, 0)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, string(idBytesOf(5)), string(h.DocId), "deleted doc must not be returned")
	}
}

// driftedRecall builds an IVF-PQ index (with the given CompactRatio) on cluster A,
// then batch-inserts a far-shifted cluster B and returns recall on B. With
// auto-rebuild enabled (ratio>0) the post-insert maybeAutoCompactVectors re-trains
// the codebooks to cover B; with it disabled (ratio 0) the frozen centroids miss B.
func driftedRecall(t *testing.T, collName string, compactRatio float64) float64 {
	return driftedRecallMode(t, collName, compactRatio, false)
}

func driftedRecallMode(t *testing.T, collName string, compactRatio float64, manual bool) float64 {
	const (
		dim = 24
		nA  = 2000
		nB  = 2000
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, collName)
	require.NoError(t, err)

	A := clusteredVecsAS(nA, dim, 40, 1)
	for i, v := range A {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
	}
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFPQ,
			Closure: 4, NProbe: 16, CompactRatio: compactRatio},
	}))

	// Far-shifted cluster B, inserted as one batch (one self-contained write → at
	// most one auto-rebuild afterwards).
	B := make([][]float32, nB)
	Bdocs := make([]*anyenc.Value, nB)
	for i := range B {
		src := clusteredVecsAS(nB, dim, 40, 2)[i]
		v := make([]float32, dim)
		for d := range v {
			v[d] = src[d] + 6
		}
		B[i] = v
		Bdocs[i] = anyenc.MustParseJson(vecDocJSON(nA+i, v))
	}
	require.NoError(t, coll.Insert(ctx, Bdocs...))

	if manual {
		require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))
	}

	all := append(append([][]float32{}, A...), B...)
	var recall float64
	for _, q := range B[:50] {
		truth := bruteIDs(all, q, k)
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
	return recall / 50
}

// TestVectorMode_IVFPQ_AutoRebuild proves the CompactRatio drift trigger: inserting
// a distribution the build-time centroids don't cover auto-rebuilds the index
// (when CompactRatio>0), recovering recall that a frozen index loses.
func TestVectorMode_IVFPQ_AutoRebuild(t *testing.T) {
	frozen := driftedRecall(t, "frozen", 0)     // no auto-rebuild
	rebuilt := driftedRecall(t, "rebuilt", 0.5) // auto-rebuild on drift ≥ 0.5
	t.Logf("recall on drifted cluster: frozen=%.3f, auto-rebuilt=%.3f", frozen, rebuilt)
	assert.Greater(t, rebuilt, frozen, "auto-rebuild must recover recall lost to centroid drift")
	assert.GreaterOrEqual(t, rebuilt, 0.9, "rebuilt index covers the new distribution")
}

// TestVectorMode_IVFPQ_Persist verifies the index round-trips through a reopen.
func TestVectorMode_IVFPQ_Persist(t *testing.T) {
	const (
		n   = 1500
		dim = 16
	)
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 30, 5)
	makeIVFPQIndex(t, coll, vecs, dim)
	require.NoError(t, fx.Close())

	db2, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	st, err := coll2.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "ivfpq", st.VectorIndexes[0].Mode)

	// Search still works after reopen (codebooks reloaded from :cb).
	hits, err := vsearch(coll2, "v", vecs[100], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(100), hits[0].DocId)
}
