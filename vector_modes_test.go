package anystore

import (
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeVectorIndex creates a vector index of the given mode on field "v".
func makeVectorIndex(t *testing.T, coll Collection, mode VectorMode, dim int) {
	t.Helper()
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, Mode: mode},
	}))
}

// vectorKnnFilter builds the programmatic $knn filter (query.NewKnn) used to
// issue a vector query through the normal Find pipeline — the same construction
// the downstream indexer uses (no JSON involved).
func vectorKnnFilter(qv []float32, k int) query.Filter {
	return query.Key{Path: []string{"v"}, Filter: query.NewKnn(qv, k)}
}

// TestVectorMode_Persist verifies every mode round-trips through reopen and is
// reported by Stats.
func TestVectorMode_Persist(t *testing.T) {
	const dim = 8
	tmpDir := t.TempDir()
	cases := []struct {
		coll string
		mode VectorMode
		want string
	}{
		{"c_btree", VectorModeBTree, "btree"},
		{"c_hybrid", VectorModeHybrid, "hybrid"},
		{"c_brute", VectorModeBruteForce, "brute"},
	}

	fx := newFixturePath(t, tmpDir)
	for _, tc := range cases {
		coll, err := fx.CreateCollection(ctx, tc.coll)
		require.NoError(t, err)
		makeVectorIndex(t, coll, tc.mode, dim)
	}
	require.NoError(t, fx.Close())

	db2, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	for _, tc := range cases {
		coll, err := db2.Collection(ctx, tc.coll)
		require.NoError(t, err)
		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		require.Len(t, st.VectorIndexes, 1, tc.coll)
		assert.Equal(t, tc.want, st.VectorIndexes[0].Mode, tc.coll)
	}
}

// TestVectorMode_BruteNoStorage verifies brute-force keeps no index data: zero
// index bytes even after inserting documents, and writes never error.
func TestVectorMode_BruteNoStorage(t *testing.T) {
	const (
		dim = 8
		n   = 200
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	makeVectorIndex(t, coll, VectorModeBruteForce, dim)

	vecs := vrand(n, dim, 3)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	// update + delete are no-ops on the index, must not error.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(0, vecs[1]))))
	require.NoError(t, coll.DeleteId(ctx, 1))

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "brute", st.VectorIndexes[0].Mode)
	assert.Equal(t, 0, st.VectorIndexes[0].SizeBytes, "brute index must store no data")
	assert.Equal(t, 0, st.VectorIndexesSizeBytes)
	assert.Equal(t, dim, st.VectorIndexes[0].Dim)
}

// TestVectorMode_BruteExact verifies brute-force search is exact: it returns the
// true nearest neighbours (recall 1.0) via both the direct API and the Find
// pipeline, and applies a residual filter correctly.
func TestVectorMode_BruteExact(t *testing.T) {
	const (
		dim = 12
		n   = 400
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	makeVectorIndex(t, coll, VectorModeBruteForce, dim)
	vecs := vrand(n, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// direct API: exact top-1 self-retrieval + recall 1.0 vs brute oracle.
	hits, err := vsearch(coll, "v", vecs[42], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	assert.InDelta(t, 0, hits[0].Distance, 1e-3)

	queries := vrand(30, dim, 99)
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
	assert.Equal(t, 1.0, recall, "brute-force search must be exact")

	// query pipeline returns the same nearest doc, decorated with _distance.
	iter, err := coll.Find(vectorKnnFilter(vecs[7], 1)).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, iter.Next())
	doc, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, 7, doc.Value().GetInt("id"))
	assert.InDelta(t, 0, iter.Distance(), 1e-3)
}

// TestVectorMode_HybridBehavesLikeBTree confirms hybrid mode is a working index
// in Phase 1 (it currently shares the btree backend; the RAM mirror lands in
// Phase 2). Recall and self-retrieval must match the btree path.
func TestVectorMode_HybridBehavesLikeBTree(t *testing.T) {
	const (
		dim = 16
		n   = 500
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	makeVectorIndex(t, coll, VectorModeHybrid, dim)
	vecs := vrand(n, dim, 11)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	hits, err := vsearch(coll, "v", vecs[42], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, "hybrid", st.VectorIndexes[0].Mode)
	assert.Greater(t, st.VectorIndexes[0].SizeBytes, 0, "hybrid keeps the btree graph")
}

// TestVectorMode_UnknownRejected ensures an out-of-range mode is rejected.
func TestVectorMode_UnknownRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	err = coll.CreateIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: 8, Mode: VectorMode(99)},
	})
	assert.Error(t, err, "unknown vector mode must be rejected")
}

// CompactRatio (and the IVF tuning params) must survive a DB reopen: they used
// to be dropped by registerIndex/getIndexInfos, permanently disabling
// auto-compaction after any restart.
func TestVectorParams_PersistAcrossReopen(t *testing.T) {
	const dim = 8
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	want := &VectorParams{
		Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64,
		CompactRatio: 0.5, NProbe: 8,
	}
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "emb", Kind: IndexKindVector, Vector: want}))
	require.NoError(t, fx.Close())

	db2, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll, err = db2.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	vidxs := coll.(*collection).loadVectorIndexes()
	require.Len(t, vidxs, 1)
	require.NotNil(t, vidxs[0].info.Vector)
	assert.Equal(t, *want, *vidxs[0].info.Vector)
	assert.Equal(t, want.CompactRatio, vidxs[0].compactRatio)
}

// A same-name vector index with a DIFFERENT definition must surface
// ErrIndexMismatch from EnsureIndex — vector params weren't compared at all, so
// e.g. a dim upgrade silently kept the old index.
func TestVectorIndex_RedefinitionDetected(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2},
	}))

	// identical definition → idempotent no-op
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2},
	}))

	// changed dim → mismatch
	err = coll.EnsureIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim * 2, Metric: VectorL2},
	})
	require.ErrorIs(t, err, ErrIndexMismatch)

	// changed metric → mismatch
	err = coll.EnsureIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorCosine},
	})
	require.ErrorIs(t, err, ErrIndexMismatch)
}

// Same for fulltext scoring params (B/K1/weights are part of the definition).
func TestFulltextIndex_RedefinitionDetected(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Kind: IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &FulltextParams{Weights: map[string]float64{"title": 8}},
	}))

	// identical → no-op
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Kind: IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &FulltextParams{Weights: map[string]float64{"title": 8}},
	}))

	// different weights → mismatch
	err = coll.EnsureIndex(ctx, IndexInfo{
		Name: "ft", Kind: IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &FulltextParams{Weights: map[string]float64{"title": 2}},
	})
	require.ErrorIs(t, err, ErrIndexMismatch)
}
