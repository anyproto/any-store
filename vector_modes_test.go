package anystore

import (
	"errors"
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

// vectorEqFilter builds the `{v: [..]}` equality filter used to issue a vector
// query through the normal Find pipeline.
func vectorEqFilter(qv []float32) query.Filter {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	for i, f := range qv {
		arr.SetArrayItem(i, a.NewNumberFloat64(float64(f)))
	}
	return query.Key{Path: []string{"v"}, Filter: query.NewCompValue(query.CompOpEq, arr)}
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

// TestVectorMode_BruteSearchNotYet asserts brute-force search returns the
// explicit not-implemented error (Phase 3 will replace this with a scan).
func TestVectorMode_BruteSearchNotYet(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	makeVectorIndex(t, coll, VectorModeBruteForce, dim)
	vecs := vrand(20, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// direct API
	_, err = coll.VectorSearch(ctx, "emb", vecs[0], 5, 64)
	assert.ErrorIs(t, err, errBruteVectorSearchNotImplemented)

	// query pipeline
	_, err = coll.Find(vectorEqFilter(vecs[0])).Limit(5).Iter(ctx)
	assert.ErrorIs(t, err, errBruteVectorSearchNotImplemented)
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

	hits, err := coll.VectorSearch(ctx, "emb", vecs[42], 1, 64)
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
	require.Error(t, err)
	assert.False(t, errors.Is(err, errBruteVectorSearchNotImplemented))
}
