package anystore

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Regression tests: tombstoning the HNSW entry
// point (update or delete of that doc) used to leave mt.entryLabel pointing at
// a dead node; with no live neighbour to route through (one-doc collections,
// or repeated entry churn) every subsequent search returned empty forever.
// tombstoneLabel now repoints the entry to a live node (or clears it so the
// next insert re-seeds).

func entryRepointColl(t *testing.T, dim int) (*fixture, Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	return fx, coll
}

// selfTop1 asserts that searching with id's own stored vector returns id as
// the closest hit.
func selfTop1(t *testing.T, coll Collection, id int, vec []float32) {
	t.Helper()
	hits, err := vsearch(coll, "v", vec, 5, 0)
	require.NoError(t, err)
	require.NotEmpty(t, hits, "search returned no hits (dead entry point?) for id %d", id)
	assert.Equal(t, idBytesOf(id), hits[0].DocId, "id %d must self-retrieve as top-1", id)
}

func TestVectorEntryRepoint_SingleDocUpsert(t *testing.T) {
	const dim = 16
	vecs := vrand(4, dim, 1)

	t.Run("cross_tx", func(t *testing.T) {
		_, coll := entryRepointColl(t, dim)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(0, vecs[0]))))
		require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(0, vecs[1]))))
		selfTop1(t, coll, 0, vecs[1])
	})

	t.Run("same_tx", func(t *testing.T) {
		fx, coll := entryRepointColl(t, dim)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(vecDocJSON(0, vecs[0]))))
		require.NoError(t, coll.UpsertOne(tx.Context(), anyenc.MustParseJson(vecDocJSON(0, vecs[2]))))
		require.NoError(t, tx.Commit())
		selfTop1(t, coll, 0, vecs[2])
	})
}

// TestVectorEntryRepoint_MixedTxSequence is the exact 10-op single-tx sequence
// from the original crash-fuzz catch: interleaved insert/upsert/update/delete
// of the same keys, which used to leave LiveCount=4 with every search empty.
func TestVectorEntryRepoint_MixedTxSequence(t *testing.T) {
	const dim = 16
	vecs := vrand(16, dim, 7)
	fx, coll := entryRepointColl(t, dim)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	c := tx.Context()
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(525, vecs[0]))))
	require.NoError(t, coll.UpsertOne(c, anyenc.MustParseJson(vecDocJSON(525, vecs[1]))))
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(1235, vecs[2]))))
	require.NoError(t, coll.UpsertOne(c, anyenc.MustParseJson(vecDocJSON(1235, vecs[3]))))
	require.NoError(t, coll.UpdateOne(c, anyenc.MustParseJson(vecDocJSON(525, vecs[4]))))
	require.NoError(t, coll.DeleteId(c, 525))
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(1696, vecs[5]))))
	require.NoError(t, coll.UpdateOne(c, anyenc.MustParseJson(vecDocJSON(1696, vecs[6]))))
	require.NoError(t, coll.UpsertOne(c, anyenc.MustParseJson(vecDocJSON(1714, vecs[7]))))
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(1258, vecs[8]))))
	require.NoError(t, tx.Commit())

	selfTop1(t, coll, 1235, vecs[3])
	selfTop1(t, coll, 1696, vecs[6])
	selfTop1(t, coll, 1714, vecs[7])
	selfTop1(t, coll, 1258, vecs[8])
	// deleted key never resurfaces
	hits, err := vsearch(coll, "v", vecs[4], 4, 0)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, idBytesOf(525), h.DocId, "deleted doc resurfaced")
	}
}

// TestVectorEntryRepoint_DeleteEntryChain deletes docs in insertion order (the
// first doc is the initial entry; each deletion may hit the repointed entry
// again), verifying survivors stay searchable down to a single doc, through
// empty, and after re-population.
func TestVectorEntryRepoint_DeleteEntryChain(t *testing.T) {
	const (
		n   = 30
		dim = 16
	)
	vecs := vrand(n+4, dim, 3)
	_, coll := entryRepointColl(t, dim)
	for i := 0; i < n; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vecs[i]))))
	}
	for del := 0; del < n-1; del++ {
		require.NoError(t, coll.DeleteId(ctx, del))
		survivor := del + 1
		selfTop1(t, coll, survivor, vecs[survivor])
	}
	// one live doc left
	selfTop1(t, coll, n-1, vecs[n-1])

	// delete the last doc: searches must return empty, not error
	require.NoError(t, coll.DeleteId(ctx, n-1))
	hits, err := vsearch(coll, "v", vecs[0], 5, 0)
	require.NoError(t, err)
	assert.Empty(t, hits, "no live docs -> no hits")

	// re-populate: the next insert re-seeds the entry
	for i := 0; i < 4; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(1000+i, vecs[n+i]))))
		selfTop1(t, coll, 1000+i, vecs[n+i])
	}
}
