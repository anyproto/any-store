package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Phase 3: per-field weights (BM25F). The v2 postings store per-field TF; the
// scorer combines them as Σ weight_f · tf_f. An unweighted index reduces exactly
// to classic BM25 (covered by the parity tests); these cover the weighting.

func ftsWeightedColl(t *testing.T, fx *fixture, name string, weights map[string]float64) Collection {
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind:     IndexKindFulltext,
		Fields:   []string{"title", "body"},
		Fulltext: &FulltextParams{Weights: weights},
	}))
	insertJSON(t, coll,
		`{"id":"t","title":"alpha","body":"filler filler filler"}`,      // match in title
		`{"id":"b","title":"filler","body":"alpha filler filler"}`,      // match in body
		`{"id":"x","title":"nothing","body":"unrelated text entirely"}`, // no match
	)
	return coll
}

func TestFtsWeights_TitleBoostChangesRanking(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()

	// With a strong title boost, the title match (t) must outrank the body match (b).
	boosted := ftsWeightedColl(t, fx, "boost", map[string]float64{"title": 8})
	ids, _ := collectIter(t, boosted.Find(`{"$text":{"$search":"alpha"}}`))
	assert.Equal(t, []string{"t", "b"}, ids)

	// With the opposite boost (body >> title), the body match wins.
	bodyBoost := ftsWeightedColl(t, fx, "bodyboost", map[string]float64{"body": 8})
	ids2, _ := collectIter(t, bodyBoost.Find(`{"$text":{"$search":"alpha"}}`))
	assert.Equal(t, []string{"b", "t"}, ids2)
}

func TestFtsWeights_UnweightedUnchanged(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	// No weights → default BM25; both match, x does not. (Sanity that the v2 format
	// + default scoring still returns the right match set.)
	plain := ftsWeightedColl(t, fx, "plain", nil)
	ids, scores := collectIter(t, plain.Find(`{"$text":{"$search":"alpha"}}`))
	assert.ElementsMatch(t, []string{"t", "b"}, ids)
	for _, s := range scores {
		assert.Greater(t, s, 0.0)
	}
}

func TestFtsWeights_Validation(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "v")
	require.NoError(t, err)
	assert.Error(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind: IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &FulltextParams{Weights: map[string]float64{"nosuchfield": 2}},
	}), "weight on unknown field must be rejected")
	assert.Error(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind: IndexKindFulltext, Fields: []string{"title"},
		Fulltext: &FulltextParams{Weights: map[string]float64{"title": -1}},
	}), "negative weight must be rejected")
}

func TestFtsWeights_SurviveReopen(t *testing.T) {
	tmpDir := t.TempDir()
	func() {
		fxLocal := newFixturePath(t, tmpDir)
		coll, err := fxLocal.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Kind: IndexKindFulltext, Fields: []string{"title", "body"},
			Fulltext: &FulltextParams{Weights: map[string]float64{"title": 4, "body": 1}},
		}))
		insertJSON(t, coll, `{"id":"a","title":"alpha","body":"beta"}`)
		require.NoError(t, fxLocal.Close())
	}()

	db, err := Open(ctx, tmpDir+"/any-store-test.db", nil)
	require.NoError(t, err)
	defer db.Close()
	collI, err := db.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	c := collI.(*collection)
	fxs := c.loadFtsIndexes()
	require.Len(t, fxs, 1)
	assert.Equal(t, 4.0, fxs[0].info.Fulltext.Weights["title"])
	assert.Equal(t, 1.0, fxs[0].info.Fulltext.Weights["body"])
}
