package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Phase 2: configurable BM25 b / k1 (FulltextParams). Default params reproduce
// the original scoring exactly (covered by the parity tests); these cover that a
// custom b actually changes length normalization and that params persist.

func ftsScoreByID(t *testing.T, coll Collection, search string) map[string]float64 {
	ids, scores := collectIter(t, coll.Find(map[string]any{"$text": map[string]any{"$search": search}}))
	m := make(map[string]float64, len(ids))
	for i, id := range ids {
		m[id] = scores[i]
	}
	return m
}

func TestFtsBM25_Validation(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "v")
	require.NoError(t, err)
	assert.Error(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind: IndexKindFulltext, Fields: []string{"body"}, Fulltext: &FulltextParams{B: 1.5},
	}), "B>1 must be rejected")
	assert.Error(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind: IndexKindFulltext, Fields: []string{"body"}, Fulltext: &FulltextParams{K1: -1},
	}), "negative K1 must be rejected")
}

// withB builds a fresh collection whose fts index uses the given b, then inserts
// one short and one long doc that each contain "alpha" exactly once.
func withB(t *testing.T, fx *fixture, name string, b float64) Collection {
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Kind: IndexKindFulltext, Fields: []string{"body"}, Fulltext: &FulltextParams{B: b},
	}))
	insertJSON(t, coll,
		`{"id":"s","body":"alpha"}`,
		`{"id":"l","body":"alpha beta gamma delta epsilon zeta eta theta iota kappa"}`,
	)
	return coll
}

func TestFtsBM25_BAffectsLengthNorm(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()

	// b high (default 0.75) penalizes the long doc more than b low (0.1), so the
	// short/long score ratio is larger at high b.
	highB := withB(t, fx, "high", 0.75)
	lowB := withB(t, fx, "low", 0.1)

	sh := ftsScoreByID(t, highB, "alpha")
	sl := ftsScoreByID(t, lowB, "alpha")
	require.Contains(t, sh, "s")
	require.Contains(t, sh, "l")

	ratioHigh := sh["s"] / sh["l"]
	ratioLow := sl["s"] / sl["l"]
	assert.Greater(t, ratioHigh, ratioLow,
		"higher b must favour the short doc more (ratioHigh=%.4f ratioLow=%.4f)", ratioHigh, ratioLow)
	// Sanity: at b=0.1 the two docs score nearly the same (length barely matters).
	assert.InDelta(t, 1.0, ratioLow, 0.15)
}

func TestFtsBM25_ParamsSurviveReopen(t *testing.T) {
	tmpDir := t.TempDir()
	func() {
		fxLocal := newFixturePath(t, tmpDir)
		coll, err := fxLocal.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
			Kind: IndexKindFulltext, Fields: []string{"body"}, Fulltext: &FulltextParams{B: 0.33, K1: 1.7},
		}))
		insertJSON(t, coll, `{"id":"a","body":"alpha beta"}`)
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
	assert.Equal(t, 0.33, fxs[0].info.Fulltext.B)
	assert.Equal(t, 1.7, fxs[0].info.Fulltext.K1)
}
