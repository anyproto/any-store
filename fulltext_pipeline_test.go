package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests exercise the first-class FTS pipeline: $text combined with custom
// residual filters and custom sorts, plus the BM25 _score sidecar, Explain, and
// Count/Delete — all flowing through qplanner.BuildPlan(Fts) → FtsIter.

func ftsPipelineColl(t *testing.T) (*fixture, Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "p")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	insertJSON(t, coll,
		`{"id":"a","body":"london crash report","year":1920,"status":"open"}`,
		`{"id":"b","body":"london fog landing","year":1937,"status":"closed"}`,
		`{"id":"c","body":"london london tower","year":1950,"status":"open"}`,
		`{"id":"d","body":"paris sunshine","year":1960,"status":"open"}`,
	)
	return fx, coll
}

// collectIter runs a query and returns (ids, scores) in result order.
func collectIter(t *testing.T, q Query) ([]string, []float64) {
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []string
	var scores []float64
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, doc.Value().GetString("id"))
		scores = append(scores, iter.Score())
	}
	require.NoError(t, iter.Err())
	return ids, scores
}

func TestFtsPipeline_ResidualEqualityFilter(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// $text + an equality predicate on a non-indexed field (residual FilterIter).
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}))
	// b is "closed" → filtered out; a and c remain (both contain london).
	assert.ElementsMatch(t, []string{"a", "c"}, ids)
}

func TestFtsPipeline_ResidualRangeFilter(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text": map[string]any{"$search": "london"},
		"year":  map[string]any{"$gte": 1940},
	}))
	// Only c (1950) matches london AND year>=1940.
	assert.Equal(t, []string{"c"}, ids)
}

func TestFtsPipeline_ScoreSidecar(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Default relevance order: c (london×2, short doc) should outrank a and b,
	// and every hit must carry a positive BM25 score via Score().
	ids, scores := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	require.Equal(t, []string{"c", "a", "b"}, ids)
	require.Len(t, scores, 3)
	// c (london×2, short doc) strictly outranks; a and b tie (london×1, same len).
	assert.Greater(t, scores[0], scores[1], "c must outrank a")
	assert.GreaterOrEqual(t, scores[1], scores[2], "non-increasing")
	assert.InDelta(t, scores[1], scores[2], 1e-12, "a and b tie")
	for _, s := range scores {
		assert.Greater(t, s, 0.0)
	}
}

func TestFtsPipeline_SortByRealFieldAscending(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// $text matches a,b,c; an explicit real-field sort (year asc) must override
	// the relevance order — i.e. the planner inserts a SortIter.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`).Sort("year"))
	assert.Equal(t, []string{"a", "b", "c"}, ids) // 1920, 1937, 1950
}

func TestFtsPipeline_SortByRealFieldDescending(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`).Sort("-year"))
	assert.Equal(t, []string{"c", "b", "a"}, ids)
}

func TestFtsPipeline_TextScoreSortIsRelevanceOrder(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Explicit {$meta:textScore} sort == default relevance order.
	idsDefault, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	idsMeta, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`).
		Sort(`{"score":{"$meta":"textScore"}}`))
	assert.Equal(t, idsDefault, idsMeta)
}

func TestFtsPipeline_FilterAndSortCombined(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Residual filter (status=open) AND a real-field sort (-year) over $text.
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Sort("-year"))
	assert.Equal(t, []string{"c", "a"}, ids) // open londons, 1950 then 1920
}

func TestFtsPipeline_LimitAfterFilter(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	// Limit applies AFTER the residual filter (status=open leaves a,c; limit 1).
	ids, _ := collectIter(t, coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Sort("year").Limit(1))
	assert.Equal(t, []string{"a"}, ids) // earliest open london
}

func TestFtsPipeline_CountWithResidual(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	n, err := coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n) // a, c
}

func TestFtsPipeline_DeleteByText(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	res, err := coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "closed",
	}).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified) // only b (closed london)
	// b is gone; a and c (open londons) and d remain.
	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.ElementsMatch(t, []string{"a", "c"}, ids)
}

func TestFtsPipeline_UpdateByText(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	res, err := coll.Find(map[string]any{"$text": map[string]any{"$search": "paris"}}).
		Update(ctx, `{"$set":{"status":"archived"}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified)
	doc, err := coll.FindId(ctx, "d")
	require.NoError(t, err)
	assert.Equal(t, "archived", doc.Value().GetString("status"))
}

func TestFtsPipeline_Explain(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	exp, err := coll.Find(map[string]any{
		"$text":  map[string]any{"$search": "london"},
		"status": "open",
	}).Explain(ctx)
	require.NoError(t, err)
	// The plan must be driven by the FtsSearch source with the residual filter
	// downstream — no full scan.
	assert.Contains(t, exp.Sql, "FtsSearch")
}

func TestFtsPipeline_ScoreZeroForNonText(t *testing.T) {
	fx, coll := ftsPipelineColl(t)
	defer fx.finish()
	iter, err := coll.Find(`{"status":"open"}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, iter.Next())
	assert.Equal(t, 0.0, iter.Score(), "Score() is 0 for non-$text queries")
}
