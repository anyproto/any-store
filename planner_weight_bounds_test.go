/*
Index/Planner tests inspired by SQLite: where.test, where2.test, where7.test

Test scenario:
Tests weight computation edge cases and index bounds behavior — verifying
that the planner correctly handles tiebreaks, MaxIndexes, compound weight
chains, sort weight contributions, and hint boost overrides.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestIndex_WeightBounds_EqualWeightTiebreak(t *testing.T) {
	// Two single-field indexes on "a" and "b". Query {a:1, b:2}.
	// Both have equal query weight (10). Both appear in Explain.Indexes.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"a":1,"b":2}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// Both indexes should appear in explain
	require.True(t, len(explain.Indexes) >= 2, "expected at least 2 indexes in explain, got %d", len(explain.Indexes))

	// Both should have non-negative weight
	for _, idx := range explain.Indexes {
		assert.True(t, idx.Weight >= 0, "index %s has negative weight %d", idx.Name, idx.Weight)
	}

	// Verify correctness
	count, err := coll.Find(`{"a":1,"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.True(t, count >= 1, "expected at least 1 result, got %d", count)
}

func TestIndex_WeightBounds_MaxTwoIndexes(t *testing.T) {
	// Create 5 indexes on fields a,b,c,d,e. Query using all 5 fields.
	// Explain shows at most 2 used (MaxIndexes=2 default).
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	for _, field := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{field}}))
	}

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d,"d":%d,"e":%d}`,
			i, i%10, i%7, i%5, i%3, i%2,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"a":1,"b":2,"c":3,"d":1,"e":0}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// Count used indexes
	usedCount := 0
	for _, idx := range explain.Indexes {
		if idx.Used {
			usedCount++
		}
	}
	assert.True(t, usedCount <= 2, "expected at most 2 used indexes, got %d", usedCount)
	assert.True(t, usedCount >= 1, "expected at least 1 used index, got %d", usedCount)
}

func TestIndex_WeightBounds_CompoundWeightChain(t *testing.T) {
	// Index (a,b,c). Run 3 queries with increasing specificity.
	// Verify weights increase: {a:1} < {a:1,b:2} < {a:1,b:2,c:0}.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b", "c"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%10, i%7, i%5,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Weight for {a:1}: chain 10, break: 10-1=9 (two remaining fields)
	explain1, err := coll.Find(`{"a":1}`).Explain(ctx)
	require.NoError(t, err)
	require.True(t, len(explain1.Indexes) >= 1)
	weight1 := explain1.Indexes[0].Weight

	// Weight for {a:1, b:2}: chain 10 → 20, break: 20-1=19
	explain2, err := coll.Find(`{"a":1,"b":2}`).Explain(ctx)
	require.NoError(t, err)
	require.True(t, len(explain2.Indexes) >= 1)
	weight2 := explain2.Indexes[0].Weight

	// Weight for {a:1, b:2, c:0}: chain 10 → 20 → 40
	explain3, err := coll.Find(`{"a":1,"b":2,"c":0}`).Explain(ctx)
	require.NoError(t, err)
	require.True(t, len(explain3.Indexes) >= 1)
	weight3 := explain3.Indexes[0].Weight

	t.Logf("Weights: {a:1}=%d, {a:1,b:2}=%d, {a:1,b:2,c:0}=%d", weight1, weight2, weight3)

	assert.True(t, weight2 > weight1, "weight for 2 fields (%d) should exceed 1 field (%d)", weight2, weight1)
	assert.True(t, weight3 > weight2, "weight for 3 fields (%d) should exceed 2 fields (%d)", weight3, weight2)
}

func TestIndex_WeightBounds_SortWeightVsQueryWeight(t *testing.T) {
	// Index on "a". Compare weight of query {a:5} vs query {a:5} + Sort("a").
	// The combined one should have higher weight.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Query-only weight: 10
	explainNoSort, err := coll.Find(`{"a":5}`).Explain(ctx)
	require.NoError(t, err)
	require.True(t, len(explainNoSort.Indexes) >= 1)
	weightNoSort := explainNoSort.Indexes[0].Weight

	// Query + sort weight: 10 + 11 = 21
	explainWithSort, err := coll.Find(`{"a":5}`).Sort("a").Explain(ctx)
	require.NoError(t, err)
	require.True(t, len(explainWithSort.Indexes) >= 1)
	weightWithSort := explainWithSort.Indexes[0].Weight

	t.Logf("Weights: query-only=%d, query+sort=%d", weightNoSort, weightWithSort)
	assert.True(t, weightWithSort > weightNoSort,
		"query+sort weight (%d) should be higher than query-only weight (%d)", weightWithSort, weightNoSort)
}

func TestIndex_WeightBounds_HintBoostOverridesWeight(t *testing.T) {
	// Index on "a" (high natural weight for query {a:1}), index on "b" (low weight).
	// IndexHint on "b" with Boost=100 makes "b" the primary (first Used) index.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Without hint: both indexes have weight 10 for their respective fields
	explainNoHint, err := coll.Find(`{"a":1,"b":2}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("No hint plan:", explainNoHint.Sql)

	// With hint: boost "b" by 100
	explainWithHint, err := coll.Find(`{"a":1,"b":2}`).
		IndexHint(IndexHint{IndexName: "b", Boost: 100}).
		Explain(ctx)
	require.NoError(t, err)
	t.Log("Hint plan:", explainWithHint.Sql)

	// The boosted index "b" should be first in the list (highest weight)
	require.True(t, len(explainWithHint.Indexes) >= 2)
	assert.Equal(t, "b", explainWithHint.Indexes[0].Name,
		"index 'b' with boost should be first, got '%s'", explainWithHint.Indexes[0].Name)
	assert.True(t, explainWithHint.Indexes[0].Used, "boosted index 'b' should be used")

	// Verify correctness is unaffected by hint
	countNoHint, err := coll.Find(`{"a":1,"b":2}`).Count(ctx)
	require.NoError(t, err)
	countWithHint, err := coll.Find(`{"a":1,"b":2}`).
		IndexHint(IndexHint{IndexName: "b", Boost: 100}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoHint, countWithHint, "hint should not change result count")
}
