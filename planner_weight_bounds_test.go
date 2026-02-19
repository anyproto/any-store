/*
Index/Planner tests inspired by SQLite: where.test, where2.test, where7.test

Test scenario:
Tests weight computation edge cases and index bounds behavior — verifying
that the planner correctly computes weights for irrelevant indexes, tiebreaks
equal-weight indexes, respects MaxIndexes, handles overlapping range bounds,
single-point bounds, empty results with IndexScan, compound weight chains,
sort weight contributions, hint boost overrides, and $ne filter bounds.

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

func TestIndex_WeightBounds_ZeroWeightNotUsed(t *testing.T) {
	// Index on "b", query {a:5} only — index is irrelevant, Explain shows FullScan.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"a":5}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// Index on "b" has no relevance for filter on "a" → should be FullScan
	assert.Contains(t, explain.Sql, "FullScan")
	assert.NotContains(t, explain.Sql, "IndexScan")

	// Verify index is not used
	for _, idx := range explain.Indexes {
		if idx.Name == "b" {
			assert.False(t, idx.Used, "index on 'b' should not be used for query on 'a'")
		}
	}

	// Verify query still returns correct result
	count, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

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

func TestIndex_WeightBounds_OverlappingRangeBounds(t *testing.T) {
	// Query {$or:[{a:{$gte:1,$lte:5}},{a:{$gte:3,$lte:8}}]} on 20 docs (a=0..19).
	// Verify results include a=1 through a=8 (union of two ranges).
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"$or":[{"a":{"$gte":1,"$lte":5}},{"a":{"$gte":3,"$lte":8}}]}`).Count(ctx)
	require.NoError(t, err)
	// Union of [1,5] and [3,8] = [1,8] → 8 values: 1,2,3,4,5,6,7,8
	assert.Equal(t, 8, count, "expected 8 results for overlapping ranges a=[1,5] OR a=[3,8]")

	// Verify the actual values
	vals := collectField(t, coll.Find(`{"$or":[{"a":{"$gte":1,"$lte":5}},{"a":{"$gte":3,"$lte":8}}]}`).Sort("a"), "a")
	assert.Len(t, vals, 8)
}

func TestIndex_WeightBounds_SinglePointBound(t *testing.T) {
	// Query {a:5} with index on "a", 20 docs (a=i). Verify exactly 1 result.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify plan uses IndexScan
	explain, err := coll.Find(`{"a":5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
	assert.NotContains(t, explain.Sql, "FullScan")
}

func TestIndex_WeightBounds_EmptyBoundsResult(t *testing.T) {
	// Query {a:{$gt:100}} on data where max a=19. Verify 0 results, still uses IndexScan.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"a":{"$gt":100}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Even with no results, planner should still choose IndexScan for an indexed field
	explain, err := coll.Find(`{"a":{"$gt":100}}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)
	assert.Contains(t, explain.Sql, "IndexScan")
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

func TestIndex_WeightBounds_BoundsWithNe(t *testing.T) {
	// Index on "a", 100 docs (a=i%10). Query {a:{$ne:5}} → expect 90 results.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"a":{"$ne":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 90, count, "expected 90 results for a != 5 (100 total - 10 with a=5)")

	// Verify via iteration
	vals := collectField(t, coll.Find(`{"a":{"$ne":5}}`).Sort("a"), "a")
	assert.Len(t, vals, 90)
	for _, v := range vals {
		assert.NotEqual(t, "5", v, "should not contain a=5")
	}
}
