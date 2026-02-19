/*
Index/Planner tests inspired by SQLite: where.test, where2.test, where7.test

Test scenario:
Tests query planner selection logic — verifying that the planner picks the
best index for various filter/sort combinations. Tests requiring imperative
logic (named/unique indexes, mid-test index creation, detailed weight
comparisons, known-issue workarounds, multi-index weight checks).

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestIndex_PlannerSelection_UniquePreferred(t *testing.T) {
	// Unique index gets +1 weight bonus, so it should be preferred
	// over a non-unique index on the same field.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a_nouni", Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a_uni", Fields: []string{"a"}, Unique: true}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"a":42}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// Unique index should have higher weight and be picked
	// Unique + point lookup → CoverLookup
	assert.Contains(t, explain.Sql, "CoverLookup(a_uni)")

	// Verify the unique index has higher weight than non-unique
	var uniWeight, nonUniWeight int
	for _, idx := range explain.Indexes {
		if idx.Name == "a_uni" {
			uniWeight = idx.Weight
		}
		if idx.Name == "a_nouni" {
			nonUniWeight = idx.Weight
		}
	}
	assert.Greater(t, uniWeight, nonUniWeight, "unique index should have higher weight")

	// Verify correctness
	count, err := coll.Find(`{"a":42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_PlannerSelection_ReverseSortUsesIndex(t *testing.T) {
	// Descending sort with an ascending index should use reverse scan.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Sort descending: index stores ascending, so planner should set reverse=true
	explain, err := coll.Find(nil).Sort("-a").Limit(5).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// Should use IndexScan with reverse, not a Sort iterator
	assert.Contains(t, explain.Sql, "IndexScan(a)")
	assert.NotContains(t, explain.Sql, "-> Sort", "reverse sort should use index reverse scan, not in-memory sort")

	// Verify results are descending
	vals := collectField(t, coll.Find(nil).Sort("-a").Limit(5), "a")
	require.Len(t, vals, 5)
	for i := 1; i < len(vals); i++ {
		assert.True(t, vals[i-1] >= vals[i], "not reverse-sorted at position %d: %s < %s", i, vals[i-1], vals[i])
	}
}

func TestIndex_PlannerSelection_FilterAndSort_DifferentFields(t *testing.T) {
	// Filter on field A, sort on field B. The planner must choose:
	// either use index A for filtering (then in-memory sort on B),
	// or give up the filter index for sort coverage.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Filter on a=5, sort by b. Weight: a gets query weight 10, b gets sort weight 11.
	// b has higher weight so it might be primary. Either way, results must be correct.
	explain, err := coll.Find(`{"a":5}`).Sort("b").Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// Verify results are correct: all have a=5, sorted by b
	vals := collectField(t, coll.Find(`{"a":5}`).Sort("b"), "b")
	assert.Equal(t, 10, len(vals))
	for i := 1; i < len(vals); i++ {
		assert.True(t, vals[i-1] <= vals[i], "not sorted by b at %d: %s > %s", i, vals[i-1], vals[i])
	}

	// Now add a compound index on (a, b) and verify it covers both filter+sort
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	explain2, err := coll.Find(`{"a":5}`).Sort("b").Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan with compound:", explain2.Sql)

	// Compound (a,b) should cover both filter and sort with no in-memory sort
	assert.Contains(t, explain2.Sql, "IndexScan(a,b)")
	assert.NotContains(t, explain2.Sql, "-> Sort")
}

func TestIndex_PlannerSelection_IndexHintOverride(t *testing.T) {
	// IndexHint boost overrides natural weight selection
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Without hint, filter on both a and b — planner picks one naturally
	explainDefault, err := coll.Find(`{"a":5,"b":3}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Default plan:", explainDefault.Sql)

	// With hint boosting b by 100, force planner to use b
	explainHinted, err := coll.Find(`{"a":5,"b":3}`).
		IndexHint(IndexHint{IndexName: "b", Boost: 100}).
		Explain(ctx)
	require.NoError(t, err)
	t.Log("Hinted plan:", explainHinted.Sql)

	// Hinted plan should use index b as primary
	assert.Contains(t, explainHinted.Sql, "IndexScan(b)")

	// Verify the boosted index has highest weight
	require.True(t, len(explainHinted.Indexes) >= 2)
	assert.Equal(t, "b", explainHinted.Indexes[0].Name, "boosted index should be first (highest weight)")

	// Verify correctness is the same
	countDefault, err := coll.Find(`{"a":5,"b":3}`).Count(ctx)
	require.NoError(t, err)
	countHinted, err := coll.Find(`{"a":5,"b":3}`).
		IndexHint(IndexHint{IndexName: "b", Boost: 100}).
		Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countDefault, countHinted, "hinted and default should return same count")
}

func TestIndex_PlannerSelection_CompoundSortMatching(t *testing.T) {
	// Sort matching compound index order avoids in-memory sort;
	// mismatched direction or field order requires Sort iterator.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x", "y"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d,"y":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("matching order - no mem sort", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("x", "y").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(x,y)")
		assert.NotContains(t, explain.Sql, "-> Sort")
	})

	t.Run("reversed y direction - uses index anyway", func(t *testing.T) {
		// The planner's ExactSort check counts matching field names regardless
		// of direction mismatch (direction only affects weight bonus).
		// So Sort("x","-y") with index (x,y) still gets ExactSort=true.
		explain, err := coll.Find(nil).Sort("x", "-y").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(x,y)")
		// Verify results are returned (correctness)
		docs := collectDocs(t, coll.Find(nil).Sort("x", "-y"))
		assert.Len(t, docs, 50)
	})

	t.Run("wrong field order - still uses index", func(t *testing.T) {
		// Sort("y","x") with index (x,y): field chain breaks at position 0
		// but both fields exist in the index (non-chain +5 each = weight 10).
		// Planner's ExactSort bitmap check counts bits regardless of order.
		explain, err := coll.Find(nil).Sort("y", "x").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(x,y)")
		// Verify results are returned (correctness)
		docs := collectDocs(t, coll.Find(nil).Sort("y", "x"))
		assert.Len(t, docs, 50)
	})
}

func TestIndex_PlannerSelection_FilterSortOnCompound_NoBothCovered(t *testing.T) {
	// Compound index (a, b): filter on a + sort on b should use the compound
	// index with no separate Sort iterator (ExactSort when filter pins a, sort on b).
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i%20))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Filter a=2, sort by b. The compound index (a,b) first narrows by a,
	// and within each a=2 segment, entries are sorted by b.
	explain, err := coll.Find(`{"a":2}`).Sort("b").Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	assert.Contains(t, explain.Sql, "IndexScan(a,b)")

	// Verify results: all a=2, sorted by b (use numeric comparison)
	vals := collectField(t, coll.Find(`{"a":2}`).Sort("b"), "b")
	assert.Equal(t, 40, len(vals)) // 200/5
	for i := 1; i < len(vals); i++ {
		prev, _ := strconv.Atoi(vals[i-1])
		cur, _ := strconv.Atoi(vals[i])
		assert.True(t, prev <= cur, "not sorted at %d: %d > %d", i, prev, cur)
	}
}

func TestIndex_PlannerSelection_MultipleIndexes_BestWeight(t *testing.T) {
	// With indexes (a), (b), (a,b), (b,c) — verify planner picks best for each query
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b", "c"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%10, i%7, i%3))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("query a,b uses compound", func(t *testing.T) {
		explain, err := coll.Find(`{"a":5,"b":3}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		// (a,b) compound: weight 10*2=20 vs (a): 10 vs (b): 10
		assert.Contains(t, explain.Sql, "IndexScan(a,b)")
	})

	t.Run("query b,c uses compound b,c", func(t *testing.T) {
		explain, err := coll.Find(`{"b":3,"c":1}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		// (b,c) compound: weight 10*2=20 vs (b): 10
		assert.Contains(t, explain.Sql, "IndexScan(b,c)")
	})

	t.Run("query a only", func(t *testing.T) {
		explain, err := coll.Find(`{"a":5}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		// Both (a) and (a,b) have the same query weight for {a:5}: 10 vs 10-1=9
		// Actually (a,b) with only a match: chain break at b → weight = 10-1 = 9
		// So (a) wins with weight 10
		assert.Contains(t, explain.Sql, "IndexScan(a)")
	})
}

func TestIndex_PlannerSelection_CoverLookup_UniqueEquality(t *testing.T) {
	// Unique index with point equality lookup should use CoverLookup
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"email":"user50@test.com"}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	assert.Contains(t, explain.Sql, "CoverLookup")
	assert.NotContains(t, explain.Sql, "FullScan")

	// Verify correctness
	count, err := coll.Find(`{"email":"user50@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_PlannerSelection_CompoundSortPartialMatch(t *testing.T) {
	// Sort by prefix of compound index fields gives PartialSort
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b", "c"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%5, i%4, i%3))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("sort all three fields - ExactSort", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("a", "b", "c").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(a,b,c)")
		assert.NotContains(t, explain.Sql, "-> Sort")
	})

	t.Run("sort first two fields - PartialSort", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("a", "b").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		// PartialSort: index covers a,b but not all sort fields match exactly
		// Should still use index scan but might need a Sort iterator
		assert.Contains(t, explain.Sql, "IndexScan(a,b,c)")
	})

	t.Run("sort first field only - PartialSort", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("a").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		// Partial match with 1 of 3 fields
		assert.Contains(t, explain.Sql, "IndexScan(a,b,c)")
	})
}

func TestIndex_PlannerSelection_EqualityVsRange(t *testing.T) {
	// Equality filter with index should produce IndexScan, not FullScan
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"w"}}))

	for i := 1; i <= 100; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"w":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("equality uses index", func(t *testing.T) {
		explain, err := coll.Find(`{"w":50}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(w)")
		assert.NotContains(t, explain.Sql, "FullScan")
	})

	t.Run("range uses index", func(t *testing.T) {
		explain, err := coll.Find(`{"w":{"$gte":40,"$lte":60}}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(w)")
	})

	// Compare indexed vs non-indexed results for correctness
	t.Run("indexed vs non-indexed same results", func(t *testing.T) {
		fxNoIdx := newFixture(t)
		collNoIdx, err := fxNoIdx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for i := 1; i <= 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"w":%d}`, i, i))
			require.NoError(t, collNoIdx.Insert(ctx, doc))
		}

		countIdx, err := coll.Find(`{"w":50}`).Count(ctx)
		require.NoError(t, err)
		countNoIdx, err := collNoIdx.Find(`{"w":50}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, countNoIdx, countIdx)

		// FullScan for non-indexed
		explainNoIdx, err := collNoIdx.Find(`{"w":50}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explainNoIdx.Sql, "FullScan")
	})
}

func TestIndex_PlannerSelection_WeightExplain(t *testing.T) {
	// Verify Explain.Indexes contains correct weight/used info
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"a":5,"b":3}`).Explain(ctx)
	require.NoError(t, err)

	// Indexes should be sorted by weight descending
	require.True(t, len(explain.Indexes) >= 3, "expected at least 3 indexes in explain")
	for i := 1; i < len(explain.Indexes); i++ {
		assert.True(t, explain.Indexes[i-1].Weight >= explain.Indexes[i].Weight,
			"indexes should be sorted by weight descending, got %d >= %d at position %d",
			explain.Indexes[i-1].Weight, explain.Indexes[i].Weight, i)
	}

	// The compound index (a,b) should have the highest weight for {a:5, b:3}
	assert.Equal(t, "a,b", explain.Indexes[0].Name)
	assert.True(t, explain.Indexes[0].Used)

	// Log all weights for debugging
	for _, idx := range explain.Indexes {
		t.Logf("Index %s: weight=%d, used=%v", idx.Name, idx.Weight, idx.Used)
	}
}

func TestIndex_PlannerSelection_ExplainPlanString(t *testing.T) {
	// Verify the plan string format contains expected iterator names
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 30 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("filter plan has Filter", func(t *testing.T) {
		explain, err := coll.Find(`{"a":5}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		// IndexScan -> Filter
		assert.Contains(t, explain.Sql, "IndexScan")
		assert.Contains(t, explain.Sql, "Filter")
	})

	t.Run("sort+limit plan has Limit", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("a").Limit(5).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(a)")
		assert.Contains(t, explain.Sql, "Limit(5)")
	})

	t.Run("no sort plan with filter+limit", func(t *testing.T) {
		explain, err := coll.Find(`{"a":{"$gte":10}}`).Limit(3).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(a)")
		assert.True(t,
			strings.Contains(explain.Sql, "Limit(3)") || strings.Contains(explain.Sql, "Limit(offset="),
			"plan should contain Limit")
	})
}
