package test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- from planner_selection_test.go ---

func TestIndex_PlannerSelection_UniquePreferred(t *testing.T) {
	// Unique index gets +1 weight bonus, so it should be preferred
	// over a non-unique index on the same field.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a_nouni", Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a_uni", Fields: []string{"a"}, Unique: true}))

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

	// Verify the unique index is the one used
	require.True(t, len(explain.Indexes) >= 2)
	assert.Equal(t, "a_uni", explain.Indexes[0].Name, "unique index should be used")
	assert.True(t, explain.Indexes[0].Used)

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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Filter on a=5, sort by b. Either way, results must be correct.
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

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
		IndexHint(anystore.IndexHint{IndexName: "b", Boost: 100}).
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
		IndexHint(anystore.IndexHint{IndexName: "b", Boost: 100}).
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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"x", "y"}}))

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

	t.Run("reversed y direction - requires in-memory sort", func(t *testing.T) {
		// Mixed sort directions (x asc, y desc) cannot be produced by a single
		// plain index scan direction, so planner should add Sort iterator.
		explain, err := coll.Find(nil).Sort("x", "-y").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "Sort")
		// Verify results are returned (correctness)
		docs := collectDocs(t, coll.Find(nil).Sort("x", "-y"))
		assert.Len(t, docs, 50)
	})

	t.Run("wrong field order - uses full scan with sort", func(t *testing.T) {
		// Sort("y","x") with index (x,y): field order mismatch means the
		// index can't provide the required sort order. CBO correctly falls
		// back to FullScan + in-memory Sort.
		explain, err := coll.Find(nil).Sort("y", "x").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "Sort")
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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b", "c"}}))

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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))

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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b", "c"}}))

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

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"w"}}))

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

func TestIndex_PlannerSelection_ExplainIndexes(t *testing.T) {
	// Verify Explain.Indexes contains correct used info
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"a":5,"b":3}`).Explain(ctx)
	require.NoError(t, err)

	// Used index should be first
	require.True(t, len(explain.Indexes) >= 3, "expected at least 3 indexes in explain")

	// The compound index (a,b) should be chosen for {a:5, b:3}
	assert.Equal(t, "a,b", explain.Indexes[0].Name)
	assert.True(t, explain.Indexes[0].Used)

	// Log all indexes for debugging
	for _, idx := range explain.Indexes {
		t.Logf("Index %s: cost=%.1f, used=%v", idx.Name, idx.Cost, idx.Used)
	}
}

func TestIndex_PlannerSelection_ExplainPlanString(t *testing.T) {
	// Verify the plan string format contains expected iterator names
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
		// With equality filter (selectivity ~1%) and small limit,
		// IndexSeek is cheaper than FullScan+Limit.
		explain, err := coll.Find(`{"a":5}`).Limit(3).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(a)")
		assert.True(t,
			strings.Contains(explain.Sql, "Limit(3)") || strings.Contains(explain.Sql, "Limit(offset="),
			"plan should contain Limit")
	})
}

func TestIndex_PlannerSelection_RichExplainPlan(t *testing.T) {
	// Verify the rich explain output contains plan details, cost breakdown, and candidates
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("rich explain is non-empty with expected sections", func(t *testing.T) {
		explain, err := coll.Find(`{"a":5}`).Sort("b").Limit(5).Explain(ctx)
		require.NoError(t, err)

		assert.NotEmpty(t, explain.Plan, "explain.Plan should not be empty")
		t.Log("Rich explain:\n", explain.Plan)

		// Header
		assert.Contains(t, explain.Plan, "Plan:")
		assert.Contains(t, explain.Plan, "Cost:")

		// Selectivity info
		assert.Contains(t, explain.Plan, "Selectivity:")

		// Iterator chain
		assert.Contains(t, explain.Plan, "Iterator:")

		// Candidates section
		assert.Contains(t, explain.Plan, "Candidates:")
		assert.Contains(t, explain.Plan, "FullScan")
		assert.Contains(t, explain.Plan, "[chosen]")
		assert.Contains(t, explain.Plan, "est_rows=")
	})

	t.Run("full scan explain has candidates", func(t *testing.T) {
		explain, err := coll.Find(`{"a":5}`).Explain(ctx)
		require.NoError(t, err)

		assert.NotEmpty(t, explain.Plan)
		assert.Contains(t, explain.Plan, "Candidates:")
		// Should have at least FullScan + IndexSeek(a) candidates
		assert.Contains(t, explain.Plan, "FullScan")
		assert.Contains(t, explain.Plan, "IndexSeek(a)")
		t.Log("Rich explain:\n", explain.Plan)
	})
}

// --- from planner_regression_test.go ---

func TestPlannerRegression_ReverseIndexSortOrder(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"-a"}})

	explain, err := coll.Find(nil).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(-a)")
	assert.NotContains(t, explain.Sql, "-> Sort")

	vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
	require.Len(t, vals, 100)
	for i := 1; i < len(vals); i++ {
		prev, perr := strconv.Atoi(vals[i-1])
		cur, cerr := strconv.Atoi(vals[i])
		require.NoError(t, perr)
		require.NoError(t, cerr)
		if prev < cur {
			t.Fatalf("expected descending order at %d: %d < %d", i, prev, cur)
		}
	}
}

func TestPlannerRegression_CompoundMixedDirectionSortOrder(t *testing.T) {
	// Create explicit x/y distribution used by this test.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"x", "y"}}))
	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d,"y":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(nil).Sort("x", "-y").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "Sort")

	iter, err := coll.Find(nil).Sort("x", "-y").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	prevX := -1
	prevY := int(^uint(0) >> 1) // max int
	first := true
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		xv := doc.Value().Get("x")
		yv := doc.Value().Get("y")
		require.NotNil(t, xv)
		require.NotNil(t, yv)
		x, xerr := strconv.Atoi(xv.String())
		y, yerr := strconv.Atoi(yv.String())
		require.NoError(t, xerr)
		require.NoError(t, yerr)

		if first {
			first = false
			prevX, prevY = x, y
			continue
		}

		if x == prevX {
			if prevY < y {
				t.Fatalf("expected y descending within same x: prev=(%d,%d) cur=(%d,%d)", prevX, prevY, x, y)
			}
		} else {
			if prevX > x {
				t.Fatalf("expected x ascending: prev=(%d,%d) cur=(%d,%d)", prevX, prevY, x, y)
			}
		}
		prevX, prevY = x, y
	}
	require.NoError(t, iter.Err())
}

func TestPlannerRegression_BoundsBuildMoreThanEightIndexes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	for i := 0; i < 9; i++ {
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{fmt.Sprintf("k%d", i)}}))
	}

	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k0":%d,"k1":%d,"k2":%d,"k3":%d,"k4":%d,"k5":%d,"k6":%d,"k7":%d,"k8":%d}`,
			i, i%3, i%4, i%5, i%6, i%7, i%8, i%9, i%10, i%11))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"k8": 3}`).Explain(ctx)
	require.NoError(t, err)

	// Expected: planner should use the k8 index. Current behavior (bug) falls back
	// to full scan because buildBoundsResult only preloads up to 8 indexes.
	assert.Contains(t, explain.Sql, "IndexScan(k8)")
	assert.NotContains(t, explain.Sql, "FullScan")
}

// --- from planner_weight_bounds_test.go ---

func TestIndex_CBO_EqualCostTiebreak(t *testing.T) {
	// Two single-field indexes on "a" and "b". Query {a:1, b:2}.
	// Both have equal cost. Both appear in Explain.Indexes.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	explain, err := coll.Find(`{"a":1,"b":2}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// Both indexes should appear in explain
	require.True(t, len(explain.Indexes) >= 2, "expected at least 2 indexes in explain, got %d", len(explain.Indexes))

	// Verify correctness
	count, err := coll.Find(`{"a":1,"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.True(t, count >= 1, "expected at least 1 result, got %d", count)
}

func TestIndex_CBO_MaxOneUsedIndex(t *testing.T) {
	// Create 5 indexes on fields a,b,c,d,e. Query using all 5 fields.
	// CBO picks exactly 1 index.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	for _, field := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{field}}))
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

	// Count used indexes — CBO picks exactly one
	usedCount := 0
	for _, idx := range explain.Indexes {
		if idx.Used {
			usedCount++
		}
	}
	assert.Equal(t, 1, usedCount, "CBO should pick exactly 1 index")
}

func TestIndex_CBO_HintBoostOverrides(t *testing.T) {
	// Index on "a" (high natural selectivity for query {a:1}), index on "b".
	// IndexHint on "b" with Boost=100 makes "b" the chosen index.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Without hint
	explainNoHint, err := coll.Find(`{"a":1,"b":2}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("No hint plan:", explainNoHint.Sql)

	// With hint: boost "b" by 100
	explainWithHint, err := coll.Find(`{"a":1,"b":2}`).
		IndexHint(anystore.IndexHint{IndexName: "b", Boost: 100}).
		Explain(ctx)
	require.NoError(t, err)
	t.Log("Hint plan:", explainWithHint.Sql)

	// The boosted index "b" should be used
	require.True(t, len(explainWithHint.Indexes) >= 2)
	assert.Equal(t, "b", explainWithHint.Indexes[0].Name,
		"index 'b' with boost should be first, got '%s'", explainWithHint.Indexes[0].Name)
	assert.True(t, explainWithHint.Indexes[0].Used, "boosted index 'b' should be used")

	// Verify correctness is unaffected by hint
	countNoHint, err := coll.Find(`{"a":1,"b":2}`).Count(ctx)
	require.NoError(t, err)
	countWithHint, err := coll.Find(`{"a":1,"b":2}`).
		IndexHint(anystore.IndexHint{IndexName: "b", Boost: 100}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoHint, countWithHint, "hint should not change result count")
}

// --- from qplanner_integration_test.go ---

// Helper to collect all docs from a query as JSON strings
func collectDocs(t testing.TB, q anystore.Query) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var results []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		results = append(results, doc.Value().String())
	}
	require.NoError(t, iter.Err())
	return results
}

// Helper to collect a specific field value from query results
func collectField(t testing.TB, q anystore.Query, field string) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var results []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		v := doc.Value().Get(field)
		if v != nil {
			results = append(results, v.String())
		}
	}
	require.NoError(t, iter.Err())
	return results
}

func setupTestCollection(t testing.TB, n int, indexes ...anystore.IndexInfo) anystore.Collection {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for _, idx := range indexes {
		require.NoError(t, coll.EnsureIndex(ctx, idx))
	}
	// Use coprime moduli so combinations of (a,b,c) overlap properly
	for i := range n {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d,"name":"item_%d","status":"%s"}`,
			i, i%10, i%7, i%3, i, []string{"active", "inactive", "pending"}[i%3],
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	return coll
}

// --- Correctness Tests ---

func TestQPlanner_SingleIndexFilter(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"a"}})

	t.Run("equality", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count) // 100/10 items with a=5

		explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("range gt", func(t *testing.T) {
		count, err := coll.Find(`{"a": {"$gt": 7}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 20, count) // a=8 and a=9, 10 each
	})

	t.Run("range gte lte", func(t *testing.T) {
		count, err := coll.Find(`{"a": {"$gte": 3, "$lte": 5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 30, count) // a=3,4,5
	})

	t.Run("in filter", func(t *testing.T) {
		count, err := coll.Find(`{"a": {"$in": [1, 3, 5]}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 30, count) // 3 values * 10 each
	})
}

func TestQPlanner_CompoundIndexFilter(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"a", "b"}})

	t.Run("both fields", func(t *testing.T) {
		// a=i%10=5, b=i%7=3 → i≡5(mod10), i≡3(mod7) → i≡45(mod70)
		// With 100 docs: i=45 → 1, possibly i=45 only. Check > 0.
		count, err := coll.Find(`{"a": 5, "b": 3}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count >= 1, "expected at least 1, got %d", count)

		explain, err := coll.Find(`{"a": 5, "b": 3}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("first field only - prefix usage", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)

		explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("second field only - less effective", func(t *testing.T) {
		count, err := coll.Find(`{"b": 3}`).Count(ctx)
		require.NoError(t, err)
		// b = i%7, so ~100/7 ≈ 14-15 items
		assert.True(t, count >= 14 && count <= 15)
	})
}

func TestQPlanner_SortWithIndex(t *testing.T) {
	coll := setupTestCollection(t, 50, anystore.IndexInfo{Fields: []string{"a"}})

	t.Run("sort ascending with index", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("a"), "a")
		require.Len(t, vals, 50)
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] <= vals[i], "not sorted at %d: %s > %s", i, vals[i-1], vals[i])
		}

		explain, err := coll.Find(nil).Sort("a").Explain(ctx)
		require.NoError(t, err)
		assert.NotContains(t, explain.Sql, "FullScan")
	})

	t.Run("sort descending with index", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
		require.Len(t, vals, 50)
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] >= vals[i], "not sorted desc at %d: %s < %s", i, vals[i-1], vals[i])
		}
	})
}

func TestQPlanner_CompoundSort(t *testing.T) {
	coll := setupTestCollection(t, 50, anystore.IndexInfo{Fields: []string{"a", "b"}})

	t.Run("sort by both fields ascending", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a", "b"))
		require.Len(t, docs, 50)
	})

	t.Run("sort with filter on first field", func(t *testing.T) {
		vals := collectField(t, coll.Find(`{"a": 3}`).Sort("b"), "b")
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] <= vals[i])
		}
	})
}

func TestQPlanner_ReverseIndex(t *testing.T) {
	coll := setupTestCollection(t, 50, anystore.IndexInfo{Fields: []string{"-a"}})

	t.Run("sort descending with reverse index", func(t *testing.T) {
		// A "-a" index stores the field bitwise-inverted, so Sort("-a")
		// matches the index's declared direction and is served by a FORWARD
		// scan of the inverted keys — descending order, no in-memory sort.
		vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
		require.Len(t, vals, 50)

		explain, err := coll.Find(nil).Sort("-a").Explain(ctx)
		require.NoError(t, err)
		// Uses index scan without an in-memory SortIter, which renders as
		// "-> Sort" (or "-> TopK(n)" when a Limit bounds it).
		assert.Contains(t, explain.Sql, "IndexScan(-a)")
		assert.NotContains(t, explain.Sql, "-> Sort", explain.Sql)
		assert.NotContains(t, explain.Sql, "TopK", explain.Sql)
	})

	t.Run("filter still works with reverse index", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}

func TestQPlanner_CompoundReverseIndex(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"a", "-b"}})

	t.Run("filter and sort matching compound reverse", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)
	})

	t.Run("sort a asc, b desc matches index", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a", "-b"))
		require.Len(t, docs, 100)
	})
}

func TestQPlanner_LimitOffset(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"a"}})

	t.Run("limit", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a").Limit(10))
		assert.Len(t, docs, 10)
	})

	t.Run("offset", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a").Offset(90))
		assert.Len(t, docs, 10)
	})

	t.Run("limit and offset", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a").Limit(5).Offset(10))
		assert.Len(t, docs, 5)
	})

	t.Run("limit with filter", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(`{"a": {"$gte": 5}}`).Limit(3))
		assert.Len(t, docs, 3)
	})
}

func TestQPlanner_MultipleIndexes(t *testing.T) {
	coll := setupTestCollection(t, 200,
		anystore.IndexInfo{Fields: []string{"a"}},
		anystore.IndexInfo{Fields: []string{"b"}},
	)

	t.Run("filter on both indexed fields", func(t *testing.T) {
		// a=i%10=3, b=i%7=2 → i≡3(mod10), i≡2(mod7) → i≡23(mod70)
		// With 200 docs: i=23,93,163 → 3 results (could be 2-3)
		count, err := coll.Find(`{"a": 3, "b": 2}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count >= 2, "expected at least 2, got %d", count)

		explain, err := coll.Find(`{"a": 3, "b": 2}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
	})

	t.Run("filter on one, sort on other", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(`{"a": 3}`).Sort("b"))
		assert.Equal(t, 20, len(docs)) // a=3 → 200/10=20
	})
}

func TestQPlanner_UniqueIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("exact lookup uses cover iter", func(t *testing.T) {
		explain, err := coll.Find(`{"email":"user25@test.com"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "CoverLookup")
	})

	t.Run("returns correct result", func(t *testing.T) {
		count, err := coll.Find(`{"email":"user25@test.com"}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("duplicate insert fails", func(t *testing.T) {
		err := coll.Insert(ctx, anyenc.MustParseJson(`{"id":999,"email":"user0@test.com"}`))
		assert.Error(t, err)
	})
}

func TestQPlanner_NoIndex_FullScan(t *testing.T) {
	coll := setupTestCollection(t, 100)

	t.Run("filter without index", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)

		explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
	})

	t.Run("sort without index", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("a").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Sort")
	})
}

func TestQPlanner_IndexHint(t *testing.T) {
	coll := setupTestCollection(t, 100,
		anystore.IndexInfo{Fields: []string{"a"}},
		anystore.IndexInfo{Fields: []string{"b"}},
	)

	t.Run("hint boosts index selection", func(t *testing.T) {
		explain, err := coll.Find(`{"a": 1, "b": 2}`).
			IndexHint(anystore.IndexHint{IndexName: "b", Boost: 100}).
			Explain(ctx)
		require.NoError(t, err)
		// The boosted index should appear first
		require.True(t, len(explain.Indexes) >= 2)
		assert.Equal(t, "b", explain.Indexes[0].Name)
	})
}

func TestQPlanner_ThreeFieldCompound(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b", "c"}}))

	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%5, i%4, i%3,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("all three fields", func(t *testing.T) {
		count, err := coll.Find(`{"a":1,"b":2,"c":0}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count > 0)
	})

	t.Run("first two fields", func(t *testing.T) {
		count, err := coll.Find(`{"a":1,"b":2}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count > 0)
	})

	t.Run("first field only", func(t *testing.T) {
		count, err := coll.Find(`{"a":1}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 40, count) // 200/5
	})

	t.Run("skip middle field", func(t *testing.T) {
		count, err := coll.Find(`{"a":1,"c":0}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count > 0)
	})
}

func TestQPlanner_FilterAndSortSameIndex(t *testing.T) {
	coll := setupTestCollection(t, 200, anystore.IndexInfo{Fields: []string{"a"}})

	t.Run("filter and sort on same field", func(t *testing.T) {
		vals := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a"), "a")
		assert.True(t, len(vals) > 0)
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] <= vals[i])
		}

		explain, err := coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a").Explain(ctx)
		require.NoError(t, err)
		// Should use index for both filter and sort — no SortIter, which
		// renders as "-> Sort" (or "-> TopK(n)" when a Limit bounds it).
		assert.NotContains(t, explain.Sql, "-> Sort", explain.Sql)
		assert.NotContains(t, explain.Sql, "TopK", explain.Sql)
	})
}

func TestQPlanner_OrFilter(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"a"}})

	count, err := coll.Find(`{"$or":[{"a":1},{"a":2}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count)
}

func TestQPlanner_NestedFieldIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"meta.score"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"meta":{"score":%d}}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"meta.score":{"$gte":200,"$lt":400}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count) // scores 200-390
}

func TestQPlanner_EmptyResult(t *testing.T) {
	coll := setupTestCollection(t, 50, anystore.IndexInfo{Fields: []string{"a"}})

	t.Run("no match with index", func(t *testing.T) {
		count, err := coll.Find(`{"a": 99}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("no match with sort", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(`{"a": 99}`).Sort("a"))
		assert.Len(t, docs, 0)
	})
}

func TestQPlanner_SortById(t *testing.T) {
	coll := setupTestCollection(t, 50)

	t.Run("sort by id ascending", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("id"), "id")
		require.Len(t, vals, 50)
		// IDs are integers stored as anyenc values; string comparison works
		// for same-length numbers but not mixed lengths. Verify count only.
		assert.Len(t, vals, 50)

		explain, err := coll.Find(nil).Sort("id").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
	})

	t.Run("sort by id descending", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("-id"), "id")
		require.Len(t, vals, 50)

		explain, err := coll.Find(nil).Sort("-id").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
	})
}

func TestQPlanner_DeleteWithIndex(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"a"}})

	res, err := coll.Find(`{"a": 5}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Modified)

	count, err := coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 90, count)
}

func TestQPlanner_UpdateWithIndex(t *testing.T) {
	coll := setupTestCollection(t, 100, anystore.IndexInfo{Fields: []string{"a"}})

	res, err := coll.Find(`{"a": 5}`).Update(ctx, `{"$set":{"a":50}}`)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Matched)
	assert.Equal(t, 10, res.Modified)

	count, err := coll.Find(`{"a": 50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count, err = coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestQPlanner_IndexScanCoveringFilter(t *testing.T) {
	// Compound index (a, b) with filter on b and sort by a.
	// IndexScan(a,b) with IndexFilterIter should filter b from the key
	// without fetching documents for non-matching entries.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	for i := range 1000 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d}`, i, i%100, i%50,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("covering filter correctness", func(t *testing.T) {
		// Force IndexScan(a,b) with covering filter via IndexHint
		q := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		)
		vals := collectField(t, q, "b")
		assert.Equal(t, 20, len(vals)) // 1000/50
		for _, v := range vals {
			assert.Equal(t, "25", v)
		}
	})

	t.Run("covering filter sorted order", func(t *testing.T) {
		q := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		)
		aVals := collectField(t, q, "a")
		for i := 1; i < len(aVals); i++ {
			prev, _ := strconv.Atoi(aVals[i-1])
			cur, _ := strconv.Atoi(aVals[i])
			assert.True(t, prev <= cur, "not sorted: a[%d]=%d > a[%d]=%d", i-1, prev, i, cur)
		}
	})

	t.Run("covering filter with limit", func(t *testing.T) {
		q := coll.Find(`{"b": 25}`).Sort("a").Limit(5).IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		)
		vals := collectField(t, q, "b")
		assert.Equal(t, 5, len(vals))
		for _, v := range vals {
			assert.Equal(t, "25", v)
		}
	})

	t.Run("same results as default plan", func(t *testing.T) {
		// Default plan (IndexSeek(b) + Sort) and forced plan should return same results
		defaultDocs := collectDocs(t, coll.Find(`{"b": 25}`).Sort("a"))
		forcedDocs := collectDocs(t, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		))
		assert.Equal(t, defaultDocs, forcedDocs)
	})

	t.Run("explain shows IndexFilter", func(t *testing.T) {
		explain, err := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexFilter")
	})
}

func TestQPlanner_IdSortOffsetSkip(t *testing.T) {
	coll := setupTestCollection(t, 200)

	// Collect all IDs sorted ascending for reference.
	allAsc := collectField(t, coll.Find(nil).Sort("id"), "id")
	require.Equal(t, 200, len(allAsc))
	// Collect all IDs sorted descending for reference.
	allDesc := collectField(t, coll.Find(nil).Sort("-id"), "id")
	require.Equal(t, 200, len(allDesc))

	t.Run("forward offset+limit", func(t *testing.T) {
		got := collectField(t, coll.Find(nil).Sort("id").Limit(10).Offset(50), "id")
		require.Equal(t, allAsc[50:60], got)
	})

	t.Run("reverse offset+limit", func(t *testing.T) {
		got := collectField(t, coll.Find(nil).Sort("-id").Limit(10).Offset(50), "id")
		require.Equal(t, allDesc[50:60], got)
	})

	t.Run("offset past end", func(t *testing.T) {
		got := collectField(t, coll.Find(nil).Sort("id").Limit(10).Offset(300), "id")
		require.Empty(t, got)
	})

	t.Run("offset near end partial results", func(t *testing.T) {
		got := collectField(t, coll.Find(nil).Sort("id").Limit(10).Offset(195), "id")
		require.Equal(t, allAsc[195:200], got)
	})

	t.Run("filter+offset uses slow path correctly", func(t *testing.T) {
		// With a filter, offset must not be batch-skipped.
		allFiltered := collectField(t, coll.Find(`{"a": 3}`).Sort("id"), "id")
		got := collectField(t, coll.Find(`{"a": 3}`).Sort("id").Limit(5).Offset(5), "id")
		require.Equal(t, allFiltered[5:10], got)
	})

	t.Run("explain shows skip", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("id").Limit(10).Offset(100).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "skip=100")
	})

	t.Run("explain no skip with filter", func(t *testing.T) {
		explain, err := coll.Find(`{"a": 3}`).Sort("id").Limit(10).Offset(100).Explain(ctx)
		require.NoError(t, err)
		assert.NotContains(t, explain.Sql, "skip=")
	})
}

// --- from pipeline_perf_test.go ---

func TestProfile_IndexedSortPipeline(t *testing.T) {
	ctx := context.Background()
	anystore.EnablePipelinePerfCounters(true)
	t.Cleanup(func() { anystore.EnablePipelinePerfCounters(false) })
	db := newFixture(t)

	coll, err := db.CreateCollection(ctx, "golden")
	require.NoError(t, err)

	// Build a representative dataset and indexes used by sort benchmarks.
	a := &anyenc.Arena{}
	batch := make([]*anyenc.Value, 0, 1000)
	for i := range 100000 {
		doc := a.NewObject()
		doc.Set("id", a.NewNumberInt(i))
		doc.Set("a", a.NewNumberInt(i%100))
		doc.Set("b", a.NewNumberInt((i/100)%50))
		doc.Set("c", a.NewNumberInt((i/5000)%10))
		doc.Set("val", a.NewNumberInt(i*7%1000))
		doc.Set("email", a.NewString(fmt.Sprintf("user%d@test.com", i)))
		batch = append(batch, doc)
		if len(batch) == cap(batch) {
			require.NoError(t, coll.Insert(ctx, batch...))
			batch = batch[:0]
			a.Reset()
		}
	}
	if len(batch) > 0 {
		require.NoError(t, coll.Insert(ctx, batch...))
	}
	require.NoError(t, coll.CreateIndex(ctx,
		anystore.IndexInfo{Fields: []string{"a"}},
		anystore.IndexInfo{Fields: []string{"b"}},
		anystore.IndexInfo{Fields: []string{"c"}},
		anystore.IndexInfo{Fields: []string{"a", "b"}},
		anystore.IndexInfo{Fields: []string{"a", "-b"}},
		anystore.IndexInfo{Fields: []string{"email"}, Unique: true},
	))

	type tc struct {
		name string
		q    anystore.Query
	}
	cases := []tc{
		{"Sort/WithIdx", coll.Find(nil).Sort("a").Limit(100)},
		{"Sort/DescWithIdx", coll.Find(nil).Sort("-a").Limit(100)},
		{"FilterSort/SimpleIdx", coll.Find(`{"a":{"$gte":40,"$lte":60}}`).Sort("a").Limit(100)},
		{"FilterSort/CompoundIdx", coll.Find(`{"a":50}`).Sort("b").Limit(100)},
	}

	for _, c := range cases {
		anystore.ResetPipelinePerfCounters()
		start := time.Now()
		var docs int

		// Run enough iterations to smooth noise, but keep test runtime acceptable.
		for range 200 {
			it, ierr := c.q.Iter(ctx)
			require.NoError(t, ierr)
			for it.Next() {
				_, derr := it.Doc()
				require.NoError(t, derr)
				docs++
			}
			require.NoError(t, it.Close())
		}
		elapsed := time.Since(start)
		s := anystore.SnapshotPipelinePerfCounters()
		p := s.Planner

		t.Logf("[%s] elapsed=%s docs=%d", c.name, elapsed, docs)
		t.Logf("[%s] index: calls=%d yields=%d ns=%d", c.name, p.IndexNextCalls, p.IndexYields, p.IndexNextNs)
		t.Logf("[%s] fetch: calls=%d yields=%d total_ns=%d lookup_ns=%d parse_ns=%d",
			c.name, p.FetchNextCalls, p.FetchYields, p.FetchNextNs, p.FetchLookupNs, p.FetchParseNs)
		t.Logf("[%s] filter: calls=%d yields=%d total_ns=%d eval_ns=%d",
			c.name, p.FilterNextCalls, p.FilterYields, p.FilterNextNs, p.FilterEvalNs)
		t.Logf("[%s] doc: calls=%d parsed_hits=%d fallbacks=%d fallback_seek_ns=%d fallback_parse_ns=%d",
			c.name, s.DocCalls, s.DocParsedHits, s.DocFallbacks, s.DocFallbackSeekNs, s.DocFallbackParseNs)

		require.Greater(t, s.DocCalls, uint64(0))
	}
}

func setupBenchCollection(b *testing.B, n int, indexes ...anystore.IndexInfo) anystore.Collection {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "bench")
	require.NoError(b, err)
	for _, idx := range indexes {
		require.NoError(b, coll.EnsureIndex(ctx, idx))
	}

	batchSize := 500
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var docs []*anyenc.Value
		for i := start; i < end; i++ {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
				`{"id":%d,"a":%d,"b":%d,"c":%d,"val":%d}`,
				i, i%100, i%50, i%10, i*7%1000,
			)))
		}
		require.NoError(b, coll.Insert(ctx, docs...))
	}
	return coll
}

func benchCount(b *testing.B, coll anystore.Collection, filter string) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		var q anystore.Query
		if filter == "" {
			q = coll.Find(nil)
		} else {
			q = coll.Find(filter)
		}
		_, err := q.Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchIter(b *testing.B, q anystore.Query) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		iter, err := q.Iter(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for iter.Next() {
			if _, err := iter.Doc(); err != nil {
				b.Fatal(err)
			}
		}
		if err := iter.Err(); err != nil {
			b.Fatal(err)
		}
		iter.Close()
	}
}

// --- Fullscan vs Index Filter Benchmarks ---

func BenchmarkFilter_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkFilter_SingleIndex_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkFilter_CompoundIndex_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a", "b"}})
	benchCount(b, coll, `{"a": 50, "b": 25}`)
}

func BenchmarkFilter_CompoundIndex_PrefixOnly_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a", "b"}})
	benchCount(b, coll, `{"a": 50}`)
}

// --- Range Filter ---

func BenchmarkRangeFilter_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchCount(b, coll, `{"a": {"$gte": 40, "$lte": 60}}`)
}

func BenchmarkRangeFilter_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": {"$gte": 40, "$lte": 60}}`)
}

// --- Presence Filter ---

// setupSparseBenchCollection adds n documents of which one in a hundred
// carries "opt", half of those as an explicit null.
func setupSparseBenchCollection(b *testing.B, n int, indexes ...anystore.IndexInfo) anystore.Collection {
	b.Helper()
	coll := setupBenchCollection(b, 0, indexes...)
	var docs []*anyenc.Value
	for i := range n {
		doc := fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%100)
		switch i % 200 {
		case 0:
			doc = fmt.Sprintf(`{"id":%d,"a":%d,"opt":%d}`, i, i%100, i)
		case 100:
			doc = fmt.Sprintf(`{"id":%d,"a":%d,"opt":null}`, i, i%100)
		}
		docs = append(docs, anyenc.MustParseJson(doc))
		if len(docs) == 500 || i == n-1 {
			require.NoError(b, coll.Insert(ctx, docs...))
			docs = docs[:0]
		}
	}
	return coll
}

func BenchmarkExists_FullScan_10k(b *testing.B) {
	coll := setupSparseBenchCollection(b, 10000)
	benchIter(b, coll.Find(`{"opt": {"$exists": true}}`))
}

func BenchmarkExists_SparseIndex_10k(b *testing.B) {
	coll := setupSparseBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"opt"}, Sparse: true})
	benchIter(b, coll.Find(`{"opt": {"$exists": true}}`))
}

func BenchmarkExistsCount_FullScan_10k(b *testing.B) {
	coll := setupSparseBenchCollection(b, 10000)
	benchCount(b, coll, `{"opt": {"$exists": true}}`)
}

func BenchmarkExistsCount_SparseIndex_10k(b *testing.B) {
	coll := setupSparseBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"opt"}, Sparse: true})
	benchCount(b, coll, `{"opt": {"$exists": true}}`)
}

// --- Sort Benchmarks ---

func BenchmarkSort_FullScan_1k(b *testing.B) {
	coll := setupBenchCollection(b, 1000)
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_Index_1k(b *testing.B) {
	coll := setupBenchCollection(b, 1000, anystore.IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a"))
}

// --- Filter + Sort Combined ---

func BenchmarkFilterSort_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(`{"a": {"$gte": 40, "$lte": 60}}`).Sort("a"))
}

func BenchmarkFilterSort_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(`{"a": {"$gte": 40, "$lte": 60}}`).Sort("a"))
}

// --- Limit with Sort ---

func BenchmarkSortLimit_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(nil).Sort("a").Limit(10))
}

func BenchmarkSortLimit_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a").Limit(10))
}

// --- Two Indexes: Filter + CoverFilter ---

func BenchmarkTwoIndexFilter_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000,
		anystore.IndexInfo{Fields: []string{"a"}},
		anystore.IndexInfo{Fields: []string{"c"}},
	)
	benchCount(b, coll, `{"a": 50, "c": 5}`)
}

func BenchmarkOneIndexFilter_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000,
		anystore.IndexInfo{Fields: []string{"a"}},
	)
	benchCount(b, coll, `{"a": 50, "c": 5}`)
}

// --- Unique Index Cover Lookup ---

func BenchmarkUniqueLookup_10k(b *testing.B) {
	fx := newFixture(b)
	coll, _ := fx.CreateCollection(ctx, "bench")
	coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true})
	var docs []*anyenc.Value
	for i := range 10000 {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i)))
	}
	require.NoError(b, coll.Insert(ctx, docs...))

	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"email":"user5000@test.com"}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFullScanLookup_10k(b *testing.B) {
	fx := newFixture(b)
	coll, _ := fx.CreateCollection(ctx, "bench")
	var docs []*anyenc.Value
	for i := range 10000 {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i)))
	}
	require.NoError(b, coll.Insert(ctx, docs...))

	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"email":"user5000@test.com"}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Unique vs Non-Unique Index Benchmarks ---

// setupBenchUniqueCollection creates a collection with n documents having unique "uid" field.
func setupBenchUniqueCollection(b *testing.B, n int, indexes ...anystore.IndexInfo) anystore.Collection {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "bench")
	require.NoError(b, err)
	for _, idx := range indexes {
		require.NoError(b, coll.EnsureIndex(ctx, idx))
	}

	batchSize := 500
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var docs []*anyenc.Value
		for i := start; i < end; i++ {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
				`{"id":%d,"uid":%d,"val":%d}`,
				i, i, i*7%1000,
			)))
		}
		require.NoError(b, coll.Insert(ctx, docs...))
	}
	return coll
}

func BenchmarkFind_UniqueIndex_10k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 10000, anystore.IndexInfo{Fields: []string{"uid"}, Unique: true})
	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"uid": 5000}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFind_NonUniqueIndex_10k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 10000, anystore.IndexInfo{Fields: []string{"uid"}})
	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"uid": 5000}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_NoIndex_1k(b *testing.B) {
	fx := newFixture(b)
	docs := make([]*anyenc.Value, 1000)
	for i := range 1000 {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"i_%d","uid":%d,"val":%d}`, i, i, i*7%1000))
	}
	b.ResetTimer()
	for n := range b.N {
		b.StopTimer()
		name := fmt.Sprintf("bench_%d", n)
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(b, err)
		b.StartTimer()
		require.NoError(b, coll.Insert(ctx, docs...))
	}
}

func BenchmarkInsert_UniqueIndex_1k(b *testing.B) {
	fx := newFixture(b)
	docs := make([]*anyenc.Value, 1000)
	for i := range 1000 {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"i_%d","uid":%d,"val":%d}`, i, i, i*7%1000))
	}
	b.ResetTimer()
	for n := range b.N {
		b.StopTimer()
		name := fmt.Sprintf("bench_%d", n)
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(b, err)
		require.NoError(b, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"uid"}, Unique: true}))
		b.StartTimer()
		require.NoError(b, coll.Insert(ctx, docs...))
	}
}

func BenchmarkInsert_NonUniqueIndex_1k(b *testing.B) {
	fx := newFixture(b)
	docs := make([]*anyenc.Value, 1000)
	for i := range 1000 {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"i_%d","uid":%d,"val":%d}`, i, i, i*7%1000))
	}
	b.ResetTimer()
	for n := range b.N {
		b.StopTimer()
		name := fmt.Sprintf("bench_%d", n)
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(b, err)
		require.NoError(b, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"uid"}}))
		b.StartTimer()
		require.NoError(b, coll.Insert(ctx, docs...))
	}
}

func BenchmarkUpdate_UniqueIndex_1k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 1000, anystore.IndexInfo{Fields: []string{"uid"}, Unique: true})
	b.ResetTimer()
	for n := range b.N {
		_, err := coll.Find(`{"uid": {"$lt": 100}}`).Update(ctx, fmt.Sprintf(`{"$set":{"val":%d}}`, n))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate_NonUniqueIndex_1k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 1000, anystore.IndexInfo{Fields: []string{"uid"}})
	b.ResetTimer()
	for n := range b.N {
		_, err := coll.Find(`{"uid": {"$lt": 100}}`).Update(ctx, fmt.Sprintf(`{"$set":{"val":%d}}`, n))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Selectivity: High vs Low ---

func BenchmarkHighSelectivity_Index_10k(b *testing.B) {
	// a%100 → 100 docs match (1% selectivity)
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkLowSelectivity_Index_10k(b *testing.B) {
	// c%10 → 1000 docs match (10% selectivity)
	coll := setupBenchCollection(b, 10000, anystore.IndexInfo{Fields: []string{"c"}})
	benchCount(b, coll, `{"c": 5}`)
}

func BenchmarkLowSelectivity_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchCount(b, coll, `{"c": 5}`)
}

// --- FilterSort/SortNonPrefix: IndexSeek(b)+Sort vs IndexScan(a,b) with covering filter ---

func BenchmarkFilterSort_SortNonPrefix_500k(b *testing.B) {
	// Setup: 500k docs, compound index (a, b), single-field index (b).
	// Query: Find({"b": 25}).Sort("a")
	// b = i%50, so ~10k docs match (2% selectivity).
	n := 500000
	coll := setupBenchCollection(b, n,
		anystore.IndexInfo{Fields: []string{"a", "b"}},
		anystore.IndexInfo{Fields: []string{"b"}},
	)

	// Sub-benchmarks: default plan vs forced IndexScan with covering filter
	b.Run("Default", func(b *testing.B) {
		// CBO picks: IndexSeek(b) + Sort
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a"))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		// Force: IndexScan(a,b) with covering filter on b
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}

func BenchmarkFilterSort_SortNonPrefix_10k(b *testing.B) {
	// Smaller dataset for quick iteration
	coll := setupBenchCollection(b, 10000,
		anystore.IndexInfo{Fields: []string{"a", "b"}},
		anystore.IndexInfo{Fields: []string{"b"}},
	)

	b.Run("Default", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a"))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}

func BenchmarkFilterSort_SortNonPrefix_Limit_500k(b *testing.B) {
	// LIMIT query: Find({"b": 25}).Sort("a").Limit(10) with 500k docs.
	// This is where covering filter + sorted index scan should shine:
	// scan ~500 index entries to find 10 matches vs fetch+sort 10k docs.
	n := 500000
	coll := setupBenchCollection(b, n,
		anystore.IndexInfo{Fields: []string{"a", "b"}},
		anystore.IndexInfo{Fields: []string{"b"}},
	)

	b.Run("Default", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").Limit(10))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").Limit(10).IndexHint(
			anystore.IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}

// ── Planner-CBO audit (act-19/20/21/22/23/24) ──
// act-19: $in multi-bound IndexScan Explain output and per-bound seek cost.
func TestIndex_Planner_InOperator_MultipleBounds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	explain, err := coll.Find(`{"a":{"$in":[5,10,15,20]}}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)
	t.Log("Rich:\n", explain.Plan)

	// Exactly 4 distinct fixed point bounds, one B-tree seek per value.
	assert.Equal(t,
		"IndexScan(a)[bounds=Bounds{['5','5'],['10','10'],['15','15'],['20','20']}] -> Fetch -> Filter -> Dedup(canonical)",
		explain.Sql)
	assert.NotContains(t, explain.Sql, "FullScan")
	// 4 seeks in the cost breakdown.
	assert.Contains(t, explain.Plan, "4×seek")

	// Correctness: exactly 4 docs, one per $in value, ascending scan order.
	count, err := coll.Find(`{"a":{"$in":[5,10,15,20]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count)
	assert.Equal(t, []string{"5", "10", "15", "20"},
		collectField(t, coll.Find(`{"a":{"$in":[5,10,15,20]}}`), "a"))
}

// act-20: Compound index equality-prefix + descending sort on the non-prefix
// field uses a reverse index scan, with no in-memory Sort.
func TestIndex_Planner_CompoundEqualityPrefix_ReverseSort(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"c", "a"}}))

	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"c":%d,"a":%d}`, i, i%10, i))))
	}

	explain, err := coll.Find(`{"c":5}`).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	assert.Contains(t, explain.Sql, "IndexScan(c,a)")
	assert.Contains(t, explain.Sql, "(reverse)")
	assert.Contains(t, explain.Sql, "[bounds=Bounds{['5','5']}]")
	assert.NotContains(t, explain.Sql, "-> Sort")

	// All c==5, a strictly descending.
	cVals := collectField(t, coll.Find(`{"c":5}`).Sort("-a"), "c")
	aVals := collectField(t, coll.Find(`{"c":5}`).Sort("-a"), "a")
	require.Len(t, aVals, 10)
	for _, cv := range cVals {
		assert.Equal(t, "5", cv)
	}
	for i := 1; i < len(aVals); i++ {
		prev, _ := strconv.Atoi(aVals[i-1])
		cur, _ := strconv.Atoi(aVals[i])
		assert.Greater(t, prev, cur, "a not strictly descending at %d", i)
	}
	assert.Equal(t, []string{"95", "85", "75", "65", "55", "45", "35", "25", "15", "5"}, aVals)
}

// act-21: No-filter no-sort query with usable indexes present still chooses FullScan.
func TestIndex_Planner_NoFilterNoSort_WithIndexesUsesFullScan(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))))
	}

	explain, err := coll.Find(nil).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	assert.Contains(t, explain.Sql, "FullScan")
	assert.NotContains(t, explain.Sql, "IndexScan")

	// Both indexes reported, none used.
	assert.Len(t, explain.Indexes, 2)
	for _, idx := range explain.Indexes {
		assert.False(t, idx.Used, "index %s should not be used", idx.Name)
	}

	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, count)
}

// act-22: IndexHint on an index with neither bounds nor sort coverage is a silent no-op.
func TestIndex_Planner_HintOnUnusableIndex_IsNoOp(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))))
	}

	explain, err := coll.Find(`{"a":5}`).
		IndexHint(anystore.IndexHint{IndexName: "b", Boost: 1000000}).
		Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)

	// The hint on the unusable (b) index cannot conjure a candidate; (a) is used.
	assert.Contains(t, explain.Sql, "IndexScan(a)")
	assert.NotContains(t, explain.Sql, "IndexScan(b)")
	require.NotEmpty(t, explain.Indexes)
	assert.Equal(t, "a", explain.Indexes[0].Name)
	assert.True(t, explain.Indexes[0].Used)

	countHinted, err := coll.Find(`{"a":5}`).
		IndexHint(anystore.IndexHint{IndexName: "b", Boost: 1000000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, countHinted)
	countPlain, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, countPlain)
}

// act-23: Partial-prefix query on a UNIQUE compound index uses range IndexScan,
// not the CoverLookup point-lookup path.
func TestIndex_Planner_UniqueCompound_PrefixUsesRangeScan(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}, Unique: true}))

	// Distinct (a,b) pairs: a=i%10, b=i -> 100 distinct pairs.
	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i))))
	}

	// Prefix-only: not a point lookup -> range IndexScan, returns all 10.
	explainPrefix, err := coll.Find(`{"a":5}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Prefix plan:", explainPrefix.Sql)
	assert.Contains(t, explainPrefix.Sql, "IndexScan(a,b)")
	assert.NotContains(t, explainPrefix.Sql, "CoverLookup")

	countPrefix, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, countPrefix)

	// Full equality -> CoverLookup point lookup, returns 1.
	explainFull, err := coll.Find(`{"a":5,"b":5}`).Explain(ctx)
	require.NoError(t, err)
	t.Log("Full plan:", explainFull.Sql)
	assert.Contains(t, explainFull.Sql, "CoverLookup(a,b)")

	countFull, err := coll.Find(`{"a":5,"b":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, countFull)
}

// act-24: FullScan with no-sort + small limit uses the early-termination scan
// estimate and beats the index seek path.
func TestIndex_Planner_FullScanLimit_EarlyTerminationCost(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	for i := range 2000 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%100))))
	}

	explain, err := coll.Find(`{"a":{"$gte":50}}`).Limit(5).Explain(ctx)
	require.NoError(t, err)
	t.Log("Plan:", explain.Sql)
	t.Log("Rich:\n", explain.Plan)

	assert.Equal(t, "FullScan(filtered) -> Limit(5)", explain.Sql)
	assert.NotContains(t, explain.Sql, "IndexScan(a)")

	// Correctness: exactly 5 docs in the window.
	docs := collectDocs(t, coll.Find(`{"a":{"$gte":50}}`).Limit(5))
	assert.Len(t, docs, 5)
}

// setupBenchRangeDesc creates a collection with n docs carrying unique string
// pks ("docNNNNNNNN"), the shape of the descending-range over-scan report:
// with the End bound dropped, Find(id>lo AND id<hi).Sort("-id").Limit(k)
// started the reverse cursor at the LAST key and filter-discarded every row
// above hi — O(rows above hi) instead of O(k).
func setupBenchRangeDesc(b *testing.B, n int) anystore.Collection {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "rangedesc")
	require.NoError(b, err)
	docs := make([]*anyenc.Value, 0, 1000)
	for i := 0; i < n; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":"doc%08d"}`, i)))
		if len(docs) == 1000 {
			require.NoError(b, coll.Insert(ctx, docs...))
			docs = docs[:0]
		}
	}
	if len(docs) > 0 {
		require.NoError(b, coll.Insert(ctx, docs...))
	}
	return coll
}

// BenchmarkRangeDescLimit_200k: descending two-sided pk range with a limit,
// near the bottom and near the top of the keyspace. With tight idBounds the
// two are equivalent (seek to hi, walk k rows); with the End dropped the
// near-bottom case scanned ~the whole collection per query.
func BenchmarkRangeDescLimit_200k(b *testing.B) {
	coll := setupBenchRangeDesc(b, 200_000)
	run := func(b *testing.B, filter string) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			it, err := coll.Find(filter).Sort("-id").Limit(10).Iter(ctx)
			require.NoError(b, err)
			n := 0
			for it.Next() {
				n++
			}
			require.NoError(b, it.Err())
			require.NoError(b, it.Close())
			if n != 10 {
				b.Fatalf("expected 10 rows, got %d", n)
			}
		}
	}
	b.Run("near_bottom", func(b *testing.B) {
		run(b, `{"id":{"$gt":"doc00000100","$lt":"doc00001000"}}`)
	})
	b.Run("near_top", func(b *testing.B) {
		run(b, `{"id":{"$gt":"doc00198000","$lt":"doc00199000"}}`)
	})
	b.Run("forward_count", func(b *testing.B) {
		// Forward no-limit overrun: Count(lo<id<hi) must stop at hi.
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			n, err := coll.Find(`{"id":{"$gt":"doc00000100","$lt":"doc00001000"}}`).Count(ctx)
			require.NoError(b, err)
			if n != 899 {
				b.Fatalf("expected 899, got %d", n)
			}
		}
	})
}

// A cost tie resolves on the index name, not on the order the collection lists
// its indexes in — creation order when live (EnsureIndex appends), catalog key
// order after a reopen — so every session picks the same plan.
func TestPlanner_PlanStableAcrossReopen(t *testing.T) {
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "plan-order-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	for _, name := range []string{"zzz", "mmm", "aaa"} {
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: name, Fields: []string{name}}))
	}
	// Identical cardinality on every indexed field => a genuine cost tie.
	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"zzz":%d,"mmm":%d,"aaa":%d}`, i, i%10, i%10, i%10))))
	}

	const tiedCond = `{"zzz":1,"mmm":1,"aaa":1}`
	freshNames := plannerIndexNames(coll)
	freshExplain, err := coll.Find(tiedCond).Explain(ctx)
	require.NoError(t, err)
	freshCosts := map[string]float64{}
	for _, ie := range freshExplain.Indexes {
		freshCosts[ie.Name] = ie.Cost
	}
	freshCount, err := coll.Find(tiedCond).Count(ctx)
	require.NoError(t, err)
	require.NoError(t, fx1.Close())

	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "objects")
	require.NoError(t, err)
	reopenedNames := plannerIndexNames(coll2)
	reopenedExplain, err := coll2.Find(tiedCond).Explain(ctx)
	require.NoError(t, err)
	reopenedCount, err := coll2.Find(tiedCond).Count(ctx)
	require.NoError(t, err)

	// This test only means something if the three seeks genuinely tie: the
	// name rung fires on an exact cost tie and nothing else. Assert it, so a
	// cost-model change that separates the costs fails loudly instead of
	// turning the test into a silent no-op.
	require.Len(t, freshCosts, 3)
	for name, cost := range freshCosts {
		assert.Equalf(t, freshCosts["aaa"], cost,
			"index %s must tie with the others for this test to exercise the tie-break", name)
	}

	// The two sessions see the indexes in different orders — creation order
	// live, catalog (name) order after a reopen. That is the input the planner
	// must not be sensitive to. Logged rather than asserted: normalizing the
	// load order is a legitimate future fix, and it must not fail this test.
	assert.ElementsMatch(t, freshNames, reopenedNames)
	t.Logf("index order: live=%v reopened=%v", freshNames, reopenedNames)

	assert.Equal(t, freshCount, reopenedCount, "row count must not depend on the session")
	assert.Equal(t, plannerUsedIndex(t, freshExplain), plannerUsedIndex(t, reopenedExplain),
		"a tied plan must resolve identically on a fresh and a reopened collection")
	assert.Equal(t, "aaa", plannerUsedIndex(t, freshExplain),
		"an exact cost tie resolves on the lowest index name")
	// The whole explain output — chosen plan, costs and the candidate listing —
	// is reproducible across sessions.
	assert.Equal(t, freshExplain.Plan, reopenedExplain.Plan)
}

func plannerIndexNames(coll anystore.Collection) []string {
	var names []string
	for _, idx := range coll.GetIndexes() {
		names = append(names, idx.Info().Name)
	}
	return names
}

func plannerUsedIndex(t *testing.T, explain anystore.Explain) string {
	t.Helper()
	var used []string
	for _, ie := range explain.Indexes {
		if ie.Used {
			used = append(used, ie.Name)
		}
	}
	return strings.Join(used, ",")
}

func plannerIndexUsed(explain anystore.Explain, name string) bool {
	for _, ie := range explain.Indexes {
		if ie.Used && ie.Name == name {
			return true
		}
	}
	return false
}

// A presence scan is priced, not preferred: it walks and fetches every entry
// of the sparse index, so it loses to a full scan once most documents carry
// the field.
func TestPlanner_PresenceScanCostedAgainstFullScan(t *testing.T) {
	for _, tc := range []struct {
		name      string
		every     int // one document in `every` lacks the field
		wantIndex bool
	}{
		{"dense", 400, false},
		{"rare", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "c")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "p", Fields: []string{"p"}, Sparse: true}))
			want := 0
			for i := range 400 {
				doc := fmt.Sprintf(`{"id":%d,"p":%d}`, i, i)
				if (tc.every == 0 && i%40 != 0) || (tc.every != 0 && i%tc.every == 1) {
					doc = fmt.Sprintf(`{"id":%d}`, i)
				} else {
					want++
				}
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
			}
			q := coll.Find(`{"p":{"$exists":true}}`)
			explain, err := q.Explain(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.wantIndex, plannerIndexUsed(explain, "p"), explain.Sql)
			cnt, err := q.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, want, cnt)
		})
	}
}

// The presence predicate is priced from the sparse index's population
// in the combined selectivity, so a LIMIT does not make a full scan look
// cheaper than the index that holds exactly the matching documents.
func TestPlanner_PresenceScanUnderLimit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "opt", Fields: []string{"opt"}, Sparse: true},
		anystore.IndexInfo{Name: "n", Fields: []string{"n"}},
	))
	var want []int
	for i := range 2000 {
		d := fmt.Sprintf(`{"id":%d,"n":%d}`, i, i%100)
		if i%100 == 0 {
			d = fmt.Sprintf(`{"id":%d,"n":%d,"opt":%d}`, i, i%100, i)
			want = append(want, i)
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	for _, q := range []anystore.Query{
		coll.Find(`{"opt":{"$exists":true}}`).Limit(5),
		coll.Find(`{"opt":{"$exists":true}}`).Sort("n").Limit(5),
		coll.Find(`{"opt":{"$exists":true},"n":{"$lt":50}}`).Limit(5),
	} {
		ex, err := q.Explain(ctx)
		require.NoError(t, err)
		assert.True(t, plannerIndexUsed(ex, "opt"), ex.Plan)
		assert.Len(t, collectIntField(t, q, "id"), 5)
	}
	assert.Equal(t, want, collectIntField(t, coll.Find(`{"opt":{"$exists":true}}`).Sort("id"), "id"))
}

// fillRange inserts n docs {"id":i,"a":i} into a fresh collection with an
// index on the given field spec.
func fillRange(t *testing.T, fx *fixture, name, indexField string, n int) anystore.Collection {
	t.Helper()
	coll, err := fx.CreateCollection(ctx, name)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{indexField}}))
	docs := make([]*anyenc.Value, 0, n)
	for i := 0; i < n; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	return coll
}

// TestCBO_TightBounds_TwoSidedRange: the estimation channel must rate a
// two-sided range by BOTH ends. Before the tight channel, And.IndexBounds
// dropped the $lt end, interpolation ranked (lo,+inf) (~60% for a mid-keyspace
// lo), the <0.5 adoption ratchet rejected it, and the flat 0.5 default sent a
// ~2%-selective query to a full scan.
func TestCBO_TightBounds_TwoSidedRange(t *testing.T) {
	fx := newFixture(t)

	run := func(t *testing.T, coll anystore.Collection) {
		// a in (2000,2100) → ~2% of 5000 docs, mid-keyspace. Index must win.
		explain, err := coll.Find(`{"a":{"$gt":2000,"$lt":2100}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Index",
			"mid-keyspace two-sided range must use the index, got: %s", explain.Sql)
		// The coarse per-field estimate (the "Selectivity:" header) must adopt
		// the interpolated fraction instead of the flat 0.50 default.
		assert.NotContains(t, explain.Plan, "Selectivity: 0.50",
			"two-sided range selectivity must not be the 0.5 default:\n%s", explain.Plan)

		// Broad two-sided range (~98%) must still favor the full scan.
		explain, err = coll.Find(`{"a":{"$gt":50,"$lt":4950}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"broad two-sided range must stay on full scan, got: %s", explain.Sql)
	}

	t.Run("ascending index", func(t *testing.T) {
		run(t, fillRange(t, fx, "asc", "a", 5000))
	})
	t.Run("descending index", func(t *testing.T) {
		// Reverse-flagged fields are stored bit-inverted; the estimation
		// bounds must be transformed into stored-key space before ranking,
		// or the fraction clamps to ~0 and the fix silently goes inert.
		run(t, fillRange(t, fx, "desc", "-a", 5000))
	})
}

// TestQuery_PkContradiction_Unsat: a contradictory constraint on the primary
// key provably matches nothing — pk values are scalars by construction (array
// pks are rejected on write), so all four verbs short-circuit to zero.
func TestQuery_PkContradiction_Unsat(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "pkunsat")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":6,"a":2}`),
	))

	for _, filter := range []string{
		`{"id":{"$gt":5,"$lt":3}}`,
		`{"$and":[{"id":1},{"id":2}]}`,
		`{"id":{"$in":[1,2],"$gte":5}}`,
	} {
		t.Run(filter, func(t *testing.T) {
			assertQueryCount(t, coll.Find(filter), 0)

			it, err := coll.Find(filter).Iter(ctx)
			require.NoError(t, err)
			assert.False(t, it.Next(), "Iter must be empty")
			require.NoError(t, it.Err())
			require.NoError(t, it.Close())

			res, err := coll.Find(filter).Update(ctx, `{"$set":{"a":9}}`)
			require.NoError(t, err)
			assert.Equal(t, 0, res.Matched)

			dres, err := coll.Find(filter).Delete(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, dres.Modified)

			// Explain deliberately bypasses the short-circuit and still
			// produces a plan for debugging.
			explain, err := coll.Find(filter).Explain(ctx)
			require.NoError(t, err)
			assert.NotEmpty(t, explain.Sql)
		})
	}
	assertCollCount(t, coll, 2)
}

// TestQuery_ArrayContradiction_MustMatch pins the soundness boundary of the
// unsat short-circuit: a "contradictory" range on a NON-pk field still matches
// array values element-wise ({a:[6,1]}: 6>5 via one element, 1<3 via another),
// so it must NOT be short-circuited — on any verb, with or without an index.
func TestQuery_ArrayContradiction_MustMatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "arrunsat")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":[6,1]}`),
		anyenc.MustParseJson(`{"id":2,"a":4}`),
	))

	const filter = `{"a":{"$gt":5,"$lt":3}}`
	hint := anystore.IndexHint{IndexName: "a", Boost: 1_000_000}

	// Sanity: the filter really matches the array doc.
	f := query.MustParseCondition(filter)
	require.True(t, f.Ok(anyenc.MustParseJson(`{"a":[6,1]}`), nil))

	for _, q := range []func() anystore.Query{
		func() anystore.Query { return coll.Find(filter) },
		func() anystore.Query { return coll.Find(filter).IndexHint(hint) },
	} {
		assertQueryCount(t, q(), 1)

		it, err := q().Iter(ctx)
		require.NoError(t, err)
		n := 0
		for it.Next() {
			n++
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, 1, n, "Iter must yield the array doc")

		res, err := q().Update(ctx, `{"$set":{"touched":true}}`)
		require.NoError(t, err)
		assert.Equal(t, 1, res.Matched, "Update must match the array doc")
	}

	dres, err := coll.Find(filter).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, dres.Modified, "Delete must remove the array doc")
	assertCollCount(t, coll, 1)
}

// TestCBO_TightBounds_AscAndDescIndexesOneField pins calculateSelectivity's
// usedFields dedup when BOTH an ascending and a descending index cover the
// same field: whichever index enumerates first, the two-sided range must not
// fall back to the flat 0.5 default (each index's tight estimation bounds go
// through its own reverse-flag transform).
func TestCBO_TightBounds_AscAndDescIndexesOneField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "ascdesc")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "asc", Fields: []string{"a"}},
		anystore.IndexInfo{Name: "desc", Fields: []string{"-a"}},
	))
	docs := make([]*anyenc.Value, 0, 5000)
	for i := 0; i < 5000; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	explain, err := coll.Find(`{"a":{"$gt":2000,"$lt":2100}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "Index", "plan: %s", explain.Sql)
	assert.NotContains(t, explain.Plan, "Selectivity: 0.50",
		"two-sided range selectivity must be interpolated regardless of index order:\n%s", explain.Plan)
}

// TestCBO_RangeInterpolation_DenseIndex exercises the end-to-end plan-time
// B-tree page interpolation: on a DENSE index (every doc indexed), a selective
// range must use the index while a broad range must stay on a full scan. Before
// interpolation, every range fell back to DefaultRangeSelectivity (0.5) and the
// selective case wrongly chose a full scan.
func TestCBO_RangeInterpolation_DenseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// 5000 docs with a = 0..4999 (uniform, dense index).
	const n = 5000
	docs := make([]*anyenc.Value, 0, n)
	for i := 0; i < n; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	t.Run("selective range uses index", func(t *testing.T) {
		// a < 50  → ~1% of the collection. Index must win.
		explain, err := coll.Find(`{"a":{"$lt":50}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Index",
			"selective range must use the index, got: %s", explain.Sql)
	})

	t.Run("selective high range uses index", func(t *testing.T) {
		// a > 4950 → ~1% of the collection (one-sided $gt). Index must win.
		explain, err := coll.Find(`{"a":{"$gt":4950}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Index",
			"selective one-sided range must use the index, got: %s", explain.Sql)
	})

	t.Run("broad range stays full scan", func(t *testing.T) {
		// a > 50 → ~99% of the collection. Random-fetching ~all docs via the index
		// is far costlier than a sequential scan; full scan must win.
		explain, err := coll.Find(`{"a":{"$gt":50}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"broad range must stay on full scan, got: %s", explain.Sql)
	})
}

// queryIdsHinted runs Find(filter).Sort(sortSpec).Limit(limit).Offset(offset)
// forcing the named index so the IndexScan/IndexSeek path (the one the
// offset fast-skip optimizes) is exercised regardless of the cost model.
func queryIdsHinted(t *testing.T, coll anystore.Collection, idxName string, filter any, sortSpec string, limit, offset int) []int {
	t.Helper()
	q := coll.Find(filter).Sort(sortSpec)
	if idxName != "" {
		q = q.IndexHint(anystore.IndexHint{IndexName: idxName, Boost: 1_000_000})
	}
	if limit > 0 {
		q = q.Limit(uint(limit))
	}
	if offset > 0 {
		q = q.Offset(uint(offset))
	}
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []int
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, int(d.Value().GetInt("id")))
	}
	require.NoError(t, iter.Err())
	return ids
}

// TestOffsetSkip_IndexOrdered_PrefixSkip is the focused correctness proof for
// the indexed-sort OFFSET streaming-skip optimization. It asserts the core
// contract: applying Offset(O)[+Limit(L)] to an index-ordered scan yields
// EXACTLY full[O:O+L], where `full` is the same query's ordered output with
// no offset (so the fast-skip is disengaged for the baseline). This validates
// that the cursor-level skip is a pure prefix-skip of the deduped logical-row
// stream — including across btree page boundaries and through the
// scalar->multi-key transition for mixed/array single-field indexes.
func TestOffsetSkip_IndexOrdered_PrefixSkip(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "golden")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"m"}}))

	// 1500 docs: large enough that the (a)/(m)/(tags) indexes span many btree
	// pages, so high offsets cross page boundaries inside the cursor skip.
	const N = 1500
	for i := 0; i < N; i++ {
		// m: half scalar, half a 2-element array. A single-field index over m
		// therefore mixes scalar (multiKey=false) and array (multiKey=true)
		// entries, exercising the fast-skip's stop-at-first-multiKey fallback.
		var mField string
		if i%2 == 0 {
			mField = fmt.Sprintf(`"m":%d`, i%50)
		} else {
			mField = fmt.Sprintf(`"m":[%d,%d]`, i%50, (i%50)+1000)
		}
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"tags":["tag-%d","cat-%d"],%s}`,
			i, i%100, (i/100)%50, i%20, i%10, mField))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	offsets := []int{0, 1, 2, 7, 99, 100, 101, 250, 999, 1000, 1499, 1500, 1501, 3000}
	limits := []int{0, 1, 10, 137}

	cases := []struct {
		name string
		idx  string // forced index
		sort string
	}{
		{"single_scalar_a_asc", "a", "a"},         // single-field scalar — fast-skip ENGAGED
		{"single_scalar_a_desc", "a", "-a"},       // reverse fast-skip
		{"single_scalar_b_asc", "b", "b"},         // another scalar field
		{"single_array_tags_asc", "tags", "tags"}, // every doc multi-key — fast-skip bails immediately
		{"single_mixed_m_asc", "m", "m"},          // scalar->multiKey transition mid-skip
		{"single_mixed_m_desc", "m", "-m"},        // reverse, mixed
		{"compound_ab_a_asc", "a,b", "a"},         // compound index, scalar — fast-skip ENGAGED (1 entry/doc)
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			// Baseline ordered stream with NO offset (fast-skip disengaged).
			full := queryIdsHinted(t, coll, c.idx, nil, c.sort, 0, 0)
			require.Lenf(t, full, N, "baseline must return all %d docs", N)

			for _, off := range offsets {
				for _, lim := range limits {
					got := queryIdsHinted(t, coll, c.idx, nil, c.sort, lim, off)

					lo := off
					if lo > len(full) {
						lo = len(full)
					}
					hi := len(full)
					if lim > 0 && lo+lim < hi {
						hi = lo + lim
					}
					want := append([]int(nil), full[lo:hi]...)

					require.Equalf(t, want, got,
						"case=%s off=%d lim=%d: Offset must be a pure prefix-skip of the index-ordered stream",
						c.name, off, lim)
				}
			}
		})
	}

	// Prove the optimization actually FIRES for the scalar single-field case:
	// Sort(a).Limit(10).Offset(1000) must fetch+parse only ~limit docs, not
	// ~limit+offset. The skipped 1000 rows are advanced at the index cursor
	// without any data-namespace fetch. For the array case (tags), every entry
	// is multi-key so the fast-skip bails immediately and the fetch count
	// stays ~limit+offset (slow-but-correct path preserved).
	t.Run("perf_proof_scalar_skips_fetch", func(t *testing.T) {
		anystore.EnablePipelinePerfCounters(true)
		defer anystore.EnablePipelinePerfCounters(false)

		const limit, offset = 10, 1000

		anystore.ResetPipelinePerfCounters()
		_ = queryIdsHinted(t, coll, "a", nil, "a", limit, offset)
		scalar := anystore.SnapshotPipelinePerfCounters().Planner

		anystore.ResetPipelinePerfCounters()
		_ = queryIdsHinted(t, coll, "tags", nil, "tags", limit, offset)
		array := anystore.SnapshotPipelinePerfCounters().Planner

		t.Logf("scalar(a): FetchYields=%d FetchNextCalls=%d", scalar.FetchYields, scalar.FetchNextCalls)
		t.Logf("array(tags): FetchYields=%d FetchNextCalls=%d", array.FetchYields, array.FetchNextCalls)

		// Scalar path: only the limit window is fetched (a small constant slack
		// well under the offset). Decisive proof the 1000 skipped rows are not
		// fetched.
		require.Lessf(t, int(scalar.FetchYields), limit+50,
			"scalar fast-skip must fetch ~limit docs, got %d (offset=%d)", scalar.FetchYields, offset)

		// Array path: the multi-key fallback still fetches across the offset
		// window (~limit+offset, scaled by the 2 entries/doc dedup), proving we
		// did NOT wrongly fast-skip a multi-key index.
		require.Greaterf(t, int(array.FetchYields), offset,
			"array index must keep the fetch-then-skip path, got %d fetches", array.FetchYields)
	})
}

func TestIndexScanCoveringFilterElision(t *testing.T) {
	newColl := func(t *testing.T, fx *fixture, arrayB bool) anystore.Collection {
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))
		var docs []*anyenc.Value
		for i := 0; i < 500; i++ {
			var doc string
			if arrayB {
				doc = fmt.Sprintf(`{"id":%d,"a":%d,"b":[%d,%d],"pad":"%s"}`, i, (7*i)%500, i%10, (i+3)%10, sortPad)
			} else {
				doc = fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"pad":"%s"}`, i, (7*i)%500, i%10, sortPad)
			}
			docs = append(docs, anyenc.MustParseJson(doc))
		}
		require.NoError(t, coll.Insert(ctx, docs...))
		return coll
	}

	for _, arrayB := range []bool{false, true} {
		name := "scalar-b"
		if arrayB {
			name = "multikey-b" // per-entry equality + DocDedup must stand in for the residual
		}
		t.Run(name, func(t *testing.T) {
			fx := newFixture(t)
			coll := newColl(t, fx, arrayB)

			elided := coll.Find(`{"b":5}`).Sort("a").Limit(100)
			shadow := coll.Find(`{"$and":[{"b":5},{"id":{"$exists":true}}]}`).Sort("a").Limit(100)
			got := collectIds(t, elided)
			want := collectIds(t, shadow)
			require.NotEmpty(t, want)
			assert.Equal(t, want, got)

			// No duplicate ids (multikey entries must still dedup without the
			// residual filter).
			seen := map[int]bool{}
			for _, id := range got {
				assert.False(t, seen[id], "duplicate id %d", id)
				seen[id] = true
			}

			// The covered scalar plan must have IndexFilter and no residual
			// Filter. A multikey (a,b) index gets no sort-service scan
			// candidate at all (array order is plan-dependent), so the elided
			// chain is unreachable there — only the differential equality
			// above applies.
			ex, err := elided.Explain(ctx)
			require.NoError(t, err)
			if !arrayB && assert.Contains(t, ex.Sql, "IndexFilter") {
				assert.NotContains(t, ex.Sql, "-> Filter ", "residual filter must be elided: %s", ex.Sql)
				assert.False(t, strings.HasSuffix(ex.Sql, "-> Filter") || strings.Contains(ex.Sql, "-> Filter -"),
					"residual filter must be elided: %s", ex.Sql)
			}
		})
	}

	t.Run("negative gates keep the residual", func(t *testing.T) {
		fx := newFixture(t)
		coll := newColl(t, fx, false)

		for _, tc := range []struct{ name, filter string }{
			{"uncovered extra field", `{"b":5,"pad":{"$exists":true}}`},
			{"two predicates on b", `{"b":{"$gte":5,"$lte":5}}`},
			{"in with two values", `{"b":{"$in":[5,6]}}`},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ex, err := coll.Find(tc.filter).Sort("a").Limit(100).Explain(ctx)
				require.NoError(t, err)
				if strings.Contains(ex.Sql, "IndexFilter") {
					assert.Contains(t, ex.Sql, "-> Filter", "residual must stay for %s: %s", tc.filter, ex.Sql)
				}
			})
		}
	})
}

// The tests below differentially verify the two sort fast paths against
// equivalent queries that cannot take them:
//   - a filterless full scan + sort hands decoded bytes to SortIter
//     (Plan.DocRaw + RawSort) instead of parsing every row;
//   - an ordered index scan whose IndexFilterIter equality checks fully cover
//     the filter elides the post-fetch FilterIter.
// The shadow query carries an extra $exists conjunct on the pk, which every
// document satisfies but no index covers — forcing the parsed/filtered path
// while selecting the same documents.

// pad pushes every document above the 256-byte compression threshold so the
// raw path exercises s2-decoded bytes, matching production-shaped documents.
var sortPad = strings.Repeat("x", 300)

func sortFastpathFixture(t *testing.T, arrayVal bool) (*fixture, anystore.Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	// 7i mod 101 permutes 0..100: unique scalar sort keys, insertion order
	// decorrelated from key order.
	var docs []*anyenc.Value
	for i := 0; i < 101; i++ {
		v := (7 * i) % 101
		var doc string
		if arrayVal {
			doc = fmt.Sprintf(`{"id":%d,"val":[%d,%d],"pad":"%s"}`, i, v, (v+13)%101, sortPad)
		} else {
			doc = fmt.Sprintf(`{"id":%d,"val":%d,"pad":"%s"}`, i, v, sortPad)
		}
		docs = append(docs, anyenc.MustParseJson(doc))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	return fx, coll
}

func collectIds(t *testing.T, q anystore.Query) []int {
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var ids []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().GetInt("id"))
	}
	require.NoError(t, iter.Err())
	return ids
}

func TestSortRawScan_MatchesParsedPath(t *testing.T) {
	for _, arrayVal := range []bool{false, true} {
		name := "scalar"
		if arrayVal {
			name = "array" // array sort field: raw path must fall back to parse
		}
		t.Run(name, func(t *testing.T) {
			_, coll := sortFastpathFixture(t, arrayVal)

			cases := []struct {
				sort          string
				limit, offset uint
			}{
				{"val", 10, 0},
				{"-val", 10, 0},
				{"val", 10, 25},
				{"val", 0, 0},  // full sort (TopK disabled)
				{"-val", 0, 0}, // full sort desc
			}
			for _, tc := range cases {
				t.Run(fmt.Sprintf("sort=%s limit=%d offset=%d", tc.sort, tc.limit, tc.offset), func(t *testing.T) {
					raw := coll.Find(nil).Sort(tc.sort)
					shadow := coll.Find(`{"id":{"$exists":true}}`).Sort(tc.sort)
					if tc.limit > 0 {
						raw, shadow = raw.Limit(tc.limit), shadow.Limit(tc.limit)
					}
					if tc.offset > 0 {
						raw, shadow = raw.Offset(tc.offset), shadow.Offset(tc.offset)
					}
					got := collectIds(t, raw)
					want := collectIds(t, shadow)
					require.NotEmpty(t, want)
					assert.Equal(t, want, got)
				})
			}

			// The fast path must actually be planned: filterless scan + sort.
			ex, err := coll.Find(nil).Sort("val").Limit(10).Explain(ctx)
			require.NoError(t, err)
			assert.Equal(t, "FullScan -> TopK(10) -> Limit(10)", ex.Sql)
		})
	}
}

// The unique-index CoverIter branch elides the residual FilterIter for
// covering counts (bounds fully represent the filter). These tests pin the
// Count == Iter parity on that branch: the elision must never change the
// counted set — including the gate-rejection shapes that must keep the
// residual, and the multikey dedup a unique array index needs.

func uniqueInColl(t *testing.T, n int) (fixture *fixture, coll anystore.Collection) {
	fx := newFixture(t)
	c, err := fx.CreateCollection(ctx, "users")
	require.NoError(t, err)
	require.NoError(t, c.CreateIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))
	for i := 0; i < n; i++ {
		require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"email":"user%d@test.com","grp":%d}`, i, i, i%2))))
	}
	return fx, c
}

func countAndIterLen(t *testing.T, coll anystore.Collection, filter string) (int, int) {
	t.Helper()
	cnt, err := coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	iterN := 0
	iter, err := coll.Find(filter).Iter(ctx)
	require.NoError(t, err)
	for iter.Next() {
		iterN++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return cnt, iterN
}

func TestUniqueInCountCovering(t *testing.T) {
	_, coll := uniqueInColl(t, 5000)

	var vals []string
	for i := 0; i < 100; i++ {
		vals = append(vals, fmt.Sprintf(`"user%d@test.com"`, i*7))
	}
	// Half the values miss (beyond the population): count = hits only.
	for i := 0; i < 50; i++ {
		vals = append(vals, fmt.Sprintf(`"missing%d@test.com"`, i))
	}
	in := fmt.Sprintf(`{"email":{"$in":[%s]}}`, strings.Join(vals, ","))

	cnt, iterN := countAndIterLen(t, coll, in)
	assert.Equal(t, 100, cnt)
	assert.Equal(t, cnt, iterN, "covering count must equal Iter cardinality")

	// Single-value Eq takes the same branch.
	cnt, iterN = countAndIterLen(t, coll, `{"email":"user42@test.com"}`)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, cnt, iterN)

	// Duplicate $in values must not double-count the doc.
	cnt, _ = countAndIterLen(t, coll,
		`{"email":{"$in":["user7@test.com","user7@test.com"]}}`)
	assert.Equal(t, 1, cnt)
}

func TestUniqueInCountKeepsResidualWhenNotCovering(t *testing.T) {
	_, coll := uniqueInColl(t, 2000)

	// Second predicate on the bounded field: bounds over-approximate, the
	// residual must stay — $nin removes one of the $in hits.
	f := `{"email":{"$in":["user1@test.com","user2@test.com","user3@test.com"],"$nin":["user2@test.com"]}}`
	cnt, iterN := countAndIterLen(t, coll, f)
	assert.Equal(t, 2, cnt)
	assert.Equal(t, cnt, iterN)

	// Uncovered second field: the filter reaches beyond the index, residual
	// must stay — grp halves the hits.
	f = `{"email":{"$in":["user1@test.com","user2@test.com","user3@test.com","user4@test.com"]},"grp":0}`
	cnt, iterN = countAndIterLen(t, coll, f)
	assert.Equal(t, 2, cnt)
	assert.Equal(t, cnt, iterN)
}

// A unique index over an array field is still multikey: one doc fans out to
// one entry per element, uniqueness enforced per entry. A $in matching two
// elements of the SAME doc must count it once — the elision relies on the
// CoverIter multiKey tagging + DocDedupIter for this.
func TestUniqueInCountMultikeyDedup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{Fields: []string{"aliases"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"aliases":["a","b","c"]}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"aliases":["d"]}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"aliases":["e","f"]}`)))

	// "a" and "b" hit doc 1 through two of its entries; "d" hits doc 2.
	cnt, iterN := countAndIterLen(t, coll, `{"aliases":{"$in":["a","b","d"]}}`)
	assert.Equal(t, 2, cnt, "same-doc multi-element hits must dedup")
	assert.Equal(t, cnt, iterN)
}
