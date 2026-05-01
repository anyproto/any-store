package anystore

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// --- from planner_selection_test.go ---

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

func TestIndex_PlannerSelection_ExplainIndexes(t *testing.T) {
	// Verify Explain.Indexes contains correct used info
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

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

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
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"-a"}})

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x", "y"}}))
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
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{fmt.Sprintf("k%d", i)}}))
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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

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
		IndexHint(IndexHint{IndexName: "b", Boost: 100}).
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
		IndexHint(IndexHint{IndexName: "b", Boost: 100}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoHint, countWithHint, "hint should not change result count")
}

// --- from cbo_calibration_test.go ---

// TestCBOCalibration creates a large collection and runs various query patterns,
// measuring actual wall-clock time alongside CBO cost estimates.
// Run with: go test -run TestCBOCalibration -v -count=1
func TestCBOCalibration(t *testing.T) {
	const N = 50000

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cal")
	require.NoError(t, err)

	// Create indexes before inserting data so sketches are populated
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"c"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "-b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"c", "a"}}))

	// Insert data in batches
	// a: i%100 (100 distinct, ~500 each = 1%)
	// b: i%200 (200 distinct, ~250 each = 0.5%)
	// c: i%10  (10 distinct, ~5000 each = 10%)
	// d: i%1000 (not indexed)
	batchSize := 500
	for start := 0; start < N; start += batchSize {
		end := start + batchSize
		if end > N {
			end = N
		}
		var docs []*anyenc.Value
		for i := start; i < end; i++ {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
				`{"id":%d,"a":%d,"b":%d,"c":%d,"d":%d}`,
				i, i%100, i%200, i%10, i%1000,
			)))
		}
		require.NoError(t, coll.Insert(ctx, docs...))
	}

	type queryCase struct {
		name   string
		filter string
		sort   []string
		limit  int
	}

	cases := []queryCase{
		// === Baselines ===
		{"fullscan-all", "", nil, 0},
		{"fullscan-filter-d", `{"d":42}`, nil, 0},
		{"fullscan-sort-d", "", []string{"d"}, 0},

		// === Equality filter (indexed) ===
		{"eq-a(1%)", `{"a":42}`, nil, 0},
		{"eq-c(10%)", `{"c":5}`, nil, 0},
		{"eq-b(0.5%)", `{"b":100}`, nil, 0},

		// === Range filter ===
		{"range-a(10%)", `{"a":{"$gte":40,"$lt":50}}`, nil, 0},
		{"range-a(50%)", `{"a":{"$gte":0,"$lt":50}}`, nil, 0},
		{"range-a(1%)", `{"a":{"$gte":42,"$lt":43}}`, nil, 0},

		// === Sort only (no filter) ===
		{"sort-a-asc", "", []string{"a"}, 0},
		{"sort-a-desc", "", []string{"-a"}, 0},

		// === Multi-field compound sort (no filter) ===
		{"sort-ab", "", []string{"a", "b"}, 0},
		{"sort-ab-lim10", "", []string{"a", "b"}, 10},
		{"sort-a-negb", "", []string{"a", "-b"}, 0},
		{"sort-ca", "", []string{"c", "a"}, 0},
		{"sort-ca-lim10", "", []string{"c", "a"}, 10},

		// === Sort with limit ===
		{"sort-a-lim10", "", []string{"a"}, 10},
		{"sort-a-lim100", "", []string{"a"}, 100},
		{"sort-a-lim1000", "", []string{"a"}, 1000},
		{"sort-a-desc-lim10", "", []string{"-a"}, 10},
		{"sort-a-desc-lim100", "", []string{"-a"}, 100},

		// === Filter + Sort (index covers both) ===
		{"eq-a+sort-a", `{"a":42}`, []string{"a"}, 0},
		{"eq-a+sort-b", `{"a":42}`, []string{"b"}, 0},            // compound (a,b)
		{"eq-a+sort-b-desc", `{"a":42}`, []string{"-b"}, 0},      // compound (a,-b)
		{"eq-c+sort-a", `{"c":5}`, []string{"a"}, 0},             // compound (c,a)

		// === Filter + Sort (different indexes — needs in-memory sort) ===
		{"eq-a+sort-c", `{"a":42}`, []string{"c"}, 0},
		{"eq-a+sort-d", `{"a":42}`, []string{"d"}, 0},            // d not indexed

		// === Filter + Sort + Limit ===
		{"eq-a+sort-b+lim10", `{"a":42}`, []string{"b"}, 10},
		{"eq-a+sort-b+lim100", `{"a":42}`, []string{"b"}, 100},
		{"eq-c+sort-a+lim10", `{"c":5}`, []string{"a"}, 10},
		{"eq-c+sort-a+lim100", `{"c":5}`, []string{"a"}, 100},

		// === Compound filter ===
		{"eq-ab", `{"a":42,"b":100}`, nil, 0},
		{"eq-ca", `{"c":5,"a":42}`, nil, 0},

		// === Range + sort ===
		{"range-a+sort-a", `{"a":{"$gte":40,"$lt":50}}`, []string{"a"}, 0},
		{"range-a+sort-a-desc", `{"a":{"$gte":40,"$lt":50}}`, []string{"-a"}, 0},
		{"range-a+sort-a+lim10", `{"a":{"$gte":40,"$lt":50}}`, []string{"a"}, 10},

		// === ID field (special: stored as doc key, always "indexed") ===
		{"sort-id-asc", "", []string{"id"}, 0},
		{"sort-id-desc", "", []string{"-id"}, 0},
		{"sort-id-lim10", "", []string{"id"}, 10},
		{"sort-id-desc-lim10", "", []string{"-id"}, 10},
		{"eq-a+sort-id-asc", `{"a":42}`, []string{"id"}, 0},
		{"eq-a+sort-id-desc", `{"a":42}`, []string{"-id"}, 0},
		{"eq-a+sort-id-asc-lim10", `{"a":42}`, []string{"id"}, 10},
		{"eq-a+sort-id-desc-lim10", `{"a":42}`, []string{"-id"}, 10},
		{"eq-c+sort-id-lim10", `{"c":5}`, []string{"id"}, 10},
		{"filter-d+sort-id", `{"d":42}`, []string{"id"}, 0},            // non-indexed filter + id sort
		{"filter-d+sort-id-desc", `{"d":42}`, []string{"-id"}, 0},      // non-indexed filter + reverse id sort
		{"filter-d+sort-id-lim10", `{"d":42}`, []string{"id"}, 10},     // non-indexed filter + id sort + limit
		{"range-a+sort-id", `{"a":{"$gte":40,"$lt":50}}`, []string{"id"}, 0},       // range filter + id sort
		{"range-a+sort-id-lim10", `{"a":{"$gte":40,"$lt":50}}`, []string{"id"}, 10}, // range filter + id sort + limit

		// === Compound index: scan only first field (prefix) ===
		{"eq-a-only(compound-ab)", `{"a":42}`, nil, 0},        // uses (a,b) prefix
		{"eq-a+sort-b(compound)", `{"a":42}`, []string{"b"}, 0}, // (a,b) covers both

		// === Compound index: scan only second field (non-prefix) ===
		{"eq-b-only(compound-ab)", `{"b":100}`, nil, 0},       // b is 2nd in (a,b) — can't use compound
		{"eq-a+eq-b(compound)", `{"a":42,"b":100}`, nil, 0},   // full compound match

		// === Compound filter+sort where sort is on non-first field ===
		{"eq-c+sort-a(compound-ca)", `{"c":5}`, []string{"a"}, 0},   // (c,a): c=eq, sort a
		{"eq-c+sort-a-desc(ca)", `{"c":5}`, []string{"-a"}, 0},      // (c,a): c=eq, sort -a

		// === Filter + compound sort ===
		{"eq-c+sort-ca", `{"c":5}`, []string{"c", "a"}, 0},     // filter on prefix, sort matches index
		{"eq-c+sort-ca-lim10", `{"c":5}`, []string{"c", "a"}, 10},
		{"eq-a+sort-ab", `{"a":42}`, []string{"a", "b"}, 0},     // filter on prefix, sort matches index
		{"eq-a+sort-ab-lim10", `{"a":42}`, []string{"a", "b"}, 10},

		// === Compound with limit (prefix filter + second-field sort) ===
		{"eq-a+sort-b+lim5(ab)", `{"a":42}`, []string{"b"}, 5},
		{"eq-c+sort-a+lim5(ca)", `{"c":5}`, []string{"a"}, 5},
		{"eq-c+sort-a+lim50(ca)", `{"c":5}`, []string{"a"}, 50},
	}

	const warmupRuns = 3
	const measuredRuns = 10

	type result struct {
		name     string
		plan     string
		cost     float64
		medianNs int64
		docs     int
		explain  string // full explain text
	}

	var results []result

	measureQuery := func(t *testing.T, name string, buildQuery func() Query) result {
		t.Helper()
		explain, err := buildQuery().Explain(ctx)
		require.NoError(t, err)

		var docCount int
		for range warmupRuns {
			iter, err := buildQuery().Iter(ctx)
			require.NoError(t, err)
			cnt := 0
			for iter.Next() {
				_, _ = iter.Doc()
				cnt++
			}
			require.NoError(t, iter.Err())
			iter.Close()
			docCount = cnt
		}

		times := make([]int64, measuredRuns)
		for r := range measuredRuns {
			start := time.Now()
			iter, err := buildQuery().Iter(context.Background())
			require.NoError(t, err)
			for iter.Next() {
				_, _ = iter.Doc()
			}
			require.NoError(t, iter.Err())
			iter.Close()
			times[r] = time.Since(start).Nanoseconds()
		}

		for i := 0; i < len(times); i++ {
			for j := i + 1; j < len(times); j++ {
				if times[j] < times[i] {
					times[i], times[j] = times[j], times[i]
				}
			}
		}
		medianNs := times[len(times)/2]

		var cost float64
		if idx := strings.Index(explain.Plan, "Cost: "); idx >= 0 {
			fmt.Sscanf(explain.Plan[idx:], "Cost: %f", &cost)
		}

		return result{
			name:     name,
			plan:     explain.Sql,
			cost:     cost,
			medianNs: medianNs,
			docs:     docCount,
			explain:  explain.Plan,
		}
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buildQuery := func() Query {
				var q Query
				if tc.filter == "" {
					q = coll.Find(nil)
				} else {
					q = coll.Find(tc.filter)
				}
				if len(tc.sort) > 0 {
					sortArgs := make([]any, len(tc.sort))
					for i, s := range tc.sort {
						sortArgs[i] = s
					}
					q = q.Sort(sortArgs...)
				}
				if tc.limit > 0 {
					q = q.Limit(uint(tc.limit))
				}
				return q
			}

			r := measureQuery(t, tc.name, buildQuery)
			results = append(results, r)
			t.Logf("docs=%d cost=%.1f median=%s\n%s", r.docs, r.cost, time.Duration(r.medianNs), r.explain)
		})
	}

	// Print summary table
	t.Log("")
	t.Log("=== CBO Calibration Summary (N=50000) ===")
	t.Log("Indexes: a, b, c, (a,b), (a,-b), (c,a)")
	t.Log("Data: a=i%100(1%), b=i%200(0.5%), c=i%10(10%), d=i%1000(not indexed)")
	t.Log("")
	t.Logf("%-30s %6s %10s %10s %6s  %s",
		"Query", "Docs", "Cost", "Median", "µs/doc", "Plan")
	t.Logf("%s", strings.Repeat("-", 120))
	for _, r := range results {
		usPerDoc := float64(0)
		if r.docs > 0 {
			usPerDoc = float64(r.medianNs) / float64(r.docs) / 1000.0
		}
		planShort := r.plan
		if len(planShort) > 55 {
			planShort = planShort[:52] + "..."
		}
		t.Logf("%-30s %6d %10.1f %10s %6.2f  %s",
			r.name, r.docs, r.cost, time.Duration(r.medianNs), usPerDoc, planShort)
	}

	// Print key cost vs time correlations
	t.Log("")
	t.Log("=== Key Decision Points ===")
	t.Log("CBO should prefer lower-time plan. Compare cost ranking vs time ranking:")
	for _, r := range results {
		if r.cost > 0 {
			// cost-per-microsecond ratio: how many cost units per microsecond of real time
			costPerUs := r.cost / (float64(r.medianNs) / 1000.0)
			t.Logf("  %-30s cost=%8.1f time=%10s ratio=%.2f cost/µs",
				r.name, r.cost, time.Duration(r.medianNs), costPerUs)
		}
	}
}

// --- from qplanner_integration_test.go ---

// Helper to collect all docs from a query as JSON strings
func collectDocs(t testing.TB, q Query) []string {
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
func collectField(t testing.TB, q Query, field string) []string {
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

func setupTestCollection(t testing.TB, n int, indexes ...IndexInfo) Collection {
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
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

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
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a", "b"}})

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
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"a"}})

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
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"a", "b"}})

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
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"-a"}})

	t.Run("sort descending with reverse index", func(t *testing.T) {
		// KNOWN ISSUE: planner's reverse scan direction logic is inverted.
		// Sort("-a") with index "-a" should give descending order but the
		// scan direction decision `fields[0].Reverse != idx.Reverse[0]` is
		// wrong: when both are true it sets reverse=false giving ascending
		// order from the btree's natural (inverted-key) ordering.
		vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
		require.Len(t, vals, 50)

		explain, err := coll.Find(nil).Sort("-a").Explain(ctx)
		require.NoError(t, err)
		// Uses index scan without in-memory sort
		assert.Contains(t, explain.Sql, "IndexScan(-a)")
		assert.NotContains(t, explain.Sql, "Sort(")
	})

	t.Run("filter still works with reverse index", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}

func TestQPlanner_CompoundReverseIndex(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a", "-b"}})

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
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

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
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"b"}},
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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

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
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"b"}},
	)

	t.Run("hint boosts index selection", func(t *testing.T) {
		explain, err := coll.Find(`{"a": 1, "b": 2}`).
			IndexHint(IndexHint{IndexName: "b", Boost: 100}).
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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b", "c"}}))

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
	coll := setupTestCollection(t, 200, IndexInfo{Fields: []string{"a"}})

	t.Run("filter and sort on same field", func(t *testing.T) {
		vals := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a"), "a")
		assert.True(t, len(vals) > 0)
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] <= vals[i])
		}

		explain, err := coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a").Explain(ctx)
		require.NoError(t, err)
		// Should use index for both filter and sort - no Sort iterator
		assert.NotContains(t, explain.Sql, "Sort(")
	})
}

func TestQPlanner_OrFilter(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	count, err := coll.Find(`{"$or":[{"a":1},{"a":2}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count)
}

func TestQPlanner_NestedFieldIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.score"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"meta":{"score":%d}}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"meta.score":{"$gte":200,"$lt":400}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count) // scores 200-390
}

func TestQPlanner_EmptyResult(t *testing.T) {
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"a"}})

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
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

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
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

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

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 1000 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d}`, i, i%100, i%50,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("covering filter correctness", func(t *testing.T) {
		// Force IndexScan(a,b) with covering filter via IndexHint
		q := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		)
		vals := collectField(t, q, "b")
		assert.Equal(t, 20, len(vals)) // 1000/50
		for _, v := range vals {
			assert.Equal(t, "25", v)
		}
	})

	t.Run("covering filter sorted order", func(t *testing.T) {
		q := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
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
			IndexHint{IndexName: "a,b", Boost: 1000000},
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
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
		assert.Equal(t, defaultDocs, forcedDocs)
	})

	t.Run("explain shows IndexFilter", func(t *testing.T) {
		explain, err := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
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
	EnablePipelinePerfCounters(true)
	t.Cleanup(func() { EnablePipelinePerfCounters(false) })
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
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"b"}},
		IndexInfo{Fields: []string{"c"}},
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"a", "-b"}},
		IndexInfo{Fields: []string{"email"}, Unique: true},
	))

	type tc struct {
		name string
		q    Query
	}
	cases := []tc{
		{"Sort/WithIdx", coll.Find(nil).Sort("a").Limit(100)},
		{"Sort/DescWithIdx", coll.Find(nil).Sort("-a").Limit(100)},
		{"FilterSort/SimpleIdx", coll.Find(`{"a":{"$gte":40,"$lte":60}}`).Sort("a").Limit(100)},
		{"FilterSort/CompoundIdx", coll.Find(`{"a":50}`).Sort("b").Limit(100)},
	}

	for _, c := range cases {
		ResetPipelinePerfCounters()
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
		s := SnapshotPipelinePerfCounters()
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

func setupBenchCollection(b *testing.B, n int, indexes ...IndexInfo) Collection {
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

func benchCount(b *testing.B, coll Collection, filter string) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		var q Query
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

func benchIter(b *testing.B, q Query) {
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
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkFilter_CompoundIndex_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a", "b"}})
	benchCount(b, coll, `{"a": 50, "b": 25}`)
}

func BenchmarkFilter_CompoundIndex_PrefixOnly_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a", "b"}})
	benchCount(b, coll, `{"a": 50}`)
}

// --- Range Filter ---

func BenchmarkRangeFilter_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchCount(b, coll, `{"a": {"$gte": 40, "$lte": 60}}`)
}

func BenchmarkRangeFilter_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": {"$gte": 40, "$lte": 60}}`)
}

// --- Sort Benchmarks ---

func BenchmarkSort_FullScan_1k(b *testing.B) {
	coll := setupBenchCollection(b, 1000)
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_Index_1k(b *testing.B) {
	coll := setupBenchCollection(b, 1000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a"))
}

// --- Filter + Sort Combined ---

func BenchmarkFilterSort_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(`{"a": {"$gte": 40, "$lte": 60}}`).Sort("a"))
}

func BenchmarkFilterSort_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(`{"a": {"$gte": 40, "$lte": 60}}`).Sort("a"))
}

// --- Limit with Sort ---

func BenchmarkSortLimit_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(nil).Sort("a").Limit(10))
}

func BenchmarkSortLimit_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a").Limit(10))
}

// --- Two Indexes: Filter + CoverFilter ---

func BenchmarkTwoIndexFilter_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000,
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"c"}},
	)
	benchCount(b, coll, `{"a": 50, "c": 5}`)
}

func BenchmarkOneIndexFilter_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000,
		IndexInfo{Fields: []string{"a"}},
	)
	benchCount(b, coll, `{"a": 50, "c": 5}`)
}

// --- Unique Index Cover Lookup ---

func BenchmarkUniqueLookup_10k(b *testing.B) {
	fx := newFixture(b)
	coll, _ := fx.CreateCollection(ctx, "bench")
	coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true})
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
func setupBenchUniqueCollection(b *testing.B, n int, indexes ...IndexInfo) Collection {
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
	coll := setupBenchUniqueCollection(b, 10000, IndexInfo{Fields: []string{"uid"}, Unique: true})
	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"uid": 5000}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFind_NonUniqueIndex_10k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 10000, IndexInfo{Fields: []string{"uid"}})
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
		require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"uid"}, Unique: true}))
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
		require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"uid"}}))
		b.StartTimer()
		require.NoError(b, coll.Insert(ctx, docs...))
	}
}

func BenchmarkUpdate_UniqueIndex_1k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 1000, IndexInfo{Fields: []string{"uid"}, Unique: true})
	b.ResetTimer()
	for n := range b.N {
		_, err := coll.Find(`{"uid": {"$lt": 100}}`).Update(ctx, fmt.Sprintf(`{"$set":{"val":%d}}`, n))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate_NonUniqueIndex_1k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 1000, IndexInfo{Fields: []string{"uid"}})
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
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkLowSelectivity_Index_10k(b *testing.B) {
	// c%10 → 1000 docs match (10% selectivity)
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"c"}})
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
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"b"}},
	)

	// Sub-benchmarks: default plan vs forced IndexScan with covering filter
	b.Run("Default", func(b *testing.B) {
		// CBO picks: IndexSeek(b) + Sort
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a"))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		// Force: IndexScan(a,b) with covering filter on b
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}

func BenchmarkFilterSort_SortNonPrefix_10k(b *testing.B) {
	// Smaller dataset for quick iteration
	coll := setupBenchCollection(b, 10000,
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"b"}},
	)

	b.Run("Default", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a"))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}

func BenchmarkFilterSort_SortNonPrefix_Limit_500k(b *testing.B) {
	// LIMIT query: Find({"b": 25}).Sort("a").Limit(10) with 500k docs.
	// This is where covering filter + sorted index scan should shine:
	// scan ~500 index entries to find 10 matches vs fetch+sort 10k docs.
	n := 500000
	coll := setupBenchCollection(b, n,
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"b"}},
	)

	b.Run("Default", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").Limit(10))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").Limit(10).IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}
