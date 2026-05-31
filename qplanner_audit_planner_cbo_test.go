package anystore

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// act-19: $in multi-bound IndexScan Explain output and per-bound seek cost.
func TestIndex_Planner_InOperator_MultipleBounds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"c", "a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))))
	}

	explain, err := coll.Find(`{"a":5}`).
		IndexHint(IndexHint{IndexName: "b", Boost: 1000000}).
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
		IndexHint(IndexHint{IndexName: "b", Boost: 1000000}).Count(ctx)
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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Unique: true}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
