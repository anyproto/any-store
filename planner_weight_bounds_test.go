/*
Index/Planner tests inspired by SQLite: where.test, where2.test, where7.test

Test scenario:
Tests CBO planner edge cases and index bounds behavior — verifying
that the planner correctly handles tiebreaks, compound index selection,
and hint boost overrides.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with CBO planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

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
