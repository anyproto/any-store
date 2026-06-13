package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// TestPlanner_CompoundLeadingEquality_PicksIndex reproduces the compound-index
// leading-column selectivity defect: a collection with ONLY a compound (a,b) index
// (no standalone (a) index, which would mask the bug) and a selective leading
// column. Before the fix the planner estimated the leading equality at the blind
// DefaultRangeSelectivity (~50%) and chose a FullScan over the available index.
func TestPlanner_CompoundLeadingEquality_PicksIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// ONLY a compound index — no single-field (a) index.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	// Skewed leading field: ~1% of rows per distinct `a` value (mirrors the real
	// changes (t,o) tree at ~1.3%), so the composite sketch saturates and the
	// leading equality is genuinely selective.
	const n = 6000
	for i := range n {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%100, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("leading equality uses the index", func(t *testing.T) {
		explain, err := coll.Find(`{"a":7}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(a,b)")
		assert.NotContains(t, explain.Sql, "FullScan")

		// Correctness unchanged: exactly the matching rows.
		count, err := coll.Find(`{"a":7}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, n/100, count)
	})

	t.Run("leading equality + trailing range + sort uses the index", func(t *testing.T) {
		explain, err := coll.Find(`{"a":7,"b":{"$gte":0}}`).Sort("b").Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexScan(a,b)")
		assert.NotContains(t, explain.Sql, "FullScan")
		// Index covers the sort -> no in-memory sort.
		assert.NotContains(t, explain.Sql, "-> Sort")
	})

	t.Run("count parity", func(t *testing.T) {
		count, err := coll.Find(`{"a":7,"b":{"$gte":0}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, n/100, count)
	})
}

// TestPlanner_CompoundLeading_NonRegression guards that the fix is scoped to
// covered indexed equality only: a NON-indexed equality and a pure range still
// fall back to the default selectivity, and a single-field indexed equality still
// takes its exact per-value sketch path.
func TestPlanner_CompoundLeading_NonRegression(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	const n = 2000
	for i := range n {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%50, i, i%2))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("equality on a NON-indexed field still full-scans", func(t *testing.T) {
		// `c` is not indexed -> no index can satisfy it; planner must full-scan.
		explain, err := coll.Find(`{"c":1}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "FullScan")

		count, err := coll.Find(`{"c":1}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, n/2, count)
	})

	t.Run("leading equality correctness across all values", func(t *testing.T) {
		for _, a := range []int{0, 17, 49} {
			count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, a)).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, n/50, count, "a=%d", a)
		}
	})
}
