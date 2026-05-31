package anystore

/*
Regression tests for correctness bugs found by the docs/qplanner audit
(see docs/qplanner/audit/findings.json). Each test failed before its fix and
passes after. Fixes live in internal/qplanner/planner.go.

  bug-01  Mixed-direction compound index (a,-b) returned the wrong within-group
          order: IndexSortMatch claimed ExactSort for an order the all-ascending
          physical storage cannot realize via a single scan direction.
  bug-01b A reverse-declared covering filter field made coveringFilterFields
          bitwise-invert the match value, so IndexFilterIter matched nothing and
          dropped every row.
  bug-02  Skip-middle compound Count over-counted: indexCoversFilter gated on ALL
          index fields instead of the contiguously-bounded prefix, so a trailing
          equality outside the bound prefix was ignored by the covering count.
  bug-03  Offset without Limit on the in-memory sort path returned zero rows:
          SortIter's bounded heap was sized to Offset and LimitIter then skipped
          exactly those rows.
*/

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// auditABPairs collects "a,b" projection of a query result, in result order.
func auditABPairs(t testing.TB, c Collection, sort ...any) []string {
	t.Helper()
	iter, err := c.Find(nil).Sort(sort...).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, fmt.Sprintf("%d,%d", d.Value().GetInt("a"), d.Value().GetInt("b")))
	}
	require.NoError(t, iter.Err())
	return out
}

func auditIntField(t testing.TB, q Query, field string) []int {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []int
	for iter.Next() {
		d, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, d.Value().GetInt(field))
	}
	require.NoError(t, iter.Err())
	return out
}

// bug-01: a mixed-direction compound index must yield the same order as the
// (correct) in-memory sort. The index can't physically realize the mixed order,
// so the planner must fall back to an in-memory sort rather than mis-serve it.
func TestIndex_Audit_MixedDirCompound_CorrectOrder(t *testing.T) {
	mk := func(withIndex bool) Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "-b"}}))
		}
		for i := 0; i < 12; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i%4))))
		}
		return coll
	}
	idx, noidx := mk(true), mk(false)
	assert.Equal(t, auditABPairs(t, noidx, "a", "-b"), auditABPairs(t, idx, "a", "-b"), "Sort(a,-b)")
	assert.Equal(t, auditABPairs(t, noidx, "-a", "b"), auditABPairs(t, idx, "-a", "b"), "Sort(-a,b)")
	// Explicit within-group check for Sort(a,-b): b strictly tracked descending.
	prevA, prevB, first := -1, -1, true
	for _, p := range auditABPairs(t, idx, "a", "-b") {
		var a, b int
		_, _ = fmt.Sscanf(p, "%d,%d", &a, &b)
		if !first && a == prevA {
			assert.LessOrEqual(t, b, prevB, "b must be descending within a=%d", a)
		}
		prevA, prevB, first = a, b, false
	}
}

// bug-01b: a reverse-declared covering filter field must not drop rows.
func TestIndex_Audit_ReverseCoveringFilter_NotDropped(t *testing.T) {
	mk := func(withIndex bool) Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b", "-c"}}))
		}
		for i := 0; i < 60; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%3, i%4, i%5))))
		}
		return coll
	}
	idx, noidx := mk(true), mk(false)
	const q = `{"a":2,"c":0}`
	explain, err := idx.Find(q).Sort("a", "b").Explain(ctx)
	require.NoError(t, err)
	// Confirm the plan actually exercises the IndexFilter covering path.
	assert.Contains(t, explain.Sql, "IndexFilter")

	collect := func(c Collection) []string {
		iter, err := c.Find(q).Sort("a", "b").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var out []string
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			out = append(out, fmt.Sprintf("%d,%d,%d",
				d.Value().GetInt("a"), d.Value().GetInt("b"), d.Value().GetInt("c")))
		}
		require.NoError(t, iter.Err())
		return out
	}
	got := collect(idx)
	assert.Equal(t, collect(noidx), got, "indexed reverse-covering-filter result must match unindexed")
	assert.NotEmpty(t, got, "covering filter must not drop all rows")
}

// bug-02: skip-middle compound Count must not over-count a trailing equality.
func TestIndex_Audit_SkipMiddleCount_NoOvercount(t *testing.T) {
	mk := func(withIndex bool) Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b", "c"}}))
		}
		for i := 0; i < 200; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%5, i%4, i%3))))
		}
		return coll
	}
	idx, noidx := mk(true), mk(false)

	for _, q := range []string{`{"a":1,"c":0}`, `{"a":2,"c":0}`, `{"a":1,"b":2,"c":0}`, `{"a":1,"b":2}`} {
		want, err := noidx.Find(q).Count(ctx)
		require.NoError(t, err)
		gotCount, err := idx.Find(q).Count(ctx)
		require.NoError(t, err)
		iterLen := len(auditIntField(t, idx.Find(q), "id"))
		assert.Equal(t, want, gotCount, "Count must match unindexed for %s", q)
		assert.Equal(t, iterLen, gotCount, "Count must equal Iter length for %s", q)
	}
	// Concrete sanity: a=1,c=0 over 200 docs (a=i%5,c=i%3) → i%15==6 → 13 docs.
	cnt, err := idx.Find(`{"a":1,"c":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 13, cnt)
}

// bug-03: offset without limit on the in-memory sort path must return the tail.
func TestIndex_Audit_OffsetWithoutLimit_InMemorySort(t *testing.T) {
	t.Run("no index, sort on field", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}
		full := auditIntField(t, coll.Find(nil).Sort("a"), "a")
		require.Len(t, full, 20)
		got := auditIntField(t, coll.Find(nil).Sort("a").Offset(5), "a")
		assert.Equal(t, full[5:], got)
	})

	t.Run("index on a, sort on non-indexed field b (forces SortIter)", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 20; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i))))
		}
		full := auditIntField(t, coll.Find(`{"a":{"$gte":0}}`).Sort("b"), "b")
		require.Len(t, full, 20)
		got := auditIntField(t, coll.Find(`{"a":{"$gte":0}}`).Sort("b").Offset(3), "b")
		assert.Equal(t, full[3:], got)
	})
}

// baseline: single-field reverse indexes scan-direction must stay correct
// (the bug-01 fix must not regress single-field reverse).
func TestIndex_Audit_SingleReverse_Baseline(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-a"}}))
	for i := 0; i < 8; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, auditIntField(t, coll.Find(nil).Sort("a"), "a"))
	assert.Equal(t, []int{7, 6, 5, 4, 3, 2, 1, 0}, auditIntField(t, coll.Find(nil).Sort("-a"), "a"))
	assert.Equal(t, []int{3, 4, 5, 6}, auditIntField(t, coll.Find(`{"a":{"$gte":3,"$lte":6}}`).Sort("a"), "a"))
}
