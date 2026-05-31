package anystore

/*
Regression tests for correctness bugs found by the docs/qplanner audit
(see docs/qplanner/CORRECTIONS.md). Each test failed before its fix and
passes after. Fixes live in internal/qplanner/planner.go.

  bug-01  Mixed-direction compound index (a,-b) is stored with reverse-flagged
          fields bitwise-inverted, so a single forward scan yields (a asc, b desc)
          and a reverse scan (a desc, b asc). Sort(a,-b) / Sort(-a,b) are served by
          a FAST index scan with NO in-memory Sort; only genuinely-unrealizable
          orders (e.g. Sort(a,b) on an (a,-b) index) fall back to in-memory sort.
  bug-01b A reverse-declared covering filter field is stored inverted, so
          coveringFilterFields bitwise-inverts the match value to match the stored
          key bytes; IndexFilterIter then compares correctly and drops no rows.
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

// bug-01: a mixed-direction compound index (a,-b) is stored inverted so a single
// forward scan realizes (a asc, b desc). Sort(a,-b) and Sort(-a,b) must be served
// by a FAST index scan (no in-memory Sort), matching the unindexed twin's order.
// Sort(a,b) is genuinely unrealizable by one scan and falls back to in-memory sort.
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
	assert.Equal(t, auditABPairs(t, noidx, "a", "b"), auditABPairs(t, idx, "a", "b"), "Sort(a,b)")

	// Sort(a,-b) is served by a FORWARD index scan: no in-memory Sort.
	exFwd, err := idx.Find(nil).Sort("a", "-b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exFwd.Sql, "IndexScan(a,-b)")
	assert.NotContains(t, exFwd.Sql, "(reverse)")
	assert.NotContains(t, exFwd.Sql, "-> Sort")

	// Sort(-a,b) is the exact opposite: a REVERSE index scan, still no Sort.
	exRev, err := idx.Find(nil).Sort("-a", "b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exRev.Sql, "IndexScan(a,-b)")
	assert.Contains(t, exRev.Sql, "(reverse)")
	assert.NotContains(t, exRev.Sql, "-> Sort")

	// Sort(a,b) cannot be realized by a single scan direction → in-memory Sort.
	exMixed, err := idx.Find(nil).Sort("a", "b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exMixed.Sql, "-> Sort", "Sort(a,b) on (a,-b) must fall back to in-memory sort")

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

// baseline: a single-field reverse index (-a) stores values inverted, so a
// FORWARD scan yields descending order. Sort("-a") is therefore served by a
// forward scan (NO "(reverse)"), and Sort("a") by a reverse scan — both with no
// in-memory Sort. (Before the invert-on-write redesign, shouldReverse picked the
// wrong direction here.)
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

	// Sort("-a") == the index's declared direction → FORWARD scan, no Sort.
	exDesc, err := coll.Find(nil).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exDesc.Sql, "IndexScan(-a)")
	assert.NotContains(t, exDesc.Sql, "(reverse)")
	assert.NotContains(t, exDesc.Sql, "-> Sort")

	// Sort("a") == opposite of declared → REVERSE scan, still no Sort.
	exAsc, err := coll.Find(nil).Sort("a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exAsc.Sql, "IndexScan(-a)")
	assert.Contains(t, exAsc.Sql, "(reverse)")
	assert.NotContains(t, exAsc.Sql, "-> Sort")
}
