package anystore

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
)

// fixedCountTx stands in for the read tx in docCount tests: it reports how
// often the fallback namespace walk ran.
type fixedCountTx struct {
	count int
	calls *int
}

func (f fixedCountTx) Count(ns *btree.Namespace) (int, error) {
	*f.calls++
	return f.count, nil
}

// TestCollQuery_DocCountSkipsWalkWithoutIndexes guards the plan-path shortcut:
// with no secondary indexes the planner has a single candidate, so
// docCountForPlan must not pay the O(namespace pages) tx.Count walk before
// every query; docCountExact (Explain) always returns the real number.
func TestCollQuery_DocCountSkipsWalkWithoutIndexes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	q := &collQuery{c: coll.(*collection)}
	idxs := q.c.loadIndexes()
	require.Empty(t, idxs)

	var calls int
	tx := fixedCountTx{count: 42, calls: &calls}

	assert.Equal(t, 0, q.docCountForPlan(tx, idxs))
	assert.Equal(t, 0, calls, "plan-path docCount must skip the namespace walk with no secondary indexes")

	assert.Equal(t, 42, q.docCountExact(tx, idxs), "explain-path docCount must stay exact")
	assert.Equal(t, 1, calls)
}

// TestCollQuery_InArrayMember_CountIterConsistent: an $in set containing an
// array (including the empty array) must produce the same result via the
// index/Count path and the filter/Iter path. In.Ok used to skip the
// whole-array membership probe that Comp.Ok performs, so Count (index bounds
// include whole-array keys) disagreed with Iter.
func TestCollQuery_InArrayMember_CountIterConsistent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"x":[]}`),
		anyenc.MustParseJson(`{"id":2,"x":1}`),
		anyenc.MustParseJson(`{"id":3,"x":[1,2]}`),
	))

	for filter, want := range map[string]int{
		`{"x":{"$in":[[],1]}}`:  3, // [] matches doc1 whole-array; 1 matches docs 2,3
		`{"x":{"$in":[[1,2]]}}`: 1, // whole-array member
		`{"x":{"$in":[[],5]}}`:  1, // empty array only
	} {
		cnt, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err, filter)
		assert.Equal(t, want, cnt, "Count %s", filter)
		iterN := 0
		it, err := coll.Find(filter).Iter(ctx)
		require.NoError(t, err, filter)
		for it.Next() {
			iterN++
		}
		require.NoError(t, it.Close())
		assert.Equal(t, want, iterN, "Iter %s", filter)
	}
}

func TestCollQuery_Count(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	t.Run("no filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(nil), 5)
	})

	t.Run("filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(`{"a":{"$in":[2,3,4]}}`), 3)
	})

}

// TestQueryCount_AndConjunctionLostInCount is the end-to-end regression pin
// for the lost-conjunct class: an indexed CountOnly query whose same-field
// conjuncts are mutually
// exclusive must Count 0, matching Iter. Pre-fix, And.IndexBounds dropped the
// $gte conjunct and the CountOnly fast path (which skips FilterIter for an
// index that "covers" the filter) returned 2. The fix gates that fast path:
// And.IndexBounds over-approximates and indexCoversFilter rejects the
// 2-predicate field (unit gate: qplanner.TestIndexCoversFilter_GatesMultiPredicateField).
// This test pins the fix at the public Count API.
func TestQueryCount_AndConjunctionLostInCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "i04")
	require.NoError(t, err)
	// Index on "a" so the planner takes the indexed PointLookup CountOnly
	// fast path — the only path the bug lives on (Iter always re-filters).
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":2}`),
		anyenc.MustParseJson(`{"id":3,"a":5}`),
		anyenc.MustParseJson(`{"id":4,"a":10}`),
	))

	// a ∈ {1,2} AND a >= 5 = ∅ for both the $and form and the equivalent
	// same-field inline form.
	for _, filter := range []string{
		`{"a":{"$in":[1,2]},"$and":[{"a":{"$gte":5}}]}`,
		`{"a":{"$in":[1,2],"$gte":5}}`,
	} {
		t.Run(filter, func(t *testing.T) {
			assertQueryCount(t, coll.Find(filter), 0)

			it, err := coll.Find(filter).Iter(ctx)
			require.NoError(t, err)
			n := 0
			for it.Next() {
				n++
			}
			require.NoError(t, it.Err())
			require.NoError(t, it.Close())
			assert.Equal(t, 0, n, "Iter must agree with Count")
		})
	}
}

// TestQueryCount_ArrayTwoSidedRange is the fail-before-fix gate for the
// array-range follow-up: a two-sided range ($gte AND $lte) over an
// ARRAY/multi-key field.
// Array filter semantics match each conjunct against the whole array
// independently, so a doc matches if SOME element is >=lo AND SOME (possibly
// different) element is <=hi. INTERSECTING the conjunct bounds (the original
// approach) narrows the seek to [lo,hi] and misses docs like [5,1,4] (5>=2,
// 1<=3, but no element in [2,3]) — under-counting Count AND Iter. The fix makes
// And.IndexBounds over-approximate (sound seek superset) and gates the CountOnly
// fast path so it is not taken when bounds don't exactly capture the filter.
func TestQueryCount_ArrayTwoSidedRange(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "arr_range")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":[5,1,4]}`), // 5>=2 and 1<=3 → matches (no element IN [2,3])
		anyenc.MustParseJson(`{"id":2,"tags":[3]}`),     // 3>=2 and 3<=3 → matches
		anyenc.MustParseJson(`{"id":3,"tags":[2,9]}`),   // 2>=2 and 2<=3 → matches
		anyenc.MustParseJson(`{"id":4,"tags":[7]}`),     // 7>=2 but no element <=3 → no match
	))

	const filter = `{"tags":{"$gte":2,"$lte":3}}`
	// Force the index so the bug path (the narrowed seek) is exercised; with 4
	// docs the cost model would otherwise pick FullScan, which filters correctly.
	hint := IndexHint{IndexName: "tags", Boost: 1_000_000}

	explain, err := coll.Find(filter).IndexHint(hint).Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "IndexScan", "reproducer must take the index path; got: %s", explain.Sql)
	// The index has seen arrays (multikey flag set), so the executed bounds
	// must stay the sound half-open over-approximation — tight seeks on this
	// index would drop doc 1 entirely.
	require.Contains(t, explain.Sql, "'<string>')",
		"a multikey index must serve wide (bracket-open) bounds; got: %s", explain.Sql)

	assertQueryCount(t, coll.Find(filter).IndexHint(hint), 3)

	it, err := coll.Find(filter).IndexHint(hint).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for it.Next() {
		n++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	assert.Equal(t, 3, n, "Iter must agree with Count")
}

// TestQueryCount_ScalarFirstCrossBoundDedup is the fail-before-fix gate for the
// scalar-first cross-bound shape: a mixed scalar/array index where a multi-key
// doc's array values
// straddle two $in bounds and each bound's first entry is scalar. The pre-fix
// countEntriesWithDedup peek-then-batch shortcut sees the scalar first entry,
// batch-counts the whole bound (including the array doc's multi-key entry),
// and double-counts the array doc → Count=4. The true distinct-doc count is 3,
// and Iter returns 3.
func TestQueryCount_ScalarFirstCrossBoundDedup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "i05")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "x", Fields: []string{"x"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"x":5}`),      // scalar, small id → sorts first under value 5
		anyenc.MustParseJson(`{"id":2,"x":10}`),     // scalar, small id → sorts first under value 10
		anyenc.MustParseJson(`{"id":3,"x":[5,10]}`), // multi-key, straddles both bounds
	))

	const filter = `{"x":{"$in":[5,10]}}`
	// Force the index so the bug path (indexed CountOnly) is exercised — with
	// only 3 docs the cost model would otherwise pick FullScan (which filters
	// correctly and hides the bug).
	hint := IndexHint{IndexName: "x", Boost: 1_000_000}

	explain, err := coll.Find(filter).IndexHint(hint).Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "IndexScan", "reproducer must take the index path; got: %s", explain.Sql)

	assertQueryCount(t, coll.Find(filter).IndexHint(hint), 3)

	it, err := coll.Find(filter).IndexHint(hint).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for it.Next() {
		n++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	assert.Equal(t, 3, n, "Iter must agree with Count")
}

// TestQueryCount_UniqueIndex_MultiKeyData_DedupsCorrectly is the fail-before-fix
// gate for the unique-index multi-key shape: a unique single-field index CAN
// hold an array doc (its elements
// are unique across docs). A multi-bound $in whose values are the doc's array
// elements routes through the unique CoverIter shortcut, which pre-fix hardcoded
// multiKey=false so DocDedup never collapsed the cross-bound repeats → Count=2.
// The true distinct-doc count is 1, and Iter returns 1.
func TestQueryCount_UniqueIndex_MultiKeyData_DedupsCorrectly(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "i06")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "x", Fields: []string{"x"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"x":["a","b"]}`),
	))

	const filter = `{"x":{"$in":["a","b"]}}`
	// Force the unique index so the CoverIter shortcut (where the bug lives) is
	// exercised rather than a FullScan over the single doc.
	hint := IndexHint{IndexName: "x", Boost: 1_000_000}

	explain, err := coll.Find(filter).IndexHint(hint).Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "CoverLookup", "reproducer must take the unique CoverIter path; got: %s", explain.Sql)

	assertQueryCount(t, coll.Find(filter).IndexHint(hint), 1)

	it, err := coll.Find(filter).IndexHint(hint).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for it.Next() {
		n++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	assert.Equal(t, 1, n, "Iter must agree with Count")
}

// TestQueryCount_Compound_ArrayNotFirst_NoDoubleCount is the forward-looking
// guard for the Branch-2 gate (BLOCKER-1): a compound index whose array field
// is NOT the first field encodes the array's 0x06 tag mid-key, so the
// canonical-key probe (Seek 0x06) would miss it and a probe-trusting batch
// count would double-count. CountEntries routes ALL compound shapes to
// sort-dedup without probing, so the doc whose tags straddle both bounds is
// counted once. (Honest note: this passes because Branch 2 never probes
// compound — it is a regression guard, not a fail-before-fix reproducer.)
func TestQueryCount_Compound_ArrayNotFirst_NoDoubleCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "compound_blocker1")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "pt", Fields: []string{"priority", "tags"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"priority":5,"tags":["a","b"]}`),
	))

	const filter = `{"priority":5,"tags":{"$in":["a","b"]}}`
	hint := IndexHint{IndexName: "pt", Boost: 1_000_000}

	explain, err := coll.Find(filter).IndexHint(hint).Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "IndexScan", "must use the compound index; got: %s", explain.Sql)

	assertQueryCount(t, coll.Find(filter).IndexHint(hint), 1)

	it, err := coll.Find(filter).IndexHint(hint).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for it.Next() {
		n++
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	assert.Equal(t, 1, n, "Iter must agree with Count")
}

func TestCollQuery_Explain(t *testing.T) {
	fx := newFixture(t)

	t.Run("no index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))

		explain, err := coll.Find(nil).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
		assert.Empty(t, explain.Indexes)
	})
	t.Run("more than 1000", func(t *testing.T) {
		var builder strings.Builder
		builder.Grow(4000)
		builder.WriteString(`{"id":{"$in":[`)
		l := 999
		for i := 1; i <= l; i++ {
			builder.WriteString(strconv.Itoa(i))
			if i < l {
				builder.WriteString(",")
			}
		}
		builder.WriteString("]}}")
		result := builder.String()

		coll, _ := fx.CreateCollection(ctx, "test_foo")

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))

		explain, err := coll.Find(result).Explain(ctx)
		require.NoError(t, err)
		assert.NotEmpty(t, explain.Sql)
	})
	t.Run("simple index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_simple_idx")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
		))

		explain, err := coll.Find(`{"a":"a1"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
		require.Len(t, explain.Indexes, 1)
		assert.Equal(t, "a", explain.Indexes[0].Name)
		assert.True(t, explain.Indexes[0].Used)
	})
	t.Run("many indexes", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_many_idx")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1", "b":"b1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2", "b":"b2"}`),
		))

		explain, err := coll.Find(`{"a":"a1"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
		require.Len(t, explain.Indexes, 2)

		t.Run("index hint", func(t *testing.T) {
			explain, err := coll.Find(`{"a":"a1"}`).IndexHint(IndexHint{IndexName: "b", Boost: 100}).Explain(ctx)
			require.NoError(t, err)
			assert.Contains(t, explain.Sql, "IndexScan")
		})
	})
	t.Run("composite index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_composite_idx")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1", "b":"b1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2", "b":"b2"}`),
		))

		explain, err := coll.Find(`{"a":"a1","b":"b1"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
		require.Len(t, explain.Indexes, 1)
		assert.True(t, explain.Indexes[0].Used)
	})
}

func TestCollQuery_Delete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	res, err := coll.Find(`{"a":{"$in":[2,3,4]}}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Matched)
	assert.Equal(t, 3, res.Modified)
	assertCollCount(t, coll, 2)
}

func TestCollQuery_Update(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	res, err := coll.Find(`{"a":{"$in":[2,3,4]}}`).Update(ctx, `{"$set":{"b":1}}`)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Matched)
	assert.Equal(t, 3, res.Modified)

	doc, err := coll.FindId(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, `{"id":2,"a":2,"b":1}`, doc.Value().String())
}

func TestCollQuery_Sort(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":3}`),
		anyenc.MustParseJson(`{"id":2, "a":1}`),
		anyenc.MustParseJson(`{"id":3, "a":2}`),
	))

	iter, err := coll.Find(nil).Sort("a").Iter(ctx)
	require.NoError(t, err)
	var ids []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().GetInt("id"))
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []int{2, 3, 1}, ids)
}

func TestCollQuery_SortDesc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":3}`),
		anyenc.MustParseJson(`{"id":2, "a":1}`),
		anyenc.MustParseJson(`{"id":3, "a":2}`),
	))

	iter, err := coll.Find(nil).Sort("-a").Iter(ctx)
	require.NoError(t, err)
	var ids []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().GetInt("id"))
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []int{1, 3, 2}, ids)
}

func TestCollQuery_LimitOffset(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	assertQueryCount(t, coll.Find(nil).Limit(3), 3)
	assertQueryCount(t, coll.Find(nil).Offset(3), 2)
	assertQueryCount(t, coll.Find(nil).Limit(2).Offset(3), 2)
}

func assertQueryCount(t testing.TB, q Query, expected int) {
	t.Helper()
	count, err := q.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, count)
}

// --- Coverage tests from query_matchall_coverage_test.go ---

// TestQuery_Coverage_MatchAllBaseline verifies the match-all baseline: both
// Find(nil) and Find("{}") return every document in the collection (N), and
// both route through the FullScan plan when no index is available.
//
// Gap item 70: Find(nil) or Find({}) — match-all baseline.
func TestQuery_Coverage_MatchAllBaseline(t *testing.T) {
	const N = 100
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	docs := make([]*anyenc.Value, N)
	for i := 0; i < N; i++ {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	t.Run("nil filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(nil), N)

		explain, err := coll.Find(nil).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"Find(nil) with no index must route through FullScan")
	})

	t.Run("empty object filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(`{}`), N)

		explain, err := coll.Find(`{}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"Find({}) with no index must route through FullScan")
	})

	t.Run("nil and empty produce same count", func(t *testing.T) {
		nilCount, err := coll.Find(nil).Count(ctx)
		require.NoError(t, err)
		emptyCount, err := coll.Find(`{}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, nilCount, emptyCount,
			"Find(nil) and Find({}) must return the same number of docs")
		assert.Equal(t, N, nilCount)
	})
}

// TestQuery_Count_IDOnlyFastPath covers query.go:380-395 (ID-only filter fast
// path) plus isIDOnlyFilter/isIDOnlyFilterNode at 534-552. A filter that only
// references "id" with equality bounds hits the tx.Get lookup loop.
//
// Limitation on observability: the fast path at query.go:382 returns before
// BuildPlan is called, but there is no perf counter, exported metric, or other
// side-effect that distinguishes it from the CBO path. Calling Explain() on
// the same filter cannot tell them apart because Explain always runs BuildPlan
// regardless of what Count would do. Counts are therefore the strongest
// available black-box observable; we deliberately avoid fabricating a signal
// (e.g. testing.AllocsPerRun) that would be fragile under race/coverage
// builds and would not actually prove which branch ran.
func TestQuery_Count_IDOnlyFastPath(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_count_id_only")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"a"}`),
		anyenc.MustParseJson(`{"id":"b"}`),
		anyenc.MustParseJson(`{"id":"c"}`),
	))

	t.Run("point_lookup_hit", func(t *testing.T) {
		n, err := coll.Find(anyenc.MustParseJson(`{"id":"a"}`)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
	t.Run("point_lookup_miss", func(t *testing.T) {
		n, err := coll.Find(anyenc.MustParseJson(`{"id":"not-there"}`)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, n)
	})
	t.Run("in_list", func(t *testing.T) {
		// $in produces multiple fixed bounds — still all id-only, fast path.
		n, err := coll.Find(anyenc.MustParseJson(`{"id":{"$in":["a","b","zzz"]}}`)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})
	t.Run("and_single_child_unwraps", func(t *testing.T) {
		// Documenting reality: parseAndArray at query/cond_parse.go:91 only
		// allocates a query.And when len(arr) > 1. A single-element $and
		// returns the bare child — so this input parses as query.Key{id},
		// NOT query.And. It still takes the fast path via the Key branch of
		// isIDOnlyFilterNode. The value-And branch of isIDOnlyFilterNode is
		// effectively unreachable from JSON and is covered directly by
		// TestQuery_IsIDOnlyFilterNode_And_Direct.
		n, err := coll.Find(anyenc.MustParseJson(`{"$and":[{"id":"a"}]}`)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

// TestQuery_Sort_ParseError covers query.go:109-112 — an invalid sort spec
// stores the error on q.err and surfaces when Iter is called.
func TestQuery_Sort_ParseError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_sort_err")
	require.NoError(t, err)

	// ParseSort rejects non-string, non-Sort arguments at query/sort.go:34
	// with the literal message "unexpected sort argument type: %T".
	_, err = coll.Find(nil).Sort(42).Iter(ctx)
	require.Error(t, err, "non-string, non-Sort sort argument must error")
	assert.Contains(t, err.Error(), "sort argument type",
		"error must originate from query/sort.go ParseSort, not a deeper layer")
}

// TestQuery_Update_ParseModifierError covers query.go:158-160.
func TestQuery_Update_ParseModifierError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_update_mod_err")
	require.NoError(t, err)
	// Insert a doc so we can verify it is UNCHANGED after the failed Update.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":"original"}`)))

	// Use an unknown top-level operator so the error originates from the
	// modifier parse layer (query/modifier_parse.go:110 emits
	// "unknown modifier '$badOp'"), not from a generic JSON-layer failure.
	res, err := coll.Find(nil).Update(ctx, `{"$badOp":{}}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown modifier",
		"error must originate from query/modifier_parse.go ParseModifier")

	// On parse failure the update must be a no-op: no matches, no modifications.
	assert.Equal(t, 0, res.Matched, "failed parse must not match any docs")
	assert.Equal(t, 0, res.Modified, "failed parse must not modify any docs")

	// Verify the stored doc is untouched.
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "the doc must still exist")

	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, "original", string(doc.Value().GetStringBytes("a")),
		"the doc's 'a' field must be unchanged after failed Update")
}

// TestQuery_Iter_FilterParseError covers query.go:117-120 via Cond -> makeQuery.
func TestQuery_Iter_FilterParseError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_iter_filter_err")
	require.NoError(t, err)

	// Use malformed $and (string instead of array) so the error originates
	// from the condition parser itself (query/cond_parse.go:87 emits
	// "$and must be an array") rather than the JSON tokenizer.
	iter, err := coll.Find(`{"$and":"not array"}`).Iter(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$and must be an array",
		"error must originate from query/cond_parse.go parseAndArray")
	assert.Nil(t, iter, "Iter must return a nil iterator on parse failure")
}

// TestQuery_IsIDOnlyFilterNode_And_Direct tests the query.And (value
// receiver) branch of isIDOnlyFilterNode directly. Since the exact-shapes fix an
// And qualifies only as a transparent single-child wrapper: two pk Keys mean
// two predicates on the pk, whose match set (an intersection) is not the
// point set tx.Has would probe — {$and:[{id:1},{id:2}]} matches nothing yet
// has two point bounds.
func TestQuery_IsIDOnlyFilterNode_And_Direct(t *testing.T) {
	eq := query.MustParseCondition(`{"id":"a"}`).(query.Key)

	// Single-child wrapper stays id-only.
	assert.True(t, isIDOnlyFilterNode(query.And{eq}, "id"),
		"single-child And wrapping an Eq pk Key must be id-only")

	// Two pk predicates are inexact — rejected even though both are Eq.
	f := query.And{eq, eq}
	assert.False(t, isIDOnlyFilterNode(f, "id"),
		"And with two pk predicates must NOT be id-only")

	// And with a non-id child → returns false.
	fMixed := query.And{
		eq,
		query.Key{Path: []string{"other"}},
	}
	assert.False(t, isIDOnlyFilterNode(fMixed, "id"),
		"And with non-id child must NOT be id-only")

	// Empty And → returns false.
	assert.False(t, isIDOnlyFilterNode(query.And{}, "id"), "empty And is not id-only")
}

// TestQuery_IsIDOnlyFilterNode_PointerAnd verifies the *query.And pointer-arm
// of isIDOnlyFilterNode: query.MustParseCondition produces *query.And for
// `{"$and": [...]}` JSON. A single-child $and delegates to the value arm and
// stays id-only; multiple pk children are rejected as inexact.
func TestQuery_IsIDOnlyFilterNode_PointerAnd(t *testing.T) {
	single := query.MustParseCondition(`{"$and":[{"id":"a"}]}`)
	assert.True(t, isIDOnlyFilterNode(single, "id"),
		"single-child pointer-And wrapping an Eq pk Key must be id-only")

	multi := query.MustParseCondition(`{"$and":[{"id":"a"},{"id":"b"}]}`)
	assert.False(t, isIDOnlyFilterNode(multi, "id"),
		"pointer-And with two pk predicates must NOT be id-only")
}

// TestQuery_Update_NoopModifier covers query.go:258-261 — when the modifier
// reports isModified=false, Matched is still incremented but Modified is not.
func TestQuery_Update_NoopModifier(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_update_noop")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	// `{"$set":{"a":1}}` — set field "a" to 1 where it's already 1 →
	// the modifier reports isModified=false.
	res, err := coll.Find(nil).Update(ctx, `{"$set":{"a":1}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Matched, "Matched counts every visited doc")
	assert.Equal(t, 0, res.Modified, "Modified counts only actually-modified docs")
}

// TestQuery_Update_ActualModify covers query.go:263-270 — when modifier does
// change the doc, newItem/update succeed and Modified is incremented.
func TestQuery_Update_ActualModify(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_update_real")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	res, err := coll.Find(nil).Update(ctx, `{"$set":{"a":42}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Matched)
	assert.Equal(t, 1, res.Modified)
}

// TestQuery_Delete_Basic covers query.go:300-350 basic Delete path — no
// match, single match, and multiple matches. Also asserts Modified is set
// to the same count as Matched (query.go:352) and verifies idempotence
// on re-delete of an already-deleted set.
func TestQuery_Delete_Basic(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_delete_basic")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":2}`),
		anyenc.MustParseJson(`{"id":3,"a":2}`),
	))

	res, err := coll.Find(anyenc.MustParseJson(`{"a":2}`)).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Matched)
	assert.Equal(t, 2, res.Modified, "Delete sets Modified == Matched (query.go:352)")

	// Verify remaining doc count.
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Verify the sole survivor's id is 1 (the one doc with a:1).
	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()
	var survivors []int
	for it.Next() {
		doc, docErr := it.Doc()
		require.NoError(t, docErr)
		survivors = append(survivors, doc.Value().GetInt("id"))
	}
	require.NoError(t, it.Err())
	assert.Equal(t, []int{1}, survivors, "only id=1 must remain after deleting a==2")

	// Idempotence: running the same Delete again must match nothing.
	res2, err := coll.Find(anyenc.MustParseJson(`{"a":2}`)).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, res2.Matched, "second Delete over already-deleted set must match 0")
	assert.Equal(t, 0, res2.Modified, "second Delete must not modify anything")
}

// TestQuery_Explain_Basic covers query.go:445-493. Asserts structural
// properties of Explain output beyond mere non-emptiness: the chosen index
// must appear first (query.go:485 prepends the used index), the chosen
// index must be flagged Used==true, Plan must mention the index name, and
// Sql must describe the iterator chain (IndexScan / Fetch / etc.).
//
// We insert enough docs that the CBO chooses IndexSeek(a) over FullScan;
// with only 1 doc FullScan wins by cost and the assertions would be wrong.
func TestQuery_Explain_Basic(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_explain")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))

	// Populate enough rows that IndexSeek(a) beats FullScan on cost.
	for i := 0; i < 200; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)),
		))
	}

	ex, err := coll.Find(anyenc.MustParseJson(`{"a":1}`)).Explain(ctx)
	require.NoError(t, err)

	// Indexes: chosen index is prepended at query.go:485, so "a" must be
	// first and marked Used.
	require.NotEmpty(t, ex.Indexes, "at least one index must be reported")
	assert.Equal(t, "a", ex.Indexes[0].Name, "chosen index is listed first")
	assert.True(t, ex.Indexes[0].Used, "chosen index must be flagged Used==true")

	// Plan is the multi-line ExplainString — it must reference the chosen
	// index name "a" (as "Index: a") and should identify the iterator kind.
	assert.Contains(t, ex.Plan, "a", "Plan should mention the index name 'a'")
	assert.Contains(t, ex.Plan, "IndexSeek", "Plan should identify the chosen iterator as IndexSeek")

	// Sql is the single-line iterator chain from plan.String() — for an
	// index-backed query it includes "IndexScan" (the actual Root type
	// name) and the index name. "Scan" is the descriptive token the task
	// prescribes; it is present inside "IndexScan".
	require.NotEmpty(t, ex.Sql, "Sql must describe the iterator chain")
	assert.Contains(t, ex.Sql, "Scan", "Sql should contain a descriptive iterator verb (Scan/Seek)")
	assert.Contains(t, ex.Sql, "a", "Sql should reference the chosen index 'a'")
}

// TestQuery_Count_FilterParseError covers the err-propagation branch at
// query.go:367-369 (q.err != nil returns early). The fast path at line 363
// triggers when q.cond is nil, so we must add a Limit to force the slow path.
func TestQuery_Count_FilterParseError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_count_filter_err")
	require.NoError(t, err)

	// Limit(1) bypasses the fast-path (which requires q.limit == 0) → the
	// q.err check at 367-369 fires. Using malformed $and produces a
	// query-layer error ("$and must be an array") from cond_parse.go:87.
	count, err := coll.Find(`{"$and":"not array"}`).Limit(1).Count(ctx)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "$and must be an array",
		"error must originate from query/cond_parse.go parseAndArray")
	assert.Equal(t, 0, count, "Count must return 0 on parse failure")
}

// --- Unsatisfiable filter fast path (audit 11 follow-up) ---

// TestIsUnsatisfiable covers the helper directly: every recognised
// shape of "this filter can never match" plus a handful of negative
// cases that must NOT be reported as unsatisfiable.
func TestIsUnsatisfiable(t *testing.T) {
	emptyIn := query.MustParseCondition(`{"a":{"$in":[]}}`)
	nonemptyIn := query.MustParseCondition(`{"a":{"$in":[1,2,3]}}`)
	eq := query.MustParseCondition(`{"a":1}`)

	t.Run("nil_filter", func(t *testing.T) {
		assert.False(t, isUnsatisfiable(nil),
			"nil filter is satisfiable (matches all)")
	})

	t.Run("All", func(t *testing.T) {
		assert.False(t, isUnsatisfiable(query.All{}))
	})

	t.Run("empty_In_top_level_via_Key", func(t *testing.T) {
		// {"a":{"$in":[]}} parses to query.Key{Path:["a"], Filter: In{Values: empty}}.
		assert.True(t, isUnsatisfiable(emptyIn))
	})

	t.Run("nonempty_In_via_Key", func(t *testing.T) {
		assert.False(t, isUnsatisfiable(nonemptyIn))
	})

	t.Run("equality_via_Key", func(t *testing.T) {
		assert.False(t, isUnsatisfiable(eq))
	})

	t.Run("In_directly_empty", func(t *testing.T) {
		assert.True(t, isUnsatisfiable(query.NewInValue()))
	})

	t.Run("And_with_empty_In_is_unsatisfiable", func(t *testing.T) {
		// {"$and":[{"a":1},{"b":{"$in":[]}}]} — short-circuits on the empty In.
		f := query.MustParseCondition(`{"$and":[{"a":1},{"b":{"$in":[]}}]}`)
		assert.True(t, isUnsatisfiable(f))
	})

	t.Run("And_value_form_with_empty_In", func(t *testing.T) {
		// `{"a":1, "b":{"$in":[]}}` parses to query.And (value, not pointer).
		f := query.MustParseCondition(`{"a":1, "b":{"$in":[]}}`)
		assert.True(t, isUnsatisfiable(f))
	})

	t.Run("Or_with_one_satisfiable_branch_is_satisfiable", func(t *testing.T) {
		// {"$or":[{"a":1},{"b":{"$in":[]}}]} — empty In is unsatisfiable but
		// the Or as a whole is satisfiable via the {"a":1} branch.
		f := query.MustParseCondition(`{"$or":[{"a":1},{"b":{"$in":[]}}]}`)
		assert.False(t, isUnsatisfiable(f))
	})

	t.Run("Or_all_branches_unsatisfiable_is_unsatisfiable", func(t *testing.T) {
		f := query.MustParseCondition(`{"$or":[{"a":{"$in":[]}},{"b":{"$in":[]}}]}`)
		assert.True(t, isUnsatisfiable(f))
	})

	t.Run("Not_on_unsatisfiable_inner_is_satisfiable", func(t *testing.T) {
		// $not on always-false is always-true, NOT unsatisfiable. The
		// helper is conservative and returns false here.
		f := query.MustParseCondition(`{"a":{"$not":{"$in":[]}}}`)
		assert.False(t, isUnsatisfiable(f))
	})
}

// TestQuery_Unsatisfiable_Count_EmptyIn pins the fast path: Count with
// {field: $in:[]} returns 0 with no error and (by construction in the
// query.go fast path) does not open a read tx or build a plan.
func TestQuery_Unsatisfiable_Count_EmptyIn(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "unsat_count")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":2}`),
	))

	// Top-level empty $in.
	n, err := coll.Find(`{"a":{"$in":[]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	// Empty $in inside $and — still unsatisfiable.
	n, err = coll.Find(`{"$and":[{"a":1},{"b":{"$in":[]}}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)

	// Empty $in inside $or with a satisfiable branch — NOT unsatisfiable.
	// Should match doc 1 via {"a":1}.
	n, err = coll.Find(`{"$or":[{"a":1},{"b":{"$in":[]}}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"$or with at least one satisfiable branch must NOT take the unsat fast path")
}

// TestQuery_Unsatisfiable_Iter_EmptyIn pins that Iter on an empty $in
// returns an iterator that yields nothing, with no error from open or
// close. Verifies that Doc() on an exhausted unsat iterator returns
// ErrDocNotFound (the chosen sentinel for the empty path).
func TestQuery_Unsatisfiable_Iter_EmptyIn(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "unsat_iter")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":2}`),
	))

	it, err := coll.Find(`{"a":{"$in":[]}}`).Iter(ctx)
	require.NoError(t, err)

	// Yields zero docs.
	count := 0
	for it.Next() {
		count++
	}
	assert.Equal(t, 0, count, "empty $in iterator must yield zero docs")
	require.NoError(t, it.Err())

	// Close is idempotent in the success direction; second close errors
	// (matches the planIterator contract).
	require.NoError(t, it.Close())
	assert.ErrorIs(t, it.Close(), ErrIterClosed)
}

// TestQuery_Unsatisfiable_Update_EmptyIn pins that UpdateMany with an
// empty-$in filter parses the modifier (so a malformed modifier still
// errors) but then short-circuits to ModifyResult{} with no write tx.
func TestQuery_Unsatisfiable_Update_EmptyIn(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "unsat_update")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"n":0}`),
		anyenc.MustParseJson(`{"id":2,"a":2,"n":0}`),
	))

	// Valid modifier + unsatisfiable filter → zero result, no error.
	res, err := coll.Find(`{"a":{"$in":[]}}`).Update(ctx, `{"$inc":{"n":1}}`)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Matched)
	assert.Equal(t, 0, res.Modified)

	// Verify no doc was modified.
	for _, id := range []int{1, 2} {
		doc, derr := coll.FindId(ctx, id)
		require.NoError(t, derr)
		assert.Equal(t, float64(0), doc.Value().Get("n").GetFloat64(),
			"doc id=%d must not have been modified", id)
	}

	// Malformed modifier MUST still surface an error (the modifier is
	// parsed BEFORE the unsat check, by design).
	_, err = coll.Find(`{"a":{"$in":[]}}`).Update(ctx, `{"$badop":{"n":1}}`)
	require.Error(t, err,
		"malformed modifier must error even when the filter is unsatisfiable")
}

// TestQuery_Unsatisfiable_Delete_EmptyIn pins that DeleteMany with an
// empty-$in filter returns a zero ModifyResult without opening a write
// tx, and leaves the collection contents untouched.
func TestQuery_Unsatisfiable_Delete_EmptyIn(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "unsat_delete")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1}`),
		anyenc.MustParseJson(`{"id":2,"a":2}`),
	))

	res, err := coll.Find(`{"a":{"$in":[]}}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Matched)
	assert.Equal(t, 0, res.Modified)

	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count, "delete with empty $in must not remove any doc")
}

// TestQuery_Unsatisfiable_AllOpsConsistent runs Count/Iter/Update/Delete
// against the SAME unsatisfiable filter shape and asserts they all agree
// on "matches nothing" — guards against any one of the four wires
// drifting away from the others.
func TestQuery_Unsatisfiable_AllOpsConsistent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "unsat_consistency")
	require.NoError(t, err)

	// Many docs to make any non-fast-path observably slower.
	for i := range 200 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"n":0}`, i, i),
		)))
	}

	// Filter shapes that are all unsatisfiable.
	for _, filter := range []string{
		`{"a":{"$in":[]}}`,
		`{"$and":[{"a":1},{"b":{"$in":[]}}]}`,
		`{"a":1, "b":{"$in":[]}}`,
		`{"$or":[{"a":{"$in":[]}},{"b":{"$in":[]}}]}`,
	} {
		t.Run(filter, func(t *testing.T) {
			n, err := coll.Find(filter).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, n, "Count")

			it, err := coll.Find(filter).Iter(ctx)
			require.NoError(t, err)
			yielded := 0
			for it.Next() {
				yielded++
			}
			require.NoError(t, it.Err())
			require.NoError(t, it.Close())
			assert.Equal(t, 0, yielded, "Iter yield")

			res, err := coll.Find(filter).Update(ctx, `{"$inc":{"n":1}}`)
			require.NoError(t, err)
			assert.Equal(t, 0, res.Matched, "Update Matched")
			assert.Equal(t, 0, res.Modified, "Update Modified")

			res, err = coll.Find(filter).Delete(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, res.Matched, "Delete Matched")
			assert.Equal(t, 0, res.Modified, "Delete Modified")
		})
	}

	// Sanity: collection unchanged.
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 200, count)
}

// TestQueryCount_IdFastPathExactShapesOnly: the id-only Count fast path probes tx.Has per
// point bound and never runs the residual filter, so it may fire only when the
// filter's match set exactly equals its bounds — a single Eq or $in predicate
// on the pk. Every shape below over-counted before the filter-shape gate.
func TestQueryCount_IdFastPathExactShapesOnly(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "bug12")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1}`),
		anyenc.MustParseJson(`{"id":2}`),
		anyenc.MustParseJson(`{"id":3}`),
	))

	countViaIter := func(t *testing.T, filter string) int {
		it, err := coll.Find(filter).Iter(ctx)
		require.NoError(t, err)
		n := 0
		for it.Next() {
			n++
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		return n
	}

	for _, tc := range []struct {
		filter string
		want   int
	}{
		// Exactly-representable shapes: the fast path stays correct.
		{`{"id":2}`, 1},
		{`{"id":{"$in":[1,2,3]}}`, 3},
		{`{"id":{"$in":[1,5]}}`, 1},
		{`{"$and":[{"id":2}]}`, 1},
		// Extra predicates beyond the first contributor: wrong before the gate.
		{`{"id":{"$gt":1,"$lt":5}}`, 2},          // was 1 (Has on exclusive Start)
		{`{"id":{"$in":[1,2,3],"$nin":[2]}}`, 2}, // was 3
		{`{"id":{"$in":[1,2,3],"$gt":1}}`, 2},    // was 3
		{`{"id":{"$in":[1,2,3],"$type":"string"}}`, 0}, // was 3; $type adds no bounds
		{`{"id":{"$gt":1}}`, 2},
		{`{"id":{"$ne":2}}`, 2},
		// Two pk predicates via $and: two point bounds, empty match set.
		{`{"$and":[{"id":1},{"id":2}]}`, 0},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			assertQueryCount(t, coll.Find(tc.filter), tc.want)
			assert.Equal(t, tc.want, countViaIter(t, tc.filter), "Count must agree with Iter")
		})
	}
}

// TestQuery_ReverseMultiIntervalOrder: IndexIter must visit a multi-interval bound
// set ($in => one point interval per value) in DESCENDING interval order when
// scanning in reverse. Before the fix it walked intervals ascending (each
// internally reversed), so a reverse ExactSort plan — which adds no SortIter —
// yielded globally misordered rows, and with Limit the WRONG rows.
func TestQuery_ReverseMultiIntervalOrder(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "bug13")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 1; i <= 9; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}
	hint := IndexHint{IndexName: "a", Boost: 1_000_000}

	collectA := func(t *testing.T, q Query) []int {
		it, err := q.Iter(ctx)
		require.NoError(t, err)
		var got []int
		for it.Next() {
			d, derr := it.Doc()
			require.NoError(t, derr)
			got = append(got, d.Value().GetInt("a"))
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		return got
	}

	// The plan must serve the sort from the index in reverse with no SortIter
	// for the bug to be reachable; pin that with explain.
	explain, err := coll.Find(`{"a":{"$in":[1,5,9]}}`).IndexHint(hint).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "IndexScan(a)(reverse)", "expected a reverse index scan plan: %s", explain.Sql)
	require.NotContains(t, explain.Sql, "Sort", "the sort must be served by the index, not a SortIter: %s", explain.Sql)

	// Full descending order across intervals.
	got := collectA(t, coll.Find(`{"a":{"$in":[1,5,9]}}`).IndexHint(hint).Sort("-a"))
	assert.Equal(t, []int{9, 5, 1}, got, "descending order must hold ACROSS intervals")

	// With Limit the cutoff must keep the HIGHEST values.
	got = collectA(t, coll.Find(`{"a":{"$in":[1,5,9]}}`).IndexHint(hint).Sort("-a").Limit(1))
	assert.Equal(t, []int{9}, got, "Limit(1) must return the doc with the highest a")

	// Offset skips from the top; ascending order is unaffected.
	got = collectA(t, coll.Find(`{"a":{"$in":[1,5,9]}}`).IndexHint(hint).Sort("-a").Offset(1).Limit(1))
	assert.Equal(t, []int{5}, got, "Offset(1).Limit(1) must return the second-highest a")
	got = collectA(t, coll.Find(`{"a":{"$in":[1,5,9]}}`).IndexHint(hint).Sort("a"))
	assert.Equal(t, []int{1, 5, 9}, got, "ascending order must be unchanged")

	// $ne carves a one-field bound set into two rays — same interval-order
	// requirement once tight bounds land (two-sided-bounds plan), and already
	// exercisable today via $in.
	got = collectA(t, coll.Find(`{"a":{"$in":[2,4,6,8]}}`).IndexHint(hint).Sort("-a").Limit(2))
	assert.Equal(t, []int{8, 6}, got, "Limit quota must carry across interval boundaries in reverse")
}

// TestQuery_ReverseMultiIntervalOrder_FullScan pins the same cross-interval
// descending contract on the FullScanIter path (pk $in bounds), which already
// consumed intervals from the top — parity guard for the reverse interval-order fix.
func TestQuery_ReverseMultiIntervalOrder_FullScan(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "bug13fs")
	require.NoError(t, err)
	for i := 1; i <= 9; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}
	it, err := coll.Find(`{"id":{"$in":[1,5,9]}}`).Sort("-id").Iter(ctx)
	require.NoError(t, err)
	var got []int
	for it.Next() {
		d, derr := it.Doc()
		require.NoError(t, derr)
		got = append(got, d.Value().GetInt("id"))
	}
	require.NoError(t, it.Err())
	require.NoError(t, it.Close())
	assert.Equal(t, []int{9, 5, 1}, got)
}

// TestQueryCount_LimitOffsetMultiKey is the regression gate:
// Count with Limit/Offset over a multi-key index must agree with Iter. The
// LimitIter cutoff used to apply to raw index-entry rows while doc dedup ran
// only in the consumer loop, so limit capped entry-rows that then collapsed
// (Limit(3).Count() = 2) and offset skipped entry-rows that were fewer
// distinct docs (Offset(4).Count() = 8 instead of 6).
func TestQueryCount_LimitOffsetMultiKey(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "i07")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "x", Fields: []string{"x"}}))
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":[%d,%d]}`, i, i, i+1))))
	}
	const filter = `{"x":{"$in":[0,1,2,3,4,5,6,7,8,9,10]}}`
	hint := IndexHint{IndexName: "x", Boost: 1_000_000}

	countViaIter := func(t *testing.T, q Query) int {
		it, err := q.Iter(ctx)
		require.NoError(t, err)
		n := 0
		for it.Next() {
			n++
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		return n
	}

	for _, tc := range []struct {
		name          string
		limit, offset uint
		want          int // hardcoded so a bug shared by Iter and Count can't hide
	}{
		{"limit3", 3, 0, 3},
		{"offset4", 0, 4, 6},
		{"limit3_offset4", 3, 4, 3},
		{"limit20", 20, 0, 10},
		{"offset20", 0, 20, 0},
		{"no_cutoff", 0, 0, 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := func() Query {
				q := coll.Find(filter).IndexHint(hint)
				if tc.limit > 0 {
					q = q.Limit(tc.limit)
				}
				if tc.offset > 0 {
					q = q.Offset(tc.offset)
				}
				return q
			}
			require.Equal(t, tc.want, countViaIter(t, q()), "Iter ground truth")
			assertQueryCount(t, q(), tc.want)
		})
	}
}

// TestQuery_SortEdgeWidening pins widenSortEdges: over a possibly-multikey
// index, a bracket edge on the sort side is opened instead of demoting the
// index-order plan, while a value cut still demotes.
func TestQuery_SortEdgeWidening(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","a":[1,"x"]}`), // array write: the index is no longer scalar-proven
		anyenc.MustParseJson(`{"id":"2","a":"m"}`),
		anyenc.MustParseJson(`{"id":"3","a":"z"}`),
		anyenc.MustParseJson(`{"id":"4","a":5}`),
		anyenc.MustParseJson(`{"id":"5","a":false}`),
		anyenc.MustParseJson(`{"id":"6"}`),
	))
	explain := func(q Query) string {
		ex, err := q.Explain(ctx)
		require.NoError(t, err)
		return ex.Sql
	}

	t.Run("lt asc opens the lower edge", func(t *testing.T) {
		q := coll.Find(`{"a":{"$lt":"y"}}`).Sort("a")
		sql := explain(q)
		assert.Contains(t, sql, `IndexScan(a)[bounds=Bounds{[-inf,'"y"')}]`, sql)
		assert.NotContains(t, sql, "-> Sort", sql)
		assert.Contains(t, sql, "-> Filter", "the widened scan relies on the residual filter: %s", sql)
		// [1,"x"] sorts by its minimum element (1), ahead of "m".
		assert.Equal(t, []string{"1", "2"}, collectIdsString(t, q))
		assertQueryCount(t, coll.Find(`{"a":{"$lt":"y"}}`), 2)
	})
	t.Run("gte desc opens the upper edge", func(t *testing.T) {
		q := coll.Find(`{"a":{"$gte":"n"}}`).Sort("-a")
		sql := explain(q)
		assert.Contains(t, sql, `[bounds=Bounds{['"n"',inf]}]`, sql)
		assert.NotContains(t, sql, "-> Sort", sql)
		// [1,"x"] matches via "x" and sorts by its maximum element, after "z".
		assert.Equal(t, []string{"3", "1"}, collectIdsString(t, q))
	})
	t.Run("value cut still demotes", func(t *testing.T) {
		for _, tc := range []struct{ filter, sort string }{
			{`{"a":{"$gte":"n"}}`, "a"},
			{`{"a":{"$lt":"y"}}`, "-a"},
			{`{"a":false}`, "a"},
		} {
			sql := explain(coll.Find(tc.filter).Sort(tc.sort))
			assert.Contains(t, sql, "-> Sort", "%s sort %s: %s", tc.filter, tc.sort, sql)
		}
	})
	t.Run("count agrees", func(t *testing.T) {
		assertQueryCount(t, coll.Find(`{"a":{"$lt":"y"}}`).Sort("a"), 2)
		assertQueryCount(t, coll.Find(`{"a":{"$gte":"n"}}`).Sort("-a"), 2)
	})
}
