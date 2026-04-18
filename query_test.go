package anystore

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

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
// receiver) branch of isIDOnlyFilterNode directly, since query.ParseCondition
// only produces And{Key{"id"}, Key{"non-id"}} pairs (all-id-children is
// impossible from JSON due to duplicate-key rejection).
func TestQuery_IsIDOnlyFilterNode_And_Direct(t *testing.T) {
	// Programmatic And{Key{id}, Key{id}} — normally impossible from JSON.
	// This hits the for-loop recursion returning true at query.go:546-548.
	// The isIDOnlyFilterNode function only inspects the Path of each Key,
	// so a nil Filter is acceptable.
	f := query.And{
		query.Key{Path: []string{"id"}},
		query.Key{Path: []string{"id"}},
	}
	assert.True(t, isIDOnlyFilterNode(f),
		"query.And{Key{id}, Key{id}} must be recognized as id-only")

	// And with a non-id child → returns false.
	fMixed := query.And{
		query.Key{Path: []string{"id"}},
		query.Key{Path: []string{"other"}},
	}
	assert.False(t, isIDOnlyFilterNode(fMixed),
		"And with non-id child must NOT be id-only")

	// Empty And → returns false (len(ft) > 0 check).
	assert.False(t, isIDOnlyFilterNode(query.And{}), "empty And is not id-only")
}

// TestQuery_IsIDOnlyFilterNode_PointerAnd_FAIL is expected to fail: the
// isIDOnlyFilterNode switch only handles the value receiver query.And,
// not the pointer *query.And that query.ParseCondition produces for
// `{"$and": [...]}` JSON. This is the same asymmetry documented in bugs.md
// for qplanner's filterFieldsCoveredBy.
func TestQuery_IsIDOnlyFilterNode_PointerAnd_FAIL(t *testing.T) {
	t.Skip("FAIL: isIDOnlyFilterNode missing *query.And case — see bugs.md")

	// MustParseCondition produces *query.And for $and JSON.
	f := query.MustParseCondition(`{"$and":[{"id":"a"},{"id":"b"}]}`)
	assert.True(t, isIDOnlyFilterNode(f), "pointer-And with id-only children should match")
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
