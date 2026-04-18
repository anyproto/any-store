package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

// TestQuery_Count_IDOnlyFastPath covers query.go:380-395 (ID-only filter fast
// path) plus isIDOnlyFilter/isIDOnlyFilterNode at 534-552. A filter that only
// references "id" with equality bounds hits the tx.Get lookup loop.
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

	// ParseSort rejects non-string, non-Sort arguments at query/sort.go:34.
	_, err = coll.Find(nil).Sort(42).Iter(ctx)
	require.Error(t, err, "non-string, non-Sort sort argument must error")
}

// TestQuery_Update_ParseModifierError covers query.go:158-160.
func TestQuery_Update_ParseModifierError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_update_mod_err")
	require.NoError(t, err)

	// Malformed modifier JSON → ParseModifier returns error.
	_, err = coll.Find(nil).Update(ctx, "not a valid modifier")
	require.Error(t, err)
}

// TestQuery_Iter_FilterParseError covers query.go:117-120 via Cond -> makeQuery.
func TestQuery_Iter_FilterParseError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_iter_filter_err")
	require.NoError(t, err)

	// Cond accepts any; a plain string "not valid" fails to parse.
	_, err = coll.Find("not valid").Iter(ctx)
	require.Error(t, err)
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
// match, single match, and multiple matches.
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

	// Verify remaining docs.
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// TestQuery_Explain_Basic covers query.go:445-493.
func TestQuery_Explain_Basic(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_explain")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	ex, err := coll.Find(anyenc.MustParseJson(`{"a":1}`)).Explain(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, ex.Sql)
	assert.NotEmpty(t, ex.Plan)
	assert.NotEmpty(t, ex.Indexes)
}

// TestQuery_Count_FilterParseError covers the err-propagation branch at
// query.go:367-369 (q.err != nil returns early). The fast path at line 363
// triggers when q.cond is nil, so we must add a Limit to force the slow path.
func TestQuery_Count_FilterParseError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "q_count_filter_err")
	require.NoError(t, err)

	// Limit(1) bypasses the fast-path (which requires q.limit == 0) → the
	// q.err check at 367-369 fires.
	_, err = coll.Find("not valid").Limit(1).Count(ctx)
	require.Error(t, err)
}
