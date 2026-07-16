package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A reverse-declared index field stores bitwise-inverted encodings, and anyenc
// strings are prefix-free EXCEPT across the NUL escape: enc("a") is a
// byte-prefix of enc("a\x00b"), and inversion preserves that relation. The
// escape-continuation group therefore sorts BELOW the base value's own entries
// in stored space, so multi-bound lists ($in, $or of equalities) must be
// walked with the longer inverted Start first — a plain byte sort of bare
// Starts emits whole runs out of order. With ExactSort claimed (no SortIter to
// repair it) the wrong ORDER selects the wrong DOCUMENTS under Limit, and on
// compound indexes an unpadded concatenated Start additionally overlaps the
// continuation group's range: duplicate rows, double-applied modifiers,
// ErrDocNotFound on delete.

func requireIndexOrderedPlan(t *testing.T, q Query, indexName string) {
	t.Helper()
	ex, err := q.Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, ex.Sql, indexName, "test premise: index must drive the plan\nplan: %s", ex.Sql)
	require.NotContains(t, ex.Sql, "Sort", "test premise: order must come from the index walk, not a SortIter\nplan: %s", ex.Sql)
	require.NotContains(t, ex.Sql, "TopK", "test premise: order must come from the index walk, not a TopK\nplan: %s", ex.Sql)
}

func TestReverseIndexNulFamily_InSortedBothDirections(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "rev_family")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "v", Fields: []string{"-v"}}))
	for _, d := range []string{
		`{"id":1,"v":"a"}`,
		`{"id":2,"v":"a\u0000b"}`,
		`{"id":3,"v":"b"}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	const filter = `{"v":{"$in":["a","a\u0000b","b"]}}`
	hint := IndexHint{IndexName: "v", Boost: 1_000_000}

	asc := coll.Find(filter).IndexHint(hint).Sort("v")
	requireIndexOrderedPlan(t, asc, "v")
	assert.Equal(t, []int{1, 2, 3}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint).Sort("v")),
		`ascending: "a" < "a\x00b" < "b"`)

	assert.Equal(t, []int{3, 2, 1}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint).Sort("-v")),
		"descending is the exact mirror")
}

func TestReverseIndexNulFamily_LimitSelectsSortHead(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "rev_limit")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "u", Fields: []string{"-u"}, Unique: true}))
	for _, d := range []string{
		`{"id":1,"u":"u7","n":0}`,
		`{"id":2,"u":"u7\u0000z","n":0}`,
		`{"id":3,"u":"u10\u0000z","n":0}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	const filter = `{"u":{"$in":["u7","u7\u0000z","u10\u0000z"]}}`

	// Ascending u: "u10\x00z" < "u7" < "u7\x00z" → the 2-head is ids [3, 1].
	got := writeOrderIterIds(t, coll.Find(filter).Sort("u").Limit(2))
	assert.Equal(t, []int{3, 1}, got)

	// The bounded sorted write must touch exactly that head.
	res, err := coll.Find(filter).Sort("u").Limit(2).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)
	touched := writeOrderIterIds(t, coll.Find(`{"n":1}`))
	assert.ElementsMatch(t, []int{3, 1}, touched)
}

func TestReverseLeadCompound_NulFamilyOpenRange_NoDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "rev_compound")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ab", Fields: []string{"-a", "b"}}))
	for _, d := range []string{
		`{"id":1,"a":"y","b":1,"n":0}`,
		`{"id":2,"a":"y\u0000z","b":2,"n":0}`,
		`{"id":3,"a":"y\u0000z","b":3,"n":0}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	const filter = `{"a":{"$in":["y","y\u0000z"]},"b":{"$lte":5}}`
	hint := IndexHint{IndexName: "ab", Boost: 1_000_000}

	ids := writeOrderIterIds(t, coll.Find(filter).IndexHint(hint))
	assert.ElementsMatch(t, []int{1, 2, 3}, ids, "each matching doc exactly once")

	res, err := coll.Find(filter).IndexHint(hint).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 3, res.Modified)
	for _, id := range []int{1, 2, 3} {
		doc, err := coll.FindId(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, 1, doc.Value().GetInt("n"), "doc %d: $inc exactly once", id)
	}

	_, err = coll.Find(filter).IndexHint(hint).Delete(ctx)
	require.NoError(t, err, "duplicate ids in the delete set roll back the whole batch")
}

func TestReverseTailCompound_NulFamilyAfterEqualityPrefix(t *testing.T) {
	// The shape unmasked by the equality-prefix ExactSort fix: {c,-a} now
	// plans an index-ordered scan instead of falling back to FullScan+Sort.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "rev_tail")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ca", Fields: []string{"c", "-a"}}))
	for _, d := range []string{
		`{"id":1,"c":1,"a":"y"}`,
		`{"id":2,"c":1,"a":"y\u0000z"}`,
		`{"id":3,"c":2,"a":"y"}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	const filter = `{"c":1,"a":{"$in":["y","y\u0000z"]}}`
	hint := IndexHint{IndexName: "ca", Boost: 1_000_000}

	assert.ElementsMatch(t, []int{1, 2}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint)))
	assert.Equal(t, []int{2, 1}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint).Sort("-a")),
		`descending a: "y\x00z" > "y"`)
	assert.Equal(t, []int{2}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint).Sort("-a").Limit(1)))
}

func TestReverseIndexGuards_NulFreeAndNumericUnaffected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "rev_guards")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "v", Fields: []string{"-v"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "m", Fields: []string{"-m"}}))
	for _, d := range []string{
		`{"id":1,"v":"a","m":1}`,
		`{"id":2,"v":"b","m":2}`,
		`{"id":3,"v":"c","m":3}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	assert.Equal(t, []int{1, 2, 3},
		writeOrderIterIds(t, coll.Find(`{"v":{"$in":["a","b","c"]}}`).IndexHint(IndexHint{IndexName: "v", Boost: 1 << 20}).Sort("v")))
	assert.Equal(t, []int{3, 2, 1},
		writeOrderIterIds(t, coll.Find(`{"m":{"$in":[1,2,3]}}`).IndexHint(IndexHint{IndexName: "m", Boost: 1 << 20}).Sort("-m")))
}
