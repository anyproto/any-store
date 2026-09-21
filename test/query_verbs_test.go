package test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A Limit/Offset window on a write verb must select the documents in the
// query's sort order — the exact set the equivalent Iter yields — not in
// whatever order the chosen plan happens to scan.

func writeOrderColl(t *testing.T, n int) anystore.Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "write_order")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "seq", Fields: []string{"seq"}}))
	// seq is a shuffled permutation of 1..n (i*7 mod 11, n=10) so that NEITHER
	// sort direction coincides with any plan's natural scan order (pk or
	// insertion) — every case below is order-sensitive.
	require.Equal(t, 10, n, "seq permutation below assumes n=10")
	for i := 1; i <= n; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"seq":%d,"n":0}`, i, i*7%11))))
	}
	return coll
}

func writeOrderIterIds(t *testing.T, q anystore.Query) []int {
	it, err := q.Iter(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	var ids []int
	for it.Next() {
		doc, err := it.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().GetInt("id"))
	}
	require.NoError(t, it.Err())
	return ids
}

func writeOrderSurvivors(t *testing.T, coll anystore.Collection) map[int]struct{} {
	ids := map[int]struct{}{}
	for _, id := range writeOrderIterIds(t, coll.Find(nil)) {
		ids[id] = struct{}{}
	}
	return ids
}

func TestCollQuery_SortLimitDelete_RemovesSameDocsAsIter(t *testing.T) {
	// Retention shape: "delete the 3 oldest". The docs Iter selects under the
	// same sort+limit are the only ones Delete may remove.
	coll := writeOrderColl(t, 10)

	wantGone := writeOrderIterIds(t, coll.Find(nil).Sort("seq").Limit(3))
	require.Equal(t, []int{8, 5, 2}, wantGone)

	res, err := coll.Find(nil).Sort("seq").Limit(3).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Modified)

	survivors := writeOrderSurvivors(t, coll)
	for _, id := range wantGone {
		assert.NotContains(t, survivors, id, "doc %d should have been deleted", id)
	}
	assert.Len(t, survivors, 7)
}

func TestCollQuery_SortDescLimitDelete_RemovesNewest(t *testing.T) {
	coll := writeOrderColl(t, 10)

	res, err := coll.Find(nil).Sort("-seq").Limit(2).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)

	// -seq: highest seq values are 10 (id 3) and 9 (id 6).
	survivors := writeOrderSurvivors(t, coll)
	assert.NotContains(t, survivors, 3)
	assert.NotContains(t, survivors, 6)
	assert.Contains(t, survivors, 8)
}

func TestCollQuery_SortLimitUpdate_ModifiesSameDocsAsIter(t *testing.T) {
	coll := writeOrderColl(t, 10)

	wantTouched := writeOrderIterIds(t, coll.Find(nil).Sort("-seq").Limit(3))
	require.Equal(t, []int{3, 6, 9}, wantTouched)

	res, err := coll.Find(nil).Sort("-seq").Limit(3).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 3, res.Modified)

	touched := writeOrderIterIds(t, coll.Find(`{"n":1}`))
	assert.ElementsMatch(t, wantTouched, touched)
}

func TestCollQuery_SortOffsetDelete_SkipsSameDocsAsIter(t *testing.T) {
	coll := writeOrderColl(t, 10)

	wantGone := writeOrderIterIds(t, coll.Find(nil).Sort("seq").Offset(7))
	require.Equal(t, []int{9, 6, 3}, wantGone)

	res, err := coll.Find(nil).Sort("seq").Offset(7).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Modified)

	survivors := writeOrderSurvivors(t, coll)
	for _, id := range wantGone {
		assert.NotContains(t, survivors, id)
	}
	assert.Len(t, survivors, 7)
}

func TestCollQuery_UnsortedLimitWrite_CardinalityOnly(t *testing.T) {
	// Guard against over-reach: with no sort, a bounded write promises only
	// HOW MANY documents it touches, not which ones.
	coll := writeOrderColl(t, 10)

	res, err := coll.Find(nil).Limit(4).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified)
	assert.Len(t, writeOrderSurvivors(t, coll), 6)
}

func TestUnboundedSortedWrite_TouchesFullMatchedSet(t *testing.T) {
	// With no Limit/Offset the selected set is order-invariant, so the write
	// plans WITHOUT the sorter (no materialize-and-sort inside the write tx)
	// and must still touch every matching document exactly once.
	coll := writeOrderColl(t, 10)

	res, err := coll.Find(`{"seq":{"$gte":4}}`).Sort("-seq").
		Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 7, res.Modified, "seq 4..10 = 7 docs")

	touched := writeOrderIterIds(t, coll.Find(`{"n":1}`))
	assert.Len(t, touched, 7)

	res, err = coll.Find(nil).Sort("seq").Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Modified)
	assert.Empty(t, writeOrderSurvivors(t, coll))
}

// Compound-tuple bounds can overlap in key space even when the per-field
// bounds are disjoint: anyenc strings are not prefix-free across the NUL
// escape, so with two selected values in one escape family ("u1" and
// "u1\x00z") and an OPEN range on the NEXT index field, the 0xff pad after
// the shorter tuple covers keys the longer tuple's own range also selects.
// IndexIter then emits the shared entries twice: duplicate rows on Iter,
// a double-applied modifier on Update, and ErrDocNotFound (rolling back the
// whole batch) on Delete.

func compoundBoundsColl(t *testing.T, unique bool) anystore.Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "compound_bounds")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "ub", Fields: []string{"u", "b"}, Unique: unique}))
	docs := []string{
		`{"id":1,"u":"u1","b":1,"n":0}`,
		`{"id":2,"u":"u1\u0000z","b":2,"n":0}`,
		`{"id":3,"u":"u1\u0000z","b":3,"n":0}`,
		`{"id":4,"u":"u2","b":4,"n":0}`,
	}
	for _, d := range docs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	return coll
}

const compoundBoundsFilter = `{"u":{"$in":["u1","u1\u0000z"]},"b":{"$gt":-1}}`

var compoundBoundsHint = anystore.IndexHint{IndexName: "ub", Boost: 1_000_000}

func requireCompoundIndexPlan(t *testing.T, q anystore.Query) {
	t.Helper()
	ex, err := q.Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, ex.Sql, "ub", "test premise: the compound index must drive the plan\nplan: %s", ex.Sql)
	require.NotContains(t, ex.Sql, "FullScan", "test premise: not a FullScan\nplan: %s", ex.Sql)
}

func TestCompoundBounds_NulFamilyOpenRange_NoDuplicateRows(t *testing.T) {
	coll := compoundBoundsColl(t, false)
	q := coll.Find(compoundBoundsFilter).IndexHint(compoundBoundsHint)
	requireCompoundIndexPlan(t, q)

	ids := writeOrderIterIds(t, coll.Find(compoundBoundsFilter).IndexHint(compoundBoundsHint))
	assert.ElementsMatch(t, []int{1, 2, 3}, ids, "each matching doc exactly once")

	count, err := coll.Find(compoundBoundsFilter).IndexHint(compoundBoundsHint).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestCompoundBounds_NulFamilyOpenRange_UpdateAppliesOnce(t *testing.T) {
	coll := compoundBoundsColl(t, false)
	res, err := coll.Find(compoundBoundsFilter).IndexHint(compoundBoundsHint).
		Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 3, res.Modified)

	for _, id := range []int{1, 2, 3} {
		doc, err := coll.FindId(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, 1, doc.Value().GetInt("n"), "doc %d: $inc must apply exactly once", id)
	}
}

func TestCompoundBounds_NulFamilyOpenRange_DeleteSucceeds(t *testing.T) {
	coll := compoundBoundsColl(t, false)
	res, err := coll.Find(compoundBoundsFilter).IndexHint(compoundBoundsHint).Delete(ctx)
	require.NoError(t, err, "a duplicate id in the delete set fails the second DeleteId and rolls back the batch")
	assert.Equal(t, 3, res.Modified)
	assert.Equal(t, map[int]struct{}{4: {}}, writeOrderSurvivors(t, coll))
}

func TestCompoundBounds_UniqueCompoundOverlap(t *testing.T) {
	coll := compoundBoundsColl(t, true)
	q := coll.Find(compoundBoundsFilter).IndexHint(compoundBoundsHint)
	requireCompoundIndexPlan(t, q)

	ids := writeOrderIterIds(t, coll.Find(compoundBoundsFilter).IndexHint(compoundBoundsHint))
	assert.ElementsMatch(t, []int{1, 2, 3}, ids)
}

func TestCompoundBounds_NegativeControls(t *testing.T) {
	// Shapes the trigger rule excludes: the bounds diverge at a type tag and
	// stay disjoint, so behavior must be identical before and after the merge.
	coll := compoundBoundsColl(t, false)
	for name, filter := range map[string]string{
		"two_sided_range_on_next": `{"u":{"$in":["u1","u1\u0000z"]},"b":{"$gt":-1,"$lt":100}}`,
		"equality_on_next":        `{"u":{"$in":["u1\u0000z"]},"b":2}`,
		"nul_free_family":         `{"u":{"$in":["u1","u2"]},"b":{"$gt":-1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			ids := writeOrderIterIds(t, coll.Find(filter).IndexHint(compoundBoundsHint))
			count, err := coll.Find(filter).IndexHint(compoundBoundsHint).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(ids), count, "Count and Iter agree")
			seen := map[int]bool{}
			for _, id := range ids {
				assert.False(t, seen[id], "no duplicates")
				seen[id] = true
			}
		})
	}
}

// The unique-index point-lookup chain (CoverLookup) has no FetchIter, so the
// residual FilterIter is the only stage that parses the document. A stale
// per-query doc cache there made the filter evaluate the PREVIOUS row's
// document from the second bound on — accepting rows the filter excludes and
// handing stale docs to Sort/Doc(). These tests pin the chain via Explain so
// a planner change routing them elsewhere fails loudly instead of silently
// passing.

func coverLookupColl(t *testing.T) anystore.Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cover_filter")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "email", Fields: []string{"email"}, Unique: true}))
	for i := 1; i <= 6; i++ {
		// active alternates: e1,e3,e5 active; e2,e4,e6 inactive
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"email":"e%d","active":%t,"rank":%d}`, i, i, i%2 == 1, 7-i))))
	}
	return coll
}

func requireCoverLookup(t *testing.T, coll anystore.Collection, q anystore.Query) {
	t.Helper()
	ex, err := q.Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, ex.Sql, "CoverLookup", "test premise: plan must route through CoverLookup\nplan: %s", ex.Sql)
}

const coverFilter = `{"email":{"$in":["e1","e2","e3","e4"]},"active":true}`

func TestCoverLookupResidualFilter_AppliedToEveryRow(t *testing.T) {
	coll := coverLookupColl(t)
	requireCoverLookup(t, coll, coll.Find(coverFilter))

	// Matching docs: e1, e3 (active). e2/e4 must be rejected by the residual.
	ids := writeOrderIterIds(t, coll.Find(coverFilter))
	assert.ElementsMatch(t, []int{1, 3}, ids)

	count, err := coll.Find(coverFilter).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestCoverLookupResidualFilter_DeleteRemovesOnlyMatching(t *testing.T) {
	coll := coverLookupColl(t)
	requireCoverLookup(t, coll, coll.Find(coverFilter))

	res, err := coll.Find(coverFilter).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)

	survivors := writeOrderSurvivors(t, coll)
	assert.Contains(t, survivors, 2, "inactive doc must survive a filtered delete")
	assert.Contains(t, survivors, 4, "inactive doc must survive a filtered delete")
	assert.NotContains(t, survivors, 1)
	assert.NotContains(t, survivors, 3)
}

func TestCoverLookupSort_OrdersByDocumentNotStaleCache(t *testing.T) {
	coll := coverLookupColl(t)
	q := coll.Find(`{"email":{"$in":["e1","e3","e5"]}}`).Sort("rank")
	requireCoverLookup(t, coll, q)

	// rank: e1→6, e3→4, e5→2; ascending rank → ids [5, 3, 1].
	ids := writeOrderIterIds(t, coll.Find(`{"email":{"$in":["e1","e3","e5"]}}`).Sort("rank"))
	assert.Equal(t, []int{5, 3, 1}, ids)
}

func TestCoverLookupSingleBound_Unaffected(t *testing.T) {
	// Control: one bound means no previous row to leak from.
	coll := coverLookupColl(t)
	ids := writeOrderIterIds(t, coll.Find(`{"email":"e2","active":true}`))
	assert.Empty(t, ids)
	ids = writeOrderIterIds(t, coll.Find(`{"email":"e1","active":true}`))
	assert.Equal(t, []int{1}, ids)
}

func TestCoverLookupIterDoc_ReturnsOwnDocument(t *testing.T) {
	coll := coverLookupColl(t)
	it, err := coll.Find(`{"email":{"$in":["e1","e3"]}}`).Iter(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()
	var emails []string
	for it.Next() {
		doc, err := it.Doc()
		require.NoError(t, err)
		emails = append(emails, doc.Value().GetString("email"))
	}
	require.NoError(t, it.Err())
	assert.True(t, strings.HasPrefix(emails[0], "e"), "sanity")
	assert.ElementsMatch(t, []string{"e1", "e3"}, emails)
}

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

func requireIndexOrderedPlan(t *testing.T, q anystore.Query, indexName string) {
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "v", Fields: []string{"-v"}}))
	for _, d := range []string{
		`{"id":1,"v":"a"}`,
		`{"id":2,"v":"a\u0000b"}`,
		`{"id":3,"v":"b"}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	const filter = `{"v":{"$in":["a","a\u0000b","b"]}}`
	hint := anystore.IndexHint{IndexName: "v", Boost: 1_000_000}

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "u", Fields: []string{"-u"}, Unique: true}))
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "ab", Fields: []string{"-a", "b"}}))
	for _, d := range []string{
		`{"id":1,"a":"y","b":1,"n":0}`,
		`{"id":2,"a":"y\u0000z","b":2,"n":0}`,
		`{"id":3,"a":"y\u0000z","b":3,"n":0}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	const filter = `{"a":{"$in":["y","y\u0000z"]},"b":{"$lte":5}}`
	hint := anystore.IndexHint{IndexName: "ab", Boost: 1_000_000}

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "ca", Fields: []string{"c", "-a"}}))
	for _, d := range []string{
		`{"id":1,"c":1,"a":"y"}`,
		`{"id":2,"c":1,"a":"y\u0000z"}`,
		`{"id":3,"c":2,"a":"y"}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	const filter = `{"c":1,"a":{"$in":["y","y\u0000z"]}}`
	hint := anystore.IndexHint{IndexName: "ca", Boost: 1_000_000}

	assert.ElementsMatch(t, []int{1, 2}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint)))
	assert.Equal(t, []int{2, 1}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint).Sort("-a")),
		`descending a: "y\x00z" > "y"`)
	assert.Equal(t, []int{2}, writeOrderIterIds(t, coll.Find(filter).IndexHint(hint).Sort("-a").Limit(1)))
}

func TestReverseIndexGuards_NulFreeAndNumericUnaffected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "rev_guards")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "v", Fields: []string{"-v"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "m", Fields: []string{"-m"}}))
	for _, d := range []string{
		`{"id":1,"v":"a","m":1}`,
		`{"id":2,"v":"b","m":2}`,
		`{"id":3,"v":"c","m":3}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}
	assert.Equal(t, []int{1, 2, 3},
		writeOrderIterIds(t, coll.Find(`{"v":{"$in":["a","b","c"]}}`).IndexHint(anystore.IndexHint{IndexName: "v", Boost: 1 << 20}).Sort("v")))
	assert.Equal(t, []int{3, 2, 1},
		writeOrderIterIds(t, coll.Find(`{"m":{"$in":[1,2,3]}}`).IndexHint(anystore.IndexHint{IndexName: "m", Boost: 1 << 20}).Sort("-m")))
}

// End-to-end coverage for index bounds whose endpoints are mid-value PREFIXES
// of the anyenc encoding — $type's bare type tag, $regex's tag + string
// prefix — on DESCENDING indexes. Bitwise key inversion preserves the
// prefix-extension relation instead of reversing it, so such a bound survives
// only via the prefix-successor transform (qplanner.transformReverseBounds).
// Without it, every row whose first inverted continuation byte is 0xFF — a
// leading 0x00 payload byte: a 1970s ObjectID timestamp, a float <= -~9e307,
// the immediate EOS of an empty string, or a stored string equal to a $regex
// prefix (its EOS inverts to 0xFF) — silently fell outside the 0xFF-padded
// End on Count, Iter, Delete and Update, with err == nil.
//
// The ascending runs pin the forward behavior (exclusive next-tag End, see
// commit 192c239) that the reverse transform must not regress.

func trpbInsert(t *testing.T, coll anystore.Collection, id int, set func(a *anyenc.Arena, d *anyenc.Value)) {
	t.Helper()
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("id", a.NewNumberInt(id))
	set(a, doc)
	require.NoError(t, coll.Insert(ctx, doc))
}

func trpbFindIds(t *testing.T, ctx context.Context, coll anystore.Collection, hint anystore.IndexHint, cond string) []int {
	t.Helper()
	it, err := coll.Find(cond).IndexHint(hint).Iter(ctx)
	require.NoError(t, err)
	var got []int
	for it.Next() {
		d, derr := it.Doc()
		require.NoError(t, derr)
		got = append(got, d.Value().GetInt("id"))
	}
	require.NoError(t, it.Close())
	slices.Sort(got)

	cnt, err := coll.Find(cond).IndexHint(hint).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, len(got), cnt, "Count vs Iter divergence for %s", cond)
	return got
}

func TestReversePrefixBounds(t *testing.T) {
	zeroOid, err := anyenc.ObjectIDFromHex("000000000000000000000000")
	require.NoError(t, err)
	liveOid := anyenc.NewObjectID()

	for _, field := range []string{"v", "-v"} {
		for _, unique := range []bool{false, true} {
			name := field
			if unique {
				name += " unique"
			}
			newColl := func(t *testing.T) (anystore.Collection, anystore.IndexHint) {
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "test")
				require.NoError(t, err)
				require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{field}, Unique: unique}))
				return coll, anystore.IndexHint{IndexName: strings.TrimPrefix(field, "-"), Boost: 100}
			}

			t.Run("type objectId "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				trpbInsert(t, coll, 1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewObjectID(zeroOid)) })
				trpbInsert(t, coll, 2, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewObjectID(liveOid)) })
				trpbInsert(t, coll, 3, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewNumberInt(7)) })

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$type":"objectId"}}`)
				assert.Equal(t, []int{1, 2}, got)

				res, err := coll.Find(`{"v":{"$type":"objectId"}}`).IndexHint(hint).Delete(ctx)
				require.NoError(t, err)
				assert.Equal(t, 2, res.Modified)
				cnt, err := coll.Find(nil).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, 1, cnt)
			})

			t.Run("type number "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				nums := []float64{-math.MaxFloat64, -1e308, -1, 0, 1, math.MaxFloat64}
				for i, n := range nums {
					n := n
					trpbInsert(t, coll, i+1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewNumberFloat64(n)) })
				}
				trpbInsert(t, coll, 100, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString("s")) })

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$type":"number"}}`)
				assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, got)
			})

			t.Run("type string empty "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				trpbInsert(t, coll, 1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString("")) })
				trpbInsert(t, coll, 2, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString("x")) })
				trpbInsert(t, coll, 3, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewNumberInt(7)) })

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$type":"string"}}`)
				assert.Equal(t, []int{1, 2}, got)
			})

			t.Run("regex prefix "+name, func(t *testing.T) {
				coll, hint := newColl(t)
				// "foo" is one boundary row: its key continues with the
				// inverted EOS (0xFF) right after the prefix bytes on a
				// descending index. "foo\xff\xffz" is the other: a raw 0xFF
				// payload run right after the prefix (legal — only NUL is
				// escaped), which the old inclusive prefix+0xFF ascending
				// End also dropped.
				words := []string{"foo", "foobar", "fop", "fo", "other", "foo\xff\xffz"}
				for i, w := range words {
					w := w
					trpbInsert(t, coll, i+1, func(a *anyenc.Arena, d *anyenc.Value) { d.Set("v", a.NewString(w)) })
				}

				got := trpbFindIds(t, ctx, coll, hint, `{"v":{"$regex":"^foo"}}`)
				assert.Equal(t, []int{1, 2, 6}, got)
			})
		}
	}
}

// A $knn clause must denote the same document set on every verb. Before the
// shared compiler, only Iter ran vector detection: a valid clause was a
// silent no-op on Update/Delete (matched nothing as a literal filter), and an
// invalid one degraded to a scalar comparison. Now the write verbs compile
// through the same seam as Iter, and $k bounds the blast radius of a write to
// a number the caller typed.

func vectorVerbColl(t *testing.T) anystore.Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "vec_verbs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "v", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: 3, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	for i := 0; i < 20; i++ {
		doc := fmt.Sprintf(`{"id":%d,"v":[%d,1,2],"n":0}`, i, i)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
	}
	return coll
}

func vectorVerbClause(k int) string {
	return fmt.Sprintf(`{"v":{"$knn":{"$query":[3,1,2],"$k":%d}}}`, k)
}

func TestVectorClauseDelete_RemovesExactlyIterSelection(t *testing.T) {
	coll := vectorVerbColl(t)

	want := writeOrderIterIds(t, coll.Find(vectorVerbClause(5)))
	require.Len(t, want, 5, "test premise: $k bounds the denoted set")

	res, err := coll.Find(vectorVerbClause(5)).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, res.Modified, "a valid $knn clause must not be a silent no-op on Delete")

	survivors := writeOrderSurvivors(t, coll)
	for _, id := range want {
		assert.NotContains(t, survivors, id, "doc %d was in Iter's selection and must be deleted", id)
	}
	assert.Len(t, survivors, 15)
}

func TestVectorClauseUpdate_ModifiesExactlyIterSelection(t *testing.T) {
	coll := vectorVerbColl(t)

	want := writeOrderIterIds(t, coll.Find(vectorVerbClause(4)))
	require.Len(t, want, 4)

	res, err := coll.Find(vectorVerbClause(4)).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified, "a valid $knn clause must not be a silent no-op on Update")

	touched := writeOrderIterIds(t, coll.Find(`{"n":1}`))
	assert.ElementsMatch(t, want, touched)
}

func TestVectorClauseInvalid_WriteVerbsAgreeWithIter(t *testing.T) {
	// Whatever Iter decides about a clause on the vector field — reject it or
	// treat it as an ordinary (never-matching) filter — the write verbs must
	// decide identically, and the collection must stay intact either way.
	// Note on `{"v":{"$gt":…}}`: these docs store v as a PLAIN array, so an
	// ordering op compares element-wise (array semantics) — a deliberately
	// ordinary filter now, not an ANN shape. $gt:5000 exceeds every element,
	// so it legally matches nothing; Rule V (vector-vs-scalar) applies only to
	// packed TypeVectorF32 values and is pinned in query/filter_vector_test.go.
	coll := vectorVerbColl(t)
	for _, cond := range []string{
		`{"v":{"$gt":5000}}`,                     // ordering op on the vector field: ordinary filter, matches nothing
		`{"v":[1,2]}`,                            // wrong dim: not the ANN shape — ordinary filter
		`{"v":[3,1,2]}`,                          // dim-sized bare array: the legacy ANN spelling — hard error
		`{"v":{"$knn":{"$query":[1,2],"$k":5}}}`, // wrong-dim $query — hard error
	} {
		iterOK := true
		if it, ierr := coll.Find(cond).Iter(ctx); ierr != nil {
			iterOK = false
		} else {
			require.NoError(t, it.Close())
		}

		res, derr := coll.Find(cond).Delete(ctx)
		assert.Equal(t, iterOK, derr == nil,
			"cond=%s: Delete's accept/reject decision must match Iter's (iterOK=%v, deleteErr=%v)", cond, iterOK, derr)
		if derr == nil {
			assert.Zero(t, res.Modified, "cond=%s matches no documents", cond)
		}

		_, cerr := coll.Find(cond).Count(ctx)
		assert.Equal(t, iterOK, cerr == nil,
			"cond=%s: Count's accept/reject decision must match Iter's", cond)

		remaining, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 20, remaining, "cond=%s: collection must be intact", cond)
	}
}

func TestVectorClauseCount_MatchesIter(t *testing.T) {
	coll := vectorVerbColl(t)
	for name, q := range map[string]func() anystore.Query{
		"plain":  func() anystore.Query { return coll.Find(vectorVerbClause(7)) },
		"limit":  func() anystore.Query { return coll.Find(vectorVerbClause(7)).Limit(5) },
		"offset": func() anystore.Query { return coll.Find(vectorVerbClause(7)).Offset(3).Limit(3) },
	} {
		t.Run(name, func(t *testing.T) {
			ids := writeOrderIterIds(t, q())
			count, err := q().Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(ids), count, "Count and Iter must denote the same set")
		})
	}
}

func TestVectorClauseUnboundedWrite_BoundedByK(t *testing.T) {
	// $k is the blast radius: an Update/Delete with no .Limit() mutates exactly
	// the k nearest — never "however many candidates the search yields". The
	// old ErrVectorWriteWithoutLimit guard is retired by construction: the
	// unbounded-delete query it rejected is unrepresentable under $knn.
	coll := vectorVerbColl(t)

	res, err := coll.Find(vectorVerbClause(5)).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, res.Modified, "Delete removes exactly $k documents")

	remaining, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 15, remaining)

	res, err = coll.Find(vectorVerbClause(4)).Update(ctx, anyenc.MustParseJson(`{"$inc":{"n":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified, "Update visits exactly $k documents")
}

// TestVerbCoherence_Knn pins the central $knn invariant: for a fixed query Q on
// a fixed snapshot, every verb denotes the same document sequence Rows(Q) —
// Count(Q) == len(Rows(Q)), Delete(Q) removes exactly set(Rows(Q)), Update(Q)
// visits exactly that set, Explain(Q) describes the ANN plan, and two identical
// runs return identical output (source-level determinism).
//
// Matrix: all five index modes × {no residual, selective residual, _distance
// residual} × {no sort, real-field sort, _distance sort} × {no paging,
// Limit(3), Offset(2).Limit(3)}. The _distance residual on the non-Iter verbs
// is the axis that catches the injectDistance gating trap: if injection were
// gated on the sidecar, Comp.Ok(nil) would be TRUE for $lt against a missing
// _distance and a bounded Delete would remove all k instead of the thresholded
// subset.
func TestVerbCoherence_Knn(t *testing.T) {
	const (
		dim = 8
		n   = 400
		k   = 6
	)

	// v = [i/10, fixed…] → L2 distance to query [q0, fixed…] is |i-q|/10:
	// distinct per doc, exact under PQ re-rank, and the _distance threshold
	// below has a predictable membership. "r" is a sort field uncorrelated
	// with id; "a" is a ~50% selective residual field.
	docJSON := func(i int) string {
		return fmt.Sprintf(`{"id":%d,"v":[%g,1,2,3,4,5,6,7],"a":%d,"r":%d}`,
			i, float32(i)/10, i%2, (i*7919)%n)
	}
	queryVec := `[20.05,1,2,3,4,5,6,7]` // between docs 200 and 201: no exact ties, no self-doc

	modes := []struct {
		name string
		mode anystore.VectorMode
	}{
		{"btree", anystore.VectorModeBTree},
		{"hybrid", anystore.VectorModeHybrid},
		{"bruteforce", anystore.VectorModeBruteForce},
		{"ivfpq", anystore.VectorModeIVFPQ},
		{"ivfsq", anystore.VectorModeIVFSQ},
	}
	residuals := []struct {
		name string
		cond string // appended after the $knn clause
	}{
		{"none", ""},
		{"selective", `,"a":1`},
		// |i-200.5|/10 < 0.35 → docs 198..203 (6 docs) are within the
		// threshold BEFORE the k-cut; survivors depend on the ANN ranking.
		{"distance", `,"_distance":{"$lt":0.35}`},
	}
	sorts := []struct {
		name string
		sort []any
	}{
		{"none", nil},
		{"real-field", []any{"-r"}},
		{"distance", []any{"_distance"}},
	}
	pagings := []struct {
		name          string
		limit, offset uint
	}{
		{"all", 0, 0},
		{"limit3", 3, 0},
		{"offset2limit3", 3, 2},
	}

	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "vc_"+m.name)
			require.NoError(t, err)
			// Docs first: IVF trains its codebooks from existing documents.
			docs := make(map[int]string, n)
			for i := 0; i < n; i++ {
				docs[i] = docJSON(i)
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docs[i])))
			}
			require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
				Name: "emb", Kind: anystore.IndexKindVector,
				Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64, Mode: m.mode},
			}))

			if m.mode == anystore.VectorModeHybrid {
				// Warm the RAM l0 mirror and dirty its overlay: read verbs
				// (Iter/Count) traverse the mirror while write verbs
				// (Delete/Update) traverse raw btree adjacency inside the
				// write tx — the coherence below asserts the two are
				// candidate-identical, an equivalence nothing exercised while
				// the write verbs never reached the vector path at all.
				for w := 0; w < 3; w++ {
					_, werr := coll.Find(fmt.Sprintf(`{"v":{"$knn":{"$query":%s,"$k":%d}}}`, queryVec, k)).Count(ctx)
					require.NoError(t, werr)
				}
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docJSON(n))))
				require.NoError(t, coll.DeleteId(ctx, n))
			}

			for _, res := range residuals {
				for _, srt := range sorts {
					for _, pg := range pagings {
						name := fmt.Sprintf("res=%s/sort=%s/page=%s", res.name, srt.name, pg.name)
						t.Run(name, func(t *testing.T) {
							cond := fmt.Sprintf(`{"v":{"$knn":{"$query":%s,"$k":%d}}%s}`, queryVec, k, res.cond)
							mkQ := func() anystore.Query {
								q := coll.Find(cond)
								if srt.sort != nil {
									q = q.Sort(srt.sort...)
								}
								if pg.limit > 0 {
									q = q.Limit(pg.limit)
								}
								if pg.offset > 0 {
									q = q.Offset(pg.offset)
								}
								return q
							}

							ids := writeOrderIterIds(t, mkQ())
							assert.LessOrEqual(t, len(ids), k, "the k-cut bounds every page")
							assert.Equal(t, ids, writeOrderIterIds(t, mkQ()),
								"two identical runs must return identical sequences")

							count, err := mkQ().Count(ctx)
							require.NoError(t, err)
							assert.Equal(t, len(ids), count, "Count(Q) == len(Rows(Q))")

							exp, err := mkQ().Explain(ctx)
							require.NoError(t, err)
							assert.Contains(t, exp.Sql, "KnnSearch(", "Explain describes the ANN plan: %s", exp.Sql)

							upd, err := mkQ().Update(ctx, anyenc.MustParseJson(`{"$inc":{"touched":1}}`))
							require.NoError(t, err)
							assert.Equal(t, len(ids), upd.Matched, "Update(Q) visits exactly Rows(Q)")

							before, err := coll.Count(ctx)
							require.NoError(t, err)
							del, err := mkQ().Delete(ctx)
							require.NoError(t, err)
							assert.Equal(t, len(ids), del.Modified, "Delete(Q) removes exactly len(Rows(Q))")
							after, err := coll.Count(ctx)
							require.NoError(t, err)
							assert.Equal(t, before-len(ids), after)
							for _, id := range ids {
								doc, gerr := coll.FindId(ctx, id)
								assert.Error(t, gerr, "doc %d was in Rows(Q) and must be deleted (got %v)", id, doc)
							}

							// Restore the deleted docs so the next cell sees
							// the same document set (the index graph churns,
							// which only adds realism — each cell is
							// self-consistent on its own snapshot).
							for _, id := range ids {
								require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(docs[id])))
							}
						})
					}
				}
			}
		})
	}
}
