package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A Limit/Offset window on a write verb must select the documents in the
// query's sort order — the exact set the equivalent Iter yields — not in
// whatever order the chosen plan happens to scan.

func writeOrderColl(t *testing.T, n int) Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "write_order")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "seq", Fields: []string{"seq"}}))
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

func writeOrderIterIds(t *testing.T, q Query) []int {
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

func writeOrderSurvivors(t *testing.T, coll Collection) map[int]struct{} {
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
