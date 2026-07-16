package anystore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// The unique-index point-lookup chain (CoverLookup) has no FetchIter, so the
// residual FilterIter is the only stage that parses the document. A stale
// per-query doc cache there made the filter evaluate the PREVIOUS row's
// document from the second bound on — accepting rows the filter excludes and
// handing stale docs to Sort/Doc(). These tests pin the chain via Explain so
// a planner change routing them elsewhere fails loudly instead of silently
// passing.

func coverLookupColl(t *testing.T) Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "cover_filter")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "email", Fields: []string{"email"}, Unique: true}))
	for i := 1; i <= 6; i++ {
		// active alternates: e1,e3,e5 active; e2,e4,e6 inactive
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"email":"e%d","active":%t,"rank":%d}`, i, i, i%2 == 1, 7-i))))
	}
	return coll
}

func requireCoverLookup(t *testing.T, coll Collection, q Query) {
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
