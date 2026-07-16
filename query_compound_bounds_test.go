package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Compound-tuple bounds can overlap in key space even when the per-field
// bounds are disjoint: anyenc strings are not prefix-free across the NUL
// escape, so with two selected values in one escape family ("u1" and
// "u1\x00z") and an OPEN range on the NEXT index field, the 0xff pad after
// the shorter tuple covers keys the longer tuple's own range also selects.
// IndexIter then emits the shared entries twice: duplicate rows on Iter,
// a double-applied modifier on Update, and ErrDocNotFound (rolling back the
// whole batch) on Delete.

func compoundBoundsColl(t *testing.T, unique bool) Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "compound_bounds")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ub", Fields: []string{"u", "b"}, Unique: unique}))
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

var compoundBoundsHint = IndexHint{IndexName: "ub", Boost: 1_000_000}

func requireCompoundIndexPlan(t *testing.T, q Query) {
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
