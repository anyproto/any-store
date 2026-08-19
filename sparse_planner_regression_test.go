package anystore

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// TestSparseCompoundIndex_PlannerDoesNotDropRows reproduces the alpha.13
// regression where the CBO would pick a sparse compound index for a query that
// does not constrain every indexed field. A sparse index omits documents missing
// (or null in) any of its fields, so seeking it silently dropped matching rows.
//
// Layout mirrors the field report: a complete index (sp,q) is declared first and
// a sparse index (sp,as) — with `as` optional — second. The planner must not
// answer either query through the sparse index.
func TestSparseCompoundIndex_PlannerDoesNotDropRows(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "sp_q", Fields: []string{"sp", "q"}}))
	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "sp_as", Fields: []string{"sp", "as"}, Sparse: true}))

	// Two docs with sp="s": one carries `as`, one does not. The sparse (sp,as)
	// index therefore holds only one of them.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"sp":"s","q":1,"as":7}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"sp":"s","q":2}`)))

	assertSparseQueryCount(t, coll,`{"sp":"s"}`, 2)
	assertSparseQueryCount(t, coll,`{"sp":"s","as":{"$exists":false}}`, 1)
	// Sanity: the queries the sparse index CAN answer still work.
	assertSparseQueryCount(t, coll,`{"sp":"s","as":7}`, 1)
	assertSparseQueryCount(t, coll,`{"sp":"s","as":{"$exists":true}}`, 1)
}

// TestSparseSingleFieldIndex_ExistsFalse covers the single-field sparse case:
// {a:{$exists:false}} matches docs that the sparse index on `a` never stored.
func TestSparseSingleFieldIndex_ExistsFalse(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx,
		IndexInfo{Name: "a_sparse", Fields: []string{"a"}, Sparse: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3}`)))

	assertSparseQueryCount(t, coll,`{"a":{"$exists":false}}`, 2)
	assertSparseQueryCount(t, coll,`{"a":1}`, 1)
}

func assertSparseQueryCount(t *testing.T, coll Collection, cond string, want int) {
	t.Helper()
	got, err := coll.Find(cond).Count(ctx)
	require.NoError(t, err)
	require.Equalf(t, want, got, "Count for %s", cond)

	// Iter must agree with Count — the bug dropped rows from both paths.
	iter, err := coll.Find(cond).Iter(ctx)
	require.NoError(t, err)
	n := 0
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	require.Equalf(t, want, n, "Iter for %s", cond)
}
