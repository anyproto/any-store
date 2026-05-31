package anystore

/*
Audit tests for the "index-maintenance" domain (docs/qplanner/audit/actionable_by_domain.json).

  act-13  Multi-index insert: a partial index write is rolled back atomically when a
          later index's unique check fails.
  act-14  Multi-index update: cross-index rollback ordering on a trailing unique conflict.
  act-15  Failed unique update keeps BOTH old values queryable via the index (not just FindId).
  act-16  Removing a non-sparse array field collapses K+1 multikey entries to one 'null'.
  act-17  Updating a leading array field of a compound index re-fans-out the entries.
  act-18  Bulk insert: index length is the sum of per-doc keys, decoupled from doc count.
*/

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func auditIdxByName(t testing.TB, coll Collection, name string) Index {
	t.Helper()
	for _, ix := range coll.GetIndexes() {
		if ix.Info().Name == name {
			return ix
		}
	}
	t.Fatalf("index %q not found", name)
	return nil
}

// act-13
func TestIndex_Maintenance_MultiIndexInsertPartialFailureRollsBack(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10,"b":100}`)))

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":20,"b":100}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// The stray index-a entry for a=20 (written before the index-b unique check
	// failed) must be erased by the whole-tx rollback.
	assertIndexLen(t, auditIdxByName(t, coll, "a"), 1)
	assertIndexLen(t, auditIdxByName(t, coll, "b"), 1)
	assertQueryCount(t, coll.Find(`{"a":20}`), 0)
	assertQueryCount(t, coll.Find(`{"a":10}`), 1)
	assertCollCount(t, coll, 1)

	// A later legitimate insert is unaffected by the rolled-back attempt.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":20,"b":200}`)))
	assertQueryCount(t, coll.Find(`{"a":20}`), 1)
}

// act-14
func TestIndex_Maintenance_MultiIndexUpdateUniqueConflictOnTrailingIndexRollsBack(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":100}`),
		anyenc.MustParseJson(`{"id":2,"a":2,"b":200}`),
	))

	// id:1 → a=99 (index-a delete+insert) then b=200 collides with id:2.
	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":99,"b":200}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	assertQueryCount(t, coll.Find(`{"a":99}`), 0)
	assertQueryCount(t, coll.Find(`{"a":1}`), 1)
	assertIndexLen(t, auditIdxByName(t, coll, "a"), 2)
	assertIndexLen(t, auditIdxByName(t, coll, "b"), 2)

	doc1, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":1,"b":100}`, doc1.Value().String())
}

// act-15
func TestIndex_Maintenance_UniqueUpdateToExisting_IndexQueryableAfterRollback(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))

	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":20}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Both old values still reachable THROUGH the index (not just via FindId):
	// catches an incomplete rollback where delete(a=10) commits but insert rolls back.
	assertQueryCount(t, coll.Find(`{"a":10}`), 1)
	assertQueryCount(t, coll.Find(`{"a":20}`), 1)
	assertIndexLen(t, auditIdxByName(t, coll, "a"), 2)
}

// act-16
func TestIndex_Maintenance_NonSparseArrayFieldRemovedBecomesNull(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`)))
	assertIndexLen(t, auditIdxByName(t, coll, "tags"), 3) // "a","b",["a","b"]

	// UpdateOne replaces the whole doc; tags is gone → one 'null' entry (non-sparse).
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"x":1}`)))
	assertIndexLen(t, auditIdxByName(t, coll, "tags"), 1)
	assertQueryCount(t, coll.Find(`{"tags":"a"}`), 0)
	assertQueryCount(t, coll.Find(`{"tags":null}`), 1)
}

// act-17
func TestIndex_Maintenance_CompoundArrayLeadingFieldUpdate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags", "p"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b"],"p":1}`)))
	assertIndexLen(t, auditIdxByName(t, coll, "tags,p"), 3) // (a,1),(b,1),([a,b],1)

	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["c"],"p":1}`)))
	assertIndexLen(t, auditIdxByName(t, coll, "tags,p"), 2) // (c,1),([c],1)
	assertQueryCount(t, coll.Find(`{"tags":"a","p":1}`), 0)
	assertQueryCount(t, coll.Find(`{"tags":"b","p":1}`), 0)
	assertQueryCount(t, coll.Find(`{"tags":"c","p":1}`), 1)
}

// act-18
func TestIndex_Maintenance_BulkInsertMixedScalarArrayMissingCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":"x"}`),           // scalar → 1
		anyenc.MustParseJson(`{"id":2,"tags":["a","b"]}`),     // 2+1 = 3
		anyenc.MustParseJson(`{"id":3,"tags":["c","d","e"]}`), // 3+1 = 4
		anyenc.MustParseJson(`{"id":4}`),                      // missing → null → 1
	))
	assertIndexLen(t, auditIdxByName(t, coll, "tags"), 9) // 1+3+4+1, NOT 4
	assertCollCount(t, coll, 4)
	assertQueryCount(t, coll.Find(`{"tags":"a"}`), 1)
	assertQueryCount(t, coll.Find(`{"tags":"x"}`), 1)
	assertQueryCount(t, coll.Find(`{"tags":"c"}`), 1)
	assertQueryCount(t, coll.Find(`{"tags":null}`), 1)
}
