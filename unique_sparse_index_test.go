/*
Index/Planner tests inspired by SQLite: index3.test, index4.test

Test scenario:
Tests unique index constraints (compound, self-update, upsert, delete+reinsert,
bulk partial failure), sparse index behavior (missing/null fields, field
appearance via update, compound sparse), sparse+unique combinations,
index length tracking through mixed mutations, nested field unique indexes,
and drop-index followed by duplicate insert.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestIndex_UniqueSparse_CompoundUnique(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Unique: true}))

	// Same a, different b — should succeed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":1,"b":2}`)))
	// Different a, same b — should succeed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":2,"b":1}`)))
	// Same a+b combo — should fail
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":1,"b":1}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	assertCollCount(t, coll, 3)
	assertIndexLen(t, coll.GetIndexes()[0], 3)
}

func TestIndex_UniqueSparse_UpdateToNewValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":2}`)))

	// Update to a completely new unique value — should succeed
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":99}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 2)

	// Verify the old value is gone and new value is queryable
	count, err := coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UniqueSparse_UpdateSameValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":10}`)))

	// Update same doc keeping same unique value but changing other field — no self-conflict
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":20}`)))

	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":1,"b":20}`, doc.Value().String())
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

func TestIndex_UniqueSparse_UpsertDuplicate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":20}`)))

	// Upsert existing doc with same id — should update (not conflict)
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":30}`)))
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":30}`, doc.Value().String())
	assertIndexLen(t, coll.GetIndexes()[0], 2)

	// Upsert new doc with value that conflicts with existing doc — should fail
	err = coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":3,"a":20}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assertCollCount(t, coll, 2)
}

func TestIndex_UniqueSparse_SparseQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Mix of docs with and without the field
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"b":20}`),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
		anyenc.MustParseJson(`{"id":4,"c":40}`),
		anyenc.MustParseJson(`{"id":5,"a":50}`),
	))

	// Only 3 docs have field "a"
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	// Query for a specific value — should use index
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Total collection count is 5
	assertCollCount(t, coll, 5)
}

func TestIndex_UniqueSparse_SparseUpdateFieldAppears(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert doc without field "a"
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":5}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Update to add field "a" — index should grow
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10,"b":5}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 1)

	// Update to remove field "a" — index should shrink
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"b":5}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

func TestIndex_UniqueSparse_SparseUniqueNoConflict(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true, Unique: true}))

	// Multiple docs without field "a" — all succeed (sparse skips them)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3}`)))

	// Doc with field "a" — success
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":10}`)))
	// Another doc with same "a" value — should fail unique constraint
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":5,"a":10}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Different "a" value — success
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":6,"a":20}`)))

	assertCollCount(t, coll, 5) // 3 without field + 2 with field
	assertIndexLen(t, coll.GetIndexes()[0], 2)
}

func TestIndex_UniqueSparse_UniqueNestedField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.email"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"meta":{"email":"a@b.com"}}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"meta":{"email":"c@d.com"}}`)))

	// Duplicate nested value — should fail
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"meta":{"email":"a@b.com"}}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	assertCollCount(t, coll, 2)
	assertIndexLen(t, coll.GetIndexes()[0], 2)
}

func TestIndex_UniqueSparse_BulkInsertPartialDuplicate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	// Bulk insert where one doc duplicates an existing value
	err = coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":2,"a":2}`),
		anyenc.MustParseJson(`{"id":3,"a":1}`), // duplicate of id:1
		anyenc.MustParseJson(`{"id":4,"a":4}`),
	)
	require.Error(t, err)

	// The whole batch should be rejected (transactional)
	assertCollCount(t, coll, 1)
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

func TestIndex_UniqueSparse_IndexLenMixedMutations(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	idx := coll.GetIndexes()[0]

	// Insert 5 docs
	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}
	assertIndexLen(t, idx, 5)

	// Delete 2 docs
	require.NoError(t, coll.DeleteId(ctx, 1))
	require.NoError(t, coll.DeleteId(ctx, 3))
	assertIndexLen(t, idx, 3)

	// Update 1 doc (change a value)
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":2,"a":99}`)))
	assertIndexLen(t, idx, 3)

	// Insert 3 more
	for i := 6; i <= 8; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}
	assertIndexLen(t, idx, 6)

	// Delete all remaining
	for _, id := range []int{2, 4, 5, 6, 7, 8} {
		require.NoError(t, coll.DeleteId(ctx, id))
	}
	assertIndexLen(t, idx, 0)
}

func TestIndex_UniqueSparse_DropUniqueThenInsertDuplicate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	// Duplicate fails while index exists
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":1}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Drop the unique index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)

	// Now the duplicate insert should succeed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":1}`)))
	assertCollCount(t, coll, 2)
}

func TestIndex_UniqueSparse_DeleteAndReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)))

	// Delete the doc
	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Reinsert with same value — should succeed (slot is free)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":42}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}

func TestIndex_UniqueSparse_CreateUniqueOnExistingDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// Insert docs with duplicate values first
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":5}`),
		anyenc.MustParseJson(`{"id":2,"a":5}`),
	))

	// Attempting to create unique index should fail
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true})
	require.Error(t, err)

	// No index should have been created
	assert.Len(t, coll.GetIndexes(), 0)

	// Data is intact
	assertCollCount(t, coll, 2)
}

func TestIndex_UniqueSparse_SparseCompoundBothMissing(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Sparse: true}))

	// Only "a" present — not indexed (b missing)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))
	// Only "b" present — not indexed (a missing)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":2}`)))
	// Both present — indexed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":1,"b":2}`)))
	// a is null — not indexed
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":null,"b":2}`)))

	assertIndexLen(t, coll.GetIndexes()[0], 1)
	assertCollCount(t, coll, 4)
}

func TestIndex_UniqueSparse_NonSparseNullIndexed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Non-sparse index indexes everything, including null/missing
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":null}`),
		anyenc.MustParseJson(`{"id":2}`),
		anyenc.MustParseJson(`{"id":3,"a":5}`),
	))

	assertIndexLen(t, coll.GetIndexes()[0], 3)
}

func TestIndex_UniqueSparse_SparseNullField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":null}`),
		anyenc.MustParseJson(`{"id":2,"a":10}`),
		anyenc.MustParseJson(`{"id":3}`),
	))

	// Only doc with a=10 is indexed
	assertIndexLen(t, coll.GetIndexes()[0], 1)
}
