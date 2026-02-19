/*
Index/Planner tests inspired by SQLite: where.test, where9.test, where2.test, where4.test

Test scenario:
Index maintenance during insert/update/delete operations. Verifies that
indexes stay consistent through bulk inserts, query-based deletes and updates,
compound index mutations, mixed operation sequences, non-indexed field updates,
full deletion and re-insertion, and array field multi-key entry management.

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

func TestIndex_Maintenance_BulkInsertIndexLength(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))

	// Insert 100 docs where x = number of bits set in i (popcount-like via simple formula)
	// Use x = i/12 so there are many duplicates
	for i := 1; i <= 100; i++ {
		x := i / 12
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d}`, i, x))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Index should have one entry per document, not per unique value
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 100)

	// Query for a specific value — x=0 means i in [1..11] = 11 docs
	count, err := coll.Find(`{"x":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 11, count)

	// Verify index scan is used
	explain, err := coll.Find(`{"x":0}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_Maintenance_DeleteViaQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	// Insert 50 docs with a=i, b=i*2
	for i := 1; i <= 50; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	idxA := coll.GetIndexes()[0]
	idxB := coll.GetIndexes()[1]
	assertIndexLen(t, idxA, 50)
	assertIndexLen(t, idxB, 50)

	// Delete all docs where a > 40 (docs with id/a = 41..50 = 10 docs)
	res, err := coll.Find(`{"a":{"$gt":40}}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Modified)

	// Both indexes should have decreased
	assertIndexLen(t, idxA, 40)
	assertIndexLen(t, idxB, 40)

	// Deleted values should not be findable
	count, err := coll.Find(`{"a":45}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Non-deleted values should still be findable
	count, err = coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Total doc count
	assertCollCount(t, coll, 40)
}

func TestIndex_Maintenance_UpdateViaQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert 20 docs with a=1..20
	for i := 1; i <= 20; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":0}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 20)

	// Update docs where a >= 15: set b=999 (non-indexed field)
	res, err := coll.Find(`{"a":{"$gte":15}}`).Update(ctx, `{"$set":{"b":999}}`)
	require.NoError(t, err)
	assert.Equal(t, 6, res.Matched)  // a=15,16,17,18,19,20
	assert.Equal(t, 6, res.Modified)

	// Index length should be unchanged (only non-indexed field changed)
	assertIndexLen(t, idx, 20)

	// Verify a=15 still returns 1 result, and b is updated
	count, err := coll.Find(`{"a":15}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	doc, err := coll.FindId(ctx, 15)
	require.NoError(t, err)
	assert.Equal(t, float64(999), doc.Value().GetFloat64("b"))
}

func TestIndex_Maintenance_UpdateIndexedFieldViaQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert 10 docs with a=1..10
	for i := 1; i <= 10; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Update docs where a=5, set a=50
	res, err := coll.Find(`{"a":5}`).Update(ctx, `{"$set":{"a":50}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified)

	// Index length unchanged (old entry removed, new one added)
	assertIndexLen(t, idx, 10)

	// Old value gone
	count, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// New value present
	count, err = coll.Find(`{"a":50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Maintenance_UpdateNonIndexedField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10,"b":100}`)))

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 1)

	// Update non-indexed field b
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10,"b":200}`)))

	// Index length should not change
	assertIndexLen(t, idx, 1)

	// Verify a=10 still works and b is updated
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, float64(200), doc.Value().GetFloat64("b"))
}

func TestIndex_Maintenance_CompoundInsertDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	// Insert 30 docs with a=i%5, b=i%7
	for i := 0; i < 30; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 30)

	// Delete 10 specific docs (id=0..9)
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}
	assertIndexLen(t, idx, 20)

	// Query on compound should still work
	// a=2, b=3: i where i%5=2 and i%7=3, i in [10..29] → i=17 only
	count, err := coll.Find(`{"a":2,"b":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Insert 5 new docs
	for i := 30; i < 35; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 25)
	assertCollCount(t, coll, 25)
}

func TestIndex_Maintenance_MixedOperationSequence(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	// Step 1: Insert 5 docs
	for i := 1; i <= 5; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 5)
	assertCollCount(t, coll, 5)

	// Step 2: Update doc id=3 (a=30 -> a=300)
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":3,"a":300}`)))
	assertIndexLen(t, idx, 5)
	count, err := coll.Find(`{"a":30}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	count, err = coll.Find(`{"a":300}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Step 3: Delete doc id=1 and id=2
	require.NoError(t, coll.DeleteId(ctx, 1))
	require.NoError(t, coll.DeleteId(ctx, 2))
	assertIndexLen(t, idx, 3)
	assertCollCount(t, coll, 3)

	// Step 4: Insert new docs
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":6,"a":60}`),
		anyenc.MustParseJson(`{"id":7,"a":70}`),
	))
	assertIndexLen(t, idx, 5)
	assertCollCount(t, coll, 5)

	// Verify final state: a values should be 40, 50, 300, 60, 70
	for _, a := range []int{40, 50, 300, 60, 70} {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, a)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "expected 1 doc with a=%d", a)
	}

	// Deleted values should not be findable
	for _, a := range []int{10, 20, 30} {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, a)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count, "expected 0 docs with a=%d", a)
	}
}

func TestIndex_Maintenance_DeleteAllDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert 20 docs
	for i := 1; i <= 20; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 20)

	// Delete all via query
	res, err := coll.Find(nil).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, res.Modified)

	// Index should be empty
	assertIndexLen(t, idx, 0)
	assertCollCount(t, coll, 0)

	// Queries should return nothing
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_Maintenance_ReinsertAfterDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert, delete all, then re-insert
	for i := 1; i <= 10; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Delete all
	_, err = coll.Find(nil).Delete(ctx)
	require.NoError(t, err)
	assertIndexLen(t, idx, 0)

	// Re-insert with different values
	for i := 1; i <= 5; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i+100, i*100))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 5)
	assertCollCount(t, coll, 5)

	// Old values should not exist
	count, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// New values should be findable
	count, err = coll.Find(`{"a":300}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Maintenance_UniqueUpdateToExisting(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 2)

	// Attempt to update doc id=1 to have a=20 (conflicts with id=2)
	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":20}`))
	assert.ErrorIs(t, err, ErrUniqueConstraint)

	// Index should be unchanged
	assertIndexLen(t, idx, 2)

	// Original doc should be unchanged
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":10}`, doc.Value().String())
}

func TestIndex_Maintenance_SparseInsertMissingField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert doc with 'a' field
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 1)

	// Insert doc without 'a' field — sparse index should skip it
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":20}`)))
	assertIndexLen(t, idx, 1)

	// Insert doc with a=null — sparse index should skip it
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":null}`)))
	assertIndexLen(t, idx, 1)

	// Insert another with 'a' field
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":40}`)))
	assertIndexLen(t, idx, 2)

	assertCollCount(t, coll, 4)
}

func TestIndex_Maintenance_ArrayFieldInsertDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	// Insert doc with array field — multi-key: each element + the array-as-value
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["go","rust","python"]}`)))
	idx := coll.GetIndexes()[0]
	// 3 unique elements + 1 array-as-value = 4 index entries
	assertIndexLen(t, idx, 4)

	// Delete the doc — all entries should be removed
	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, idx, 0)
}

func TestIndex_Maintenance_ArrayFieldUpdate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`)))
	idx := coll.GetIndexes()[0]
	// 2 elements + 1 array-as-value = 3 entries
	assertIndexLen(t, idx, 3)

	// Update to different array
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["x","y","z"]}`)))
	// Old entries removed, new: 3 elements + 1 array-as-value = 4 entries
	assertIndexLen(t, idx, 4)

	// Query for old tag should return 0
	count, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Query for new tag should return 1
	count, err = coll.Find(`{"tags":"x"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Maintenance_MultipleIndexesSameCollection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	indexes := coll.GetIndexes()
	require.Len(t, indexes, 3)

	// Insert docs
	for i := 1; i <= 20; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i%3))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// All three indexes should have 20 entries
	for _, idx := range indexes {
		assertIndexLen(t, idx, 20)
	}

	// Delete 5 docs
	res, err := coll.Find(`{"a":0}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified) // a=i%5=0 for i=5,10,15,20 = 4 docs

	// All indexes should decrease consistently
	for _, idx := range indexes {
		assertIndexLen(t, idx, 16)
	}

	// Update some docs: change b for docs with a=1
	updateRes, err := coll.Find(`{"a":1}`).Update(ctx, `{"$set":{"b":99}}`)
	require.NoError(t, err)
	assert.True(t, updateRes.Modified > 0)

	// All indexes should still have 16 entries (update doesn't change count)
	for _, idx := range indexes {
		assertIndexLen(t, idx, 16)
	}
}

func TestIndex_Maintenance_UpsertCreatesIndexEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	// UpsertOne on new doc should create index entry
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	assertIndexLen(t, idx, 1)

	// UpsertOne on existing doc with different 'a' value
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":20}`)))
	assertIndexLen(t, idx, 1)

	// Old value should be gone, new value should be present
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Maintenance_SparseDeleteAndReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	idx := coll.GetIndexes()[0]

	// Insert mix of docs with and without 'a'
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"b":20}`),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
	))
	assertIndexLen(t, idx, 2) // only id=1 and id=3

	// Delete doc with 'a' field
	require.NoError(t, coll.DeleteId(ctx, 1))
	assertIndexLen(t, idx, 1)

	// Delete doc without 'a' field — index should not change
	require.NoError(t, coll.DeleteId(ctx, 2))
	assertIndexLen(t, idx, 1)

	// Re-insert doc with 'a' field
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":40}`)))
	assertIndexLen(t, idx, 2)

	assertCollCount(t, coll, 2) // id=3 and id=4
}

func TestIndex_Maintenance_UpdateSparseFieldAppears(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	idx := coll.GetIndexes()[0]

	// Insert doc without indexed field
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":10}`)))
	assertIndexLen(t, idx, 0)

	// Update to add indexed field — should create index entry
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":42,"b":10}`)))
	assertIndexLen(t, idx, 1)

	count, err := coll.Find(`{"a":42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Update to remove indexed field — should remove index entry
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"b":10}`)))
	assertIndexLen(t, idx, 0)

	count, err = coll.Find(`{"a":42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
