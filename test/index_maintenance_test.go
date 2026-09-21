package test

import (
	"fmt"
	"sort"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex_Maintenance_BulkInsertIndexLength(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"x"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	assert.Equal(t, 6, res.Matched) // a=15,16,17,18,19,20
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 2)

	// Attempt to update doc id=1 to have a=20 (conflicts with id=2)
	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":20}`))
	assert.ErrorIs(t, err, anystore.ErrUniqueConstraint)

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert doc with 'a' field
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 1)

	// Insert doc without 'a' field — sparse index should skip it
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"b":20}`)))
	assertIndexLen(t, idx, 1)

	// Insert doc with a=null — the field exists, so it is indexed under null
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":null}`)))
	assertIndexLen(t, idx, 2)

	// Insert another with 'a' field
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":40}`)))
	assertIndexLen(t, idx, 3)

	assertCollCount(t, coll, 4)
}

func TestIndex_Maintenance_ArrayFieldInsertDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Sparse: true}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Sparse: true}))

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

// --- from index_upsert_mutation_test.go ---

func TestIndex_UpsertMutation_UpsertOneInsert(t *testing.T) {
	// UpsertOne on a new document should create index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	assertIndexLen(t, idx, 1)

	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify the query works (CBO may pick FullScan for 1 doc — that's fine)
	explain, err := coll.Find(`{"a":10}`).Explain(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, explain.Sql)
}

func TestIndex_UpsertMutation_UpsertOneUpdate(t *testing.T) {
	// UpsertOne on an existing document should update index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	assertIndexLen(t, idx, 1)

	// Upsert same id with different a value
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":20}`)))
	assertIndexLen(t, idx, 1) // still 1 entry, old removed + new added

	// Old value gone
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// New value present
	count, err = coll.Find(`{"a":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UpsertMutation_UpsertOneUniqueConstraint(t *testing.T) {
	// UpsertOne with unique index: inserting a new doc with duplicate
	// indexed field should fail (UpsertOne matches by id, not indexed field)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))

	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"email":"a@test.com"}`)))

	// Different id, same email → should fail with unique constraint
	err = coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":2,"email":"a@test.com"}`))
	assert.Error(t, err, "inserting duplicate unique value with different id should fail")

	// Same id, same email → should succeed (update, same value)
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"email":"a@test.com","name":"updated"}`)))

	// Same id, new email → should succeed (update, old removed)
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"email":"b@test.com"}`)))

	count, err := coll.Find(`{"email":"a@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"email":"b@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UpsertMutation_UpsertIdInsert(t *testing.T) {
	// UpsertId creates a new document when id doesn't exist
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	mod := query.MustParseModifier(`{"$set":{"a":42}}`)
	res, err := coll.UpsertId(ctx, 1, mod)
	require.NoError(t, err)
	assert.Equal(t, 0, res.Matched)
	assert.Equal(t, 1, res.Modified)

	assertIndexLen(t, idx, 1)

	count, err := coll.Find(`{"a":42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UpsertMutation_UpsertIdUpdate(t *testing.T) {
	// UpsertId updates an existing document and index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	assertIndexLen(t, idx, 1)

	mod := query.MustParseModifier(`{"$set":{"a":99}}`)
	res, err := coll.UpsertId(ctx, 1, mod)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Matched)
	assert.Equal(t, 1, res.Modified)

	assertIndexLen(t, idx, 1)

	// Old value gone
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// New value present
	count, err = coll.Find(`{"a":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UpsertMutation_UpsertIdNotModified(t *testing.T) {
	// UpsertId with $set to same value → not modified, index unchanged
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	assertIndexLen(t, idx, 1)

	mod := query.MustParseModifier(`{"$set":{"a":10}}`)
	res, err := coll.UpsertId(ctx, 1, mod)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Matched)
	assert.Equal(t, 0, res.Modified) // no change

	assertIndexLen(t, idx, 1) // unchanged
}

func TestIndex_UpsertMutation_UpsertCompoundIndex(t *testing.T) {
	// UpsertOne with compound index: verify both fields updated in index
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	idx := coll.GetIndexes()[0]

	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":2}`)))
	assertIndexLen(t, idx, 1)

	// Verify compound query works
	count, err := coll.Find(`{"a":1,"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Upsert with changed b
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":99}`)))
	assertIndexLen(t, idx, 1)

	// Old compound gone
	count, err = coll.Find(`{"a":1,"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// New compound present
	count, err = coll.Find(`{"a":1,"b":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UpsertMutation_UpsertMultipleIndexes(t *testing.T) {
	// UpsertOne with multiple indexes: all stay consistent
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

	indexes := coll.GetIndexes()
	require.Len(t, indexes, 2)

	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10,"b":20}`)))
	for _, idx := range indexes {
		assertIndexLen(t, idx, 1)
	}

	// Update both fields
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":30,"b":40}`)))
	for _, idx := range indexes {
		assertIndexLen(t, idx, 1)
	}

	// Verify via both indexes
	count, err := coll.Find(`{"a":30}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"b":40}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Old values gone
	count, err = coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"b":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_UpsertMutation_FindUpdateIndexConsistency(t *testing.T) {
	// Find().Update() should update index entries for all modified documents
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"status":"old"}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 50)

	// Update all docs where a=5: change a to 50
	res, err := coll.Find(`{"a":5}`).Update(ctx, `{"$set":{"a":50}}`)
	require.NoError(t, err)
	assert.Equal(t, 5, res.Matched)
	assert.Equal(t, 5, res.Modified)

	assertIndexLen(t, idx, 50) // same count, entries swapped

	// Old value gone from index
	count, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// New value present
	count, err = coll.Find(`{"a":50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	// Index used for query
	explain, err := coll.Find(`{"a":50}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(a)")
}

func TestIndex_UpsertMutation_FindDeleteIndexConsistency(t *testing.T) {
	// Find().Delete() should remove index entries for all deleted documents
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 100)

	// Delete all docs where a=3
	res, err := coll.Find(`{"a":3}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Modified)

	assertIndexLen(t, idx, 90) // 100 - 10

	// Deleted values gone
	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Other values unaffected
	count, err = coll.Find(`{"a":4}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}

func TestIndex_UpsertMutation_FindDeleteCompoundIndex(t *testing.T) {
	// Find().Delete() with compound index
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	idx := coll.GetIndexes()[0]

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 100)

	// Delete docs matching compound filter
	res, err := coll.Find(`{"a":5}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Modified)

	assertIndexLen(t, idx, 90)

	// Verify nothing with a=5 remains
	count, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_UpsertMutation_FindUpdateNonIndexedField(t *testing.T) {
	// Find().Update() changing a non-indexed field should not affect index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"status":"old"}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 20)

	// Update non-indexed field "status"
	res, err := coll.Find(`{"a":2}`).Update(ctx, `{"$set":{"status":"new"}}`)
	require.NoError(t, err)
	assert.Equal(t, 4, res.Modified)

	assertIndexLen(t, idx, 20) // unchanged

	// Verify the update actually happened
	docs := collectField(t, coll.Find(`{"a":2}`), "status")
	assert.Len(t, docs, 4)
	for _, s := range docs {
		assert.Equal(t, `"new"`, s)
	}
}

func TestIndex_UpsertMutation_FindUpdateWithLimit(t *testing.T) {
	// Find().Limit().Update() should only update limited number of docs
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"v":0}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 50)

	// Update only first 2 docs matching a=5
	res, err := coll.Find(`{"a":5}`).Limit(2).Update(ctx, `{"$set":{"v":1}}`)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Matched)
	assert.Equal(t, 2, res.Modified)

	// Check that only 2 were updated
	count, err := coll.Find(`{"v":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	assertIndexLen(t, idx, 50) // unchanged (a field not modified)
}

func TestIndex_UpsertMutation_FindDeleteWithLimit(t *testing.T) {
	// Find().Limit().Delete() should only delete limited number of docs
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 50)

	// Delete only first 2 docs matching a=5
	res, err := coll.Find(`{"a":5}`).Limit(2).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)

	assertIndexLen(t, idx, 48) // 50 - 2

	// 3 remaining with a=5
	count, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestIndex_UpsertMutation_DeleteIdIndexConsistency(t *testing.T) {
	// DeleteId should remove the correct index entry
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 10)

	// Delete doc with id=5 (a=50)
	require.NoError(t, coll.DeleteId(ctx, 5))
	assertIndexLen(t, idx, 9)

	count, err := coll.Find(`{"a":50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Other docs unaffected
	count, err = coll.Find(`{"a":40}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// DeleteId on non-existent id
	err = coll.DeleteId(ctx, 999)
	assert.Error(t, err)
}

func TestIndex_UpsertMutation_UpsertBulkWithIndex(t *testing.T) {
	// Bulk upsert: mix of inserts and updates, index stays consistent
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	// Insert initial batch
	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 20)

	// Upsert mix: update existing (0-9), insert new (20-29)
	for i := range 20 {
		var id int
		if i < 10 {
			id = i // existing (0-9)
		} else {
			id = i + 10 // new (20-29)
		}
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, id, id+100))
		require.NoError(t, coll.UpsertOne(ctx, doc))
	}

	// Should have 30 docs total: 10 updated (0-9) + 10 original (10-19) + 10 new (20-29)
	totalCount, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, totalCount)

	assertIndexLen(t, idx, 30)

	// Updated docs should have new a values (id+100)
	count, err := coll.Find(`{"a":105}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // id=5 → a=105

	// Original unchanged docs still have old a values
	count, err = coll.Find(`{"a":15}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // id=15 → a=15
}

func TestIndex_UpsertMutation_UpdateOneIndexConsistency(t *testing.T) {
	// UpdateOne should correctly update index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":20}`)))
	assertIndexLen(t, idx, 2)

	// UpdateOne: change id=1's a from 10 to 30
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":30}`)))
	assertIndexLen(t, idx, 2)

	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":30}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Other doc unaffected
	count, err = coll.Find(`{"a":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UpsertMutation_FindUpdateUniqueConstraintViaInsert(t *testing.T) {
	// The unique constraint is enforced on EVERY write path, including
	// Find().Update(): collection.update does deleteKeys+insertKeys, and
	// insertKeys runs the AppendSeekKey prefix check on each insert, so an
	// update onto another doc's unique value returns ErrUniqueConstraint and
	// the whole tx rolls back (see TestIndex_Maintenance_FindUpdateUniqueConstraintEnforced
	// and TestIndex_UniqueSparse_FindUpdateMultiDocCollisionRollsBack).
	// This test verifies that direct Insert enforces uniqueness.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":20}`)))

	// Direct insert with duplicate unique value should fail
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":10}`))
	assert.Error(t, err, "inserting duplicate unique value should fail")

	// Only 2 docs total
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestIndex_UpsertMutation_UpsertSparseIndex(t *testing.T) {
	// UpsertOne with sparse index: upserting a doc without the indexed field
	// should not create an index entry
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Sparse: true}))

	idx := coll.GetIndexes()[0]

	// Insert doc with field
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))
	assertIndexLen(t, idx, 1)

	// Insert doc without field
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":2,"b":20}`)))
	assertIndexLen(t, idx, 1) // still 1

	// Upsert doc 1 to remove field a
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"b":30}`)))
	assertIndexLen(t, idx, 0) // field a removed, sparse index has 0 entries

	// Upsert doc 2 to add field a
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":2,"a":40}`)))
	assertIndexLen(t, idx, 1)

	count, err := coll.Find(`{"a":40}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_UpsertMutation_FindDeleteAllThenInsert(t *testing.T) {
	// Delete all docs, then insert new ones. Index should be clean.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	idx := coll.GetIndexes()[0]

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 50)

	// Delete all
	res, err := coll.Find(nil).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, res.Modified)
	assertIndexLen(t, idx, 0)

	// Insert new docs
	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i+100, i*100))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertIndexLen(t, idx, 10)

	// Verify index works for new data
	count, err := coll.Find(`{"a":500}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	explain, err := coll.Find(`{"a":500}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(a)")
}

func TestIndex_UpsertMutation_UpsertIdWithIncrement(t *testing.T) {
	// UpsertId with $inc modifier: both insert and update paths with index
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"counter"}}))

	idx := coll.GetIndexes()[0]
	mod := query.MustParseModifier(`{"$inc":{"counter":1}}`)

	// First call: insert (id doesn't exist)
	res, err := coll.UpsertId(ctx, "doc1", mod)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified)
	assertIndexLen(t, idx, 1)

	// Verify counter=1
	doc, err := coll.FindId(ctx, "doc1")
	require.NoError(t, err)
	assert.Equal(t, float64(1), doc.Value().GetFloat64("counter"))

	// Second call: update (increment to 2)
	res, err = coll.UpsertId(ctx, "doc1", mod)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Modified)
	assertIndexLen(t, idx, 1) // still 1 entry

	doc, err = coll.FindId(ctx, "doc1")
	require.NoError(t, err)
	assert.Equal(t, float64(2), doc.Value().GetFloat64("counter"))

	// Query by the new counter value
	count, err := coll.Find(`{"counter":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Old counter value gone
	count, err = coll.Find(`{"counter":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// ── Index-maintenance audit (act-13..18) ──
/*
Audit tests for the "index-maintenance" domain (any-store-tests:docs/any-store/qplanner/audit/actionable_by_domain.json).

  act-13  Multi-index insert: a partial index write is rolled back atomically when a
          later index's unique check fails.
  act-14  Multi-index update: cross-index rollback ordering on a trailing unique conflict.
  act-15  Failed unique update keeps BOTH old values queryable via the index (not just FindId).
  act-16  Removing a non-sparse array field collapses K+1 multikey entries to one 'null'.
  act-17  Updating a leading array field of a compound index re-fans-out the entries.
  act-18  Bulk insert: index length is the sum of per-doc keys, decoupled from doc count.
*/

func auditIdxByName(t testing.TB, coll anystore.Collection, name string) anystore.Index {
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10,"b":100}`)))

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":20,"b":100}`))
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":100}`),
		anyenc.MustParseJson(`{"id":2,"a":2,"b":200}`),
	))

	// id:1 → a=99 (index-a delete+insert) then b=200 collides with id:2.
	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":99,"b":200}`))
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))

	err = coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":20}`))
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags", "p"}}))
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))
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

// --- from single_index_test.go ---

func TestIndex_Single_DeleteAndQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Insert: 9 docs with a=1, 1 doc with a=2
	for i := 1; i <= 9; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":1,"b":%d}`, i, i))))
	}
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":10,"a":2,"b":0}`)))

	// All a=1 docs present
	vals := collectField(t, coll.Find(`{"a":1}`).Sort("b"), "b")
	assert.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}, vals)

	// Delete even b values (ids 2,4,6,8)
	for _, id := range []int{2, 4, 6, 8} {
		require.NoError(t, coll.DeleteId(ctx, id))
	}

	vals = collectField(t, coll.Find(`{"a":1}`).Sort("b"), "b")
	assert.Equal(t, []string{"1", "3", "5", "7", "9"}, vals)

	// Delete b>2 (ids 3,5,7,9)
	for _, id := range []int{3, 5, 7, 9} {
		require.NoError(t, coll.DeleteId(ctx, id))
	}

	vals = collectField(t, coll.Find(`{"a":1}`).Sort("b"), "b")
	assert.Equal(t, []string{"1"}, vals)

	// Delete last a=1 doc
	require.NoError(t, coll.DeleteId(ctx, 1))
	count, err := coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// a=2 doc untouched
	vals = collectField(t, coll.Find(`{"a":2}`), "b")
	assert.Equal(t, []string{"0"}, vals)
}

func TestIndex_Single_IndexedVsNonIndexed(t *testing.T) {
	fx := newFixture(t)

	collIdx, err := fx.CreateCollection(ctx, "indexed")
	require.NoError(t, err)
	require.NoError(t, collIdx.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	collNoIdx, err := fx.CreateCollection(ctx, "noidx")
	require.NoError(t, err)

	// Insert identical data
	for i := 1; i <= 50; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, collIdx.Insert(ctx, doc))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}

	// Compare equality results
	countIdx, err := collIdx.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	countNoIdx, err := collNoIdx.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, countIdx)

	// Compare range results
	valsIdx := collectField(t, collIdx.Find(`{"a":{"$gte":3,"$lt":7}}`).Sort("a"), "a")
	valsNoIdx := collectField(t, collNoIdx.Find(`{"a":{"$gte":3,"$lt":7}}`).Sort("a"), "a")
	assert.Equal(t, valsNoIdx, valsIdx)

	// Compare sorted full scan
	valsIdx = collectField(t, collIdx.Find(nil).Sort("a"), "a")
	valsNoIdx = collectField(t, collNoIdx.Find(nil).Sort("a"), "a")
	assert.Equal(t, valsNoIdx, valsIdx)

	// Verify indexed collection uses IndexScan
	explain, err := collIdx.Find(`{"a": 5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// Verify non-indexed collection uses FullScan
	explain, err = collNoIdx.Find(`{"a": 5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "FullScan")
}

func TestIndex_Single_ReverseField(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-a"}}))

	for i := 1; i <= 8; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	// Range query with reverse index should still return correct count
	count, err := coll.Find(`{"a":{"$gt":3,"$lt":7}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Sort("-a") matches the index's declared direction (the field is stored
	// inverted), so it is served by a FORWARD index scan with no in-memory sort
	// and yields strictly descending order.
	vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
	require.Equal(t, []string{"8", "7", "6", "5", "4", "3", "2", "1"}, vals)

	explain, err := coll.Find(nil).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(-a)")
	assert.NotContains(t, explain.Sql, "(reverse)")
	assert.NotContains(t, explain.Sql, "-> Sort", explain.Sql)
	assert.NotContains(t, explain.Sql, "TopK", explain.Sql)

	// Sort("a") is the opposite of the declared direction → reverse scan, no sort.
	valsAsc := collectField(t, coll.Find(nil).Sort("a"), "a")
	require.Equal(t, []string{"1", "2", "3", "4", "5", "6", "7", "8"}, valsAsc)
	explainAsc, err := coll.Find(nil).Sort("a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explainAsc.Sql, "IndexScan(-a)")
	assert.Contains(t, explainAsc.Sql, "(reverse)")
	assert.NotContains(t, explainAsc.Sql, "-> Sort", explainAsc.Sql)
	assert.NotContains(t, explainAsc.Sql, "TopK", explainAsc.Sql)

	// Verify equality filter still works correctly
	count, err = coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Single_ReverseFieldWithRange(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-a"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	// Range queries should return correct COUNT regardless of index direction
	count, err := coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	count, err = coll.Find(`{"a":{"$gt":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	count, err = coll.Find(`{"a":{"$lt":4}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Range on a reverse index, Sort("-a"): a forward scan over the inverted
	// storage yields strictly descending order, served entirely by the index
	// (no in-memory sort). The reverse-field bounds segment renders as an
	// "unknown type" placeholder in Explain (the decode path intentionally does
	// not un-invert), so we assert only the plan shape, not the bounds string.
	vals := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("-a"), "a")
	require.Equal(t, []string{"7", "6", "5", "4", "3"}, vals)

	explain, err := coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(-a)")
	assert.NotContains(t, explain.Sql, "(reverse)")
	assert.NotContains(t, explain.Sql, "-> Sort")
}

// --- from compound_index_test.go ---

// TestIndex_Compound_RangeFirstEqualitySecond tests range on first field
// combined with equality on second field of a compound index.
func TestIndex_Compound_RangeFirstEqualitySecond(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	// Insert 60 docs: a=i%6, b=i%4
	for i := range 60 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%6, i%4))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Range on a (2..4), equality on b=1
	// a in {2,3,4}, b=1: find manually
	// a=i%6 in {2,3,4} and b=i%4=1
	// Expected: i where i%6 in {2,3,4} and i%4=1
	var expected int
	for i := range 60 {
		if i%6 >= 2 && i%6 <= 4 && i%4 == 1 {
			expected++
		}
	}

	count, err := coll.Find(`{"a":{"$gte":2,"$lte":4},"b":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, count, "range on first + equality on second")

	// Also compare with unindexed collection
	collNoIdx, err := fx.CreateCollection(ctx, "test_noidx")
	require.NoError(t, err)
	for i := range 60 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%6, i%4))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}
	countNoIdx, err := collNoIdx.Find(`{"a":{"$gte":2,"$lte":4},"b":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count, "indexed and non-indexed should match")
}

// TestIndex_Compound_ThreeFieldSortOrder verifies that a three-field compound
// index produces correct multi-level sort ordering.
func TestIndex_Compound_ThreeFieldSortOrder(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b", "c"}}))

	// Insert docs with known values
	for i := range 120 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%3, i%4, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("sort all three ascending", func(t *testing.T) {
		iter, err := coll.Find(nil).Sort("a", "b", "c").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()

		var prevA, prevB, prevC int
		prevA = -1
		first := true
		count := 0
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			a := doc.Value().GetInt("a")
			b := doc.Value().GetInt("b")
			c := doc.Value().GetInt("c")
			if !first {
				ok := a > prevA ||
					(a == prevA && b > prevB) ||
					(a == prevA && b == prevB && c >= prevC)
				assert.True(t, ok, "sort order violated at doc %d: prev=(%d,%d,%d) cur=(%d,%d,%d)",
					count, prevA, prevB, prevC, a, b, c)
			}
			prevA = a
			prevB = b
			prevC = c
			first = false
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 120, count)
	})

	t.Run("filter first, sort second and third", func(t *testing.T) {
		iter, err := coll.Find(`{"a":1}`).Sort("b", "c").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()

		var prevB, prevC int
		prevB = -1
		first := true
		count := 0
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			a := doc.Value().GetInt("a")
			b := doc.Value().GetInt("b")
			c := doc.Value().GetInt("c")
			assert.Equal(t, 1, a)
			if !first {
				ok := b > prevB || (b == prevB && c >= prevC)
				assert.True(t, ok, "sort order violated: prev=(%d,%d) cur=(%d,%d)", prevB, prevC, b, c)
			}
			prevB = b
			prevC = c
			first = false
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 40, count) // 120/3 = 40 docs with a=1
	})
}

// TestIndex_Compound_MixedDirectionsSort tests a compound index with mixed
// ascending/descending directions and verifies sort correctness.
func TestIndex_Compound_MixedDirectionsSort(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "-b"}}))

	// Insert docs
	for i := range 40 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%4, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Unindexed twin with identical data: its in-memory sort is the correct
	// reference order. (a,-b) is stored with the reverse field inverted, so a
	// single forward scan realizes (a asc, b desc) and a reverse scan (a desc,
	// b asc); the indexed RESULT ORDER — incl. b within each a group — must match
	// the unindexed twin exactly while being served by a FAST index scan.
	fx2 := newFixture(t)
	twin, err := fx2.CreateCollection(ctx, "twin")
	require.NoError(t, err)
	for i := range 40 {
		require.NoError(t, twin.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%4, i%5))))
	}

	abPairs := func(c anystore.Collection, sort ...any) []string {
		iter, err := c.Find(nil).Sort(sort...).Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var out []string
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			out = append(out, fmt.Sprintf("%d,%d", d.Value().GetInt("a"), d.Value().GetInt("b")))
		}
		require.NoError(t, iter.Err())
		return out
	}

	t.Run("sort matching index direction a asc b desc", func(t *testing.T) {
		got := abPairs(coll, "a", "-b")
		assert.Equal(t, abPairs(twin, "a", "-b"), got, "indexed (a,-b) order must match the correct in-memory order")
		require.Len(t, got, 40)
		// Served by a FORWARD index scan: no in-memory Sort.
		ex, err := coll.Find(nil).Sort("a", "-b").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "IndexScan(a,-b)")
		assert.NotContains(t, ex.Sql, "(reverse)")
		assert.NotContains(t, ex.Sql, "-> Sort")
		// Within each a group, b must be non-increasing (descending).
		prevA, prevB, first := -1, -1, true
		for _, p := range got {
			var a, b int
			_, _ = fmt.Sscanf(p, "%d,%d", &a, &b)
			if !first {
				if a == prevA {
					assert.True(t, b <= prevB, "b must be descending within a=%d: got %d after %d", a, b, prevB)
				} else {
					assert.True(t, a > prevA, "a must be ascending: got %d after %d", a, prevA)
				}
			}
			prevA, prevB, first = a, b, false
		}
	})

	t.Run("sort exact reverse of index", func(t *testing.T) {
		// Sort("-a","b"): a descending, b ascending within each a group.
		got := abPairs(coll, "-a", "b")
		assert.Equal(t, abPairs(twin, "-a", "b"), got)
		require.Len(t, got, 40)
		// The exact opposite of the declared directions → REVERSE scan, no Sort.
		ex, err := coll.Find(nil).Sort("-a", "b").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "IndexScan(a,-b)")
		assert.Contains(t, ex.Sql, "(reverse)")
		assert.NotContains(t, ex.Sql, "-> Sort")
		prevA, prevB, first := 1<<30, -1, true
		for _, p := range got {
			var a, b int
			_, _ = fmt.Sscanf(p, "%d,%d", &a, &b)
			if !first {
				if a == prevA {
					assert.True(t, b >= prevB, "b must be ascending within a=%d: got %d after %d", a, b, prevB)
				} else {
					assert.True(t, a < prevA, "a must be descending: got %d after %d", a, prevA)
				}
			}
			prevA, prevB, first = a, b, false
		}
	})

	t.Run("sort direction mismatch produces correct results", func(t *testing.T) {
		// (a ASC, b ASC) is mixed relative to declared (a ASC, -b DESC) and is
		// not realizable by a single scan direction → genuine in-memory sort.
		assert.Equal(t, abPairs(twin, "a", "b"), abPairs(coll, "a", "b"))
		ex, err := coll.Find(nil).Sort("a", "b").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "-> Sort", "Sort(a,b) on (a,-b) must fall back to in-memory sort")
	})
}

// TestIndex_Compound_AllFieldsRangeQueried tests range queries on both fields
// of a compound index simultaneously.
func TestIndex_Compound_AllFieldsRangeQueried(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Range on both: a in [3,6], b in [2,5]
	var expected int
	for i := range 100 {
		if i%10 >= 3 && i%10 <= 6 && i%7 >= 2 && i%7 <= 5 {
			expected++
		}
	}

	count, err := coll.Find(`{"a":{"$gte":3,"$lte":6},"b":{"$gte":2,"$lte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, count)

	// Compare with non-indexed
	collNoIdx, err := fx.CreateCollection(ctx, "noidx")
	require.NoError(t, err)
	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}
	countNoIdx, err := collNoIdx.Find(`{"a":{"$gte":3,"$lte":6},"b":{"$gte":2,"$lte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count, "indexed and non-indexed should match")
}

// TestIndex_Compound_FullMatchVsSingleFieldSelection tests that the planner
// prefers a compound index over a single-field index when filtering on both
// fields of the compound.
func TestIndex_Compound_FullMatchVsSingleFieldSelection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Query on both a and b: compound index should be preferred
	explain, err := coll.Find(`{"a":5,"b":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// The compound index should be the used one
	require.True(t, len(explain.Indexes) >= 2, "should have at least 2 indexes reported")
	assert.Equal(t, "a,b", explain.Indexes[0].Name, "compound index should be used")
	assert.True(t, explain.Indexes[0].Used)

	// Verify correctness regardless of index choice
	count, err := coll.Find(`{"a":5,"b":3}`).Count(ctx)
	require.NoError(t, err)
	// a=5 and b=3: a=i%10=5, b=i%5=3 → i≡5(mod10) and i≡3(mod5)
	// i≡5(mod10) already means i%5=0, so i%5=3 is impossible → expect 0
	// Let me recalculate: i%10=5 means i in {5,15,25,35,45,55,65,75,85,95}
	// i%5 for these: {0,0,0,0,0,0,0,0,0,0} → all 0, none is 3
	assert.Equal(t, 0, count, "no doc has a=5 and b=3 with these moduli")

	// Try a query that does have results
	count, err = coll.Find(`{"a":5,"b":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count, "all a=5 docs have b=0 since 5%5=0")
}

// TestIndex_Compound_RangeOnEachPosition tests range queries targeting
// different positions within a compound index.
func TestIndex_Compound_RangeOnEachPosition(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	// 80 docs: a=i%8, b=i%5
	for i := range 80 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%8, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("range only on first field", func(t *testing.T) {
		// a > 5: a in {6,7}, 10 each = 20
		count, err := coll.Find(`{"a":{"$gt":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 20, count)

		explain, err := coll.Find(`{"a":{"$gt":5}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("range only on second field", func(t *testing.T) {
		// b < 2: b in {0,1}
		var expected int
		for i := range 80 {
			if i%5 < 2 {
				expected++
			}
		}
		count, err := coll.Find(`{"b":{"$lt":2}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected, count)
	})

	t.Run("range on first, gt on second", func(t *testing.T) {
		// a >= 4 and b > 3
		var expected int
		for i := range 80 {
			if i%8 >= 4 && i%5 > 3 {
				expected++
			}
		}
		count, err := coll.Find(`{"a":{"$gte":4},"b":{"$gt":3}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected, count)
	})
}

// TestIndex_Compound_CompareIndexedVsUnindexed verifies that compound indexed
// queries return identical results to non-indexed queries across various filter
// and sort combinations.
func TestIndex_Compound_CompareIndexedVsUnindexed(t *testing.T) {
	fx := newFixture(t)

	// Create two collections with same data, one indexed, one not
	collIdx, err := fx.CreateCollection(ctx, "indexed")
	require.NoError(t, err)
	require.NoError(t, collIdx.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	collNoIdx, err := fx.CreateCollection(ctx, "unindexed")
	require.NoError(t, err)

	for i := range 80 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%8, i%5))
		require.NoError(t, collIdx.Insert(ctx, doc))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}

	tests := []struct {
		name   string
		filter string
		sort   []string
	}{
		{"equality first", `{"a":3}`, []string{"b"}},
		{"range first", `{"a":{"$gte":2,"$lte":5}}`, []string{"a", "b"}},
		{"equality both", `{"a":3,"b":2}`, nil},
		{"range both", `{"a":{"$gt":1,"$lt":6},"b":{"$gte":1,"$lte":3}}`, []string{"a", "b"}},
		{"all docs sorted", ``, []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filter any
			if tt.filter == "" {
				filter = nil
			} else {
				filter = tt.filter
			}
			qIdx := collIdx.Find(filter)
			qNoIdx := collNoIdx.Find(filter)
			if len(tt.sort) > 0 {
				sortArgs := make([]any, len(tt.sort))
				for i, s := range tt.sort {
					sortArgs[i] = s
				}
				qIdx = qIdx.Sort(sortArgs...)
				qNoIdx = qNoIdx.Sort(sortArgs...)
			}

			idxDocs := collectDocs(t, qIdx)
			noIdxDocs := collectDocs(t, qNoIdx)
			assert.Equal(t, len(noIdxDocs), len(idxDocs), "count mismatch for %s", tt.name)

			if len(tt.sort) > 0 {
				// With sort, order must match exactly
				assert.Equal(t, noIdxDocs, idxDocs, "results mismatch for %s", tt.name)
			}
		})
	}
}

// --- from array_nested_index_test.go ---

func TestIndex_ArrayNested_ArrayField_MultipleEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

	doc := anyenc.MustParseJson(`{"id":1,"tags":["go","rust","python"]}`)
	require.NoError(t, coll.Insert(ctx, doc))

	// From fillKeysBuf: array ["go","rust","python"] produces keys:
	// "go", "rust", "python" (3 unique elements) + ["go","rust","python"] (the array itself) = 4 entries
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 4)

	// Query for an element should find the document
	count, err := coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_ArrayNested_ArrayField_QueryElement(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

	// Insert multiple docs, some containing "go" in their tags
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["go","rust"]}`),
		anyenc.MustParseJson(`{"id":2,"tags":["python","java"]}`),
		anyenc.MustParseJson(`{"id":3,"tags":["go","python","c"]}`),
		anyenc.MustParseJson(`{"id":4,"tags":["haskell"]}`),
		anyenc.MustParseJson(`{"id":5,"tags":["go"]}`),
	))

	// Query for "go" should find docs 1, 3, 5
	count, err := coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	// Query for "python" should find docs 2, 3
	count, err = coll.Find(`{"tags":"python"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Query for "haskell" should find doc 4 only
	count, err = coll.Find(`{"tags":"haskell"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Query for non-existent tag
	count, err = coll.Find(`{"tags":"cobol"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_ArrayNested_ArrayField_UpdateArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

	// Insert doc with tags ["a","b"]
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`)))
	idx := coll.GetIndexes()[0]
	// "a", "b", ["a","b"] = 3 entries
	assertIndexLen(t, idx, 3)

	// Update to tags ["c","d"]
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["c","d"]}`)))

	// Old entries removed, new entries added: "c", "d", ["c","d"] = 3
	assertIndexLen(t, idx, 3)

	// Query old values — should find nothing
	count, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Query new values — should find the doc
	count, err = coll.Find(`{"tags":"c"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"tags":"d"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_ArrayNested_ArrayField_DeleteDoc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":2,"tags":["c","d"]}`),
	))

	idx := coll.GetIndexes()[0]
	// Doc1: "a","b",["a","b"] = 3; Doc2: "c","d",["c","d"] = 3; total = 6
	assertIndexLen(t, idx, 6)

	// Delete doc 1
	require.NoError(t, coll.DeleteId(ctx, 1))

	// Only doc2 entries remain: "c","d",["c","d"] = 3
	assertIndexLen(t, idx, 3)

	// Query for deleted doc's tags — nothing
	count, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Query for remaining doc's tags — found
	count, err = coll.Find(`{"tags":"c"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Delete doc 2
	require.NoError(t, coll.DeleteId(ctx, 2))
	assertIndexLen(t, idx, 0)
}

func TestIndex_ArrayNested_ArrayField_EmptyArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

	// Insert doc with empty array
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":[]}`)))

	idx := coll.GetIndexes()[0]
	// Empty array: no elements to expand, but the array value itself [] is indexed
	// From writeValues: when arr is empty, the if-len(arr)!=0 branch is skipped,
	// then v.MarshalTo is called on the full value (which is []), producing one key.
	assertIndexLen(t, idx, 1)
}

func TestIndex_ArrayNested_ArrayField_DuplicateElements(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

	// Insert doc with duplicate elements
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","a","b"]}`)))

	idx := coll.GetIndexes()[0]
	// From fillKeysBuf test cases: {"id":1,"a":["a", "a", "b", "c", "b"]} → ["a","b","c",full-array]
	// So ["a","a","b"] → deduplicated elements "a","b" + full array ["a","a","b"] = 3 entries
	assertIndexLen(t, idx, 3)

	// Query should still find the doc
	count, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"tags":"b"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_ArrayNested_NestedField_DotNotation(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"meta.score"}}))

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"meta":{"score":%d}}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 20)

	// Range query on nested field
	count, err := coll.Find(`{"meta.score":{"$gte":50}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 15, count) // scores 50,60,...,190

	count, err = coll.Find(`{"meta.score":{"$gte":50,"$lt":100}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count) // scores 50,60,70,80,90

	// Equality
	count, err = coll.Find(`{"meta.score":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify index is used
	explain, err := coll.Find(`{"meta.score":{"$gte":50}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_ArrayNested_NestedField_MissingParent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"meta.score"}}))

	// Insert doc without "meta" field at all
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"name":"no-meta"}`)))
	// Insert doc with meta but no score
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"meta":{"name":"has-meta"}}`)))
	// Insert doc with meta.score
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"meta":{"score":42}}`)))

	idx := coll.GetIndexes()[0]
	// Doc1: meta missing → null key; Doc2: meta.score missing → null key; Doc3: meta.score=42
	// All 3 docs get indexed (non-sparse index indexes nulls)
	assertIndexLen(t, idx, 3)

	// Query for score=42 should find only doc3
	count, err := coll.Find(`{"meta.score":42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Sparse index should skip docs without meta.score
	fx2 := newFixture(t)
	coll2, err := fx2.CreateCollection(ctx, "test2")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"meta.score"}, Sparse: true}))

	require.NoError(t, coll2.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"name":"no-meta"}`),
		anyenc.MustParseJson(`{"id":2,"meta":{"name":"has-meta"}}`),
		anyenc.MustParseJson(`{"id":3,"meta":{"score":42}}`),
	))

	idx2 := coll2.GetIndexes()[0]
	// Only doc3 has meta.score — sparse index should have 1 entry
	assertIndexLen(t, idx2, 1)
}

func TestIndex_ArrayNested_NestedField_DeepNesting(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a.b.c"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":{"b":{"c":10}}}`),
		anyenc.MustParseJson(`{"id":2,"a":{"b":{"c":20}}}`),
		anyenc.MustParseJson(`{"id":3,"a":{"b":{"c":30}}}`),
		anyenc.MustParseJson(`{"id":4,"a":{"b":{}}}`), // c missing
		anyenc.MustParseJson(`{"id":5,"a":{}}`),       // b missing
		anyenc.MustParseJson(`{"id":6}`),              // a missing
	))

	idx := coll.GetIndexes()[0]
	// All 6 docs indexed (non-sparse: missing = null key)
	assertIndexLen(t, idx, 6)

	// Range query
	count, err := coll.Find(`{"a.b.c":{"$gte":15}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count) // c=20, c=30

	// Equality
	count, err = coll.Find(`{"a.b.c":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify index scan
	explain, err := coll.Find(`{"a.b.c":10}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_ArrayNested_CompoundArrayNested(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags", "meta.score"}}))

	// Doc with array tags and nested score
	// tags: ["go","rust"] → elements "go","rust" + array ["go","rust"]
	// meta.score: 80 → single value
	// Cartesian product: "go"/80, "rust"/80, ["go","rust"]/80 = 3 entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["go","rust"],"meta":{"score":80}}`),
	))

	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 3)

	// Insert doc with single-element array
	// tags: ["python"] → "python" + ["python"]
	// meta.score: 90
	// Product: "python"/90, ["python"]/90 = 2 entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":2,"tags":["python"],"meta":{"score":90}}`),
	))
	assertIndexLen(t, idx, 5)

	// Query by tag element + nested score
	count, err := coll.Find(`{"tags":"go","meta.score":80}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Query by tag only
	count, err = coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Query by nested field only
	count, err = coll.Find(`{"meta.score":{"$gte":85}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // only doc2 with score=90

	// Insert doc with missing nested field
	// tags: ["go"] → "go" + ["go"]
	// meta.score: missing → null
	// Product: "go"/null, ["go"]/null = 2 entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":3,"tags":["go"]}`),
	))
	assertIndexLen(t, idx, 7)

	// Query "go" should now find doc1 and doc3
	count, err = coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

// --- from multikey_in_dedup_test.go ---

// Multi-key index dedup invariants.
//
// When an index is built on an array-valued field, one index entry per
// array element exists per document. A query whose bounds match multiple
// elements of the same document must still return that document exactly
// once, and Count must agree with the distinct-doc count of Iter.
//
// The pipeline achieves this via:
//   - CanonicalKeyDedupIter (O(1) memory) for single-field indexes
//   - SeenSetDedupIter (O(distinct) memory) for compound indexes
//   - Guard on the covering-index Count fast path when len(Bounds) > 1
//
// Scope: dedup is wired for both single-field and compound multi-key
// indexes with any bound shape (point, range, or empty). Scalar-valued
// fields pay a runtime TypeArray check and a pass-through cost.
func TestMultiKeyIn_Dedup(t *testing.T) {
	// ------------------------------------------------------------------
	// Count() used to over-count on multi-key because the covering-index
	// fast path counts index ENTRIES, not distinct documents. Fixed by
	// guarding the fast path with len(Bounds) <= 1.
	// ------------------------------------------------------------------
	t.Run("count_over_multikey_in_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"]}`),
		))

		filter := `{"tags":{"$in":["ai","theory"]}}`

		n, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err)
		t.Logf("Count returned: %d (expected 1)", n)
		assert.Equal(t, 1, n, "one distinct doc matches; Count should dedup")
	})

	// ------------------------------------------------------------------
	// Iter() previously surfaced duplicate docs whenever IndexSeek was
	// chosen (large collection, Sort, or IndexHint). Fixed by the
	// CanonicalKeyDedupIter wrap.
	// ------------------------------------------------------------------
	t.Run("iter_over_multikey_in_is_correct_under_indexseek", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

		// One doc with both "ai" and "theory", plus filler so the CBO
		// prefers IndexSeek over FullScan.
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"],"t":5}`),
		))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":"f%d","tags":["filler"],"t":%d}`, i, i)),
			))
		}

		filter := `{"tags":{"$in":["ai","theory"]}}`
		q := coll.Find(filter).Sort("-t") // Sort biases the planner toward the index

		exp, err := q.Explain(ctx)
		require.NoError(t, err)
		t.Logf("plan (Iter):\n%s", exp.Plan)

		ids, err := iterIds(t, coll, q)
		require.NoError(t, err)
		t.Logf("observed ids: %v", ids)
		assert.Equal(t, []string{"p1"}, ids,
			"p1 must appear exactly once — not one per matching tag element")
	})

	// ------------------------------------------------------------------
	// Deterministic version: IndexHint boost forces IndexSeek regardless
	// of the cost model, so the invariant is exercised stably.
	// ------------------------------------------------------------------
	t.Run("iter_over_multikey_in_is_correct_with_IndexHint", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "tags", Fields: []string{"tags"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c","d"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["a","x"]}`),
		))

		q := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).
			IndexHint(anystore.IndexHint{IndexName: "tags", Boost: 10000})

		exp, err := q.Explain(ctx)
		require.NoError(t, err)
		t.Logf("plan (Iter):\n%s", exp.Plan)

		ids, err := iterIds(t, coll, q)
		require.NoError(t, err)
		t.Logf("observed ids: %v", ids)
		assert.Equal(t, []string{"p1", "p2"}, ids,
			"p1 matches 3 ranges, p2 matches 1 — each doc must appear once")
	})

	// -------- baselines that MUST keep passing after the fix --------

	t.Run("baseline_single_value_tag_filter_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"tags"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"tags":"ai"}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids)

		n, err := coll.Find(`{"tags":"ai"}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("baseline_fullscan_without_index_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		// No index on tags — planner must use FullScan.

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["ai","theory","philosophy"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["other"]}`),
		))

		filter := `{"tags":{"$in":["ai","theory"]}}`
		ids, err := iterIds(t, coll, coll.Find(filter))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids)

		n, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n, "FullScan visits each doc once; Count is correct")
	})

	t.Run("baseline_in_over_scalar_field_is_correct", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"status"}}))

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft"}`),
			anyenc.MustParseJson(`{"id":"p2","status":"published"}`),
			anyenc.MustParseJson(`{"id":"p3","status":"archived"}`),
		))

		filter := `{"status":{"$in":["draft","published"]}}`
		ids, err := iterIds(t, coll, coll.Find(filter))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, ids)

		n, err := coll.Find(filter).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n, "scalar field: one index entry per doc; safe to count")
	})

	// ---------------- range bounds on single-field multi-key ----------------

	t.Run("iter_range_bounds_on_multikey_no_duplicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "tags", Fields: []string{"tags"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["b","z"]}`),
		))

		ids, err := iterIds(t, coll,
			coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).
				IndexHint(anystore.IndexHint{IndexName: "tags", Boost: 10000}))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, ids, "range bounds: each doc exactly once")

		n, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, n)
	})

	t.Run("iter_full_index_scan_no_filter_no_duplicates", func(t *testing.T) {
		// Pure Sort over an index with no filter ⇒ IndexScan with empty
		// bounds. Dedup must still run.
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "tags", Fields: []string{"tags"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c"]}`),
			anyenc.MustParseJson(`{"id":"p2","tags":["d"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{}`).Sort("tags"))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1", "p2"}, ids,
			"Sort over multi-key index without a filter must not surface duplicates")
	})

	// ---------------- compound-index coverage (SeenSetDedupIter branch) ----------------

	t.Run("compound_scalar_array_no_duplicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Name: "status_tags", Fields: []string{"status", "tags"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft","tags":["a","b"]}`),
			anyenc.MustParseJson(`{"id":"p2","status":"draft","tags":["c"]}`),
			anyenc.MustParseJson(`{"id":"p3","status":"published","tags":["a"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"status":"draft","tags":{"$in":["a","b"]}}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids,
			"compound (scalar, array) must dedup via SeenSetDedupIter")

		n, err := coll.Find(`{"status":"draft","tags":{"$in":["a","b"]}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})

	t.Run("compound_array_scalar_no_duplicates", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Name: "tags_status", Fields: []string{"tags", "status"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft","tags":["a","b"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"tags":{"$in":["a","b"]},"status":"draft"}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids,
			"compound (array, scalar) must dedup via SeenSetDedupIter")
	})

	t.Run("compound_range_on_trailing_array_no_duplicates", func(t *testing.T) {
		// Index (status, tags); range on tags produces multiple compound
		// entries per doc. SeenSet must dedup.
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Name: "status_tags", Fields: []string{"status", "tags"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","status":"draft","tags":["a","b","c"]}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"status":"draft","tags":{"$gte":"a","$lte":"c"}}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids,
			"compound range on trailing array must dedup")
	})

	t.Run("compound_scalar_scalar_sanity", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "posts")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Name: "author_status", Fields: []string{"authorId", "status"},
		}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":"p1","authorId":"u1","status":"draft"}`),
			anyenc.MustParseJson(`{"id":"p2","authorId":"u1","status":"published"}`),
		))

		ids, err := iterIds(t, coll, coll.Find(`{"authorId":"u1","status":"draft"}`))
		require.NoError(t, err)
		assert.Equal(t, []string{"p1"}, ids, "compound scalar-scalar: no dup expected or needed")

		n, err := coll.Find(`{"authorId":"u1","status":"draft"}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	})
}

func iterIds(t *testing.T, _ anystore.Collection, q anystore.Query) ([]string, error) {
	t.Helper()
	it, err := q.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer it.Close()

	var out []string
	for it.Next() {
		d, err := it.Doc()
		if err != nil {
			return nil, err
		}
		out = append(out, string(d.Value().GetStringBytes("id")))
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	sort.Strings(out)
	return out, nil
}

// --- Coverage tests from multikey_dedup_coverage_test.go ---

// TestMultiKeyDedup_Coverage_PureSortNoBounds exercises buildIndexScanChain
// on a multi-key index with a pure Sort() and no filter (noBounds==true).
// Covers internal/qplanner/planner.go:1017-1029 — the dedup wrap must still
// run even when bounds are empty. Each doc must appear exactly once despite
// multiple index entries per array element.
func TestMultiKeyDedup_Coverage_PureSortNoBounds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "posts")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	// Each doc has multiple tags → multiple index entries per doc.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"p1","tags":["a","b","c","d","e"]}`),
		anyenc.MustParseJson(`{"id":"p2","tags":["b","c","f"]}`),
		anyenc.MustParseJson(`{"id":"p3","tags":["x"]}`),
		anyenc.MustParseJson(`{"id":"p4","tags":["a","z"]}`),
	))

	// Pure sort on the multi-key indexed field with no filter.
	q := coll.Find(`{}`).Sort("tags")

	it, err := q.Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	counts := map[string]int{}
	var order []string
	for it.Next() {
		d, err := it.Doc()
		require.NoError(t, err)
		id := string(d.Value().GetStringBytes("id"))
		counts[id]++
		order = append(order, id)
	}
	require.NoError(t, it.Err())

	// Each doc must appear exactly once (dedup must run over empty bounds).
	for _, id := range []string{"p1", "p2", "p3", "p4"} {
		assert.Equal(t, 1, counts[id],
			"doc %q must appear exactly once under Sort('tags')+no filter", id)
	}
	sorted := append([]string{}, order...)
	sort.Strings(sorted)
	assert.ElementsMatch(t, []string{"p1", "p2", "p3", "p4"}, sorted)

	// Count() must also dedup correctly (not return 11 entries).
	n, err := coll.Find(`{}`).Sort("tags").Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, n,
		"Count on sort-only multi-key must match distinct-doc count")
}

// --- from complex_filter_index_test.go ---

// setupComplexFilterColl creates a collection with 100 docs where a=i%10, b=i%7
// and applies the given indexes.
func setupComplexFilterColl(t testing.TB, indexes ...anystore.IndexInfo) anystore.Collection {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for _, idx := range indexes {
		require.NoError(t, coll.EnsureIndex(ctx, idx))
	}
	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	return coll
}

func TestIndex_ComplexFilter_OrMixedFields(t *testing.T) {
	// Index on "a" only; "b" is not indexed
	coll := setupComplexFilterColl(t, anystore.IndexInfo{Fields: []string{"a"}})

	// {$or: [{a:1}, {b:2}]}
	// a=1 → 10 docs (i%10==1)
	// b=2 → ~14-15 docs (i%7==2)
	// overlap: docs where a=1 AND b=2 → i≡1(mod10), i≡2(mod7) → i=51 → 1 doc
	count, err := coll.Find(`{"$or":[{"a":1},{"b":2}]}`).Count(ctx)
	require.NoError(t, err)

	// Compare with no-index collection to get exact answer
	collNoIdx := setupComplexFilterColl(t)
	countNoIdx, err := collNoIdx.Find(`{"$or":[{"a":1},{"b":2}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count)
}

func TestIndex_ComplexFilter_ExistsWithSparseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"optional"}, Sparse: true}))

	// Insert 50 docs with "optional" field and 50 without
	for i := range 100 {
		var doc *anyenc.Value
		if i%2 == 0 {
			doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"optional":%d}`, i, i*10))
		} else {
			doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))
		}
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Query {optional: {$exists: true}} → 50 docs
	count, err := coll.Find(`{"optional":{"$exists":true}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, count)

	// Sparse index should only have entries for docs with the field
	indexes := coll.GetIndexes()
	require.Len(t, indexes, 1)
	idxLen, err := indexes[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, idxLen, "sparse index should only contain docs with the field")
}

// TestAudit08_CompoundArrayArray_QueryCount verifies that despite emitting 9
// raw index entries for a single doc, Count() correctly dedups and returns 1
// when the query matches via element-x-element (the inner Cartesian path).
func TestAudit08_CompoundArrayArray_QueryCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit08_count")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "ix_tags_cats",
		Fields: []string{"tags", "cats"},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
		`{"id":"d1","tags":["a","b"],"cats":["x","y"]}`,
	)))

	// {tags:"a", cats:"x"} matches the (a,x) Cartesian entry; doc must dedup to 1.
	n, err := coll.Find(`{"tags":"a","cats":"x"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "single doc must dedup to 1 despite 9 raw entries")
}

// TestAudit08_CompoundArrayArray_QueryIter verifies Iter() yields the doc
// exactly once for an element-x-element query, dedup notwithstanding the
// 9-entry fan-out.
func TestAudit08_CompoundArrayArray_QueryIter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit08_iter")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "ix_tags_cats",
		Fields: []string{"tags", "cats"},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
		`{"id":"d1","tags":["a","b"],"cats":["x","y"]}`,
	)))

	it, err := coll.Find(`{"tags":"a","cats":"x"}`).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	var ids []string
	for it.Next() {
		d, err := it.Doc()
		require.NoError(t, err)
		ids = append(ids, string(d.Value().GetStringBytes("id")))
	}
	require.NoError(t, it.Err())
	assert.Equal(t, []string{"d1"}, ids,
		"compound array x array: Iter must yield d1 exactly once")
}

// TestAudit08_CompoundArrayArray_TwoDocsOverlap exercises dedup when two
// documents share overlapping array elements. With $in over both array fields,
// each doc's compound entries get visited multiple times — but Count and Iter
// must still report each distinct document once.
func TestAudit08_CompoundArrayArray_TwoDocsOverlap(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit08_overlap")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "ix_tags_cats",
		Fields: []string{"tags", "cats"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"cats":["x","y"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"],"cats":["y","z"]}`),
	))

	// {tags:{$in:[b]}, cats:{$in:[y]}}: both d1 and d2 contain "b" in tags
	// and "y" in cats. Each must appear exactly once.
	n, err := coll.Find(`{"tags":{"$in":["b"]},"cats":{"$in":["y"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n, "both d1 and d2 match; Count must dedup to 2")

	it, err := coll.Find(`{"tags":{"$in":["b"]},"cats":{"$in":["y"]}}`).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	var ids []string
	for it.Next() {
		d, err := it.Doc()
		require.NoError(t, err)
		ids = append(ids, string(d.Value().GetStringBytes("id")))
	}
	require.NoError(t, it.Err())
	sort.Strings(ids)
	assert.Equal(t, []string{"d1", "d2"}, ids,
		"each doc must appear in Iter exactly once despite multiple matching index entries")
}

// ── Compound-index audit (act-43/44) ──
/*
Audit tests for the "compound-index" domain (any-store-tests:docs/any-store/qplanner/audit/actionable_by_domain.json).
Rewritten for the btree/v2 cost-based planner (the agent draft targeted SQLite EXPLAIN).

  act-43  Whole-tuple reverse of an all-ascending compound index IS realizable by a
          single reverse scan (no in-memory Sort) — distinct from the mixed-direction
          bug which now falls back to an in-memory sort.
  act-44  Equality on the first field + range on the second, with sort on the second:
          index-covered ascending or reverse, no in-memory sort.
*/

// act-43
func TestIndex_Compound_SortReversedIndex_WholeTuple(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))
	id := 0
	for a := 0; a < 3; a++ {
		for b := 0; b < 3; b++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, id, a, b))))
			id++
		}
	}
	pairs := func(sort ...any) []string {
		iter, err := coll.Find(nil).Sort(sort...).Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var out []string
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			out = append(out, fmt.Sprintf("%d,%d", d.Value().GetInt("a"), d.Value().GetInt("b")))
		}
		require.NoError(t, iter.Err())
		return out
	}
	assert.Equal(t, []string{"2,2", "2,1", "2,0", "1,2", "1,1", "1,0", "0,2", "0,1", "0,0"},
		pairs("-a", "-b"))

	explain, err := coll.Find(nil).Sort("-a", "-b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(a,b)")
	assert.Contains(t, explain.Sql, "(reverse)")
	assert.NotContains(t, explain.Sql, "-> Sort")
	assert.NotContains(t, explain.Sql, "TopK")
}

// act-44
func TestIndex_Compound_FilterAndRangeOnSecond(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))
	for a := 1; a <= 3; a++ {
		for b := 1; b <= 10; b++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, a*100+b, a, b))))
		}
	}

	// equality a + range b ($gte:5), sort b ascending → index-covered, no in-memory sort.
	assert.Equal(t, []string{"5", "6", "7", "8", "9", "10"},
		collectField(t, coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("b"), "b"))
	ex1, err := coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ex1.Sql, "IndexScan(a,b)")
	assert.NotContains(t, ex1.Sql, "-> Sort")
	c1, err := coll.Find(`{"a":2,"b":{"$gte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, c1)

	// sort b descending → reverse scan, still no in-memory sort.
	assert.Equal(t, []string{"10", "9", "8", "7", "6", "5"},
		collectField(t, coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("-b"), "b"))
	ex2, err := coll.Find(`{"a":2,"b":{"$gte":5}}`).Sort("-b").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ex2.Sql, "IndexScan(a,b)")
	assert.Contains(t, ex2.Sql, "(reverse)")
	assert.NotContains(t, ex2.Sql, "-> Sort")

	// upper-bound range.
	assert.Equal(t, []string{"1", "2", "3", "4"},
		collectField(t, coll.Find(`{"a":2,"b":{"$lt":5}}`).Sort("b"), "b"))
}

/*
Index/Planner tests inspired by SQLite: index3.test, index4.test
Test scenario:
Tests unique index constraints (compound, self-update, upsert, delete+reinsert,
bulk partial failure), sparse index behavior (missing fields, field
appearance via update, compound sparse), sparse+unique combinations,
index length tracking through mixed mutations, nested field unique indexes,
and drop-index followed by duplicate insert.
These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
