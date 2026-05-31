package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"github.com/anyproto/any-store/v2/query"
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

// --- from index_upsert_mutation_test.go ---

func TestIndex_UpsertMutation_UpsertOneInsert(t *testing.T) {
	// UpsertOne on a new document should create index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"counter"}}))

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

// TestAudit02_Reversibility_ArrayShrinksToSingleElement pins the per-doc
// reversibility claim documented at index.go:148-150:
//
//	"Reversible per-doc: an array shrinking from 3 elements to 1 next time
//	 round will see its single new entry written with IndexValueScalar."
//
// Setup: insert {tags:["a","b","c"]} → 4 entries (3 elements + whole-array),
// all IndexValueMultiKey because len(keysBuf) > 1 at insertKeys time.
//
// Then update to {tags:["x"]} (a single-element array). The update path
// (collection.update at collection.go:407) calls deleteKeys(prev) +
// insertKeys(new). After the update, only entries derived from the new
// value should remain.
//
// IMPORTANT — observed actual behaviour: a single-element array still
// produces TWO keysBuf entries inside writeValues (one for the element "x",
// one for the whole-array ["x"]). See audit_01 single-element-array case.
// So len(keysBuf) == 2 at insertKeys time, which means the "shrunk" entries
// are STILL written as IndexValueMultiKey. The doc claim's wording
// ("array shrinking from 3 elements to 1") therefore only delivers
// IndexValueScalar when the field type changes from array to scalar
// (covered by the next subtest), not when the array merely shrinks to a
// single element. Pinning the actually-observed behaviour here.
func TestAudit02_Reversibility_ArrayShrinksToSingleElement(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit02_shrink")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	// Initial insert: 3-element array → 4 entries (elements + whole-array),
	// every entry IndexValueMultiKey.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b","c"]}`)))

	before := readRawIndexEntries(t, fx.DB, "audit02_shrink", "ix_tags")
	require.Len(t, before, 4, "3-element array baseline must be 4 entries")
	for i, e := range before {
		require.NotEmptyf(t, e.Value, "before entry %d: value must not be empty", i)
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"before entry %d: 3-element array must be multi-key", i)
	}

	// Update: array shrinks to a single element. The update path will
	// deleteKeys(prev) — removing all 4 old entries — then insertKeys(new).
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["x"]}`)))

	after := readRawIndexEntries(t, fx.DB, "audit02_shrink", "ix_tags")
	// Observed: single-element array still emits 2 keysBuf entries
	// (element "x" + whole-array ["x"]). All old entries for "a","b","c"
	// + the old whole-array must be gone.
	require.Len(t, after, 2,
		"after shrink to single-element array: 2 entries (element + whole-array), "+
			"old multi-element entries must be deleted")
	for i, e := range after {
		require.NotEmptyf(t, e.Value, "after entry %d: value must not be empty", i)
		// Pinning observed behaviour: even though the array "shrunk", a
		// single-element array still produces len(keysBuf) == 2, so each
		// surviving entry is still tagged IndexValueMultiKey. The literal
		// reading of the doc claim ("entry written with IndexValueScalar")
		// therefore does NOT apply when shrinking to a 1-element array —
		// only when shrinking to a scalar field. Surprising but consistent
		// with audit_01_valuebyte_basic_test.go's single-element-array
		// pin.
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"after entry %d: single-element array still tagged multi-key "+
				"(len(keysBuf)==2 at insertKeys time)", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"after entry %d: multi-key bit must be set", i)
	}

	// Additionally verify none of the old element keys ("a","b","c") are
	// findable — proving deleteKeys ran cleanly.
	for _, gone := range []string{"a", "b", "c"} {
		count, qerr := coll.Find(`{"tags":"` + gone + `"}`).Count(ctx)
		require.NoError(t, qerr)
		assert.Equalf(t, 0, count, "old tag %q must be gone after update", gone)
	}
	count, qerr := coll.Find(`{"tags":"x"}`).Count(ctx)
	require.NoError(t, qerr)
	assert.Equal(t, 1, count, "new tag \"x\" must be findable")
}

// TestAudit02_Reversibility_ArrayShrinksToScalar exercises the actual
// reversibility path that produces IndexValueScalar: the field type changes
// from array (multi-key) to scalar (single key). This is the case the
// index.go:148-150 doc claim is really about.
//
// Setup: insert {tags:["a","b","c"]} → 4 multi-key entries.
// Update:  set tags to a plain scalar string "single".
// After:   exactly ONE entry, tagged IndexValueScalar (bit 0 cleared).
func TestAudit02_Reversibility_ArrayShrinksToScalar(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit02_to_scalar")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b","c"]}`)))

	before := readRawIndexEntries(t, fx.DB, "audit02_to_scalar", "ix_tags")
	require.Len(t, before, 4, "3-element array baseline must be 4 entries")
	for i, e := range before {
		require.NotEmptyf(t, e.Value, "before entry %d: value must not be empty", i)
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"before entry %d: 3-element array must be multi-key", i)
	}

	// Update: tags becomes a scalar string, NOT an array. writeValues
	// takes the non-array path → exactly one keysBuf entry → insertKeys
	// records IndexValueScalar.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":"single"}`)))

	after := readRawIndexEntries(t, fx.DB, "audit02_to_scalar", "ix_tags")
	require.Len(t, after, 1,
		"after shrink to scalar: exactly one entry must remain "+
			"(per-doc reversibility — old 4 multi-key entries deleted, new scalar entry written)")
	require.NotEmpty(t, after[0].Value, "surviving entry value must not be empty")
	assert.Equal(t, qplanner.IndexValueScalar, after[0].Value,
		"shrink-to-scalar must write IndexValueScalar (bit 0 cleared) — "+
			"this is the per-doc reversibility claim at index.go:148-150")
	assert.Zero(t, after[0].Value[0]&qplanner.IndexEntryFlagMultiKey,
		"multi-key flag bit must be cleared on the new scalar entry")

	// Old element keys gone, new scalar findable.
	for _, gone := range []string{"a", "b", "c"} {
		count, qerr := coll.Find(`{"tags":"` + gone + `"}`).Count(ctx)
		require.NoError(t, qerr)
		assert.Equalf(t, 0, count, "old tag %q must be gone after update", gone)
	}
	count, qerr := coll.Find(`{"tags":"single"}`).Count(ctx)
	require.NoError(t, qerr)
	assert.Equal(t, 1, count, "new scalar value \"single\" must be findable")
}

// TestAudit02_Reversibility_ScalarGrowsToArray covers the reverse direction
// of reversibility: a doc that was originally scalar (one entry tagged
// IndexValueScalar) gets updated to a multi-element array. The new entries
// must all be tagged IndexValueMultiKey AND the original scalar entry must
// be deleted (no stale entry left behind with the wrong bit).
func TestAudit02_Reversibility_ScalarGrowsToArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit02_to_array")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	// Initial: scalar string → exactly one entry, IndexValueScalar.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"tags":"x"}`)))

	before := readRawIndexEntries(t, fx.DB, "audit02_to_array", "ix_tags")
	require.Len(t, before, 1, "scalar baseline must be exactly 1 entry")
	require.NotEmpty(t, before[0].Value)
	assert.Equal(t, qplanner.IndexValueScalar, before[0].Value,
		"scalar baseline entry must be tagged IndexValueScalar")
	originalScalarKey := before[0].Key

	// Update: scalar → 2-element array. New keysBuf will have 2 element
	// entries + 1 whole-array entry = 3 entries, all IndexValueMultiKey.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"tags":["a","b"]}`)))

	after := readRawIndexEntries(t, fx.DB, "audit02_to_array", "ix_tags")
	require.Len(t, after, 3,
		"after scalar → 2-element array: 3 entries (2 elements + whole-array)")
	for i, e := range after {
		require.NotEmptyf(t, e.Value, "after entry %d: value must not be empty", i)
		assert.Equalf(t, qplanner.IndexValueMultiKey, e.Value,
			"after entry %d: every new array entry must be IndexValueMultiKey", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"after entry %d: multi-key bit must be set", i)
	}

	// Assert the original scalar entry's exact key is gone — proving
	// deleteKeys cleaned up the prior shape before insertKeys ran.
	for i, e := range after {
		assert.NotEqualf(t, originalScalarKey, e.Key,
			"after entry %d: original scalar entry key must have been deleted", i)
	}

	// Old scalar value "x" must no longer be findable except as part of
	// the new array — but since we replaced "x" with ["a","b"], a query
	// for tags:"x" should return 0.
	count, qerr := coll.Find(`{"tags":"x"}`).Count(ctx)
	require.NoError(t, qerr)
	assert.Equal(t, 0, count, "old scalar value \"x\" must be gone after update")

	for _, want := range []string{"a", "b"} {
		count, qerr := coll.Find(`{"tags":"` + want + `"}`).Count(ctx)
		require.NoError(t, qerr)
		assert.Equalf(t, 1, count, "new tag %q must be findable", want)
	}
}
