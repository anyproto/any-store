/*
Index/Planner tests inspired by SQLite: upsert and query mutation patterns

Test scenario:
Tests UpsertOne/UpsertId interactions with indexes, and Find().Update()/
Find().Delete() verifying that indexes remain consistent after mutations.
Covers upsert insert path, upsert update path, unique constraint enforcement
during upsert, compound index updates via upsert, and query-driven mutations
with index verification.

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
	"github.com/anyproto/any-store/query"
)

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
	// Unique constraint is enforced on Insert, not on Find().Update().
	// The update path (collection.update) does deleteKeys+insertKeys which
	// is a btree Put (overwrite), not a uniqueness-checked insert.
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
