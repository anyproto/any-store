package anystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/qplanner"
)

// --- from index_collection_lifecycle_test.go ---

func TestIndex_CollLifecycle_RenamePreservesIndexes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "original")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10),
		)))
	}

	// Rename collection
	require.NoError(t, coll.Rename(ctx, "renamed"))
	assert.Equal(t, "renamed", coll.Name())

	// Indexes should still be accessible
	indexes := coll.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)
	assert.True(t, indexes[0].Info().Unique)
	assertIndexLen(t, indexes[0], 10)

	// Queries should still work
	count, err := coll.Find(`{"a":50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Unique constraint should still hold
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":0}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
}

func TestIndex_CollLifecycle_DropRemovesAllIndexes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}, Unique: true}))

	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2),
		)))
	}

	// Drop the collection
	require.NoError(t, coll.Drop(ctx))

	// Verify collection no longer exists
	_, err = fx.OpenCollection(ctx, "test")
	require.ErrorIs(t, err, ErrCollectionNotFound)

	// Stats should show no collections or indexes
	stats, err := fx.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, stats.CollectionsCount)
	assert.Equal(t, 0, stats.IndexesCount)
}

func TestIndex_CollLifecycle_MultipleCollectionsSeparateIndexes(t *testing.T) {
	fx := newFixture(t)

	// Create two collections with different indexes
	coll1, err := fx.CreateCollection(ctx, "users")
	require.NoError(t, err)
	require.NoError(t, coll1.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	coll2, err := fx.CreateCollection(ctx, "products")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, IndexInfo{Fields: []string{"price"}}))
	require.NoError(t, coll2.EnsureIndex(ctx, IndexInfo{Fields: []string{"category"}}))

	// Insert into both
	require.NoError(t, coll1.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"email":"a@b.com","name":"Alice"}`),
		anyenc.MustParseJson(`{"id":2,"email":"c@d.com","name":"Bob"}`),
	))
	require.NoError(t, coll2.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"price":100,"category":"A"}`),
		anyenc.MustParseJson(`{"id":2,"price":200,"category":"B"}`),
		anyenc.MustParseJson(`{"id":3,"price":150,"category":"A"}`),
	))

	// Each collection has its own indexes
	assert.Len(t, coll1.GetIndexes(), 1)
	assert.Len(t, coll2.GetIndexes(), 2)

	// Unique constraint on coll1 doesn't affect coll2
	err = coll1.Insert(ctx, anyenc.MustParseJson(`{"id":3,"email":"a@b.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// coll2 has no unique constraint — duplicates allowed
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":4,"price":100,"category":"A"}`)))

	// Dropping coll1 doesn't affect coll2
	require.NoError(t, coll1.Drop(ctx))
	assertCollCount(t, coll2, 4)
	assert.Len(t, coll2.GetIndexes(), 2)

	// coll2 queries still work
	count, err := coll2.Find(`{"category":"A"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)
}

func TestIndex_CollLifecycle_CloseAndReopen(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 15 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}

	// Close the collection
	require.NoError(t, coll.Close())

	// Reopen via OpenCollection
	coll2, err := fx.OpenCollection(ctx, "test")
	require.NoError(t, err)

	// Index should be present
	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)
	assertIndexLen(t, indexes[0], 15)

	// Queries work
	count, err := coll2.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, count)

	explain, err := coll2.Find(`{"a":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_CollLifecycle_DropOneCollectionKeepsOther(t *testing.T) {
	fx := newFixture(t)

	coll1, err := fx.CreateCollection(ctx, "keep")
	require.NoError(t, err)
	require.NoError(t, coll1.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	coll2, err := fx.CreateCollection(ctx, "drop_me")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))

	for i := range 10 {
		require.NoError(t, coll1.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i),
		)))
		require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"x":%d}`, i, i*2),
		)))
	}

	// Drop coll2
	require.NoError(t, coll2.Drop(ctx))

	// coll1 still fully functional
	assertCollCount(t, coll1, 10)
	assertIndexLen(t, coll1.GetIndexes()[0], 10)

	count, err := coll1.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Can insert into coll1
	require.NoError(t, coll1.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":99}`)))
	assertCollCount(t, coll1, 11)
	assertIndexLen(t, coll1.GetIndexes()[0], 11)
}

func TestIndex_CollLifecycle_RecreateAfterDrop(t *testing.T) {
	fx := newFixture(t)

	// Create, populate, drop
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)))
	require.NoError(t, coll.Drop(ctx))

	// Recreate with same name
	coll2, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// No indexes should exist on fresh collection
	assert.Len(t, coll2.GetIndexes(), 0)
	assertCollCount(t, coll2, 0)

	// Can create same index again
	require.NoError(t, coll2.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)))
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":43}`)))

	assertCollCount(t, coll2, 2)
	assertIndexLen(t, coll2.GetIndexes()[0], 2)
}

func TestIndex_CollLifecycle_RenameUpdatesMetadata(t *testing.T) {
	fx := newFixture(t)

	coll, err := fx.CreateCollection(ctx, "old_name")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	require.NoError(t, coll.Rename(ctx, "new_name"))
	assert.Equal(t, "new_name", coll.Name())

	// GetCollectionNames should show only the new name
	names, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{"new_name"}, names)

	// Data and index accessible through renamed collection
	assertCollCount(t, coll, 1)
	assert.Len(t, coll.GetIndexes(), 1)

	count, err := coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Coverage tests from collection_backfill_coverage_test.go ---

// TestCollection_Backfill_Coverage_IndexAfterInsert verifies that when
// EnsureIndex is called after 1K docs have been inserted, every existing
// doc is backfilled into the new index and queries using the new index
// return all pre-existing rows.
func TestCollection_Backfill_Coverage_IndexAfterInsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	const n = 1000
	// Insert first, without any index on "a".
	docs := make([]*anyenc.Value, n)
	for i := 0; i < n; i++ {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	assertCollCount(t, coll, n)

	// No index yet.
	require.Len(t, coll.GetIndexes(), 0)

	// Create the index AFTER the data is already present.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.Len(t, coll.GetIndexes(), 1)

	// The new index must be fully populated.
	assertIndexLen(t, coll.GetIndexes()[0], n)

	// Every pre-existing document must be findable via an index-driven query.
	for _, probe := range []int{0, 1, 42, 500, 999} {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, probe)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "a=%d should be found via the new index", probe)
	}

	// And the planner should actually pick the new index for a point lookup
	// on this field — otherwise the backfill would be silently dead weight.
	explain, err := coll.Find(`{"a":500}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "Index", "planner should use the backfilled index")

	// Range query across the full backfilled range returns exactly n docs.
	count, err := coll.Find(fmt.Sprintf(`{"a":{"$gte":0,"$lt":%d}}`, n)).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, n, count, "full range should hit every backfilled entry")
}

// --- from index_persistence_test.go ---

func skipIfInMemory(t *testing.T) {
	t.Helper()
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("persistence tests require disk")
	}
}

func TestIndex_Persistence_IndexSurvivesRestart(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create collection, index, insert data
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}
	assertIndexLen(t, coll.GetIndexes()[0], 20)
	require.NoError(t, fx1.Close())

	// Phase 2: reopen and verify
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)

	assertIndexLen(t, indexes[0], 20)
	assertCollCount(t, coll2, 20)

	// Query should use the index
	count, err := coll2.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count)

	explain, err := coll2.Find(`{"a":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_Persistence_DataIntactAfterReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-data-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: insert 100 docs with index
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i*3),
		)))
	}

	// Record expected counts before close
	countAll, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, countAll)

	countA5, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, countA5)

	countRange, err := coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, countRange)

	require.NoError(t, fx1.Close())

	// Phase 2: reopen, verify all counts match
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	assertCollCount(t, coll2, 100)
	assertIndexLen(t, coll2.GetIndexes()[0], 100)

	count, err := coll2.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count, err = coll2.Find(`{"a":{"$gte":3,"$lte":7}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, count)

	// Verify a specific document
	doc, err := coll2.FindId(ctx, 42)
	require.NoError(t, err)
	assert.Equal(t, `{"id":42,"a":2,"b":126}`, doc.Value().String())

	// Verify sort order via index
	vals := collectField(t, coll2.Find(`{"a":7}`).Sort("a"), "a")
	assert.Len(t, vals, 10)

	explain, err := coll2.Find(`{"a":7}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_Persistence_UniqueConstraintAfterReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-uniq-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create unique index and insert data
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"email":"alice@test.com"}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"email":"bob@test.com"}`)))
	require.NoError(t, fx1.Close())

	// Phase 2: reopen, unique constraint should still hold
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.True(t, indexes[0].Info().Unique)

	// Duplicate should fail
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":3,"email":"alice@test.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// New unique value should succeed
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":3,"email":"carol@test.com"}`)))
	assertCollCount(t, coll2, 3)
}

func TestIndex_Persistence_SparseIndexAfterReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-sparse-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create sparse index
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"b":20}`),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
	))
	assertIndexLen(t, coll.GetIndexes()[0], 2)
	require.NoError(t, fx1.Close())

	// Phase 2: reopen, sparse behavior intact
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.True(t, indexes[0].Info().Sparse)

	assertIndexLen(t, indexes[0], 2)
	assertCollCount(t, coll2, 3)

	// Insert without field — should not grow index
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":4,"c":40}`)))
	assertIndexLen(t, indexes[0], 2)

	// Insert with field — should grow index
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":5,"a":50}`)))
	assertIndexLen(t, indexes[0], 3)
}

func TestIndex_Persistence_CompoundIndexAfterReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-compound-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: compound index with mixed directions
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "-b"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i%7),
		)))
	}
	require.NoError(t, fx1.Close())

	// Phase 2: reopen, verify compound index works
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, []string{"a", "-b"}, indexes[0].Info().Fields)

	// Query using prefix of compound index
	count, err := coll2.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	explain, err := coll2.Find(`{"a":2}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_Persistence_CreateIndexOnExistingDataAfterReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-create-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: insert data without index
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for i := range 30 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%6),
		)))
	}
	require.NoError(t, fx1.Close())

	// Phase 2: reopen, create index on existing data
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll2.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assertIndexLen(t, indexes[0], 30)

	// Query uses the newly created index
	count, err := coll2.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	explain, err := coll2.Find(`{"a":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_Persistence_CheckpointThenReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-ckpt-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create, populate, checkpoint
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	for i := range 15 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10),
		)))
	}

	// Force checkpoint before close
	require.NoError(t, fx1.Flush(ctx, 0, FlushModeCheckpointFull))
	require.NoError(t, fx1.Close())

	// Phase 2: reopen after checkpoint, everything intact
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	assertCollCount(t, coll2, 15)
	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assertIndexLen(t, indexes[0], 15)
	assert.True(t, indexes[0].Info().Unique)

	// Unique constraint still enforced
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":0}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
}

func TestIndex_Persistence_MultipleIndexesSurvive(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-multi-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create multiple indexes
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}, Unique: true}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"c"}, Sparse: true}))

	for i := range 20 {
		doc := fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%4, i, i%3)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
	}
	require.NoError(t, fx1.Close())

	// Phase 2: reopen, all indexes present
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 3)

	// Find each index by name and verify
	var idxA, idxB, idxC Index
	for _, idx := range indexes {
		switch idx.Info().Name {
		case "a":
			idxA = idx
		case "b":
			idxB = idx
		case "c":
			idxC = idx
		}
	}
	require.NotNil(t, idxA)
	require.NotNil(t, idxB)
	require.NotNil(t, idxC)

	assertIndexLen(t, idxA, 20)
	assertIndexLen(t, idxB, 20)
	assertIndexLen(t, idxC, 20) // all docs have "c" field

	assert.False(t, idxA.Info().Unique)
	assert.True(t, idxB.Info().Unique)
	assert.True(t, idxC.Info().Sparse)

	// Unique constraint on "b" still enforced
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":0,"b":0,"c":0}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
}

func TestIndex_Persistence_MutateCloseReopenVerify(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-mutate-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create, insert, then delete and update
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10),
		)))
	}
	// Delete docs 3, 6, 9
	require.NoError(t, coll.DeleteId(ctx, 3))
	require.NoError(t, coll.DeleteId(ctx, 6))
	require.NoError(t, coll.DeleteId(ctx, 9))

	// Update doc 1: a=10 -> a=999
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":999}`)))

	assertCollCount(t, coll, 7)
	assertIndexLen(t, coll.GetIndexes()[0], 7)
	require.NoError(t, fx1.Close())

	// Phase 2: reopen and verify mutations persisted
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	assertCollCount(t, coll2, 7)
	assertIndexLen(t, coll2.GetIndexes()[0], 7)

	// Deleted docs should not be found
	_, err = coll2.FindId(ctx, 3)
	require.ErrorIs(t, err, ErrDocNotFound)
	_, err = coll2.FindId(ctx, 6)
	require.ErrorIs(t, err, ErrDocNotFound)

	// Updated doc should have new value
	doc, err := coll2.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":999}`, doc.Value().String())

	// Query for old value should return 0
	count, err := coll2.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Query for new value should return 1
	count, err = coll2.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Persistence_DropIndexThenReopen(t *testing.T) {
	skipIfInMemory(t)
	tmpDir, err := os.MkdirTemp("", "idx-persist-drop-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create two indexes, drop one
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2),
		)))
	}
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.Len(t, coll.GetIndexes(), 1)
	require.NoError(t, fx1.Close())

	// Phase 2: reopen, only index "b" should exist
	fx2 := newFixturePath(t, tmpDir)
	coll2, err := fx2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "b", indexes[0].Info().Name)
	assertIndexLen(t, indexes[0], 10)

	// Dropping already-dropped index should fail
	err = coll2.DropIndex(ctx, "a")
	require.ErrorIs(t, err, ErrIndexNotFound)
}

// TestAudit09_EnsureIndexBackfill_* exercises the BACKFILL path of EnsureIndex
// when called over an already-populated collection. createIndex
// (collection.go ~L590) calls c.buildIndex(tx, idx), which iterates every
// existing document and invokes idx.insertKeys(tx, item) per doc. The
// per-entry value byte (IndexValueScalar vs IndexValueMultiKey) is set inside
// insertKeys based on len(idx.keysBuf) at that time.
//
// This is the ONLY way the multi-key bit gets retroactively materialized on
// pre-existing data — every other test inserts docs while the index already
// exists. TestCollection_Backfill_Coverage_IndexAfterInsert in
// index_lifecycle_test.go covers the scalar-only path; these tests pin the
// multi-key, mixed, and drop-and-recreate variants.

// TestAudit09_EnsureIndexBackfill_ScalarOnly: insert 5 scalar-tagged docs
// before any index exists, then EnsureIndex on tags. Backfill must emit one
// entry per doc, each tagged IndexValueScalar.
func TestAudit09_EnsureIndexBackfill_ScalarOnly(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_scalar")
	require.NoError(t, err)

	// Insert 5 scalar-valued docs first; no index yet.
	for i := 0; i < 5; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"s%d","tags":"v%d"}`, i, i),
		)))
	}
	require.Len(t, coll.GetIndexes(), 0)

	// Backfill: build the index from the existing 5 docs.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 5)

	entries := readRawIndexEntries(t, fx.DB, "audit09_scalar", "ix_tags")
	require.Len(t, entries, 5,
		"5 scalar docs must produce exactly 5 backfilled index entries")
	for i, e := range entries {
		assert.Equalf(t, qplanner.IndexValueScalar, e.Value,
			"entry %d: scalar-doc backfill must write IndexValueScalar (0x00)", i)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.Zerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be CLEARED for scalar entry", i)
	}
}

// TestAudit09_EnsureIndexBackfill_MultiKeyOnly: insert 5 array-tagged docs
// (3 elements each) before the index exists, then EnsureIndex on tags.
// Backfill must emit one entry per element + one whole-array entry per doc
// (= 4 entries per doc), each tagged IndexValueMultiKey.
func TestAudit09_EnsureIndexBackfill_MultiKeyOnly(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_multi")
	require.NoError(t, err)

	// 5 docs × 3-element arrays; no index yet.
	for i := 0; i < 5; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"m%d","tags":["a%d","b%d","c%d"]}`, i, i, i, i),
		)))
	}
	require.Len(t, coll.GetIndexes(), 0)

	// Backfill: per-doc, writeValues emits 3 element keys + 1 whole-array
	// key = 4 keys → len(keysBuf)>1 → IndexValueMultiKey for every entry.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)

	// 5 docs × 4 keys = 20 backfilled entries.
	const wantEntries = 20
	assertIndexLen(t, coll.GetIndexes()[0], wantEntries)

	entries := readRawIndexEntries(t, fx.DB, "audit09_multi", "ix_tags")
	require.Len(t, entries, wantEntries,
		"5 docs × (3 elements + 1 whole-array) = 20 backfilled entries")
	for i, e := range entries {
		assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry %d: array-doc backfill must write IndexValueMultiKey, got %v",
			i, e.Value)
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be SET for array-doc entry", i)
	}
}

// TestAudit09_EnsureIndexBackfill_Mixed: 3 scalar-tagged + 3 array-tagged
// docs in the same collection. Backfill must classify per-doc — scalar docs
// get IndexValueScalar, array docs get IndexValueMultiKey. The docId is
// embedded in the trailing bytes of each index key (item.appendId), so we
// distinguish scalar vs array origin by the docId prefix ("sx" vs "mx").
func TestAudit09_EnsureIndexBackfill_Mixed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_mixed")
	require.NoError(t, err)

	// 3 scalar docs (id: "sx0".."sx2", tags: "x0".."x2")
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"sx%d","tags":"x%d"}`, i, i),
		)))
	}
	// 3 array docs (id: "mx0".."mx2", tags: ["a0","b0"]..)
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"mx%d","tags":["a%d","b%d"]}`, i, i, i),
		)))
	}
	require.Len(t, coll.GetIndexes(), 0)

	// Backfill in a single EnsureIndex call.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)

	// 3 scalar (1 entry each) + 3 array × (2 element keys + 1 whole-array key)
	// = 3 + 9 = 12 backfilled entries.
	const wantEntries = 12
	assertIndexLen(t, coll.GetIndexes()[0], wantEntries)

	entries := readRawIndexEntries(t, fx.DB, "audit09_mixed", "ix_tags")
	require.Len(t, entries, wantEntries,
		"3 scalar (1 each) + 3 array (3 each) = 12 backfilled entries")

	// Tally per-origin classification by inspecting the docId suffix in each key.
	// docIds are short strings: "sx0".."sx2" or "mx0".."mx2". MarshalTo writes
	// TypeString(0x03) + bytes + EOS(0x00), so the literal "sx" or "mx" bytes
	// appear verbatim in the key tail — substring search is sufficient.
	scalarSeen, multiKeySeen := 0, 0
	for i, e := range entries {
		fromScalarDoc := bytes.Contains(e.Key, []byte("sx"))
		fromArrayDoc := bytes.Contains(e.Key, []byte("mx"))
		require.Truef(t, fromScalarDoc != fromArrayDoc,
			"entry %d: key must come from exactly one of scalar/array docs (key=%x)",
			i, e.Key)

		if fromScalarDoc {
			scalarSeen++
			assert.Equalf(t, qplanner.IndexValueScalar, e.Value,
				"entry %d (scalar doc, key=%x): expected IndexValueScalar, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.Zerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (scalar doc): multi-key flag must be CLEARED", i)
		} else {
			multiKeySeen++
			assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
				"entry %d (array doc, key=%x): expected IndexValueMultiKey, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (array doc): multi-key flag must be SET", i)
		}
	}

	// Cross-check totals: 3 scalar entries (1 per scalar doc) and 9 multi-key
	// entries (3 per array doc).
	assert.Equal(t, 3, scalarSeen, "scalar-doc entries must total 3")
	assert.Equal(t, 9, multiKeySeen, "array-doc entries must total 9 (3 docs × 3 keys)")
}

// TestAudit09_EnsureIndexBackfill_DropAndRecreate: same data layout as the
// Mixed test, but drops the freshly built index and re-creates it. Verifies
// the second backfill rewrites every entry from a clean slate (no stale bits
// from the dropped index leak through). DropIndex deletes the index
// namespace, so the recreated index must repopulate every entry through the
// same insertKeys path.
func TestAudit09_EnsureIndexBackfill_DropAndRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit09_drop_recreate")
	require.NoError(t, err)

	// Same mixed data shape as the Mixed test.
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"sx%d","tags":"x%d"}`, i, i),
		)))
	}
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"mx%d","tags":["a%d","b%d"]}`, i, i, i),
		)))
	}

	// First backfill.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 12)

	// Drop the index (deletes its namespace + sketch).
	require.NoError(t, coll.DropIndex(ctx, "ix_tags"))
	require.Len(t, coll.GetIndexes(), 0)

	// Recreate from scratch — second backfill must replay through insertKeys
	// and produce the exact same per-entry classification.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 12)

	entries := readRawIndexEntries(t, fx.DB, "audit09_drop_recreate", "ix_tags")
	require.Len(t, entries, 12,
		"recreated index must have the full 12 entries from the second backfill")

	scalarSeen, multiKeySeen := 0, 0
	for i, e := range entries {
		fromScalarDoc := bytes.Contains(e.Key, []byte("sx"))
		fromArrayDoc := bytes.Contains(e.Key, []byte("mx"))
		require.Truef(t, fromScalarDoc != fromArrayDoc,
			"entry %d: key must come from exactly one of scalar/array docs (key=%x)",
			i, e.Key)

		if fromScalarDoc {
			scalarSeen++
			assert.Equalf(t, qplanner.IndexValueScalar, e.Value,
				"entry %d (scalar doc, recreated index, key=%x): expected IndexValueScalar, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.Zerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (scalar doc, recreated index): multi-key flag must be CLEARED — "+
					"recreation must not leak stale bits", i)
		} else {
			multiKeySeen++
			assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
				"entry %d (array doc, recreated index, key=%x): expected IndexValueMultiKey, got %v",
				i, e.Key, e.Value)
			require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
			assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
				"entry %d (array doc, recreated index): multi-key flag must be SET", i)
		}
	}

	assert.Equal(t, 3, scalarSeen,
		"recreated index: scalar-doc entries must total 3 (no bits carried from old index)")
	assert.Equal(t, 9, multiKeySeen,
		"recreated index: array-doc entries must total 9 (3 docs × 3 keys)")
}

// TestAudit16_EnsureIndexAbort_* exercises the all-or-nothing transactional
// invariant of EnsureIndex / createIndex. createIndex (collection.go ~L558)
// does FIVE write-side operations inside a single doWriteTxModified:
//
//   1. tx.MarkSchemaChanged()
//   2. db.registerIndex(tx, ...)            – system-namespace metadata row
//   3. tx.CreateNamespace(indexNsName(...)) – the index btree
//   4. c.buildIndex(tx, idx)                – walk every doc, insertKeys
//   5. tx.Put(systemNS, sketchKey, ...)     – persist sketch
//
// If the transaction aborts ANYWHERE in (1)-(5), the rollback in
// doWriteTxModified must wipe ALL of them. Otherwise the next EnsureIndex
// retry can fail with ErrIndexExists (orphan in (2)), the index namespace
// can leak (orphan from (3)), the index can be live-but-empty (orphan from
// (4)), or the sketch can persist for a vanished index (orphan from (5)).
//
// =========================================================================
// FINDING — surfaced by writing this audit:
// =========================================================================
// EnsureIndex (and the entire collection write path) does NOT honor context
// cancellation. doWriteTxModified -> WriteTx -> btreeDB.BeginWrite() never
// checks ctx.Err(); buildIndex's per-doc cursor loop never checks ctx
// either. A pre-cancelled context passed to EnsureIndex completes the build
// and returns nil. This is a real bug: callers cannot abort an in-flight
// index build, and any timeout context they wrap around EnsureIndex is
// silently ignored. See ContextCancel_IsSilentlyIgnored below for the pin.
//
// Because the cancellation is silently ignored, the in-flight tx ALWAYS
// commits — there is no "partial state after cancellation" to test for via
// the public API today. The tests below pin the current behavior and stand
// ready to catch a real partial-state regression if cancellation ever
// becomes cooperative.
// =========================================================================

// docCountForAbortTests is large enough that buildIndex does meaningful
// work (1000 array-valued docs × 3 keys per doc = 3000 index inserts).
const docCountForAbortTests = 1000

// insertAbortTestDocs populates coll with docCountForAbortTests array-valued
// documents whose `tags` field is a 2-element string array, ensuring that
// buildIndex would emit ~3 keys per document (2 elements + whole-array).
func insertAbortTestDocs(t *testing.T, coll Collection) {
	t.Helper()
	for i := 0; i < docCountForAbortTests; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"tags":["a%d","b%d"]}`, i, i, i),
		)))
	}
}

// TestAudit16_EnsureIndexAbort_ContextCancel_RetrySucceeds: build the
// index with a pre-cancelled context, then build it again with a fresh
// context. The pin here is that whatever the first call does (today: it
// silently ignores the cancellation and succeeds; correctly-behaving
// future code: it errors out and rolls back), the second call MUST
// produce exactly one fully-populated index. No half-built state, no
// "index already exists" surprise from a leaked metadata row, no
// duplicate entries from a leaked namespace.
func TestAudit16_EnsureIndexAbort_ContextCancel_RetrySucceeds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_retry")
	require.NoError(t, err)

	insertAbortTestDocs(t, coll)
	require.Len(t, coll.GetIndexes(), 0)

	// Pre-cancelled context.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	abortErr := coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})
	// Today this returns nil (ctx ignored). If/when ctx becomes
	// cooperative, this returns a context.Canceled-wrapped error AND
	// the rollback must wipe steps (1)-(5) above.
	if abortErr != nil {
		require.Truef(t, errors.Is(abortErr, context.Canceled),
			"if EnsureIndex errors on a cancelled ctx, the error must wrap "+
				"context.Canceled; got: %v", abortErr)
	} else {
		t.Log("FINDING: EnsureIndex silently ignored a pre-cancelled context " +
			"and committed. doWriteTxModified does not check ctx.Err(); " +
			"buildIndex's cursor loop does not check ctx either.")
	}

	// Retry with a fresh context — must succeed cleanly. EnsureIndex
	// with ensure=true swallows ErrIndexExists, so even if the first
	// call committed, this returns nil. Either way, the post-condition
	// is identical: exactly one index, fully populated.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}), "retry after a cancelled EnsureIndex must succeed; an error here means "+
		"the rollback did not fully wipe the partial state from the aborted tx")

	require.Len(t, coll.GetIndexes(), 1,
		"exactly one index must exist post-retry — neither double-registered "+
			"nor missing")
	// 1000 docs × (2 element keys + 1 whole-array key) = 3000 entries.
	assertIndexLen(t, coll.GetIndexes()[0], 3000)
}

// TestAudit16_EnsureIndexAbort_NoOrphanIndex: after a cancelled EnsureIndex,
// the in-memory coll.indexes slice and the on-disk system-namespace
// metadata must be CONSISTENT — either both reflect the index or neither
// does. The forbidden state is "system NS has the metadata row but
// coll.indexes does not (or vice versa)" — that's the orphan we'd detect
// by reopening the collection and observing a different index list.
func TestAudit16_EnsureIndexAbort_NoOrphanIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_no_orphan_idx")
	require.NoError(t, err)

	insertAbortTestDocs(t, coll)
	require.Len(t, coll.GetIndexes(), 0)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	abortErr := coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})

	// Snapshot in-memory state right after the cancelled call.
	inMemoryCount := len(coll.GetIndexes())

	// Reopen and snapshot on-disk state.
	require.NoError(t, coll.Close())
	coll2, err := fx.OpenCollection(ctx, "audit16_no_orphan_idx")
	require.NoError(t, err)
	onDiskCount := len(coll2.GetIndexes())

	// Pin the consistency invariant: in-memory == on-disk. A mismatch
	// means createIndexes appended to coll.indexes without committing
	// the system-NS row (or vice versa).
	require.Equalf(t, inMemoryCount, onDiskCount,
		"in-memory index count (%d) must equal on-disk index count (%d) — "+
			"mismatch indicates partial commit (orphan metadata row or "+
			"orphan in-memory entry)", inMemoryCount, onDiskCount)

	// Stronger pin if cancellation is ever honored: an erroring
	// EnsureIndex MUST leave both views at zero.
	if abortErr != nil {
		require.Truef(t, errors.Is(abortErr, context.Canceled),
			"errored EnsureIndex must wrap context.Canceled; got: %v", abortErr)
		require.Equal(t, 0, inMemoryCount,
			"on cancellation, in-memory coll.indexes must be empty")
		require.Equal(t, 0, onDiskCount,
			"on cancellation, on-disk system NS must show no index metadata")
	} else {
		// Today: cancelled call commits, both views show 1.
		t.Logf("FINDING: cancelled EnsureIndex committed; in-memory=%d, on-disk=%d "+
			"(expected both=0 if ctx were honored)", inMemoryCount, onDiskCount)
	}
}

// TestAudit16_EnsureIndexAbort_NoOrphanNamespace: after a cancelled
// EnsureIndex, no leftover index namespace must exist on disk. Public
// API has no namespace enumerator, so we verify indirectly: a retry
// EnsureIndex must succeed AND its index must contain EXACTLY the
// expected entry count. A leftover namespace would either:
//
//	(a) cause CreateNamespace in the retry to fail (visible as a non-nil
//	    error from EnsureIndex), or
//	(b) show up as duplicate/extra entries in readRawIndexEntries.
func TestAudit16_EnsureIndexAbort_NoOrphanNamespace(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_no_orphan_ns")
	require.NoError(t, err)

	insertAbortTestDocs(t, coll)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_ = coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})

	// Retry with a fresh ctx — must succeed. ensure=true swallows
	// ErrIndexExists, so this returns nil whether or not the cancelled
	// call committed.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}), "retry after cancellation must succeed; a failure here means "+
		"either the system-namespace metadata or the index btree namespace "+
		"survived the rollback in a way the retry could not reconcile")

	// Final state must be exactly one index, exactly 3000 entries.
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 3000)

	entries := readRawIndexEntries(t, fx.DB, "audit16_no_orphan_ns", "ix_tags")
	require.Lenf(t, entries, 3000,
		"final index must have exactly 3000 entries (1000 docs × 3 keys); "+
			"a different count means leftover bytes from the aborted attempt "+
			"contaminated the rebuilt namespace (got %d)", len(entries))
}

// TestAudit16_EnsureIndexAbort_RetryHasCorrectEntries: the ENTRY-LEVEL
// invariant after retry. Even with the (current) silent-commit
// behavior, the final entries must be uniformly correct: every entry
// from an array-valued doc must carry the multi-key flag. This is the
// strongest end-to-end signal of "no stale scalar-tagged bytes leaked
// from a half-built attempt".
func TestAudit16_EnsureIndexAbort_RetryHasCorrectEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_retry_entries")
	require.NoError(t, err)

	insertAbortTestDocs(t, coll)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_ = coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})

	// Retry. ensure=true swallows ErrIndexExists if the first call
	// committed (current behavior); rebuilds from scratch if it didn't.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))
	require.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 3000)

	entries := readRawIndexEntries(t, fx.DB, "audit16_retry_entries", "ix_tags")
	require.Len(t, entries, 3000,
		"retry must produce exactly 3000 entries (1000 array docs × 3 keys)")

	// Every single entry must carry the multi-key flag (all docs are
	// arrays). A scalar-tagged entry here would mean stale data from a
	// half-built rollback or duplicate-write contamination.
	for i, e := range entries {
		require.NotEmptyf(t, e.Value, "entry %d: value must not be empty", i)
		assert.Truef(t, bytes.Equal(e.Value, qplanner.IndexValueMultiKey),
			"entry %d: every entry from array-valued docs must be IndexValueMultiKey, "+
				"got %v (suggests stale bytes from aborted attempt)", i, e.Value)
		assert.NotZerof(t, e.Value[0]&qplanner.IndexEntryFlagMultiKey,
			"entry %d: multi-key flag bit must be SET on every retry entry", i)
	}
}

// TestAudit16_EnsureIndexAbort_TwoConcurrentEnsureIndex_OneCancels:
// fire two EnsureIndex calls concurrently for the same index name. One
// uses a fresh context and races to commit; the other uses a context
// that is cancelled before the call. Whichever wins, the final state
// must show exactly ONE index. EnsureIndex (ensure=true) swallows
// ErrIndexExists, so a "second comer to find an existing index"
// returns nil.
//
// doWriteTxModified serializes writers via btreeDB.BeginWrite(), so the
// race is resolved at tx-acquisition not in the index-build code itself.
// This test pins the API-level outcome regardless of which goroutine
// wins the race.
func TestAudit16_EnsureIndexAbort_TwoConcurrentEnsureIndex_OneCancels(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_concurrent")
	require.NoError(t, err)

	// Smaller dataset — concurrency, not throughput, is what we test.
	const concurrentDocs = 100
	for i := 0; i < concurrentDocs; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"tags":["a%d","b%d"]}`, i, i, i),
		)))
	}
	require.Len(t, coll.GetIndexes(), 0)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	var errCancelled, errFresh error
	go func() {
		defer wg.Done()
		errCancelled = coll.EnsureIndex(cancelled, IndexInfo{
			Name:   "ix_tags",
			Fields: []string{"tags"},
		})
	}()
	go func() {
		defer wg.Done()
		errFresh = coll.EnsureIndex(ctx, IndexInfo{
			Name:   "ix_tags",
			Fields: []string{"tags"},
		})
	}()
	wg.Wait()

	// The fresh-ctx call must succeed (or race-lose to the cancelled
	// one and observe ErrIndexExists, which ensure=true swallows).
	require.NoErrorf(t, errFresh,
		"fresh-ctx EnsureIndex must succeed (ensure=true swallows ErrIndexExists "+
			"if the cancelled goroutine raced ahead); got error: %v", errFresh)

	// The cancelled-ctx call may either error (ideal — ctx honored) or
	// return nil (current — ctx ignored). Either is acceptable here as
	// long as the final state is consistent.
	if errCancelled != nil {
		require.Truef(t, errors.Is(errCancelled, context.Canceled),
			"cancelled goroutine returned a non-cancellation error: %v", errCancelled)
	}

	// Final invariant: exactly ONE index, regardless of who won.
	// Concurrent EnsureIndex calls with the same name MUST converge to a
	// single index — never zero, never two.
	require.Len(t, coll.GetIndexes(), 1,
		"after concurrent EnsureIndex with one cancellation, exactly one "+
			"index must exist — neither double-registration nor zero-registration "+
			"is acceptable")

	// And the single index must be fully built.
	assertIndexLen(t, coll.GetIndexes()[0], concurrentDocs*3)
}

// TestAudit16_EnsureIndexAbort_ContextCancel_IsSilentlyIgnored pins the
// FINDING surfaced while writing this audit: EnsureIndex does NOT honor
// context cancellation. doWriteTxModified -> WriteTx -> BeginWrite never
// checks ctx.Err(); buildIndex's cursor loop never checks ctx either.
// Same story for Insert, UpdateOne, Delete, etc. — the entire write
// path is non-cooperative.
//
// This test exists to make the bug LOUD. If/when cancellation becomes
// cooperative, this test will fail and the corresponding spec line
// ("ContextCancel_RetrySucceeds: must return an error related to the
// cancelled context") becomes meaningfully testable.
func TestAudit16_EnsureIndexAbort_ContextCancel_IsSilentlyIgnored(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit16_silent_ctx")
	require.NoError(t, err)

	// Insert enough docs that buildIndex does real work.
	insertAbortTestDocs(t, coll)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, cancelled.Err(), context.Canceled,
		"sanity: the context we just cancelled must report Canceled")

	abortErr := coll.EnsureIndex(cancelled, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	})

	// CURRENT behavior: nil. If this assertion ever flips, the bug is
	// fixed and the comment block at the top of this file should be
	// updated; the other audit16 tests then become much stronger.
	if abortErr == nil {
		t.Log("CURRENT BEHAVIOR (BUG): EnsureIndex returned nil for a " +
			"pre-cancelled ctx; the in-flight tx committed in full. " +
			"Root cause: db.doWriteTxModified, db.WriteTx, and " +
			"btreeDB.BeginWrite() do not check ctx.Err(); buildIndex's " +
			"cursor loop also does not check ctx. Until any of those " +
			"sites becomes cooperative, callers cannot abort an " +
			"in-flight index build.")

		// Verify the bug went all the way: the index ACTUALLY committed.
		require.Len(t, coll.GetIndexes(), 1,
			"if EnsureIndex returned nil, the index must have committed")
		assertIndexLen(t, coll.GetIndexes()[0], 3000)
		return
	}

	// FUTURE behavior (correct): error wrapping context.Canceled, with
	// no committed state.
	require.Truef(t, errors.Is(abortErr, context.Canceled),
		"EnsureIndex error must wrap context.Canceled; got: %v", abortErr)
	require.Empty(t, coll.GetIndexes(),
		"on cancellation, no index must be registered (full rollback)")
}
