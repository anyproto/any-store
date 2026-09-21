package test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- from index_collection_lifecycle_test.go ---

func TestIndex_CollLifecycle_RenamePreservesIndexes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "original")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))

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
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)
}

func TestIndex_CollLifecycle_DropRemovesAllIndexes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}, Unique: true}))

	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2),
		)))
	}

	// Drop the collection
	require.NoError(t, coll.Drop(ctx))

	// Verify collection no longer exists
	_, err = fx.OpenCollection(ctx, "test")
	require.ErrorIs(t, err, anystore.ErrCollectionNotFound)

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
	require.NoError(t, coll1.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))

	coll2, err := fx.CreateCollection(ctx, "products")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"price"}}))
	require.NoError(t, coll2.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"category"}}))

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
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll1.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	coll2, err := fx.CreateCollection(ctx, "drop_me")
	require.NoError(t, err)
	require.NoError(t, coll2.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"x"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)))
	require.NoError(t, coll.Drop(ctx))

	// Recreate with same name
	coll2, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// No indexes should exist on fresh collection
	assert.Len(t, coll2.GetIndexes(), 0)
	assertCollCount(t, coll2, 0)

	// Can create same index again
	require.NoError(t, coll2.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)))
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":43}`)))

	assertCollCount(t, coll2, 2)
	assertIndexLen(t, coll2.GetIndexes()[0], 2)
}

func TestIndex_CollLifecycle_RenameUpdatesMetadata(t *testing.T) {
	fx := newFixture(t)

	coll, err := fx.CreateCollection(ctx, "old_name")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
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

func TestIndex_Persistence_IndexSurvivesRestart(t *testing.T) {
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create collection, index, insert data
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-data-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: insert 100 docs with index
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-uniq-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create unique index and insert data
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))

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
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)

	// New unique value should succeed
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(`{"id":3,"email":"carol@test.com"}`)))
	assertCollCount(t, coll2, 3)
}

func TestIndex_Persistence_SparseIndexAfterReopen(t *testing.T) {
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-sparse-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create sparse index
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Sparse: true}))

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
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-compound-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: compound index with mixed directions
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "-b"}}))

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
	skipIfInMemory(t, "the database is closed and reopened")
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

	require.NoError(t, coll2.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
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
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-ckpt-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create, populate, checkpoint
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))

	for i := range 15 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10),
		)))
	}

	// Force checkpoint before close
	require.NoError(t, fx1.Flush(ctx, 0, anystore.FlushModeCheckpointFull))
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
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)
}

func TestIndex_Persistence_MultipleIndexesSurvive(t *testing.T) {
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-multi-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create multiple indexes
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}, Unique: true}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"c"}, Sparse: true}))

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
	var idxA, idxB, idxC anystore.Index
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
	require.ErrorIs(t, err, anystore.ErrUniqueConstraint)
}

func TestIndex_Persistence_MutateCloseReopenVerify(t *testing.T) {
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-mutate-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create, insert, then delete and update
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.ErrorIs(t, err, anystore.ErrDocNotFound)
	_, err = coll2.FindId(ctx, 6)
	require.ErrorIs(t, err, anystore.ErrDocNotFound)

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
	skipIfInMemory(t, "the database is closed and reopened")
	tmpDir, err := os.MkdirTemp("", "idx-persist-drop-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Phase 1: create two indexes, drop one
	fx1 := newFixturePath(t, tmpDir)
	coll, err := fx1.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

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
	require.ErrorIs(t, err, anystore.ErrIndexNotFound)
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
func insertAbortTestDocs(t *testing.T, coll anystore.Collection) {
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

	abortErr := coll.EnsureIndex(cancelled, anystore.IndexInfo{
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
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
		errCancelled = coll.EnsureIndex(cancelled, anystore.IndexInfo{
			Name:   "ix_tags",
			Fields: []string{"tags"},
		})
	}()
	go func() {
		defer wg.Done()
		errFresh = coll.EnsureIndex(ctx, anystore.IndexInfo{
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

	abortErr := coll.EnsureIndex(cancelled, anystore.IndexInfo{
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

// explainSQL returns explain.Sql for a hinted query on the "a" index.
func explainSQL(t *testing.T, coll anystore.Collection, filter string) string {
	t.Helper()
	explain, err := coll.Find(filter).
		IndexHint(anystore.IndexHint{IndexName: "a", Boost: 1_000_000}).Explain(ctx)
	require.NoError(t, err)
	return explain.Sql
}

// assertTightBounds asserts the hinted index scan carries BOTH range ends
// (no ",inf]" upper bound in its bounds string).
func assertTightBounds(t *testing.T, coll anystore.Collection, filter string) {
	t.Helper()
	sql := explainSQL(t, coll, filter)
	require.Contains(t, sql, "IndexScan(a)", "plan: %s", sql)
	assert.Contains(t, sql, "'5')", "expected tight (two-sided) bounds, got: %s", sql)
}

// assertWideBounds asserts the hinted index scan kept the sound half-open
// over-approximation (upper bound +inf).
func assertWideBounds(t *testing.T, coll anystore.Collection, filter string) {
	t.Helper()
	sql := explainSQL(t, coll, filter)
	require.Contains(t, sql, "IndexScan(a)", "plan: %s", sql)
	assert.Contains(t, sql, "'<string>')", "expected wide (bracket-open) bounds, got: %s", sql)
}

const twoSided = `{"a":{"$gt":2,"$lt":5}}`

// TestMultikeyFlag_PkTightBounds: the pk namespace needs no flag — array pks
// are rejected on write, so tight idBounds are unconditional.
func TestMultikeyFlag_PkTightBounds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "pk")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}
	explain, err := coll.Find(`{"id":{"$gt":2,"$lt":5}}`).Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "idBounds", "plan: %s", explain.Sql)
	assert.NotContains(t, explain.Sql, "inf]", "pk range must carry both ends: %s", explain.Sql)
	assertQueryCount(t, coll.Find(`{"id":{"$gt":2,"$lt":5}}`), 2)

	// Descending pk range with limit — the seek must start at the End key,
	// not the last key of the collection.
	it, err := coll.Find(`{"id":{"$gt":2,"$lt":5}}`).Sort("-id").Limit(1).Iter(ctx)
	require.NoError(t, err)
	require.True(t, it.Next())
	d, err := it.Doc()
	require.NoError(t, err)
	assert.Equal(t, 4, d.Value().GetInt("id"))
	require.NoError(t, it.Close())
}

// TestMultikeyFlag_ChannelConsistency pins the "one bounds set per chain"
// invariant on the paths where mixing tight flags with wide execution (or
// vice versa) produces wrong rows rather than just bad plans.
func TestMultikeyFlag_ChannelConsistency(t *testing.T) {
	t.Run("unique multikey index point-collapse must not CoverIter", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "uniqmk")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}, Unique: true}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":[4,6]}`)))

		// {$gte:5,$lte:5} tight-collapses to the point [5,5]; if PointLookup
		// were computed from the tight channel while the index (multikey!)
		// executes wide — or if tight bounds leaked into the seek — the doc
		// (matching via 6>=5 and 4<=5, with no entry at 5) would vanish.
		const f = `{"a":{"$gte":5,"$lte":5}}`
		hint := anystore.IndexHint{IndexName: "a", Boost: 1_000_000}
		assertQueryCount(t, coll.Find(f).IndexHint(hint), 1)
		it, err := coll.Find(f).IndexHint(hint).Iter(ctx)
		require.NoError(t, err)
		n := 0
		for it.Next() {
			n++
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, 1, n)
	})

	t.Run("compound multikey point-collapse must keep the SortIter", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "compmk")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "ab", Fields: []string{"a", "b"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"a":[4,6],"b":1}`),
			anyenc.MustParseJson(`{"id":2,"a":5,"b":2}`),
			anyenc.MustParseJson(`{"id":3,"a":[5,9],"b":0}`),
		))
		// All three match {$gte:5,$lte:5} under array semantics. A tight
		// equalityPrefix on a multikey index would claim ExactSort on b and
		// skip the SortIter while executing wide (a,b)-ordered bounds.
		const f = `{"a":{"$gte":5,"$lte":5}}`
		hint := anystore.IndexHint{IndexName: "ab", Boost: 1_000_000}
		it, err := coll.Find(f).IndexHint(hint).Sort("b").Iter(ctx)
		require.NoError(t, err)
		var got []int
		for it.Next() {
			d, derr := it.Doc()
			require.NoError(t, derr)
			got = append(got, d.Value().GetInt("b"))
		}
		require.NoError(t, it.Err())
		require.NoError(t, it.Close())
		assert.Equal(t, []int{0, 1, 2}, got)
	})

	t.Run("verify chain reads the wide channel of the verify index", func(t *testing.T) {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "verifymk")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx,
			anystore.IndexInfo{Name: "a", Fields: []string{"a"}},
			anystore.IndexInfo{Name: "b", Fields: []string{"b"}},
		))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1,"b":[2,4]}`)))

		// Candidate index a is scalar-proven; b's tight bounds collapse to
		// [3,3]. A verify probe against b's (multikey) index keyed on that
		// point would miss the doc's entries (2, 4, whole-array).
		const f = `{"a":1,"b":{"$gte":3,"$lte":3}}`
		hint := anystore.IndexHint{IndexName: "a", Boost: 1_000_000}
		assertQueryCount(t, coll.Find(f).IndexHint(hint), 1)
	})
}

// TestQuery_TightSeekDifferential is the randomized oracle: for scalar-only
// and array-bearing datasets alike, indexed queries (tight seeks where proven)
// must agree with a full in-memory evaluation of the filter.
func TestQuery_TightSeekDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	type dataset struct {
		name   string
		arrays bool
	}
	for _, ds := range []dataset{{"scalar", false}, {"arrays", true}} {
		t.Run(ds.name, func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "diff")
			require.NoError(t, err)
			require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))

			var docs []*anyenc.Value
			for i := 0; i < 300; i++ {
				var doc string
				if ds.arrays && i%3 == 0 {
					doc = fmt.Sprintf(`{"id":%d,"a":[%d,%d]}`, i, rng.Intn(50), rng.Intn(50))
				} else {
					doc = fmt.Sprintf(`{"id":%d,"a":%d}`, i, rng.Intn(50))
				}
				docs = append(docs, anyenc.MustParseJson(doc))
			}
			require.NoError(t, coll.Insert(ctx, docs...))

			var filters []string
			for i := 0; i < 40; i++ {
				lo, hi := rng.Intn(50), rng.Intn(50)
				switch i % 5 {
				case 0:
					filters = append(filters, fmt.Sprintf(`{"a":{"$gt":%d,"$lt":%d}}`, lo, hi))
				case 1:
					filters = append(filters, fmt.Sprintf(`{"a":{"$gte":%d,"$lte":%d}}`, lo, hi))
				case 2:
					filters = append(filters, fmt.Sprintf(`{"a":{"$in":[%d,%d,%d],"$gt":%d}}`, lo, hi, rng.Intn(50), rng.Intn(50)))
				case 3:
					filters = append(filters, fmt.Sprintf(`{"a":{"$gt":%d,"$lt":%d,"$ne":%d}}`, lo, hi, rng.Intn(50)))
				case 4:
					filters = append(filters, fmt.Sprintf(`{"id":{"$gt":%d,"$lt":%d}}`, lo, hi))
				}
			}

			hint := anystore.IndexHint{IndexName: "a", Boost: 1_000_000}
			for _, f := range filters {
				cond := query.MustParseCondition(f)
				var want []int
				for _, d := range docs {
					if cond.Ok(d, nil) {
						want = append(want, d.GetInt("id"))
					}
				}
				slices.Sort(want)

				for _, q := range []anystore.Query{coll.Find(f), coll.Find(f).IndexHint(hint)} {
					it, err := q.Iter(ctx)
					require.NoError(t, err)
					var got []int
					for it.Next() {
						d, derr := it.Doc()
						require.NoError(t, derr)
						got = append(got, d.Value().GetInt("id"))
					}
					require.NoError(t, it.Err())
					require.NoError(t, it.Close())
					slices.Sort(got)
					require.Equal(t, want, got, "Iter mismatch for %s", f)
				}
				assertQueryCount(t, coll.Find(f), len(want))
				assertQueryCount(t, coll.Find(f).IndexHint(hint), len(want))

				// Sorted + limited variants must return prefixes of the
				// ordered match set.
				it, err := coll.Find(f).IndexHint(hint).Sort("a").Limit(3).Iter(ctx)
				require.NoError(t, err)
				n := 0
				for it.Next() {
					n++
				}
				require.NoError(t, it.Err())
				require.NoError(t, it.Close())
				require.Equal(t, min(3, len(want)), n, "Sort+Limit count mismatch for %s", f)
			}
		})
	}
}

// TestMultikeyFlag_ExplainShowsBothEnds is the explain-level dropped-range-end
// acceptance: both bounds ends survive to the executed plan for the pk and a
// scalar secondary index.
func TestMultikeyFlag_ExplainShowsBothEnds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "ends")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	for i := 0; i < 20; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	sql := explainSQL(t, coll, `{"a":{"$gt":5,"$lt":9}}`)
	require.Contains(t, sql, "IndexScan(a)")
	require.False(t, strings.Contains(sql, "inf"), "secondary-index bounds must be two-sided: %s", sql)

	explain, err := coll.Find(`{"id":{"$gt":5,"$lt":9}}`).Sort("-id").Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "idBounds", "plan: %s", explain.Sql)
	require.False(t, strings.Contains(explain.Sql, "inf"), "pk bounds must be two-sided: %s", explain.Sql)
}

// TestMultikeyFlag_VerifyChainResidualPredicates: tight bounds can collapse a
// multi-conjunct field ({$gte:5,$lte:5,$nin:[5]}) to a point, flipping
// PointLookup=true — but the CountOnly verify chain skips the residual
// FilterIter, so a bound-LESS conjunct on the covered field ($nin, $type)
// would silently vanish from the count. The verify-chain gate must mirror
// indexCoversFilter's one-predicate-per-bounded-field rule.
func TestMultikeyFlag_VerifyChainResidualPredicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "verifyresid")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx,
		anystore.IndexInfo{Name: "a", Fields: []string{"a"}},
		anystore.IndexInfo{Name: "b", Fields: []string{"b"}},
	))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":5,"b":1}`)))

	hint := anystore.IndexHint{IndexName: "a", Boost: 1_000_000}
	for _, tc := range []struct {
		filter string
		want   int
	}{
		{`{"a":{"$gte":5,"$lte":5,"$nin":[5]},"b":1}`, 0},
		{`{"a":{"$gte":5,"$lte":5,"$type":"string"},"b":1}`, 0},
		{`{"a":{"$gte":5,"$lte":5},"b":1}`, 1},
	} {
		t.Run(tc.filter, func(t *testing.T) {
			it, err := coll.Find(tc.filter).IndexHint(hint).Iter(ctx)
			require.NoError(t, err)
			n := 0
			for it.Next() {
				n++
			}
			require.NoError(t, it.Err())
			require.NoError(t, it.Close())
			require.Equal(t, tc.want, n, "Iter")
			assertQueryCount(t, coll.Find(tc.filter).IndexHint(hint), tc.want)
		})
	}
}

// TestMultikeyFlag_ScanCostPricedFromSeekBounds: an unproven (multikey-
// flagged) index seeks WIDE even though its tight bounds are narrow. The
// scan-cost estimate must price the wide seek — charging the tight fraction
// picks an index seek that still walks half the index. 1000 scalar docs plus
// one array doc (inserted and deleted: the flag is sticky) — a narrow
// two-sided mid-range query must stay on the full scan, because the real
// index seek would visit ~90% of the entries.
func TestMultikeyFlag_ScanCostPricedFromSeekBounds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "seekprice")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	docs := make([]*anyenc.Value, 0, 1000)
	for i := 0; i < 1000; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":9999,"a":[1,2]}`)))
	require.NoError(t, coll.DeleteId(ctx, 9999))

	explain, err := coll.Find(`{"a":{"$gt":100,"$lt":105}}`).Explain(ctx)
	require.NoError(t, err)
	require.Contains(t, explain.Sql, "FullScan",
		"an unproven index seeking wide must not be priced at the tight fraction: %s\n%s",
		explain.Sql, explain.Plan)
}

// These tests guard the DDL visibility gate (visibleIndexes / forTx / the fts
// Search gate): createIndexes publishes new handles into the shared CoW
// snapshot at EXECUTION time — same-tx writes must maintain the new index —
// but their namespaces exist only in the creating write tx's uncommitted
// view. A concurrent tx that planned with such a handle would scan an empty
// namespace and return wrong results with no error (Count = 0 while Iter,
// picking another plan, finds the rows). The gate keeps pending handles
// invisible to every tx but the creator's until the commit publication.

func explainHasIndex(e anystore.Explain, name string) bool {
	for _, ie := range e.Indexes {
		if ie.Name == name {
			return true
		}
	}
	return false
}

func TestCreateIndexUncommitted_ConcurrentReaderConsistent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		name := "other"
		if i < 30 {
			name = "x"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"name":%q}`, i, name))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{Fields: []string{"name"}}))

	const filter = `{"name":"x"}`

	// Concurrent reader during the uncommitted window: Count and Iter agree
	// on the correct answer, and the pending index is not even a candidate.
	// The boost hint pins the planner to the index IF it is a candidate —
	// pre-gate that forced the empty-namespace seek and Count returned 0.
	hint := anystore.IndexHint{IndexName: "name", Boost: 1_000_000}
	cnt, err := coll.Find(filter).IndexHint(hint).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	ids, _ := collectIter(t, coll.Find(filter))
	assert.Len(t, ids, 30)
	explain, err := coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "name"),
		"pending index must not be a candidate for a concurrent reader")

	// The creating tx keeps seeing its own index.
	cntTx, err := coll.Find(filter).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 30, cntTx)
	explainTx, err := coll.Find(filter).Explain(tx.Context())
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explainTx, "name"),
		"creating tx must plan with its own uncommitted index")

	require.NoError(t, tx.Commit())

	// Committed: the gate lifts for everyone.
	cnt, err = coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	explain, err = coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explain, "name"))
}

func TestCreateFtsIndexUncommitted_ConcurrentReader(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"a","body":"london crash report"}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"b","body":"paris sunshine"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))

	// Concurrent reader: exactly the pre-DDL behavior — no full-text index.
	iter, err := coll.Find(`{"$text":{"$search":"london"}}`).Iter(ctx)
	if err == nil {
		for iter.Next() {
		}
		err = iter.Err()
		require.NoError(t, iter.Close())
	}
	assert.ErrorIs(t, err, anystore.ErrNoFulltextIndex)

	require.NoError(t, tx.Commit())

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.Equal(t, []string{"a"}, ids)
}

func TestCreateVectorIndexUncommitted_ConcurrentReader(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(20, dim, 3)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 32},
	}))

	// Concurrent reader: exactly the pre-DDL behavior — no vector index
	// (prev == nil, nothing to substitute).
	_, err = vsearch(coll, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, anystore.ErrIndexNotFound)

	// The creating tx searches its own uncommitted index.
	fq := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[7], 3, 32)))
	iterTx, err := fq.Iter(tx.Context())
	require.NoError(t, err)
	var gotTx int
	for iterTx.Next() {
		gotTx++
	}
	require.NoError(t, iterTx.Err())
	require.NoError(t, iterTx.Close())
	assert.Equal(t, 3, gotTx)

	require.NoError(t, tx.Commit())

	hits, err := vsearch(coll, "v", vecs[0], 3, 32)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}

// These tests guard the snapshot side of the DDL visibility gate: visibility
// is decided against the READER'S OWN snapshot (validFromCookie fast path +
// per-snapshot namespace resolution), so a read tx opened BEFORE a DDL commit
// that plans AFTER it behaves exactly as if the DDL never happened — for a
// local commit, a compaction, and a peer process's commit adopted by
// reconcile. A wall-clock publication flag cannot pass these: after the
// commit the flag is down for everyone, including readers whose snapshots
// predate the index.

// vsearchCtx is vsearch through an explicit context (an open tx's view).
func vsearchCtx(qctx context.Context, coll anystore.Collection, field string, q []float32, k, ef int) ([]vhit, error) {
	iter, err := coll.Find(fmt.Sprintf(`{%q:%s}`, field, vknnJSON(q, k, ef))).Iter(qctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []vhit
	for iter.Next() {
		d, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		out = append(out, vhit{DocId: d.Value().Get("id").MarshalTo(nil), Distance: iter.Distance()})
	}
	return out, iter.Err()
}

func countIterCtx(t *testing.T, qctx context.Context, q anystore.Query) int {
	t.Helper()
	iter, err := q.Iter(qctx)
	require.NoError(t, err)
	defer iter.Close()
	var n int
	for iter.Next() {
		n++
	}
	require.NoError(t, iter.Err())
	return n
}

func TestStaleReaderAcrossCreateIndexCommit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		name := "other"
		if i < 30 {
			name = "x"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"name":%q}`, i, name))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{Fields: []string{"name"}}))

	const filter = `{"name":"x"}`

	// The stale reader's snapshot has no index namespace: the boost hint pins
	// the planner to the index IF it is a candidate — served by wall-clock
	// state it seeks an empty namespace and Count returns 0 silently.
	hint := anystore.IndexHint{IndexName: "name", Boost: 1_000_000}
	cnt, err := coll.Find(filter).IndexHint(hint).Count(rtx.Context())
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	assert.Equal(t, 30, countIterCtx(t, rtx.Context(), coll.Find(filter)))
	explain, err := coll.Find(filter).Explain(rtx.Context())
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "name"),
		"an index the snapshot predates must not be a candidate")

	// A fresh tx (snapshot cookie at the commit) uses the index.
	cnt, err = coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	explain, err = coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explain, "name"))
}

func TestStaleReaderAcrossFtsCreateCommit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"a","body":"london crash report"}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"b","body":"paris sunshine"}`)))

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))

	// Stale reader: exactly the pre-DDL behavior — no full-text index (served
	// by wall-clock state, its snapshot's meta doc-count reads as 0: silent
	// empty matches).
	iter, err := coll.Find(`{"$text":{"$search":"london"}}`).Iter(rtx.Context())
	if err == nil {
		for iter.Next() {
		}
		err = iter.Err()
		require.NoError(t, iter.Close())
	}
	assert.ErrorIs(t, err, anystore.ErrNoFulltextIndex)

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.Equal(t, []string{"a"}, ids)
}

func TestStaleReaderAcrossVectorCreateCommit(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(20, dim, 3)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 32},
	}))

	// Stale reader: the graph namespaces do not exist in its snapshot —
	// exactly the pre-DDL behavior.
	_, err = vsearchCtx(rtx.Context(), coll, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, anystore.ErrIndexNotFound)

	hits, err := vsearch(coll, "v", vecs[0], 3, 32)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}

func TestStaleReaderAcrossVectorCompactCommit(t *testing.T) {
	const dim = 8
	const n = 40
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < 15; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	// The compaction re-roots the graph namespaces and its commit clears
	// prev, so the stale reader below is served by a transient handle opened
	// from its own snapshot — searching the identical pre-compaction graph,
	// it must return identical results.
	before, err := vsearchCtx(rtx.Context(), coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	require.Len(t, before, 5)

	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))

	after, err := vsearchCtx(rtx.Context(), coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"a reader's snapshot must serve identical results across a compaction commit")

	// A fresh tx searches the compacted graph.
	hits, err := vsearch(coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	assert.Len(t, hits, 5)
}

// A same-tx drop+recreate under one name can land the recreated tree on the
// freed old root's page number (freelist-first allocation), so root-page
// equality alone would admit the pending handle to a concurrent reader whose
// snapshot holds the OLD tree at that page — the catalog-identity half of the
// slow path must exclude it (the snapshot's row carries the old definition).
func TestConcurrentReaderAcrossDropRecreateSameTx(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{Name: "nm", Fields: []string{"a"}}))
	for i := 0; i < 200; i++ {
		a := "other"
		if i < 30 {
			a = "x"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%q,"b":"y%d"}`, i, a, i%7))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.DropIndex(tx.Context(), "nm"))
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{Name: "nm", Fields: []string{"b"}}))

	// Concurrent reader during the window: the pending fields-b handle must
	// not serve a fields-a query even if its recreated root reuses the freed
	// page number the reader's snapshot still maps to the fields-a tree.
	const filter = `{"a":"x"}`
	hint := anystore.IndexHint{IndexName: "nm", Boost: 1_000_000}
	cnt, err := coll.Find(filter).IndexHint(hint).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, cnt)
	explain, err := coll.Find(filter).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "nm"),
		"a pending redefinition must not be a candidate for a concurrent reader")

	require.NoError(t, tx.Commit())

	cnt, err = coll.Find(`{"b":"y3"}`).Count(ctx)
	require.NoError(t, err)
	assert.NotZero(t, cnt)
	explain, err = coll.Find(`{"b":"y3"}`).Explain(ctx)
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explain, "nm"))
}

// Fts flavor of the same hazard: the five recreated namespaces can reuse
// freed page numbers, and a partial or full root coincidence must never let
// a concurrent reader search old postings through the new handle's field
// configuration — the definition mismatch excludes it (ErrNoFulltextIndex).
func TestConcurrentReaderAcrossFtsDropRecreateSameTx(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{Name: "t", Kind: anystore.IndexKindFulltext, Fields: []string{"body"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"a","body":"london crash report","title":"weather"}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"b","body":"paris sunshine","title":"london"}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.DropIndex(tx.Context(), "t"))
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{Name: "t", Kind: anystore.IndexKindFulltext, Fields: []string{"title"}}))

	iter, err := coll.Find(`{"$text":{"$search":"london"}}`).Iter(ctx)
	if err == nil {
		for iter.Next() {
		}
		err = iter.Err()
		require.NoError(t, iter.Close())
	}
	assert.ErrorIs(t, err, anystore.ErrNoFulltextIndex,
		"a pending fts redefinition must be invisible, never garbled results")

	require.NoError(t, tx.Commit())

	ids, _ := collectIter(t, coll.Find(`{"$text":{"$search":"london"}}`))
	assert.Equal(t, []string{"b"}, ids, "committed: the title index answers")
}

// A brute-force handle has no namespaces to resolve, but a stale reader whose
// snapshot contains the committed index must still be served: the slow path
// rebuilds from the snapshot's catalog row (metadata-only handle → scan).
// Restamp trigger: collection reopened after an unrelated cookie bump.
func TestStaleReaderBruteForceAfterReopen(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, Mode: anystore.VectorModeBruteForce},
	}))
	vecs := vrand(20, dim, 11)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	require.NoError(t, coll.Close())

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	// Unrelated schema commit bumps the cookie past the reader's snapshot.
	_, err = fx.CreateCollection(ctx, "other")
	require.NoError(t, err)

	// Reopen stamps the reloaded handles at the newer cookie.
	coll2, err := fx.OpenCollection(ctx, "docs")
	require.NoError(t, err)

	hits, err := vsearchCtx(rtx.Context(), coll2, "v", vecs[0], 3, 0)
	require.NoError(t, err, "a committed brute-force index must serve a reader its snapshot contains")
	assert.Len(t, hits, 3)
}

// A mid-tx collection reopen through an ambient write tx that already ran DDL
// sees that tx's own uncommitted index: the reloaded handle must be stamped
// for the COMMIT's cookie (init's SchemaChanged branch), or a concurrent
// reader at the begin cookie would seek a namespace that exists only in the
// writer's view — the phantom would even survive a rollback.
func TestAmbientReopenPendingRangeInvisible(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":"x%d"}`, i, i%5))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{Name: "nm", Fields: []string{"a"}}))
	require.NoError(t, coll.Close())
	coll2, err := fx.OpenCollection(tx.Context(), "docs")
	require.NoError(t, err)

	// The ambient tx sees and uses its own index through the reloaded handle.
	explainTx, err := coll2.Find(`{"a":"x1"}`).Explain(tx.Context())
	require.NoError(t, err)
	assert.True(t, explainHasIndex(explainTx, "nm"))

	// A concurrent reader must not: correct count via scan, no candidate.
	cnt, err := coll2.Find(`{"a":"x1"}`).IndexHint(anystore.IndexHint{IndexName: "nm", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, cnt)
	explain, err := coll2.Find(`{"a":"x1"}`).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "nm"),
		"an index reloaded from the writer's uncommitted view must stay invisible to concurrent readers")

	require.NoError(t, tx.Rollback())

	// The rolled-back index never becomes visible.
	cnt, err = coll2.Find(`{"a":"x1"}`).IndexHint(anystore.IndexHint{IndexName: "nm", Boost: 1_000_000}).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, cnt)
	explain, err = coll2.Find(`{"a":"x1"}`).Explain(ctx)
	require.NoError(t, err)
	assert.False(t, explainHasIndex(explain, "nm"))
}

// The vector flavor of the mid-tx reopen: writable-aware namespace
// resolution lets the reopen see the tx's own uncommitted graph namespaces,
// and init's SchemaChanged stamp keeps the reloaded handle invisible to
// concurrent readers — the phantom never escapes, even across a rollback.
func TestAmbientReopenPendingVectorInvisible(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(20, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 32},
	}))
	require.NoError(t, coll.Close())
	coll2, err := fx.OpenCollection(tx.Context(), "docs")
	require.NoError(t, err)

	// The ambient tx sees its own index through the reloaded handle.
	hitsTx, err := vsearchCtx(tx.Context(), coll2, "v", vecs[7], 3, 32)
	require.NoError(t, err)
	assert.Len(t, hitsTx, 3)

	// A concurrent reader must not: the graph exists only in the writer's
	// uncommitted view.
	_, err = vsearchCtx(ctx, coll2, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, anystore.ErrIndexNotFound)

	require.NoError(t, tx.Rollback())

	// The rolled-back index never becomes visible.
	_, err = vsearchCtx(ctx, coll2, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, anystore.ErrIndexNotFound)
}

// A stale reader on a redefined index (drop+recreate same name, different
// definition) fails noisy: its snapshot's catalog row no longer matches the
// current handle, and old data must never be served under a new definition
// (the SQLITE_SCHEMA posture).
func TestStaleReaderAcrossVectorDropRecreateDef(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 32},
	}))
	vecs := vrand(20, dim, 13)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	require.NoError(t, coll.DropIndex(ctx, "emb"))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, Mode: anystore.VectorModeBruteForce},
	}))

	_, err = vsearchCtx(rtx.Context(), coll, "v", vecs[0], 3, 32)
	assert.ErrorIs(t, err, anystore.ErrIndexNotFound,
		"a redefined index must fail noisy for a reader on the old definition's snapshot")

	hits, err := vsearch(coll, "v", vecs[0], 3, 0)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}

// A same-definition drop+recreate moves the roots; the stale reader is served
// by the transient rebuild from its OWN snapshot and must see exactly the
// results it saw before the DDL — even if the recreated roots collide with
// freed page numbers (the rebuild never trusts the handle's roots).
func TestStaleReaderAcrossVectorSameDefRecreate(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	info := anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}
	require.NoError(t, coll.EnsureIndex(ctx, info))
	vecs := vrand(30, dim, 17)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Commit()) }()

	before, err := vsearchCtx(rtx.Context(), coll, "v", vecs[29], 5, 64)
	require.NoError(t, err)
	require.Len(t, before, 5)

	require.NoError(t, coll.DropIndex(ctx, "emb"))
	require.NoError(t, coll.EnsureIndex(ctx, info))

	after, err := vsearchCtx(rtx.Context(), coll, "v", vecs[29], 5, 64)
	require.NoError(t, err)
	assert.Equal(t, before, after,
		"a reader's snapshot must serve identical results across a same-definition recreate")
}
