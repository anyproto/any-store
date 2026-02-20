/*
Index/Planner tests inspired by SQLite: index.test, index3.test, index4.test

Test scenario:
Tests that indexes and their data survive database close and reopen.
Covers: index definitions persist, index data is intact after restart,
queries use indexes after reopen, unique constraints hold after reopen,
sparse indexes survive, creating indexes on existing data after reopen,
checkpoint followed by reopen, and multiple indexes on same collection.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

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
