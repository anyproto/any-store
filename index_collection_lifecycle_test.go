/*
Index/Planner tests inspired by SQLite: index.test

Test scenario:
Tests for collection lifecycle operations with indexes: rename collection
preserves indexes, drop collection removes all indexes, multiple
collections with separate indexes don't interfere, collection close and
reopen via OpenCollection preserves indexes.

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
