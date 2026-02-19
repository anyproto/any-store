/*
Index/Planner tests inspired by SQLite: index.test, numindex1.test

Test scenario:
Edge cases and stress tests for the index and query planner system.
Covers large datasets, highly selective filters, all-same-value indexes,
nested field indexes, many duplicates, index create/drop/recreate lifecycle,
EnsureIndex idempotency, and multiple index management.

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

func TestIndex_EdgeCases_LargeDataset(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 2000 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%100))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Equality filter: a=42 → 2000/100 = 20 docs
	count, err := coll.Find(`{"a": 42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count)

	// Range filter: a >= 90 → a=90..99, 10 values * 20 each = 200
	count, err = coll.Find(`{"a": {"$gte": 90}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 200, count)

	// Sort + limit
	docs := collectDocs(t, coll.Find(nil).Sort("a").Limit(10))
	assert.Len(t, docs, 10)
}

func TestIndex_EdgeCases_HighlySelective(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 1000 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%100))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// a=42 → 1000/100 = 10 results
	count, err := coll.Find(`{"a": 42}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	// Verify index is used
	explain, err := coll.Find(`{"a": 42}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_EdgeCases_WideRange(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_indexed")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	collNoIdx, err := fx.CreateCollection(ctx, "test_noidx")
	require.NoError(t, err)

	for i := range 1000 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}

	// Wide range matching all docs
	countIdx, err := coll.Find(`{"a": {"$gte": 0, "$lte": 999}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1000, countIdx)

	// Compare with fullscan results
	countNoIdx, err := collNoIdx.Find(`{"a": {"$gte": 0, "$lte": 999}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, countIdx)

	// Verify same sorted results
	resultIdx := collectField(t, coll.Find(`{"a": {"$gte": 0, "$lte": 999}}`).Sort("a"), "a")
	resultNoIdx := collectField(t, collNoIdx.Find(`{"a": {"$gte": 0, "$lte": 999}}`).Sort("a"), "a")
	assert.Equal(t, resultNoIdx, resultIdx)
}

func TestIndex_EdgeCases_AllSameValue(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":1}`, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// All docs match
	count, err := coll.Find(`{"a": 1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, count)

	// No docs match
	count, err = coll.Find(`{"a": 2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Verify index length
	indexes := coll.GetIndexes()
	require.Len(t, indexes, 1)
	assertIndexLen(t, indexes[0], 100)
}

func TestIndex_EdgeCases_NestedFieldIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.score"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"meta":{"score":%d}}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Range query on nested field
	count, err := coll.Find(`{"meta.score": {"$gte": 50}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 50, count) // scores 50-99

	// Equality query on nested field
	count, err = coll.Find(`{"meta.score": 75}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Verify index is used for nested field queries
	explain, err := coll.Find(`{"meta.score": {"$gte": 50}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_EdgeCases_ManyDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 500 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Verify count for each value
	for v := range 5 {
		count, err := coll.Find(fmt.Sprintf(`{"a": %d}`, v)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 100, count, "expected 100 for a=%d", v)
	}

	// Verify total
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 500, count)
}

func TestIndex_EdgeCases_CreateDropRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// Create index and insert data
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Query with index
	count, err := coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// Drop the index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)

	// Query still works via fullscan
	count, err = coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	explain, err = coll.Find(`{"a": 5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "FullScan")

	// Create a new index on a different field
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	assert.Len(t, coll.GetIndexes(), 1)

	// Query on new index
	count, err = coll.Find(`{"b": 3}`).Count(ctx)
	require.NoError(t, err)
	// b = i%7, i in [0,50): values with b=3 are i=3,10,17,24,31,38,45 → 7 or 8
	assert.True(t, count >= 7 && count <= 8, "expected 7-8, got %d", count)

	explain, err = coll.Find(`{"b": 3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

func TestIndex_EdgeCases_EnsureIndexIdempotent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	idxInfo := IndexInfo{Fields: []string{"a"}}

	// Call EnsureIndex twice — no error
	require.NoError(t, coll.EnsureIndex(ctx, idxInfo))
	require.NoError(t, coll.EnsureIndex(ctx, idxInfo))

	// Only one index should exist
	indexes := coll.GetIndexes()
	assert.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)

	// Insert data and verify index works
	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Call EnsureIndex a third time after data insertion — still no error
	require.NoError(t, coll.EnsureIndex(ctx, idxInfo))
	assert.Len(t, coll.GetIndexes(), 1)
}

func TestIndex_EdgeCases_MultipleIndexesDropOne(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Both indexes work
	assert.Len(t, coll.GetIndexes(), 2)

	// Drop index on "a"
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 1)

	// Query on "b" still uses index
	count, err := coll.Find(`{"b": 3}`).Count(ctx)
	require.NoError(t, err)
	// b = i%7, i in [0,100): values with b=3 are i=3,10,17,24,31,38,45,52,59,66,73,80,87,94 → 14 or 15
	assert.True(t, count >= 14 && count <= 15, "expected 14-15, got %d", count)

	explain, err := coll.Find(`{"b": 3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// Query on "a" now uses fullscan
	count, err = coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	explain, err = coll.Find(`{"a": 5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "FullScan")
}

func TestIndex_EdgeCases_EmptyCollection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Query empty collection
	count, err := coll.Find(`{"a": 1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Index should be empty
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Insert one doc
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":1}`)))

	count, err = coll.Find(`{"a": 1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	assertIndexLen(t, coll.GetIndexes()[0], 1)

	// Delete it
	require.NoError(t, coll.DeleteId(ctx, 1))

	count, err = coll.Find(`{"a": 1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	assertIndexLen(t, coll.GetIndexes()[0], 0)
}
