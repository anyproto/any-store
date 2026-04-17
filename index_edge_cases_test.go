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
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
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

// TestIndex_EdgeCases_LargeKeyPanic reproduces a panic in btree.rebuildLeafPage
// when a non-unique index key exceeds the page's local payload capacity (~1002 bytes
// for a 4096-byte page). The index key for non-unique indexes is Tuple(field_value, doc_id),
// so large field values produce keys that don't fit on a single page.
// Bug: contentOff goes negative → slice bounds out of range [-N:]
func TestIndex_EdgeCases_LargeKeyPanic(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "testcoll")
	require.NoError(t, err)

	arena := &anyenc.Arena{}
	// Insert 50 docs with large "data" fields (2KB-5KB).
	// The index key will be Tuple(data_value, doc_id) which exceeds page capacity.
	for i := 0; i < 50; i++ {
		arena.Reset()
		obj := arena.NewObject()
		obj.Set("id", arena.NewString(fmt.Sprintf("doc-%04d", i)))
		obj.Set("val", arena.NewNumberInt(i))
		dataSize := 2048 + (i * 64) // 2KB to ~5KB
		data := make([]byte, dataSize)
		for j := range data {
			data[j] = byte('a' + (j % 26))
		}
		obj.Set("data", arena.NewString(string(data)))
		require.NoError(t, coll.UpsertOne(ctx, obj), "insert doc %d", i)
	}

	// Creating a non-unique index on "data" triggers the panic:
	// EnsureIndex → buildIndex → insertKeys → btree.Put →
	// splitLeafAndInsertWithPath → rebuildLeafPage → PANIC
	// (slice bounds out of range [-N:] because the key exceeds maxLocalPayload)
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"data"}})
	require.NoError(t, err, "EnsureIndex on large-value field should not panic")
}

func randomString(rng *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rng.Intn(len(letters))]
	}
	return string(b)
}

func TestDropRecreateIndexCorruption(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "corrupt.db")

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll, err := db.Collection(ctx, "testcoll")
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(11223344))
	arena := &anyenc.Arena{}

	// Step 1: Insert 500 docs
	for i := 0; i < 500; i++ {
		arena.Reset()
		obj := arena.NewObject()
		obj.Set("id", arena.NewString(fmt.Sprintf("doc-%06d", i)))
		obj.Set("val", arena.NewNumberInt(rng.Intn(100000)))
		obj.Set("data", arena.NewString(randomString(rng, 50+rng.Intn(100))))
		if err := coll.UpsertOne(ctx, obj); err != nil {
			t.Fatal(err)
		}
	}

	// Step 2: Cycle create/update/drop index
	for cycle := 0; cycle < 10; cycle++ {
		// EnsureIndex - corruption detected on cycle 4
		if err := coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"val"}}); err != nil {
			t.Fatalf("BUG CONFIRMED - cycle %d: EnsureIndex(val): %v", cycle, err)
		}

		// Updates between create and drop
		for j := 0; j < 20; j++ {
			arena.Reset()
			obj := arena.NewObject()
			key := fmt.Sprintf("doc-%06d", rng.Intn(500))
			obj.Set("id", arena.NewString(key))
			obj.Set("val", arena.NewNumberInt(rng.Intn(100000)))
			obj.Set("data", arena.NewString(randomString(rng, 50+rng.Intn(100))))
			_ = coll.UpsertOne(ctx, obj)
		}

		// Drop all indexes
		for _, idx := range coll.GetIndexes() {
			if err := coll.DropIndex(ctx, idx.Info().Name); err != nil {
				t.Fatalf("cycle %d: DropIndex: %v", cycle, err)
			}
		}
	}
	t.Log("All cycles passed - bug not triggered with this seed")
}

// --- Coverage tests from index_field_validation_coverage_test.go ---

// TestIndex_Coverage_UnderscorePrefixedFieldAllowed verifies that a user
// field named with a leading underscore (e.g. "_internal") is a valid index
// target. MongoDB reserves only "_id"; any other underscore-prefixed name is
// user namespace.
//
// Gap item 65: Index on an underscore-prefixed field name.
func TestIndex_Coverage_UnderscorePrefixedFieldAllowed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"_internal"}}),
		"CreateIndex on '_internal' must succeed")

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"_internal":"alpha"}`),
		anyenc.MustParseJson(`{"id":2,"_internal":"beta"}`),
		anyenc.MustParseJson(`{"id":3,"_internal":"gamma"}`),
	))

	vals := collectField(t, coll.Find(`{"_internal":"beta"}`), "id")
	assert.Equal(t, []string{"2"}, vals,
		"query by _internal must find exactly the one matching doc")
}

// TestIndex_Coverage_EmptySegmentInPathRejected verifies that CreateIndex
// rejects malformed path specifications with empty segments: a double dot
// ("a..b"), a leading dot (".a"), or a trailing dot ("a.").
//
// Gap item 66: Index path with empty segment.
func TestIndex_Coverage_EmptySegmentInPathRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	cases := []string{"a..b", ".a", "a."}
	for _, field := range cases {
		t.Run(field, func(t *testing.T) {
			err := coll.CreateIndex(ctx, IndexInfo{Fields: []string{field}})
			assert.Error(t, err,
				"CreateIndex with empty path segment %q must return a validation error", field)
		})
	}
}
