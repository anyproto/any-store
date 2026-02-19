/*
Index/Planner tests inspired by SQLite: where.test, where9.test

Test scenario:
Tests $or filters, $and ranges, $ne, $exists with sparse indexes, $in with
many values, complex nested filter combinations, and $or with sort — all
exercising index usage in the query planner.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// setupComplexFilterColl creates a collection with 100 docs where a=i%10, b=i%7
// and applies the given indexes.
func setupComplexFilterColl(t testing.TB, indexes ...IndexInfo) Collection {
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

func TestIndex_ComplexFilter_OrIndexedField(t *testing.T) {
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {$or: [{a:1}, {a:2}, {a:3}]} → 10 each = 30
	count, err := coll.Find(`{"$or":[{"a":1},{"a":2},{"a":3}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 30, count)

	// Compare with non-indexed collection
	collNoIdx := setupComplexFilterColl(t)
	countNoIdx, err := collNoIdx.Find(`{"$or":[{"a":1},{"a":2},{"a":3}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count)
}

func TestIndex_ComplexFilter_OrMixedFields(t *testing.T) {
	// Index on "a" only; "b" is not indexed
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

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

func TestIndex_ComplexFilter_AndRangeSameField(t *testing.T) {
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {$and: [{a:{$gte:3}}, {a:{$lte:7}}]} should be same as {a:{$gte:3,$lte:7}}
	countAnd, err := coll.Find(`{"$and":[{"a":{"$gte":3}},{"a":{"$lte":7}}]}`).Count(ctx)
	require.NoError(t, err)

	countRange, err := coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Count(ctx)
	require.NoError(t, err)

	assert.Equal(t, countRange, countAnd)
	// a in {3,4,5,6,7} → 5 values * 10 each = 50
	assert.Equal(t, 50, countAnd)
}

func TestIndex_ComplexFilter_NestedOrAnd(t *testing.T) {
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {$or: [{$and: [{a:{$gte:1}},{a:{$lte:3}}]}, {a:7}]}
	// arm1: a in {1,2,3} → 30 docs
	// arm2: a=7 → 10 docs
	// no overlap → 40 docs
	count, err := coll.Find(`{"$or":[{"$and":[{"a":{"$gte":1}},{"a":{"$lte":3}}]},{"a":7}]}`).Count(ctx)
	require.NoError(t, err)

	// Verify against non-indexed collection
	collNoIdx := setupComplexFilterColl(t)
	countNoIdx, err := collNoIdx.Find(`{"$or":[{"$and":[{"a":{"$gte":1}},{"a":{"$lte":3}}]},{"a":7}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count)
	assert.Equal(t, 40, count)
}

func TestIndex_ComplexFilter_NeWithIndex(t *testing.T) {
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {a: {$ne: 5}} → 100 - 10 = 90
	count, err := coll.Find(`{"a":{"$ne":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 90, count)

	// Compare with non-indexed
	collNoIdx := setupComplexFilterColl(t)
	countNoIdx, err := collNoIdx.Find(`{"a":{"$ne":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count)
}

func TestIndex_ComplexFilter_ExistsWithSparseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"optional"}, Sparse: true}))

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

func TestIndex_ComplexFilter_InWithManyValues(t *testing.T) {
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {a: {$in: [0,1,2,3,4,5,6,7,8,9]}} → all 100 docs
	count, err := coll.Find(`{"a":{"$in":[0,1,2,3,4,5,6,7,8,9]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, count)

	// Verify index is used (not full scan)
	explain, err := coll.Find(`{"a":{"$in":[0,1,2,3,4,5,6,7,8,9]}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan", "expected index scan for $in query")
}

func TestIndex_ComplexFilter_OrWithSort(t *testing.T) {
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {$or:[{a:1},{a:5}]} sorted by "a"
	vals := collectField(t, coll.Find(`{"$or":[{"a":1},{"a":5}]}`).Sort("a"), "a")
	require.Equal(t, 20, len(vals))

	// Verify sorted: all a=1 values should come before a=5 values
	for i := 1; i < len(vals); i++ {
		assert.True(t, vals[i-1] <= vals[i], "not sorted at %d: %s > %s", i, vals[i-1], vals[i])
	}

	// Collect integer values to verify groups
	var intVals []int
	iter, err := coll.Find(`{"$or":[{"a":1},{"a":5}]}`).Sort("a").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		intVals = append(intVals, doc.Value().GetInt("a"))
	}
	require.NoError(t, iter.Err())
	assert.True(t, sort.IntsAreSorted(intVals), "results should be sorted by a")
}

func TestIndex_ComplexFilter_ComplexNestedFilter(t *testing.T) {
	coll := setupComplexFilterColl(t,
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"b"}},
	)

	// {$and: [{$or:[{a:1},{a:2}]}, {b:{$gte:3}}]}
	// arm1 of or: a=1 → 10 docs, arm2: a=2 → 10 docs → 20 docs
	// b>=3 means b in {3,4,5,6} → ~57 of 100 docs (100*4/7 ≈ 57)
	// intersection: (a=1 or a=2) AND b>=3
	count, err := coll.Find(`{"$and":[{"$or":[{"a":1},{"a":2}]},{"b":{"$gte":3}}]}`).Count(ctx)
	require.NoError(t, err)

	// Verify against non-indexed collection
	collNoIdx := setupComplexFilterColl(t)
	countNoIdx, err := collNoIdx.Find(`{"$and":[{"$or":[{"a":1},{"a":2}]},{"b":{"$gte":3}}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count)
	assert.True(t, count > 0, "expected at least some results, got %d", count)
}

func TestIndex_ComplexFilter_EmptyOrResult(t *testing.T) {
	coll := setupComplexFilterColl(t, IndexInfo{Fields: []string{"a"}})

	// {$or:[{a:99},{a:100}]} → 0 results (values don't exist)
	count, err := coll.Find(`{"$or":[{"a":99},{"a":100}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Also verify iteration returns nothing
	docs := collectDocs(t, coll.Find(`{"$or":[{"a":99},{"a":100}]}`))
	assert.Len(t, docs, 0)
}
