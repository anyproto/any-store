/*
Index/Planner tests inspired by SQLite: index.test, descidx1.test

Test scenario:
Tests for deterministic sort ordering when multiple documents share the
same sort key value. Verifies that sort is stable (preserves relative
order of equal elements), that index-based sort and in-memory sort
produce consistent results, and that tiebreaking behavior is predictable
across repeated queries.

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

func TestIndex_SortStability_DuplicateKeysConsistent(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert docs with duplicate sort key values
	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"label":"doc_%d"}`, i, i%4, i),
		)))
	}

	// Run same sort query multiple times — results should be identical each time
	var firstRun []string
	for run := range 5 {
		vals := collectField(t, coll.Find(nil).Sort("a"), "id")
		require.Len(t, vals, 20)
		if run == 0 {
			firstRun = vals
		} else {
			assert.Equal(t, firstRun, vals, "sort order changed on run %d", run)
		}
	}
}

func TestIndex_SortStability_SameKeyDifferentIds(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"group"}}))

	// All docs have same sort key — ordering should be stable
	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"group":1}`, i),
		)))
	}

	ids := collectField(t, coll.Find(nil).Sort("group"), "id")
	require.Len(t, ids, 10)

	// Run again — same order
	ids2 := collectField(t, coll.Find(nil).Sort("group"), "id")
	assert.Equal(t, ids, ids2)
}

func TestIndex_SortStability_IndexedVsNonIndexed(t *testing.T) {
	fx := newFixture(t)

	// Create two collections with same data — one indexed, one not
	collIdx, err := fx.CreateCollection(ctx, "test_idx")
	require.NoError(t, err)
	require.NoError(t, collIdx.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	collNoIdx, err := fx.CreateCollection(ctx, "test_noidx")
	require.NoError(t, err)

	for i := range 30 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i*3))
		require.NoError(t, collIdx.Insert(ctx, doc))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}

	// Both should return same count for same query
	countIdx, err := collIdx.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	countNoIdx, err := collNoIdx.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, countIdx)

	// Sort should produce same field values (order within equal keys may differ)
	aValsIdx := collectField(t, collIdx.Find(nil).Sort("a"), "a")
	aValsNoIdx := collectField(t, collNoIdx.Find(nil).Sort("a"), "a")
	assert.Equal(t, aValsNoIdx, aValsIdx)
}

func TestIndex_SortStability_FilteredWithDuplicateKeys(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert docs where filtered subset has duplicate sort values
	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i%2),
		)))
	}

	// Filter on b, sort on a — multiple docs with same a value
	ids1 := collectField(t, coll.Find(`{"b":0}`).Sort("a"), "id")
	ids2 := collectField(t, coll.Find(`{"b":0}`).Sort("a"), "id")
	assert.Equal(t, ids1, ids2, "filtered sort should be deterministic")
	assert.True(t, len(ids1) > 0)
}

func TestIndex_SortStability_DescendingWithDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 15 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%3),
		)))
	}

	// Descending sort — verify values are actually descending
	vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
	require.Len(t, vals, 15)
	for i := 1; i < len(vals); i++ {
		assert.True(t, vals[i-1] >= vals[i],
			"not descending at %d: %s < %s", i, vals[i-1], vals[i])
	}

	// Run twice — same order
	vals2 := collectField(t, coll.Find(nil).Sort("-a"), "a")
	assert.Equal(t, vals, vals2)
}

func TestIndex_SortStability_CompoundSortTiebreaker(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	// Insert docs where primary sort key (a) has duplicates,
	// second field (b) breaks the tie
	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%4, 100-i),
		)))
	}

	// Sort by (a, b) — within same a, ordered by b
	ids := collectField(t, coll.Find(nil).Sort("a", "b"), "id")
	require.Len(t, ids, 20)

	// Verify: for docs with same "a" value, "b" values should be ascending (numeric)
	iter, err := coll.Find(nil).Sort("a", "b").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	var prevA float64
	var prevB float64
	first := true
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		a := doc.Value().Get("a").GetFloat64()
		b := doc.Value().Get("b").GetFloat64()
		if !first && a == prevA {
			assert.True(t, b >= prevB,
				"within same a=%v, b should be ascending: prev=%v, cur=%v", a, prevB, b)
		}
		prevA = a
		prevB = b
		first = false
	}
	require.NoError(t, iter.Err())
}

func TestIndex_SortStability_WithLimitAndDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// 20 docs, only 4 distinct values for "a"
	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%4),
		)))
	}

	// Limit within a group of duplicates — should be deterministic
	ids1 := collectField(t, coll.Find(nil).Sort("a").Limit(7), "id")
	ids2 := collectField(t, coll.Find(nil).Sort("a").Limit(7), "id")
	assert.Equal(t, ids1, ids2)
	assert.Len(t, ids1, 7)

	// All returned values should have smallest "a" values
	aVals := collectField(t, coll.Find(nil).Sort("a").Limit(7), "a")
	for _, v := range aVals {
		assert.True(t, v == "0" || v == "1",
			"with limit=7 on 20 docs with a=0..3 (5 each), first 7 should have a=0 or a=1, got %s", v)
	}
}

func TestIndex_SortStability_ReverseIndexDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"-a"}}))

	for i := range 12 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%3),
		)))
	}

	// Sort descending using reverse index — results should be stable/deterministic
	ids1 := collectField(t, coll.Find(nil).Sort("-a"), "id")
	ids2 := collectField(t, coll.Find(nil).Sort("-a"), "id")
	assert.Equal(t, ids1, ids2)
	require.Len(t, ids1, 12)

	// All 12 docs returned
	assertCollCount(t, coll, 12)

	// Verify explain uses the reverse index
	explain, err := coll.Find(nil).Sort("-a").Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(-a)")
	assert.NotContains(t, explain.Sql, "Sort(")
}

func TestIndex_SortStability_AfterUpdate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 10 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i),
		)))
	}

	// Sort ascending — doc id:5 has a=5
	vals := collectField(t, coll.Find(nil).Sort("a"), "a")
	require.Len(t, vals, 10)
	assert.Equal(t, "5", vals[5])

	// Update doc id:5 to have a=0 (should move to front)
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":5,"a":-1}`)))

	// After update, sort should reflect new position
	iter, err := coll.Find(nil).Sort("a").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	var firstId float64
	if iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		firstId = doc.Value().Get("id").GetFloat64()
	}
	require.NoError(t, iter.Err())

	// Doc with a=-1 should be first (smallest value)
	assert.Equal(t, float64(5), firstId)
}

func TestIndex_SortStability_ManyDuplicates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// 500 docs with a = i%5
	for i := range 500 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}

	// Sort ascending
	vals := collectField(t, coll.Find(nil).Sort("a"), "a")
	require.Len(t, vals, 500)

	// Verify groups: first 100 should be a=0, next 100 a=1, etc.
	for i, v := range vals {
		expectedGroup := fmt.Sprintf("%d", i/100)
		assert.Equal(t, expectedGroup, v, "at position %d", i)
	}

	// Run again — same order
	vals2 := collectField(t, coll.Find(nil).Sort("a"), "a")
	assert.Equal(t, vals, vals2)
}
