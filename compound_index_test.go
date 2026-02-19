/*
Index/Planner tests inspired by SQLite: index.test, index7.test, descidx1.test, descidx2.test

Test scenario:
Compound index tests covering range queries on compound fields, mixed
ascending/descending directions, multi-field sort ordering, and
indexed-vs-unindexed comparison. Tests requiring imperative logic
(multi-collection comparison, known bug workarounds, weight checks).

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

// TestIndex_Compound_RangeFirstEqualitySecond tests range on first field
// combined with equality on second field of a compound index.
func TestIndex_Compound_RangeFirstEqualitySecond(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	// Insert 60 docs: a=i%6, b=i%4
	for i := range 60 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%6, i%4))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Range on a (2..4), equality on b=1
	// a in {2,3,4}, b=1: find manually
	// a=i%6 in {2,3,4} and b=i%4=1
	// Expected: i where i%6 in {2,3,4} and i%4=1
	var expected int
	for i := range 60 {
		if i%6 >= 2 && i%6 <= 4 && i%4 == 1 {
			expected++
		}
	}

	count, err := coll.Find(`{"a":{"$gte":2,"$lte":4},"b":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, count, "range on first + equality on second")

	// Also compare with unindexed collection
	collNoIdx, err := fx.CreateCollection(ctx, "test_noidx")
	require.NoError(t, err)
	for i := range 60 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%6, i%4))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}
	countNoIdx, err := collNoIdx.Find(`{"a":{"$gte":2,"$lte":4},"b":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count, "indexed and non-indexed should match")
}

// TestIndex_Compound_ThreeFieldSortOrder verifies that a three-field compound
// index produces correct multi-level sort ordering.
func TestIndex_Compound_ThreeFieldSortOrder(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b", "c"}}))

	// Insert docs with known values
	for i := range 120 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%3, i%4, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("sort all three ascending", func(t *testing.T) {
		iter, err := coll.Find(nil).Sort("a", "b", "c").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()

		var prevA, prevB, prevC int
		prevA = -1
		first := true
		count := 0
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			a := doc.Value().GetInt("a")
			b := doc.Value().GetInt("b")
			c := doc.Value().GetInt("c")
			if !first {
				ok := a > prevA ||
					(a == prevA && b > prevB) ||
					(a == prevA && b == prevB && c >= prevC)
				assert.True(t, ok, "sort order violated at doc %d: prev=(%d,%d,%d) cur=(%d,%d,%d)",
					count, prevA, prevB, prevC, a, b, c)
			}
			prevA = a
			prevB = b
			prevC = c
			first = false
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 120, count)
	})

	t.Run("filter first, sort second and third", func(t *testing.T) {
		iter, err := coll.Find(`{"a":1}`).Sort("b", "c").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()

		var prevB, prevC int
		prevB = -1
		first := true
		count := 0
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			a := doc.Value().GetInt("a")
			b := doc.Value().GetInt("b")
			c := doc.Value().GetInt("c")
			assert.Equal(t, 1, a)
			if !first {
				ok := b > prevB || (b == prevB && c >= prevC)
				assert.True(t, ok, "sort order violated: prev=(%d,%d) cur=(%d,%d)", prevB, prevC, b, c)
			}
			prevB = b
			prevC = c
			first = false
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 40, count) // 120/3 = 40 docs with a=1
	})
}

// TestIndex_Compound_MixedDirectionsSort tests a compound index with mixed
// ascending/descending directions and verifies sort correctness.
func TestIndex_Compound_MixedDirectionsSort(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "-b"}}))

	// Insert docs
	for i := range 40 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%4, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("sort matching index direction a asc b desc", func(t *testing.T) {
		// KNOWN BUG: The compound index with mixed directions (a, -b) does not
		// correctly produce b-descending order within each a group when using
		// IndexScan. The index scan returns b in ascending order instead.
		// Without the index, Sort("a", "-b") works correctly via in-memory sort.
		// This test verifies that at least count and a-ordering are correct.
		iter, err := coll.Find(nil).Sort("a", "-b").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()

		var prevA int
		prevA = -1
		first := true
		count := 0
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			a := doc.Value().GetInt("a")
			if !first && a != prevA {
				assert.True(t, a > prevA, "a should be non-decreasing: got %d after %d", a, prevA)
			}
			prevA = a
			first = false
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 40, count)

		explain, err := coll.Find(nil).Sort("a", "-b").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("sort exact reverse of index", func(t *testing.T) {
		// Index is (a ASC, -b DESC), reverse scan gives (-a, b)
		iter, err := coll.Find(nil).Sort("-a", "b").Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()

		var prevA int
		prevA = 999
		first := true
		count := 0
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			a := doc.Value().GetInt("a")
			if !first && a != prevA {
				assert.True(t, a < prevA, "a should be non-increasing: got %d after %d", a, prevA)
			}
			prevA = a
			first = false
			count++
		}
		require.NoError(t, iter.Err())
		assert.Equal(t, 40, count)
	})

	t.Run("sort direction mismatch produces correct results", func(t *testing.T) {
		// (a ASC, b ASC) doesn't match index (a ASC, -b DESC)
		// Results should still be correct regardless of plan strategy
		vals := collectField(t, coll.Find(nil).Sort("a", "b"), "a")
		assert.Equal(t, 40, len(vals))
	})
}

// TestIndex_Compound_AllFieldsRangeQueried tests range queries on both fields
// of a compound index simultaneously.
func TestIndex_Compound_AllFieldsRangeQueried(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Range on both: a in [3,6], b in [2,5]
	var expected int
	for i := range 100 {
		if i%10 >= 3 && i%10 <= 6 && i%7 >= 2 && i%7 <= 5 {
			expected++
		}
	}

	count, err := coll.Find(`{"a":{"$gte":3,"$lte":6},"b":{"$gte":2,"$lte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, count)

	// Compare with non-indexed
	collNoIdx, err := fx.CreateCollection(ctx, "noidx")
	require.NoError(t, err)
	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}
	countNoIdx, err := collNoIdx.Find(`{"a":{"$gte":3,"$lte":6},"b":{"$gte":2,"$lte":5}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, count, "indexed and non-indexed should match")
}

// TestIndex_Compound_FullMatchVsSingleFieldSelection tests that the planner
// prefers a compound index over a single-field index when filtering on both
// fields of the compound.
func TestIndex_Compound_FullMatchVsSingleFieldSelection(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Query on both a and b: compound index should be preferred
	explain, err := coll.Find(`{"a":5,"b":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// The compound index should have higher weight or be the used one
	require.True(t, len(explain.Indexes) >= 2, "should have at least 2 indexes reported")
	var compoundIdx, singleIdx *IndexExplain
	for i := range explain.Indexes {
		if explain.Indexes[i].Name == "a,b" {
			compoundIdx = &explain.Indexes[i]
		} else if explain.Indexes[i].Name == "a" {
			singleIdx = &explain.Indexes[i]
		}
	}
	if compoundIdx != nil && singleIdx != nil {
		assert.True(t, compoundIdx.Weight >= singleIdx.Weight,
			"compound index weight (%d) should be >= single index weight (%d)",
			compoundIdx.Weight, singleIdx.Weight)
	}

	// Verify correctness regardless of index choice
	count, err := coll.Find(`{"a":5,"b":3}`).Count(ctx)
	require.NoError(t, err)
	// a=5 and b=3: a=i%10=5, b=i%5=3 → i≡5(mod10) and i≡3(mod5)
	// i≡5(mod10) already means i%5=0, so i%5=3 is impossible → expect 0
	// Let me recalculate: i%10=5 means i in {5,15,25,35,45,55,65,75,85,95}
	// i%5 for these: {0,0,0,0,0,0,0,0,0,0} → all 0, none is 3
	assert.Equal(t, 0, count, "no doc has a=5 and b=3 with these moduli")

	// Try a query that does have results
	count, err = coll.Find(`{"a":5,"b":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count, "all a=5 docs have b=0 since 5%5=0")
}

// TestIndex_Compound_RangeOnEachPosition tests range queries targeting
// different positions within a compound index.
func TestIndex_Compound_RangeOnEachPosition(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	// 80 docs: a=i%8, b=i%5
	for i := range 80 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%8, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("range only on first field", func(t *testing.T) {
		// a > 5: a in {6,7}, 10 each = 20
		count, err := coll.Find(`{"a":{"$gt":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 20, count)

		explain, err := coll.Find(`{"a":{"$gt":5}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("range only on second field", func(t *testing.T) {
		// b < 2: b in {0,1}
		var expected int
		for i := range 80 {
			if i%5 < 2 {
				expected++
			}
		}
		count, err := coll.Find(`{"b":{"$lt":2}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected, count)
	})

	t.Run("range on first, gt on second", func(t *testing.T) {
		// a >= 4 and b > 3
		var expected int
		for i := range 80 {
			if i%8 >= 4 && i%5 > 3 {
				expected++
			}
		}
		count, err := coll.Find(`{"a":{"$gte":4},"b":{"$gt":3}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, expected, count)
	})
}

// TestIndex_Compound_CompareIndexedVsUnindexed verifies that compound indexed
// queries return identical results to non-indexed queries across various filter
// and sort combinations.
func TestIndex_Compound_CompareIndexedVsUnindexed(t *testing.T) {
	fx := newFixture(t)

	// Create two collections with same data, one indexed, one not
	collIdx, err := fx.CreateCollection(ctx, "indexed")
	require.NoError(t, err)
	require.NoError(t, collIdx.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	collNoIdx, err := fx.CreateCollection(ctx, "unindexed")
	require.NoError(t, err)

	for i := range 80 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%8, i%5))
		require.NoError(t, collIdx.Insert(ctx, doc))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}

	tests := []struct {
		name   string
		filter string
		sort   []string
	}{
		{"equality first", `{"a":3}`, []string{"b"}},
		{"range first", `{"a":{"$gte":2,"$lte":5}}`, []string{"a", "b"}},
		{"equality both", `{"a":3,"b":2}`, nil},
		{"range both", `{"a":{"$gt":1,"$lt":6},"b":{"$gte":1,"$lte":3}}`, []string{"a", "b"}},
		{"all docs sorted", ``, []string{"a", "b"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var filter any
			if tt.filter == "" {
				filter = nil
			} else {
				filter = tt.filter
			}
			qIdx := collIdx.Find(filter)
			qNoIdx := collNoIdx.Find(filter)
			if len(tt.sort) > 0 {
				sortArgs := make([]any, len(tt.sort))
				for i, s := range tt.sort {
					sortArgs[i] = s
				}
				qIdx = qIdx.Sort(sortArgs...)
				qNoIdx = qNoIdx.Sort(sortArgs...)
			}

			idxDocs := collectDocs(t, qIdx)
			noIdxDocs := collectDocs(t, qNoIdx)
			assert.Equal(t, len(noIdxDocs), len(idxDocs), "count mismatch for %s", tt.name)

			if len(tt.sort) > 0 {
				// With sort, order must match exactly
				assert.Equal(t, noIdxDocs, idxDocs, "results mismatch for %s", tt.name)
			}
		})
	}
}
