package anystore

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// --- from limit_offset_index_test.go ---

// collectIntField collects an integer field from query results as []int.
func collectIntField(t testing.TB, q Query, field string) []int {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var results []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		v := doc.Value().Get(field)
		if v != nil {
			n, err := strconv.Atoi(v.String())
			require.NoError(t, err)
			results = append(results, n)
		}
	}
	require.NoError(t, iter.Err())
	return results
}

func TestIndex_LimitOffset_FilterSortLimit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert 100 docs with a = i%20 (values 0..19, ~5 each)
	for i := 0; i < 100; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%20))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("filter gte + sort + limit", func(t *testing.T) {
		vals := collectIntField(t, coll.Find(`{"a":{"$gte":10}}`).Sort("a").Limit(5), "a")
		require.Len(t, vals, 5)
		// All returned values should be >= 10 and sorted
		for _, v := range vals {
			assert.True(t, v >= 10, "expected >= 10, got %d", v)
		}
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i] >= vals[i-1], "not sorted at %d", i)
		}
	})

	t.Run("filter + sort + offset + limit", func(t *testing.T) {
		// Get all matching results for comparison
		allVals := collectIntField(t, coll.Find(`{"a":{"$gte":10}}`).Sort("a"), "a")
		require.True(t, len(allVals) >= 10, "expected at least 10 results")

		// Get windowed result
		windowVals := collectIntField(t, coll.Find(`{"a":{"$gte":10}}`).Sort("a").Offset(5).Limit(5), "a")
		require.Len(t, windowVals, 5)

		// Window should match slice of full results
		assert.Equal(t, allVals[5:10], windowVals)
	})

	t.Run("filter narrows + sort orders + limit caps", func(t *testing.T) {
		// a in range [5, 15), sorted ascending, limit 3
		vals := collectIntField(t, coll.Find(`{"a":{"$gte":5,"$lt":15}}`).Sort("a").Limit(3), "a")
		require.Len(t, vals, 3)
		for _, v := range vals {
			assert.True(t, v >= 5 && v < 15, "value %d out of range", v)
		}
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i] >= vals[i-1])
		}
	})
}

func TestIndex_LimitOffset_PaginationConsistency(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 50; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Paginate through all 50 docs with page size 10
	var allPaged []int
	for page := 0; page < 5; page++ {
		pageVals := collectIntField(t,
			coll.Find(nil).Sort("a").Offset(uint(page*10)).Limit(10), "a")
		require.Len(t, pageVals, 10, "page %d should have 10 results", page)
		allPaged = append(allPaged, pageVals...)
	}

	// Page 6 should be empty
	extraVals := collectIntField(t, coll.Find(nil).Sort("a").Offset(50).Limit(10), "a")
	assert.Len(t, extraVals, 0)

	// All 50 values collected across pages
	require.Len(t, allPaged, 50)

	// No gaps, no duplicates, correct order
	expected := make([]int, 50)
	for i := range expected {
		expected[i] = i + 1
	}
	assert.Equal(t, expected, allPaged)
}

func TestIndex_LimitOffset_CompoundSortLimit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x", "y"}}))

	for i := 0; i < 100; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d,"y":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("compound sort limit 10", func(t *testing.T) {
		type xy struct{ x, y int }
		iter, err := coll.Find(nil).Sort("x", "y").Limit(10).Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()

		var results []xy
		for iter.Next() {
			doc, err := iter.Doc()
			require.NoError(t, err)
			v := doc.Value()
			x, _ := strconv.Atoi(v.Get("x").String())
			y, _ := strconv.Atoi(v.Get("y").String())
			results = append(results, xy{x, y})
		}
		require.NoError(t, iter.Err())
		require.Len(t, results, 10)

		// Verify compound sort order: sorted by x first, then by y
		for i := 1; i < len(results); i++ {
			prev, cur := results[i-1], results[i]
			if prev.x == cur.x {
				assert.True(t, prev.y <= cur.y,
					"wrong order at %d: (%d,%d) should come before (%d,%d)",
					i, prev.x, prev.y, cur.x, cur.y)
			} else {
				assert.True(t, prev.x < cur.x,
					"wrong order at %d: x=%d should come before x=%d",
					i, prev.x, cur.x)
			}
		}

		// Verify planner uses compound index
		explain, err := coll.Find(nil).Sort("x", "y").Limit(10).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
		// SortIter renders as "-> Sort" (or "-> TopK(n)" when a Limit turns it
		// into a bounded sort); neither may appear — the index supplies order.
		assert.NotContains(t, explain.Sql, "-> Sort", explain.Sql)
		assert.NotContains(t, explain.Sql, "TopK", explain.Sql)
	})

	t.Run("compound sort matches sorted full result", func(t *testing.T) {
		// Get first 10 from sorted query
		limited := collectDocs(t, coll.Find(nil).Sort("x", "y").Limit(10))
		// Get all sorted, take first 10
		all := collectDocs(t, coll.Find(nil).Sort("x", "y"))
		require.True(t, len(all) >= 10)
		assert.Equal(t, all[:10], limited)
	})
}

func TestIndex_LimitOffset_LimitOneUniqueIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	for i := 1; i <= 100; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("limit 1 with unique equality", func(t *testing.T) {
		vals := collectIntField(t, coll.Find(`{"a":42}`).Limit(1), "a")
		require.Len(t, vals, 1)
		assert.Equal(t, 42, vals[0])

		explain, err := coll.Find(`{"a":42}`).Limit(1).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "CoverLookup")
	})

	t.Run("limit 1 sort asc returns minimum", func(t *testing.T) {
		vals := collectIntField(t, coll.Find(nil).Sort("a").Limit(1), "a")
		require.Len(t, vals, 1)
		assert.Equal(t, 1, vals[0])
	})

	t.Run("limit 1 sort desc returns maximum", func(t *testing.T) {
		vals := collectIntField(t, coll.Find(nil).Sort("-a").Limit(1), "a")
		require.Len(t, vals, 1)
		assert.Equal(t, 100, vals[0])
	})
}

func TestIndex_LimitOffset_IndexedVsUnindexed(t *testing.T) {
	// Compare indexed and unindexed results for sort+limit to verify correctness
	fx := newFixture(t)

	collIdx, err := fx.CreateCollection(ctx, "indexed")
	require.NoError(t, err)
	require.NoError(t, collIdx.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	collNoIdx, err := fx.CreateCollection(ctx, "unindexed")
	require.NoError(t, err)

	for i := 1; i <= 50; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, collIdx.Insert(ctx, doc))
		require.NoError(t, collNoIdx.Insert(ctx, doc))
	}

	t.Run("sort limit matches", func(t *testing.T) {
		idxVals := collectIntField(t, collIdx.Find(nil).Sort("a").Limit(10), "a")
		noIdxVals := collectIntField(t, collNoIdx.Find(nil).Sort("a").Limit(10), "a")
		assert.Equal(t, noIdxVals, idxVals)
	})

	t.Run("sort offset limit matches", func(t *testing.T) {
		idxVals := collectIntField(t, collIdx.Find(nil).Sort("a").Offset(20).Limit(10), "a")
		noIdxVals := collectIntField(t, collNoIdx.Find(nil).Sort("a").Offset(20).Limit(10), "a")
		assert.Equal(t, noIdxVals, idxVals)
	})

	t.Run("filter sort limit matches", func(t *testing.T) {
		idxVals := collectIntField(t, collIdx.Find(`{"a":{"$gte":10}}`).Sort("a").Limit(5), "a")
		noIdxVals := collectIntField(t, collNoIdx.Find(`{"a":{"$gte":10}}`).Sort("a").Limit(5), "a")
		assert.Equal(t, noIdxVals, idxVals)
	})

	t.Run("desc sort limit matches", func(t *testing.T) {
		idxVals := collectIntField(t, collIdx.Find(nil).Sort("-a").Limit(10), "a")
		noIdxVals := collectIntField(t, collNoIdx.Find(nil).Sort("-a").Limit(10), "a")
		assert.Equal(t, noIdxVals, idxVals)
	})
}

func TestIndex_LimitOffset_FilterSortLimitOffset_FullCombo(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert 200 docs with a = i%20
	for i := 0; i < 200; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%20))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("filter + sort + limit + offset", func(t *testing.T) {
		// Get full sorted filtered result for reference
		allVals := collectIntField(t, coll.Find(`{"a":{"$gte":5,"$lt":15}}`).Sort("a"), "a")

		// Windowed
		windowVals := collectIntField(t,
			coll.Find(`{"a":{"$gte":5,"$lt":15}}`).Sort("a").Offset(10).Limit(10), "a")

		require.True(t, len(allVals) >= 20, "expected at least 20 filtered results, got %d", len(allVals))
		require.Len(t, windowVals, 10)

		// Window matches the right slice of full results
		assert.Equal(t, allVals[10:20], windowVals)
	})

	t.Run("filter + desc sort + limit + offset", func(t *testing.T) {
		allVals := collectIntField(t, coll.Find(`{"a":{"$gte":5,"$lt":15}}`).Sort("-a"), "a")

		windowVals := collectIntField(t,
			coll.Find(`{"a":{"$gte":5,"$lt":15}}`).Sort("-a").Offset(5).Limit(5), "a")

		require.True(t, len(allVals) >= 10)
		require.Len(t, windowVals, 5)

		assert.Equal(t, allVals[5:10], windowVals)
	})

	t.Run("paginate through filtered results", func(t *testing.T) {
		allVals := collectIntField(t, coll.Find(`{"a":{"$gte":10}}`).Sort("a"), "a")
		totalCount := len(allVals)

		var paged []int
		pageSize := 15
		for offset := 0; offset < totalCount+pageSize; offset += pageSize {
			page := collectIntField(t,
				coll.Find(`{"a":{"$gte":10}}`).Sort("a").Offset(uint(offset)).Limit(uint(pageSize)), "a")
			if len(page) == 0 {
				break
			}
			paged = append(paged, page...)
		}

		// Should collect same as full query
		assert.Equal(t, allVals, paged)
	})
}

func TestIndex_LimitOffset_WithDuplicateValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Many docs with same 'a' value
	for i := 0; i < 30; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i/10)) // a = 0,0,...,1,1,...,2,2,...
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("sort limit with duplicates", func(t *testing.T) {
		vals := collectIntField(t, coll.Find(nil).Sort("a").Limit(15), "a")
		require.Len(t, vals, 15)

		// Should be sorted
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i] >= vals[i-1])
		}
	})

	t.Run("sort offset limit skips into duplicates", func(t *testing.T) {
		allVals := collectIntField(t, coll.Find(nil).Sort("a"), "a")
		windowVals := collectIntField(t, coll.Find(nil).Sort("a").Offset(5).Limit(10), "a")
		require.Len(t, windowVals, 10)
		assert.Equal(t, allVals[5:15], windowVals)
	})

	t.Run("pagination consistency with duplicates", func(t *testing.T) {
		allSorted := collectIntField(t, coll.Find(nil).Sort("a"), "a")
		sort.Ints(allSorted) // stable reference

		var paged []int
		for offset := 0; offset < 30; offset += 10 {
			page := collectIntField(t, coll.Find(nil).Sort("a").Offset(uint(offset)).Limit(10), "a")
			paged = append(paged, page...)
		}
		assert.Equal(t, allSorted, paged)
	})
}

// --- from index_sort_stability_test.go ---

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
	assert.NotContains(t, explain.Sql, "-> Sort", explain.Sql)
	assert.NotContains(t, explain.Sql, "TopK", explain.Sql)
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

// --- from index_datatypes_test.go ---

func TestIndex_DataTypes_LargeNumbers(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"big"}}))

	// Insert large numbers
	largeVals := []int64{100001, 500002, 999999, 42, 750003}
	for i, v := range largeVals {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"big":%d}`, i, v),
		)))
	}

	// Equality on large number
	count, err := coll.Find(`{"big":999999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Range on large numbers
	count, err = coll.Find(`{"big":{"$gte":100001,"$lte":999999}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count) // 100001, 500002, 750003, 999999

	// Sort ascending — verify correct numeric order
	vals := collectField(t, coll.Find(nil).Sort("big"), "big")
	require.Len(t, vals, 5)
	assert.Equal(t, "42", vals[0])
	assert.Equal(t, "100001", vals[1])
	assert.Equal(t, "500002", vals[2])
	assert.Equal(t, "750003", vals[3])
	assert.Equal(t, "999999", vals[4])

	// Sort descending
	valsDesc := collectField(t, coll.Find(nil).Sort("-big"), "big")
	require.Len(t, valsDesc, 5)
	assert.Equal(t, "999999", valsDesc[0])
	assert.Equal(t, "42", valsDesc[4])

	// Also verify truly large numbers work for equality and range (even if formatting differs)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":10,"big":9876543210}`)))
	count, err = coll.Find(`{"big":9876543210}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"big":{"$gt":999999}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_DataTypes_EmptyString(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"s"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"s":""}`),
		anyenc.MustParseJson(`{"id":2,"s":"hello"}`),
		anyenc.MustParseJson(`{"id":3,"s":""}`),
		anyenc.MustParseJson(`{"id":4,"s":"world"}`),
	))

	// Query for empty string
	count, err := coll.Find(`{"s":""}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Non-empty
	count, err = coll.Find(`{"s":"hello"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	assertIndexLen(t, coll.GetIndexes()[0], 4)
}

func TestIndex_DataTypes_UnicodeStrings(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"text"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"text":"hello"}`),
		anyenc.MustParseJson(`{"id":2,"text":"\u4f60\u597d"}`),
		anyenc.MustParseJson(`{"id":3,"text":"\u00e9l\u00e8ve"}`),
		anyenc.MustParseJson(`{"id":4,"text":"\u0410\u043b\u0438\u0441\u0430"}`),
		anyenc.MustParseJson(`{"id":5,"text":"\u4f60\u597d"}`),
	))

	// Equality on unicode
	count, err := coll.Find(`{"text":"\u4f60\u597d"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	count, err = coll.Find(`{"text":"\u00e9l\u00e8ve"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"text":"\u0410\u043b\u0438\u0441\u0430"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	assertIndexLen(t, coll.GetIndexes()[0], 5)
}

// --- Coverage tests from anyenc_nul_coverage_test.go ---

// TestAnyenc_Coverage_EmbeddedNulPreservesSeparation verifies that inserting
// two documents whose name field differs only by an embedded NUL byte
// ({id:"d1", name:"a\x00b"} and {id:"d2", name:"a"}) remain independently
// queryable. Each of the two equality queries must match exactly one doc.
//
// Gap item 47: String with embedded NUL byte ("a\x00b").
func TestAnyenc_Coverage_EmbeddedNulPreservesSeparation(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// JSON \u0000 produces a literal NUL byte in the resulting string.
	d1 := anyenc.MustParseJson(`{"id":"d1","name":"a\u0000b"}`)
	d2 := anyenc.MustParseJson(`{"id":"d2","name":"a"}`)
	require.NoError(t, coll.Insert(ctx, d1, d2))

	// Query by name = "a\x00b" — must return exactly d1.
	got1 := collectField(t, coll.Find(`{"name":"a\u0000b"}`), "id")
	assert.Equal(t, []string{`"d1"`}, got1,
		"query by name=\"a\\x00b\" must match only d1")

	// Query by name = "a" — must return exactly d2.
	got2 := collectField(t, coll.Find(`{"name":"a"}`), "id")
	assert.Equal(t, []string{`"d2"`}, got2,
		"query by name=\"a\" must match only d2")
}

// --- from index_edge_cases_test.go ---

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

// TestAudit03_MultiBoundOverlap_* exercises the most important integration
// invariant of the multi-key bit + dedup pipeline introduced on the `btree`
// branch (see any-store-tests:docs/any-store/plans/2026-04-29-multikey-bit-and-dedup-pipeline.md):
//
//	A `$in` filter that produces multiple bounds, executed against a
//	multi-key (array-valued) index whose documents have OVERLAPPING values
//	with those bounds, MUST surface each matching doc exactly once.
//
// Concretely: doc {id:"d1", tags:["a","b","c"]} with index on `tags` produces
// 4 raw entries (per AUDIT01: 3 element entries + 1 whole-array entry, all
// tagged IndexValueMultiKey). A query `{tags:{$in:["a","b"]}}` builds two
// bounds — one over key "a", one over key "b" — and BOTH bounds match the
// SAME doc. Without the per-entry value-byte + DocDedup pipeline, Count
// would return 2 (one per matching bound) and Iter would yield d1 twice.
//
// This file is the black-box mirror of audit_01_valuebyte_basic_test.go: that
// file pins the producer side (insertKeys writes the right value byte); this
// file pins the consumer side (Count via IndexIter.CountEntries and
// Iter via planIterator.Next consume that byte and dedup correctly).

// TestAudit03_MultiBoundOverlap_SingleDocCount: the minimal regression. One
// doc whose tags array overlaps two `$in` bounds — Count must be 1.
func TestAudit03_MultiBoundOverlap_SingleDocCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit03_count_single")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	// $in builds 2 bounds: ["a","a"] and ["b","b"]. Both bounds match d1
	// because d1.tags has both "a" and "b". The CountEntries multi-bound
	// path must dedup the docId across bounds.
	n, err := coll.Find(`{"tags":{"$in":["a","b"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"single doc whose array overlaps two $in bounds must be counted once "+
			"(Count=%d means the IndexIter.CountEntries multi-bound dedup is broken)", n)
}

// TestAudit03_MultiBoundOverlap_SingleDocIter: same data as the Count test —
// Iter must yield d1 exactly once. Exercises planIterator.Next + DocDedup
// (the consumer-side dedup that lives outside CountEntries).
func TestAudit03_MultiBoundOverlap_SingleDocIter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit03_iter_single")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	ids := collectField(t, coll.Find(`{"tags":{"$in":["a","b"]}}`), "id")
	assert.Equal(t, []string{`"d1"`}, ids,
		"Iter over $in:[a,b] on multi-key index with single overlapping doc "+
			"must yield d1 exactly once (got %v)", ids)
}

// TestAudit03_MultiBoundOverlap_MultiDocCount: three docs with mixed overlap.
// The expected count is the number of DISTINCT docs that match the union of
// bounds, not the sum of (doc, bound) hits.
//
//	d1.tags = [a,b]   matches "a" and "b"  → 2 hits, dedup to 1
//	d2.tags = [b,c]   matches "b" and "c"  → 2 hits, dedup to 1
//	d3.tags = [x]     matches none         → 0 hits
//
// Without dedup the count would be 4. With correct dedup, it is 2.
func TestAudit03_MultiBoundOverlap_MultiDocCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit03_count_multi")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x"]}`),
	))

	n, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n,
		"distinct match count must be 2 (d1+d2). A result of 4 would mean "+
			"each (doc, bound) hit was counted separately — broken dedup. "+
			"A result of 3 would mean only one bound dedup'd. Got %d.", n)
}

// TestAudit03_MultiBoundOverlap_MultiDocIter: same data as the Count test —
// Iter must yield d1, d2 (and only those) with no duplicates, in some order.
func TestAudit03_MultiBoundOverlap_MultiDocIter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit03_iter_multi")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x"]}`),
	))

	ids := collectField(t, coll.Find(`{"tags":{"$in":["a","b","c"]}}`), "id")
	// Sort for stable comparison — the planner is free to choose any order.
	sort.Strings(ids)
	assert.Equal(t, []string{`"d1"`, `"d2"`}, ids,
		"Iter must yield exactly d1 and d2 (no duplicates, no d3); got %v", ids)
}

// TestAudit03_MultiBoundOverlap_HeavyScale: 200 docs each with two tags —
// a unique per-doc tag and the shared tag "shared". The query
// `tags $in [shared, t5, t10]` exercises BOTH the cross-bound dedup
// (d5 matches "shared" AND "t5"; d10 matches "shared" AND "t10") AND the
// scale needed to ensure the planner actually picks IndexScan (so the
// IndexIter.CountEntries multi-bound path is the one being exercised, not
// FullScan).
//
// Expected: every doc matches "shared", so distinct doc count = 200.
// Without dedup, the count would be 202 (d5 and d10 counted twice each).
func TestAudit03_MultiBoundOverlap_HeavyScale(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit03_heavy")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	const N = 200
	for i := 0; i < N; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"d%d","tags":["t%d","shared"]}`, i, i),
		)))
	}

	// Sanity: d5 and d10 each match the "shared" bound AND their per-doc
	// bound — without dedup, the count would over-report by 2.
	n, err := coll.Find(`{"tags":{"$in":["shared","t5","t10"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, N, n,
		"every doc has the 'shared' tag, so distinct count = %d. "+
			"d5 and d10 ALSO match their per-doc bounds — if dedup is broken, "+
			"the count would be %d (or higher). Got %d.", N, N+2, n)
}

// TestAudit05_CompoundMultiKey_*
//
// Compound multi-key indexes (e.g. (tags, priority) where tags is an array
// dimension) no longer carry a planner-side dedup wrap. The recent commit
// series dropped SeenSetDedupIter; compound multi-key now relies on:
//
//   - IndexIter setting multiKey=true per entry based on the per-entry
//     value byte that insertKeys writes for array-derived rows.
//   - multiKey flowing through FetchIter / FilterIter / SortIter as a
//     passthrough on Iterator.Next.
//   - The consumer (planIterator.Next, plus the Count/Update/Delete loops
//     in query.go) calling qplanner.DocDedup.Accept(docId, mk) to drop
//     duplicate doc emissions.
//
// These tests are end-to-end black-box guards: a regression that drops the
// multiKey propagation in any iterator (or removes the consumer-side
// DocDedup) would let a compound multi-key query emit the same doc twice
// (once per matching array element). Both Count and Iter paths are
// exercised, since they go through different consumer code (query.go's
// count loop vs. planIterator.Next).
//
// Index used in all four subtests: Fields:["tags", "priority"], where
// "tags" is the array dimension. With $in:["a","b"] over tags, a doc whose
// tags include both "a" and "b" produces two index entries — both with
// priority=5 — and a regression would surface that as a Count of 2 or as
// the same id appearing twice from Iter.

// TestAudit05_CompoundMultiKey_Count_SingleDocSameArrayValues asserts that
// Count on a compound multi-key query returns 1 (not 2) for a single doc
// whose tags array matches both bounds of an $in.
func TestAudit05_CompoundMultiKey_Count_SingleDocSameArrayValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags", "priority"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"priority":5}`),
	))

	// d1's tags array produces two index entries: (a,5,d1) and (b,5,d1),
	// both flagged multiKey=true. The $in over tags hits both bounds, so
	// without consumer-side DocDedup the count loop would tally 2.
	count, err := coll.Find(`{"tags":{"$in":["a","b"]}, "priority":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count,
		"compound multi-key: doc whose array matches multiple $in bounds must count once")
}

// TestAudit05_CompoundMultiKey_Iter_SingleDocSameArrayValues asserts that
// Iter yields the doc exactly once over the same query — exercising the
// planIterator.Next dedup path rather than the count loop.
func TestAudit05_CompoundMultiKey_Iter_SingleDocSameArrayValues(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags", "priority"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"priority":5}`),
	))

	ids := collectField(t,
		coll.Find(`{"tags":{"$in":["a","b"]}, "priority":5}`), "id")
	assert.Equal(t, []string{`"d1"`}, ids,
		"compound multi-key: Iter must emit the doc exactly once even though "+
			"two index entries (a,5) and (b,5) match")
}

// TestAudit05_CompoundMultiKey_Count_TwoDocsOverlappingTags inserts two
// docs whose tags arrays each match multiple $in bounds, with one bound
// ("b") shared between them. Count must collapse each doc to one despite
// emitting four raw matching index entries: (a,5,d1), (b,5,d1), (b,5,d2),
// (c,5,d2).
func TestAudit05_CompoundMultiKey_Count_TwoDocsOverlappingTags(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags", "priority"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"priority":5}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"],"priority":5}`),
	))

	count, err := coll.Find(`{"tags":{"$in":["a","b","c"]}, "priority":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count,
		"compound multi-key: two docs each matching multiple $in bounds must count as 2")
}

// TestAudit05_CompoundMultiKey_Iter_TwoDocsOverlappingTags is the Iter
// counterpart of the previous case: each doc must appear exactly once,
// no dupes, even though up to four raw index entries match.
func TestAudit05_CompoundMultiKey_Iter_TwoDocsOverlappingTags(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags", "priority"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"priority":5}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"],"priority":5}`),
	))

	ids := collectField(t,
		coll.Find(`{"tags":{"$in":["a","b","c"]}, "priority":5}`), "id")
	require.Len(t, ids, 2,
		"compound multi-key: Iter must yield 2 docs (no duplicates), got %v", ids)

	// Order can vary depending on index traversal; sort for a stable assert.
	sort.Strings(ids)
	assert.Equal(t, []string{`"d1"`, `"d2"`}, ids,
		"compound multi-key: Iter must emit d1 and d2 each exactly once")
}

// Audit 06: UpdateMany / DeleteMany over a multi-key $in query must apply
// the modifier (or the delete) exactly once per matching document, not once
// per matching index entry.
//
// Background: when the data field is an array (e.g. tags:["a","b","c"]),
// the index emits one entry per array element. A query like
// {"tags":{"$in":["a","b","c"]}} therefore visits the same docId multiple
// times via different index entries. The query.go Update/Delete loops
// guard against double-application by routing the iterator output through
// qplanner.DocDedup, which dedups on (docId, multi-key bit). Without that
// dedup, $inc would be applied N times per doc and Delete would either
// double-count or fail trying to delete an already-removed key.

// TestAudit06_UpdateOnce_OverlappingArray:
// d1 has tags:["a","b","c"]. The $in:["a","b","c"] query visits d1 three
// times via the array index. UpdateMany must $inc d1.n exactly once
// (n == 1, not 3). Modified count must be 1, not 3.
func TestAudit06_UpdateOnce_OverlappingArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"],"n":0}`),
	))

	res, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).
		Update(ctx, `{"$inc":{"n":1}}`)
	require.NoError(t, err)
	assert.Equal(t, 1, res.Matched, "matched should be 1 (one doc), not the number of index hits")
	assert.Equal(t, 1, res.Modified, "modified should be 1 (one doc), not the number of index hits")

	doc, err := coll.FindId(ctx, "d1")
	require.NoError(t, err)
	got := doc.Value().Get("n").GetFloat64()
	assert.Equal(t, float64(1), got,
		"$inc:{n:1} must apply exactly once per doc; got n=%v (expected 1, NOT 3)", got)
}

// TestAudit06_UpdateOnce_TwoDocs:
// d1 tags=[a,b], d2 tags=[b,c]. Both match $in:[a,b,c] via multiple
// array entries each (d1 via "a" and "b"; d2 via "b" and "c"; "b"
// matches a third bound for both). Update must hit each doc exactly
// once: d1.n==1, d2.n==1, modified==2.
func TestAudit06_UpdateOnce_TwoDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"n":0}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"],"n":0}`),
	))

	res, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).
		Update(ctx, `{"$inc":{"n":1}}`)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Matched, "matched should be 2 distinct docs")
	assert.Equal(t, 2, res.Modified, "modified should be 2 distinct docs (NOT one per matching index entry)")

	doc1, err := coll.FindId(ctx, "d1")
	require.NoError(t, err)
	assert.Equal(t, float64(1), doc1.Value().Get("n").GetFloat64(),
		"d1.n must be incremented exactly once")

	doc2, err := coll.FindId(ctx, "d2")
	require.NoError(t, err)
	assert.Equal(t, float64(1), doc2.Value().Get("n").GetFloat64(),
		"d2.n must be incremented exactly once")
}

// TestAudit06_DeleteOnce_OverlappingArray:
// d1 has tags:["a","b","c"]. $in:["a","b"] matches d1 via two index
// entries. DeleteMany must remove d1 exactly once and report 1 deleted
// (not 2, and not error from a double-delete attempt).
func TestAudit06_DeleteOnce_OverlappingArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	res, err := coll.Find(`{"tags":{"$in":["a","b"]}}`).Delete(ctx)
	require.NoError(t, err, "Delete must not error from a double-delete attempt")
	assert.Equal(t, 1, res.Matched, "matched should be 1 (one doc), not 2 index hits")
	assert.Equal(t, 1, res.Modified, "deleted should be 1 (one doc), not 2 index hits")

	_, err = coll.FindId(ctx, "d1")
	assert.True(t, errors.Is(err, ErrDocNotFound), "d1 should be gone, got err=%v", err)

	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "collection should be empty")
}

// TestAudit06_DeleteOnce_TwoDocs:
// d1 tags=[a,b], d2 tags=[b,c]. $in:[a,b,c] matches both, with multiple
// matching entries for each. DeleteMany must remove each doc once,
// report 2 deleted, and leave the collection empty.
func TestAudit06_DeleteOnce_TwoDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
	))

	res, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Matched, "matched should be 2 distinct docs")
	assert.Equal(t, 2, res.Modified, "deleted should be 2 distinct docs (NOT one per matching index entry)")

	_, err = coll.FindId(ctx, "d1")
	assert.True(t, errors.Is(err, ErrDocNotFound), "d1 should be gone, got err=%v", err)
	_, err = coll.FindId(ctx, "d2")
	assert.True(t, errors.Is(err, ErrDocNotFound), "d2 should be gone, got err=%v", err)

	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "collection should be empty after deleting both docs")
}

// Audit 07: Sort + $in over a multi-key array index.
//
// When the index is on an array-valued field (e.g. tags), one index entry
// exists per array element per document. A query like
// {"tags":{"$in":[...]}} therefore visits the same docId multiple times
// via different index entries. SortIter must propagate the multiKey flag
// from upstream through the post-sort emission, and planIterator's
// consumer-side DocDedup must collapse duplicates AFTER the sort is
// applied — preserving the sort order on the deduplicated stream.
//
// The unit test TestSortIter_PreservesMultiKeyAcrossSort
// (internal/qplanner/sort_iter_test.go) pins the propagation invariant
// for SortIter alone. These tests pin the end-to-end invariant via the
// public coll.Find(...).Sort(...).Iter() API, ensuring the wire-up between
// the multi-key index, the sort, and the consumer-side dedup is correct.

// collectIdsString collects the "id" field (string-typed) from a query.
func collectIdsString(t testing.TB, q Query) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var out []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		out = append(out, string(doc.Value().GetStringBytes("id")))
	}
	require.NoError(t, iter.Err())
	return out
}

// TestAudit07_SortMultiKey_OrderPreserved verifies that a sort over a
// scalar field (priority) produces the docs in the correct ascending
// order even when the source is a multi-key $in over an array index
// (tags) that emits the same doc multiple times. The consumer-side
// DocDedup must collapse the duplicates AFTER the sort.
func TestAudit07_SortMultiKey_OrderPreserved(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	// Each doc overlaps with the others on tags, so the $in scan will
	// emit each docId at least twice via the multi-key index.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"priority":30}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"],"priority":10}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["a","c"],"priority":20}`),
	))

	q := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Sort("priority")

	ids := collectIdsString(t, q)
	assert.Equal(t, []string{"d2", "d3", "d1"}, ids,
		"docs must appear in ascending-priority order with no duplicates "+
			"after sort + consumer-side multi-key dedup")
}

// TestAudit07_SortMultiKey_DescendingOrder is the descending-order
// counterpart of OrderPreserved: the same data, but sorted by -priority.
// The post-sort dedup must yield (d1, d3, d2).
func TestAudit07_SortMultiKey_DescendingOrder(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"priority":30}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"],"priority":10}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["a","c"],"priority":20}`),
	))

	q := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Sort("-priority")

	ids := collectIdsString(t, q)
	assert.Equal(t, []string{"d1", "d3", "d2"}, ids,
		"docs must appear in descending-priority order with no duplicates")
}

// TestAudit07_SortMultiKey_WithLimit pins the (sort + $in over multi-key
// + Limit) interaction. The limit must apply to the deduplicated, sorted
// stream — not to the raw multi-key entry stream. With 3 distinct docs
// and Limit(2) ascending, the result must be the two smallest-priority
// docs (d2, d3), each appearing exactly once.
func TestAudit07_SortMultiKey_WithLimit(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"],"priority":30}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"],"priority":10}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["a","c"],"priority":20}`),
	))

	q := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Sort("priority").Limit(2)

	ids := collectIdsString(t, q)
	require.Len(t, ids, 2,
		"Limit(2) must apply to the post-dedup stream; expected exactly 2 distinct docs")
	assert.Equal(t, []string{"d2", "d3"}, ids,
		"top-2 by ascending priority on the deduplicated multi-key stream")
}

// TestAudit07_SortMultiKey_DocAppearsExactlyOnce stress-tests the
// invariant at scale: 50 docs each carry a "common" tag (so every doc
// matches the $in) plus a "uniq"+i tag (so the multi-key index emits
// every doc TWICE — once for "common", once for "uniqN"). The query
// filters only on "common", which still hits every doc twice if
// upstream uses the multi-key index entries (the whole-array entry +
// the per-element entry both match a single-bound scan in this codec
// — see audit 01). The consumer-side DocDedup must collapse these so
// every doc appears exactly once in the sorted output.
func TestAudit07_SortMultiKey_DocAppearsExactlyOnce(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	for i := 0; i < 50; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":"d%d","tags":["common","uniq%d"],"priority":%d}`, i, i, i%5,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	q := coll.Find(`{"tags":{"$in":["common"]}}`).Sort("priority")

	ids := collectIdsString(t, q)

	// All 50 docs must appear, each exactly once.
	require.Len(t, ids, 50,
		"every doc must appear exactly once after sort + consumer-side dedup")

	seen := make(map[string]int, 50)
	for _, id := range ids {
		seen[id]++
	}
	require.Len(t, seen, 50,
		"set of returned docIds must contain 50 distinct values")
	for id, n := range seen {
		assert.Equalf(t, 1, n,
			"doc %q must appear exactly once, saw it %d times", id, n)
	}

	// Also verify the sort order: priorities are i%5 ∈ [0,1,2,3,4],
	// so the emitted sequence (when read in order) must be non-decreasing.
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var prev float64 = -1
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		p := doc.Value().Get("priority").GetFloat64()
		assert.GreaterOrEqual(t, p, prev,
			"priority must be non-decreasing across the sorted+deduped stream")
		prev = p
	}
	require.NoError(t, iter.Err())
}

// Audit 10: Range queries ($gte/$lte) over a single-field multi-key
// (array-valued) index.
//
// Background — the question being audited:
//
//   A doc {tags:["a","b","c"]} on an index over `tags` produces multiple
//   raw index entries (one per array element + the whole-array entry).
//   For a single-bound range scan covering MULTIPLE distinct values held
//   by the same doc (e.g. $gte:"a", $lte:"c"), the
//   IndexIter.countEntriesBatch fast path uses cursor.CountUntil, which
//   counts entries by walking page headers — it does NOT read per-entry
//   value bytes and does NOT dedup. The within-doc dedup invariant
//   (insertKeys: each doc has ≤1 entry per distinct value per index)
//   only protects against duplicate entries for the SAME value; it does
//   not collapse the per-element entries the same doc contributes for
//   DIFFERENT values inside the range.
//
//   So if the planner ever fed Count() the raw IndexIter for a $gte/$lte
//   range, Count would over-report by the number of distinct array
//   elements per doc that fall in the range.
//
// What these tests verify (the actual end-to-end behaviour):
//
//   The planner does NOT route $gte/$lte to the raw IndexIter
//   CountEntries fast path. The covering count shortcut (planner.go:900)
//   is gated on idx.PointLookup, and a range bound is not PointLookup.
//   So the chain wrapping IndexIter is FetchIter[+FilterIter] +
//   CanonicalKeyDedupIter (single-field). The CanonicalKeyDedupIter
//   collapses per-doc duplicates at the boundary (it returns
//   multiKey=false for every emit), and Count's generic Next-loop
//   wrapper sees a clean unique-per-doc stream. Iter's planIterator.Next
//   benefits from the same dedup wrap.
//
//   Therefore Count and Iter both report the correct per-doc count even
//   though the raw index entry count would be larger. These tests pin
//   that observed behaviour. If a future refactor exposes the raw
//   IndexIter to Count for a non-PointLookup range scan, these tests
//   will start failing — and that's the regression signal we want.

// TestAudit10_RangeMultiKey_SingleDocCountAndIter verifies subtests 1 & 2:
//   - Count() on $gte:"a", $lte:"c" against a single doc with tags=[a,b,c]
//     returns 1 (NOT 3) — dedup happens.
//   - Iter() over the same range yields d1 exactly once — planIterator's
//     consumer-side DocDedup gate (or upstream CanonicalKeyDedupIter)
//     collapses the duplicates.
func TestAudit10_RangeMultiKey_SingleDocCountAndIter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit10_single")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	// Sanity: confirm the raw index actually holds multiple entries for d1
	// in the range — otherwise the test wouldn't be exercising the case
	// the audit is about. Expect 4 entries: one per element ("a","b","c")
	// plus the whole-array entry.
	rawEntries := readRawIndexEntries(t, fx.DB, "audit10_single", "ix_tags")
	require.Len(t, rawEntries, 4,
		"single doc with 3-element array must produce 4 raw index entries "+
			"(one per element + one whole-array). If this changes, the audit assumptions need re-checking.")

	t.Run("Count over $gte/$lte range collapses to one doc", func(t *testing.T) {
		count, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Count(ctx)
		require.NoError(t, err)
		// PINNED behaviour: Count returns 1 (the number of distinct
		// matching docs), NOT 3 (the number of in-range raw index entries
		// for d1) and NOT 4 (3 elements + whole-array entry).
		//
		// If this assertion ever fails with count==3 or count==4, it
		// means a planner change has exposed the raw IndexIter (or its
		// countEntriesBatch fast path) to Count for a range scan,
		// bypassing the CanonicalKeyDedupIter wrap. That would be a
		// correctness regression — Count() must report distinct docs.
		assert.Equal(t, 1, count,
			"$gte/$lte range over [a,c] on tags=[a,b,c] must count d1 exactly once, "+
				"not once per matching array element")
	})

	t.Run("Iter over $gte/$lte range yields the doc once", func(t *testing.T) {
		ids := collectIdsString(t, coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`))
		// PINNED behaviour: planIterator's dedup (either via
		// CanonicalKeyDedupIter wrap or the consumer-side DocDedup gate
		// in planIterator.Next) collapses the multiple in-range hits for
		// d1 to a single emission.
		assert.Equal(t, []string{"d1"}, ids,
			"Iter over $gte/$lte range must yield d1 exactly once, not once per matching array element")
	})
}

// TestAudit10_RangeMultiKey_PointRange covers subtest 3: a point lookup
// expressed as a degenerate range [b, b]. d1 has tags=[a,b], d2 has
// tags=[b,c], d3 has tags=[x,y]. The range [b,b] hits d1 (via "b") and
// d2 (via "b") and not d3. Both should appear exactly once.
//
// Note: a degenerate range $gte==$lte is normalised by the planner to a
// PointLookup bound (Start == End, both inclusive). When the index is
// covering and CountOnly is set, the planner CAN route this to the raw
// IndexIter.CountEntries fast path. countEntriesBatch will issue a
// single CountUntil for this bound. Within-doc dedup in insertKeys
// guarantees ≤1 entry per distinct value per doc, and "b" is a single
// distinct value — so the raw entry count equals the doc count even
// without value-byte reads. So even if the fast path fires, count==2
// is correct here.
func TestAudit10_RangeMultiKey_PointRange(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit10_point")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x","y"]}`),
	))

	t.Run("Count of [b,b] hits d1 and d2 once each", func(t *testing.T) {
		count, err := coll.Find(`{"tags":{"$gte":"b","$lte":"b"}}`).Count(ctx)
		require.NoError(t, err)
		// PINNED: 2 distinct docs match. d1 has one entry for "b"
		// (within-doc dedup invariant), d2 has one entry for "b". d3
		// has no "b". The raw entry count in [b,b] is 2 = the doc
		// count, so even the fast path would be correct here.
		assert.Equal(t, 2, count,
			"point range [b,b] must count d1 and d2 exactly once each (got %d)", count)
	})

	t.Run("Iter of [b,b] yields d1 and d2", func(t *testing.T) {
		ids := collectIdsString(t, coll.Find(`{"tags":{"$gte":"b","$lte":"b"}}`))
		// Order: by index key then docId. Both keys are "b"; docIds are
		// "d1","d2" (sorted lexically). Either order would be acceptable
		// in principle, but our index emits in key+docId order so it's
		// stable.
		assert.ElementsMatch(t, []string{"d1", "d2"}, ids,
			"Iter over [b,b] must yield exactly {d1, d2}")
		assert.Len(t, ids, 2, "no duplicates")
	})
}

// TestAudit10_RangeMultiKey_RangeAcrossMultipleDocs covers subtest 4:
// the canonical multi-doc range case. Range [a, c]: d1 (tags=[a,b])
// matches via "a" and "b"; d2 (tags=[b,c]) matches via "b" and "c"; d3
// (tags=[x,y]) does not match. Count must be 2 distinct docs (NOT 4
// raw entries: a/d1, b/d1, b/d2, c/d2 + whole-array entries).
func TestAudit10_RangeMultiKey_RangeAcrossMultipleDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit10_range_multi")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x","y"]}`),
	))

	// Sanity: verify that a naive count of raw entries in the range
	// would over-report. d1 contributes entries at keys "a" and "b"
	// (plus whole-array entry — but the whole-array entry is ["a","b"]
	// which sorts AFTER "b" in any-enc array encoding, so it may or may
	// not fall in [a,c]). We don't pin the exact raw count here — it's
	// fine for that to evolve — we just verify it's >2 (more than one
	// per doc) so the test is meaningful.
	rawEntries := readRawIndexEntries(t, fx.DB, "audit10_range_multi", "ix_tags")
	// d1: 2 elements + 1 whole = 3 entries
	// d2: 2 elements + 1 whole = 3 entries
	// d3: 2 elements + 1 whole = 3 entries
	// total: 9
	require.Len(t, rawEntries, 9,
		"3 docs with 2-element arrays each must produce 9 raw index entries "+
			"(per doc: 2 elements + 1 whole-array)")

	t.Run("Count over [a,c] returns 2 distinct docs", func(t *testing.T) {
		count, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Count(ctx)
		require.NoError(t, err)
		// PINNED: 2 distinct docs. Both d1 and d2 have multiple matching
		// entries in the range; if dedup failed we'd see ≥4. This is
		// the canonical assertion of the audit.
		assert.Equal(t, 2, count,
			"$gte/$lte range [a,c] must count d1 and d2 exactly once each "+
				"(got %d) — d3 has no matching tags", count)
	})

	t.Run("Iter over [a,c] yields d1 and d2 exactly once each", func(t *testing.T) {
		ids := collectIdsString(t, coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`))
		assert.ElementsMatch(t, []string{"d1", "d2"}, ids,
			"Iter over [a,c] must yield exactly {d1, d2}")
		assert.Len(t, ids, 2, "Iter must dedup; no duplicates allowed")
	})
}

// TestAudit10_RangeMultiKey_ReverseRange covers subtest 5: reverse
// direction (Sort("-tags")) — the dedup must work in reverse too.
//
// CanonicalKeyDedupIter has a Reverse field (matched against
// IndexIter.Reverse): in reverse scans the canonical representative is
// the MAX in-range array element; in forward scans it's the MIN. The
// gating logic ensures one emission per doc regardless of direction.
func TestAudit10_RangeMultiKey_ReverseRange(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit10_reverse")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x","y"]}`),
	))

	t.Run("Iter with Sort(-tags) over [a,c] dedups in reverse", func(t *testing.T) {
		ids := collectIdsString(t,
			coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Sort("-tags"))
		// PINNED: each doc appears exactly once. Reverse scan canonical
		// = MAX in-range tag. d2's MAX is "c"; d1's MAX is "b". So the
		// emission order in a reverse canonical-dedup scan is d2 first
		// (canonical "c"), then d1 (canonical "b"). d3 is not in range.
		assert.Equal(t, []string{"d2", "d1"}, ids,
			"reverse range Iter must yield d2 then d1, each exactly once "+
				"(reverse canonical = max in-range element)")
	})

	t.Run("Count over [a,c] is direction-independent", func(t *testing.T) {
		// Sort doesn't influence Count's filter, but exercise the path
		// to make sure no count regression sneaks in via a
		// reverse-direction code branch.
		count, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Sort("-tags").Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, count,
			"Count is order-invariant: reverse-sorted range must still report 2 distinct docs")
	})

	t.Run("explain confirms IndexScan + dedup wrap on the reverse scan", func(t *testing.T) {
		explain, err := coll.Find(`{"tags":{"$gte":"a","$lte":"c"}}`).Sort("-tags").Explain(ctx)
		require.NoError(t, err)
		// The plan should use the index (IndexScan or IndexSeek), not a
		// full-collection scan + sort. We don't pin the exact SQL string
		// — it can evolve — but it MUST contain "tags" (the index name)
		// and MUST NOT contain the in-memory sort iterator, which
		// SortIter.String renders as "-> Sort" (or "-> TopK(n)" when a
		// Limit bounds it). Either would mean the index didn't provide order.
		assert.Contains(t, explain.Sql, "tags",
			"plan must use the tags index for a range query on tags; got: %s", explain.Sql)
		assert.NotContains(t, explain.Sql, "-> Sort",
			"reverse scan must use index order, not an in-memory Sort: %s", explain.Sql)
		assert.NotContains(t, explain.Sql, "TopK",
			"reverse scan must use index order, not an in-memory TopK sort: %s", explain.Sql)
	})
}

// TestAudit10_RangeMultiKey_LargeArray pins the multi-element-array
// case at small scale: 10 docs each with a 5-element array fully inside
// the range. Tests the "obvious over-count" scenario — without dedup,
// a count of 50 would be expected (5 entries per doc × 10 docs); with
// dedup, the count is 10.
func TestAudit10_RangeMultiKey_LargeArray(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit10_large_array")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	for i := 0; i < 10; i++ {
		// All 10 docs share the same 5 tag values, all in [a, e].
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":"d%d","tags":["a","b","c","d","e"]}`, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"tags":{"$gte":"a","$lte":"e"}}`).Count(ctx)
	require.NoError(t, err)
	// PINNED: 10 distinct docs. Without dedup we would see 50 (5 element
	// entries per doc × 10 docs) or 60 (with whole-array entries).
	assert.Equal(t, 10, count,
		"$gte/$lte range over 10 docs each with 5 in-range tags must count 10 distinct docs, "+
			"not 50 raw element entries")

	ids := collectIdsString(t, coll.Find(`{"tags":{"$gte":"a","$lte":"e"}}`))
	assert.Len(t, ids, 10, "Iter must yield 10 distinct docs")
	seen := make(map[string]int, 10)
	for _, id := range ids {
		seen[id]++
	}
	require.Len(t, seen, 10, "all 10 distinct docIds must appear")
	for id, n := range seen {
		assert.Equalf(t, 1, n, "doc %q must appear exactly once, saw %d", id, n)
	}
}

// TestAudit11_*: degenerate `$in` shapes on a multi-key index.
//
// The dedup pipeline (any-store-tests:docs/any-store/plans/2026-04-29-multikey-bit-and-dedup-pipeline.md)
// splits IndexIter.CountEntries on len(Bounds):
//
//	len(Bounds) <= 1: page-batch CountUntil  (no per-entry walk, no seen-set)
//	len(Bounds) >  1: peek-then-batch with seen-set dedup on docId
//
// Two boundary cases are not exercised today and are pinned here:
//
//   - len(Bounds) == 0: empty $in:[]. No bounds → countEntriesBatch's loop
//     iterates zero times → returns 0. The planner separately rejects an
//     index with 0 bounds (Plan B `if len(idx.Bounds) == 0 { continue }`),
//     so the query falls back to fullscan, where In.Ok over an empty Values
//     map returns false for every doc → also 0. Either way: must be 0, no err.
//
//   - len(Bounds) == 1: singleton $in:[x]. Single-bound CountEntries hits
//     the page-batch fast path with NO per-entry walk and NO seen-set. The
//     in-doc dedup invariant (insertKeys writes ≤1 entry per (doc, distinct
//     value) on a single-field array index) is what keeps the count correct
//     here — a doc whose tags array contains "b" twice (e.g. ["b","b"]) must
//     still count as 1, and a doc whose tags is ["a","b","c"] queried with
//     $in:["b"] must produce a single index entry under "b" (because each
//     distinct array value yields exactly one entry). That invariant is
//     covered elsewhere; these tests pin the consumer-side single-bound
//     fast path: if anyone changes countEntriesBatch into something that
//     misreads value bytes, double-counts within-bound, or special-cases
//     multi-key entries badly, these tests catch it.

// TestAudit11_EmptyIn_Count: empty $in array must yield Count = 0 with no
// error. Pins the len(Bounds)==0 branch of CountEntries (and the planner's
// fallback to fullscan + In.Ok-over-empty-map).
func TestAudit11_EmptyIn_Count(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit11_empty_count")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x","y"]}`),
	))

	n, err := coll.Find(`{"tags":{"$in":[]}}`).Count(ctx)
	require.NoError(t, err,
		"empty $in must not error: filter parses to In{Values:{}} which "+
			"is a valid (always-false) filter; index simply contributes "+
			"zero bounds, planner falls back to fullscan")
	assert.Equal(t, 0, n,
		"empty $in must match zero docs (got %d): nothing belongs to the "+
			"empty set. A non-zero result would mean either the filter is "+
			"being treated as 'match all' or the count loop is over-tallying.", n)
}

// TestAudit11_EmptyIn_Iter: same data — Iter over $in:[] must yield zero
// docs (no error). Exercises planIterator.Next on the zero-bounds path.
func TestAudit11_EmptyIn_Iter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit11_empty_iter")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d3","tags":["x","y"]}`),
	))

	ids := collectField(t, coll.Find(`{"tags":{"$in":[]}}`), "id")
	assert.Empty(t, ids,
		"Iter over $in:[] must yield zero docs (got %v); the empty-set "+
			"filter must reject everything regardless of how the planner "+
			"chooses to execute it", ids)
}

// TestAudit11_SingletonIn_Count_MultiKeyDoc: $in with exactly one value on a
// multi-key (array) field. Hits the single-bound page-batch CountEntries
// path. The doc has tags:["a","b","c"] — the index emits 3 entries, but the
// query touches only the bound covering "b", so CountUntil over that bound
// must return 1.
func TestAudit11_SingletonIn_Count_MultiKeyDoc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit11_singleton_count")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	n, err := coll.Find(`{"tags":{"$in":["b"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"singleton $in:['b'] over array tags must count d1 exactly once "+
			"(got %d). The single-bound CountEntries fast path has no "+
			"seen-set; correctness here depends on within-doc dedup at "+
			"insert time (each distinct array value → exactly one entry).", n)
}

// TestAudit11_SingletonIn_Iter_MultiKeyDoc: same data as the Count case —
// Iter must yield d1 exactly once. Exercises planIterator.Next on a single
// bound; even with no seen-set in CountEntries, the per-doc DocDedup in the
// iterator path catches any duplicate emissions.
func TestAudit11_SingletonIn_Iter_MultiKeyDoc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit11_singleton_iter")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	ids := collectField(t, coll.Find(`{"tags":{"$in":["b"]}}`), "id")
	assert.Equal(t, []string{`"d1"`}, ids,
		"Iter over $in:['b'] (singleton) on multi-key tags must yield d1 "+
			"exactly once (got %v). A duplicate would mean the iterator "+
			"is leaking the within-doc invariant or the consumer-side "+
			"DocDedup is mis-keyed for the single-bound path.", ids)
}

// TestAudit11_SingletonIn_NoMatch: singleton $in whose value is absent from
// the index. Single-bound CountEntries seeks the start, finds no matching
// entries before the end bound, and returns 0.
func TestAudit11_SingletonIn_NoMatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit11_singleton_nomatch")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
	))

	n, err := coll.Find(`{"tags":{"$in":["zzz"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n,
		"singleton $in with no matching value must count to 0 (got %d). "+
			"A non-zero result would mean the bound's End-check is wrong, "+
			"or the seek/peek logic accepts an out-of-range entry.", n)

	// Also pin Iter on the same shape — same expected outcome, different
	// consumer code (planIterator.Next vs. Count's loop).
	ids := collectField(t, coll.Find(`{"tags":{"$in":["zzz"]}}`), "id")
	assert.Empty(t, ids,
		"Iter over singleton $in with no match must yield zero docs (got %v)", ids)
}

// TestAudit11_SingletonIn_MultipleDocs: 5 docs each with tags:["common", "uniq"+i].
// Singleton $in:["common"] must count to 5 and Iter must yield all 5 distinct
// docs each exactly once. This is the canonical single-bound case where many
// docs share one value — CountEntries' page-batch CountUntil tallies all 5
// entries directly (still no seen-set needed), and within-doc dedup at insert
// time guarantees each (doc, "common") is exactly one entry.
func TestAudit11_SingletonIn_MultipleDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit11_singleton_multidoc")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	const N = 5
	for i := 0; i < N; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":"d%d","tags":["common","uniq%d"]}`, i, i),
		)))
	}

	n, err := coll.Find(`{"tags":{"$in":["common"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, N, n,
		"singleton $in:['common'] must count all %d docs (got %d). Each "+
			"doc has 'common' as one of its tags; within-doc dedup ensures "+
			"each contributes exactly one entry under 'common'.", N, n)

	ids := collectField(t, coll.Find(`{"tags":{"$in":["common"]}}`), "id")
	require.Len(t, ids, N,
		"Iter over singleton $in:['common'] must yield exactly %d docs "+
			"(got %d: %v); duplicates would indicate broken consumer-side "+
			"DocDedup on the single-bound path", N, len(ids), ids)

	sort.Strings(ids)
	expected := make([]string, N)
	for i := 0; i < N; i++ {
		expected[i] = fmt.Sprintf(`"d%d"`, i)
	}
	sort.Strings(expected)
	assert.Equal(t, expected, ids,
		"Iter must yield d0..d%d each exactly once (got %v)", N-1, ids)
}

// ── Single-index audit (act-01/03/04) ──
/*
Audit tests for the "single-index" domain (any-store-tests:docs/any-store/qplanner/audit/actionable_by_domain.json).

  act-01  $ne on an indexed scalar uses a two-bound seek and still includes
          null/missing (non-sparse): the index IS used (IndexScan, not FullScan),
          value 5 is excluded, and null/missing docs survive the residual Filter.
  act-03  Cross-type ordering: equality is type-strict (number 5 != string "5");
          $gte:0 sweeps strings/bools because number-0 sorts before them; the
          ascending sort order is Null<Number<String<False<True.
  act-04  Negative numbers round-trip through the order-preserving encoding for
          equality, two-sided range and asc/desc sort.

All helpers used here (newFixture, collectField, collectIdsString, assertIndexLen)
are defined elsewhere in the package test suite and reused as-is.
*/


// act-01
func TestIndex_Single_Ne_TwoBoundSeek_IncludesNullAndMissing(t *testing.T) {
	// Builds a collection; when withIndex is true a non-sparse index {a} is added.
	mk := func(withIndex bool, docs ...string) Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "c")
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		}
		for _, d := range docs {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
		}
		return coll
	}

	sortedIds := func(c Collection, filter string) []string {
		ids := collectIdsString(t, c.Find(filter).Sort("id"))
		sort.Strings(ids)
		return ids
	}

	t.Run("null and missing survive $ne", func(t *testing.T) {
		// id values are JSON strings so collectIdsString (GetStringBytes) resolves them.
		docs := []string{
			`{"id":"1","a":5}`,
			`{"id":"2","a":7}`,
			`{"id":"3","a":null}`,
			`{"id":"4"}`, // missing a
		}
		idx := mk(true, docs...)
		noidx := mk(false, docs...)

		// (1) Count == 3, identical with and without the index.
		idxCount, err := idx.Find(`{"a":{"$ne":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, idxCount)
		noidxCount, err := noidx.Find(`{"a":{"$ne":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, idxCount, noidxCount)

		// (2) Same id set for indexed and unindexed: 2 (a=7), 3 (null), 4 (missing).
		want := []string{"2", "3", "4"}
		assert.ElementsMatch(t, want, sortedIds(idx, `{"a":{"$ne":5}}`))
		assert.ElementsMatch(t, want, sortedIds(noidx, `{"a":{"$ne":5}}`))

		// (3) Both null and missing are indexed (non-sparse) => 4 entries.
		assertIndexLen(t, idx.GetIndexes()[0], 4)

		// (4) Explain: index IS used via the two-bound seek, with a residual Filter.
		explain, err := idx.Find(`{"a":{"$ne":5}}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan(a)")
		assert.Contains(t, explain.Sql, "[-inf,'5'),('5',inf]")
		assert.Contains(t, explain.Sql, "-> Filter")
		assert.NotContains(t, explain.Sql, "FullScan")
	})

	t.Run("dense 0..9 dataset", func(t *testing.T) {
		var docs []string
		for i := 0; i < 10; i++ {
			docs = append(docs, fmt.Sprintf(`{"id":"%d","a":%d}`, i, i))
		}
		idx := mk(true, docs...)
		noidx := mk(false, docs...)

		// $ne:5 excludes exactly a=5; ascending a-values are 0,1,2,3,4,6,7,8,9.
		gotIdx := collectField(t, idx.Find(`{"a":{"$ne":5}}`).Sort("a"), "a")
		gotNoidx := collectField(t, noidx.Find(`{"a":{"$ne":5}}`).Sort("a"), "a")
		want := []string{"0", "1", "2", "3", "4", "6", "7", "8", "9"}
		assert.Equal(t, want, gotIdx)
		assert.Equal(t, want, gotNoidx)

		cnt, err := idx.Find(`{"a":{"$ne":5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 9, cnt)
	})
}

// act-03
func TestIndex_Single_MixedTypeOrdering(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","a":5}`),
		anyenc.MustParseJson(`{"id":"2","a":"5"}`),
		anyenc.MustParseJson(`{"id":"3","a":"hello"}`),
		anyenc.MustParseJson(`{"id":"4","a":true}`),
	))

	// Equality is type-strict: number 5 matches only the numeric doc.
	cnt, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, []string{"1"}, collectIdsString(t, coll.Find(`{"a":5}`)))

	// String "5" matches only the string doc.
	cnt, err = coll.Find(`{"a":"5"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, []string{"2"}, collectIdsString(t, coll.Find(`{"a":"5"}`)))

	// $gte:0 sweeps all four docs (number 0 sorts before all strings/bools).
	cnt, err = coll.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, cnt)

	// Ascending sort order: Number(5) < String("5") < String("hello") < True.
	assert.Equal(t,
		[]string{"5", "\"5\"", "\"hello\"", "true"},
		collectField(t, coll.Find(nil).Sort("a"), "a"),
	)
	// And the corresponding id order.
	assert.Equal(t,
		[]string{"1", "2", "3", "4"},
		collectIdsString(t, coll.Find(nil).Sort("a")),
	)
}

// act-04
func TestIndex_Single_NegativeNumbers(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for _, v := range []int{-5, -3, -1, 0, 2, 4} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, v+100, v))))
	}

	// (1) Two-sided range across the sign boundary, ascending.
	assert.Equal(t,
		[]string{"-3", "-1", "0"},
		collectField(t, coll.Find(`{"a":{"$gte":-3,"$lt":2}}`).Sort("a"), "a"),
	)

	// (2) Full ascending sort across the sign boundary.
	assert.Equal(t,
		[]string{"-5", "-3", "-1", "0", "2", "4"},
		collectField(t, coll.Find(nil).Sort("a"), "a"),
	)

	// (3) Equality on a negative value.
	cnt, err := coll.Find(`{"a":-3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)

	// (4) Full descending sort across the sign boundary.
	assert.Equal(t,
		[]string{"4", "2", "0", "-1", "-3", "-5"},
		collectField(t, coll.Find(nil).Sort("-a"), "a"),
	)
}

// ── Limit/offset audit (act-25/26/27/28) ──
// act-25: Offset >= result size on the in-memory sort (TopK) path returns an
// empty window with no error. Forces the FullScan -> TopK -> Limit path by
// leaving the sort field UNINDEXED (adding an index on "a" would degrade to the
// index-ordered ExactSort path with a cursor-level skip instead).
func TestIndex_LimitOffset_OffsetLargerThanResultSet_InMemorySort(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "noidx")
	require.NoError(t, err)
	// NO index on "a" on purpose.

	for i := 0; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	// local closure: collect the int "a" field, asserting no iterator error.
	collectA := func(q Query) []int {
		t.Helper()
		iter, err := q.Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var out []int
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			out = append(out, int(d.Value().GetInt("a")))
		}
		require.NoError(t, iter.Err())
		return out
	}

	// (1) Offset == N (boundary) -> empty window, in-memory TopK path.
	t.Run("offset_equals_size", func(t *testing.T) {
		ex, err := coll.Find(nil).Sort("a").Offset(10).Limit(5).Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FullScan(filtered) -> TopK(15) -> Limit(offset=10,limit=5)", ex.Sql)

		got := collectA(coll.Find(nil).Sort("a").Offset(10).Limit(5))
		assert.Len(t, got, 0)
	})

	// (2) Offset well past the end -> still empty, no error.
	t.Run("offset_past_end", func(t *testing.T) {
		got := collectA(coll.Find(nil).Sort("a").Offset(100).Limit(5))
		assert.Len(t, got, 0)
	})

	// (3) Limit larger than the result set -> everything, in order, TopK(100).
	t.Run("limit_larger_than_result", func(t *testing.T) {
		ex, err := coll.Find(nil).Sort("a").Limit(100).Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FullScan(filtered) -> TopK(100) -> Limit(100)", ex.Sql)

		got := collectA(coll.Find(nil).Sort("a").Limit(100))
		assert.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, got)
	})

	// (4) Offset == N-1 -> exactly the last element survives.
	t.Run("offset_n_minus_one", func(t *testing.T) {
		got := collectA(coll.Find(nil).Sort("a").Offset(9).Limit(5))
		assert.Equal(t, []int{9}, got)
	})
}

// act-26: TopK-heap sort stability/determinism with duplicate keys under
// offset+limit on a NON-indexed sort. collectAndSort appends the unique docId
// after the sort key so the heap order is a total order: eviction is
// deterministic and pagination yields no duplicate/missing ids across pages.
func TestIndex_LimitOffset_TopKStability_DuplicateKeys_NonIndexedSort(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "noidx")
	require.NoError(t, err)
	// NO index: forces the in-memory TopK/Sort path.

	// 30 docs, a = i/10 -> 3 groups of 10 identical sort keys (0,1,2).
	for i := 0; i < 30; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i/10))))
	}

	collectIDs := func(q Query) []int {
		t.Helper()
		iter, err := q.Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var out []int
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			out = append(out, int(d.Value().GetInt("id")))
		}
		require.NoError(t, iter.Err())
		return out
	}

	// Baseline: full ordered stream (unbounded in-memory Sort).
	full := collectIDs(coll.Find(nil).Sort("a"))
	require.Len(t, full, 30)

	// Determinism: a windowed query with active TopK eviction (TopK=15) must be
	// identical across repeated runs.
	t.Run("deterministic_eviction", func(t *testing.T) {
		// Confirm the eviction-active TopK path is taken (15 < 30 rows).
		ex, err := coll.Find(nil).Sort("a").Offset(7).Limit(8).Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FullScan(filtered) -> TopK(15) -> Limit(offset=7,limit=8)", ex.Sql)

		run1 := collectIDs(coll.Find(nil).Sort("a").Offset(7).Limit(8))
		run2 := collectIDs(coll.Find(nil).Sort("a").Offset(7).Limit(8))
		require.Len(t, run1, 8)
		assert.Equal(t, run1, run2)
		// And the window must match the same slice of the full ordered stream.
		assert.Equal(t, full[7:15], run1)
	})

	// Pagination across the full set in 3 pages of 10 reconstructs `full`
	// exactly: no duplicate/missing ids, even with the duplicate sort keys.
	t.Run("pagination_no_gaps_no_dups", func(t *testing.T) {
		var paged []int
		for _, off := range []int{0, 10, 20} {
			window := collectIDs(coll.Find(nil).Sort("a").Offset(uint(off)).Limit(10))
			require.Lenf(t, window, 10, "window at offset %d", off)
			paged = append(paged, window...)
		}
		assert.Equal(t, full, paged)
	})
}

// act-27: Filter + offset on the in-memory sort path skips filtered+sorted
// rows, never raw scan rows. The sort field "b" is unindexed so the offset is
// applied by LimitIter ABOVE the filtered+sorted stream (TopK), not as a
// cursor-level skip leaking below the filter.
func TestIndex_LimitOffset_FilterOffset_InMemorySort_SkipsFilteredRows(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	// Index only on "a"; the sort field "b" is intentionally unindexed.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// 40 docs: a=i, b=40-i. Filter a>=20 keeps a=20..39 -> b=1..20.
	for i := 0; i < 40; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, 40-i))))
	}

	const filter = `{"a":{"$gte":20}}`

	// Full filtered+sorted reference: b ascending over the kept rows.
	allFiltered := collectIntField(t, coll.Find(filter).Sort("b"), "b")
	require.Len(t, allFiltered, 20)
	// Sanity: the kept rows are exactly b = 1..20 ascending.
	wantAll := make([]int, 20)
	for i := range wantAll {
		wantAll[i] = i + 1
	}
	require.Equal(t, wantAll, allFiltered)

	// The window [offset 5, limit 5] must equal allFiltered[5:10] == [6,7,8,9,10].
	window := collectIntField(t, coll.Find(filter).Sort("b").Offset(5).Limit(5), "b")
	assert.Equal(t, allFiltered[5:10], window)
	assert.Equal(t, []int{6, 7, 8, 9, 10}, window)

	// Explain: offset is applied above the TopK by LimitIter (never as a
	// cursor-level "skip=" below the filter).
	ex, err := coll.Find(filter).Sort("b").Offset(5).Limit(5).Explain(ctx)
	require.NoError(t, err)
	assert.Equal(t,
		"IndexScan(a)[bounds=Bounds{['20',inf]}] -> Fetch -> Filter -> Dedup(canonical) -> TopK(10) -> Limit(offset=5,limit=5)",
		ex.Sql)
	assert.NotContains(t, ex.Sql, "skip=")
	assert.Contains(t, ex.Sql, "TopK")
}

// act-28: Explain token contract for sort+limit.
//   - index-covered sort+limit: neither Sort nor TopK (just Limit).
//   - non-indexed sort+limit: TopK(limit+offset), never Sort.
//   - non-indexed unlimited sort: Sort, never TopK.
func TestIndex_LimitOffset_ExplainTokens_TopKvsSort(t *testing.T) {
	fx := newFixture(t)

	// (1) Indexed sort + limit -> IndexScan, neither TopK nor Sort.
	t.Run("indexed_sort_limit_no_topk_no_sort", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "idx")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}

		ex, err := coll.Find(nil).Sort("a").Limit(5).Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, "IndexScan(a) -> Fetch -> Dedup(canonical) -> Limit(5)", ex.Sql)
		assert.Contains(t, ex.Sql, "IndexScan(a)")
		assert.NotContains(t, ex.Sql, "TopK")
		assert.NotContains(t, ex.Sql, "-> Sort")

		// And the values are the smallest 5, in order.
		vals := collectIntField(t, coll.Find(nil).Sort("a").Limit(5), "a")
		assert.Equal(t, []int{0, 1, 2, 3, 4}, vals)
	})

	// (2) Non-indexed sort + limit -> TopK(5), never Sort.
	t.Run("nonindexed_sort_limit_topk", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "noidx")
		require.NoError(t, err)
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}

		ex, err := coll.Find(nil).Sort("a").Limit(5).Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FullScan(filtered) -> TopK(5) -> Limit(5)", ex.Sql)
		assert.Contains(t, ex.Sql, "TopK(5)")
		assert.NotContains(t, ex.Sql, "-> Sort")

		vals := collectIntField(t, coll.Find(nil).Sort("a").Limit(5), "a")
		assert.Equal(t, []int{0, 1, 2, 3, 4}, vals)
	})

	// (3) Non-indexed unlimited sort -> Sort, never TopK.
	t.Run("nonindexed_unlimited_sort", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "noidx2")
		require.NoError(t, err)
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}

		ex, err := coll.Find(nil).Sort("a").Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FullScan(filtered) -> Sort", ex.Sql)
		assert.Contains(t, ex.Sql, "-> Sort")
		assert.NotContains(t, ex.Sql, "TopK")
	})

	// (4) Non-indexed sort + offset + limit -> TopK(limit+offset).
	t.Run("nonindexed_sort_offset_limit_topk_sum", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "noidx3")
		require.NoError(t, err)
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
		}

		ex, err := coll.Find(nil).Sort("-a").Offset(5).Limit(5).Explain(ctx)
		require.NoError(t, err)
		assert.Equal(t, "FullScan(filtered) -> TopK(10) -> Limit(offset=5,limit=5)", ex.Sql)
		assert.Contains(t, ex.Sql, "TopK(10)")
		assert.NotContains(t, ex.Sql, "-> Sort")
	})
}

// ── Array/nested audit (act-02/29/30/31/32) ──
// act-02: $ne over a multikey (array) index performs a two-bound negation
// seek that visits a straddling element via BOTH bounds; CanonicalKeyDedup
// emits it exactly once. The residual FilterIter applies all-elements $ne
// semantics. $nin desugars to Nor and yields NO index bounds, so it must
// FullScan even when given a max-Boost IndexHint. In every case the indexed
// result equals the fullscan result.
func TestIndex_ArrayNested_NeOverMultiKey_DedupAndAgreement(t *testing.T) {
	sortedIds := func(ids []string) []string {
		out := append([]string(nil), ids...)
		sort.Strings(out)
		return out
	}

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	// id3 straddles the negated value "a": "A" < "a" < "z", so both the
	// lower bound [-inf,"a") and the upper bound ("a",inf] visit it.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"3","tags":["A","z"]}`),
		anyenc.MustParseJson(`{"id":"4","tags":"a"}`),
		anyenc.MustParseJson(`{"id":"5","tags":"d"}`),
	))

	// (a) Explain: index IS used with the two-bound split, and the chain
	// ends in Dedup(canonical). Identical with and without an IndexHint.
	neExplain, err := coll.Find(`{"tags":{"$ne":"a"}}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, neExplain.Sql, "IndexScan(tags)")
	assert.Contains(t, neExplain.Sql, "Dedup(canonical)")
	assert.Contains(t, neExplain.Sql, `[-inf,'"a"'),('"a"',inf]`)

	neHintExplain, err := coll.Find(`{"tags":{"$ne":"a"}}`).
		IndexHint(IndexHint{IndexName: "tags", Boost: 1000000}).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, neHintExplain.Sql, "IndexScan(tags)")
	assert.Contains(t, neHintExplain.Sql, "Dedup(canonical)")
	assert.Contains(t, neHintExplain.Sql, `[-inf,'"a"'),('"a"',inf]`)

	// (b) Count == 3 and == fullscan count (no-index twin).
	neCount, err := coll.Find(`{"tags":{"$ne":"a"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, neCount)

	fxNo := newFixture(t)
	collNo, err := fxNo.CreateCollection(ctx, "test_noidx")
	require.NoError(t, err)
	require.NoError(t, collNo.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"2","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"3","tags":["A","z"]}`),
		anyenc.MustParseJson(`{"id":"4","tags":"a"}`),
		anyenc.MustParseJson(`{"id":"5","tags":"d"}`),
	))
	neCountNo, err := collNo.Find(`{"tags":{"$ne":"a"}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, neCount, neCountNo)

	// (c) Iter ids: raw scan order is 3,2,5; compare as a sorted set.
	// id3 the straddler appears exactly once; id1 and id4 (contain "a") excluded.
	neIds := collectIdsString(t, coll.Find(`{"tags":{"$ne":"a"}}`))
	assert.Len(t, neIds, 3) // exactly once each — no duplicate straddler
	assert.Equal(t, []string{"2", "3", "5"}, sortedIds(neIds))
	neIdsNo := collectIdsString(t, collNo.Find(`{"tags":{"$ne":"a"}}`))
	assert.Equal(t, sortedIds(neIds), sortedIds(neIdsNo))

	// $nin desugars to Nor: NO index bounds, must FullScan even when hinted.
	ninExplain, err := coll.Find(`{"tags":{"$nin":["a"]}}`).
		IndexHint(IndexHint{IndexName: "tags", Boost: 1000000}).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ninExplain.Sql, "FullScan")
	assert.NotContains(t, ninExplain.Sql, "IndexScan")

	ninCount, err := coll.Find(`{"tags":{"$nin":["a"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, ninCount)
	ninIds := collectIdsString(t, coll.Find(`{"tags":{"$nin":["a"]}}`))
	assert.Len(t, ninIds, 3)
	assert.Equal(t, []string{"2", "3", "5"}, sortedIds(ninIds))
}

// act-29: A nested index path that crosses an array intermediate is NOT
// implicitly traversed. Value.Get does strconv.Atoi on the segment after the
// array; the non-numeric "name" fails -> nil -> one 'null' entry (non-sparse).
// Positional access (items.0.name) resolves via the numeric index but is a
// different, unindexed path.
func TestIndex_ArrayNested_NestedField_IntermediateArray_NotTraversed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "items.name", Fields: []string{"items.name"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"items":[{"name":"a"},{"name":"b"}]}`),
		anyenc.MustParseJson(`{"id":2,"items":[{"name":"c"}]}`),
	))

	// Each doc contributes exactly one 'null' entry: the array intermediate
	// stops traversal so the indexed value is null for both docs.
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 2)

	// No implicit array traversal: items.name="a" finds nothing. This query
	// still IndexScans the items.name index (which holds only null entries).
	cnt, err := coll.Find(`{"items.name":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, cnt)

	// Positional access works (resolves via the numeric index). This is a
	// different, unindexed path -> FullScan; do not assert IndexScan.
	posCnt, err := coll.Find(`{"items.0.name":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, posCnt)

	// Both docs are findable via the shared 'null' entry.
	nullCnt, err := coll.Find(`{"items.name":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, nullCnt)
}

// act-30: Querying by a whole NON-empty array value uses the post-loop
// whole-array index entry as a single, order-sensitive point bound.
func TestIndex_ArrayNested_WholeArrayEquality_UsesIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"2","tags":["a"]}`),
	))

	// Whole-array equality uses the index with a single point bound on the
	// order-sensitive whole-array encoding.
	wholeExplain, err := coll.Find(`{"tags":["a","b"]}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, wholeExplain.Sql, "IndexScan(tags)")
	assert.Contains(t, wholeExplain.Sql, `['["a","b"]','["a","b"]']`)

	cnt, err := coll.Find(`{"tags":["a","b"]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt)
	assert.Equal(t, []string{"1"}, collectIdsString(t, coll.Find(`{"tags":["a","b"]}`)))

	// Order-sensitive: ["b","a"] is a different encoding -> no match.
	revCnt, err := coll.Find(`{"tags":["b","a"]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, revCnt)

	// Single-element whole array matches only its own doc.
	oneCnt, err := coll.Find(`{"tags":["a"]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, oneCnt)
	assert.Equal(t, []string{"2"}, collectIdsString(t, coll.Find(`{"tags":["a"]}`)))
}

// act-31: A null ARRAY ELEMENT is indexed as a real element (the sparse
// guard only short-circuits whole-field null), so it survives even on a
// sparse index. On a non-sparse index a null element makes a doc
// indistinguishable from a missing-field doc by a {tags:null} query.
// Duplicate nulls dedup within a doc.
func TestIndex_ArrayNested_NullElementInArray_IndexedAndQueryable(t *testing.T) {
	// Sub A: non-sparse.
	fxA := newFixture(t)
	collA, err := fxA.CreateCollection(ctx, "ns")
	require.NoError(t, err)
	require.NoError(t, collA.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))
	require.NoError(t, collA.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["a",null,"b"]}`), // null,a,b,whole-array = 4
		anyenc.MustParseJson(`{"id":2}`),                       // missing -> 1 null
	))
	assertIndexLen(t, collA.GetIndexes()[0], 5)

	// {tags:null} matches BOTH the null-element doc and the missing-field doc.
	nullA, err := collA.Find(`{"tags":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, nullA)
	aA, err := collA.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, aA)

	// Sub B: sparse contrast. The null element is still indexed (v is an
	// array, so the sparse guard does not fire); only the missing-field doc
	// is skipped from the index. It is NOT, however, skipped from the result:
	// {tags:null} matches a missing field too (see Sub A), so the planner must
	// not seek the sparse index here — doing so would silently drop id:2.
	fxB := newFixture(t)
	collB, err := fxB.CreateCollection(ctx, "sp")
	require.NoError(t, err)
	require.NoError(t, collB.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}, Sparse: true}))
	require.NoError(t, collB.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["a",null,"b"]}`), // null,a,b,whole-array = 4
		anyenc.MustParseJson(`{"id":2}`),                       // missing -> skipped
	))
	assertIndexLen(t, collB.GetIndexes()[0], 4)
	nullB, err := collB.Find(`{"tags":null}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, nullB)

	// Sub C: duplicate nulls collapse within a doc.
	fxC := newFixture(t)
	collC, err := fxC.CreateCollection(ctx, "dup")
	require.NoError(t, err)
	require.NoError(t, collC.EnsureIndex(ctx, IndexInfo{Name: "tags", Fields: []string{"tags"}}))
	require.NoError(t, collC.Insert(ctx,
		anyenc.MustParseJson(`{"id":3,"tags":[null,null,"a"]}`), // null + a + whole-array = 3
	))
	assertIndexLen(t, collC.GetIndexes()[0], 3)
}

// act-32: A deep nested path whose LEAF is an array fans out multikey-style
// (K elements + whole-array) path-agnostically. $in over nested-leaf
// elements dedups overlapping docs to one result.
func TestIndex_ArrayNested_NestedLeafArray_MultiKeyFanout(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "abc", Fields: []string{"a.b.c"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":{"b":{"c":["x","y"]}}}`),
		anyenc.MustParseJson(`{"id":2,"a":{"b":{"c":["y","z"]}}}`),
	))

	// 3 entries per doc (2 elements + whole-array) regardless of nesting depth.
	idx := coll.GetIndexes()[0]
	assertIndexLen(t, idx, 6)

	xCount, err := coll.Find(`{"a.b.c":"x"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, xCount)

	yCount, err := coll.Find(`{"a.b.c":"y"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, yCount)

	// The single-field equality on the nested-leaf array uses the index.
	eqExplain, err := coll.Find(`{"a.b.c":"x"}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, eqExplain.Sql, "IndexScan(abc)")

	// $in dedup: id1 has both x and y but collapses to one doc; id2 has y.
	inCount, err := coll.Find(`{"a.b.c":{"$in":["x","y"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, inCount)

	// Iter over the $in filter yields 2 results, each id exactly once.
	inIds := collectIntField(t, coll.Find(`{"a.b.c":{"$in":["x","y"]}}`), "id")
	sort.Ints(inIds)
	assert.Equal(t, []int{1, 2}, inIds)
}

// ── Or/complex-filter audit (act-33/34/35/36/37) ──
// =============================================================================
// Domain: or-complex-filter
//
// These tests pin the soundness of the planner's handling of filter operators
// that intentionally produce NO index bounds ($nor, $not, $exists, cross-field
// $or), plus the over-approximating two-range $and. For every such operator the
// contract is: the index cannot narrow the candidate set, so the planner must
// FullScan-and-filter (or, for a partially-indexable predicate, IndexScan the
// indexable conjunct and apply the rest as a residual Filter), and the result
// must be byte-for-byte identical to an unindexed twin collection.
//
// Ground-truth references (verified in query/filter.go in this worktree):
//   - Nor.IndexBounds    returns bounds unchanged  (pure $nor => FullScan)
//   - Not.IndexBounds    returns bounds unchanged  (negation  => FullScan)
//   - Exists.IndexBounds returns bounds unchanged  ($exists   => FullScan)
//   - Or.IndexBounds     returns bounds unchanged when any branch yields no
//                        bounds for the field (cross-field $or => FullScan)
//   - And.IndexBounds    returns the FIRST conjunct's bounds (over-approx);
//                        remaining conjuncts are a residual Filter.
//
// The FullScan token in Explain.Sql prints as "FullScan(filtered)" whenever a
// residual filter is present (fullscan_iter.go String()).
// =============================================================================

// act-33: $nor over an indexed collection is sound and fullscans (pure $nor);
// mixed equality+$nor narrows on the equality field and applies NOR as a
// residual filter.
func TestIndex_ComplexFilter_NorIsSoundAndFullScans(t *testing.T) {
	// Local closure: build a collection of 100 docs (a=i%10, b=i%7), optionally
	// indexed on "a". Twin (unindexed) collection is built by passing nil.
	build := func(t *testing.T, indexes ...IndexInfo) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for _, idx := range indexes {
			require.NoError(t, coll.EnsureIndex(ctx, idx))
		}
		for i := 0; i < 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idx := build(t, IndexInfo{Fields: []string{"a"}})
	noidx := build(t)

	t.Run("pure nor fullscans and agrees with unindexed", func(t *testing.T) {
		f := `{"$nor":[{"a":1},{"a":2}]}`

		// Nor.IndexBounds returns bounds unchanged => no narrowing => FullScan.
		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")
		assert.NotContains(t, ex.Sql, "IndexScan")

		// a=1 (10 docs) + a=2 (10 docs) excluded => 80 docs remain.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 80, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo, "indexed Count must equal unindexed Count")

		// Exact id-set parity between indexed and unindexed collections.
		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.Len(t, idsIdx, 80)
		assert.ElementsMatch(t, idsNo, idsIdx)
	})

	t.Run("mixed equality+nor narrows on indexed field, nor is residual", func(t *testing.T) {
		f := `{"a":5,"$nor":[{"b":1},{"b":2}]}`

		// The equality on "a" narrows via the index; the $nor is a residual Filter.
		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "IndexScan(a)")
		assert.Contains(t, ex.Sql, "-> Filter")

		// a=5 => 10 docs (ids 5,15,...,95). Of those, b=i%7 in {1,2} are removed.
		// ids with a=5 and (b==1 or b==2): 15(b=1), 65(b=2), 85(b=1) => 3 removed
		// => 7 remain.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 7, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo)

		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.Len(t, idsIdx, 7)
		assert.ElementsMatch(t, idsNo, idsIdx)
	})
}

// act-34: $not operator form over an index is sound and fullscans.
func TestIndex_ComplexFilter_NotOperatorSound(t *testing.T) {
	build := func(t *testing.T, indexes ...IndexInfo) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for _, idx := range indexes {
			require.NoError(t, coll.EnsureIndex(ctx, idx))
		}
		for i := 0; i < 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idx := build(t, IndexInfo{Fields: []string{"a"}})
	noidx := build(t)

	t.Run("not eq fullscans, 90 docs, agrees with unindexed", func(t *testing.T) {
		f := `{"a":{"$not":{"$eq":5}}}`

		// Not.IndexBounds returns bounds unchanged => FullScan, never index access.
		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan(filtered)")
		assert.NotContains(t, ex.Sql, "IndexScan")
		assert.NotContains(t, ex.Sql, "IndexSeek")
		assert.NotContains(t, ex.Sql, "CoverLookup")

		// a=5 => 10 docs excluded => 90 remain.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 90, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo)

		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.Len(t, idsIdx, 90)
		assert.ElementsMatch(t, idsNo, idsIdx)
	})

	t.Run("not gte negated range, 80 docs", func(t *testing.T) {
		f := `{"a":{"$not":{"$gte":8}}}`

		ex, err := idx.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")
		assert.NotContains(t, ex.Sql, "IndexScan")

		// !(a>=8) => a in 0..7 => 80 docs.
		cntIdx, err := idx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 80, cntIdx)

		cntNo, err := noidx.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cntIdx, cntNo)

		idsIdx := collectIntField(t, idx.Find(f), "id")
		idsNo := collectIntField(t, noidx.Find(f), "id")
		assert.ElementsMatch(t, idsNo, idsIdx)
	})
}

// act-35: $exists:false and $exists:true over a non-sparse index both fullscan
// (the answer is NOT derived from index entries). A buggy index-based answer to
// $exists:true would yield 100 (non-sparse index len==100), so the FullScan and
// the exact count of 50 are the load-bearing assertions.
func TestIndex_ComplexFilter_ExistsFalseAndNonSparse(t *testing.T) {
	// Local closure: 100 docs; even i has "opt", odd i does not. Index on "opt".
	build := func(t *testing.T, sparse bool) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"opt"}, Sparse: sparse}))
		for i := 0; i < 100; i++ {
			var doc *anyenc.Value
			if i%2 == 0 {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"opt":%d}`, i, i))
			} else {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))
			}
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	// Unindexed baseline (no index at all) for parity.
	baseline := func(t *testing.T) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			var doc *anyenc.Value
			if i%2 == 0 {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"opt":%d}`, i, i))
			} else {
				doc = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))
			}
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idxLen := func(t *testing.T, c Collection) int {
		t.Helper()
		n, err := c.GetIndexes()[0].Len(ctx)
		require.NoError(t, err)
		return n
	}

	nonSparse := build(t, false)
	base := baseline(t)

	t.Run("non-sparse exists:false fullscans, 50 docs", func(t *testing.T) {
		f := `{"opt":{"$exists":false}}`

		ex, err := nonSparse.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")

		// Odd ids (no "opt") => 50.
		cnt, err := nonSparse.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 50, cnt)

		cntBase, err := base.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cnt, cntBase)

		assert.ElementsMatch(t,
			collectIntField(t, base.Find(f), "id"),
			collectIntField(t, nonSparse.Find(f), "id"))
	})

	t.Run("non-sparse exists:true fullscans, 50 docs not 100", func(t *testing.T) {
		f := `{"opt":{"$exists":true}}`

		// Non-sparse index has 100 entries (one "null" per missing-field doc).
		assert.Equal(t, 100, idxLen(t, nonSparse))

		ex, err := nonSparse.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")
		// Critical: must NOT answer $exists:true from the index (would be 100).
		assert.NotContains(t, ex.Sql, "IndexScan")

		cnt, err := nonSparse.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 50, cnt, "exists:true must be 50, not the index length 100")

		cntBase, err := base.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cnt, cntBase)

		assert.ElementsMatch(t,
			collectIntField(t, base.Find(f), "id"),
			collectIntField(t, nonSparse.Find(f), "id"))
	})

	t.Run("sparse exists:false fullscans, 50 docs, sparse index len 50", func(t *testing.T) {
		sparse := build(t, true)
		f := `{"opt":{"$exists":false}}`

		// Sparse index skips missing fields => 50 entries.
		assert.Equal(t, 50, idxLen(t, sparse))

		ex, err := sparse.Find(f).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "FullScan")

		cnt, err := sparse.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 50, cnt)

		cntBase, err := base.Find(f).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, cnt, cntBase)

		assert.ElementsMatch(t,
			collectIntField(t, base.Find(f), "id"),
			collectIntField(t, sparse.Find(f), "id"))
	})
}

// act-36: Contradictory two-range $and on one indexed field (a>5 AND a<3)
// returns 0. And.IndexBounds yields only the first conjunct's over-approx seek
// bounds ('5',inf]; the residual FilterIter rejects every row.
func TestIndex_ComplexFilter_ContradictoryRangeAnd(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	// Both the explicit $and form and the inline two-operator form must behave
	// identically: seek the first conjunct, re-filter to empty.
	cases := map[string]string{
		"explicit $and": `{"$and":[{"a":{"$gt":5}},{"a":{"$lt":3}}]}`,
		"inline range":  `{"a":{"$gt":5,"$lt":3}}`,
	}

	for name, f := range cases {
		f := f
		t.Run(name, func(t *testing.T) {
			cnt, err := coll.Find(f).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 0, cnt)

			// Count must equal the iterated length.
			ids := collectIntField(t, coll.Find(f), "id")
			assert.Len(t, ids, 0)
			assert.Equal(t, cnt, len(ids))
		})
	}

	t.Run("explain explicit $and uses over-approx index seek + residual Filter", func(t *testing.T) {
		ex, err := coll.Find(`{"$and":[{"a":{"$gt":5}},{"a":{"$lt":3}}]}`).Explain(ctx)
		require.NoError(t, err)
		// First conjunct a>5 produces the over-approx seek bounds; second is residual.
		assert.Contains(t, ex.Sql, "IndexScan(a)")
		assert.Contains(t, ex.Sql, "-> Filter")
		// The covering-count fast path must NOT fire (two predicates on a).
		assert.NotContains(t, ex.Sql, "CoverLookup")
	})
}

// act-37: $or whose two branches are on two SEPARATE indexed fields must
// fullscan. Or.IndexBounds returns bounds unchanged whenever any branch yields
// no bounds for a given field, so a cross-field $or produces empty bounds for
// every index. Guards against a future change that wrongly seeks one index and
// silently drops the other branch.
func TestIndex_ComplexFilter_OrTwoIndexedFieldsUnion(t *testing.T) {
	// Local closure: 100 docs (a=i%10, b=i%7); optionally index BOTH a and b.
	build := func(t *testing.T, indexed bool) Collection {
		t.Helper()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		if indexed {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
		}
		for i := 0; i < 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
			require.NoError(t, coll.Insert(ctx, doc))
		}
		return coll
	}

	idx := build(t, true)
	noidx := build(t, false)

	f := `{"$or":[{"a":1},{"b":2}]}`

	// a=1 => 10 docs (i%10==1). b=2 => 15 docs (i%7==2). Overlap a=1 AND b=2
	// (i%10==1 AND i%7==2 => i=51) => 2 docs counted in both branches, so the
	// union is 10+15-2 = 23. Exact value verified empirically.
	cntIdx, err := idx.Find(f).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 23, cntIdx)

	cntNo, err := noidx.Find(f).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 23, cntNo)
	assert.Equal(t, cntNo, cntIdx, "indexed must equal unindexed")

	// Exact id-set parity.
	assert.ElementsMatch(t,
		collectIntField(t, noidx.Find(f), "id"),
		collectIntField(t, idx.Find(f), "id"))

	// The plan must FullScan: no index candidate may be Used, and no IndexScan
	// token may appear (a single-index seek would silently drop the other branch).
	ex, err := idx.Find(f).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ex.Sql, "FullScan")
	assert.NotContains(t, ex.Sql, "IndexScan")

	// Both indexes are reported as present but unused candidates.
	require.Len(t, ex.Indexes, 2)
	for _, ie := range ex.Indexes {
		assert.False(t, ie.Used, "index %q must not be used for a cross-field $or", ie.Name)
	}
}

// A $in set containing null must match missing-field documents, agreeing with
// {"$eq":null} and with the covering Count path (a missing field is indexed
// under the null key, and In.IndexBounds emits a point bound for the null
// member — Iter previously dropped what Count included).
func TestIndex_InNullMatchesMissingField(t *testing.T) {
	fx := newFixture(t)

	build := func(t *testing.T, withIndex bool) Collection {
		coll, err := fx.CreateCollection(ctx, fmt.Sprintf("innull_%v", withIndex))
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		}
		for _, doc := range []string{
			`{"id":1}`,
			`{"id":2,"a":null}`,
			`{"id":3,"a":1}`,
		} {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
		}
		return coll
	}

	idx := build(t, true)
	noidx := build(t, false)

	check := func(t *testing.T, filter string, wantIds []int) {
		for name, coll := range map[string]Collection{"indexed": idx, "fullscan": noidx} {
			cnt, err := coll.Find(filter).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(wantIds), cnt, "%s Count %s", name, filter)
			assert.ElementsMatch(t, wantIds, collectIntField(t, coll.Find(filter), "id"),
				"%s Iter %s", name, filter)
		}
	}

	check(t, `{"a":{"$in":[null]}}`, []int{1, 2})
	check(t, `{"a":null}`, []int{1, 2}) // the $eq the $in must agree with
	check(t, `{"a":{"$in":[null,1]}}`, []int{1, 2, 3})
	check(t, `{"a":{"$in":[1]}}`, []int{3}) // no null member: missing stays excluded
}

// A compound multikey index fans one document into several entries (one per
// element combination plus a whole-array entry). Dedup must happen IN-PLAN,
// below Sort/Limit/Offset and the covering Count, so raw entries never consume
// result slots: every verb agrees with the FullScan oracle.
func TestIndex_CompoundMultikeyDedupBelowCutoffs(t *testing.T) {
	newColl := func(t *testing.T, name string, withIndex bool) Collection {
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(t, err)
		if withIndex {
			require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "tb", Fields: []string{"tags", "b"}}))
		}
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"tags":[1,2],"b":1}`),
			anyenc.MustParseJson(`{"id":2,"tags":[2],"b":2}`)))
		for i := 0; i < 300; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"tags":[100],"b":%d}`, 10+i, i))))
		}
		return coll
	}

	hint := IndexHint{IndexName: "tb", Boost: 1 << 30}
	filter := `{"tags":{"$in":[1,2]}}`

	t.Run("limit counts documents not entries", func(t *testing.T) {
		coll := newColl(t, "lim", true)
		got := collectIntField(t, coll.Find(filter).IndexHint(hint).Limit(2), "id")
		assert.ElementsMatch(t, []int{1, 2}, got)
		cnt, err := coll.Find(filter).IndexHint(hint).Limit(2).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, cnt)
	})

	t.Run("offset skips documents not entries", func(t *testing.T) {
		// entries (1,d1),(2,d1),(2,d2): the offset must skip ONE DOC (d1),
		// not eat d1's first entry and then emit it through its second.
		coll := newColl(t, "off", true)
		got := collectIntField(t, coll.Find(filter).IndexHint(hint).Offset(1), "id")
		assert.Equal(t, []int{2}, got)
	})

	t.Run("sort topk ranks documents not entries", func(t *testing.T) {
		coll := newColl(t, "topk", true)
		got := collectIntField(t, coll.Find(filter).IndexHint(hint).Sort("b").Limit(2), "id")
		assert.Equal(t, []int{1, 2}, got)
	})

	t.Run("bounded delete removes limit documents", func(t *testing.T) {
		coll := newColl(t, "del", true)
		res, err := coll.Find(filter).IndexHint(hint).Limit(2).Delete(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, res.Modified)
	})

	t.Run("bounded update modifies limit documents", func(t *testing.T) {
		coll := newColl(t, "upd", true)
		res, err := coll.Find(filter).IndexHint(hint).Limit(2).Update(ctx, query.MustParseModifier(`{"$set":{"u":1}}`))
		require.NoError(t, err)
		assert.Equal(t, 2, res.Modified)
	})

	t.Run("plan pins the dedup stage below the fetch", func(t *testing.T) {
		coll := newColl(t, "explain", true)
		ex, err := coll.Find(filter).IndexHint(hint).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Sql, "-> Dedup(docid) -> Fetch")
	})
}

// The covering Count path (CountEntries) must not report the entry count for
// a compound prefix bound over multikey data: one array doc is one document.
// A scalar-only compound index keeps the page-batch answer (and its speed).
func TestIndex_CompoundCoveringCountMultikey(t *testing.T) {
	fx := newFixture(t)

	t.Run("array suffix fan-out counts one doc", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "fanout")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":-1,"b":[1,2,3]}`)))

		cnt, err := coll.Find(`{"a":-1}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, cnt)
		assert.Equal(t, []int{1}, collectIntField(t, coll.Find(`{"a":-1}`), "id"))
	})

	t.Run("multi-bound in over array leading field", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "multibound")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags", "b"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1,"tags":[1,2],"b":1}`),
			anyenc.MustParseJson(`{"id":2,"tags":[2],"b":2}`)))

		cnt, err := coll.Find(`{"tags":{"$in":[1,2]}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, cnt)
	})

	t.Run("scalar-only compound stays exact", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "scalar")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
		for i := 0; i < 10; i++ {
			require.NoError(t, coll.Insert(ctx,
				anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i))))
		}
		cnt, err := coll.Find(`{"a":1}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, cnt)
	})
}

// A UNIQUE index can still be multikey (each array element unique across
// docs): a multi-bound $in reaches the same doc through several elements via
// the CoverIter point-lookup path, which must dedup below Offset/Limit too.
func TestIndex_UniqueMultikeyCoverLookupDedup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "uniqmk")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":[1,2],"b":5}`),
		anyenc.MustParseJson(`{"id":2,"a":[3],"b":5}`)))

	filter := `{"a":{"$in":[1,2,3]},"b":5}`

	cnt, err := coll.Find(filter).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
	assert.ElementsMatch(t, []int{1, 2}, collectIntField(t, coll.Find(filter), "id"))
	// bounds (1,5),(2,5),(3,5): the offset must skip doc 1, not just its
	// first cross-bound repeat.
	assert.Equal(t, []int{2}, collectIntField(t, coll.Find(filter).Offset(1), "id"))
	got := collectIntField(t, coll.Find(filter).Limit(2), "id")
	assert.ElementsMatch(t, []int{1, 2}, got)

	// The array at the SUFFIX field: the whole-array key detector (byte-0
	// probe) cannot see mid-key arrays, so CoverIter must assume multikey
	// for compound lookups rather than trust the probe.
	sfx, err := fx.CreateCollection(ctx, "uniqmk_suffix")
	require.NoError(t, err)
	require.NoError(t, sfx.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}, Unique: true}))
	require.NoError(t, sfx.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":[2,3]}`),
		anyenc.MustParseJson(`{"id":2,"a":1,"b":[4]}`)))

	sfxFilter := `{"a":1,"b":{"$in":[2,3,4]}}`
	cnt, err = sfx.Find(sfxFilter).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
	assert.ElementsMatch(t, []int{1, 2}, collectIntField(t, sfx.Find(sfxFilter), "id"))
	assert.Equal(t, []int{2}, collectIntField(t, sfx.Find(sfxFilter).Offset(1), "id"))
	res, err := sfx.Find(sfxFilter).Update(ctx, query.MustParseModifier(`{"$set":{"u":1}}`))
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)
}

// Array-valued sort fields must order identically under every plan (Mongo
// semantics: min element ascending / max element descending, independent of
// the query predicate). Each row runs against a plain and an indexed
// collection and compares order AND Limit membership.
func TestIndex_ArraySortPlanIndependence(t *testing.T) {
	fx := newFixture(t)
	seq := 0

	build := func(t *testing.T, indexFields []string, docs ...string) (idx, plain Collection) {
		seq++
		mk := func(name string, fields []string) Collection {
			coll, err := fx.CreateCollection(ctx, fmt.Sprintf("%s_%d", name, seq))
			require.NoError(t, err)
			if fields != nil {
				require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "srt", Fields: fields}))
			}
			for _, d := range docs {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
			}
			return coll
		}
		return mk("idx", indexFields), mk("plain", nil)
	}

	hint := IndexHint{IndexName: "srt", Boost: 1 << 30}
	check := func(t *testing.T, idx, plain Collection, filter any, want []int, sorts ...any) {
		pOrder := collectIntField(t, plain.Find(filter).Sort(sorts...), "id")
		iOrder := collectIntField(t, idx.Find(filter).IndexHint(hint).Sort(sorts...), "id")
		assert.Equal(t, want, pOrder, "plain order")
		assert.Equal(t, want, iOrder, "indexed order")
		if len(want) > 1 {
			pTop := collectIntField(t, plain.Find(filter).Sort(sorts...).Limit(1), "id")
			iTop := collectIntField(t, idx.Find(filter).IndexHint(hint).Sort(sorts...).Limit(1), "id")
			assert.Equal(t, want[:1], pTop, "plain limit")
			assert.Equal(t, want[:1], iTop, "indexed limit")
		}
	}

	t.Run("single-field asc min element", func(t *testing.T) {
		idx, plain := build(t, []string{"x"},
			`{"id":1,"x":[5,1]}`, `{"id":2,"x":3}`, `{"id":3,"x":[2,9]}`, `{"id":4,"x":0}`)
		check(t, idx, plain, nil, []int{4, 1, 3, 2}, "x")
	})
	t.Run("single-field desc max element", func(t *testing.T) {
		idx, plain := build(t, []string{"x"},
			`{"id":1,"x":[1,9]}`, `{"id":2,"x":[8,2]}`, `{"id":3,"x":3}`)
		check(t, idx, plain, nil, []int{1, 2, 3}, "-x")
	})
	t.Run("bounds on the sort field use the global min", func(t *testing.T) {
		// -1 is OUT of bounds: the in-bounds canonical element (5) must not
		// leak into the order — the gate forces a SortIter on unproven data.
		idx, plain := build(t, []string{"x"},
			`{"id":1,"x":[5,-1]}`, `{"id":2,"x":3}`)
		check(t, idx, plain, `{"x":{"$gt":0}}`, []int{1, 2}, "x")
	})
	t.Run("compound equality prefix asc", func(t *testing.T) {
		idx, plain := build(t, []string{"a", "x"},
			`{"id":1,"a":1,"x":[1,9]}`, `{"id":2,"a":1,"x":[8,2]}`, `{"id":3,"a":1,"x":5}`)
		check(t, idx, plain, `{"a":1}`, []int{1, 2, 3}, "x")
	})
	t.Run("compound equality prefix desc", func(t *testing.T) {
		// the whole-array index entry sorts above every scalar: a reverse
		// order-providing scan would surface id=2 ([8,2]) before id=1 (max 9).
		idx, plain := build(t, []string{"a", "x"},
			`{"id":1,"a":1,"x":[1,9]}`, `{"id":2,"a":1,"x":[8,2]}`, `{"id":3,"a":1,"x":5}`)
		check(t, idx, plain, `{"a":1}`, []int{1, 2, 3}, "-x")
	})
	t.Run("compound asc with object elements", func(t *testing.T) {
		// The whole-array index entry (TypeArray tag) sorts BELOW object
		// entries, so a forward order-providing scan would surface the array
		// doc first; the min-element key orders it by enc({"x":9}) instead.
		idx, plain := build(t, []string{"a", "x"},
			`{"id":1,"a":1,"x":[{"x":9}]}`, `{"id":2,"a":1,"x":{"x":1}}`)
		check(t, idx, plain, `{"a":1}`, []int{2, 1}, "x")
	})
	t.Run("empty array missing null object", func(t *testing.T) {
		idx, plain := build(t, []string{"x"},
			`{"id":1}`, `{"id":2,"x":null}`, `{"id":3,"x":[]}`, `{"id":4,"x":1}`, `{"id":5,"x":{"k":1}}`)
		check(t, idx, plain, nil, []int{1, 2, 4, 3, 5}, "x")
	})
}

// The order-providing gate: over possibly-multikey data an index scan may
// claim ExactSort only when nothing narrows or reverses the traversal of the
// sort-matched fields; scalar-proven indexes keep today's plans.
func TestIndex_ArraySortOrderProvidingGate(t *testing.T) {
	fx := newFixture(t)

	mk := func(t *testing.T, name string, docs ...string) Collection {
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "srt", Fields: []string{"x"}}))
		for _, d := range docs {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
		}
		return coll
	}
	hint := IndexHint{IndexName: "srt", Boost: 1 << 30}

	t.Run("scalar-proven keeps the order-providing scan", func(t *testing.T) {
		coll := mk(t, "scalar", `{"id":1,"x":1}`, `{"id":2,"x":2}`)
		ex, err := coll.Find(`{"x":{"$gt":0}}`).IndexHint(hint).Sort("x").Explain(ctx)
		require.NoError(t, err)
		assert.NotContains(t, ex.Sql, "Sort", "bounds on the sort field are fine when scalar-proven: %s", ex.Sql)
		assert.NotContains(t, ex.Sql, "TopK")
	})
	t.Run("unproven with bounds on the sort field re-sorts", func(t *testing.T) {
		coll := mk(t, "arr", `{"id":1,"x":[5,-1]}`, `{"id":2,"x":3}`)
		ex, err := coll.Find(`{"x":{"$gt":0}}`).IndexHint(hint).Sort("x").Explain(ctx)
		require.NoError(t, err)
		assert.True(t, strings.Contains(ex.Sql, "Sort") || strings.Contains(ex.Sql, "TopK"),
			"unproven multikey + bounds on the sort field must re-sort: %s", ex.Sql)
	})
	t.Run("unproven without bounds keeps the order-providing scan", func(t *testing.T) {
		coll := mk(t, "arrnobound", `{"id":1,"x":[5,1]}`, `{"id":2,"x":3}`)
		ex, err := coll.Find(nil).IndexHint(hint).Sort("x").Explain(ctx)
		require.NoError(t, err)
		assert.NotContains(t, ex.Sql, "Sort", "no bounds on the sort run: index order == min-element order: %s", ex.Sql)
		assert.NotContains(t, ex.Sql, "TopK")
	})
}
