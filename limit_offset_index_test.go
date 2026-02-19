/*
Index/Planner tests inspired by SQLite: where2.test, where.test

Test scenario:
Tests limit/offset behavior with index-backed queries requiring imperative
logic: filter+sort+limit with non-trivial assertions, pagination loop
verification, compound sort type struct verification, unique index
CoverLookup, two-collection comparison, windowed verification, and
duplicate value handling.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

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
		assert.NotContains(t, explain.Sql, "Sort(")
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
