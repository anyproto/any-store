package anystore

import (
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
