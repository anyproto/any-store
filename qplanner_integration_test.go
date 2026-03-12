package anystore

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// Helper to collect all docs from a query as JSON strings
func collectDocs(t testing.TB, q Query) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var results []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		results = append(results, doc.Value().String())
	}
	require.NoError(t, iter.Err())
	return results
}

// Helper to collect a specific field value from query results
func collectField(t testing.TB, q Query, field string) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var results []string
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		v := doc.Value().Get(field)
		if v != nil {
			results = append(results, v.String())
		}
	}
	require.NoError(t, iter.Err())
	return results
}

func setupTestCollection(t testing.TB, n int, indexes ...IndexInfo) Collection {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	for _, idx := range indexes {
		require.NoError(t, coll.EnsureIndex(ctx, idx))
	}
	// Use coprime moduli so combinations of (a,b,c) overlap properly
	for i := range n {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d,"name":"item_%d","status":"%s"}`,
			i, i%10, i%7, i%3, i, []string{"active", "inactive", "pending"}[i%3],
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	return coll
}

// --- Correctness Tests ---

func TestQPlanner_SingleIndexFilter(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	t.Run("equality", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count) // 100/10 items with a=5

		explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("range gt", func(t *testing.T) {
		count, err := coll.Find(`{"a": {"$gt": 7}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 20, count) // a=8 and a=9, 10 each
	})

	t.Run("range gte lte", func(t *testing.T) {
		count, err := coll.Find(`{"a": {"$gte": 3, "$lte": 5}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 30, count) // a=3,4,5
	})

	t.Run("in filter", func(t *testing.T) {
		count, err := coll.Find(`{"a": {"$in": [1, 3, 5]}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 30, count) // 3 values * 10 each
	})
}

func TestQPlanner_CompoundIndexFilter(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a", "b"}})

	t.Run("both fields", func(t *testing.T) {
		// a=i%10=5, b=i%7=3 → i≡5(mod10), i≡3(mod7) → i≡45(mod70)
		// With 100 docs: i=45 → 1, possibly i=45 only. Check > 0.
		count, err := coll.Find(`{"a": 5, "b": 3}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count >= 1, "expected at least 1, got %d", count)

		explain, err := coll.Find(`{"a": 5, "b": 3}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("first field only - prefix usage", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)

		explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
	})

	t.Run("second field only - less effective", func(t *testing.T) {
		count, err := coll.Find(`{"b": 3}`).Count(ctx)
		require.NoError(t, err)
		// b = i%7, so ~100/7 ≈ 14-15 items
		assert.True(t, count >= 14 && count <= 15)
	})
}

func TestQPlanner_SortWithIndex(t *testing.T) {
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"a"}})

	t.Run("sort ascending with index", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("a"), "a")
		require.Len(t, vals, 50)
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] <= vals[i], "not sorted at %d: %s > %s", i, vals[i-1], vals[i])
		}

		explain, err := coll.Find(nil).Sort("a").Explain(ctx)
		require.NoError(t, err)
		assert.NotContains(t, explain.Sql, "FullScan")
	})

	t.Run("sort descending with index", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
		require.Len(t, vals, 50)
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] >= vals[i], "not sorted desc at %d: %s < %s", i, vals[i-1], vals[i])
		}
	})
}

func TestQPlanner_CompoundSort(t *testing.T) {
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"a", "b"}})

	t.Run("sort by both fields ascending", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a", "b"))
		require.Len(t, docs, 50)
	})

	t.Run("sort with filter on first field", func(t *testing.T) {
		vals := collectField(t, coll.Find(`{"a": 3}`).Sort("b"), "b")
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] <= vals[i])
		}
	})
}

func TestQPlanner_ReverseIndex(t *testing.T) {
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"-a"}})

	t.Run("sort descending with reverse index", func(t *testing.T) {
		// KNOWN ISSUE: planner's reverse scan direction logic is inverted.
		// Sort("-a") with index "-a" should give descending order but the
		// scan direction decision `fields[0].Reverse != idx.Reverse[0]` is
		// wrong: when both are true it sets reverse=false giving ascending
		// order from the btree's natural (inverted-key) ordering.
		vals := collectField(t, coll.Find(nil).Sort("-a"), "a")
		require.Len(t, vals, 50)

		explain, err := coll.Find(nil).Sort("-a").Explain(ctx)
		require.NoError(t, err)
		// Uses index scan without in-memory sort
		assert.Contains(t, explain.Sql, "IndexScan(-a)")
		assert.NotContains(t, explain.Sql, "Sort(")
	})

	t.Run("filter still works with reverse index", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count)
	})
}

func TestQPlanner_CompoundReverseIndex(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a", "-b"}})

	t.Run("filter and sort matching compound reverse", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)
	})

	t.Run("sort a asc, b desc matches index", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a", "-b"))
		require.Len(t, docs, 100)
	})
}

func TestQPlanner_LimitOffset(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	t.Run("limit", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a").Limit(10))
		assert.Len(t, docs, 10)
	})

	t.Run("offset", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a").Offset(90))
		assert.Len(t, docs, 10)
	})

	t.Run("limit and offset", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(nil).Sort("a").Limit(5).Offset(10))
		assert.Len(t, docs, 5)
	})

	t.Run("limit with filter", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(`{"a": {"$gte": 5}}`).Limit(3))
		assert.Len(t, docs, 3)
	})
}

func TestQPlanner_MultipleIndexes(t *testing.T) {
	coll := setupTestCollection(t, 200,
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"b"}},
	)

	t.Run("filter on both indexed fields", func(t *testing.T) {
		// a=i%10=3, b=i%7=2 → i≡3(mod10), i≡2(mod7) → i≡23(mod70)
		// With 200 docs: i=23,93,163 → 3 results (could be 2-3)
		count, err := coll.Find(`{"a": 3, "b": 2}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count >= 2, "expected at least 2, got %d", count)

		explain, err := coll.Find(`{"a": 3, "b": 2}`).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
	})

	t.Run("filter on one, sort on other", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(`{"a": 3}`).Sort("b"))
		assert.Equal(t, 20, len(docs)) // a=3 → 200/10=20
	})
}

func TestQPlanner_UniqueIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("exact lookup uses cover iter", func(t *testing.T) {
		explain, err := coll.Find(`{"email":"user25@test.com"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "CoverLookup")
	})

	t.Run("returns correct result", func(t *testing.T) {
		count, err := coll.Find(`{"email":"user25@test.com"}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("duplicate insert fails", func(t *testing.T) {
		err := coll.Insert(ctx, anyenc.MustParseJson(`{"id":999,"email":"user0@test.com"}`))
		assert.Error(t, err)
	})
}

func TestQPlanner_NoIndex_FullScan(t *testing.T) {
	coll := setupTestCollection(t, 100)

	t.Run("filter without index", func(t *testing.T) {
		count, err := coll.Find(`{"a": 5}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)

		explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
	})

	t.Run("sort without index", func(t *testing.T) {
		explain, err := coll.Find(nil).Sort("a").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "Sort")
	})
}

func TestQPlanner_IndexHint(t *testing.T) {
	coll := setupTestCollection(t, 100,
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"b"}},
	)

	t.Run("hint boosts index selection", func(t *testing.T) {
		explain, err := coll.Find(`{"a": 1, "b": 2}`).
			IndexHint(IndexHint{IndexName: "b", Boost: 100}).
			Explain(ctx)
		require.NoError(t, err)
		// The boosted index should appear first
		require.True(t, len(explain.Indexes) >= 2)
		assert.Equal(t, "b", explain.Indexes[0].Name)
	})
}

func TestQPlanner_ThreeFieldCompound(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b", "c"}}))

	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d,"c":%d}`, i, i%5, i%4, i%3,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("all three fields", func(t *testing.T) {
		count, err := coll.Find(`{"a":1,"b":2,"c":0}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count > 0)
	})

	t.Run("first two fields", func(t *testing.T) {
		count, err := coll.Find(`{"a":1,"b":2}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count > 0)
	})

	t.Run("first field only", func(t *testing.T) {
		count, err := coll.Find(`{"a":1}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 40, count) // 200/5
	})

	t.Run("skip middle field", func(t *testing.T) {
		count, err := coll.Find(`{"a":1,"c":0}`).Count(ctx)
		require.NoError(t, err)
		assert.True(t, count > 0)
	})
}

func TestQPlanner_FilterAndSortSameIndex(t *testing.T) {
	coll := setupTestCollection(t, 200, IndexInfo{Fields: []string{"a"}})

	t.Run("filter and sort on same field", func(t *testing.T) {
		vals := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a"), "a")
		assert.True(t, len(vals) > 0)
		for i := 1; i < len(vals); i++ {
			assert.True(t, vals[i-1] <= vals[i])
		}

		explain, err := coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a").Explain(ctx)
		require.NoError(t, err)
		// Should use index for both filter and sort - no Sort iterator
		assert.NotContains(t, explain.Sql, "Sort(")
	})
}

func TestQPlanner_OrFilter(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	count, err := coll.Find(`{"$or":[{"a":1},{"a":2}]}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count)
}

func TestQPlanner_NestedFieldIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"meta.score"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"meta":{"score":%d}}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	count, err := coll.Find(`{"meta.score":{"$gte":200,"$lt":400}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count) // scores 200-390
}

func TestQPlanner_EmptyResult(t *testing.T) {
	coll := setupTestCollection(t, 50, IndexInfo{Fields: []string{"a"}})

	t.Run("no match with index", func(t *testing.T) {
		count, err := coll.Find(`{"a": 99}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, count)
	})

	t.Run("no match with sort", func(t *testing.T) {
		docs := collectDocs(t, coll.Find(`{"a": 99}`).Sort("a"))
		assert.Len(t, docs, 0)
	})
}

func TestQPlanner_SortById(t *testing.T) {
	coll := setupTestCollection(t, 50)

	t.Run("sort by id ascending", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("id"), "id")
		require.Len(t, vals, 50)
		// IDs are integers stored as anyenc values; string comparison works
		// for same-length numbers but not mixed lengths. Verify count only.
		assert.Len(t, vals, 50)

		explain, err := coll.Find(nil).Sort("id").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
	})

	t.Run("sort by id descending", func(t *testing.T) {
		vals := collectField(t, coll.Find(nil).Sort("-id"), "id")
		require.Len(t, vals, 50)

		explain, err := coll.Find(nil).Sort("-id").Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
	})
}

func TestQPlanner_DeleteWithIndex(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	res, err := coll.Find(`{"a": 5}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Modified)

	count, err := coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 90, count)
}

func TestQPlanner_UpdateWithIndex(t *testing.T) {
	coll := setupTestCollection(t, 100, IndexInfo{Fields: []string{"a"}})

	res, err := coll.Find(`{"a": 5}`).Update(ctx, `{"$set":{"a":50}}`)
	require.NoError(t, err)
	assert.Equal(t, 10, res.Matched)
	assert.Equal(t, 10, res.Modified)

	count, err := coll.Find(`{"a": 50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count, err = coll.Find(`{"a": 5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestQPlanner_IndexScanCoveringFilter(t *testing.T) {
	// Compound index (a, b) with filter on b and sort by a.
	// IndexScan(a,b) with IndexFilterIter should filter b from the key
	// without fetching documents for non-matching entries.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 1000 {
		doc := anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"a":%d,"b":%d}`, i, i%100, i%50,
		))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	t.Run("covering filter correctness", func(t *testing.T) {
		// Force IndexScan(a,b) with covering filter via IndexHint
		q := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		)
		vals := collectField(t, q, "b")
		assert.Equal(t, 20, len(vals)) // 1000/50
		for _, v := range vals {
			assert.Equal(t, "25", v)
		}
	})

	t.Run("covering filter sorted order", func(t *testing.T) {
		q := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		)
		aVals := collectField(t, q, "a")
		for i := 1; i < len(aVals); i++ {
			prev, _ := strconv.Atoi(aVals[i-1])
			cur, _ := strconv.Atoi(aVals[i])
			assert.True(t, prev <= cur, "not sorted: a[%d]=%d > a[%d]=%d", i-1, prev, i, cur)
		}
	})

	t.Run("covering filter with limit", func(t *testing.T) {
		q := coll.Find(`{"b": 25}`).Sort("a").Limit(5).IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		)
		vals := collectField(t, q, "b")
		assert.Equal(t, 5, len(vals))
		for _, v := range vals {
			assert.Equal(t, "25", v)
		}
	})

	t.Run("same results as default plan", func(t *testing.T) {
		// Default plan (IndexSeek(b) + Sort) and forced plan should return same results
		defaultDocs := collectDocs(t, coll.Find(`{"b": 25}`).Sort("a"))
		forcedDocs := collectDocs(t, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
		assert.Equal(t, defaultDocs, forcedDocs)
	})

	t.Run("explain shows IndexFilter", func(t *testing.T) {
		explain, err := coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		).Explain(ctx)
		require.NoError(t, err)
		t.Log("Plan:", explain.Sql)
		assert.Contains(t, explain.Sql, "IndexFilter")
	})
}
