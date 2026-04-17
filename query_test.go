package anystore

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestCollQuery_Count(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	t.Run("no filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(nil), 5)
	})

	t.Run("filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(`{"a":{"$in":[2,3,4]}}`), 3)
	})

}

func TestCollQuery_Explain(t *testing.T) {
	fx := newFixture(t)

	t.Run("no index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))

		explain, err := coll.Find(nil).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan")
		assert.Empty(t, explain.Indexes)
	})
	t.Run("more than 1000", func(t *testing.T) {
		var builder strings.Builder
		builder.Grow(4000)
		builder.WriteString(`{"id":{"$in":[`)
		l := 999
		for i := 1; i <= l; i++ {
			builder.WriteString(strconv.Itoa(i))
			if i < l {
				builder.WriteString(",")
			}
		}
		builder.WriteString("]}}")
		result := builder.String()

		coll, _ := fx.CreateCollection(ctx, "test_foo")

		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
			anyenc.MustParseJson(`{"id":3, "a":"a3"}`),
			anyenc.MustParseJson(`{"id":4, "a":"a4"}`),
			anyenc.MustParseJson(`{"id":5, "a":"a5"}`),
		))

		explain, err := coll.Find(result).Explain(ctx)
		require.NoError(t, err)
		assert.NotEmpty(t, explain.Sql)
	})
	t.Run("simple index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_simple_idx")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2"}`),
		))

		explain, err := coll.Find(`{"a":"a1"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
		require.Len(t, explain.Indexes, 1)
		assert.Equal(t, "a", explain.Indexes[0].Name)
		assert.True(t, explain.Indexes[0].Used)
	})
	t.Run("many indexes", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_many_idx")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1", "b":"b1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2", "b":"b2"}`),
		))

		explain, err := coll.Find(`{"a":"a1"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
		require.Len(t, explain.Indexes, 2)

		t.Run("index hint", func(t *testing.T) {
			explain, err := coll.Find(`{"a":"a1"}`).IndexHint(IndexHint{IndexName: "b", Boost: 100}).Explain(ctx)
			require.NoError(t, err)
			assert.Contains(t, explain.Sql, "IndexScan")
		})
	})
	t.Run("composite index", func(t *testing.T) {
		coll, err := fx.CreateCollection(ctx, "test_composite_idx")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(`{"id":1, "a":"a1", "b":"b1"}`),
			anyenc.MustParseJson(`{"id":2, "a":"a2", "b":"b2"}`),
		))

		explain, err := coll.Find(`{"a":"a1","b":"b1"}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "IndexScan")
		require.Len(t, explain.Indexes, 1)
		assert.True(t, explain.Indexes[0].Used)
	})
}

func TestCollQuery_Delete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	res, err := coll.Find(`{"a":{"$in":[2,3,4]}}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Matched)
	assert.Equal(t, 3, res.Modified)
	assertCollCount(t, coll, 2)
}

func TestCollQuery_Update(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	res, err := coll.Find(`{"a":{"$in":[2,3,4]}}`).Update(ctx, `{"$set":{"b":1}}`)
	require.NoError(t, err)
	assert.Equal(t, 3, res.Matched)
	assert.Equal(t, 3, res.Modified)

	doc, err := coll.FindId(ctx, 2)
	require.NoError(t, err)
	assert.Equal(t, `{"id":2,"a":2,"b":1}`, doc.Value().String())
}

func TestCollQuery_Sort(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":3}`),
		anyenc.MustParseJson(`{"id":2, "a":1}`),
		anyenc.MustParseJson(`{"id":3, "a":2}`),
	))

	iter, err := coll.Find(nil).Sort("a").Iter(ctx)
	require.NoError(t, err)
	var ids []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().GetInt("id"))
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []int{2, 3, 1}, ids)
}

func TestCollQuery_SortDesc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":3}`),
		anyenc.MustParseJson(`{"id":2, "a":1}`),
		anyenc.MustParseJson(`{"id":3, "a":2}`),
	))

	iter, err := coll.Find(nil).Sort("-a").Iter(ctx)
	require.NoError(t, err)
	var ids []int
	for iter.Next() {
		doc, err := iter.Doc()
		require.NoError(t, err)
		ids = append(ids, doc.Value().GetInt("id"))
	}
	require.NoError(t, iter.Close())
	assert.Equal(t, []int{1, 3, 2}, ids)
}

func TestCollQuery_LimitOffset(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1, "a":1}`),
		anyenc.MustParseJson(`{"id":2, "a":2}`),
		anyenc.MustParseJson(`{"id":3, "a":3}`),
		anyenc.MustParseJson(`{"id":4, "a":4}`),
		anyenc.MustParseJson(`{"id":5, "a":5}`),
	))

	assertQueryCount(t, coll.Find(nil).Limit(3), 3)
	assertQueryCount(t, coll.Find(nil).Offset(3), 2)
	assertQueryCount(t, coll.Find(nil).Limit(2).Offset(3), 2)
}

func assertQueryCount(t testing.TB, q Query, expected int) {
	t.Helper()
	count, err := q.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, expected, count)
}

// --- Coverage tests from query_matchall_coverage_test.go ---

// TestQuery_Coverage_MatchAllBaseline verifies the match-all baseline: both
// Find(nil) and Find("{}") return every document in the collection (N), and
// both route through the FullScan plan when no index is available.
//
// Gap item 70: Find(nil) or Find({}) — match-all baseline.
func TestQuery_Coverage_MatchAllBaseline(t *testing.T) {
	const N = 100
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	docs := make([]*anyenc.Value, N)
	for i := 0; i < N; i++ {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	t.Run("nil filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(nil), N)

		explain, err := coll.Find(nil).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"Find(nil) with no index must route through FullScan")
	})

	t.Run("empty object filter", func(t *testing.T) {
		assertQueryCount(t, coll.Find(`{}`), N)

		explain, err := coll.Find(`{}`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, explain.Sql, "FullScan",
			"Find({}) with no index must route through FullScan")
	})

	t.Run("nil and empty produce same count", func(t *testing.T) {
		nilCount, err := coll.Find(nil).Count(ctx)
		require.NoError(t, err)
		emptyCount, err := coll.Find(`{}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, nilCount, emptyCount,
			"Find(nil) and Find({}) must return the same number of docs")
		assert.Equal(t, N, nilCount)
	})
}
