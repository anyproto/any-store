package anystore

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func aggRows(t *testing.T, coll Collection, q AggQuery) []string {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	var res []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		res = append(res, d.Value().String())
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	return res
}

func expectJson(t *testing.T, jsons ...string) []string {
	t.Helper()
	res := make([]string, len(jsons))
	for i, j := range jsons {
		res[i] = anyenc.MustParseJson(j).String()
	}
	return res
}

func TestCollection_Aggregate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"cat":"a","v":10,"tags":["x","y"]}`),
		anyenc.MustParseJson(`{"id":2,"cat":"b","v":20,"tags":["x"]}`),
		anyenc.MustParseJson(`{"id":3,"cat":"a","v":30,"tags":[]}`),
		anyenc.MustParseJson(`{"id":4,"cat":"b","v":40}`),
		anyenc.MustParseJson(`{"id":5,"cat":"a","v":50,"tags":["z"]}`),
	))

	t.Run("group sum sorted", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$group": {"_id": "$cat", "total": {"$sum": "$v"}, "n": {"$count": {}}}},
			{"$sort": {"id": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"a","total":90,"n":3}`,
			`{"id":"b","total":60,"n":2}`,
		), got)
	})

	t.Run("match prefix pushdown", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"cat": "a"}},
			{"$sort": {"v": -1}},
			{"$limit": 2},
			{"$project": {"v": 1}}
		]`))
		assert.Equal(t, expectJson(t, `{"v":50}`, `{"v":30}`), got)
	})

	t.Run("unwind group", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$unwind": "$tags"},
			{"$group": {"_id": "$tags", "n": {"$count": {}}}},
			{"$sort": {"id": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"x","n":2}`,
			`{"id":"y","n":1}`,
			`{"id":"z","n":1}`,
		), got)
	})

	t.Run("count stage", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"v": {"$gte": 20}}},
			{"$count": "n"}
		]`))
		assert.Equal(t, expectJson(t, `{"n":4}`), got)
	})

	t.Run("unsatisfiable prefix yields count 0", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"cat": {"$in": []}}},
			{"$count": "n"}
		]`))
		assert.Equal(t, expectJson(t, `{"n":0}`), got)
	})

	t.Run("empty pipeline passes documents through", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[]`))
		assert.Len(t, got, 5)
	})

	t.Run("addFields and project chain", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": 1}},
			{"$addFields": {"vv": "$v", "kind": "doc"}},
			{"$project": {"vv": 1, "kind": 1}}
		]`))
		assert.Equal(t, expectJson(t, `{"vv":10,"kind":"doc"}`), got)
	})

	t.Run("aggregate count helper", func(t *testing.T) {
		n, err := coll.Aggregate(`[{"$unwind": "$tags"}]`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 4, n)
	})

	t.Run("score and distance are zero", func(t *testing.T) {
		iter, err := coll.Aggregate(`[{"$limit": 1}]`).Iter(ctx)
		require.NoError(t, err)
		require.True(t, iter.Next())
		assert.Zero(t, iter.Score())
		assert.Zero(t, iter.Distance())
		require.NoError(t, iter.Close())
	})

	t.Run("parse error surfaces at Iter", func(t *testing.T) {
		_, err := coll.Aggregate(`[{"$bogus": 1}]`).Iter(ctx)
		assert.ErrorContains(t, err, "unknown stage")
	})

	t.Run("double close", func(t *testing.T) {
		iter, err := coll.Aggregate(`[{"$limit": 1}]`).Iter(ctx)
		require.NoError(t, err)
		require.NoError(t, iter.Close())
		assert.ErrorIs(t, iter.Close(), ErrIterClosed)
	})

	t.Run("group limit error", func(t *testing.T) {
		iter, err := coll.Aggregate(`[{"$group": {"_id": "$id"}}]`).GroupLimit(2).Iter(ctx)
		require.NoError(t, err)
		assert.False(t, iter.Next())
		assert.ErrorIs(t, iter.Err(), ErrGroupLimitExceeded)
		require.NoError(t, iter.Close())
	})
}

func TestCollection_Aggregate_IndexPushdown(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"cat", "v"}}))

	var docs []*anyenc.Value
	for i := 0; i < 100; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"cat":"c%d","v":%d}`, i, i%4, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	q := coll.Aggregate(`[
		{"$match": {"cat": "c1"}},
		{"$sort": {"v": -1}},
		{"$limit": 3},
		{"$group": {"_id": "$cat", "top": {"$push": "$v"}}}
	]`)

	explain, err := q.Explain(ctx)
	require.NoError(t, err)
	var used bool
	for _, ie := range explain.Indexes {
		if ie.Used && ie.Name == "cat,v" {
			used = true
		}
	}
	assert.True(t, used, "prefix $match/$sort must use the cat,v index:\n%s", explain.Plan)
	assert.Contains(t, explain.Plan, "Pushdown: filter=")
	assert.Contains(t, explain.Plan, "limit=3")
	assert.Contains(t, explain.Plan, "Stages:")
	assert.Contains(t, explain.Plan, "$group")

	got := aggRows(t, coll, q)
	assert.Equal(t, expectJson(t, `{"id":"c1","top":[97,93,89]}`), got)
}

func TestCollection_Aggregate_FulltextPrefix(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"cat":"a","body":"hello world"}`),
		anyenc.MustParseJson(`{"id":2,"cat":"b","body":"hello there world hello"}`),
		anyenc.MustParseJson(`{"id":3,"cat":"a","body":"nothing else"}`),
	))

	// $text drives the source; _score is available to later stages.
	got := aggRows(t, coll, coll.Aggregate(`[
		{"$match": {"$text": {"$search": "hello"}}},
		{"$group": {"_id": null, "n": {"$count": {}}, "best": {"$max": "$_score"}}}
	]`))
	require.Len(t, got, 1)
	v := anyenc.MustParseJson(got[0])
	assert.Equal(t, float64(2), v.GetFloat64("n"))
	assert.Greater(t, v.GetFloat64("best"), float64(0))
}

func TestCollection_Aggregate_GroupOrderIndependent(t *testing.T) {
	// Group emit order is first-seen scan order; verify results are complete
	// and consistent regardless.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	var docs []*anyenc.Value
	for i := 0; i < 1000; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"k":%d}`, i, i%7)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	got := aggRows(t, coll, coll.Aggregate(`[{"$group": {"_id": "$k", "n": {"$count": {}}}}]`))
	sort.Strings(got)
	require.Len(t, got, 7)
	for _, j := range got {
		v := anyenc.MustParseJson(j)
		// 1000 docs over 7 keys: keys 0..5 get 143, key 6 gets 142.
		n := v.GetFloat64("n")
		assert.InDelta(t, 143, n, 1, j)
	}
}
