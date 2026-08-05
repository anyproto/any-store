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

func TestCollection_Aggregate_ExprMatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"cat"}}))

	var docs []*anyenc.Value
	for i := 0; i < 100; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"cat":"c%d","allocated":%d,"capacity":50}`, i, i%4, i)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))

	t.Run("filter pushed, $expr residual", func(t *testing.T) {
		q := coll.Aggregate(`[
			{"$match": {"cat": "c1", "$expr": {"$gt": ["$allocated", "$capacity"]}}}
		]`)
		explain, err := q.Explain(ctx)
		require.NoError(t, err)
		var used bool
		for _, ie := range explain.Indexes {
			if ie.Used && ie.Name == "cat" {
				used = true
			}
		}
		assert.True(t, used, "the ordinary part must still use the cat index:\n%s", explain.Plan)
		assert.Contains(t, explain.Plan, "Pushdown: filter=")
		assert.NotContains(t, explain.Plan, `Pushdown: filter={"$expr"`)
		assert.Contains(t, explain.Plan, `1. $match {"$expr":{$gt:[$allocated,$capacity]}}`)

		// cat=c1: ids 1,5,...,97; allocated>50: 53,57,...,97 → 12 rows.
		n, err := q.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 12, n)
	})

	t.Run("pure $expr is a full scan with the residual applied", func(t *testing.T) {
		q := coll.Aggregate(`[
			{"$match": {"$expr": {"$gt": ["$allocated", "$capacity"]}}},
			{"$count": "n"}
		]`)
		explain, err := q.Explain(ctx)
		require.NoError(t, err)
		for _, ie := range explain.Indexes {
			assert.False(t, ie.Used, "pure $expr must not drive an index:\n%s", explain.Plan)
		}
		assert.NotContains(t, explain.Plan, "Pushdown: filter=")
		assert.Contains(t, explain.Plan, `1. $match {"$expr"`)

		got := aggRows(t, coll, q)
		assert.Equal(t, expectJson(t, `{"n":49}`), got) // allocated 51..99
	})

	t.Run("residual respects in-pipeline limit", func(t *testing.T) {
		// $limit after an $expr match must apply after the predicate, so it
		// cannot be pushed into the access plan.
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"$expr": {"$gt": ["$allocated", "$capacity"]}}},
			{"$sort": {"allocated": 1}},
			{"$limit": 2},
			{"$project": {"allocated": 1}}
		]`))
		assert.Equal(t, expectJson(t, `{"allocated":51}`, `{"allocated":52}`), got)
	})

	t.Run("$expr against accumulator output", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$group": {"_id": "$cat", "n": {"$count": {}}, "over": {"$sum": {"$cond": [{"$gt": ["$allocated", "$capacity"]}, 1, 0]}}}},
			{"$match": {"$expr": {"$gt": ["$over", 12]}}},
			{"$sort": {"id": 1}}
		]`))
		require.NotEmpty(t, got)
		for _, j := range got {
			v := anyenc.MustParseJson(j)
			assert.Greater(t, v.GetFloat64("over"), float64(12), j)
		}
	})
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

// The vocabulary re-exports exist because internal/aggregate is unimportable
// by consumers; the exact lists are pinned by that package's snapshot tests.
func TestAggregateVocabularies(t *testing.T) {
	assert.Contains(t, AggregateStages(), "$match")
	assert.True(t, sort.StringsAreSorted(AggregateStages()))
	assert.Contains(t, AggregateAccumulators(), "$sum")
	assert.True(t, sort.StringsAreSorted(AggregateAccumulators()))
}

func TestCollection_AggregateLookup(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"a","cat":"g1","refs":["b","c","b","dangling"]}`),
		anyenc.MustParseJson(`{"id":"b","cat":"g1","parent":"a"}`),
		anyenc.MustParseJson(`{"id":"c","cat":"g2","parent":"a","refs":["a"],"self":"c"}`),
		anyenc.MustParseJson(`{"id":7,"cat":"g2","parent":null}`),
		anyenc.MustParseJson(`{"id":"g1","label":"Group One"}`),
		anyenc.MustParseJson(`{"id":"g2","label":"Group Two"}`),
		anyenc.MustParseJson(`{"id":"t1","tags":["x","y"],"ref":"b"}`),
		anyenc.MustParseJson(`{"id":0,"label":"zero"}`),
		anyenc.MustParseJson(`{"id":"mix","refs":[7,"a",7,true]}`),
	))

	t.Run("single id", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": "b"}},
			{"$lookup": {"localField": "parent", "foreignField": "id", "as": "p"}},
			{"$project": {"id": 1, "p": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"b","p":[{"id":"a","cat":"g1","refs":["b","c","b","dangling"]}]}`,
		), got)
	})

	t.Run("explicit self from", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": "b"}},
			{"$lookup": {"from": "objects", "localField": "parent", "as": "p"}},
			{"$project": {"id": 1, "n": {"$literal": 1}}}
		]`))
		assert.Len(t, got, 1)
	})

	t.Run("from mismatch fails at build", func(t *testing.T) {
		// Cross-collection joins are out of scope: parse accepts the spec, the
		// execution setup rejects it naming both collections.
		q := coll.Aggregate(`[{"$lookup": {"from": "other", "localField": "parent", "as": "p"}}]`)
		_, err := q.Iter(ctx)
		require.ErrorIs(t, err, errAggLookupFrom)
		assert.ErrorContains(t, err, `"other"`)
		assert.ErrorContains(t, err, `"objects"`)
		_, err = q.Explain(ctx)
		assert.ErrorIs(t, err, errAggLookupFrom)
	})

	t.Run("array ids dedup and order", func(t *testing.T) {
		// refs ["b","c","b","dangling"]: first-occurrence dedup, output keeps
		// first-occurrence order, unresolved ids match nothing.
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": "a"}},
			{"$lookup": {"localField": "refs", "as": "linked"}},
			{"$project": {"id": 1, "linked": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"a","linked":[{"id":"b","cat":"g1","parent":"a"},{"id":"c","cat":"g2","parent":"a","refs":["a"],"self":"c"}]}`,
		), got)
	})

	t.Run("missing and null local", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": {"$in": ["a", 7]}}},
			{"$sort": {"id": 1}},
			{"$lookup": {"localField": "parent", "as": "p"}},
			{"$project": {"id": 1, "p": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":7,"p":[]}`,
			`{"id":"a","p":[]}`,
		), got)
	})

	t.Run("self match", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": "c"}},
			{"$lookup": {"localField": "self", "as": "me"}},
			{"$project": {"id": 1, "me": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"c","me":[{"id":"c","cat":"g2","parent":"a","refs":["a"],"self":"c"}]}`,
		), got)
	})

	t.Run("number ids and non-id types", func(t *testing.T) {
		// Numeric pks resolve like string ones; a wrong-typed element (true)
		// matches nothing, silently.
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": "mix"}},
			{"$lookup": {"localField": "refs", "as": "linked"}},
			{"$unwind": "$linked"},
			{"$project": {"id": 1, "linked": "$linked.id"}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"mix","linked":7}`,
			`{"id":"mix","linked":"a"}`,
		), got)
	})

	t.Run("lookup after group resolves group keys", func(t *testing.T) {
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"cat": {"$in": ["g1", "g2"]}}},
			{"$group": {"_id": "$cat", "n": {"$count": {}}}},
			{"$lookup": {"localField": "id", "as": "meta"}},
			{"$sort": {"id": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"g1","n":2,"meta":[{"id":"g1","label":"Group One"}]}`,
			`{"id":"g2","n":2,"meta":[{"id":"g2","label":"Group Two"}]}`,
		), got)
	})

	t.Run("lookup unwind group rollup", func(t *testing.T) {
		// Two-hop pattern: resolve linked docs, unwind, aggregate over their fields.
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": "a"}},
			{"$lookup": {"localField": "refs", "as": "linked"}},
			{"$unwind": "$linked"},
			{"$group": {"_id": "$linked.cat", "n": {"$count": {}}}},
			{"$sort": {"id": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"id":"g1","n":1}`,
			`{"id":"g2","n":1}`,
		), got)
	})

	t.Run("as replaces field under unwind multi-emit", func(t *testing.T) {
		// as == localField with an upstream $unwind re-emitting the same doc:
		// the overlay must be undone between rows so the second row reads the
		// stored scalar, not the first row's array.
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": "t1"}},
			{"$unwind": "$tags"},
			{"$lookup": {"localField": "ref", "as": "ref"}},
			{"$project": {"tags": 1, "ref": 1}}
		]`))
		assert.Equal(t, expectJson(t,
			`{"tags":"x","ref":[{"id":"b","cat":"g1","parent":"a"}]}`,
			`{"tags":"y","ref":[{"id":"b","cat":"g1","parent":"a"}]}`,
		), got)
	})

	t.Run("empty prefix count feeds lookup", func(t *testing.T) {
		// The unsatisfiable-prefix fast path streams no snapshot, but $count
		// still synthesizes a row; $lookup must run against a live read tx.
		got := aggRows(t, coll, coll.Aggregate(`[
			{"$match": {"id": {"$in": []}}},
			{"$count": "n"},
			{"$lookup": {"localField": "n", "as": "d"}}
		]`))
		assert.Equal(t, expectJson(t, `{"n":0,"d":[{"id":0,"label":"zero"}]}`), got)
	})

	t.Run("count verb", func(t *testing.T) {
		n, err := coll.Aggregate(`[
			{"$lookup": {"localField": "refs", "as": "linked"}},
			{"$unwind": "$linked"}
		]`).Count(ctx)
		require.NoError(t, err)
		// a: [b,c]; c: [a]; mix: [7,a] — 5 unwound rows.
		assert.Equal(t, 5, n)
	})

	t.Run("explain lists the stage", func(t *testing.T) {
		ex, err := coll.Aggregate(`[{"$lookup": {"localField": "parent", "as": "p"}}]`).Explain(ctx)
		require.NoError(t, err)
		assert.Contains(t, ex.Plan, `$lookup {"localField":"parent","foreignField":"id","as":"p"}`)
	})

	t.Run("custom primary key rejected", func(t *testing.T) {
		cpk, err := fx.CreateCollection(ctx, "custompk", CollectionOptions{PrimaryKey: "uid"})
		require.NoError(t, err)
		_, err = cpk.Aggregate(`[{"$lookup": {"localField": "ref", "as": "d"}}]`).Iter(ctx)
		assert.ErrorIs(t, err, errAggLookupPrimaryKey)
	})
}

// BenchmarkAggregateLookup measures the steady-state per-row cost of a
// single-id $lookup over a warm store; the iterator reopen every n rows is
// amortized noise.
func BenchmarkAggregateLookup(b *testing.B) {
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "objects")
	require.NoError(b, err)
	tx, err := coll.WriteTx(ctx)
	require.NoError(b, err)
	const n = 10_000
	for i := range n {
		require.NoError(b, coll.Insert(tx.Context(), anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"ref":%d,"pad":"%016d"}`, i, (i+1)%n, i))))
	}
	require.NoError(b, tx.Commit())

	const pipeline = `[{"$lookup": {"localField": "ref", "as": "r"}}]`
	open := func() Iterator {
		it, oerr := coll.Aggregate(pipeline).Iter(ctx)
		if oerr != nil {
			b.Fatal(oerr)
		}
		return it
	}
	iter := open()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if !iter.Next() {
			if cerr := iter.Err(); cerr != nil {
				b.Fatal(cerr)
			}
			_ = iter.Close()
			iter = open()
			if !iter.Next() {
				b.Fatal("empty iterator")
			}
		}
		doc, derr := iter.Doc()
		if derr != nil {
			b.Fatal(derr)
		}
		if len(doc.Value().GetArray("r")) != 1 {
			b.Fatal("lookup missed")
		}
	}
	b.StopTimer()
	_ = iter.Close()
}
