package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Aggregation benchmarks. The interesting numbers are allocs/op (streaming
// stages must be O(1) amortized — the per-iteration cost is the stored-doc
// parse, not pipeline machinery) and the retained-memory behaviour of the
// blocking stages (top-K sort vs full sort).

func aggBenchColl(b *testing.B, n int) Collection {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "bench")
	require.NoError(b, err)
	docs := make([]*anyenc.Value, n)
	for i := 0; i < n; i++ {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(
			`{"id":%d,"cat":"c%d","v":%d,"tags":["t%d","t%d"],"pad":"%032d"}`,
			i, i%16, i, i%7, i%13, i))
	}
	require.NoError(b, coll.Insert(ctx, docs...))
	return coll
}

func runAggBench(b *testing.B, q AggQuery) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer() // exclude collection setup done in the caller
	for i := 0; i < b.N; i++ {
		iter, err := q.Iter(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for iter.Next() {
		}
		err = iter.Err()
		// Close before any Fatal: a leaked iterator holds the read tx open
		// and deadlocks the fixture's db.Close in cleanup.
		if cerr := iter.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAggGroupSum(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		b.Run(fmt.Sprintf("docs=%d", n), func(b *testing.B) {
			coll := aggBenchColl(b, n)
			runAggBench(b, coll.Aggregate(`[{"$group": {"_id": "$cat", "s": {"$sum": "$v"}, "n": {"$count": {}}}}]`))
		})
	}
}

func BenchmarkAggMatchGroup(b *testing.B) {
	coll := aggBenchColl(b, 100_000)
	require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"cat"}}))
	b.Run("indexed match prefix", func(b *testing.B) {
		runAggBench(b, coll.Aggregate(`[
			{"$match": {"cat": "c3"}},
			{"$group": {"_id": null, "s": {"$sum": "$v"}}}
		]`))
	})
	b.Run("full scan match", func(b *testing.B) {
		runAggBench(b, coll.Aggregate(`[
			{"$match": {"v": {"$lt": 6250}}},
			{"$group": {"_id": null, "s": {"$sum": "$v"}}}
		]`))
	})
}

func BenchmarkAggUnwindGroup(b *testing.B) {
	coll := aggBenchColl(b, 10_000)
	runAggBench(b, coll.Aggregate(`[
		{"$unwind": "$tags"},
		{"$group": {"_id": "$tags", "n": {"$count": {}}}}
	]`))
}

func BenchmarkAggSortLimit(b *testing.B) {
	coll := aggBenchColl(b, 100_000)
	// The in-pipeline sort (after $group breaks pushdown) with a folded
	// top-K limit vs the full sort.
	b.Run("topK", func(b *testing.B) {
		runAggBench(b, coll.Aggregate(`[
			{"$group": {"_id": "$v", "n": {"$count": {}}}},
			{"$sort": {"id": -1}},
			{"$limit": 10}
		]`).GroupLimit(-1))
	})
	b.Run("full", func(b *testing.B) {
		runAggBench(b, coll.Aggregate(`[
			{"$group": {"_id": "$v", "n": {"$count": {}}}},
			{"$sort": {"id": -1}}
		]`).GroupLimit(-1))
	})
}

func BenchmarkAggProjectStream(b *testing.B) {
	coll := aggBenchColl(b, 100_000)
	runAggBench(b, coll.Aggregate(`[
		{"$addFields": {"vv": "$v"}},
		{"$project": {"cat": 1, "vv": 1}},
		{"$count": "n"}
	]`))
}
