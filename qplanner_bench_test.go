package anystore

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// setupBenchCollection creates a collection with n documents and the given indexes.
// Documents have fields: id, a (i%100), b (i%50), c (i%10), val (i*7%1000).
func setupBenchCollection(b *testing.B, n int, indexes ...IndexInfo) Collection {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "bench")
	require.NoError(b, err)
	for _, idx := range indexes {
		require.NoError(b, coll.EnsureIndex(ctx, idx))
	}

	batchSize := 500
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var docs []*anyenc.Value
		for i := start; i < end; i++ {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
				`{"id":%d,"a":%d,"b":%d,"c":%d,"val":%d}`,
				i, i%100, i%50, i%10, i*7%1000,
			)))
		}
		require.NoError(b, coll.Insert(ctx, docs...))
	}
	return coll
}

func benchCount(b *testing.B, coll Collection, filter string) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		var q Query
		if filter == "" {
			q = coll.Find(nil)
		} else {
			q = coll.Find(filter)
		}
		_, err := q.Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchIter(b *testing.B, q Query) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for range b.N {
		iter, err := q.Iter(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for iter.Next() {
			if _, err := iter.Doc(); err != nil {
				b.Fatal(err)
			}
		}
		if err := iter.Err(); err != nil {
			b.Fatal(err)
		}
		iter.Close()
	}
}

// --- Fullscan vs Index Filter Benchmarks ---

func BenchmarkFilter_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkFilter_SingleIndex_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkFilter_CompoundIndex_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a", "b"}})
	benchCount(b, coll, `{"a": 50, "b": 25}`)
}

func BenchmarkFilter_CompoundIndex_PrefixOnly_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a", "b"}})
	benchCount(b, coll, `{"a": 50}`)
}

// --- Range Filter ---

func BenchmarkRangeFilter_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchCount(b, coll, `{"a": {"$gte": 40, "$lte": 60}}`)
}

func BenchmarkRangeFilter_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": {"$gte": 40, "$lte": 60}}`)
}

// --- Sort Benchmarks ---

func BenchmarkSort_FullScan_1k(b *testing.B) {
	coll := setupBenchCollection(b, 1000)
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_Index_1k(b *testing.B) {
	coll := setupBenchCollection(b, 1000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(nil).Sort("a"))
}

func BenchmarkSort_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a"))
}

// --- Filter + Sort Combined ---

func BenchmarkFilterSort_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(`{"a": {"$gte": 40, "$lte": 60}}`).Sort("a"))
}

func BenchmarkFilterSort_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(`{"a": {"$gte": 40, "$lte": 60}}`).Sort("a"))
}

// --- Limit with Sort ---

func BenchmarkSortLimit_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchIter(b, coll.Find(nil).Sort("a").Limit(10))
}

func BenchmarkSortLimit_Index_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchIter(b, coll.Find(nil).Sort("a").Limit(10))
}

// --- Two Indexes: Filter + CoverFilter ---

func BenchmarkTwoIndexFilter_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000,
		IndexInfo{Fields: []string{"a"}},
		IndexInfo{Fields: []string{"c"}},
	)
	benchCount(b, coll, `{"a": 50, "c": 5}`)
}

func BenchmarkOneIndexFilter_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000,
		IndexInfo{Fields: []string{"a"}},
	)
	benchCount(b, coll, `{"a": 50, "c": 5}`)
}

// --- Unique Index Cover Lookup ---

func BenchmarkUniqueLookup_10k(b *testing.B) {
	fx := newFixture(b)
	coll, _ := fx.CreateCollection(ctx, "bench")
	coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true})
	var docs []*anyenc.Value
	for i := range 10000 {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i)))
	}
	require.NoError(b, coll.Insert(ctx, docs...))

	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"email":"user5000@test.com"}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFullScanLookup_10k(b *testing.B) {
	fx := newFixture(b)
	coll, _ := fx.CreateCollection(ctx, "bench")
	var docs []*anyenc.Value
	for i := range 10000 {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i)))
	}
	require.NoError(b, coll.Insert(ctx, docs...))

	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"email":"user5000@test.com"}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Unique vs Non-Unique Index Benchmarks ---

// setupBenchUniqueCollection creates a collection with n documents having unique "uid" field.
func setupBenchUniqueCollection(b *testing.B, n int, indexes ...IndexInfo) Collection {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "bench")
	require.NoError(b, err)
	for _, idx := range indexes {
		require.NoError(b, coll.EnsureIndex(ctx, idx))
	}

	batchSize := 500
	for start := 0; start < n; start += batchSize {
		end := start + batchSize
		if end > n {
			end = n
		}
		var docs []*anyenc.Value
		for i := start; i < end; i++ {
			docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(
				`{"id":%d,"uid":%d,"val":%d}`,
				i, i, i*7%1000,
			)))
		}
		require.NoError(b, coll.Insert(ctx, docs...))
	}
	return coll
}

func BenchmarkFind_UniqueIndex_10k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 10000, IndexInfo{Fields: []string{"uid"}, Unique: true})
	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"uid": 5000}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFind_NonUniqueIndex_10k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 10000, IndexInfo{Fields: []string{"uid"}})
	b.ResetTimer()
	for range b.N {
		_, err := coll.Find(`{"uid": 5000}`).Count(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInsert_NoIndex_1k(b *testing.B) {
	fx := newFixture(b)
	docs := make([]*anyenc.Value, 1000)
	for i := range 1000 {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"i_%d","uid":%d,"val":%d}`, i, i, i*7%1000))
	}
	b.ResetTimer()
	for n := range b.N {
		b.StopTimer()
		name := fmt.Sprintf("bench_%d", n)
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(b, err)
		b.StartTimer()
		require.NoError(b, coll.Insert(ctx, docs...))
	}
}

func BenchmarkInsert_UniqueIndex_1k(b *testing.B) {
	fx := newFixture(b)
	docs := make([]*anyenc.Value, 1000)
	for i := range 1000 {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"i_%d","uid":%d,"val":%d}`, i, i, i*7%1000))
	}
	b.ResetTimer()
	for n := range b.N {
		b.StopTimer()
		name := fmt.Sprintf("bench_%d", n)
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(b, err)
		require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"uid"}, Unique: true}))
		b.StartTimer()
		require.NoError(b, coll.Insert(ctx, docs...))
	}
}

func BenchmarkInsert_NonUniqueIndex_1k(b *testing.B) {
	fx := newFixture(b)
	docs := make([]*anyenc.Value, 1000)
	for i := range 1000 {
		docs[i] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"i_%d","uid":%d,"val":%d}`, i, i, i*7%1000))
	}
	b.ResetTimer()
	for n := range b.N {
		b.StopTimer()
		name := fmt.Sprintf("bench_%d", n)
		coll, err := fx.CreateCollection(ctx, name)
		require.NoError(b, err)
		require.NoError(b, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"uid"}}))
		b.StartTimer()
		require.NoError(b, coll.Insert(ctx, docs...))
	}
}

func BenchmarkUpdate_UniqueIndex_1k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 1000, IndexInfo{Fields: []string{"uid"}, Unique: true})
	b.ResetTimer()
	for n := range b.N {
		_, err := coll.Find(`{"uid": {"$lt": 100}}`).Update(ctx, fmt.Sprintf(`{"$set":{"val":%d}}`, n))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUpdate_NonUniqueIndex_1k(b *testing.B) {
	coll := setupBenchUniqueCollection(b, 1000, IndexInfo{Fields: []string{"uid"}})
	b.ResetTimer()
	for n := range b.N {
		_, err := coll.Find(`{"uid": {"$lt": 100}}`).Update(ctx, fmt.Sprintf(`{"$set":{"val":%d}}`, n))
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Selectivity: High vs Low ---

func BenchmarkHighSelectivity_Index_10k(b *testing.B) {
	// a%100 → 100 docs match (1% selectivity)
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"a"}})
	benchCount(b, coll, `{"a": 50}`)
}

func BenchmarkLowSelectivity_Index_10k(b *testing.B) {
	// c%10 → 1000 docs match (10% selectivity)
	coll := setupBenchCollection(b, 10000, IndexInfo{Fields: []string{"c"}})
	benchCount(b, coll, `{"c": 5}`)
}

func BenchmarkLowSelectivity_FullScan_10k(b *testing.B) {
	coll := setupBenchCollection(b, 10000)
	benchCount(b, coll, `{"c": 5}`)
}

// --- FilterSort/SortNonPrefix: IndexSeek(b)+Sort vs IndexScan(a,b) with covering filter ---

func BenchmarkFilterSort_SortNonPrefix_500k(b *testing.B) {
	// Setup: 500k docs, compound index (a, b), single-field index (b).
	// Query: Find({"b": 25}).Sort("a")
	// b = i%50, so ~10k docs match (2% selectivity).
	n := 500000
	coll := setupBenchCollection(b, n,
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"b"}},
	)

	// Sub-benchmarks: default plan vs forced IndexScan with covering filter
	b.Run("Default", func(b *testing.B) {
		// CBO picks: IndexSeek(b) + Sort
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a"))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		// Force: IndexScan(a,b) with covering filter on b
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}

func BenchmarkFilterSort_SortNonPrefix_10k(b *testing.B) {
	// Smaller dataset for quick iteration
	coll := setupBenchCollection(b, 10000,
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"b"}},
	)

	b.Run("Default", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a"))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}

func BenchmarkFilterSort_SortNonPrefix_Limit_500k(b *testing.B) {
	// LIMIT query: Find({"b": 25}).Sort("a").Limit(10) with 500k docs.
	// This is where covering filter + sorted index scan should shine:
	// scan ~500 index entries to find 10 matches vs fetch+sort 10k docs.
	n := 500000
	coll := setupBenchCollection(b, n,
		IndexInfo{Fields: []string{"a", "b"}},
		IndexInfo{Fields: []string{"b"}},
	)

	b.Run("Default", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").Limit(10))
	})

	b.Run("IndexScan_CoverFilter", func(b *testing.B) {
		benchIter(b, coll.Find(`{"b": 25}`).Sort("a").Limit(10).IndexHint(
			IndexHint{IndexName: "a,b", Boost: 1000000},
		))
	})
}
