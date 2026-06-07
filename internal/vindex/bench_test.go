package vindex

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
)

const (
	benchN   = 10000
	benchDim = 128
)

// buildBench builds an in-memory index of n vectors and returns the db + index.
func buildBench(b testing.TB, n, dim int) (*btree.DB, *Index, [][]float32) {
	b.Helper()
	vecs := randVecs(n, dim, 1)
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	if err != nil {
		b.Fatal(err)
	}
	wtx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: Cosine, EfSearch: 64}, 1)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < n; i++ {
		if err := ix.Insert(wtx, docID(i), vecs[i]); err != nil {
			b.Fatal(err)
		}
	}
	if err := wtx.Commit(); err != nil {
		b.Fatal(err)
	}
	return db, ix, vecs
}

// BenchmarkVindexSelectNeighbors isolates the Malkov neighbour-selection
// heuristic: a realistic ~efConstruction candidate set, selecting M0. It must be
// allocation-free (all scratch is reused on the pooled searcher).
func BenchmarkVindexSelectNeighbors(b *testing.B) {
	db, ix, _ := buildBench(b, benchN, benchDim)
	defer db.Close()
	rtx, err := db.BeginRead()
	if err != nil {
		b.Fatal(err)
	}
	defer rtx.Rollback()

	q := randVecs(1, benchDim, 5)[0]
	mt, err := ix.readMeta(rtx)
	if err != nil {
		b.Fatal(err)
	}
	s := ix.getSearcher(rtx, q)
	defer ix.putSearcher(s)
	ep := mt.entryLabel
	for lc := mt.topLayer; lc > 0; lc-- {
		if ep, err = s.greedyClosest(ep, lc); err != nil {
			b.Fatal(err)
		}
	}
	found, err := s.searchLayer(ep, ix.efC, 0)
	if err != nil {
		b.Fatal(err)
	}
	// Stable input copy (the heuristic returns s.sel, which it also overwrites).
	cands := append([]candidate(nil), found...)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.selectNeighborsHeuristic(cands, ix.m0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVindexVecDist isolates per-vector "read+distance" cost for the tier:
// f32 (zero-copy + SIMD distance) vs int8 (dequant + SIMD distance), at dim768.
// The gap is the dequant scalar loop — what an int8 SIMD kernel would remove.
func BenchmarkVindexVecDist(b *testing.B) {
	const dim = 768
	q := randVecs(1, dim, 1)[0]
	v := randVecs(1, dim, 2)[0]
	df := distanceFor(L2)
	f32rec := f32bytes(v)
	i8rec := encodeVec(nil, v, QuantInt8)
	dst := make([]float32, dim)

	b.Run("f32", func(b *testing.B) {
		b.ReportAllocs()
		var sink float32
		for i := 0; i < b.N; i++ {
			fv := bytesAsF32(f32rec, dim)
			sink += df(q, fv)
		}
		_ = sink
	})
	b.Run("int8", func(b *testing.B) {
		b.ReportAllocs()
		var sink float32
		for i := 0; i < b.N; i++ {
			fv, _ := decodeVecInto(i8rec, dim, QuantInt8, dst)
			sink += df(q, fv)
		}
		_ = sink
	})
}

func BenchmarkVindexSearch(b *testing.B) {
	db, ix, _ := buildBench(b, benchN, benchDim)
	defer db.Close()
	queries := randVecs(512, benchDim, 99)
	rtx, err := db.BeginRead()
	if err != nil {
		b.Fatal(err)
	}
	defer rtx.Rollback()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ix.Search(rtx, queries[i%len(queries)], 10, 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVindexBatchInsert measures a from-scratch batch build: N vectors
// inserted into a fresh index within a single write transaction (one fsync),
// which is how collection.Insert(docs...) drives the vector index. Reported
// ns/op is per whole batch; divide by batchN for per-vector cost. This is the
// realistic "bulk load" path, distinct from BenchmarkVindexInsert which appends
// to an already-large graph.
func BenchmarkVindexBatchInsert(b *testing.B) {
	const batchN = 2000
	vecs := randVecs(batchN, benchDim, 11)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		if err != nil {
			b.Fatal(err)
		}
		wtx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		ix, err := Create(wtx, "vix", Params{Dim: benchDim, Metric: Cosine, EfSearch: 64}, 1)
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < batchN; j++ {
			if err := ix.Insert(wtx, docID(j), vecs[j]); err != nil {
				b.Fatal(err)
			}
		}
		if err := wtx.Commit(); err != nil {
			b.Fatal(err)
		}
		db.Close()
	}
	b.ReportMetric(float64(batchN), "vecs/batch")
}

func BenchmarkVindexInsert(b *testing.B) {
	db, ix, _ := buildBench(b, benchN, benchDim)
	defer db.Close()
	extra := randVecs(4096, benchDim, 7)
	wtx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ix.Insert(wtx, docID(benchN+i), extra[i%len(extra)]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = wtx.Commit()
}
