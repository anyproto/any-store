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
