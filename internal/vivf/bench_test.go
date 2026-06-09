package vivf

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// benchIndexSQ builds an IVF-SQ index (int8 full vectors in :cell, scanned
// directly) with the given metric, for the SQ search benchmark.
func benchIndexSQ(b *testing.B, n, dim int, normalize bool) (*btree.DB, *btree.ReadTx, *StoreIndex, [][]float32) {
	b.Helper()
	vecs := clusteredVecs(n, dim, 100, 7)
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	ids := make([][]byte, n)
	for i := range vecs {
		ids[i] = bid(i)
	}
	wtx, err := db.BeginWrite()
	require.NoError(b, err)
	p := StoreParams{Dim: dim, NList: 256, M: dim / 8, Assign: 4, NProbe: 16, Normalize: normalize, SQ: true, KMeansPP: true, Seed: 1}
	_, err = BulkBuild(wtx, "ivf", p, ids, vecs)
	require.NoError(b, err)
	require.NoError(b, wtx.Commit())
	rtx, err := db.BeginRead()
	require.NoError(b, err)
	b.Cleanup(func() { _ = rtx.Rollback() })
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(b, err)
	return db, rtx, ix, vecs
}

// BenchmarkSearchCandidatesSQ measures IVF-SQ search throughput (the scanCellsSQ
// hot loop scores every probed-cell member by exact int8 distance) at dim768,
// for cosine and L2 — the path the byte kernel accelerates.
func BenchmarkSearchCandidatesSQ(b *testing.B) {
	const n, dim = 20000, 768
	for _, m := range []struct {
		name string
		norm bool
	}{{"cosine", true}, {"l2", false}} {
		b.Run(m.name, func(b *testing.B) {
			_, rtx, ix, vecs := benchIndexSQ(b, n, dim, m.norm)
			queries := vecs[:256]
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				cands, err := ix.SearchCandidates(rtx, queries[i%len(queries)], 100)
				if err != nil {
					b.Fatal(err)
				}
				if len(cands) == 0 {
					b.Fatal("no candidates")
				}
			}
		})
	}
}

// benchIndex builds a moderate clustered index in an in-memory btree for the
// search benchmark.
func benchIndex(b *testing.B, n, dim int) (*btree.DB, *btree.ReadTx, *StoreIndex, [][]float32) {
	b.Helper()
	vecs := clusteredVecs(n, dim, 100, 7)
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(b, err)
	b.Cleanup(func() { _ = db.Close() })
	ids := make([][]byte, n)
	for i := range vecs {
		ids[i] = bid(i)
	}
	wtx, err := db.BeginWrite()
	require.NoError(b, err)
	p := StoreParams{Dim: dim, NList: 256, M: dim / 8, Assign: 4, NProbe: 16, Normalize: true, KMeansPP: true, Seed: 1}
	_, err = BulkBuild(wtx, "ivf", p, ids, vecs)
	require.NoError(b, err)
	require.NoError(b, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(b, err)
	b.Cleanup(func() { _ = rtx.Rollback() })
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(b, err)
	return db, rtx, ix, vecs
}

// BenchmarkOpenTx measures opening an existing index (decoding the codebooks into
// RAM) — the path the arena/flat codebook layout targets (it ran once per open /
// cross-process reconcile and used to build ~m·256 slice headers).
func BenchmarkOpenTx(b *testing.B) {
	const (
		n   = 20000
		dim = 768 // m≈96 → ~24k codeword headers in the nested layout
	)
	db, _, _, _ := benchIndex(b, n, dim)
	rtx, err := db.BeginRead()
	require.NoError(b, err)
	b.Cleanup(func() { _ = rtx.Rollback() })
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ix, err := OpenTx(rtx, "ivf")
		if err != nil {
			b.Fatal(err)
		}
		_ = ix
	}
}

func BenchmarkSearchCandidates(b *testing.B) {
	const (
		n   = 20000
		dim = 64
	)
	_, rtx, ix, vecs := benchIndex(b, n, dim)
	queries := vecs[:256]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := queries[i%len(queries)]
		cands, err := ix.SearchCandidates(rtx, q, 100)
		if err != nil {
			b.Fatal(err)
		}
		if len(cands) == 0 {
			b.Fatal("no candidates")
		}
	}
}
