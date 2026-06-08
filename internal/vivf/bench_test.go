package vivf

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

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
