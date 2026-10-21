package vivf

import (
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// benchIndex builds a clustered index with the given metric in an in-memory
// btree, for the search and open benchmarks.
func benchIndex(b *testing.B, n, dim int, normalize bool) (*btree.DB, *btree.ReadTx, *StoreIndex, [][]float32) {
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
	p := StoreParams{Dim: dim, NList: 256, Assign: 1, NProbe: 32, Normalize: normalize, KMeansPP: true, Seed: 1}
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

// BenchmarkSearchCandidates measures search throughput (the scanCells hot loop
// scores every probed-cell member by exact int8 distance) at dim768, for cosine
// and L2 — the path the byte kernel accelerates.
func BenchmarkSearchCandidates(b *testing.B) {
	const n, dim = 20000, 768
	for _, m := range []struct {
		name string
		norm bool
	}{{"cosine", true}, {"l2", false}} {
		b.Run(m.name, func(b *testing.B) {
			_, rtx, ix, vecs := benchIndex(b, n, dim, m.norm)
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

// BenchmarkOpenTx measures opening an existing index (aliasing the centroid
// blob) — it runs once per open / cross-process reconcile.
func BenchmarkOpenTx(b *testing.B) {
	const (
		n   = 20000
		dim = 768
	)
	db, _, _, _ := benchIndex(b, n, dim, true)
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

// BenchmarkBulkBuild measures a whole bulk build — k-means and placement — of a
// clustered 20k×768 set into a fresh in-memory btree, one build per iteration.
// A build takes seconds: run it with -benchtime=Nx.
func BenchmarkBulkBuild(b *testing.B) {
	const (
		n   = 20000
		dim = 768
	)
	vecs := clusteredVecs(n, dim, 100, 7)
	ids := make([][]byte, n)
	for i := range ids {
		ids[i] = bid(i)
	}
	p := StoreParams{Dim: dim, NList: 256, Assign: 1, NProbe: 32, Normalize: true, KMeansPP: true, Seed: 1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, err := btree.Open(":memory:", btree.Options{InMemory: true})
		require.NoError(b, err)
		wtx, err := db.BeginWrite()
		require.NoError(b, err)
		b.StartTimer()
		if _, err := BulkBuild(wtx, "ivf", p, ids, vecs); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		_ = wtx.Rollback()
		_ = db.Close()
		b.StartTimer()
	}
}

// BenchmarkBulkBuildPlace isolates the placement pass of a bulk build — each
// vector's cells and int8 record, and the :lbl/:doc/:cell writes — from
// training: k-means runs once, and each iteration places the same clustered
// 20k×768 set into a fresh in-memory btree.
func BenchmarkBulkBuildPlace(b *testing.B) {
	const (
		n     = 20000
		dim   = 768
		nlist = 256
	)
	norm := clusteredVecs(n, dim, 100, 7)
	ids := make([][]byte, n)
	for i := range norm {
		norm[i] = normalize(norm[i])
		ids[i] = bid(i)
	}
	cents, assign := kmeans(norm, nlist, 15, 1, true)
	coarse := bytesAsF32(encodeCentroids(cents), nlist*dim)
	for _, closure := range []int{1, 2} {
		b.Run(fmt.Sprintf("assign%d", closure), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db, err := btree.Open(":memory:", btree.Options{InMemory: true})
				require.NoError(b, err)
				wtx, err := db.BeginWrite()
				require.NoError(b, err)
				ix := &StoreIndex{dim: dim, nlist: nlist, assign: closure, normalize: true, coarse: coarse}
				for _, ns := range []struct {
					dst  **btree.Namespace
					name string
				}{{&ix.vcell, nsCell}, {&ix.vlbl, nsLbl}, {&ix.vdoc, nsDoc}} {
					*ns.dst, err = wtx.CreateNamespace("ivf" + ns.name)
					require.NoError(b, err)
				}
				b.StartTimer()
				if _, err := ix.place(wtx, ids, norm, assign); err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				_ = wtx.Rollback()
				_ = db.Close()
				b.StartTimer()
			}
		})
	}
}
