package vivf

import (
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// clusteredVecs draws n vectors from `centers` Gaussian clusters — a stand-in for
// real embeddings (which have cluster structure, unlike uniform-random vectors).
func clusteredVecs(n, dim, centers int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	cs := make([][]float32, centers)
	for c := range cs {
		v := make([]float32, dim)
		for d := range v {
			v[d] = rng.Float32()*2 - 1
		}
		cs[c] = v
	}
	out := make([][]float32, n)
	for i := range out {
		base := cs[rng.Intn(centers)]
		v := make([]float32, dim)
		for d := range v {
			v[d] = base[d] + float32(rng.NormFloat64())*0.1
		}
		out[i] = v
	}
	return out
}

func bid(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i+1))
	return b
}

func openMem(t testing.TB) *btree.DB {
	t.Helper()
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func buildStore(t testing.TB, db *btree.DB, p StoreParams, vecs [][]float32) {
	t.Helper()
	ids := make([][]byte, len(vecs))
	for i := range vecs {
		ids[i] = bid(i)
	}
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = BulkBuild(wtx, "ivf", p, ids, vecs)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
}

// TestStoreIVFPQSmall exercises build -> search -> insert -> delete on a real
// in-memory btree with clustered synthetic data.
func TestStoreIVFPQSmall(t *testing.T) {
	const (
		n   = 4000
		dim = 64
		k   = 10
	)
	vecs := clusteredVecs(n, dim, 40, 7)
	db := openMem(t)
	p := StoreParams{Dim: dim, NList: 64, M: 16, Assign: 2, NProbe: 16, Normalize: true, KMeansPP: true, Seed: 1}
	buildStore(t, db, p, vecs)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(t, err)

	// A query equal to vecs[100] must return doc 100 first (distance ~0).
	cands, err := ix.SearchCandidates(rtx, vecs[100], 50)
	require.NoError(t, err)
	require.NotEmpty(t, cands)
	require.Equal(t, bid(100), cands[0].DocID, "nearest of a stored vector is itself")
	for i := 1; i < len(cands); i++ {
		require.LessOrEqual(t, cands[i-1].Distance, cands[i].Distance, "candidates must be closest-first")
	}
	require.NoError(t, rtx.Rollback())

	// Insert a brand-new vector, then find it.
	newVec := clusteredVecs(1, dim, 40, 999)[0]
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ixw, err := OpenTx(&wtx.ReadTx, "ivf")
	require.NoError(t, err)
	require.NoError(t, ixw.Insert(wtx, bid(99999), newVec))
	require.NoError(t, wtx.Commit())

	rtx, _ = db.BeginRead()
	ix, _ = OpenTx(rtx, "ivf")
	cands, err = ix.SearchCandidates(rtx, newVec, 50)
	require.NoError(t, err)
	require.Equal(t, bid(99999), cands[0].DocID, "inserted vector is its own nearest neighbour")
	require.NoError(t, rtx.Rollback())

	// Delete it; it must no longer appear.
	wtx, _ = db.BeginWrite()
	ixw, _ = OpenTx(&wtx.ReadTx, "ivf")
	removed, err := ixw.Delete(wtx, bid(99999))
	require.NoError(t, err)
	require.True(t, removed)
	require.NoError(t, wtx.Commit())

	rtx, _ = db.BeginRead()
	ix, _ = OpenTx(rtx, "ivf")
	cands, err = ix.SearchCandidates(rtx, newVec, 50)
	require.NoError(t, err)
	for _, c := range cands {
		require.NotEqual(t, bid(99999), c.DocID, "deleted vector must not be returned")
	}
	require.NoError(t, rtx.Rollback())
}

// TestStoreIVFPQLUTPathsAgree verifies the precomputed-table ADC path (used by
// small/medium indexes) and the per-cell sqL2 fallback (large indexes) return the
// same candidates and distances — the precomp decomposition must be exact.
func TestStoreIVFPQLUTPathsAgree(t *testing.T) {
	const dim = 48
	vecs := clusteredVecs(3000, dim, 50, 3)
	db := openMem(t)
	p := StoreParams{Dim: dim, NList: 64, M: 12, Assign: 3, NProbe: 12, Normalize: true, KMeansPP: true, Seed: 2}
	buildStore(t, db, p, vecs)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(t, err)
	require.NotNil(t, ix.precomp, "this config should use the precomputed table")

	for _, qi := range []int{0, 100, 500, 1500, 2999} {
		withPre, err := ix.SearchCandidates(rtx, vecs[qi], 50)
		require.NoError(t, err)
		saved := ix.precomp
		ix.precomp = nil // force the sqL2 fallback
		without, err := ix.SearchCandidates(rtx, vecs[qi], 50)
		ix.precomp = saved
		require.NoError(t, err)

		require.Equal(t, len(withPre), len(without))
		for i := range withPre {
			require.Equal(t, withPre[i].DocID, without[i].DocID, "query %d rank %d docID", qi, i)
			require.InDelta(t, withPre[i].Distance, without[i].Distance, 1e-4, "query %d rank %d dist", qi, i)
		}
	}
}

// TestStoreIVFPQRecallReal builds the btree-resident index on the real export and
// confirms its recall@10 matches the in-RAM prototype / HNSW baseline (~0.97),
// proving the storage layer preserves the algorithm's recall.
func TestStoreIVFPQRecallReal(t *testing.T) {
	if testing.Short() {
		t.Skip("recall diagnostic")
	}
	dir := vbenchDir()
	base, err := readF32(filepath.Join(dir, "base.f32"))
	if err != nil {
		t.Skipf("no ASV_VBENCH export at %s: %v", dir, err)
	}
	queries, err := readF32(filepath.Join(dir, "query.f32"))
	require.NoError(t, err)
	gt, err := readI32(filepath.Join(dir, "gt.i32"))
	require.NoError(t, err)
	qidx, err := readI32(filepath.Join(dir, "qidx.i32"))
	require.NoError(t, err)
	self := func(i int) int { return int(qidx[0][i]) }
	const k = 10
	dim := len(base[0])

	db := openMem(t)
	p := StoreParams{Dim: dim, NList: 256, M: 96, Assign: 4, NProbe: 16, Normalize: true, KMeansPP: true, Seed: 42}
	buildStore(t, db, p, base)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(t, err)

	const ef = 100 // re-rank depth (≈ prototype kFactor=10 at k=10)
	var sum float64
	for i, q := range queries {
		cands, err := ix.SearchCandidates(rtx, q, ef)
		require.NoError(t, err)
		got := make([]int, 0, k)
		for _, c := range cands {
			label := int(binary.BigEndian.Uint64(c.DocID)) - 1
			got = append(got, label)
		}
		got = exclude(got, self(i), k)
		sum += recallVsGT(got, gt[i], k)
	}
	r := sum / float64(len(queries))
	t.Logf("btree-resident IVF-PQ recall@%d = %.4f (nlist=256 M=96 assign=4 nprobe=16 ef=%d) — HNSW=0.970", k, r, ef)
	require.GreaterOrEqual(t, r, 0.95, "btree IVF-PQ must preserve prototype recall")
}
