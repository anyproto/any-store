package vindex

import (
	"encoding/binary"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func randVecs(n, dim int, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	v := make([][]float32, n)
	for i := range v {
		x := make([]float32, dim)
		for d := range x {
			x[d] = rng.Float32()*2 - 1
		}
		v[i] = x
	}
	return v
}

func docID(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i+1))
	return b
}

func l2(a, b []float32) float32 {
	var s float32
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return float32(math.Sqrt(float64(s)))
}

func bruteTopK(vecs [][]float32, q []float32, k int) map[string]bool {
	type p struct {
		i int
		d float32
	}
	all := make([]p, len(vecs))
	for i := range vecs {
		all[i] = p{i, l2(q, vecs[i])}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].d < all[j].d })
	out := map[string]bool{}
	for i := 0; i < k && i < len(all); i++ {
		out[string(docID(all[i].i))] = true
	}
	return out
}

func newTestIndex(t testing.TB, dim int, metric Metric) (*btree.DB, *Index) {
	t.Helper()
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: metric, EfSearch: 64}, 1)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	return db, ix
}

func insertAll(t testing.TB, db *btree.DB, ix *Index, vecs [][]float32) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	for i, v := range vecs {
		require.NoError(t, ix.Insert(wtx, docID(i), v))
	}
	require.NoError(t, wtx.Commit())
}

func TestVindexRecall(t *testing.T) {
	const (
		n   = 2000
		dim = 32
		k   = 10
	)
	vecs := randVecs(n, dim, 7)
	queries := randVecs(100, dim, 99)

	db, ix := newTestIndex(t, dim, L2)
	insertAll(t, db, ix, vecs)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// self-query returns itself
	hits, err := ix.Search(rtx, vecs[123], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, docID(123), hits[0].DocID)
	assert.InDelta(t, 0, hits[0].Distance, 1e-3)

	var recall float64
	for _, q := range queries {
		truth := bruteTopK(vecs, q, k)
		hits, err := ix.Search(rtx, q, k, 64)
		require.NoError(t, err)
		hit := 0
		for _, h := range hits {
			if truth[string(h.DocID)] {
				hit++
			}
		}
		recall += float64(hit) / float64(k)
	}
	recall /= float64(len(queries))
	t.Logf("btree-resident HNSW recall@%d = %.3f", k, recall)
	assert.Greater(t, recall, 0.85)
}

func TestVindexDelete(t *testing.T) {
	const (
		n   = 1000
		dim = 32
	)
	vecs := randVecs(n, dim, 3)
	db, ix := newTestIndex(t, dim, L2)
	insertAll(t, db, ix, vecs)

	// delete doc 50
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ok, err := ix.Delete(wtx, docID(50))
	require.NoError(t, err)
	require.True(t, ok)
	again, err := ix.Delete(wtx, docID(50))
	require.NoError(t, err)
	require.False(t, again)
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	hits, err := ix.Search(rtx, vecs[50], 10, 64)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, docID(50), h.DocID, "deleted doc leaked into results")
	}
}

func TestVindexUpdate(t *testing.T) {
	const dim = 24
	vecs := randVecs(500, dim, 5)
	db, ix := newTestIndex(t, dim, L2)
	insertAll(t, db, ix, vecs)

	// move doc 10 onto doc 400's location
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, ix.Insert(wtx, docID(10), vecs[400]))
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	hits, err := ix.Search(rtx, vecs[400], 5, 64)
	require.NoError(t, err)
	var found bool
	for _, h := range hits {
		if string(h.DocID) == string(docID(10)) {
			found = true
		}
	}
	assert.True(t, found, "updated doc not found at new location")
}

func TestVindexReopen(t *testing.T) {
	const dim = 32
	dir := t.TempDir() + "/v.db"
	vecs := randVecs(800, dim, 11)
	queries := randVecs(20, dim, 22)

	db, err := btree.Open(dir, btree.Options{})
	require.NoError(t, err)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: L2, EfSearch: 64}, 1)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	insertAll(t, db, ix, vecs)

	pre := make([][]Hit, len(queries))
	rtx, _ := db.BeginRead()
	for i, q := range queries {
		pre[i], _ = ix.Search(rtx, q, 10, 64)
	}
	rtx.Rollback()
	require.NoError(t, db.Close())

	// reopen
	db2, err := btree.Open(dir, btree.Options{})
	require.NoError(t, err)
	defer db2.Close()
	ix2, err := Open(db2, "vix", 1)
	require.NoError(t, err)
	require.Equal(t, dim, ix2.Dim())

	rtx2, _ := db2.BeginRead()
	defer rtx2.Rollback()
	for i, q := range queries {
		post, err := ix2.Search(rtx2, q, 10, 64)
		require.NoError(t, err)
		require.Equal(t, len(pre[i]), len(post))
		for j := range post {
			assert.Equal(t, pre[i][j].DocID, post[j].DocID, "result drift after reopen")
		}
	}
}
