package anystore

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vrand(n, dim int, seed int64) [][]float32 {
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

func vecDocJSON(id int, vec []float32) string {
	parts := make([]string, len(vec))
	for i, f := range vec {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return fmt.Sprintf(`{"id":%d,"v":[%s]}`, id, strings.Join(parts, ","))
}

// idBytesOf returns the stored document-id bytes for an integer id.
func idBytesOf(id int) []byte {
	doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, id))
	it, _ := newItem(doc)
	return it.appendId(nil)
}

func vl2(a, b []float32) float32 {
	var s float32
	for i := range a {
		d := a[i] - b[i]
		s += d * d
	}
	return float32(math.Sqrt(float64(s)))
}

func bruteIDs(vecs [][]float32, q []float32, k int) map[string]bool {
	type p struct {
		i int
		d float32
	}
	all := make([]p, len(vecs))
	for i := range vecs {
		all[i] = p{i, vl2(q, vecs[i])}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].d < all[j].d })
	out := map[string]bool{}
	for i := 0; i < k && i < len(all); i++ {
		out[string(idBytesOf(all[i].i))] = true
	}
	return out
}

func TestVectorIndex_EndToEnd(t *testing.T) {
	const (
		n   = 1000
		dim = 16
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))

	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// self-query returns the same doc
	hits, err := coll.VectorSearch(ctx, "emb", vecs[42], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	assert.InDelta(t, 0, hits[0].Distance, 1e-3)

	// recall vs brute
	queries := vrand(50, dim, 99)
	var recall float64
	for _, q := range queries {
		truth := bruteIDs(vecs, q, k)
		hh, err := coll.VectorSearch(ctx, "emb", q, k, 64)
		require.NoError(t, err)
		hit := 0
		for _, h := range hh {
			if truth[string(h.DocId)] {
				hit++
			}
		}
		recall += float64(hit) / float64(k)
	}
	recall /= float64(len(queries))
	t.Logf("integrated vector index recall@%d = %.3f", k, recall)
	assert.Greater(t, recall, 0.85)
}

func TestVectorIndex_DeleteUpdate(t *testing.T) {
	const dim = 16
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2},
	}))

	vecs := vrand(500, dim, 3)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// delete doc 50 → must vanish from results
	require.NoError(t, coll.DeleteId(ctx, 50))
	hits, err := coll.VectorSearch(ctx, "emb", vecs[50], 10, 64)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, idBytesOf(50), h.DocId, "deleted doc leaked")
	}

	// update doc 10's vector onto doc 400's location → found near 400
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(10, vecs[400]))))
	hits, err = coll.VectorSearch(ctx, "emb", vecs[400], 5, 64)
	require.NoError(t, err)
	var found bool
	for _, h := range hits {
		if string(h.DocId) == string(idBytesOf(10)) {
			found = true
		}
	}
	assert.True(t, found, "updated doc not found at new location")
}

func TestVectorIndex_BuildOnExisting(t *testing.T) {
	const (
		n   = 800
		dim = 16
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	vecs := vrand(n, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	// create the index AFTER the docs exist → must build from them
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))

	hits, err := coll.VectorSearch(ctx, "emb", vecs[123], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(123), hits[0].DocId)
}

func TestVectorIndex_Drop(t *testing.T) {
	const dim = 16
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2},
	}))
	vecs := vrand(200, dim, 9)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	// works before drop
	hits, err := coll.VectorSearch(ctx, "emb", vecs[0], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)

	require.NoError(t, coll.DropIndex(ctx, "emb"))
	// search on a dropped index errors
	_, err = coll.VectorSearch(ctx, "emb", vecs[0], 1, 64)
	require.ErrorIs(t, err, ErrIndexNotFound)
	// inserts after drop must not fail (index gone)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(999, vecs[1]))))
}

func TestVectorIndex_Reopen(t *testing.T) {
	const (
		n   = 600
		dim = 16
	)
	dir := t.TempDir()
	path := filepath.Join(dir, "v.db")
	vecs := vrand(n, dim, 11)
	queries := vrand(20, dim, 22)

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	pre := make([][]VectorHit, len(queries))
	for i, q := range queries {
		pre[i], err = coll.VectorSearch(ctx, "emb", q, 10, 64)
		require.NoError(t, err)
	}
	require.NoError(t, db.Close())

	// reopen — index loads from disk, no rebuild
	db2, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	for i, q := range queries {
		post, err := coll2.VectorSearch(ctx, "emb", q, 10, 64)
		require.NoError(t, err)
		require.Equal(t, len(pre[i]), len(post))
		for j := range post {
			assert.Equal(t, pre[i][j].DocId, post[j].DocId, "result drift after reopen q%d", i)
		}
	}
}
