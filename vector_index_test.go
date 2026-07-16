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

// vhit mirrors the old VectorHit result for tests.
type vhit struct {
	DocId    []byte
	Distance float32
}

// vsearch runs a k-NN search through the public Find() pipeline via the $knn
// operator and returns docId+distance pairs, closest-first. ef<=0 uses the
// index default. k must be > 0: $k is mandatory in the clause.
func vsearch(coll Collection, field string, q []float32, k, ef int) ([]vhit, error) {
	if k <= 0 {
		panic("vsearch: $knn requires k > 0")
	}
	fq := coll.Find(fmt.Sprintf(`{%q:%s}`, field, vknnJSON(q, k, ef)))
	iter, err := fq.Iter(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var out []vhit
	for iter.Next() {
		d, derr := iter.Doc()
		if derr != nil {
			return nil, derr
		}
		out = append(out, vhit{DocId: d.Value().Get("id").MarshalTo(nil), Distance: iter.Distance()})
	}
	return out, iter.Err()
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
	hits, err := vsearch(coll, "v", vecs[42], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	assert.InDelta(t, 0, hits[0].Distance, 1e-3)

	// recall vs brute
	queries := vrand(50, dim, 99)
	var recall float64
	for _, q := range queries {
		truth := bruteIDs(vecs, q, k)
		hh, err := vsearch(coll, "v", q, k, 64)
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
	hits, err := vsearch(coll, "v", vecs[50], 10, 64)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, idBytesOf(50), h.DocId, "deleted doc leaked")
	}

	// update doc 10's vector onto doc 400's location → found near 400
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(10, vecs[400]))))
	hits, err = vsearch(coll, "v", vecs[400], 5, 64)
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

	hits, err := vsearch(coll, "v", vecs[123], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(123), hits[0].DocId)
}

func TestVectorIndex_Stats(t *testing.T) {
	const (
		n   = 500
		dim = 32
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorCosine, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 9)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1, "vector index must appear in stats")
	vs := st.VectorIndexes[0]
	assert.Equal(t, "emb", vs.Name)
	assert.Equal(t, "v", vs.Field)
	assert.Equal(t, dim, vs.Dim)
	assert.Equal(t, "cosine", vs.Metric)
	assert.Equal(t, n, vs.NodeCount)
	assert.Equal(t, n, vs.LiveCount)
	assert.Equal(t, 0, vs.DeletedCount)
	assert.Greater(t, vs.VectorBytes, 0, "vec namespace should have size")
	assert.Greater(t, vs.GraphBytes, 0, "adj namespace should have size")
	assert.Greater(t, vs.SizeBytes, vs.VectorBytes, "total > just vectors")
	assert.Greater(t, st.VectorIndexesSizeBytes, 0)
	assert.GreaterOrEqual(t, st.TotalSizeBytes, st.VectorIndexesSizeBytes)

	// delete some docs → DeletedCount/LiveCount reflect the tombstones
	for i := 0; i < 100; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}
	st2, err := coll.Stats(ctx)
	require.NoError(t, err)
	vs2 := st2.VectorIndexes[0]
	assert.Equal(t, n, vs2.NodeCount, "physical node count unchanged by tombstones")
	assert.Equal(t, n-100, vs2.LiveCount)
	assert.Equal(t, 100, vs2.DeletedCount)
	t.Logf("vector index stats: nodes=%d live=%d deleted=%d  vec=%dB graph=%dB total=%dB",
		vs2.NodeCount, vs2.LiveCount, vs2.DeletedCount, vs2.VectorBytes, vs2.GraphBytes, vs2.SizeBytes)
}

func TestVectorIndex_Int8Quantization(t *testing.T) {
	const (
		dim = 32
		n   = 800
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, Quantization: VectorQuantInt8},
	}))
	vecs := vrand(n, dim, 4)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	// self-query still returns the doc with int8 storage
	hits, err := vsearch(coll, "v", vecs[42], 1, 128)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "int8", st.VectorIndexes[0].Quantization)
	t.Logf("int8 vector index: nodes=%d vec=%dB graph=%dB", st.VectorIndexes[0].NodeCount,
		st.VectorIndexes[0].VectorBytes, st.VectorIndexes[0].GraphBytes)

	// reopen-from-disk preserves int8 + results (covered for in-memory here via a
	// fresh Find through the pipeline)
	iter, err := coll.Find(fmt.Sprintf(`{"v":{"$knn":{"$query":[%s],"$k":1}}}`, joinFloats(vecs[7]))).Iter(ctx)
	require.NoError(t, err)
	require.True(t, iter.Next())
	d, _ := iter.Doc()
	assert.Equal(t, idBytesOf(7), d.Value().Get("id").MarshalTo(nil))
	require.NoError(t, iter.Close())
}

func joinFloats(vec []float32) string {
	parts := make([]string, len(vec))
	for i, f := range vec {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return strings.Join(parts, ",")
}

func TestVectorIndex_UpdateSkipsUnchangedVector(t *testing.T) {
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

	vecJSON := func(vec []float32) string {
		parts := make([]string, len(vec))
		for i, f := range vec {
			parts[i] = fmt.Sprintf("%g", f)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	nodesAndTombs := func() (int, int) {
		st, serr := coll.Stats(ctx)
		require.NoError(t, serr)
		require.Len(t, st.VectorIndexes, 1)
		return st.VectorIndexes[0].NodeCount, st.VectorIndexes[0].DeletedCount
	}

	n0, d0 := nodesAndTombs()

	// update a NON-vector field, keeping the same vector → index must NOT change
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(
		fmt.Sprintf(`{"id":5,"v":%s,"label":"changed"}`, vecJSON(vecs[5])))))
	n1, d1 := nodesAndTombs()
	assert.Equal(t, n0, n1, "vector index reindexed despite unchanged vector")
	assert.Equal(t, d0, d1, "vector index tombstoned despite unchanged vector")

	// now change the vector → index MUST reindex (old tombstoned, new node added)
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(
		fmt.Sprintf(`{"id":5,"v":%s}`, vecJSON(vecs[100])))))
	n2, d2 := nodesAndTombs()
	assert.Equal(t, n1+1, n2, "changed vector should add a node")
	assert.Equal(t, d1+1, d2, "changed vector should tombstone the old node")
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
	hits, err := vsearch(coll, "v", vecs[0], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)

	require.NoError(t, coll.DropIndex(ctx, "emb"))
	// the index is gone: dropping it again reports not-found
	require.ErrorIs(t, coll.DropIndex(ctx, "emb"), ErrIndexNotFound)
	// and "v" is now an ordinary field — a {"v":[..]} query is no longer an ANN
	// search (no vector index), so it runs as a plain filter without error.
	iter, ferr := coll.Find(fmt.Sprintf(`{"v":%s}`, vqJSON(vecs[0]))).Iter(ctx)
	require.NoError(t, ferr)
	require.NoError(t, iter.Close())
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
	pre := make([][]vhit, len(queries))
	for i, q := range queries {
		pre[i], err = vsearch(coll, "v", q, 10, 64)
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
		post, err := vsearch(coll2, "v", q, 10, 64)
		require.NoError(t, err)
		require.Equal(t, len(pre[i]), len(post))
		for j := range post {
			assert.Equal(t, pre[i][j].DocId, post[j].DocId, "result drift after reopen q%d", i)
		}
	}
}
