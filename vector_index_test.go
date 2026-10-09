package anystore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
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
	idVal := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, id)).Get("id")
	if idVal == nil {
		panic("idBytesOf: document without id")
	}
	return idVal.MarshalTo(nil)
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
	slices.SortFunc(all, func(a, b p) int { return cmp.Compare(a.d, b.d) })
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

// ---- $knn driver/probe plan duality ---------------------------------------

// knnPlanHints force each $knn plan form: the vector index name forces the ANN
// driver, the pk field the pk probe, a secondary index name its probe.
var knnPlanHints = []struct {
	name string
	hint []IndexHint
}{
	{"cbo", nil},
	{"driver", []IndexHint{{IndexName: "emb", Boost: 1 << 30}}},
	{"probe-ids", []IndexHint{{IndexName: "id", Boost: 1 << 30}}},
	{"probe-a", []IndexHint{{IndexName: "a", Boost: 1 << 30}}},
}

// knnProbeColl builds a brute-or-ANN collection with a secondary index on "a".
func knnProbeColl(t *testing.T, mode VectorMode, n, dim int) (Collection, [][]float32) {
	fx := newFixture(t)
	t.Cleanup(fx.finish)
	coll, err := fx.CreateCollection(ctx, "knnprobe")
	require.NoError(t, err)
	makeVectorIndex(t, coll, mode, dim)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		doc := anyenc.MustParseJson(vecDocJSON(i, vc))
		doc.Set("a", anyenc.MustParseJson(fmt.Sprintf("%d", i%10)))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	return coll, vecs
}

func collectKnn(t *testing.T, q Query) (ids []int, dists []float32) {
	t.Helper()
	iter, err := q.Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, doc.Value().GetInt("id"))
		dists = append(dists, iter.Distance())
	}
	require.NoError(t, iter.Err())
	return ids, dists
}

// knnCond builds $knn + restriction conditions programmatically (the
// production construction path).
func knnCond(qv []float32, k int, extra query.Filter) query.Filter {
	knn := vectorKnnFilter(qv, k)
	if extra == nil {
		return knn
	}
	return query.And{knn, extra}
}

func idInFilter(ids ...int) query.Filter {
	vals := make([]*anyenc.Value, len(ids))
	for i, id := range ids {
		vals[i] = anyenc.MustParseJson(fmt.Sprintf("%d", id))
	}
	return query.Key{Path: []string{"id"}, Filter: query.NewInValue(vals...)}
}

// On the brute-force backend the driver is exact too, so every plan must
// produce identical rows, order, and distances.
func TestKnnProbe_BruteDifferential(t *testing.T) {
	const dim, n, k = 12, 300, 8
	coll, vecs := knnProbeColl(t, VectorModeBruteForce, n, dim)

	conds := []query.Filter{
		knnCond(vecs[5], k, idInFilter(3, 15, 27, 42, 55, 63, 78, 81, 99, 120, 150, 999)),
		knnCond(vecs[5], k, query.MustParseCondition(`{"a":3}`)),
		knnCond(vecs[9], k, nil),
	}
	for ci, cond := range conds {
		var baseIds []int
		var baseDists []float32
		for _, p := range knnPlanHints {
			q := coll.Find(cond)
			if len(p.hint) > 0 {
				q = q.IndexHint(p.hint...)
			}
			ids, dists := collectKnn(t, q)
			if p.name == "cbo" {
				baseIds, baseDists = ids, dists
				continue
			}
			require.Equal(t, baseIds, ids, "cond %d plan %s: rows diverged", ci, p.name)
			require.Equal(t, baseDists, dists, "cond %d plan %s: distances diverged", ci, p.name)
		}
	}
}

// Under a selective filter the exact probe must return the TRUE k nearest
// filter survivors — computed here by a brute oracle — and the CBO must route
// the restricted shapes to it. The ANN driver (post-filter) has no such
// guarantee; this is the recall fix, not just the perf fix.
func TestKnnProbe_ExactUnderFilter(t *testing.T) {
	const dim, n, k = 12, 400, 10
	coll, vecs := knnProbeColl(t, VectorModeBTree, n, dim)

	// Oracle: true k nearest among docs with a == 3 (L2).
	q := vecs[123]
	type oc struct {
		id int
		d  float64
	}
	var all []oc
	for i, vc := range vecs {
		if i%10 != 3 {
			continue
		}
		var sum float64
		for d := 0; d < dim; d++ {
			diff := float64(vc[d]) - float64(q[d])
			sum += diff * diff
		}
		all = append(all, oc{i, sum})
	}
	slices.SortFunc(all, func(x, y oc) int {
		if x.d != y.d {
			return cmp.Compare(x.d, y.d)
		}
		return cmp.Compare(x.id, y.id)
	})
	want := make([]int, k)
	for i := range want {
		want[i] = all[i].id
	}

	cond := knnCond(q, k, query.MustParseCondition(`{"a":3}`))
	ids, _ := collectKnn(t, coll.Find(cond).IndexHint(IndexHint{IndexName: "a", Boost: 1 << 30}))
	assert.Equal(t, want, ids, "probe plan must return the exact filtered k-nearest")
	assert.Len(t, ids, k, "probe plan must fill k when enough docs pass the filter")

	ex, err := coll.Find(cond).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, ex.Plan+ex.Sql, "KnnProbe", "selective filter must route to the probe plan")

	broad, err := coll.Find(knnCond(q, k, nil)).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, broad.Plan+broad.Sql, "KnnSearch", "unrestricted $knn must keep the ANN driver")
}

// A probe plan must honor _distance residual predicates and _distance sorts
// exactly like the driver (brute backend: bit-exact).
func TestKnnProbe_DistanceResidualAndSort(t *testing.T) {
	const dim, n, k = 12, 200, 12
	coll, vecs := knnProbeColl(t, VectorModeBruteForce, n, dim)

	restriction := idInFilter(1, 5, 9, 13, 17, 21, 25, 29, 33, 37, 41, 45, 49, 53)
	base := knnCond(vecs[13], k, restriction)

	var driverIds, probeIds []int
	for _, p := range []string{"emb", "id"} {
		q := coll.Find(base).IndexHint(IndexHint{IndexName: p, Boost: 1 << 30}).Sort("-_distance")
		ids, dists := collectKnn(t, q)
		for i := 1; i < len(dists); i++ {
			assert.GreaterOrEqual(t, dists[i-1], dists[i], "descending distance order")
		}
		if p == "emb" {
			driverIds = ids
		} else {
			probeIds = ids
		}
	}
	assert.Equal(t, driverIds, probeIds, "sorted-by-distance rows must match across plans")
}

// Bounded writes select the same rows under either plan form.
func TestKnnProbe_DeleteMatchesIterSelection(t *testing.T) {
	const dim, n, k = 12, 200, 5
	coll, vecs := knnProbeColl(t, VectorModeBruteForce, n, dim)

	restriction := idInFilter(0, 10, 20, 30, 40, 50, 60, 70, 80, 90)
	cond := knnCond(vecs[30], k, restriction)
	expect, _ := collectKnn(t, coll.Find(cond))
	require.Len(t, expect, k)

	res, err := coll.Find(cond).IndexHint(IndexHint{IndexName: "id", Boost: 1 << 30}).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, k, res.Modified)
	for _, id := range expect {
		_, gerr := coll.FindId(ctx, id)
		assert.ErrorIs(t, gerr, ErrDocNotFound, "doc %d must be deleted", id)
	}
}

// A REVERSE-declared unique index chosen as the $knn probe driver goes
// through CoverIter, which must receive un-finalized bounds — the
// reverse-tail pad would double-pad its seek and silently select nothing.
func TestKnnProbe_ReverseUniqueCoverDriver(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	t.Cleanup(fx.finish)
	coll, err := fx.CreateCollection(ctx, "knnrev")
	require.NoError(t, err)
	makeVectorIndex(t, coll, VectorModeBruteForce, dim)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "u", Fields: []string{"-u"}, Unique: true}))
	vecs := vrand(60, dim, 21)
	for i, vc := range vecs {
		doc := anyenc.MustParseJson(vecDocJSON(i, vc))
		doc.Set("u", anyenc.MustParseJson(fmt.Sprintf("%d", i)))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	for _, extra := range []query.Filter{
		query.MustParseCondition(`{"u":9}`),
		query.MustParseCondition(`{"u":{"$in":[3,17,42,99]}}`),
	} {
		cond := knnCond(vecs[9], 3, extra)
		driverIds, driverDists := collectKnn(t, coll.Find(cond).IndexHint(IndexHint{IndexName: "emb", Boost: 1 << 30}))
		probeIds, probeDists := collectKnn(t, coll.Find(cond).IndexHint(IndexHint{IndexName: "u", Boost: 1 << 30}))
		require.NotEmpty(t, driverIds)
		assert.Equal(t, driverIds, probeIds, "reverse-unique cover probe lost rows")
		assert.Equal(t, driverDists, probeDists)
	}
}

// TestVectorIndex_CompactMovesRootAndDetects checks the cross-process plumbing:
// compaction must move the index's namespace root pages, and the pre-compaction
// vectorIndex object must detect that (rootUnchanged == false) so a peer's
// reconcile reopens it with fresh handles instead of reading freed pages.
func TestVectorIndex_CompactMovesRootAndDetects(t *testing.T) {
	const dim = 16
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))
	vecs := vrand(300, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < 100; i++ { // create tombstones so compaction isn't a no-op
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	c := coll.(*collection)
	oldVI := c.loadVectorIndexes()[0]
	oldRoot := oldVI.ix.MetaRoot()

	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))

	newVI := c.loadVectorIndexes()[0]
	require.NotEqual(t, oldRoot, newVI.ix.MetaRoot(), "compaction must move the meta root page")

	require.NoError(t, c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		assert.False(t, oldVI.boundIn(tx, c.cur().name), "stale object must detect the moved root")
		assert.True(t, newVI.boundIn(tx, c.cur().name), "fresh object must be current")
		return nil
	}))
}

// TestKnnResidual_NoKnnSurvives is the hard post-condition behind the
// fail-closed design: for every legal placement, the residual handed to the
// FilterIter contains no Knn (a leaked Knn rejects every candidate — 0 rows,
// no-op Delete, err == nil — silently, on all verbs).
func TestKnnResidual_NoKnnSurvives(t *testing.T) {
	for _, cond := range []string{
		fmt.Sprintf(`{"v":%s}`, kd),
		fmt.Sprintf(`{"v":%s,"a":1}`, kd),
		fmt.Sprintf(`{"$and":[{"v":%s}]}`, kd),
		fmt.Sprintf(`{"$and":[{"v":%s},{"a":1}]}`, kd),
		fmt.Sprintf(`{"$and":[{"$and":[{"v":%s},{"a":1}]},{"b":2}]}`, kd),
		fmt.Sprintf(`{"$and":[{"$and":[{"v":%s}]},{"$and":[{"a":1},{"b":2}]}]}`, kd),
	} {
		f := query.MustParseCondition(cond)
		require.True(t, query.ContainsKnn(f), cond)
		residual := knnResidualFilter(f)
		assert.False(t, residual != nil && query.ContainsKnn(residual),
			"the Knn must be stripped from the residual: %s -> %v", cond, residual)
	}

	// Empty residual collapses to nil, NOT All{}: ef sizing and the
	// brute-force topK both key off "no residual" — All{} would flip every
	// bare $knn onto the ×10 over-fetch / full-ranking path invisibly.
	assert.Nil(t, knnResidualFilter(query.MustParseCondition(fmt.Sprintf(`{"v":%s}`, kd))))
	assert.Nil(t, knnResidualFilter(query.MustParseCondition(fmt.Sprintf(`{"$and":[{"v":%s}]}`, kd))))
}

// CompactRatio (and the IVF tuning params) must survive a DB reopen: they used
// to be dropped by registerIndex/getIndexInfos, permanently disabling
// auto-compaction after any restart.
func TestVectorParams_PersistAcrossReopen(t *testing.T) {
	const dim = 8
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	want := &VectorParams{
		Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64,
		CompactRatio: 0.5, NProbe: 8,
	}
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "emb", Kind: IndexKindVector, Vector: want}))
	require.NoError(t, fx.Close())

	db2, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll, err = db2.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	vidxs := coll.(*collection).loadVectorIndexes()
	require.Len(t, vidxs, 1)
	require.NotNil(t, vidxs[0].info.Vector)
	assert.Equal(t, *want, *vidxs[0].info.Vector)
	assert.Equal(t, want.CompactRatio, vidxs[0].compactRatio)
}

// makeVectorIndex creates a vector index of the given mode on field "v".
func makeVectorIndex(t *testing.T, coll Collection, mode VectorMode, dim int) {
	t.Helper()
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name:   "emb",
		Kind:   IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64, Mode: mode},
	}))
}

// vectorKnnFilter builds the programmatic $knn filter (query.NewKnn) used to
// issue a vector query through the normal Find pipeline — the same construction
// the downstream indexer uses (no JSON involved).
func vectorKnnFilter(qv []float32, k int) query.Filter {
	return query.Key{Path: []string{"v"}, Filter: query.NewKnn(qv, k)}
}

func vqJSON(vec []float32) string {
	parts := make([]string, len(vec))
	for i, f := range vec {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// vknnJSON renders the $knn clause value: {"$knn":{"$query":[...],"$k":k}}.
// ef<=0 omits $ef (the index default applies).
func vknnJSON(vec []float32, k, ef int) string {
	if ef > 0 {
		return fmt.Sprintf(`{"$knn":{"$query":%s,"$k":%d,"$ef":%d}}`, vqJSON(vec), k, ef)
	}
	return fmt.Sprintf(`{"$knn":{"$query":%s,"$k":%d}}`, vqJSON(vec), k)
}

const kd = `{"$knn":{"$query":[3,1,2],"$k":4}}`

// A vector index object is reused by a version only when every namespace
// of the index is at the root page the object was opened against. After a
// drop and a recreate with the same definition the freed pages come back:
// the :meta root may, while the others moved. A reader whose snapshot has
// the old index must not be served the new object, whose trees its
// snapshot does not have.
func TestVectorIndex_OlderReaderAfterRecreateOnSameRoots(t *testing.T) {
	const dim = 8
	info := IndexInfo{Name: "emb", Kind: IndexKindVector, Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2}}
	metaCameBack := false
	// How many pages are taken between the drop and the recreate decides
	// which of the freed roots the recreate gets back.
	for fillers := range 7 {
		t.Run(fmt.Sprintf("fillers=%d", fillers), func(t *testing.T) {
			metaCameBack = metaCameBack || vectorRecreateOnSameRoots(t, info, fillers)
		})
	}
	require.True(t, metaCameBack, "no run put :meta back on its old page; the sweep no longer covers the case")
}

// vectorRecreateOnSameRoots runs one case of
// TestVectorIndex_OlderReaderAfterRecreateOnSameRoots and reports whether
// the recreated index's :meta root is the dropped one's.
func vectorRecreateOnSameRoots(t *testing.T, info IndexInfo, fillers int) (metaCameBack bool) {
	const dim = 8
	{
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "docs")
		require.NoError(t, err)
		require.NoError(t, coll.CreateIndex(ctx, info))
		vecs := vrand(50, dim, 7)
		for i, vc := range vecs {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
		}
		c := coll.(*collection)
		before := c.cur().vindexes[0]

		rtx, err := fx.ReadTx(ctx)
		require.NoError(t, err)
		defer func() { _ = rtx.Commit() }()
		hits, err := vsearchIn(rtx.Context(), coll, "v", vecs[7], 3, 64)
		require.NoError(t, err)
		require.NotEmpty(t, hits)

		wtx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.DropIndex(wtx.Context(), "emb"))
		for i := range fillers {
			_, err = fx.CreateCollection(wtx.Context(), fmt.Sprintf("filler%d", i))
			require.NoError(t, err)
		}
		require.NoError(t, coll.CreateIndex(wtx.Context(), info))
		require.NoError(t, wtx.Commit())
		after := c.cur().vindexes[0]
		require.False(t, before == after)
		if before.ix.MetaRoot() == after.ix.MetaRoot() {
			metaCameBack = true
			t.Logf(":meta back on page %d, roots before %v, after %v", after.ix.MetaRoot(), before.ix.Roots(), after.ix.Roots())
		}

		// The older reader: its own snapshot's index, whatever root pages
		// the new one took.
		hits, err = vsearchIn(rtx.Context(), coll, "v", vecs[7], 3, 64)
		require.NoError(t, err)
		require.NotEmpty(t, hits)
		assert.Equal(t, string(idBytesOf(7)), string(hits[0].DocId))
		require.NoError(t, rtx.Commit())
		// And the newest state through the new object.
		hits, err = vsearch(coll, "v", vecs[7], 3, 64)
		require.NoError(t, err)
		assert.Equal(t, string(idBytesOf(7)), string(hits[0].DocId))
	}
	return metaCameBack
}

// vsearchIn is vsearch in the transaction of ctx.
func vsearchIn(tctx context.Context, coll Collection, field string, q []float32, k, ef int) ([]vhit, error) {
	iter, err := coll.Find(fmt.Sprintf(`{%q:%s}`, field, vknnJSON(q, k, ef))).Iter(tctx)
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
		out = append(out, vhit{DocId: idBytesOf(d.Value().GetInt("id"))})
	}
	return out, iter.Err()
}

// A rebuild can put every namespace of a vector index back on the root
// pages it had — two compactions of a small index do, the freed pages
// coming back in order — so the roots alone do not tell a reader's
// snapshot's index from the rebuilt one. The build identity does: a reader
// older than the rebuilds is served an object of its own snapshot's build,
// not the head's, whose RAM state belongs to the rebuilt graph.
func TestVectorIndex_OlderReaderAfterTwoCompactions(t *testing.T) {
	const dim = 8
	for _, mode := range []VectorMode{VectorModeBTree, VectorModeHybrid, VectorModeIVFSQ} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			vecs := vrand(20, dim, 11)
			for i, vc := range vecs {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
			}
			require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
				Name: "emb", Kind: IndexKindVector,
				Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: mode},
			}))
			c := coll.(*collection)
			roots := func(vi *vectorIndex) map[string]uint32 {
				if vi.isIVF() {
					return vi.ivf.Roots()
				}
				return vi.ix.Roots()
			}
			first := c.cur().vindexes[0]

			rtx, err := fx.ReadTx(ctx)
			require.NoError(t, err)
			defer func() { _ = rtx.Commit() }()
			hits, err := vsearchIn(rtx.Context(), coll, "v", vecs[7], 3, 64)
			require.NoError(t, err)
			require.Equal(t, string(idBytesOf(7)), string(hits[0].DocId))

			for round := range 2 {
				for i := range 3 {
					require.NoError(t, coll.DeleteId(ctx, 10+round*3+i))
				}
				require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))
			}
			head := c.cur().vindexes[0]
			require.False(t, first == head)
			require.Equal(t, roots(first), roots(head), "the recipe no longer puts every root back; the test covers nothing")

			s, err := c.resolve(rtx.btreeReadTx())
			require.NoError(t, err)
			require.Len(t, s.vindexes, 1)
			assert.False(t, s.vindexes[0] == head, "the older reader was served the rebuilt index's object")
			// A document the reader's snapshot has and the rebuilt index does
			// not, through the reader's own index.
			hits, err = vsearchIn(rtx.Context(), coll, "v", vecs[12], 1, 64)
			require.NoError(t, err)
			require.NotEmpty(t, hits)
			assert.Equal(t, string(idBytesOf(12)), string(hits[0].DocId))
			hits, err = vsearch(coll, "v", vecs[7], 3, 64)
			require.NoError(t, err)
			assert.Equal(t, string(idBytesOf(7)), string(hits[0].DocId))
		})
	}
}

// An index an earlier version built in the removed ivfpq mode: the catalog
// still describes it. Its collection opens with the index quarantined — $knn
// on its field and a compaction are ErrVectorIndexUnsupported, writes pass it
// by and leave its namespaces untouched, Stats report it with its sizes,
// EnsureIndex of the name in another mode says why it mismatches — and
// DropIndex removes it, namespaces included, so a supported mode can take its
// place. A replacement under another name serves unqualified $knn meanwhile.
// Creating one anew is refused.
func TestVectorIndex_RemovedModeQuarantined(t *testing.T) {
	const dim = 8
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(64, dim, 11)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	info := IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFSQ},
	}
	require.NoError(t, coll.CreateIndex(ctx, info))

	removed := *info.Vector
	removed.Mode = vectorModeIVFPQ
	err = coll.CreateIndex(ctx, IndexInfo{Name: "emb2", Kind: IndexKindVector, Vector: &removed})
	require.ErrorIs(t, err, ErrVectorIndexUnsupported)

	d := fx.DB.(*db)
	thisBuildTx(t, d, func(tx *btree.WriteTx) error { return forgeVectorMode(tx, d, "docs", "emb", vectorModeIVFPQ) })
	require.NoError(t, fx.Close())

	db2, err := Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	d2 := db2.(*db)
	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	st, err := coll2.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "ivfpq", st.VectorIndexes[0].Mode)
	assert.True(t, st.VectorIndexes[0].Unsupported)
	assert.Positive(t, st.VectorIndexes[0].SizeBytes, "the namespaces still hold the index")
	assert.Equal(t, st.VectorIndexes[0].SizeBytes, st.VectorIndexesSizeBytes)

	_, err = vsearch(coll2, "v", vecs[0], 3, 0)
	require.ErrorIs(t, err, ErrVectorIndexUnsupported)
	require.ErrorIs(t, coll2.CompactVectorIndex(ctx, "emb"), ErrVectorIndexUnsupported)
	err = coll2.EnsureIndex(ctx, info)
	require.ErrorIs(t, err, ErrIndexMismatch)
	require.ErrorIs(t, err, ErrVectorIndexUnsupported)

	// Writes to the collection succeed and leave the index as it was: its
	// :meta record (every maintained write re-encodes it) is byte-identical.
	before := vectorMetaBytes(t, d2, "docs", "emb")
	require.NotEmpty(t, before)
	require.NoError(t, coll2.Insert(ctx, anyenc.MustParseJson(vecDocJSON(100, vecs[1]))))
	assert.Equal(t, before, vectorMetaBytes(t, d2, "docs", "emb"))
	require.NoError(t, coll2.UpdateOne(ctx, anyenc.MustParseJson(vecDocJSON(100, vecs[2]))))
	require.NoError(t, coll2.DeleteId(ctx, 100))
	assert.Equal(t, before, vectorMetaBytes(t, d2, "docs", "emb"))

	// A replacement under another name: unqualified $knn resolves to it, the
	// quarantined one is still reachable by name.
	info2 := info
	info2.Name = "emb2"
	require.NoError(t, coll2.CreateIndex(ctx, info2))
	hits, err := vsearch(coll2, "v", vecs[0], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, string(idBytesOf(0)), string(hits[0].DocId))
	_, err = coll2.Find(fmt.Sprintf(`{"v":{"$knn":{"$query":%s,"$k":1,"$index":"emb"}}}`, vqJSON(vecs[0]))).Count(ctx)
	require.ErrorIs(t, err, ErrVectorIndexUnsupported)
	require.NoError(t, coll2.DropIndex(ctx, "emb2"))

	require.NoError(t, coll2.DropIndex(ctx, "emb"))
	names, err := d2.btreeDB.ListNamespaces()
	require.NoError(t, err)
	for _, n := range names {
		assert.False(t, strings.HasPrefix(n, vectorIndexNsPrefix("docs", "emb")), n)
	}

	require.NoError(t, coll2.CreateIndex(ctx, info))
	hits, err = vsearch(coll2, "v", vecs[0], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, string(idBytesOf(0)), string(hits[0].DocId))
}

// The switch at run time, as a peer process's DDL (otherBuildTx: the cookie
// moves, the head is not told): a supported index becomes quarantined — a
// reader pinned before keeps searching its own build — and back. The way
// back is the trap: the quarantined verdict was cached in this process's
// head, and a verb must not end on it once the committed schema says the
// index is fine again (a process healing on the error would drop a peer's
// fresh index).
func TestVectorIndex_RemovedModeSwitchAtRunTime(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(64, dim, 11)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFSQ},
	}))
	d := fx.DB.(*db)
	c := coll.(*collection)
	supported := c.cur().vindexes[0]

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	defer func() { _ = rtx.Commit() }()

	otherBuildTx(t, d, func(tx *btree.WriteTx) error { return forgeVectorMode(tx, d, "docs", "emb", vectorModeIVFPQ) })
	_, err = vsearch(coll, "v", vecs[0], 1, 0)
	require.ErrorIs(t, err, ErrVectorIndexUnsupported)
	head := c.cur().vindexes[0]
	assert.True(t, head.unsupported)
	assert.False(t, head == supported, "the supported handle was reused for the quarantined index")
	hits, err := vsearchIn(rtx.Context(), coll, "v", vecs[0], 1, 0)
	require.NoError(t, err, "the older reader searches its own build")
	require.Len(t, hits, 1)

	otherBuildTx(t, d, func(tx *btree.WriteTx) error { return forgeVectorMode(tx, d, "docs", "emb", VectorModeIVFSQ) })
	// Every verb validates its sources before its transaction; each must get
	// past the cached verdict on its own (the first to run refreshes the head
	// for the rest, so the order here is not what proves it — the stash
	// check in the commit is).
	knn := fmt.Sprintf(`{"v":%s,"id":-1}`, vknnJSON(vecs[0], 1, 0))
	_, err = coll.Find(knn).Count(ctx)
	require.NoError(t, err, "Count ended on the cached quarantined verdict")
	_, err = coll.Find(knn).Explain(ctx)
	require.NoError(t, err, "Explain ended on the cached quarantined verdict")
	_, err = coll.Find(knn).Delete(ctx)
	require.NoError(t, err, "Delete ended on the cached quarantined verdict")
	hits, err = vsearch(coll, "v", vecs[0], 1, 0)
	require.NoError(t, err, "the verb ended on the cached quarantined verdict")
	require.Len(t, hits, 1)
	assert.Equal(t, string(idBytesOf(0)), string(hits[0].DocId))
	assert.False(t, c.cur().vindexes[0].unsupported)
}

// The CompactRatio drift trigger fires an auto-rebuild through the public
// write path: the build identity changes after a batch the frozen centroids
// do not cover, and does not with the trigger off (the black-box recall
// checks in test/ cannot tell, IVF-SQ recall being int8-bound either way).
func TestVectorIndex_IVFAutoRebuildFires(t *testing.T) {
	const dim = 16
	for _, ratio := range []float64{0, 0.5} {
		t.Run(fmt.Sprintf("ratio=%g", ratio), func(t *testing.T) {
			fx := newFixture(t)
			coll, err := fx.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			a := vrand(500, dim, 1)
			for i, vc := range a {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
			}
			require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
				Name: "emb", Kind: IndexKindVector,
				Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFSQ, CompactRatio: ratio},
			}))
			c := coll.(*collection)
			built := c.cur().vindexes[0].ivf.Build()
			b := vrand(500, dim, 2)
			docs := make([]*anyenc.Value, len(b))
			for i, vc := range b {
				for d := range vc {
					vc[d] += 6 // far from every build-time centroid
				}
				docs[i] = anyenc.MustParseJson(vecDocJSON(1000+i, vc))
			}
			require.NoError(t, coll.Insert(ctx, docs...))
			after := c.cur().vindexes[0].ivf.Build()
			if ratio > 0 {
				assert.NotEqual(t, built, after, "the drifted batch must trigger a rebuild")
			} else {
				assert.Equal(t, built, after, "no auto-rebuild with the trigger off")
			}
			hits, err := vsearch(coll, "v", b[3], 1, 0)
			require.NoError(t, err)
			require.Len(t, hits, 1)
			assert.Equal(t, string(idBytesOf(1003)), string(hits[0].DocId))
		})
	}
}

// forgeVectorMode rewrites the catalog record of a vector index with the
// given mode — the record an index built in that mode by another version
// would have.
func forgeVectorMode(tx *btree.WriteTx, d *db, collName, indexName string, mode VectorMode) error {
	key := indexKey(collName, indexName)
	raw, err := tx.Get(d.systemNS, key)
	if err != nil {
		return err
	}
	var p anyenc.Parser
	v, err := p.Parse(raw)
	if err != nil {
		return err
	}
	var a anyenc.Arena
	v.Get("vector").Set("mode", a.NewNumberInt(int(mode)))
	return tx.Put(d.systemNS, key, v.MarshalTo(nil))
}

// vectorMetaBytes returns a vector index's :meta record.
func vectorMetaBytes(t *testing.T, d *db, collName, indexName string) []byte {
	t.Helper()
	var raw []byte
	require.NoError(t, d.doReadTx(ctx, func(tx *btree.ReadTx) error {
		ns, err := tx.GetNamespace(vectorIndexNsPrefix(collName, indexName) + ":meta")
		if err != nil {
			return err
		}
		raw, err = tx.Get(ns, []byte("m"))
		return err
	}))
	return raw
}

// A document the transaction deletes after the probe kept it is yielded as
// kept: its Doc fails with ErrDocNotFound, its Distance stands and the scan
// goes on to the end. The ANN driver skips it and yields the first k of the
// candidates it collected, fewer once they run out. A sort over either plan
// collects every row at the first Next and yields the deleted one the
// probe's way.
func TestKnnProbe_DeletedSinceKept(t *testing.T) {
	const dim, n, k = 12, 200, 5
	plans := []struct {
		name string // the plan, as Explain names it
		cond func(vecs [][]float32) query.Filter
		hint IndexHint
	}{
		{"KnnProbeIds", func(v [][]float32) query.Filter {
			return knnCond(v[30], k, idInFilter(0, 10, 20, 30, 40, 50, 60, 70, 80, 90))
		}, IndexHint{IndexName: "id", Boost: 1 << 30}},
		{"KnnProbeSeek", func(v [][]float32) query.Filter {
			return knnCond(v[30], k, query.MustParseCondition(`{"a":3}`))
		}, IndexHint{IndexName: "a", Boost: 1 << 30}},
		{"KnnSearch", func(v [][]float32) query.Filter {
			return knnCond(v[30], k, nil)
		}, IndexHint{IndexName: "emb", Boost: 1 << 30}},
	}
	for _, mode := range []VectorMode{VectorModeBruteForce, VectorModeBTree} {
		coll, vecs := knnProbeColl(t, mode, n, dim)
		// run opens q in a write transaction, deletes gone after the first
		// Next and drains: the ids yielded with a document, and the rows
		// whose Doc failed with ErrDocNotFound, each at distance dist.
		run := func(t *testing.T, q Query, gone int, dist float32) (got []int, missing int) {
			t.Helper()
			tx, err := coll.WriteTx(ctx)
			require.NoError(t, err)
			// A failed assertion leaves no writer behind the next subtest.
			t.Cleanup(func() { _ = tx.Rollback() })
			iter, err := q.Iter(tx.Context())
			require.NoError(t, err)
			t.Cleanup(func() { _ = iter.Close() })
			require.True(t, iter.Next())
			require.NoError(t, coll.DeleteId(tx.Context(), gone))
			for {
				d, err := iter.Doc()
				if errors.Is(err, ErrDocNotFound) {
					assert.Equal(t, dist, iter.Distance())
					missing++
				} else {
					require.NoError(t, err)
					got = append(got, d.Value().GetInt("id"))
				}
				if !iter.Next() {
					break
				}
			}
			require.NoError(t, iter.Err())
			require.NoError(t, iter.Close())
			require.NoError(t, tx.Rollback())
			return got, missing
		}
		for _, p := range plans {
			cond := p.cond(vecs)
			q := func() Query { return coll.Find(cond).IndexHint(p.hint) }
			ex, err := q().Explain(ctx)
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(ex.Plan, "Plan: "+p.name), ex.Plan)
			expect, dists := collectKnn(t, q())
			require.Len(t, expect, k)
			gone := expect[2]
			t.Run(fmt.Sprintf("%v/%s", mode, p.name), func(t *testing.T) {
				got, missing := run(t, q(), gone, dists[2])
				assert.NotContains(t, got, gone)
				if p.name == "KnnSearch" {
					// The first k candidates that remain: the next fills
					// k while the candidates last.
					assert.Equal(t, 0, missing)
					assert.GreaterOrEqual(t, len(got), k-1)
					assert.LessOrEqual(t, len(got), k)
					return
				}
				assert.Equal(t, 1, missing)
				assert.Equal(t, append([]int{expect[0], expect[1]}, expect[3:]...), got)
			})
			// The contract of a sort, under either plan; passes without the
			// fix too: the sort drains the probe before the delete.
			t.Run(fmt.Sprintf("%v/%s sorted", mode, p.name), func(t *testing.T) {
				got, missing := run(t, q().Sort("-_distance"), gone, dists[2])
				assert.Equal(t, 1, missing)
				assert.NotContains(t, got, gone)
				assert.Len(t, got, k-1)
			})
		}
	}
}
