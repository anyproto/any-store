package test

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doc with vector + a couple scalar fields for filter/sort tests.
func vecDocFull(id int, vec []float32, lang, name string) string {
	parts := make([]string, len(vec))
	for i, f := range vec {
		parts[i] = fmt.Sprintf("%g", f)
	}
	return fmt.Sprintf(`{"id":%d,"v":[%s],"lang":%q,"name":%q}`, id, strings.Join(parts, ","), lang, name)
}

func setupPipeline(t *testing.T, n, dim int) (anystore.Collection, [][]float32) {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		lang := "en"
		if i%2 == 1 {
			lang = "fr"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocFull(i, vc, lang, fmt.Sprintf("name%04d", i)))))
	}
	return coll, vecs
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

// TestPipeline_Minimum: Find({"v":{"$knn":..}}).Iter() — implicit distance order + Distance().
func TestPipeline_Minimum(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 800, dim)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[42], 64, 0))).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	var (
		count    int
		prevDist float32 = -1
		topDocId []byte
	)
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		dist := iter.Distance()
		// distances must be non-decreasing (implicit _distance asc sort)
		assert.GreaterOrEqual(t, dist, prevDist, "results not in distance order")
		prevDist = dist
		if count == 0 {
			topDocId = d.Value().Get("id").MarshalTo(nil)
		}
		count++
	}
	require.NoError(t, iter.Err())
	require.Greater(t, count, 0)
	// nearest neighbour of vecs[42] is doc 42 itself (distance ~0)
	want := anyenc.MustParseJson(`42`).MarshalTo(nil)
	assert.Equal(t, want, topDocId, "closest doc should be the self-vector")
}

// TestPipeline_DistanceFilterAndSort: _distance threshold + explicit multi-key sort.
func TestPipeline_DistanceFilterAndSort(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 800, dim)

	const thr = 1.5
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"_distance":{"$lt":%g}}`, vknnJSON(vecs[100], 64, 0), thr)).
		Sort("_distance", "name").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	var count int
	var prev float32 = -1
	for iter.Next() {
		dist := iter.Distance()
		assert.Less(t, dist, float32(thr), "result violates _distance < threshold")
		assert.GreaterOrEqual(t, dist, prev, "not sorted by _distance")
		prev = dist
		_, derr := iter.Doc()
		require.NoError(t, derr)
		count++
	}
	require.NoError(t, iter.Err())
	require.Greater(t, count, 0)
}

// TestPipeline_AdditionalFilter: a normal field filter combined with the vector clause.
func TestPipeline_AdditionalFilter(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 800, dim)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"lang":"en"}`, vknnJSON(vecs[10], 32, 0))).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	var count int
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		assert.Equal(t, "en", string(d.Value().Get("lang").GetStringBytes()), "additional filter not applied")
		count++
	}
	require.NoError(t, iter.Err())
	require.Greater(t, count, 0)
}

// setupPipelineEf is like setupPipeline but with a small index EfSearch so
// over-fetch behaviour is observable.
func setupPipelineEf(t *testing.T, n, dim, efSearch int) (anystore.Collection, [][]float32) {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: efSearch},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		lang := "en"
		if i%2 == 1 {
			lang = "fr"
		}
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocFull(i, vc, lang, fmt.Sprintf("name%04d", i)))))
	}
	return coll, vecs
}

// TestPipeline_OverFetch: a filtered $knn fills $k because the auto ef
// over-fetches candidates (ef = k×factor), even though the index's own
// EfSearch is small.
func TestPipeline_OverFetch(t *testing.T) {
	const (
		dim      = 16
		efSearch = 16 // small: k alone (30) would not yield 30 en survivors
		limit    = 30 // $k
	)
	coll, vecs := setupPipelineEf(t, 1000, dim, efSearch)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"lang":"en"}`, vknnJSON(vecs[0], limit, 0))).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var count int
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		require.Equal(t, "en", string(d.Value().Get("lang").GetStringBytes()))
		count++
	}
	require.NoError(t, iter.Err())
	assert.Equal(t, limit, count, "over-fetch should fill $k despite a selective filter")
}

// TestPipeline_ExplicitEf: an explicit $ef bounds the candidate set — it
// disables the auto ×10 residual over-fetch, so a selective residual
// under-fills $k (the documented escape valve is raising $ef).
func TestPipeline_ExplicitEf(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipelineEf(t, 1000, dim, 64)

	const k = 30
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"lang":"en"}`, vknnJSON(vecs[0], k, k))).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var count int
	for iter.Next() {
		count++
	}
	require.NoError(t, iter.Err())
	assert.Less(t, count, k, "an explicit $ef == $k must not over-fetch: ~half the candidates are filtered out")
	assert.Greater(t, count, 0)
}

// TestPipeline_Errors: malformed vector queries return errors instead of
// silently degrading to a literal filter or matching everything.
func TestPipeline_Errors(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 100, dim) // field "v", dim 16

	knn := func(k int) string { return vknnJSON(vecs[0], k, 0) } // dim-16 query
	cases := []struct {
		name   string
		find   string
		sort   []any
		wantIs error
	}{
		{"legacy bare-array ANN clause", fmt.Sprintf(`{"v":%s}`, vqJSON(vecs[0])), nil, anystore.ErrLegacyVectorClause},
		{"legacy clause under $or", fmt.Sprintf(`{"$or":[{"v":%s},{"id":1}]}`, vqJSON(vecs[0])), nil, anystore.ErrLegacyVectorClause},
		{"wrong-dim $query", `{"v":{"$knn":{"$query":[1,2,3],"$k":5}}}`, nil, anystore.ErrInvalidVectorQuery},
		{"$knn on unindexed field", fmt.Sprintf(`{"nosuch":%s}`, knn(5)), nil, anystore.ErrNoVectorIndex},
		{"$index naming no index", `{"v":{"$knn":{"$query":[1,2,3],"$k":5,"$index":"bogus"}}}`, nil, anystore.ErrNoVectorIndex},
		{"$knn under $or", fmt.Sprintf(`{"$or":[{"v":%s},{"id":1}]}`, knn(5)), nil, anystore.ErrKnnBadPlacement},
		{"$knn under $nor", fmt.Sprintf(`{"$nor":[{"v":%s}]}`, knn(5)), nil, anystore.ErrKnnBadPlacement},
		{"two $knn clauses", fmt.Sprintf(`{"$and":[{"v":%s},{"v":%s}]}`, knn(5), knn(5)), nil, anystore.ErrMultipleVectorClauses},
		{"$knn with $text", fmt.Sprintf(`{"v":%s,"$text":{"$search":"x"}}`, knn(5)), nil, anystore.ErrKnnWithText},
		{"_distance filter w/o $knn", `{"_distance":{"$lt":1.0}}`, nil, anystore.ErrDistanceWithoutVector},
		{"_distance filter w/o $knn, inside $or", `{"$or":[{"_distance":{"$lt":1.0}},{"id":1}]}`, nil, anystore.ErrDistanceWithoutVector},
		{"_distance sort w/o $knn", `{"id":{"$gt":5}}`, []any{"-_distance"}, anystore.ErrDistanceWithoutVector},
		{"_distance sort, no filter", ``, []any{"_distance"}, anystore.ErrDistanceWithoutVector},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fq anystore.Query
			if tc.find == "" {
				fq = coll.Find(nil)
			} else {
				fq = coll.Find(tc.find)
			}
			if tc.sort != nil {
				fq = fq.Sort(tc.sort...)
			}
			_, err := fq.Iter(ctx)
			require.ErrorIs(t, err, tc.wantIs)
		})
	}

	// sanity: a valid $knn query (optionally with _distance) does NOT error
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"_distance":{"$lt":2.0}}`, knn(10))).
		Sort("-_distance").Iter(ctx)
	require.NoError(t, err)
	require.NoError(t, iter.Close())

	// Non-$knn predicates on a vector-indexed field are ORDINARY filters on
	// every verb — the rule, not the exception (programmatic consumers depend
	// on it: {"v":{"$exists":true}}.Count decides index builds downstream).
	for _, legal := range []string{
		`{"v":"hello"}`,          // non-array literal
		`{"v":[1,2,3]}`,          // wrong-dim array: not the ANN shape
		`{"v":{"$exists":true}}`, // predicate op
		`{"v":{"$size":16}}`,
	} {
		t.Run("legal literal "+legal, func(t *testing.T) {
			it, lerr := coll.Find(legal).Iter(ctx)
			require.NoError(t, lerr)
			require.NoError(t, it.Close())
			_, cerr := coll.Find(legal).Count(ctx)
			require.NoError(t, cerr)
		})
	}
}

// TestPipeline_Limit: limit applies over the distance-ordered results.
func TestPipeline_Limit(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 800, dim)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[5], 10, 0))).Limit(10).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var count int
	for iter.Next() {
		count++
	}
	require.NoError(t, iter.Err())
	assert.LessOrEqual(t, count, 10)
	assert.Greater(t, count, 0)
}

// TestPipeline_Offset: Offset pages the distance-ordered ANN results. The
// candidate list is sized for Offset+Limit, so a later page still fills and its
// window matches the corresponding slice of the full ranking (C2).
func TestPipeline_Offset(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 800, dim)
	qv := vknnJSON(vecs[5], 20, 0)

	collect := func(query anystore.Query) []string {
		iter, err := query.Iter(ctx)
		require.NoError(t, err)
		defer iter.Close()
		var (
			ids  []string
			prev float32 = -1
		)
		for iter.Next() {
			d, derr := iter.Doc()
			require.NoError(t, derr)
			dist := iter.Distance()
			assert.GreaterOrEqual(t, dist, prev, "results not in distance order")
			prev = dist
			ids = append(ids, string(d.Value().Get("id").MarshalTo(nil)))
		}
		require.NoError(t, iter.Err())
		return ids
	}

	full := collect(coll.Find(fmt.Sprintf(`{"v":%s}`, qv)).Limit(20))
	require.Len(t, full, 20)

	page2 := collect(coll.Find(fmt.Sprintf(`{"v":%s}`, qv)).Limit(10).Offset(10))
	require.Len(t, page2, 10, "offset page must still fill")
	assert.Equal(t, full[10:20], page2, "offset window must match the full-ranking slice")
}

// clusteredVecsAS draws n vectors from `centers` Gaussian clusters — realistic
// embedding-like structure (uniform-random vectors are near-equidistant in high
// dim and intrinsically hard for any ANN index).
func clusteredVecsAS(n, dim, centers int, seed int64) [][]float32 {
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

// makeIVFPQIndex inserts vecs, then creates an IVF-PQ index (which trains from the
// existing documents — the bulk-load pattern).
func makeIVFPQIndex(t *testing.T, coll anystore.Collection, vecs [][]float32, dim int) {
	t.Helper()
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb",
		Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{
			Field: "v", Dim: dim, Metric: anystore.VectorL2, Mode: anystore.VectorModeIVFPQ,
			Closure: 4, NProbe: 16,
		},
	}))
}

// TestVectorMode_IVFPQ_EndToEnd drives the btree-resident IVF-PQ index through the
// public Find() pipeline: self-retrieval, recall vs a brute oracle, _distance
// decoration, residual filtering, and update/delete.
func TestVectorMode_IVFPQ_EndToEnd(t *testing.T) {
	const (
		n   = 3000
		dim = 32
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 60, 7)
	makeIVFPQIndex(t, coll, vecs, dim)

	// Stats reports the new mode.
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "ivfpq", st.VectorIndexes[0].Mode)
	assert.Greater(t, st.VectorIndexes[0].SizeBytes, 0)

	// Self-query: a stored vector's nearest neighbour is itself (re-rank makes the
	// exact distance ~0), returned first.
	hits, err := vsearch(coll, "v", vecs[42], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	assert.InDelta(t, 0, hits[0].Distance, 1e-3)

	// Recall@k vs a brute-force L2 oracle. Queries are perturbed copies of random
	// base vectors (the realistic "find documents similar to this one" case), so the
	// true neighbours are well-defined near the source.
	rng := rand.New(rand.NewSource(123))
	queries := make([][]float32, 50)
	for i := range queries {
		src := vecs[rng.Intn(len(vecs))]
		q := make([]float32, dim)
		for d := range q {
			q[d] = src[d] + float32(rng.NormFloat64())*0.02
		}
		queries[i] = q
	}
	var recall float64
	for _, q := range queries {
		truth := bruteIDs(vecs, q, k)
		hh, err := vsearch(coll, "v", q, k, 0)
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
	t.Logf("IVF-PQ recall@%d = %.3f", k, recall)
	assert.GreaterOrEqual(t, recall, 0.85, "IVF-PQ + re-rank should reach high recall on clustered data")

	// _distance decoration + ascending order through the pipeline.
	iter, err := coll.Find(vectorKnnFilter(vecs[7], 5)).Iter(ctx)
	require.NoError(t, err)
	var last float32 = -1
	got7 := false
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		if doc.Value().GetInt("id") == 7 {
			got7 = true
		}
		d := float32(iter.Distance())
		require.GreaterOrEqual(t, d, last, "results must be _distance-ascending")
		last = d
	}
	require.NoError(t, iter.Close())
	assert.True(t, got7, "self-query must return its own document")
}

// TestVectorMode_IVFPQ_FilterUpdateDelete covers a residual metadata filter plus
// update/delete maintenance on the IVF-PQ index.
func TestVectorMode_IVFPQ_FilterUpdateDelete(t *testing.T) {
	const (
		n   = 2000
		dim = 24
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 40, 13)
	makeIVFPQIndex(t, coll, vecs, dim)

	// Residual filter: vector clause + id predicate. Every returned doc must satisfy
	// the predicate.
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"id":{"$lt":100}}`, vknnJSON(vecs[5], 10, 0))).Iter(ctx)
	require.NoError(t, err)
	cnt := 0
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		require.Less(t, doc.Value().GetInt("id"), 100, "residual filter must hold")
		cnt++
	}
	require.NoError(t, iter.Close())
	assert.Positive(t, cnt)

	// Update doc 5's vector to be near doc 1900's cluster; querying with the new
	// vector must now return doc 5.
	newVec := vecs[1900]
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(5, newVec))))
	hits, err := vsearch(coll, "v", newVec, 3, 0)
	require.NoError(t, err)
	found := false
	for _, h := range hits {
		if string(h.DocId) == string(idBytesOf(5)) {
			found = true
		}
	}
	assert.True(t, found, "updated vector must be findable at its new location")

	// Delete doc 5; it must no longer be returned for that vector.
	require.NoError(t, coll.DeleteId(ctx, 5))
	hits, err = vsearch(coll, "v", newVec, 5, 0)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, string(idBytesOf(5)), string(h.DocId), "deleted doc must not be returned")
	}
}

// driftedRecall builds an IVF-PQ index (with the given CompactRatio) on cluster A,
// then batch-inserts a far-shifted cluster B and returns recall on B. With
// auto-rebuild enabled (ratio>0) the post-insert maybeAutoCompactVectors re-trains
// the codebooks to cover B; with it disabled (ratio 0) the frozen centroids miss B.
func driftedRecall(t *testing.T, collName string, compactRatio float64) float64 {
	return driftedRecallMode(t, collName, compactRatio, false)
}

func driftedRecallMode(t *testing.T, collName string, compactRatio float64, manual bool) float64 {
	const (
		dim = 24
		nA  = 2000
		nB  = 2000
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, collName)
	require.NoError(t, err)

	A := clusteredVecsAS(nA, dim, 40, 1)
	for i, v := range A {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
	}
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, Mode: anystore.VectorModeIVFPQ,
			Closure: 4, NProbe: 16, CompactRatio: compactRatio},
	}))

	// Far-shifted cluster B, inserted as one batch (one self-contained write → at
	// most one auto-rebuild afterwards).
	B := make([][]float32, nB)
	Bdocs := make([]*anyenc.Value, nB)
	for i := range B {
		src := clusteredVecsAS(nB, dim, 40, 2)[i]
		v := make([]float32, dim)
		for d := range v {
			v[d] = src[d] + 6
		}
		B[i] = v
		Bdocs[i] = anyenc.MustParseJson(vecDocJSON(nA+i, v))
	}
	require.NoError(t, coll.Insert(ctx, Bdocs...))

	if manual {
		require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))
	}

	all := append(append([][]float32{}, A...), B...)
	var recall float64
	for _, q := range B[:50] {
		truth := bruteIDs(all, q, k)
		hh, err := vsearch(coll, "v", q, k, 0)
		require.NoError(t, err)
		hit := 0
		for _, h := range hh {
			if truth[string(h.DocId)] {
				hit++
			}
		}
		recall += float64(hit) / float64(k)
	}
	return recall / 50
}

// TestVectorMode_IVFPQ_AutoRebuild proves the CompactRatio drift trigger: inserting
// a distribution the build-time centroids don't cover auto-rebuilds the index
// (when CompactRatio>0), recovering recall that a frozen index loses.
func TestVectorMode_IVFPQ_AutoRebuild(t *testing.T) {
	frozen := driftedRecall(t, "frozen", 0)     // no auto-rebuild
	rebuilt := driftedRecall(t, "rebuilt", 0.5) // auto-rebuild on drift ≥ 0.5
	t.Logf("recall on drifted cluster: frozen=%.3f, auto-rebuilt=%.3f", frozen, rebuilt)
	assert.Greater(t, rebuilt, frozen, "auto-rebuild must recover recall lost to centroid drift")
	assert.GreaterOrEqual(t, rebuilt, 0.9, "rebuilt index covers the new distribution")
}

// TestVectorMode_IVFPQ_Persist verifies the index round-trips through a reopen.
func TestVectorMode_IVFPQ_Persist(t *testing.T) {
	const (
		n   = 1500
		dim = 16
	)
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 30, 5)
	makeIVFPQIndex(t, coll, vecs, dim)
	require.NoError(t, fx.Close())

	db2, err := anystore.Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	st, err := coll2.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "ivfpq", st.VectorIndexes[0].Mode)

	// Search still works after reopen (codebooks reloaded from :cb).
	hits, err := vsearch(coll2, "v", vecs[100], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(100), hits[0].DocId)
}

// makeIVFSQIndex inserts vecs, then creates an IVF-SQ index (int8 full vectors per
// cell, scanned directly — no PQ). IVF-SQ favours Closure=1 + higher NProbe.
func makeIVFSQIndex(t *testing.T, coll anystore.Collection, vecs [][]float32, dim int) {
	t.Helper()
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb",
		Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{
			Field: "v", Dim: dim, Metric: anystore.VectorL2, Mode: anystore.VectorModeIVFSQ, NProbe: 32,
		},
	}))
}

// TestVectorMode_IVFSQ_EndToEnd drives the IVF-SQ index through Find(): self-
// retrieval, recall vs a brute oracle, _distance ordering, and update/delete.
func TestVectorMode_IVFSQ_EndToEnd(t *testing.T) {
	const (
		n   = 3000
		dim = 32
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 60, 7)
	makeIVFSQIndex(t, coll, vecs, dim)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "ivfsq", st.VectorIndexes[0].Mode)
	assert.Greater(t, st.VectorIndexes[0].SizeBytes, 0)

	// Self-query: a stored vector's nearest neighbour is itself, returned first.
	hits, err := vsearch(coll, "v", vecs[42], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	// int8 storage → a vector's distance to itself is the small quantization error,
	// not exactly 0.
	assert.InDelta(t, 0, hits[0].Distance, 5e-2)

	// Recall@k vs a brute-force L2 oracle (perturbed-neighbour queries).
	rng := rand.New(rand.NewSource(123))
	var recall float64
	const nq = 50
	for qi := 0; qi < nq; qi++ {
		src := vecs[rng.Intn(len(vecs))]
		q := make([]float32, dim)
		for d := range q {
			q[d] = src[d] + float32(rng.NormFloat64())*0.02
		}
		truth := bruteIDs(vecs, q, k)
		hh, err := vsearch(coll, "v", q, k, 0)
		require.NoError(t, err)
		hit := 0
		for _, h := range hh {
			if truth[string(h.DocId)] {
				hit++
			}
		}
		recall += float64(hit) / float64(k)
	}
	recall /= nq
	t.Logf("IVF-SQ recall@%d = %.3f", k, recall)
	assert.GreaterOrEqual(t, recall, 0.9, "IVF-SQ should reach high recall on clustered data")

	// _distance ordering through the pipeline.
	iter, err := coll.Find(vectorKnnFilter(vecs[7], 5)).Iter(ctx)
	require.NoError(t, err)
	var last float32 = -1
	for iter.Next() {
		d := float32(iter.Distance())
		require.GreaterOrEqual(t, d, last, "results must be _distance-ascending")
		last = d
	}
	require.NoError(t, iter.Close())

	// Update doc 5 to a far cluster, then find it there; then delete it.
	newVec := vecs[2900]
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(5, newVec))))
	hits, err = vsearch(coll, "v", newVec, 3, 0)
	require.NoError(t, err)
	found := false
	for _, h := range hits {
		if string(h.DocId) == string(idBytesOf(5)) {
			found = true
		}
	}
	assert.True(t, found, "updated vector must be findable at its new location")
	require.NoError(t, coll.DeleteId(ctx, 5))
	hits, err = vsearch(coll, "v", newVec, 5, 0)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, string(idBytesOf(5)), string(h.DocId), "deleted doc must not be returned")
	}
}

// TestVectorMode_IVFSQ_Persist verifies an IVF-SQ index round-trips through reopen.
func TestVectorMode_IVFSQ_Persist(t *testing.T) {
	const (
		n   = 1500
		dim = 16
	)
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := clusteredVecsAS(n, dim, 30, 5)
	makeIVFSQIndex(t, coll, vecs, dim)
	require.NoError(t, fx.Close())

	db2, err := anystore.Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)
	st, err := coll2.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, "ivfsq", st.VectorIndexes[0].Mode)
	hits, err := vsearch(coll2, "v", vecs[100], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(100), hits[0].DocId)
}

// Rule V: no ordering comparison against a vector may ever match
// everything. Syntax-independent — this must hold no matter what the ANN query
// syntax becomes.
//
// The mass delete needed NO vector index and no vector-valued document: anyenc
// resolves cross-type comparisons on the type tag, and TypeVectorF32 (10) sorts
// above every scalar tag, so an ordering op with a vector operand was true for
// every document — including ones where the field is absent.
func TestVectorOrderingNeverMatchesEverything(t *testing.T) {
	t.Run("no vector index anywhere", func(t *testing.T) {
		ctx := context.Background()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "plain")
		require.NoError(t, err)

		docs := []string{
			`{"id":1,"n":5,"s":"abc","tags":[1,2]}`,
			`{"id":2,"n":-1,"s":"zzz"}`,
			`{"id":3,"b":true}`,
			`{"id":4,"o":{"k":"v"}}`,
			`{"id":5}`,
		}
		for _, d := range docs {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
		}

		// Parse-time: an ordering op against a $vector literal is rejected outright,
		// on every field — present, absent, scalar, array.
		for _, cond := range []string{
			`{"n":{"$lt":{"$vector":[0]}}}`,
			`{"s":{"$lt":{"$vector":[0]}}}`,
			`{"tags":{"$lt":{"$vector":[0]}}}`,
			`{"nosuchfield":{"$lt":{"$vector":[0]}}}`,
			`{"nosuchfield":{"$gt":{"$vector":[0]}}}`,
		} {
			_, err := coll.Find(cond).Count(ctx)
			assert.ErrorIs(t, err, query.ErrVectorNotOrderable, "cond=%s", cond)
		}

		// Eval-time: a hand-built filter never sees the parser. It must still match
		// nothing — and, above all, Delete must remove nothing.
		a := &anyenc.Arena{}
		vec := a.NewVectorF32([]float32{0})
		for _, op := range []query.CompOp{query.CompOpGt, query.CompOpGte, query.CompOpLt, query.CompOpLte} {
			for _, path := range []string{"n", "s", "tags", "nosuchfield"} {
				f := query.Key{Path: []string{path}, Filter: query.NewCompValue(op, vec)}

				count, err := coll.Find(f).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, 0, count, "op=%d path=%s must match no documents", op, path)

				res, err := coll.Find(f).Delete(ctx)
				require.NoError(t, err)
				assert.Equal(t, 0, res.Modified, "op=%d path=%s must delete nothing", op, path)
			}
		}

		remaining, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, len(docs), remaining, "the collection must be intact")
	})

	t.Run("vector-indexed field", func(t *testing.T) {
		ctx := context.Background()
		fx := newFixture(t)
		coll, err := fx.CreateCollection(ctx, "vec")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
			Name:   "v",
			Vector: &anystore.VectorParams{Field: "v", Dim: 3, Metric: anystore.VectorCosine},
		}))

		const n = 20
		for i := 0; i < n; i++ {
			doc := fmt.Sprintf(`{"id":%d,"v":{"$vector":[%d,1,2]}}`, i, i)
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(doc)))
		}

		// The clause as originally filed: a scalar ordering op on a vector field.
		// It used to Count 20 of 20 and Delete the entire collection, err=nil.
		for _, cond := range []string{
			`{"v":{"$gt":1}}`,
			`{"v":{"$gte":1}}`,
			`{"v":{"$lt":1}}`,
			`{"v":{"$lte":1}}`,
		} {
			count, err := coll.Find(cond).Count(ctx)
			require.NoError(t, err, cond)
			assert.Equal(t, 0, count, "cond=%s must match no documents", cond)

			res, err := coll.Find(cond).Delete(ctx)
			require.NoError(t, err, cond)
			assert.Equal(t, 0, res.Modified, "cond=%s must delete nothing", cond)
		}

		remaining, err := coll.Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, n, remaining, "the collection must be intact")
	})
}

// Count is the only verb that does not go through makeQuery, so it used to skip
// the q.err check entirely: a query that failed to parse left q.cond nil, which
// the no-filter fast path read as "count everything". Every other verb errored.
// Found while wiring Rule V's parse rejection, but it is general — any invalid
// operator hit it.
func TestCount_ReportsParseError(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}

	_, err = coll.Find(`{"a":{"$bogusOp":1}}`).Count(ctx)
	assert.Error(t, err, "Count must not swallow a parse error and count the whole collection")

	_, err = coll.Find(`{"a":{"$lt":{"$vector":[0]}}}`).Count(ctx)
	assert.ErrorIs(t, err, query.ErrVectorNotOrderable)

	// a valid no-filter Count still takes the fast path
	n, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

// $type:"vectorF32" is the supported way to select vector-valued documents now
// that ordering ops against a vector are always false.
//
// The axis that matters is the DESCENDING index combined with a sort that
// selects it: a reverse index key is bitwise-inverted, so a vector in one lands
// under the tag ^Type(10) = 0xF5. anyenc used to reject that as an unknown type,
// which made extractDocId hand back the whole key as the docId — the Fetch then
// missed and the row vanished, with err=nil. A forward-only or sort-free test
// passes while every one of those rows is being dropped, so both axes are here.
func TestType_VectorF32_AcrossIndexes(t *testing.T) {
	for _, idx := range []string{"", "v", "-v"} {
		for _, sort := range []string{"", "v", "-v"} {
			name := fmt.Sprintf("index=%q/sort=%q", idx, sort)
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "c")
				require.NoError(t, err)
				if idx != "" {
					require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{idx}}))
				}

				for _, d := range []string{
					`{"id":1,"v":{"$vector":[1,2]}}`,
					`{"id":2,"v":{"$vector":[3,4]}}`,
					`{"id":3,"v":{"$vector":[5,6]}}`,
					`{"id":4,"v":1}`,
					`{"id":5,"v":"s"}`,
					`{"id":6,"v":[1,2]}`,
				} {
					require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
				}

				count, err := coll.Find(`{"v":{"$type":"vectorF32"}}`).Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, 3, count, "Count must find the 3 vector-valued docs")

				q := coll.Find(`{"v":{"$type":"vectorF32"}}`)
				if sort != "" {
					q = q.Sort(sort)
				}
				iter, err := q.Iter(ctx)
				require.NoError(t, err)
				var iterated int
				for iter.Next() {
					iterated++
				}
				require.NoError(t, iter.Close())
				assert.Equal(t, 3, iterated, "Iter must agree with Count")
			})
		}
	}
}

// A vector-valued document must survive a scan of a DESCENDING index even when
// there is no filter at all — the reverse key holds an inverted vector tag, and
// failing to parse it silently dropped the row.
func TestReverseIndex_KeepsVectorValuedDocs(t *testing.T) {
	ctx := context.Background()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"-v"}}))

	for _, d := range []string{
		`{"id":1,"v":{"$vector":[1,2]}}`,
		`{"id":2,"v":{"$vector":[3,4]}}`,
		`{"id":3,"v":1}`,
		`{"id":4,"v":"s"}`,
	} {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(d)))
	}

	iter, err := coll.Find(nil).Sort("-v").Iter(ctx)
	require.NoError(t, err)
	var ids []string
	for iter.Next() {
		d, derr := iter.Doc()
		require.NoError(t, derr)
		ids = append(ids, d.Value().Get("id").String())
	}
	require.NoError(t, iter.Close())
	assert.Len(t, ids, 4, "no document may be dropped by a reverse-index scan, got ids=%v", ids)
}

// Regression tests: tombstoning the HNSW entry
// point (update or delete of that doc) used to leave mt.entryLabel pointing at
// a dead node; with no live neighbour to route through (one-doc collections,
// or repeated entry churn) every subsequent search returned empty forever.
// tombstoneLabel now repoints the entry to a live node (or clears it so the
// next insert re-seeds).

func entryRepointColl(t *testing.T, dim int) (*fixture, anystore.Collection) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	return fx, coll
}

// selfTop1 asserts that searching with id's own stored vector returns id as
// the closest hit.
func selfTop1(t *testing.T, coll anystore.Collection, id int, vec []float32) {
	t.Helper()
	hits, err := vsearch(coll, "v", vec, 5, 0)
	require.NoError(t, err)
	require.NotEmpty(t, hits, "search returned no hits (dead entry point?) for id %d", id)
	assert.Equal(t, idBytesOf(id), hits[0].DocId, "id %d must self-retrieve as top-1", id)
}

func TestVectorEntryRepoint_SingleDocUpsert(t *testing.T) {
	const dim = 16
	vecs := vrand(4, dim, 1)

	t.Run("cross_tx", func(t *testing.T) {
		_, coll := entryRepointColl(t, dim)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(0, vecs[0]))))
		require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(0, vecs[1]))))
		selfTop1(t, coll, 0, vecs[1])
	})

	t.Run("same_tx", func(t *testing.T) {
		fx, coll := entryRepointColl(t, dim)
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(vecDocJSON(0, vecs[0]))))
		require.NoError(t, coll.UpsertOne(tx.Context(), anyenc.MustParseJson(vecDocJSON(0, vecs[2]))))
		require.NoError(t, tx.Commit())
		selfTop1(t, coll, 0, vecs[2])
	})
}

// TestVectorEntryRepoint_MixedTxSequence is the exact 10-op single-tx sequence
// from the original crash-fuzz catch: interleaved insert/upsert/update/delete
// of the same keys, which used to leave LiveCount=4 with every search empty.
func TestVectorEntryRepoint_MixedTxSequence(t *testing.T) {
	const dim = 16
	vecs := vrand(16, dim, 7)
	fx, coll := entryRepointColl(t, dim)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	c := tx.Context()
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(525, vecs[0]))))
	require.NoError(t, coll.UpsertOne(c, anyenc.MustParseJson(vecDocJSON(525, vecs[1]))))
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(1235, vecs[2]))))
	require.NoError(t, coll.UpsertOne(c, anyenc.MustParseJson(vecDocJSON(1235, vecs[3]))))
	require.NoError(t, coll.UpdateOne(c, anyenc.MustParseJson(vecDocJSON(525, vecs[4]))))
	require.NoError(t, coll.DeleteId(c, 525))
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(1696, vecs[5]))))
	require.NoError(t, coll.UpdateOne(c, anyenc.MustParseJson(vecDocJSON(1696, vecs[6]))))
	require.NoError(t, coll.UpsertOne(c, anyenc.MustParseJson(vecDocJSON(1714, vecs[7]))))
	require.NoError(t, coll.Insert(c, anyenc.MustParseJson(vecDocJSON(1258, vecs[8]))))
	require.NoError(t, tx.Commit())

	selfTop1(t, coll, 1235, vecs[3])
	selfTop1(t, coll, 1696, vecs[6])
	selfTop1(t, coll, 1714, vecs[7])
	selfTop1(t, coll, 1258, vecs[8])
	// deleted key never resurfaces
	hits, err := vsearch(coll, "v", vecs[4], 4, 0)
	require.NoError(t, err)
	for _, h := range hits {
		assert.NotEqual(t, idBytesOf(525), h.DocId, "deleted doc resurfaced")
	}
}

// TestVectorEntryRepoint_DeleteEntryChain deletes docs in insertion order (the
// first doc is the initial entry; each deletion may hit the repointed entry
// again), verifying survivors stay searchable down to a single doc, through
// empty, and after re-population.
func TestVectorEntryRepoint_DeleteEntryChain(t *testing.T) {
	const (
		n   = 30
		dim = 16
	)
	vecs := vrand(n+4, dim, 3)
	_, coll := entryRepointColl(t, dim)
	for i := 0; i < n; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vecs[i]))))
	}
	for del := 0; del < n-1; del++ {
		require.NoError(t, coll.DeleteId(ctx, del))
		survivor := del + 1
		selfTop1(t, coll, survivor, vecs[survivor])
	}
	// one live doc left
	selfTop1(t, coll, n-1, vecs[n-1])

	// delete the last doc: searches must return empty, not error
	require.NoError(t, coll.DeleteId(ctx, n-1))
	hits, err := vsearch(coll, "v", vecs[0], 5, 0)
	require.NoError(t, err)
	assert.Empty(t, hits, "no live docs -> no hits")

	// re-populate: the next insert re-seeds the entry
	for i := 0; i < 4; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(1000+i, vecs[n+i]))))
		selfTop1(t, coll, 1000+i, vecs[n+i])
	}
}

// TestVectorMetricModeMatrix pins the accepted metric x mode combinations.
// vivf's distance surface is L2, or cosine via unit-normalization — it has no
// dot-product ranking path — so VectorDot on the IVF modes must be refused at
// validation instead of silently ranking by L2 (wrong order, no error).
func TestVectorMetricModeMatrix(t *testing.T) {
	const dim = 32
	modes := []anystore.VectorMode{anystore.VectorModeBTree, anystore.VectorModeHybrid, anystore.VectorModeBruteForce, anystore.VectorModeIVFPQ, anystore.VectorModeIVFSQ}
	metrics := []anystore.VectorMetric{anystore.VectorCosine, anystore.VectorL2, anystore.VectorDot}
	for _, mode := range modes {
		for _, metric := range metrics {
			t.Run(fmt.Sprintf("mode=%d metric=%d", mode, metric), func(t *testing.T) {
				fx := newFixture(t)
				coll, err := fx.CreateCollection(ctx, "docs")
				require.NoError(t, err)
				// IVF modes train from existing documents at create time.
				rng := rand.New(rand.NewSource(11))
				for i := 0; i < 128; i++ {
					v := make([]float32, dim)
					for d := range v {
						v[d] = float32(rng.NormFloat64())
					}
					require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
				}
				err = coll.CreateIndex(ctx, anystore.IndexInfo{
					Name:   "emb",
					Kind:   anystore.IndexKindVector,
					Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: metric, Mode: mode},
				})
				if metric == anystore.VectorDot && (mode == anystore.VectorModeIVFPQ || mode == anystore.VectorModeIVFSQ) {
					assert.ErrorIs(t, err, anystore.ErrVectorMetricUnsupported)
				} else {
					assert.NoError(t, err)
				}
			})
		}
	}
}

// TestVectorDot_RankingMatchesBruteOracle proves VectorDot actually ranks by
// inner product where it is accepted: brute-force mode scans exactly, so the
// result order must equal the descending-dot oracle.
func TestVectorDot_RankingMatchesBruteOracle(t *testing.T) {
	const (
		n   = 300
		dim = 8
		k   = 5
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(41))
	vecs := make([][]float32, n)
	for i := range vecs {
		v := make([]float32, dim)
		for d := range v {
			v[d] = float32(rng.NormFloat64())
		}
		vecs[i] = v
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
	}
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorDot, Mode: anystore.VectorModeBruteForce},
	}))

	q := make([]float32, dim)
	for d := range q {
		q[d] = float32(rng.NormFloat64())
	}
	dot := func(a, b []float32) float32 {
		var s float32
		for i := range a {
			s += a[i] * b[i]
		}
		return s
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		da, db := dot(q, vecs[a]), dot(q, vecs[b])
		switch {
		case da > db:
			return -1
		case da < db:
			return 1
		}
		return 0
	})

	hits, err := vsearch(coll, "v", q, k, 0)
	require.NoError(t, err)
	require.Len(t, hits, k)
	for i, h := range hits {
		assert.Equal(t, idBytesOf(order[i]), h.DocId, "rank %d", i)
	}
}

func vstat(t *testing.T, coll anystore.Collection, name string) anystore.VectorIndexStats {
	t.Helper()
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	for _, vs := range st.VectorIndexes {
		if vs.Name == name {
			return vs
		}
	}
	t.Fatalf("vector index %q not present in stats", name)
	return anystore.VectorIndexStats{}
}

// TestVectorIndex_CompactManual churns an index with deletes and replaces, then
// compacts and asserts the tombstones are reclaimed, the graph is dense, and
// search still serves the live set correctly.
func TestVectorIndex_CompactManual(t *testing.T) {
	const (
		n   = 600
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))

	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// Churn: delete every 3rd, replace every 3rd (i%3==1) onto a fresh vector.
	live := make(map[int][]float32, n)
	for i := range vecs {
		live[i] = vecs[i]
	}
	repl := vrand(n, dim, 71)
	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			require.NoError(t, coll.DeleteId(ctx, i))
			delete(live, i)
		case 1:
			require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(i, repl[i]))))
			live[i] = repl[i]
		}
	}

	pre := vstat(t, coll, "emb")
	require.Greater(t, pre.DeletedCount, 0, "expected tombstones before compaction")
	require.Greater(t, pre.NodeCount, pre.LiveCount)
	require.Equal(t, len(live), pre.LiveCount)

	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))

	post := vstat(t, coll, "emb")
	assert.Equal(t, 0, post.DeletedCount, "no tombstones after compaction")
	assert.Equal(t, len(live), post.LiveCount, "live count must survive compaction")
	assert.Equal(t, len(live), post.NodeCount, "label space must be dense after compaction")
	assert.Less(t, post.SizeBytes, pre.SizeBytes, "compaction should reclaim storage")

	// Self-retrieval: each survivor's own vector retrieves itself.
	for i, v := range live {
		hits, herr := vsearch(coll, "v", v, 1, 64)
		require.NoError(t, herr)
		require.Len(t, hits, 1)
		assert.Equal(t, idBytesOf(i), hits[0].DocId, "self-retrieval mismatch for doc %d", i)
	}
	// Deleted docs never reappear.
	for i := 0; i < n; i++ {
		if _, ok := live[i]; ok {
			continue
		}
		hits, herr := vsearch(coll, "v", vecs[i], 5, 64)
		require.NoError(t, herr)
		for _, h := range hits {
			assert.NotEqual(t, idBytesOf(i), h.DocId, "deleted doc %d leaked", i)
		}
	}
}

// TestVectorIndex_CompactReopen verifies a compacted index persists correctly:
// a fresh handle reopening the file reads the rebuilt graph (no tombstones) and
// serves search correctly.
func TestVectorIndex_CompactReopen(t *testing.T) {
	const (
		n   = 400
		dim = 16
	)
	tmpDir := t.TempDir()
	fx := newFixturePath(t, tmpDir)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < n/2; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}
	require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))
	require.NoError(t, fx.Close())

	db2, err := anystore.Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	coll2, err := db2.Collection(ctx, "docs")
	require.NoError(t, err)

	st := vstat(t, coll2, "emb")
	assert.Equal(t, 0, st.DeletedCount, "reopened compacted index has no tombstones")
	assert.Equal(t, n/2, st.LiveCount)
	assert.Equal(t, n/2, st.NodeCount)

	for i := n / 2; i < n; i++ {
		hits, herr := vsearch(coll2, "v", vecs[i], 1, 64)
		require.NoError(t, herr)
		require.Len(t, hits, 1)
		require.Equal(t, idBytesOf(i), hits[0].DocId, "survivor %d after reopen", i)
	}
	for i := 0; i < n/2; i++ {
		hits, herr := vsearch(coll2, "v", vecs[i], 3, 64)
		require.NoError(t, herr)
		for _, h := range hits {
			require.NotEqual(t, idBytesOf(i), h.DocId, "deleted doc %d survived reopen", i)
		}
	}
}

// TestVectorIndex_CompactAuto verifies CompactRatio fires a synchronous rebuild
// once tombstones reach the ratio, off the self-contained write that crossed it.
func TestVectorIndex_CompactAuto(t *testing.T) {
	const (
		n   = 400
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64, CompactRatio: 0.5},
	}))

	vecs := vrand(n, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	peakDeleted := 0
	for i := 0; i < n/2; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
		if d := vstat(t, coll, "emb").DeletedCount; d > peakDeleted {
			peakDeleted = d
		}
	}
	final := vstat(t, coll, "emb")
	t.Logf("auto-compact: live=%d deleted=%d node=%d (peak deleted=%d)",
		final.LiveCount, final.DeletedCount, final.NodeCount, peakDeleted)

	require.GreaterOrEqual(t, peakDeleted, 100, "threshold should have been approached")
	assert.Less(t, final.DeletedCount, peakDeleted, "auto-compaction should have reset tombstones")
	assert.Less(t, final.NodeCount, n, "node space should be densified below the original allocation")
	assert.Equal(t, final.LiveCount, n/2)

	// Search still correct after the in-flight rebuilds.
	hits, err := vsearch(coll, "v", vecs[n-1], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(n-1), hits[0].DocId)
}

// TestVectorIndex_CompactAuto_BulkDelete verifies CompactRatio also fires after a
// query-based bulk delete (coll.Find(...).Delete()), not just the single-doc
// mutators — the tombstones a bulk delete creates are reclaimed once it commits.
func TestVectorIndex_CompactAuto_BulkDelete(t *testing.T) {
	const (
		n   = 400
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64, CompactRatio: 0.5},
	}))
	vecs := vrand(n, dim, 13)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// One bulk delete of half the corpus — well past the 0.5 ratio — in a single
	// self-contained write. Auto-compaction must reclaim the tombstones it creates.
	res, err := coll.Find(`{"id":{"$lt":200}}`).Delete(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 200, res.Modified)

	final := vstat(t, coll, "emb")
	t.Logf("bulk-delete auto-compact: live=%d deleted=%d node=%d",
		final.LiveCount, final.DeletedCount, final.NodeCount)
	assert.Equal(t, 200, final.LiveCount)
	assert.Equal(t, 0, final.DeletedCount, "bulk delete should have triggered auto-compaction")
	assert.LessOrEqual(t, final.NodeCount, 200, "node space should be densified to the live set")

	// Search still serves the surviving live set.
	hits, err := vsearch(coll, "v", vecs[n-1], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(n-1), hits[0].DocId)
}

// TestVectorIndex_CompactAuto_DeferredInUserTx verifies auto-compaction does NOT
// run inside a caller-managed transaction (it needs its own committed tx), but
// does run on the next self-contained write.
func TestVectorIndex_CompactAuto_DeferredInUserTx(t *testing.T) {
	const (
		n   = 200
		dim = 16
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64, CompactRatio: 0.5},
	}))
	vecs := vrand(n, dim, 9)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// Delete 150/200 inside ONE user-managed tx — well past the 0.5 ratio.
	tx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	for i := 0; i < 150; i++ {
		require.NoError(t, coll.DeleteId(tx.Context(), i))
	}
	require.NoError(t, tx.Commit())

	inTx := vstat(t, coll, "emb")
	require.Greater(t, inTx.DeletedCount, 100,
		"auto-compaction must be deferred inside a user tx (tombstones should remain)")

	// A self-contained write now triggers the deferred compaction.
	require.NoError(t, coll.DeleteId(ctx, 150))
	after := vstat(t, coll, "emb")
	assert.Less(t, after.DeletedCount, inTx.DeletedCount, "self-contained write should compact")
	assert.Less(t, after.NodeCount, n)
}

// A compaction inside a caller-managed tx that rolls back must restore the
// pre-compaction handle: the rollback reverts the namespace recreation and
// frees the compacted roots, so a published compacted handle would fail every
// subsequent vector op with "btree: key not found" until reopen.
func TestVectorIndex_CompactAmbientRollback_RestoresHandle(t *testing.T) {
	const (
		n   = 200
		dim = 8
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < n; i += 3 {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))
	require.NoError(t, tx.Rollback())

	// The restored handle must serve both sides of the index.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(n+1, vecs[1]))))
	hits, err := vsearch(coll, "v", vecs[1], 2, 64)
	require.NoError(t, err)
	require.Len(t, hits, 2)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// During an uncommitted ambient-tx compaction the compacted roots exist only
// in the writer's view; a concurrent reader must keep searching through the
// replaced handle (still valid in its committed snapshot) instead of failing
// on the recreated namespaces. After commit the compacted handle serves all.
func TestVectorIndex_CompactAmbientCommit_ConcurrentReader(t *testing.T) {
	const (
		n   = 200
		dim = 8
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < n; i += 3 {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))

	// Window: concurrent reader searches through the pre-compaction handle.
	hits, err := vsearch(coll, "v", vecs[1], 2, 64)
	require.NoError(t, err)
	require.Len(t, hits, 2)
	assert.Equal(t, idBytesOf(1), hits[0].DocId)

	// Explain must agree with execution: the reader is index-served (via the
	// pre-compaction handle), so the index is listed.
	explain, err := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[1], 2, 64))).Explain(ctx)
	require.NoError(t, err)
	listed := false
	for _, ie := range explain.Indexes {
		if ie.Name == "emb" {
			listed = true
		}
	}
	assert.True(t, listed, "Explain must list the index execution serves via the pre-compaction handle")

	// The compacting tx itself searches the compacted handle.
	fq := coll.Find(fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[1], 2, 64)))
	iterTx, err := fq.Iter(tx.Context())
	require.NoError(t, err)
	var gotTx int
	for iterTx.Next() {
		gotTx++
	}
	require.NoError(t, iterTx.Err())
	require.NoError(t, iterTx.Close())
	assert.Equal(t, 2, gotTx)

	require.NoError(t, tx.Commit())

	hits, err = vsearch(coll, "v", vecs[1], 2, 64)
	require.NoError(t, err)
	require.Len(t, hits, 2)
	assert.Equal(t, idBytesOf(1), hits[0].DocId)
	require.NoError(t, fx.IntegrityCheck(ctx))
}

// Chained same-tx DDL: CreateIndex then CompactVectorIndex in one ambient tx.
// Requires writable-aware namespace resolution (the compact re-opens the
// index it created earlier in this same tx). A concurrent reader during the
// window errors exactly as before the DDL began (nothing committed to serve);
// after commit the index answers.
func TestVectorIndex_CreateThenCompactSameTx(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(30, dim, 3)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))

	// The chain's committed tail is nil (created in this tx): a concurrent
	// reader errors exactly as before the DDL began.
	_, rerr := vsearch(coll, "v", vecs[0], 3, 64)
	assert.ErrorIs(t, rerr, anystore.ErrIndexNotFound)

	require.NoError(t, tx.Commit())

	hits, err := vsearch(coll, "v", vecs[0], 3, 64)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}

// Chained same-tx DDL: CompactVectorIndex twice in one ambient tx. The second
// compact re-opens namespaces the first one re-created (same-tx). A
// concurrent reader during the whole window is served the ORIGINAL committed
// handle through the prevTarget committed-tail chain and must see results
// identical to the pre-tx state.
func TestVectorIndex_CompactTwiceSameTx(t *testing.T) {
	const dim = 8
	const n = 40
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	vecs := vrand(n, dim, 7)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.DeleteId(ctx, i))
	}
	before, err := vsearch(coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	require.Len(t, before, 5)

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))
	// New tombstones inside the tx so the second compact has work.
	for i := 10; i < 15; i++ {
		require.NoError(t, coll.DeleteId(tx.Context(), i))
	}
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))

	// Concurrent reader mid-window: the committed-tail chain serves the
	// original handle — identical results to the pre-tx state.
	during, err := vsearch(coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	assert.Equal(t, before, during,
		"a concurrent reader across chained compacts must see the committed state")

	require.NoError(t, tx.Commit())

	hits, err := vsearch(coll, "v", vecs[n-1], 5, 64)
	require.NoError(t, err)
	assert.Len(t, hits, 5)
}

// The IVF flavor of chained same-tx DDL (vivf.Rebuild re-opens through the
// same writable-aware resolution).
func TestVectorIndex_CreateThenCompactSameTxIVF(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	vecs := vrand(64, dim, 11)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(tx.Context(), anystore.IndexInfo{
		Name: "emb",
		Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{
			Field: "v", Dim: dim, Metric: anystore.VectorL2, Mode: anystore.VectorModeIVFSQ, NProbe: 32,
		},
	}))
	require.NoError(t, coll.CompactVectorIndex(tx.Context(), "emb"))
	require.NoError(t, tx.Commit())

	hits, err := vsearch(coll, "v", vecs[0], 3, 0)
	require.NoError(t, err)
	assert.Len(t, hits, 3)
}

// knnDetectColl builds a 3-dim vector-indexed collection ("v", index "emb")
// with a handful of docs, for exercising detectKnnQuery through the verbs.
func knnDetectColl(t *testing.T) anystore.Collection {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "knn_detect")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: 3, Metric: anystore.VectorL2, EfSearch: 64},
	}))
	for i := 0; i < 10; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"v":[%d,1,2],"a":%d,"b":%d}`, i, i, i%2, i%3))))
	}
	return coll
}

const kd = `{"$knn":{"$query":[3,1,2],"$k":4}}`

// TestDetectKnn_Placement pins the detection rows of the $knn design (T2.4):
// legal placements search; illegal ones hard-error with the named sentinel, on
// every verb (Iter and Count exercised here; the verb-agreement property is
// TestVectorClauseInvalid_WriteVerbsAgreeWithIter).
func TestDetectKnn_Placement(t *testing.T) {
	coll := knnDetectColl(t)

	legal := []struct {
		name string
		cond string
	}{
		{"plain", fmt.Sprintf(`{"v":%s}`, kd)},
		{"flat residual", fmt.Sprintf(`{"v":%s,"a":1}`, kd)},
		{"$and single", fmt.Sprintf(`{"$and":[{"v":%s}]}`, kd)},
		{"$and multi (*And)", fmt.Sprintf(`{"$and":[{"v":%s},{"a":1}]}`, kd)},
		{"nested *And (the residual-leak shape)", fmt.Sprintf(`{"$and":[{"$and":[{"v":%s},{"a":1}]},{"b":2}]}`, kd)},
	}
	for _, tc := range legal {
		t.Run("legal/"+tc.name, func(t *testing.T) {
			ids := writeOrderIterIds(t, coll.Find(tc.cond))
			assert.NotEmpty(t, ids, "a legal $knn placement must search, not silently return 0 rows")
			n, err := coll.Find(tc.cond).Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, len(ids), n)
			assert.LessOrEqual(t, n, 4, "the k-cut bounds the denoted set")
		})
	}

	illegal := []struct {
		name   string
		cond   string
		wantIs error
	}{
		{"$or", fmt.Sprintf(`{"$or":[{"v":%s},{"a":1}]}`, kd), anystore.ErrKnnBadPlacement},
		{"$nor", fmt.Sprintf(`{"$nor":[{"v":%s}]}`, kd), anystore.ErrKnnBadPlacement},
		{"$or nested under $and", fmt.Sprintf(`{"$and":[{"$or":[{"v":%s},{"a":1}]},{"b":2}]}`, kd), anystore.ErrKnnBadPlacement},
		{"two $knn", fmt.Sprintf(`{"$and":[{"v":%s},{"v":%s}]}`, kd, kd), anystore.ErrMultipleVectorClauses},
		{"$knn with $text", fmt.Sprintf(`{"v":%s,"$text":{"$search":"x"}}`, kd), anystore.ErrKnnWithText},
		{"unindexed field", fmt.Sprintf(`{"w":%s}`, kd), anystore.ErrNoVectorIndex},
		{"$index names no index", `{"v":{"$knn":{"$query":[3,1,2],"$k":4,"$index":"nope"}}}`, anystore.ErrNoVectorIndex},
		{"wrong dim", `{"v":{"$knn":{"$query":[3,1],"$k":4}}}`, anystore.ErrInvalidVectorQuery},
		{"legacy bare array", `{"v":[3,1,2]}`, anystore.ErrLegacyVectorClause},
		{"legacy under $not", `{"v":{"$not":{"$eq":[3,1,2]}}}`, anystore.ErrLegacyVectorClause},
		{"_distance without $knn", `{"_distance":{"$lt":1.0},"a":1}`, anystore.ErrDistanceWithoutVector},
		{"_distance inside $or without $knn", `{"$or":[{"_distance":{"$lt":1.0}},{"a":1}]}`, anystore.ErrDistanceWithoutVector},
		{"_distance inside $not without $knn", `{"a":{"$gt":0},"$nor":[{"_distance":{"$lt":1.0}}]}`, anystore.ErrDistanceWithoutVector},
	}
	for _, tc := range illegal {
		t.Run("illegal/"+tc.name, func(t *testing.T) {
			_, err := coll.Find(tc.cond).Iter(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Iter")
			_, err = coll.Find(tc.cond).Count(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Count")
			_, err = coll.Find(tc.cond).Delete(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Delete")
			_, err = coll.Find(tc.cond).Explain(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Explain")
		})
	}

	// An invalid source errors even when an unrelated clause makes the filter
	// unsatisfiable — validation precedes the unsatisfiable() short-circuit.
	t.Run("validation precedes unsatisfiable", func(t *testing.T) {
		cond := `{"v":{"$knn":{"$query":[3,1],"$k":4}},"a":{"$in":[]}}`
		_, err := coll.Find(cond).Iter(ctx)
		require.ErrorIs(t, err, anystore.ErrInvalidVectorQuery, "Iter")
		_, err = coll.Find(cond).Count(ctx)
		require.ErrorIs(t, err, anystore.ErrInvalidVectorQuery, "Count")
		_, err = coll.Find(cond).Delete(ctx)
		require.ErrorIs(t, err, anystore.ErrInvalidVectorQuery, "Delete")
		_, err = coll.Find(cond).Explain(ctx)
		require.ErrorIs(t, err, anystore.ErrInvalidVectorQuery, "Explain")
	})
}

// TestDetectKnn_HandBuilt pins the programmatic path: a pre-built query.Filter
// bypasses the parser entirely (ParseCondition short-circuits), so detection is
// the only validation it ever sees — every parse rule must hold there too.
func TestDetectKnn_HandBuilt(t *testing.T) {
	coll := knnDetectColl(t)
	knn := func() query.Filter {
		return query.Key{Path: []string{"v"}, Filter: query.NewKnn([]float32{3, 1, 2}, 4)}
	}

	t.Run("legal Key{v,Knn}", func(t *testing.T) {
		ids := writeOrderIterIds(t, coll.Find(knn()))
		assert.NotEmpty(t, ids)
	})
	t.Run("legal And{Key{v,Knn},Key{v,Comp}} — strip by node, not path", func(t *testing.T) {
		// Two Keys on ONE path: the $ne residual must survive the Knn strip.
		// A path-keyed strip would drop it and widen the deleted set.
		f := query.And{
			knn(),
			query.Key{Path: []string{"v"}, Filter: query.NewComp(query.CompOpNe, "nothing-stored-matches")},
		}
		ids := writeOrderIterIds(t, coll.Find(f))
		assert.NotEmpty(t, ids, "the $ne residual matches every doc; the knn still selects k")
		f2 := query.And{
			knn(),
			// $eq "x" on v matches nothing → residual excludes everything.
			query.Key{Path: []string{"v"}, Filter: query.NewComp(query.CompOpEq, "x")},
		}
		n, err := coll.Find(f2).Count(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "the co-path residual must be preserved and applied")
	})

	for _, tc := range []struct {
		name   string
		f      query.Filter
		wantIs error
	}{
		{"bare Knn (no Key)", query.NewKnn([]float32{3, 1, 2}, 4), anystore.ErrKnnBadPlacement},
		{"Not{Key{v,Knn}}", query.Not{Filter: knn()}, anystore.ErrKnnBadPlacement},
		{"Key{v,Not{Knn}}", query.Key{Path: []string{"v"}, Filter: query.Not{Filter: query.NewKnn([]float32{3, 1, 2}, 4)}}, anystore.ErrKnnBadPlacement},
		{"Or{Key{v,Knn},…}", query.Or{knn(), query.MustParseCondition(`{"a":1}`)}, anystore.ErrKnnBadPlacement},
		{"k=0", query.Key{Path: []string{"v"}, Filter: query.Knn{Query: []float32{3, 1, 2}}}, anystore.ErrInvalidVectorQuery},
		{"empty query", query.Key{Path: []string{"v"}, Filter: query.Knn{K: 3}}, anystore.ErrInvalidVectorQuery},
		{"ef below k", query.Key{Path: []string{"v"}, Filter: query.NewKnn([]float32{3, 1, 2}, 4, query.KnnEf(2))}, anystore.ErrInvalidVectorQuery},
		{"legacy programmatic (the downstream indexer shape)", legacyEqFilter([]float32{3, 1, 2}), anystore.ErrLegacyVectorClause},
	} {
		t.Run("illegal/"+tc.name, func(t *testing.T) {
			_, err := coll.Find(tc.f).Iter(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Iter")
			_, err = coll.Find(tc.f).Count(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Count")
			_, err = coll.Find(tc.f).Delete(ctx)
			require.ErrorIs(t, err, tc.wantIs, "Delete")
		})
	}

	t.Run("non-ANN predicates on the vector field stay ordinary on every verb", func(t *testing.T) {
		// The downstream EnsureVectorIndex pattern: Count with {v:{$exists}}.
		n, err := coll.Find(query.Key{Path: []string{"v"}, Filter: query.Exists{}}).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, n)
	})
}

// legacyEqFilter reproduces the pre-$knn programmatic ANN construction
// (query.NewCompValue(CompOpEq, <plain array>)) that must now hard-error.
func legacyEqFilter(qv []float32) query.Filter {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	for i, f := range qv {
		arr.SetArrayItem(i, a.NewNumberFloat64(float64(f)))
	}
	return query.Key{Path: []string{"v"}, Filter: query.NewCompValue(query.CompOpEq, arr)}
}

// TestDetectKnn_AmbiguousIndex: two vector indexes on one field require $index;
// either name resolves; a $knn without it errors rather than searching
// whichever index loaded first.
func TestDetectKnn_AmbiguousIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "knn_ambig")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb_l2", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: 3, Metric: anystore.VectorL2},
	}))
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb_cos", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: 3, Metric: anystore.VectorCosine},
	}))
	for i := 0; i < 6; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":[%d,1,2]}`, i, i))))
	}

	_, err = coll.Find(fmt.Sprintf(`{"v":%s}`, kd)).Iter(ctx)
	require.ErrorIs(t, err, anystore.ErrAmbiguousVectorIndex)
	_, err = coll.Find(fmt.Sprintf(`{"v":%s}`, kd)).Count(ctx)
	require.ErrorIs(t, err, anystore.ErrAmbiguousVectorIndex)

	for _, name := range []string{"emb_l2", "emb_cos"} {
		cond := fmt.Sprintf(`{"v":{"$knn":{"$query":[3,1,2],"$k":2,"$index":%q}}}`, name)
		ids := writeOrderIterIds(t, coll.Find(cond))
		assert.NotEmpty(t, ids, "$index %s must resolve", name)
		exp, eerr := coll.Find(cond).Explain(ctx)
		require.NoError(t, eerr)
		var used []string
		for _, ix := range exp.Indexes {
			if ix.Used {
				used = append(used, ix.Name)
			}
		}
		assert.Equal(t, []string{name}, used, "Explain must report the resolved index as used")
	}
}

// TestDetectKnn_ExplainReportsKnn: Explain names the ANN plan and the driving
// vector index — Explain printing FullScan for a working ANN query is why the
// verb divergence survived as long as it did.
func TestDetectKnn_ExplainReportsKnn(t *testing.T) {
	coll := knnDetectColl(t)
	exp, err := coll.Find(fmt.Sprintf(`{"v":%s,"a":1}`, kd)).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, exp.Sql, "KnnSearch(k=4,ef=", "the plan names the ANN source: %s", exp.Sql)
	assert.Contains(t, exp.Sql, "Filter", "the residual filter appears in the plan: %s", exp.Sql)
	var used []string
	for _, ix := range exp.Indexes {
		if ix.Used {
			used = append(used, ix.Name)
		}
	}
	assert.Equal(t, []string{"emb"}, used)
}

// TestDetectKnn_PointerBuiltFilters pins the pointer-form walk: every
// composite filter has value-receiver methods, so &Not{…}/&Key{…}/&Or{…}
// satisfy query.Filter too, and a value-only type switch would skip them —
// letting &Not{Key{v,Knn}} reflect fail-closed Ok into a full-collection
// Delete, and &Key{v,Knn} silently return 0 rows instead of searching.
func TestDetectKnn_PointerBuiltFilters(t *testing.T) {
	coll := knnDetectColl(t)
	knnLeaf := func() query.Knn { return query.NewKnn([]float32{3, 1, 2}, 4) }

	t.Run("legal pointer forms search like their value forms", func(t *testing.T) {
		for name, f := range map[string]query.Filter{
			"&Key{v,Knn}": &query.Key{Path: []string{"v"}, Filter: knnLeaf()},
			"Key{v,&Knn}": query.Key{Path: []string{"v"}, Filter: ptrKnn(knnLeaf())},
			"&And{Key{v,Knn},residual}": func() query.Filter {
				a := query.And{query.Key{Path: []string{"v"}, Filter: knnLeaf()}, query.MustParseCondition(`{"a":1}`)}
				return &a
			}(),
		} {
			ids := writeOrderIterIds(t, coll.Find(f))
			require.NotEmpty(t, ids, "%s must search, not silently return 0 rows", name)
			n, err := coll.Find(f).Count(ctx)
			require.NoError(t, err, name)
			assert.Equal(t, len(ids), n, name)
		}
	})

	t.Run("illegal pointer forms error instead of bypassing detection", func(t *testing.T) {
		for name, f := range map[string]query.Filter{
			"&Not{Key{v,Knn}}": &query.Not{Filter: query.Key{Path: []string{"v"}, Filter: knnLeaf()}},
			"&Or{Key{v,Knn},…}": func() query.Filter {
				o := query.Or{query.Key{Path: []string{"v"}, Filter: knnLeaf()}, query.MustParseCondition(`{"a":1}`)}
				return &o
			}(),
			"&Nor{Key{v,Knn}}": func() query.Filter {
				n := query.Nor{query.Key{Path: []string{"v"}, Filter: knnLeaf()}}
				return &n
			}(),
			"&Knn bare": ptrKnn(knnLeaf()),
		} {
			_, err := coll.Find(f).Iter(ctx)
			require.ErrorIs(t, err, anystore.ErrKnnBadPlacement, "%s: Iter", name)
			res, err := coll.Find(f).Delete(ctx)
			require.ErrorIs(t, err, anystore.ErrKnnBadPlacement, "%s: Delete", name)
			require.Zero(t, res.Modified, name)
			remaining, cerr := coll.Find(nil).Count(ctx)
			require.NoError(t, cerr)
			require.Equal(t, 10, remaining, "%s: collection must be intact — this exact shape deleted everything before the pointer-walk fix", name)
		}
	})
}

func ptrKnn(k query.Knn) *query.Knn { return &k }

// neArrComp builds Comp{Ne, <plain array>} — the programmatic twin of the JSON
// {"$ne":[...]} spelling.
func neArrComp(qv []float32) *query.Comp {
	a := &anyenc.Arena{}
	arr := a.NewArray()
	for i, f := range qv {
		arr.SetArrayItem(i, a.NewNumberFloat64(float64(f)))
	}
	return query.NewCompValue(query.CompOpNe, arr)
}

// TestDetectKnn_NeDimArrayIsLegacy: $ne with a dim-sized plain array on a
// vector-indexed field is the match-all mirror of the legacy $eq spelling —
// on packed storage the operand never byte-equals any stored value, so the
// $ne would silently select EVERY document ("delete all but this vector"
// removes that vector too). Loud error, both spellings.
func TestDetectKnn_NeDimArrayIsLegacy(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "knn_ne")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: 3, Metric: anystore.VectorL2},
	}))
	// Packed storage — the encoding for which the $ne is a silent match-all.
	a := &anyenc.Arena{}
	for i := 0; i < 8; i++ {
		obj := a.NewObject()
		obj.Set("id", a.NewNumberInt(i))
		obj.Set("v", a.NewVectorF32([]float32{float32(i), 1, 2}))
		require.NoError(t, coll.Insert(ctx, obj))
		a.Reset()
	}

	for name, f := range map[string]any{
		"json $ne":         `{"v":{"$ne":[3,1,2]}}`,
		"programmatic $ne": query.Key{Path: []string{"v"}, Filter: neArrComp([]float32{3, 1, 2})},
	} {
		_, err = coll.Find(f).Iter(ctx)
		require.ErrorIs(t, err, anystore.ErrLegacyVectorClause, "%s: Iter", name)
		res, derr := coll.Find(f).Delete(ctx)
		require.ErrorIs(t, derr, anystore.ErrLegacyVectorClause, "%s: Delete", name)
		require.Zero(t, res.Modified, name)
	}
	remaining, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 8, remaining, "the $ne must never have executed as a match-all literal")

	// Wrong-dim $ne stays an ordinary filter (not ANN-shaped).
	n, err := coll.Find(`{"v":{"$ne":[1,2]}}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 8, n, "non-dim-sized $ne is an ordinary literal filter")
}

// TestDetectKnn_NestedKeyDistancePath: a nested-Key filter on a real stored
// sub-field literally named _distance (a._distance) is NOT the synthetic
// top-level _distance — the reference walk descends with the path suffix.
func TestDetectKnn_NestedKeyDistancePath(t *testing.T) {
	coll := knnDetectColl(t)
	f := query.Key{Path: []string{"a"}, Filter: query.Key{Path: []string{"_distance"}, Filter: query.NewComp(query.CompOpGt, 1)}}
	n, err := coll.Find(f).Count(ctx)
	require.NoError(t, err, "a._distance is a stored sub-field, not the synthetic _distance")
	assert.Zero(t, n)
	// The synthetic reference itself still errors, wherever it hides.
	_, err = coll.Find(query.Not{Filter: query.Key{Path: []string{"_distance"}, Filter: query.NewComp(query.CompOpLt, 1)}}).Count(ctx)
	require.ErrorIs(t, err, anystore.ErrDistanceWithoutVector)
}

// TestKnn_SortedResultsKeepDistance: SortIter clears the in-flight decorated
// doc, so the public iterator re-fetches — the _distance field must be
// re-injected from the sidecar, as docs/vector-search.md promises.
func TestKnn_SortedResultsKeepDistance(t *testing.T) {
	coll := knnDetectColl(t)
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s}`, kd)).Sort("-_distance").Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	rows := 0
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		dv := doc.Value().Get("_distance")
		require.NotNil(t, dv, "sorted $knn result lost the documented _distance field")
		assert.InDelta(t, float64(iter.Distance()), dv.GetFloat64(), 1e-6)
		rows++
	}
	require.NoError(t, iter.Err())
	require.NotZero(t, rows)
}

// TestExplain_UnconsideredSourceIndexesCarryNoCost: vector/fts indexes are
// listed for visibility, but only the driving index carries the plan's cost —
// an unconsidered index with a phantom cost would read as a CBO candidate.
func TestExplain_UnconsideredSourceIndexesCarryNoCost(t *testing.T) {
	coll := knnDetectColl(t)
	exp, err := coll.Find(`{"a":1}`).Explain(ctx)
	require.NoError(t, err)
	var sawVector bool
	for _, ix := range exp.Indexes {
		if ix.Name == "emb" {
			sawVector = true
			assert.False(t, ix.Used)
			assert.Zero(t, ix.Cost, "the CBO never costed the vector index for a plain range query")
		}
	}
	assert.True(t, sawVector, "the vector index must still be listed")
}

// opaqueWrapKnn is a CUSTOM Filter implementation wrapping a $knn clause — a
// foreign type the detection walk cannot see through, by construction (a
// walker only knows the library's own node types; arbitrary user code is not
// introspectable).
type opaqueWrapKnn struct{ inner query.Filter }

func (w opaqueWrapKnn) Ok(v *anyenc.Value, b *syncpool.DocBuffer) bool { return w.inner.Ok(v, b) }
func (w opaqueWrapKnn) IndexBounds(_ string, bs query.Bounds) query.Bounds {
	return bs
}
func (w opaqueWrapKnn) String() string { return "opaque" }

// TestKnn_InsideCustomFilterFailsClosed pins the failure DIRECTIONS for a Knn
// smuggled inside a custom Filter implementation, where detection is
// structurally blind (see docs/query-filter-contract.md rule 5: custom filters
// must not embed source filters).
//
//   - A pass-through wrapper inherits fail-closed Ok: the query silently
//     matches NOTHING — zero rows, no-op writes, err == nil. Annoying but
//     safe; this is precisely why Knn.Ok returns false and Text.Ok's true is
//     the bug (a fail-open source filter here would over-delete instead).
//   - A NEGATING wrapper reflects fail-closed into match-all, exactly like
//     query.Not would — but unlike &Not{…} (the library's own type, walked
//     since the pointer-form fix) this is the user's own matching code, which
//     could just as easily `return true` outright: wrapping a Knn grants no
//     destructive power that writing a custom filter didn't already grant.
//
// If an unwrap protocol (e.g. a Children() []Filter interface the walkers
// descend) ever lands, update this test deliberately — until then these are
// the documented consequences, not defects.
func TestKnn_InsideCustomFilterFailsClosed(t *testing.T) {
	coll := knnDetectColl(t)
	kn := query.Key{Path: []string{"v"}, Filter: query.NewKnn([]float32{3, 1, 2}, 4)}

	res, err := coll.Find(opaqueWrapKnn{kn}).Delete(ctx)
	require.NoError(t, err)
	assert.Zero(t, res.Modified, "pass-through wrapper: fail-closed Knn.Ok matches nothing")
	ids := writeOrderIterIds(t, coll.Find(opaqueWrapKnn{kn}))
	assert.Empty(t, ids, "pass-through wrapper: Iter agrees (0 rows, err == nil)")
	n, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, n, "collection intact — the safe failure direction")
}

// makeVectorIndex creates a vector index of the given mode on field "v".
func makeVectorIndex(t *testing.T, coll anystore.Collection, mode anystore.VectorMode, dim int) {
	t.Helper()
	require.NoError(t, coll.CreateIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2, EfSearch: 64, Mode: mode},
	}))
}

// vectorKnnFilter builds the programmatic $knn filter (query.NewKnn) used to
// issue a vector query through the normal Find pipeline — the same construction
// the downstream indexer uses (no JSON involved).
func vectorKnnFilter(qv []float32, k int) query.Filter {
	return query.Key{Path: []string{"v"}, Filter: query.NewKnn(qv, k)}
}

// TestVectorMode_Persist verifies every mode round-trips through reopen and is
// reported by Stats.
func TestVectorMode_Persist(t *testing.T) {
	const dim = 8
	tmpDir := t.TempDir()
	cases := []struct {
		coll string
		mode anystore.VectorMode
		want string
	}{
		{"c_btree", anystore.VectorModeBTree, "btree"},
		{"c_hybrid", anystore.VectorModeHybrid, "hybrid"},
		{"c_brute", anystore.VectorModeBruteForce, "brute"},
	}

	fx := newFixturePath(t, tmpDir)
	for _, tc := range cases {
		coll, err := fx.CreateCollection(ctx, tc.coll)
		require.NoError(t, err)
		makeVectorIndex(t, coll, tc.mode, dim)
	}
	require.NoError(t, fx.Close())

	db2, err := anystore.Open(ctx, filepath.Join(tmpDir, "any-store-test.db"), nil)
	require.NoError(t, err)
	defer db2.Close()
	for _, tc := range cases {
		coll, err := db2.Collection(ctx, tc.coll)
		require.NoError(t, err)
		st, err := coll.Stats(ctx)
		require.NoError(t, err)
		require.Len(t, st.VectorIndexes, 1, tc.coll)
		assert.Equal(t, tc.want, st.VectorIndexes[0].Mode, tc.coll)
	}
}

// TestVectorMode_BruteNoStorage verifies brute-force keeps no index data: zero
// index bytes even after inserting documents, and writes never error.
func TestVectorMode_BruteNoStorage(t *testing.T) {
	const (
		dim = 8
		n   = 200
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	makeVectorIndex(t, coll, anystore.VectorModeBruteForce, dim)

	vecs := vrand(n, dim, 3)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}
	// update + delete are no-ops on the index, must not error.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(vecDocJSON(0, vecs[1]))))
	require.NoError(t, coll.DeleteId(ctx, 1))

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.VectorIndexes, 1)
	assert.Equal(t, "brute", st.VectorIndexes[0].Mode)
	assert.Equal(t, 0, st.VectorIndexes[0].SizeBytes, "brute index must store no data")
	assert.Equal(t, 0, st.VectorIndexesSizeBytes)
	assert.Equal(t, dim, st.VectorIndexes[0].Dim)
}

// TestVectorMode_BruteExact verifies brute-force search is exact: it returns the
// true nearest neighbours (recall 1.0) via both the direct API and the Find
// pipeline, and applies a residual filter correctly.
func TestVectorMode_BruteExact(t *testing.T) {
	const (
		dim = 12
		n   = 400
		k   = 10
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	makeVectorIndex(t, coll, anystore.VectorModeBruteForce, dim)
	vecs := vrand(n, dim, 5)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	// direct API: exact top-1 self-retrieval + recall 1.0 vs brute oracle.
	hits, err := vsearch(coll, "v", vecs[42], 1, 0)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)
	assert.InDelta(t, 0, hits[0].Distance, 1e-3)

	queries := vrand(30, dim, 99)
	var recall float64
	for _, q := range queries {
		truth := bruteIDs(vecs, q, k)
		hh, err := vsearch(coll, "v", q, k, 0)
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
	assert.Equal(t, 1.0, recall, "brute-force search must be exact")

	// query pipeline returns the same nearest doc, decorated with _distance.
	iter, err := coll.Find(vectorKnnFilter(vecs[7], 1)).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	require.True(t, iter.Next())
	doc, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, 7, doc.Value().GetInt("id"))
	assert.InDelta(t, 0, iter.Distance(), 1e-3)
}

// TestVectorMode_HybridBehavesLikeBTree confirms hybrid mode is a working index
// in Phase 1 (it currently shares the btree backend; the RAM mirror lands in
// Phase 2). Recall and self-retrieval must match the btree path.
func TestVectorMode_HybridBehavesLikeBTree(t *testing.T) {
	const (
		dim = 16
		n   = 500
	)
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	makeVectorIndex(t, coll, anystore.VectorModeHybrid, dim)
	vecs := vrand(n, dim, 11)
	for i, vc := range vecs {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, vc))))
	}

	hits, err := vsearch(coll, "v", vecs[42], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	assert.Equal(t, idBytesOf(42), hits[0].DocId)

	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, "hybrid", st.VectorIndexes[0].Mode)
	assert.Greater(t, st.VectorIndexes[0].SizeBytes, 0, "hybrid keeps the btree graph")
}

// TestVectorMode_UnknownRejected ensures an out-of-range mode is rejected.
func TestVectorMode_UnknownRejected(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	err = coll.CreateIndex(ctx, anystore.IndexInfo{
		Name:   "emb",
		Kind:   anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: 8, Mode: anystore.VectorMode(99)},
	})
	assert.Error(t, err, "unknown vector mode must be rejected")
}

// A same-name vector index with a DIFFERENT definition must surface
// ErrIndexMismatch from EnsureIndex — vector params weren't compared at all, so
// e.g. a dim upgrade silently kept the old index.
func TestVectorIndex_RedefinitionDetected(t *testing.T) {
	const dim = 8
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2},
	}))

	// identical definition → idempotent no-op
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorL2},
	}))

	// changed dim → mismatch
	err = coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim * 2, Metric: anystore.VectorL2},
	})
	require.ErrorIs(t, err, anystore.ErrIndexMismatch)

	// changed metric → mismatch
	err = coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "emb", Kind: anystore.IndexKindVector,
		Vector: &anystore.VectorParams{Field: "v", Dim: dim, Metric: anystore.VectorCosine},
	})
	require.ErrorIs(t, err, anystore.ErrIndexMismatch)
}

// Same for fulltext scoring params (B/K1/weights are part of the definition).
func TestFulltextIndex_RedefinitionDetected(t *testing.T) {
	fx := newFixture(t)
	defer fx.finish()
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "ft", Kind: anystore.IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &anystore.FulltextParams{Weights: map[string]float64{"title": 8}},
	}))

	// identical → no-op
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "ft", Kind: anystore.IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &anystore.FulltextParams{Weights: map[string]float64{"title": 8}},
	}))

	// different weights → mismatch
	err = coll.EnsureIndex(ctx, anystore.IndexInfo{
		Name: "ft", Kind: anystore.IndexKindFulltext, Fields: []string{"title", "body"},
		Fulltext: &anystore.FulltextParams{Weights: map[string]float64{"title": 2}},
	})
	require.ErrorIs(t, err, anystore.ErrIndexMismatch)
}
