package anystore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
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

func setupPipeline(t *testing.T, n, dim int) (Collection, [][]float32) {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
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

// TestPipeline_Minimum: Find({"v":[..]}).Iter() — implicit distance order + Distance().
func TestPipeline_Minimum(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 800, dim)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s}`, vqJSON(vecs[42]))).Iter(ctx)
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
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"_distance":{"$lt":%g}}`, vqJSON(vecs[100]), thr)).
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

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"lang":"en"}`, vqJSON(vecs[10]))).Iter(ctx)
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
func setupPipelineEf(t *testing.T, n, dim, efSearch int) (Collection, [][]float32) {
	t.Helper()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: efSearch},
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

// TestPipeline_OverFetch: a filtered query with a limit fills the limit because
// the planner over-fetches candidates (ef = limit×factor), even though the
// index's own EfSearch is small.
func TestPipeline_OverFetch(t *testing.T) {
	const (
		dim      = 16
		efSearch = 16 // small: limit alone (30) would not yield 30 en survivors
		limit    = 30
	)
	coll, vecs := setupPipelineEf(t, 1000, dim, efSearch)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"lang":"en"}`, vqJSON(vecs[0]))).Limit(limit).Iter(ctx)
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
	assert.Equal(t, limit, count, "over-fetch should fill the limit despite a selective filter")
}

// TestPipeline_VectorEf: an explicit VectorEf bounds the candidate set.
func TestPipeline_VectorEf(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipelineEf(t, 1000, dim, 64)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s}`, vqJSON(vecs[0]))).VectorEf(5).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()
	var count int
	for iter.Next() {
		count++
	}
	require.NoError(t, iter.Err())
	assert.LessOrEqual(t, count, 5, "VectorEf(5) must cap the candidate set")
	assert.Greater(t, count, 0)
}

// TestPipeline_Errors: malformed vector queries return errors instead of
// silently degrading to a literal filter or matching everything.
func TestPipeline_Errors(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 100, dim) // field "v", dim 16

	cases := []struct {
		name   string
		find   string
		sort   []any
		wantIs error
	}{
		{"wrong-dim vector", `{"v":[1,2,3]}`, nil, ErrInvalidVectorQuery},
		{"non-array on vector field", `{"v":"hello"}`, nil, ErrInvalidVectorQuery},
		{"range op on vector field", `{"v":{"$gt":[0.0]}}`, nil, ErrInvalidVectorQuery},
		{"_distance filter w/o vector clause", `{"_distance":{"$lt":1.0}}`, nil, ErrDistanceWithoutVector},
		{"_distance sort w/o vector clause", `{"id":{"$gt":5}}`, []any{"-_distance"}, ErrDistanceWithoutVector},
		{"_distance sort, no filter", ``, []any{"_distance"}, ErrDistanceWithoutVector},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fq Query
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

	// sanity: a valid vector query (optionally with _distance) does NOT error
	iter, err := coll.Find(fmt.Sprintf(`{"v":%s,"_distance":{"$lt":2.0}}`, vqJSON(vecs[0]))).
		Sort("-_distance").Iter(ctx)
	require.NoError(t, err)
	require.NoError(t, iter.Close())
}

// TestPipeline_Limit: limit applies over the distance-ordered results.
func TestPipeline_Limit(t *testing.T) {
	const dim = 16
	coll, vecs := setupPipeline(t, 800, dim)

	iter, err := coll.Find(fmt.Sprintf(`{"v":%s}`, vqJSON(vecs[5]))).Limit(10).Iter(ctx)
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
	qv := vqJSON(vecs[5])

	collect := func(query Query) []string {
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
