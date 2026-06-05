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
