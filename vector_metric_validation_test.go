package anystore

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// TestVectorMetricModeMatrix pins the accepted metric x mode combinations.
// vivf's distance surface is L2, or cosine via unit-normalization — it has no
// dot-product ranking path — so VectorDot on the IVF modes must be refused at
// validation instead of silently ranking by L2 (wrong order, no error).
func TestVectorMetricModeMatrix(t *testing.T) {
	const dim = 32
	modes := []VectorMode{VectorModeBTree, VectorModeHybrid, VectorModeBruteForce, VectorModeIVFPQ, VectorModeIVFSQ}
	metrics := []VectorMetric{VectorCosine, VectorL2, VectorDot}
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
				err = coll.CreateIndex(ctx, IndexInfo{
					Name: "emb",
					Kind: IndexKindVector,
					Vector: &VectorParams{Field: "v", Dim: dim, Metric: metric, Mode: mode},
				})
				if metric == VectorDot && (mode == VectorModeIVFPQ || mode == VectorModeIVFSQ) {
					assert.ErrorIs(t, err, ErrVectorMetricUnsupported)
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
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb",
		Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorDot, Mode: VectorModeBruteForce},
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
