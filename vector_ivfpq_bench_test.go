package anystore

import (
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// BenchmarkIVFPQQuery measures the full Find() vector-query path (ANN search +
// pipeline sort/limit) for the IVF-PQ index, so the cost of where the distance
// sort happens (internal vs the pipeline SortIter) is visible end to end.
func BenchmarkIVFPQQuery(b *testing.B) {
	const (
		n   = 20000
		dim = 64
	)
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(b, err)
	vecs := clusteredVecsAS(n, dim, 100, 7)
	for i, v := range vecs {
		require.NoError(b, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
	}
	require.NoError(b, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFPQ, Closure: 4, NProbe: 16},
	}))

	// $k covers the page window (offset+limit); Limit/Offset paginate within it.
	mkQueries := func(k int) []string {
		queries := make([]string, 256)
		for i := range queries {
			queries[i] = fmt.Sprintf(`{"v":%s}`, vknnJSON(vecs[i], k, 0))
		}
		return queries
	}

	cases := []struct {
		name          string
		k             int
		limit, offset uint
	}{
		{"limit10", 10, 10, 0},
		{"limit1", 1, 1, 0},
		{"limit100", 100, 100, 0},
		{"limit10_offset200", 210, 10, 200},
	}
	for _, tc := range cases {
		queries := mkQueries(tc.k)
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				iter, err := coll.Find(queries[i%len(queries)]).Limit(tc.limit).Offset(tc.offset).Iter(ctx)
				if err != nil {
					b.Fatal(err)
				}
				for iter.Next() {
				}
				_ = iter.Close()
			}
		})
	}
}
