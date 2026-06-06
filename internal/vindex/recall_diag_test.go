package vindex

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// clusteredVecs generates n vectors drawn from `centers` Gaussian clusters — a
// stand-in for real embeddings, which have cluster structure (unlike uniform
// random vectors, which are nearly equidistant in high dim and intrinsically
// hard for ANN).
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
			v[d] = base[d] + float32(rng.NormFloat64())*0.1 // tight cluster
		}
		out[i] = v
	}
	return out
}

func recallAtEf(t *testing.T, db interface { /*unused*/
}, ix *Index, rtxVecs [][]float32, queries [][]float32, k, ef int, search func(q []float32, k, ef int) []string) float64 {
	var sum float64
	for _, q := range queries {
		truth := bruteTopK(rtxVecs, q, k)
		got := search(q, k, ef)
		hit := 0
		for _, id := range got {
			if truth[id] {
				hit++
			}
		}
		sum += float64(hit) / float64(k)
	}
	return sum / float64(len(queries))
}

// queriesFromData builds realistic queries: perturbed copies of random data
// points (the "find documents similar to this one" case), so ground truth is
// unambiguous — the source point and its near neighbours.
func queriesFromData(vecs [][]float32, nq int, noise float32, seed int64) [][]float32 {
	rng := rand.New(rand.NewSource(seed))
	out := make([][]float32, nq)
	for i := range out {
		src := vecs[rng.Intn(len(vecs))]
		q := make([]float32, len(src))
		for d := range q {
			q[d] = src[d] + float32(rng.NormFloat64())*noise
		}
		out[i] = q
	}
	return out
}

// TestVindexRecallDiag diagnoses the low 100k recall: is the graph unreachable
// (recall stays low even at huge ef) or is it the data/search-depth (recall
// climbs with ef, and clustered data is far better than uniform)?
func TestVindexRecallDiag(t *testing.T) {
	if testing.Short() {
		t.Skip("diagnostic")
	}
	const (
		n  = 20000
		k  = 10
		nq = 100
	)
	efs := []int{64, 128, 256, 512, 1024, 2048}

	run := func(name string, dim int, vecs, queries [][]float32) {
		db, ix := newTestIndex(t, dim, L2)
		insertAll(t, db, ix, vecs)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer rtx.Rollback()
		search := func(q []float32, k, ef int) []string {
			hits, err := ix.Search(rtx, q, k, ef)
			require.NoError(t, err)
			ids := make([]string, len(hits))
			for i, h := range hits {
				ids[i] = string(h.DocID)
			}
			return ids
		}
		t.Logf("--- %s (n=%d dim=%d k=%d) ---", name, n, dim, k)
		for _, ef := range efs {
			r := recallAtEf(t, nil, ix, vecs, queries, k, ef, search)
			t.Logf("  ef=%-5d recall@%d = %.3f", ef, k, r)
		}
	}

	u128 := randVecs(n, 128, 7)
	run("uniform dim128", 128, u128, queriesFromData(u128, nq, 0.01, 99))

	c128 := clusteredVecs(n, 128, 200, 11)
	run("clustered dim128", 128, c128, queriesFromData(c128, nq, 0.02, 13))

	c768 := clusteredVecs(n, 768, 200, 11)
	run("clustered dim768", 768, c768, queriesFromData(c768, nq, 0.02, 13))
}
