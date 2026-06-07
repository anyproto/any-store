package anystore

import (
	"sort"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestVectorCompactThreshold empirically maps tombstone accumulation to search
// performance, to justify a default CompactRatio. Tombstoned nodes are still
// traversed during search (they route but never enter results), so as the
// deleted/live ratio grows, search latency rises and recall falls. The test
// deletes in steps WITHOUT compacting and records, at each deleted/live ratio,
// the search latency (relative to the tombstone-free baseline) and recall — then
// reports the ratio at which latency has degraded ~30%.
//
// Tunable: ASV_THRESH_N / ASV_THRESH_DIM / ASV_THRESH_Q / ASV_THRESH_EF.
// Defaults are small; run with larger N to confirm on big data.
//
// Measured results (in-memory):
//
//	N=5000   dim=128: latency 1.31x at deleted/live=0.25, 1.83x at 1.0
//	N=50000  dim=768: latency 1.41x at deleted/live=0.25, 2.24x at 1.0
//
// i.e. search latency degrades ~30% by a deleted/live ratio of ~0.2–0.25
// (steeper at larger N / higher dim), while recall holds or rises as the live
// set shrinks. So a ~30%-degradation compaction trigger ≈ CompactRatio 0.2–0.25;
// 0.5 is the balanced default (see VectorParams.CompactRatio). On a file-backed
// DB the tombstone reads add page-cache pressure, so degradation likely crosses
// 30% a little earlier than the in-memory figures above.
func TestVectorCompactThreshold(t *testing.T) {
	if testing.Short() {
		t.Skip("skip compaction-threshold sweep in -short")
	}
	// Small defaults so a plain `go test` is a fast smoke run; scale up via env
	// (e.g. ASV_THRESH_N=50000 ASV_THRESH_DIM=768) to confirm on big data.
	n := envIntDefault("ASV_THRESH_N", 3000)
	dim := envIntDefault("ASV_THRESH_DIM", 128)
	nq := envIntDefault("ASV_THRESH_Q", 150)
	ef := envIntDefault("ASV_THRESH_EF", 64)
	const k = 10
	rq := nq
	if rq > 40 {
		rq = 40 // cap recall queries (brute oracle is O(live*rq*dim))
	}

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, EfSearch: 64},
	}))

	vecs := vrand(n, dim, 42)
	a := &anyenc.Arena{}
	tx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	for i, v := range vecs {
		require.NoError(t, coll.Insert(tx.Context(), vecDocArena(a, i, v)))
		if (i+1)%10000 == 0 {
			require.NoError(t, tx.Commit())
			tx, err = coll.WriteTx(ctx)
			require.NoError(t, err)
		}
	}
	require.NoError(t, tx.Commit())

	queries := vrand(nq, dim, 7)

	// measure returns the median per-query search latency over `reps` batches.
	measure := func() time.Duration {
		const reps = 5
		for _, q := range queries[:min(20, nq)] { // warm
			_, _ = coll.VectorSearch(ctx, "emb", q, k, ef)
		}
		means := make([]time.Duration, reps)
		for r := 0; r < reps; r++ {
			start := time.Now()
			for _, q := range queries {
				_, serr := coll.VectorSearch(ctx, "emb", q, k, ef)
				require.NoError(t, serr)
			}
			means[r] = time.Since(start) / time.Duration(nq)
		}
		sort.Slice(means, func(i, j int) bool { return means[i] < means[j] })
		return means[reps/2]
	}

	// recallAt computes recall@k vs a brute oracle over the current live set
	// (ids [delFrom, n)), sampled over rq queries.
	recallAt := func(delFrom int) float64 {
		var sum float64
		for qi := 0; qi < rq; qi++ {
			q := queries[qi]
			truth := bruteLivePrefix(vecs, delFrom, q, k)
			hits, herr := coll.VectorSearch(ctx, "emb", q, k, ef)
			require.NoError(t, herr)
			hit := 0
			for _, h := range hits {
				if truth[string(h.DocId)] {
					hit++
				}
			}
			sum += float64(hit) / float64(k)
		}
		return sum / float64(rq)
	}

	baseLat := measure()
	baseRecall := recallAt(0)

	type row struct {
		delRatio, latRatio, recall float64
		live, deleted              int
	}
	var rows []row
	var crossing float64

	delFrom := 0
	step := n / 10 // 10% of original per step
	if step < 1 {
		step = 1
	}
	for {
		end := delFrom + step
		if end >= n {
			break
		}
		dtx, derr := coll.WriteTx(ctx)
		require.NoError(t, derr)
		for i := delFrom; i < end; i++ {
			require.NoError(t, coll.DeleteId(dtx.Context(), i))
		}
		require.NoError(t, dtx.Commit())
		delFrom = end

		st := vstat(t, coll, "emb")
		lat := measure()
		r := row{
			delRatio: float64(st.DeletedCount) / float64(st.LiveCount),
			latRatio: float64(lat) / float64(baseLat),
			recall:   recallAt(delFrom),
			live:     st.LiveCount,
			deleted:  st.DeletedCount,
		}
		rows = append(rows, r)
		if crossing == 0 && r.latRatio >= 1.30 {
			crossing = r.delRatio
		}
		if r.delRatio >= 3.0 { // deleted ≈ 75% — plenty past any sane threshold
			break
		}
	}

	t.Logf("N=%d dim=%d ef=%d  baseline: latency=%s recall@%d=%.3f",
		n, dim, ef, baseLat.Round(time.Microsecond), k, baseRecall)
	t.Logf("%-14s %-10s %-10s %s", "deleted/live", "latency", "recall", "(live/deleted)")
	for _, r := range rows {
		t.Logf("%-14.2f %-10.2f %-10.3f (%d/%d)", r.delRatio, r.latRatio, r.recall, r.live, r.deleted)
	}
	if crossing > 0 {
		t.Logf("=> search latency degrades 30%% at deleted/live ≈ %.2f → suggested CompactRatio", crossing)
	} else {
		t.Logf("=> latency never reached 1.30x within the swept range (max deleted/live ≈ 3.0)")
	}
}

// bruteLivePrefix is the exact top-k (by L2) over the live docs [delFrom, n),
// returning the marshaled-id set.
func bruteLivePrefix(vecs [][]float32, delFrom int, q []float32, k int) map[string]bool {
	type p struct {
		i int
		d float32
	}
	all := make([]p, 0, len(vecs)-delFrom)
	for i := delFrom; i < len(vecs); i++ {
		all = append(all, p{i, vl2(q, vecs[i])})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].d < all[j].d })
	out := make(map[string]bool, k)
	for i := 0; i < k && i < len(all); i++ {
		out[string(idBytesOf(all[i].i))] = true
	}
	return out
}
