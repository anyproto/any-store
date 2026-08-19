package vivf

import (
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// shiftAll returns a copy of vecs with delta added to every component (moves them
// far from the original space → high reconstruction error against frozen centroids).
func shiftAll(vecs [][]float32, delta float32) [][]float32 {
	out := make([][]float32, len(vecs))
	for i, v := range vecs {
		c := make([]float32, len(v))
		for d := range v {
			c[d] = v[d] + delta
		}
		out[i] = c
	}
	return out
}

func driftScore(t *testing.T, db *btree.DB) float64 {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(t, err)
	s, err := ix.DriftScore(rtx)
	require.NoError(t, err)
	return s
}

func recallAt(t *testing.T, db *btree.DB, queries, all [][]float32, k int) float64 {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ix, err := OpenTx(rtx, "ivf")
	require.NoError(t, err)
	var sum float64
	for _, q := range queries {
		truth := bruteTopK(all, q, k) // L2 oracle over the full set
		tset := make(map[int]bool, k)
		for _, idx := range truth {
			tset[idx] = true
		}
		cands, err := ix.SearchCandidates(rtx, q, 10*k)
		require.NoError(t, err)
		hit := 0
		for i, c := range cands {
			if i >= k {
				break
			}
			idx := 0 // docID == bid(i) == big-endian (i+1)
			for _, b := range c.DocID {
				idx = idx<<8 | int(b)
			}
			if tset[idx-1] {
				hit++
			}
		}
		sum += float64(hit) / float64(k)
	}
	return sum / float64(len(queries))
}

// TestStoreIVFPQDrift verifies the drift signal rises when the distribution shifts
// away from the frozen codebooks, and that Rebuild clears it and restores recall.
func TestStoreIVFPQDrift(t *testing.T) {
	const (
		dim = 32
		k   = 10
	)
	base := clusteredVecs(2000, dim, 40, 1)
	db := openMem(t)
	p := StoreParams{Dim: dim, NList: 64, M: 16, Assign: 2, NProbe: 16, Normalize: false, KMeansPP: true, Seed: 1}
	buildStore(t, db, p, base)

	require.InDelta(t, 0, driftScore(t, db), 0.01, "fresh build has ~0 drift")

	// Insert an equal number of far-shifted vectors (a distribution the build-time
	// centroids do not cover) → reconstruction error and churn both climb.
	drifted := shiftAll(clusteredVecs(2000, dim, 40, 2), 6)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ixw, err := OpenTx(&wtx.ReadTx, "ivf")
	require.NoError(t, err)
	for i, v := range drifted {
		require.NoError(t, ixw.Insert(wtx, bid(2000+i), v)) // contiguous with base labels
	}
	require.NoError(t, wtx.Commit())

	score := driftScore(t, db)
	t.Logf("drift score after 100%% drifted inserts = %.2f", score)
	require.GreaterOrEqual(t, score, 0.5, "drift must be detected after a large distribution shift")

	// Recall on the drifted cluster is poor while centroids are stale.
	dq := drifted[:50]
	allAfter := append(append([][]float32{}, base...), drifted...)
	preRecall := recallAt(t, db, dq, allAfter, k)
	t.Logf("pre-rebuild recall on drifted cluster = %.3f", preRecall)

	// Rebuild: re-train from the live set.
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	_, err = Rebuild(wtx, "ivf")
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	require.InDelta(t, 0, driftScore(t, db), 0.01, "rebuild clears accumulated drift")
	postRecall := recallAt(t, db, dq, allAfter, k)
	t.Logf("post-rebuild recall on drifted cluster = %.3f", postRecall)
	require.Greater(t, postRecall, preRecall, "rebuild must improve recall on the drifted data")
	require.GreaterOrEqual(t, postRecall, 0.85, "rebuilt index covers the new distribution")
}
