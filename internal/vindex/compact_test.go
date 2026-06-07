package vindex

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestVindexCompact builds an index, churns it with deletes and replaces (which
// only tombstone), then compacts and asserts the tombstones are reclaimed, the
// label space is dense again, and the rebuilt graph still serves the live set
// correctly (self-retrieval + deleted exclusion + recall).
func TestVindexCompact(t *testing.T) {
	const (
		n   = 1500
		dim = 24
		k   = 10
	)
	vecs := randVecs(n, dim, 7)
	db, ix := newTestIndex(t, dim, L2)
	insertAll(t, db, ix, vecs)

	// Churn: delete every 3rd doc, replace every 3rd (i%3==1) with a fresh vector
	// (the replace tombstones the old label). The rest stay as-is.
	live := make(map[int][]float32, n)
	for i := range vecs {
		live[i] = vecs[i]
	}
	repl := randVecs(n, dim, 99)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		switch i % 3 {
		case 0:
			ok, derr := ix.Delete(wtx, docID(i))
			require.NoError(t, derr)
			require.True(t, ok)
			delete(live, i)
		case 1:
			require.NoError(t, ix.Insert(wtx, docID(i), repl[i]))
			live[i] = repl[i]
		}
	}
	require.NoError(t, wtx.Commit())

	// Pre-compaction: tombstones inflate the total above the live count.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	liveCnt, delCnt, total, err := ix.Counts(rtx)
	require.NoError(t, rtx.Rollback())
	require.NoError(t, err)
	require.Equal(t, len(live), liveCnt)
	require.Greater(t, delCnt, 0, "expected tombstones before compaction")
	require.Greater(t, total, liveCnt, "tombstones should inflate the label space")

	// Compact (its own tx). Use the returned index — the old one's handles point
	// at the now-recreated namespaces.
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	cix, err := Compact(wtx, "vix", 1)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	// Post-compaction: dense, no tombstones, live count preserved.
	rtx, err = db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	liveCnt2, delCnt2, total2, err := cix.Counts(rtx)
	require.NoError(t, err)
	require.Equal(t, len(live), liveCnt2, "live count must survive compaction")
	require.Equal(t, 0, delCnt2, "no tombstones after compaction")
	require.Equal(t, len(live), total2, "label space must be dense after compaction")

	// Self-retrieval: every live doc's own vector retrieves itself at distance ~0.
	// This proves the rebuilt label→vector→docId mapping is consistent.
	for i, v := range live {
		hits, serr := cix.Search(rtx, v, 1, 64)
		require.NoError(t, serr)
		require.Len(t, hits, 1)
		require.Equal(t, docID(i), hits[0].DocID, "self-retrieval mismatch for doc %d", i)
		require.InDelta(t, 0, hits[0].Distance, 1e-4)
	}

	// Deleted docs must never reappear, and recall over the live set stays high.
	deleted := map[string]bool{}
	for i := 0; i < n; i++ {
		if _, ok := live[i]; !ok {
			deleted[string(docID(i))] = true
		}
	}
	liveIdx := make([]int, 0, len(live))
	for i := range live {
		liveIdx = append(liveIdx, i)
	}
	var hitSum, queries float64
	for qi := 0; qi < 50; qi++ {
		q := randVecs(1, dim, int64(5000+qi))[0]
		truth := bruteLiveTopK(live, q, k)
		hits, serr := cix.Search(rtx, q, k, 128)
		require.NoError(t, serr)
		hit := 0
		for _, h := range hits {
			require.False(t, deleted[string(h.DocID)], "deleted doc returned by search")
			if truth[string(h.DocID)] {
				hit++
			}
		}
		hitSum += float64(hit) / float64(k)
		queries++
	}
	require.Greater(t, hitSum/queries, 0.90, "recall regressed after compaction")
}

// bruteLiveTopK is exact nearest-by-L2 over a live-doc map keyed by index.
func bruteLiveTopK(live map[int][]float32, q []float32, k int) map[string]bool {
	type p struct {
		id string
		d  float32
	}
	all := make([]p, 0, len(live))
	for i, v := range live {
		all = append(all, p{string(docID(i)), l2(q, v)})
	}
	// simple selection of k smallest (k is tiny)
	out := map[string]bool{}
	for n := 0; n < k && n < len(all); n++ {
		best := -1
		for j := range all {
			if out[all[j].id] {
				continue
			}
			if best < 0 || all[j].d < all[best].d {
				best = j
			}
		}
		out[all[best].id] = true
	}
	return out
}

// TestVindexCompactNoop confirms a clean (tombstone-free, dense) index compacts
// to a no-op that returns a working index over the same data.
func TestVindexCompactNoop(t *testing.T) {
	const dim = 16
	vecs := randVecs(200, dim, 3)
	db, ix := newTestIndex(t, dim, L2)
	insertAll(t, db, ix, vecs)

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	cix, err := Compact(wtx, "vix", 1)
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	_, deleted, total, err := cix.Counts(rtx)
	require.NoError(t, err)
	require.Equal(t, 0, deleted)
	require.Equal(t, len(vecs), total)
	hits, err := cix.Search(rtx, vecs[0], 1, 64)
	require.NoError(t, err)
	require.Len(t, hits, 1)
	require.Equal(t, docID(0), hits[0].DocID)
}
