package vector

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildFlatIdx(t testing.TB, n, dim int, seed int64) (*FlatHNSW, [][]float32, []uint64) {
	vecs, keys := randVectors(n, dim, seed)
	g := NewFlatHNSW(dim, L2, 1)
	g.EfSearch = 64
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}
	return g, vecs, keys
}

func TestDeleteExcludesFromResults(t *testing.T) {
	const dim = 32
	g, vecs, keys := buildFlatIdx(t, 1000, dim, 5)

	// self-query returns the point itself
	res := g.Search(vecs[100], 1)
	require.Len(t, res, 1)
	require.Equal(t, keys[100], res[0].Key)

	require.True(t, g.Delete(keys[100]))
	require.False(t, g.Delete(keys[100]), "double delete is a no-op")
	require.Equal(t, 999, g.Len())
	require.Equal(t, 1000, g.PhysicalLen(), "tombstone keeps the slot")

	// the deleted key must no longer appear, even for its own vector
	for _, r := range g.Search(vecs[100], 10) {
		assert.NotEqual(t, keys[100], r.Key, "deleted key leaked into results")
	}
}

func TestDeleteRecallHoldsThenCompact(t *testing.T) {
	const (
		n   = 3000
		dim = 48
		k   = 10
	)
	g, vecs, keys := buildFlatIdx(t, n, dim, 7)
	queries, _ := randVectors(100, dim, 22)

	// ground truth over the LIVE set after deleting 30%
	deleteSet := make(map[uint64]bool)
	for i := 0; i < n*30/100; i++ {
		g.Delete(keys[i])
		deleteSet[keys[i]] = true
	}
	require.InDelta(t, 0.30, g.DeletedFraction(), 0.001)

	brute := NewBrute(dim, L2)
	for i := range vecs {
		if !deleteSet[keys[i]] {
			brute.Add(keys[i], vecs[i])
		}
	}

	recall := func() float64 {
		var r float64
		for _, q := range queries {
			r += recallAt(g.Search(q, k), brute.Search(q, k))
		}
		return r / float64(len(queries))
	}

	preCompact := recall()
	t.Logf("recall@%d with 30%% tombstones (pre-compact)  = %.3f", k, preCompact)
	assert.Greater(t, preCompact, 0.80)

	physBefore := g.PhysicalLen()
	g.Compact()
	require.Equal(t, n-len(deleteSet), g.PhysicalLen(), "compact reclaims tombstones")
	require.Less(t, g.PhysicalLen(), physBefore)
	require.Equal(t, n-len(deleteSet), g.Len())

	postCompact := recall()
	t.Logf("recall@%d after Compact                        = %.3f", k, postCompact)
	assert.Greater(t, postCompact, 0.80)

	// none of the deleted keys may survive compaction
	for _, q := range queries {
		for _, r := range g.Search(q, k) {
			assert.False(t, deleteSet[r.Key])
		}
	}
}

func TestUpdateMovesVector(t *testing.T) {
	const dim = 24
	g, vecs, keys := buildFlatIdx(t, 800, dim, 3)

	// move key[10] to sit exactly on key[500]'s location
	require.True(t, g.Update(keys[10], vecs[500]))
	require.Equal(t, 800, g.Len(), "update keeps the live count")
	require.Equal(t, 801, g.PhysicalLen(), "old node tombstoned, new node appended")

	// querying near vecs[500] should now find key[10] too
	res := g.Search(vecs[500], 5)
	var found10 bool
	for _, r := range res {
		assert.NotEqual(t, keys[10], "")
		if r.Key == keys[10] {
			found10 = true
		}
	}
	assert.True(t, found10, "updated key not found at its new location")

	require.False(t, g.Update(99999, vecs[0]), "update of missing key returns false")
}

func TestCompactVsRebuildRecall(t *testing.T) {
	const (
		n   = 3000
		dim = 48
		k   = 10
	)
	g, vecs, keys := buildFlatIdx(t, n, dim, 7)
	queries, _ := randVectors(100, dim, 22)

	deleteSet := make(map[uint64]bool)
	for i := 0; i < n/2; i++ { // delete 50%
		g.Delete(keys[i])
		deleteSet[keys[i]] = true
	}
	brute := NewBrute(dim, L2)
	for i := range vecs {
		if !deleteSet[keys[i]] {
			brute.Add(keys[i], vecs[i])
		}
	}
	recall := func() float64 {
		var r float64
		for _, q := range queries {
			r += recallAt(g.Search(q, k), brute.Search(q, k))
		}
		return r / float64(len(queries))
	}

	g.Compact()
	rc := recall()
	g.Rebuild()
	rr := recall()
	t.Logf("after 50%% deletes: Compact recall=%.3f  Rebuild recall=%.3f", rc, rr)
	assert.Greater(t, rc, 0.75)
	assert.Greater(t, rr, 0.85)
}

func TestHardDeleteRepairConsistency(t *testing.T) {
	const dim = 16
	g, _, keys := buildFlatIdx(t, 500, dim, 1)
	// hard-delete should also remove from results and keep graph consistent
	require.True(t, g.DeleteHardRepair(keys[42], true))
	require.Equal(t, 499, g.Len())
	for _, q := range keys {
		_ = q
	}
	// no dangling edge to the removed id should remain
	g.mu.RLock()
	removed := uint32(42) // keys[i]=i+1 so key 43 -> id 42; just scan for any deleted-id edge
	_ = removed
	for id := uint32(0); id < uint32(len(g.keys)); id++ {
		if g.deleted[id] {
			continue
		}
		for lc := int32(0); lc <= g.level[id]; lc++ {
			for _, nb := range g.neighbors(id, lc) {
				assert.False(t, g.deleted[nb], "live node %d still links to deleted %d", id, nb)
			}
		}
	}
	g.mu.RUnlock()
}
