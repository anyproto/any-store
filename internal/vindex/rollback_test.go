package vindex

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

func rbVec(rng *rand.Rand, dim int, base float32) []float32 {
	v := make([]float32, dim)
	for i := range v {
		v[i] = base + rng.Float32()
	}
	return v
}

func rbID(i int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(i+1))
	return b
}

// A search inside a write tx must see the tx's own (uncommitted) inserts via
// direct btree reads — the hybrid RAM caches are bypassed on a writer's view.
func TestSearchInsideWriteTxSeesOwnInsert(t *testing.T) {
	const dim = 8
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	defer db.Close()

	rng := rand.New(rand.NewSource(7))
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: L2, EfSearch: 64}, 1)
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, ix.Insert(wtx, rbID(i), rbVec(rng, dim, 10)))
	}
	require.NoError(t, wtx.Commit())

	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	defer wtx.Rollback()
	vec := rbVec(rng, dim, -10)
	require.NoError(t, ix.Insert(wtx, rbID(999), vec))
	cands, err := ix.SearchCandidates(&wtx.ReadTx, vec, 10)
	require.NoError(t, err)
	require.NotEmpty(t, cands)
	require.Equal(t, rbID(999), cands[0].DocID, "in-tx search must see the tx's own insert")
}

// Hybrid-mode regression: a search inside a write tx that later rolls back must
// not feed the process-wide vector tier / l0 mirror with uncommitted labels or
// gens — a committed re-user of the same label would otherwise be served the
// rolled-back ghost vector forever.
func TestHybridRollbackDoesNotPoisonCaches(t *testing.T) {
	const dim = 8
	db, err := btree.Open(":memory:", btree.Options{InMemory: true})
	require.NoError(t, err)
	defer db.Close()

	rng := rand.New(rand.NewSource(7))
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ix, err := Create(wtx, "vix", Params{Dim: dim, Metric: L2, EfSearch: 64}, 1)
	require.NoError(t, err)
	ix.SetHybrid(true)
	ix.SetVectorCache(true)
	for i := 0; i < 50; i++ {
		require.NoError(t, ix.Insert(wtx, rbID(i), rbVec(rng, dim, 10)))
	}
	require.NoError(t, wtx.Commit())

	// warm the mirror + tier from committed state
	rtx0, err := db.BeginRead()
	require.NoError(t, err)
	_, err = ix.SearchCandidates(rtx0, rbVec(rng, dim, 10), 5)
	require.NoError(t, err)
	require.NoError(t, rtx0.Rollback())

	// ghost insert + in-tx search (must NOT feed the caches), then rollback
	ghostVec := rbVec(rng, dim, -10)
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, ix.Insert(wtx, rbID(999), ghostVec))
	_, err = ix.SearchCandidates(&wtx.ReadTx, ghostVec, 5)
	require.NoError(t, err)
	require.NoError(t, wtx.Rollback())

	// committed insert reusing the rolled-back label
	realVec := rbVec(rng, dim, 20)
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, ix.Insert(wtx, rbID(1000), realVec))
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// The real vector must be its own nearest neighbour at ~0 distance; with a
	// poisoned tier its distance is computed from the ghost bytes instead.
	cands, err := ix.SearchCandidates(rtx, realVec, 5)
	require.NoError(t, err)
	require.NotEmpty(t, cands)
	require.Equal(t, rbID(1000), cands[0].DocID, "committed reader served rolled-back ghost vector")
	require.Less(t, cands[0].Distance, float32(0.01))

	// And the ghost query must not surface the real doc.
	cands, err = ix.SearchCandidates(rtx, ghostVec, 5)
	require.NoError(t, err)
	for _, c := range cands {
		require.NotEqual(t, rbID(1000), c.DocID)
	}
}

// The insert vector cache must reset when the label sequence moves backwards
// (savepoint/tx rollback): the tx pointer alone is unsound because a savepoint
// rollback keeps the same WriteTx and pooled WriteTx objects can be reissued at
// the same address.
func TestVecCacheResetOnLabelRollback(t *testing.T) {
	ix := &Index{dim: 4, vcacheCap: 8}
	s := &searcher{ix: ix, rtx: &btree.ReadTx{}}

	s.beginVecCache(10)
	buf, hit := s.vcache.reserve(9)
	require.False(t, hit)
	copy(buf, []float32{1, 2, 3, 4})
	_, hit = s.vcache.reserve(9)
	require.True(t, hit)

	// normal forward progression keeps the cache
	s.beginVecCache(11)
	_, hit = s.vcache.reserve(9)
	require.True(t, hit)

	// label sequence moved backwards → rollback happened → cache must drop
	s.beginVecCache(9)
	_, hit = s.vcache.reserve(9)
	require.False(t, hit, "cache served a label from a rolled-back allocation")
}
