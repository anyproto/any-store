package anystore

import (
	"fmt"
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSketchNoAccumulatingDriftAcrossRolledBackTxs is the regression test for the
// rolled-back-sketch-delta fix. insertKeys/deleteKeys mutate the live index sketch
// in place and set sketchModified; a committed tx clears that flag via
// persistSketches, but a ROLLED-BACK tx does not. Before the fix, those phantom
// deltas accumulated unbounded across rolled-back txs (a non-stale write tx never
// reloaded the sketch) and were persisted to disk on the next clean commit —
// drifting the planner's cardinality estimate (advisory only; never query results).
//
// The fix (db.resetUncommittedSketches, called at write-tx begin): if an index's
// sketchModified is still set when a new write tx begins, its live sketch is
// rebased to the last committed on-disk state before the new tx applies its own
// deltas — so phantom deltas from a prior rolled-back tx are discarded instead of
// accumulating, and are never persisted. This is the in-methodology analog of
// resetting to the committed snapshot at tx begin, with zero cost on the
// all-commit happy path.
//
// Guarantees asserted here:
//   - NO ACCUMULATION: after M rolled-back txs the sketch docCount is bounded to
//     N + K (the most recent rolled-back tx's deltas, cleaned up at the next write
//     tx begin), NOT N + (1+M)*K.
//   - NO DISK POLLUTION: a clean commit after the rollbacks persists the true
//     count (N+1), not the accumulated drift.
//   - The real Count() is exact (== N) throughout (the sketch is advisory).
func TestSketchNoAccumulatingDriftAcrossRolledBackTxs(t *testing.T) {
	const (
		N = 10 // committed baseline docs
		K = 3  // docs inserted (then rolled back) per iteration
		M = 50 // number of rolled-back txs
	)

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"k"}}))
	insp := fx.DB.(IndexSketchInspector)

	// --- 1) Commit a clean baseline of N distinct docs. ---
	for i := 0; i < N; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"k":%d}`, i, i),
		)))
	}
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, N, cnt, "baseline real Count")
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	require.Len(t, st.Indexes, 1)
	require.Equal(t, uint64(N), st.Indexes[0].SketchDocCount, "baseline sketch docCount == N")

	// --- 2) Loop a rolled-back insert of K docs M times. Each new write tx begin
	// rebases the live sketch to the committed state (N) before adding its own K and
	// rolling back, so the phantom deltas DO NOT accumulate. ---
	for it := 0; it < M; it++ {
		tx, err := fx.WriteTx(ctx)
		require.NoError(t, err)
		base := 2000 + it*K
		for j := 0; j < K; j++ {
			require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"k":%d}`, base+j, base+j),
			)))
		}
		require.NoError(t, tx.Rollback())
	}

	// Real count never moved.
	cnt, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, N, cnt, "real Count after M rolled-back txs is STILL N")

	// NO ACCUMULATION: sketch docCount is bounded to N+K (the last rolled-back tx's
	// deltas, not yet cleaned up because no further write tx has begun), NOT the
	// pre-fix N+(1+M)*K. The decisive check: it does not grow with M.
	st, err = coll.Stats(ctx)
	require.NoError(t, err)
	gotDrift := st.Indexes[0].SketchDocCount
	t.Logf("after %d rolled-back txs (K=%d each): real Count=%d, sketch docCount=%d (bounded residual=N+K=%d, pre-fix would be %d)",
		M, K, cnt, gotDrift, N+K, N+(1+M)*K)
	assert.LessOrEqual(t, gotDrift, uint64(N+K),
		"sketch docCount must be bounded to a single tx's deltas, not accumulate with M")

	// --- 3) On-disk sketch is still N: every tx above rolled back, persist never ran. ---
	on, err := insp.InspectIndexSketch(ctx, "coll", "k")
	require.NoError(t, err)
	assert.Equal(t, uint64(N), on.DocCount, "on-disk sketch still N after only-rolled-back txs")

	// --- 4) A clean commit now persists the TRUE count (N+1), not accumulated drift:
	// its write-tx begin rebased live to the committed N, then it added 1. ---
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":9999,"k":9999}`)))
	cnt, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, N+1, cnt, "real Count now N+1 after one clean commit")
	on, err = insp.InspectIndexSketch(ctx, "coll", "k")
	require.NoError(t, err)
	assert.Equal(t, uint64(N+1), on.DocCount,
		"on-disk sketch == true N+1 (phantom deltas discarded, not persisted)")

	// In-memory sketch now also reflects the true committed count.
	st, err = coll.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint64(N+1), st.Indexes[0].SketchDocCount, "in-memory sketch == true N+1")
}

// TestSketchNoDriftOnUniqueViolationRollback covers the most common real trigger
// of the rolled-back-sketch-delta class (case C1): a batch Insert whose later doc
// violates a unique constraint auto-rolls-back the whole tx (doWriteTx), leaving
// the earlier docs' sketch increments phantom. The fix's write-tx-begin rebase
// must keep those phantoms from accumulating or reaching disk; the real Count and
// unique enforcement are unaffected (the sketch is advisory).
func TestSketchNoDriftOnUniqueViolationRollback(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "coll")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"u"}, Unique: true}))
	insp := fx.DB.(IndexSketchInspector)

	// Commit one doc with u=99.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":0,"u":99}`)))

	// Repeatedly attempt a batch whose 3rd doc collides on u=99 → ErrUniqueConstraint
	// → the whole Insert tx auto-rolls-back, leaking the first two docs' increments.
	for it := 0; it < 25; it++ {
		base := 100 + it*10
		err = coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, base, base)),
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"u":%d}`, base+1, base+1)),
			anyenc.MustParseJson(`{"id":777,"u":99}`), // collides → rollback
		)
		require.ErrorIs(t, err, ErrUniqueConstraint, "unique constraint must still be enforced (real seek, not sketch)")
	}

	// Real count is exactly 1 throughout (every batch rolled back).
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "real Count unaffected by rolled-back unique-violation batches")

	// Sketch docCount did NOT accumulate to 1 + 25*2: it is bounded to the last
	// rolled-back batch's residual (≤ 1 + 2), cleaned up at each subsequent write-tx
	// begin. Pre-fix this would be 51.
	st, err := coll.Stats(ctx)
	require.NoError(t, err)
	t.Logf("after 25 unique-violation rollbacks: real Count=%d, sketch docCount=%d (pre-fix would be %d)",
		cnt, st.Indexes[0].SketchDocCount, 1+25*2)
	assert.LessOrEqual(t, st.Indexes[0].SketchDocCount, uint64(1+2),
		"sketch docCount bounded to one batch's deltas, not accumulating across unique-violation rollbacks")

	// A clean commit persists the true count (2), not accumulated drift.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"u":1}`)))
	cnt, err = coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt)
	on, err := insp.InspectIndexSketch(ctx, "coll", "u")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), on.DocCount, "on-disk sketch == true 2 (phantoms discarded, not persisted)")
}
