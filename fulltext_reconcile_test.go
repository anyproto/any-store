package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// A concurrent read tx's staleness pass must not rebuild a collection's index
// sets while a local write tx has uncommitted index DDL published in them —
// its older snapshot would evict the writer's uncommitted indexes. The
// indexSetDDLTxs counter carries that in-flight state.

func TestIndexSetDDLTxs_BalancedAcrossCommitAndRollback(t *testing.T) {
	fx := newFixture(t)
	collIface, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	coll := collIface.(*collection)

	ddlTxs := func() int {
		coll.mu.Lock()
		defer coll.mu.Unlock()
		return coll.indexSetDDLTxs
	}
	require.Zero(t, ddlTxs())

	// Committed DDL: counter is 1 while the tx is open, 0 after commit.
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(tx.Context(), IndexInfo{Fields: []string{"a"}, Kind: IndexKindFulltext}))
	assert.Equal(t, 1, ddlTxs(), "uncommitted DDL must be marked in flight")
	require.NoError(t, tx.Commit())
	assert.Zero(t, ddlTxs(), "commit must release the in-flight marker")

	// Rolled-back DDL: same lifecycle through the undo path.
	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(tx2.Context(), IndexInfo{Fields: []string{"b"}}))
	assert.Equal(t, 1, ddlTxs())
	require.NoError(t, tx2.Rollback())
	assert.Zero(t, ddlTxs(), "rollback must release the in-flight marker")
}

func TestReconcileSkipsWhileLocalIndexDDLInFlight(t *testing.T) {
	fx := newFixture(t)
	collIface, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	coll := collIface.(*collection)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"text"}, Kind: IndexKindFulltext}))

	// Plant a ghost fts handle that no on-disk catalog entry backs — the
	// stand-in for a writer's just-published, not-yet-committed index as seen
	// by a reconcile running on an older snapshot.
	ghost := &ftsIndex{c: coll, info: IndexInfo{Name: "ghost", Kind: IndexKindFulltext, Fields: []string{"g"}}}
	coll.mu.Lock()
	coll.storeFtsIndexes(append(coll.loadFtsIndexes(), ghost))
	coll.indexSetDDLTxs++
	coll.mu.Unlock()

	inSnapshot := func() bool {
		for _, fxi := range coll.loadFtsIndexes() {
			if fxi == ghost {
				return true
			}
		}
		return false
	}

	// With DDL in flight, reconcile must leave the sets untouched.
	require.NoError(t, fx.DB.(*db).doReadTx(ctx, func(tx *btree.ReadTx) error {
		coll.reconcileIndexes(tx)
		return nil
	}))
	assert.True(t, inSnapshot(), "reconcile evicted an index while local DDL was in flight")

	// Once the writer resolved, the same reconcile evicts the ghost by
	// omission (it has no catalog entry in the snapshot).
	coll.mu.Lock()
	coll.indexSetDDLTxs--
	coll.mu.Unlock()
	require.NoError(t, fx.DB.(*db).doReadTx(ctx, func(tx *btree.ReadTx) error {
		coll.reconcileIndexes(tx)
		return nil
	}))
	assert.False(t, inSnapshot(), "reconcile must evict a catalog-less handle once no DDL is in flight")
}
