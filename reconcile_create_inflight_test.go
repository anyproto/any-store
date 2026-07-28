package anystore

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests guard the create-in-flight gap in reconcileIndexSet: a read tx
// whose staleness verdict was baked at begin (an unconsumed schema-cookie
// delta — a peer commit, or a local commit racing beginRead between snapshot
// capture and the local-counter load) reconciles ALL registered handles
// against its own, older snapshot. A collection created after that snapshot
// has no catalog key there yet, so collectionVanished reports true via the
// keyNotFound branch and the staleness pass invalidates a live handle. The
// create commits fine; disk is correct; the cached handle is dead — every
// later op fails ErrCollectionClosed until the caller reopens.
//
// reconcileIndexSet guards rename-in-flight explicitly but not
// create-in-flight, though the same safety argument transfers: the creating
// writer holds the cross-process write lock, so any cookie bump a concurrent
// read pass consumes predates the creating tx's begin.

// The in-flight variant: the create sits inside an open write tx (handle
// registered mid-tx, catalog write uncommitted) while a plain read op begins
// with a stale local-counter cache and runs the full production
// ReadTx→checkStale path.
func TestReconcile_ReadTxSparesCreateInFlightHandle(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("staleness counters model cross-process commits; not applicable in-memory")
	}
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	seed, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	// Committed counters, to rewind against below.
	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Create in-flight: handle registered in openedCollections, catalog write
	// pending until wtx.Commit. The write tx's own begin ran checkStale with
	// counters in sync, so nothing is consumed yet.
	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	fresh, err := fx.CreateCollection(wtx.Context(), "fresh")
	require.NoError(t, err)

	// Emulate an unconsumed cookie bump: the state a reader captures when its
	// beginRead races a commit (snapshot fields baked before the writer's
	// counter store lands) or when a peer process committed DDL.
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)

	// Any read op now begins a read tx that sees IsSchemaStale and runs
	// reconcileIndexSet against a snapshot that predates the create.
	_, err = seed.Count(ctx)
	require.NoError(t, err)

	require.NoError(t, wtx.Commit())

	// The create committed; the handle must still be live.
	_, err = fresh.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a create-in-flight handle")
}

// The post-commit variant: the create commits normally, but the reader's
// snapshot predates it (begun before the create, checkStale preempted until
// after — the gap between BeginRead and checkStale inside db.ReadTx is a real
// preemption point). The committed handle must survive the pass.
func TestReconcile_ReadTxSparesFreshlyCommittedHandle(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("staleness counters model cross-process commits; not applicable in-memory")
	}
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	_, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the create below.
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	require.True(t, reader.IsSchemaStale())

	fresh, err := fx.CreateCollection(ctx, "fresh")
	require.NoError(t, err)

	// The reader resumes exactly where db.ReadTx would: checkStale on its own
	// pre-create snapshot.
	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	_, err = fresh.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a freshly committed handle")
}

// The rename analog: after Rename commits, the handle is registered under its
// NEW name, which a pre-rename snapshot cannot resolve (keyNotFound on the
// new key). Rename's publication raises the handle's visibility bound to the
// rename commit cookie so older-snapshot passes skip it.
func TestReconcile_ReadTxSparesFreshlyRenamedHandle(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("staleness counters model cross-process commits; not applicable in-memory")
	}
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	coll, err := fx.CreateCollection(ctx, "before")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the rename below.
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	require.True(t, reader.IsSchemaStale())

	require.NoError(t, coll.Rename(ctx, "after"))

	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())

	_, err = coll.Count(ctx)
	require.NoError(t, err, "read-tx staleness pass invalidated a freshly renamed handle")
}

// The consumption pin: a stale pass on a reader whose snapshot predates
// later DDL must consume only up to the SNAPSHOT counters — recording the
// raised/live values would mark the later DDL as reconciled though this pass
// never saw it, and no subsequent tx would ever reconcile it. The next begin
// must still report schema staleness and converge.
func TestCheckStale_ConsumesOnlySnapshotCounters(t *testing.T) {
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip("staleness counters model cross-process commits; not applicable in-memory")
	}
	fx := newFixture(t)
	dbi := fx.DB.(*db)

	_, err := fx.CreateCollection(ctx, "seed")
	require.NoError(t, err)

	rtx, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	fcc, sc := rtx.DiskFileChangeCounter(), rtx.DiskSchemaCookie()
	require.NoError(t, rtx.Rollback())

	// Stale verdict baked at begin; snapshot predates the DDL below.
	// Assertions run after each tx is released so a failure cannot leave a
	// reader open (Close would block in the fixture cleanup).
	dbi.btreeDB.UpdateLocalCounters(fcc, sc-1)
	reader, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	readerStale := reader.IsSchemaStale()

	later, err := fx.CreateCollection(ctx, "later")
	require.NoError(t, err)

	// The pass runs with a snapshot that lacks the create: it must neither
	// invalidate the new handle nor consume the create's cookie bump.
	dbi.checkStale(reader)
	require.NoError(t, reader.Rollback())
	require.True(t, readerStale)

	next, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	nextStale := next.IsSchemaStale()
	// A full-vision pass converges.
	dbi.checkStale(next)
	require.NoError(t, next.Rollback())
	require.True(t, nextStale,
		"stale pass must not consume DDL its snapshot never contained")

	settled, err := dbi.btreeDB.BeginRead()
	require.NoError(t, err)
	settledStale := settled.IsSchemaStale()
	require.NoError(t, settled.Rollback())
	require.False(t, settledStale)

	_, err = later.Count(ctx)
	require.NoError(t, err)
}
