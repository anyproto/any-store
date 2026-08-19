package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SnapshotHeaderCounters must be bounded by the reader's own snapshot on the
// MAX side: a commit that lands after the reader began is invisible to it,
// while the raised detection read (pager.readHeaderCounters) sees it. A
// regression that routes the snapshot read through the raised path returns
// the post-commit cookie here.
func TestSnapshotHeaderCounters_BoundedByReaderSnapshot(t *testing.T) {
	db := tempDB(t)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("a")
	require.NoError(t, err)
	tx.MarkDataChanged()
	tx.MarkSchemaChanged()
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	baseSC := rtx.DiskSchemaCookie()

	// Commit more DDL while the reader is open.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("b")
	require.NoError(t, err)
	tx.MarkDataChanged()
	tx.MarkSchemaChanged()
	require.NoError(t, tx.Commit())

	_, sc, err := rtx.SnapshotHeaderCounters()
	require.NoError(t, err)
	assert.Equal(t, baseSC, sc, "snapshot counters must not see a post-begin commit")

	// The raised detection read at the same frame bound DOES see it — the
	// divergence the snapshot-bounded accessor exists for.
	_, raisedSC, err := db.pager.readHeaderCounters(rtx.WalMaxFrame())
	require.NoError(t, err)
	assert.Equal(t, baseSC+1, raisedSC, "raised read should see the newer commit")

	require.NoError(t, rtx.Rollback())
}

// SnapshotHeaderCounters must be bounded on the MIN side by the reader's
// begin-captured floor, not the live checkpoint frontier: a slot-0 reader
// (WAL fully backfilled at begin, minFrame = maxFrame+1) that survives a WAL
// restart must not admit new-generation frame numbers that recycle below its
// old maxFrame. A regression to liveMinFrame returns post-restart counters.
func TestSnapshotHeaderCounters_ImmuneToWALRestart(t *testing.T) {
	db := tempDB(t)

	// Build a WAL noticeably longer than the post-restart one so recycled
	// frame numbers land below the old maxFrame.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("a")
	require.NoError(t, err)
	val := make([]byte, 1024)
	for i := 0; i < 64; i++ {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "k%03d", i), val))
	}
	tx.MarkDataChanged()
	tx.MarkSchemaChanged()
	require.NoError(t, tx.Commit())

	// Backfill everything, keep the WAL: the next reader takes slot 0.
	require.NoError(t, db.Checkpoint(CheckpointFull))

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	require.Equal(t, 0, rtx.walSlot,
		"precondition: reader must hold a slot-0 snapshot (empty WAL range)")
	baseFCC := rtx.DiskFileChangeCounter()
	baseSC := rtx.DiskSchemaCookie()

	// Restart the WAL under the held reader (the reset waits only on slots
	// 1-4; slot 0 does not block it), then recycle low frame numbers.
	require.NoError(t, db.Checkpoint(CheckpointRestart))
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("b")
	require.NoError(t, err)
	tx.MarkDataChanged()
	tx.MarkSchemaChanged()
	require.NoError(t, tx.Commit())

	fcc, sc, err := rtx.SnapshotHeaderCounters()
	require.NoError(t, err)
	assert.Equal(t, baseSC, sc, "slot-0 snapshot must not see post-restart schema cookie")
	assert.Equal(t, baseFCC, fcc, "slot-0 snapshot must not see post-restart change counter")

	require.NoError(t, rtx.Rollback())
}
