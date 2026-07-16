//go:build (linux || darwin) && (amd64 || arm64)

package btree

// A cross-process reader's SHM read-mark pins its snapshot: no checkpoint may
// backfill the DB file past any live reader's mark (wal.c:367-370 — a mark is
// written only under that slot's exclusive lock). This test pins a read tx in
// the parent, lets a child process checkpoint / commit / checkpoint across the
// pinned frame, and asserts three things the mark protocol guarantees:
//
//  1. the parent's SHM mark survives the child's checkpoints verbatim,
//  2. nBackfill never advances past the pinned mark,
//  3. the pinned read tx still observes its own snapshot generation — with a
//     clobbered mark, the second checkpoint backfills the child's newer
//     versions into the DB file and the reader's page lookups (bounded below
//     by nBackfill+1 > its maxFrame) fall through to the DB file, serving
//     future bytes.

import (
	"bytes"
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// shmNBackfillAttemptedForTest reads nBackfillAttempted directly from SHM
// region 0 (there is no production single-word reader for it yet).
func shmNBackfillAttemptedForTest(wi *walIndex) uint32 {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return 0
	}
	return atomic.LoadUint32((*uint32)(unsafe.Pointer(&region[htNBackfillAttemptedOff])))
}

const (
	mpMarkNumDocs = 8
	mpMarkValSize = 32 * 1024 // overflow-chain values, several pages per doc
)

func mpMarkOpts() Options {
	return Options{
		PageSize:              4096,
		CacheSize:             2000,
		InProcess:             false,
		DisableAutoCheckpoint: true, // the child's two explicit checkpoints are the only ones
	}
}

func TestMultiProcessCheckpointPreservesPeerReadMark(t *testing.T) {
	dbPath := os.Getenv("TEST_MPREADMARK_DB_PATH")
	if dbPath != "" {
		mpMarkCheckpointerChild(t, dbPath)
		return
	}
	if testing.Short() {
		t.Skip("multi-process test skipped in -short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db, err := testOpen(t, path, mpMarkOpts())
	require.NoError(t, err)
	defer db.Close()

	// Generation 1: uniform fill=1 values, committed to the WAL only
	// (auto-checkpoint disabled), so a reader must pin WAL frames.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("docs")
	require.NoError(t, err)
	for i := 0; i < mpMarkNumDocs; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, bytes.Repeat([]byte{1}, mpMarkValSize)))
	}
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	// Pin the snapshot. nBackfill(0) < mxFrame forces a slot 1-4 claim whose
	// SHM mark is the pinned frame.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	pinned := rtx.WalMaxFrame()
	require.NotZero(t, pinned, "reader must pin a WAL frame for this test to bite")

	idx := db.pager.wal.index
	slot := 0
	for i := 1; i < 5; i++ {
		if idx.shmReadMark(i) == pinned {
			slot = i
			break
		}
	}
	require.NotZero(t, slot, "no reader slot carries the pinned frame %d", pinned)

	// Child: PASSIVE checkpoint, commit generation 2, PASSIVE checkpoint.
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestMultiProcessCheckpointPreservesPeerReadMark$",
		"-test.timeout=60s",
	)
	cmd.Env = append(os.Environ(), "TEST_MPREADMARK_DB_PATH="+path)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "checkpointer child failed:\n%s", out)

	// 1. The live reader's mark survived both checkpoints.
	if got := idx.shmReadMark(slot); got != pinned {
		t.Errorf("slot %d read-mark clobbered across peer checkpoints: got %#x want %d", slot, got, pinned)
	}
	// 2. The backfill frontier respected the pinned snapshot.
	if nb := idx.shmNBackfill(); nb > pinned {
		t.Errorf("nBackfill advanced past the live reader's mark: %d > %d", nb, pinned)
	}
	// 2b. The crash-safety hint tracked the checkpoints: checkpoint #1 stored
	// mxSafeFrame (== the pinned frame) before backfilling; checkpoint #2 had
	// nothing safe to backfill and left it. A stale value here (e.g. 0) means
	// the publish wrote without storing mxSafeFrame first.
	if nba := shmNBackfillAttemptedForTest(idx); nba != pinned {
		t.Errorf("nBackfillAttempted not tracking checkpoints: got %d want %d", nba, pinned)
	}
	// 3. The pinned tx still reads its own generation.
	for i := 0; i < mpMarkNumDocs; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val, gerr := rtx.Get(ns, key)
		require.NoError(t, gerr, "doc %d", i)
		require.Len(t, val, mpMarkValSize, "doc %d", i)
		for j := range val {
			if val[j] != 1 {
				t.Fatalf("snapshot isolation violated: doc %d byte %d = %d (future generation served from DB file)", i, j, val[j])
			}
		}
	}

	// Sanity: a FRESH read tx must see generation 2 — the child really committed.
	require.NoError(t, rtx.Rollback())
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx2.Rollback()
	val, err := rtx2.Get(ns, binary.BigEndian.AppendUint32(nil, 0))
	require.NoError(t, err)
	require.Equal(t, byte(2), val[0], "fresh reader must see the child's generation")
}

// mpMarkCheckpointerChild is the peer process: checkpoint #1 backfills the
// parent's pinned generation (legal — up to the mark), then a commit past the
// mark and checkpoint #2, which must stop at the parent's mark.
func mpMarkCheckpointerChild(t *testing.T, dbPath string) {
	db, err := testOpen(t, dbPath, mpMarkOpts())
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Checkpoint(CheckpointPassive))

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("docs")
	require.NoError(t, err)
	for i := 0; i < mpMarkNumDocs; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, bytes.Repeat([]byte{2}, mpMarkValSize)))
	}
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	require.NoError(t, db.Checkpoint(CheckpointPassive))
}
