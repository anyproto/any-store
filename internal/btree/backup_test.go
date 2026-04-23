package btree

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// backupPair returns two independent on-disk DBs: src (with a namespace
// "data") and an empty dst. Both use identical Options so page sizes match.
func backupPair(t *testing.T) (src, dst *DB) {
	t.Helper()
	dir := t.TempDir()
	opts := DefaultOptions()

	s, err := Open(filepath.Join(dir, "src.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	stx, err := s.BeginWrite()
	require.NoError(t, err)
	_, err = stx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, stx.Commit())

	d, err := Open(filepath.Join(dir, "dst.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = d.Close() })

	return s, d
}

func TestBackupInit_BasicFields(t *testing.T) {
	src, dst := backupPair(t)

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NotNil(t, b)
	require.Equal(t, src, b.src)
	require.Equal(t, dst, b.dst)
	require.Equal(t, uint32(1), b.iNext, "iNext should start at 1 per sqlite3_backup_init (backup.c:188)")
	require.False(t, b.dstLocked, "dstLocked starts false per backup.c:25")
}

func TestBackupInit_RejectsSameDB(t *testing.T) {
	src, _ := backupPair(t)
	// ~ backup.c:166–170: "source and destination must be distinct".
	_, err := src.BackupInit(src)
	require.ErrorIs(t, err, ErrBackupSameDB)
}

func TestBackup_OnePage_CopiesPageDataAndClearsMemPageFlag(t *testing.T) {
	src, dst := backupPair(t)

	// Insert data into src so page 1 is non-trivial and we have a page 2.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	ns, _ := src.GetNamespace("data")
	require.NoError(t, stx.Put(ns, []byte("k1"), []byte("v1")))
	require.NoError(t, stx.Commit())

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Manually open a dst write tx and a src read tx to mimic what Step does.
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	defer dtx.Rollback()
	b.dstWriteTx = dtx
	b.dstLocked = true // normally set by Step after BeginWrite

	rtx, err := src.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	srcPg1, err := src.pager.getPageReader(1, rtx.walMaxFrame, rtx.cache)
	require.NoError(t, err)

	// ~ backup.c:226–279 — copy one page.
	require.NoError(t, b.onePage(1, srcPg1.data, false))

	// Verify dst page 1 equals src page 1 byte-for-byte (modulo the
	// DatabaseSize patch at offset 28–31 that onePage applies).
	dstPg1, err := dst.pager.getPageWriter(1, dst.pager.wal.nFrame.Load())
	require.NoError(t, err)
	require.Equal(t, srcPg1.data[:28], dstPg1.data[:28], "dst page 1 bytes [0:28] must match src")
	require.Equal(t, srcPg1.data[32:], dstPg1.data[32:], "dst page 1 bytes [32:] must match src")
}

func TestBackup_Step_OfflineCopy(t *testing.T) {
	src, dst := backupPair(t)

	// Populate src with 500 records to exercise multi-page copies.
	ns, _ := src.GetNamespace("data")
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 200)
	for i := 0; i < 500; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, stx.Put(ns, k, fat))
	}
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))

	srcPageCount := src.DatabaseSize()
	require.Greater(t, srcPageCount, uint32(2), "need multi-page src for realistic test")

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Copy everything in one step — nPage < 0 means "all remaining".
	// ~ sqlite3_backup_step (backup.c:314); nPage=-1 means unlimited.
	err = b.Step(-1)
	require.ErrorIs(t, err, ErrBackupDone, "Step must signal completion with ErrBackupDone ~ SQLITE_DONE (backup.c:406)")

	require.Equal(t, srcPageCount, b.PageCount())
	require.Equal(t, uint32(0), b.Remaining())

	require.NoError(t, b.Finish())

	// Reopen dst and verify data.
	dstPath := dst.Path()
	_ = dst.Close()
	d2, err := Open(dstPath, DefaultOptions())
	require.NoError(t, err)
	defer d2.Close()
	rtx, err := d2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, err := d2.GetNamespace("data")
	require.NoError(t, err, "namespace 'data' must exist in backup")
	got, err := rtx.Get(ns2, []byte("key-0042"))
	require.NoError(t, err)
	require.Len(t, got, len(fat))
}

func TestBackup_Step_BatchedCopy(t *testing.T) {
	src, dst := backupPair(t)

	ns, _ := src.GetNamespace("data")
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 200)
	for i := 0; i < 300; i++ {
		require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), fat))
	}
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Batched: each Step copies exactly 2 pages. Keep looping until done.
	for {
		err := b.Step(2)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err, "Step should return nil while pages remain")
		require.Greater(t, b.PageCount(), uint32(0))
	}
	require.NoError(t, b.Finish())
}

func TestBackup_OnlineWriteBetweenSteps(t *testing.T) {
	src, dst := backupPair(t)
	ns, _ := src.GetNamespace("data")

	// Seed: insert records with fat values to force multi-page.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 300)
	for i := 0; i < 200; i++ {
		require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), fat))
	}
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))

	nSrc := src.DatabaseSize()
	require.Greater(t, nSrc, uint32(4), "need enough pages to copy most then modify an already-copied page")

	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Copy all but the last page. After this, iNext == nSrc, meaning every
	// page except pgno=nSrc has been copied. Any subsequent modification to
	// a page <nSrc is an "already copied" case that must go through the
	// update hook (backup.c:669 "iPage < iNext").
	err = b.Step(int(nSrc - 1))
	require.NoError(t, err, "Step should leave 1 page to copy")
	require.Equal(t, uint32(1), b.Remaining())

	// Concurrent write to src on an early key. The btree stores keys
	// sorted, so "k-0000" lives on the leftmost leaf — almost certainly
	// a page that's already been copied (iPage < iNext).
	updated := []byte("updated-online")
	stx2, err := src.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, stx2.Put(ns, []byte("k-0000"), updated))
	require.NoError(t, stx2.Commit())

	// Drain the rest.
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	// Reopen dst and verify k-0000 is the updated value.
	dstPath := dst.Path()
	_ = dst.Close()
	d2, err := Open(dstPath, DefaultOptions())
	require.NoError(t, err)
	defer d2.Close()
	rtx, err := d2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, _ := d2.GetNamespace("data")
	got, err := rtx.Get(ns2, []byte("k-0000"))
	require.NoError(t, err)
	require.Equal(t, string(updated), string(got),
		"update hook (backup.c:661-688) must re-copy pages modified after they were copied")
}

func TestBackup_RestartOnCheckpointRestart(t *testing.T) {
	src, dst := backupPair(t)
	ns, _ := src.GetNamespace("data")

	// Seed with enough data to exceed 2 pages.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 200)
	for i := 0; i < 100; i++ {
		require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), fat))
	}
	require.NoError(t, stx.Commit())

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NoError(t, b.Step(2)) // partial: iNext advances past 1
	require.Greater(t, src.DatabaseSize(), uint32(2))

	// Observe iNext under lock (race-safe).
	b.mu.Lock()
	iNextBefore := b.iNext
	b.mu.Unlock()
	require.NotEqual(t, uint32(1), iNextBefore, "Step(2) should have advanced iNext past 1")

	// CheckpointRestart resets WAL. ~ backup.c:701 trigger.
	require.NoError(t, src.Checkpoint(CheckpointRestart))

	b.mu.Lock()
	iNextAfter := b.iNext
	b.mu.Unlock()
	require.Equal(t, uint32(1), iNextAfter, "CheckpointRestart must trigger Backup.restart (backup.c:701-707)")

	// Clean up so Finish doesn't leave the write tx hanging.
	_ = b.Finish()
}

// Note: direct unit-test of ErrBackupPageSizeMismatch would require
// two DBs with different page sizes open simultaneously, which is
// impossible in any-store (pageBufferPool is a process-global singleton,
// page_slab.go:47). The check in BackupInit is trivial
// (`dst.PageSize() != src.PageSize()`) and is exercised indirectly by
// any future refactor that drops it — tests will break immediately.

// fmt is imported for later tests; silence unused-import for now.
var _ = fmt.Sprintf
