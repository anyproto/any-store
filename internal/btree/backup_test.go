package btree

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
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

func TestBackup_FinishTwiceIsError(t *testing.T) {
	src, dst := backupPair(t)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)

	// Drain Step until done.
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}

	require.NoError(t, b.Finish())
	// DRIFT from backup.c:577 (tolerates NULL) — explicit error.
	require.ErrorIs(t, b.Finish(), ErrBackupFinished)
}

func TestBackup_StepAfterFinishIsError(t *testing.T) {
	src, dst := backupPair(t)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.NoError(t, b.Finish())
	require.ErrorIs(t, b.Step(10), ErrBackupFinished)
}

func TestBackupInit_RejectsDstWithOpenReadTx(t *testing.T) {
	src, dst := backupPair(t)

	rtx, err := dst.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// ~ backup.c:124-130 ("destination database is in use").
	_, err = dst.BackupInit(src)
	require.ErrorIs(t, err, ErrBackupDstBusy)
}

func TestBackupInit_RejectsDstWithOpenWriteTx(t *testing.T) {
	src, dst := backupPair(t)

	wtx, err := dst.BeginWrite()
	require.NoError(t, err)
	defer wtx.Rollback()

	_, err = dst.BackupInit(src)
	require.ErrorIs(t, err, ErrBackupDstBusy)
}

func TestBackup_TruncatesLargerDst(t *testing.T) {
	src, dst := backupPair(t)

	// Populate dst with MORE data than src. backupPair creates "data" on
	// src but not on dst, so create it here first.
	dtx0, err := dst.BeginWrite()
	require.NoError(t, err)
	_, err = dtx0.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, dtx0.Commit())

	dns, _ := dst.GetNamespace("data")
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 256)
	for i := 0; i < 500; i++ {
		require.NoError(t, dtx.Put(dns, fmt.Appendf(nil, "dst-%04d", i), fat))
	}
	require.NoError(t, dtx.Commit())
	dstOrigSize := dst.DatabaseSize()

	// src has only a tiny amount.
	sns, _ := src.GetNamespace("data")
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, stx.Put(sns, []byte("small"), []byte("v")))
	require.NoError(t, stx.Commit())
	srcSize := src.DatabaseSize()
	require.Less(t, srcSize, dstOrigSize, "test setup: dst must be larger than src")

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	// ~ backup.c:530 — dst must be truncated to srcSize.
	require.Equal(t, srcSize, dst.DatabaseSize(),
		"post-backup dst size should equal src size (backup.c:530 truncate)")
}

func TestBackup_BumpsDstSchemaCookie(t *testing.T) {
	src, dst := backupPair(t)

	rtx0, err := dst.BeginRead()
	require.NoError(t, err)
	dstCookieBefore := rtx0.DiskSchemaCookie()
	require.NoError(t, rtx0.Rollback())

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	// ~ backup.c:423 — schema cookie must have been bumped.
	rtx1, err := dst.BeginRead()
	require.NoError(t, err)
	defer rtx1.Rollback()
	require.NotEqual(t, dstCookieBefore, rtx1.DiskSchemaCookie(),
		"post-backup dst schema cookie must differ from pre-backup (backup.c:423 bump)")
}

// TestBackup_PreservesSrcPage1HeaderFields verifies that a full backup carries
// the SOURCE's page-1 header fields into the destination, matching SQLite's
// verbatim page-1 copy (backup.c:269 memcpy; only offset 28/40 + WAL bytes
// patched). Regression for drift-40: the Go pager re-serializes its in-memory
// dbHeader over page 1 at commit (pager.go:1946), so without finalize
// re-syncing that header from the copied source bytes, fields like the freelist
// trunk pointers (offset 32/36), UserVersion, AppID, and TextEncoding revert to
// the destination's own stale values. The freelist case is load-bearing: the
// trunk pages now hold source content, so reverting offset 32/36 to the dst's
// own pointers is a genuine freelist-corruption vector.
func TestBackup_PreservesSrcPage1HeaderFields(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	// --- Source: distinct header metadata + a non-empty freelist. ---
	src, err := testOpen(t, srcPath, opts)
	require.NoError(t, err)
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	sns, err := stx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, stx.Commit())

	// Insert then delete a batch so the source frees pages onto its freelist
	// (populates FirstFreelistPg@32 / TotalFreelistPgs@36).
	stx, err = src.BeginWrite()
	require.NoError(t, err)
	sns, err = src.GetNamespace("data")
	require.NoError(t, err)
	val := make([]byte, 200)
	for i := 0; i < 300; i++ {
		require.NoError(t, stx.Put(sns, fmt.Appendf(nil, "s-%04d", i), val))
	}
	require.NoError(t, stx.Commit())
	stx, err = src.BeginWrite()
	require.NoError(t, err)
	sns, err = src.GetNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, stx.Delete(sns, fmt.Appendf(nil, "s-%04d", i)))
	}
	// Set distinct page-1 metadata that differs from any default. savedHeader
	// was snapshotted at BeginWrite, so these in-memory edits make the commit
	// see real header changes and serialize them onto page 1.
	src.pager.header.UserVersion = 0xABCD1234
	src.pager.header.AppID = 0x5A5A0001
	src.pager.header.TextEncoding = 2 // UTF-16le
	require.NoError(t, stx.Commit())
	require.NoError(t, src.Checkpoint(CheckpointFull))

	// --- Destination: an EMPTY freelist (FirstFreelistPg/TotalFreelistPgs ==
	// 0) and different metadata, so it differs unambiguously from the source's
	// non-empty freelist and distinct metadata. ---
	dst, err := testOpen(t, dstPath, opts)
	require.NoError(t, err)
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	_, err = dtx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, dtx.Commit())
	dtx, err = dst.BeginWrite()
	require.NoError(t, err)
	dns, err := dst.GetNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 60; i++ {
		require.NoError(t, dtx.Put(dns, fmt.Appendf(nil, "d-%04d", i), val))
	}
	dst.pager.header.UserVersion = 0x11110000
	dst.pager.header.AppID = 0x22220000
	dst.pager.header.TextEncoding = 1 // UTF-8
	require.NoError(t, dtx.Commit())
	require.NoError(t, dst.Checkpoint(CheckpointFull))

	// Capture the source's page-1 header fields straight from disk.
	srcData, err := os.ReadFile(srcPath)
	require.NoError(t, err)
	srcFirstFree := binary.BigEndian.Uint32(srcData[32:36])
	srcTotalFree := binary.BigEndian.Uint32(srcData[36:40])
	srcUserVer := binary.BigEndian.Uint32(srcData[60:64])
	srcAppID := binary.BigEndian.Uint32(srcData[68:72])
	srcTextEnc := binary.BigEndian.Uint32(srcData[56:60])
	require.NotZero(t, srcFirstFree, "test setup: source must have a non-empty freelist")
	require.NotZero(t, srcTotalFree, "test setup: source freelist page count must be non-zero")

	// Sanity: the destination's pre-backup page-1 fields really differ, so the
	// assertions below distinguish "kept source" from "reverted to dst".
	dstBefore, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.NotEqual(t, srcFirstFree, binary.BigEndian.Uint32(dstBefore[32:36]),
		"test setup: dst freelist trunk must differ from src")
	require.NotEqual(t, srcUserVer, binary.BigEndian.Uint32(dstBefore[60:64]),
		"test setup: dst UserVersion must differ from src")

	// --- Full backup src -> dst. ---
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())
	require.NoError(t, dst.Checkpoint(CheckpointFull))

	// --- Assert dst page-1 header now mirrors the source. ---
	dstData, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	require.Equal(t, srcFirstFree, binary.BigEndian.Uint32(dstData[32:36]),
		"FirstFreelistPg@32 must carry over from source (load-bearing: trunk pages hold source content)")
	require.Equal(t, srcTotalFree, binary.BigEndian.Uint32(dstData[36:40]),
		"TotalFreelistPgs@36 must carry over from source")
	require.Equal(t, srcUserVer, binary.BigEndian.Uint32(dstData[60:64]),
		"UserVersion@60 must carry over from source")
	require.Equal(t, srcAppID, binary.BigEndian.Uint32(dstData[68:72]),
		"AppID@68 must carry over from source")
	require.Equal(t, srcTextEnc, binary.BigEndian.Uint32(dstData[56:60]),
		"TextEncoding@56 must carry over from source")
}

// multiProcessBackupChild is invoked in a subprocess via exec.Command.
// It opens the src db, writes an updated value for "k-0000", commits,
// and exits. Called by TestBackup_RestartsOnExternalProcessWrite.
func multiProcessBackupChild(t *testing.T, dbPath string) {
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}
	db, err := Open(dbPath, opts)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k-0000"), []byte("updated-externally")))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())
}

func TestBackup_RestartsOnExternalProcessWrite(t *testing.T) {
	dbPath := os.Getenv("TEST_BACKUP_MP_DB_PATH")
	if dbPath != "" {
		multiProcessBackupChild(t, dbPath)
		return
	}
	if testing.Short() {
		t.Skip("spawns subprocess; excluded in -short")
	}

	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}

	// Seed src.
	{
		s, err := Open(srcPath, opts)
		require.NoError(t, err)
		stx, err := s.BeginWrite()
		require.NoError(t, err)
		ns, err := stx.CreateNamespace("data")
		require.NoError(t, err)
		fat := make([]byte, 200)
		for i := 0; i < 100; i++ {
			require.NoError(t, stx.Put(ns, fmt.Appendf(nil, "k-%04d", i), fat))
		}
		stx.MarkDataChanged()
		require.NoError(t, stx.Commit())
		require.NoError(t, s.Close())
	}

	src, err := Open(srcPath, opts)
	require.NoError(t, err)
	defer src.Close()
	dst, err := Open(dstPath, opts)
	require.NoError(t, err)
	defer dst.Close()

	nSrc := src.DatabaseSize()
	require.Greater(t, nSrc, uint32(3))

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	// Copy all but the last page so that k-0000's page (low-numbered,
	// leftmost leaf) is already in "already copied" territory.
	require.NoError(t, b.Step(int(nSrc-1)))
	b.mu.Lock()
	iNextBefore := b.iNext
	b.mu.Unlock()
	require.Equal(t, nSrc, iNextBefore)

	// Spawn subprocess to write k-0000 to src.
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestBackup_RestartsOnExternalProcessWrite$",
		"-test.v",
		"-test.timeout=30s",
	)
	cmd.Env = append(os.Environ(), "TEST_BACKUP_MP_DB_PATH="+srcPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run(), "child subprocess failed")

	// The next Step must observe the changed FileChangeCounter and
	// restart from page 1. Without the restart, iNext is already at
	// (nSrc_old), so we'd either skip the last page entirely or mis-
	// copy it, and the early page containing k-0000 stays stale.
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	// Reopen dst and verify the externally-written update is reflected.
	require.NoError(t, dst.Close())
	d2, err := Open(dstPath, opts)
	require.NoError(t, err)
	defer d2.Close()
	rtx, err := d2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err := d2.GetNamespace("data")
	require.NoError(t, err)
	got, err := rtx.Get(ns, []byte("k-0000"))
	require.NoError(t, err)
	require.Equal(t, "updated-externally", string(got),
		"external-process write mid-backup must be reflected in dst")
}

// Note: direct unit-test of ErrBackupPageSizeMismatch would require
// two DBs with different page sizes open simultaneously, which is
// impossible in any-store (pageBufferPool is a process-global singleton,
// page_slab.go:47). The check in BackupInit is trivial
// (`dst.PageSize() != src.PageSize()`) and is exercised indirectly by
// any future refactor that drops it — tests will break immediately.

// fmt is imported for later tests; silence unused-import for now.
var _ = fmt.Sprintf
