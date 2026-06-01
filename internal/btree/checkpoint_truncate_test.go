package btree

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// fileSize returns the physical on-disk size of the database file in bytes.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

// TestCheckpoint_PhysicalTruncateAfterShrink asserts that a full-backfill
// checkpoint physically shrinks the DB file down to the committed page count
// after a shrinking commit (here driven by backup's truncateTo, the same path
// used by sqlite3PagerTruncateImage / VACUUM). Mirrors SQLite walCheckpoint
// (wal.c:2320-2329): when mxSafeFrame==hdr.mxFrame the DB file is truncated to
// hdr.nPage*szPage.
//
// Before the fix, checkpointWithMode only wrote backfilled pages via WriteAt +
// fdatasync and never called dbFile.Truncate, so the file only ever grew and
// this test failed (the file stayed at its pre-shrink size).
func TestCheckpoint_PhysicalTruncateAfterShrink(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	src, err := testOpen(t, srcPath, opts)
	require.NoError(t, err)
	defer func() { _ = src.Close() }()
	dst, err := testOpen(t, dstPath, opts)
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	// src: a tiny database.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	sns, err := stx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, stx.Put(sns, []byte("small"), []byte("v")))
	require.NoError(t, stx.Commit())

	// dst: a much larger database (many pages).
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	dns, err := dtx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, dtx.Commit())
	dtx2, err := dst.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 256)
	for i := 0; i < 800; i++ {
		require.NoError(t, dtx2.Put(dns, fmt.Appendf(nil, "dst-%05d", i), fat))
	}
	require.NoError(t, dtx2.Commit())

	// Checkpoint dst so the LARGE image is physically materialized on disk.
	require.NoError(t, dst.Checkpoint(CheckpointFull))
	bigPages := dst.DatabaseSize()
	pageSize := int64(dst.PageSize())
	bigFileSize := fileSize(t, dstPath)
	require.Equal(t, int64(bigPages)*pageSize, bigFileSize,
		"after checkpointing the large image the file should be bigPages*pageSize")
	require.Greater(t, bigPages, uint32(50), "test setup: dst must be many pages")

	// Backup the tiny src over the large dst. backup.finalize calls
	// pager.truncateTo(nSrcPage), committing a SHRINKING image to dst's WAL.
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

	smallPages := dst.DatabaseSize()
	require.Less(t, smallPages, bigPages, "backup must have shrunk the logical page count")

	// At this point the shrink lives in dst's WAL; the physical file is still
	// the large size (truncation is deferred to checkpoint).
	require.Equal(t, bigFileSize, fileSize(t, dstPath),
		"file should still be large before the post-shrink checkpoint")

	// Full-backfill checkpoint: must physically shrink the file to the
	// committed page count.
	require.NoError(t, dst.Checkpoint(CheckpointFull))

	wantSize := int64(smallPages) * pageSize
	gotSize := fileSize(t, dstPath)
	require.Equal(t, wantSize, gotSize,
		"checkpoint must physically truncate the DB file to smallPages*pageSize (SQLite wal.c:2320-2329)")
	require.Less(t, gotSize, bigFileSize, "file must be physically smaller after the shrinking checkpoint")
}

// TestCheckpoint_NoTruncateWithOlderSnapshotReader asserts that the
// full-backfill truncate is SKIPPED while a concurrent reader is pinned to an
// older (pre-shrink) snapshot, then performed once that reader closes.
//
// This is the concurrent-reader-safety edge case SQLite protects (wal.c:2229-2245
// + 2322): a reader holding readmark==its mxFrame lowers mxSafeFrame below the
// live mxFrame, so mxSafeFrame!=authoritativeMxFrame() at the truncate guard and
// the trailing pages the reader still reads from the DB file are preserved.
func TestCheckpoint_NoTruncateWithOlderSnapshotReader(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Grow to many pages.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 256)
	for i := 0; i < 800; i++ {
		require.NoError(t, tx2.Put(ns, fmt.Appendf(nil, "k-%05d", i), fat))
	}
	require.NoError(t, tx2.Commit())

	// Checkpoint so the large image is on disk, then write one more commit so
	// nBackfill < mxFrame and a new reader pins a REAL readmark slot (1-4)
	// rather than the slot-0 "read nothing from WAL" fast path.
	require.NoError(t, db.Checkpoint(CheckpointFull))
	bigPages := db.DatabaseSize()
	pageSize := int64(db.PageSize())
	bigFileSize := fileSize(t, path)
	require.Equal(t, int64(bigPages)*pageSize, bigFileSize)
	require.Greater(t, bigPages, uint32(50))

	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx3.Put(ns, []byte("marker"), []byte("1")))
	require.NoError(t, tx3.Commit())

	// Open a reader pinned to this PRE-shrink snapshot. It takes a readmark
	// slot at the current mxFrame.
	reader, err := db.BeginRead()
	require.NoError(t, err)
	readerOpen := true
	defer func() {
		if readerOpen {
			_ = reader.Rollback()
		}
	}()

	// Now commit a SHRINKING image via truncateTo. This drives the pager's
	// dbSize down and commits the smaller page-1 header to the WAL, advancing
	// mxFrame past the reader's pinned snapshot.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	smallPages := uint32(8)
	require.Less(t, smallPages, bigPages)
	require.NoError(t, db.pager.truncateTo(smallPages))
	require.NoError(t, wtx.Commit())
	require.Equal(t, smallPages, db.DatabaseSize())

	// Checkpoint while the older-snapshot reader is still open. The reader's
	// readmark lowers mxSafeFrame below the live mxFrame, so the truncate guard
	// (mxSafeFrame==authoritativeMxFrame()) is FALSE and the file is NOT shrunk:
	// the trailing pages the reader still reads must remain on disk.
	//
	// The non-PASSIVE checkpoint reports ErrBusy because the active reader
	// blocked a complete backfill (BUSY-means-retry, wal.c:2352-2356); the
	// data-path guard below (file must NOT shrink) is what this test verifies
	// and is unaffected by the error return.
	require.ErrorIs(t, db.Checkpoint(CheckpointFull), ErrBusy)
	require.Equal(t, bigFileSize, fileSize(t, path),
		"file must NOT shrink while an older-snapshot reader is open (concurrent-reader safety, wal.c:2322)")

	// Close the reader, then checkpoint again. Now mxSafeFrame can reach the
	// live mxFrame, the guard passes, and the file physically shrinks.
	require.NoError(t, reader.Rollback())
	readerOpen = false

	require.NoError(t, db.Checkpoint(CheckpointFull))
	wantSize := int64(smallPages) * pageSize
	require.Equal(t, wantSize, fileSize(t, path),
		"file must physically shrink to smallPages*pageSize once no older reader pins the old size")
	require.Less(t, fileSize(t, path), bigFileSize)
}
