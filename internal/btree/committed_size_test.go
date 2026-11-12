package btree

// The committed size goes with the committed frame. SQLite moves the two
// together in its private WAL header, only once a commit's frames are written
// and synced (wal.c:4238-4250), and the pager counts pages from that header
// (sqlite3WalDbsize, wal.c:3669-3674; pagerPagecount, pager.c:3292). A commit
// that fails before it is published leaves both as they were, and a reader's
// page count is the one of the commit its snapshot ends at.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedPublish = errors.New("injected failure before the commit is published")

const (
	committedSizeRows = 1200 // about 100 pages of 4 KiB
	committedSizeSrc  = 10
)

// committedNPage is the size half of wi's committed position.
func committedNPage(wi *walIndex) uint32 {
	_, n := wi.mxCommitFrame.LoadPos()
	return n
}

// committedSizeShrinkFixture opens a database of about 100 pages whose last
// commits are still only in the WAL, and a small source to back up over it.
func committedSizeShrinkFixture(t *testing.T, m restartMode) (dst, src *DB, dstPath string) {
	t.Helper()
	dst, dstPath = openRestartDB(t, m)
	restartPut(t, dst, 0, committedSizeRows/2, 1)
	require.NoError(t, dst.Checkpoint(CheckpointPassive))
	restartPut(t, dst, committedSizeRows/2, committedSizeRows, 1)
	src, _ = openRestartDB(t, m)
	restartPut(t, src, 0, committedSizeSrc, 9)
	require.Greater(t, dst.pager.dbSize.Load(), 5*src.pager.dbSize.Load(), "setup: the backup shrinks the destination")
	return dst, src, dstPath
}

// failPublication makes the next commit fail after its frames are written
// and synced, before it is published.
func failPublication(t *testing.T) {
	t.Helper()
	armed := true
	testCommitPublishErrHook = func() error {
		if armed {
			armed = false
			return errInjectedPublish
		}
		return nil
	}
	t.Cleanup(func() { testCommitPublishErrHook = nil })
}

// shrinkingBackupFails backs src up over dst and fails the destination's
// commit before it is published.
func shrinkingBackupFails(t *testing.T, dst, src *DB) {
	t.Helper()
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
	failPublication(t)
	require.ErrorIs(t, b.Finish(), errInjectedPublish)
	testCommitPublishErrHook = nil
}

// TestCommittedSize_ShrinkFailsBeforePublication: a shrinking commit that
// fails once its frames are synced must leave the committed size alone. With
// the failed commit's size kept, a TRUNCATE checkpoint skipped the pages above
// it as orphans, truncated the database file to it and reset the WAL that held
// them: acknowledged pages lost.
func TestCommittedSize_ShrinkFailsBeforePublication(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		t.Run("retry", func(t *testing.T) {
			dst, src, dstPath := committedSizeShrinkFixture(t, m)
			shrinkingBackupFails(t, dst, src)
			restartCheck(t, dst, 0, committedSizeRows, 1)

			restartPut(t, dst, 0, 3, 2)
			restartCheck(t, dst, 0, 3, 2)
			restartCheck(t, dst, 3, committedSizeRows, 1)
			require.NoError(t, dst.Checkpoint(CheckpointTruncate))
			dst = restartReopen(t, dst, dstPath, m)
			restartCheck(t, dst, 0, 3, 2)
			restartCheck(t, dst, 3, committedSizeRows, 1)
			require.NoError(t, dst.IntegrityCheck())
		})
		t.Run("reopen", func(t *testing.T) {
			dst, src, dstPath := committedSizeShrinkFixture(t, m)
			shrinkingBackupFails(t, dst, src)
			require.NoError(t, dst.IntegrityCheck())
			dst = restartReopen(t, dst, dstPath, m) // Close's own checkpoint only
			restartCheck(t, dst, 0, committedSizeRows, 1)
			require.NoError(t, dst.IntegrityCheck())
		})
		t.Run("truncate", func(t *testing.T) {
			dst, src, dstPath := committedSizeShrinkFixture(t, m)
			shrinkingBackupFails(t, dst, src)
			require.NoError(t, dst.Checkpoint(CheckpointTruncate))
			restartCheck(t, dst, 0, committedSizeRows, 1)
			dst = restartReopen(t, dst, dstPath, m)
			restartCheck(t, dst, 0, committedSizeRows, 1)
			require.NoError(t, dst.IntegrityCheck())
		})
	})
}

// TestCommittedSize_ReaderDuringUncommittedShrink: a reader's page count is
// its snapshot's, not the size an open write transaction has shrunk the
// database to. Read during the shrink, after it is rolled back, and after it
// commits.
func TestCommittedSize_ReaderDuringUncommittedShrink(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		dst, src, _ := committedSizeShrinkFixture(t, m)

		b, err := dst.BackupInit(src)
		require.NoError(t, err)
		require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
		require.Less(t, dst.pager.dbSize.Load(), uint32(committedSizeRows/20), "setup: the shrink is in the open transaction")
		restartCheck(t, dst, 0, committedSizeRows, 1)
		failPublication(t)
		require.ErrorIs(t, b.Finish(), errInjectedPublish)
		testCommitPublishErrHook = nil
		restartCheck(t, dst, 0, committedSizeRows, 1)

		b, err = dst.BackupInit(src)
		require.NoError(t, err)
		require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
		rtx, err := dst.BeginRead() // the committed 100 pages, held across the commit
		require.NoError(t, err)
		heldErr := func() error {
			defer func() { _ = rtx.Rollback() }()
			if err := b.Finish(); err != nil {
				return err
			}
			ns, err := rtx.GetNamespace("t1")
			if err != nil {
				return err
			}
			for _, i := range []int{0, committedSizeRows / 2, committedSizeRows - 1} {
				if _, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i))); err != nil {
					return fmt.Errorf("row %d of the held snapshot: %w", i, err)
				}
			}
			return nil
		}()
		require.NoError(t, heldErr)
		restartCheck(t, dst, 0, committedSizeSrc, 9)
		require.NoError(t, dst.IntegrityCheck())
	})
}

// TestCommittedSize_NewDatabase: a database that was just created has a
// committed size of one page, page 1, before its first commit (newDatabase
// sets nPage = 1, btree.c:3540-3541). Its first read, its first allocation,
// a first transaction that rolls back and its first commit all start from
// it.
func TestCommittedSize_NewDatabase(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, path := openRestartDB(t, m)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		bound, snapNPage := db.pager.readerDbSizeBound(rtx.cache), rtx.walHdr.nPage
		require.NoError(t, rtx.Rollback())
		require.Equal(t, uint32(1), bound, "a new database is page 1")
		if m.opts.InProcess || m.opts.InMemory {
			require.Equal(t, uint32(1), snapNPage, "the snapshot carries the committed size")
		}

		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := wtx.CreateNamespace("t1")
		require.NoError(t, wtx.Rollback())
		require.NoError(t, err)
		require.NotEqual(t, uint32(1), ns.RootPage(), "the first allocation is not page 1")

		restartPut(t, db, 0, 50, 1)
		restartCheck(t, db, 0, 50, 1)
		require.NoError(t, db.IntegrityCheck())
		db = restartReopen(t, db, path, m)
		restartCheck(t, db, 0, 50, 1)
		require.NoError(t, db.IntegrityCheck())
	})
}

// heldRead reads rows of the committed 100 pages through rtx, which began
// before the shrinks the caller runs in between, and ends it.
func heldRead(rtx *ReadTx, between func() error) error {
	defer func() { _ = rtx.Rollback() }()
	if err := between(); err != nil {
		return err
	}
	ns, err := rtx.GetNamespace("t1")
	if err != nil {
		return err
	}
	for i := 0; i < committedSizeRows; i++ {
		v, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
		if err != nil {
			return fmt.Errorf("row %d of the held snapshot: %w", i, err)
		}
		if len(v) != 300 || v[0] != 1 {
			return fmt.Errorf("row %d of the held snapshot is not its own", i)
		}
	}
	return nil
}

// TestCommittedSize_ReaderHeldThroughFailedShrink: a reader of the committed
// 100 pages keeps its snapshot through a shrinking commit that fails, an
// integrity check right after it, and the commit that then succeeds. A
// backup's destination cannot have a transaction open when the backup starts
// (BackupInit, as sqlite3_backup_init), so the reader begins once the pages
// are copied, and the commit after the failure is an ordinary one.
func TestCommittedSize_ReaderHeldThroughFailedShrink(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		dst, src, _ := committedSizeShrinkFixture(t, m)
		b, err := dst.BackupInit(src)
		require.NoError(t, err)
		require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
		rtx, err := dst.BeginRead()
		require.NoError(t, err)
		require.NoError(t, heldRead(rtx, func() error {
			failPublication(t)
			if err := b.Finish(); !errors.Is(err, errInjectedPublish) {
				return fmt.Errorf("the shrinking commit: %v", err)
			}
			testCommitPublishErrHook = nil
			if err := dst.IntegrityCheck(); err != nil {
				return err
			}
			wtx, err := dst.BeginWrite()
			if err != nil {
				return err
			}
			restartPutTx(t, wtx, committedSizeRows, committedSizeRows+3, 2)
			return wtx.Commit()
		}))
		restartCheck(t, dst, 0, committedSizeRows, 1)
		restartCheck(t, dst, committedSizeRows, committedSizeRows+3, 2)
		require.NoError(t, dst.IntegrityCheck())

		b, err = dst.BackupInit(src)
		require.NoError(t, err)
		require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
		require.NoError(t, b.Finish())
		restartCheck(t, dst, 0, committedSizeSrc, 9)
		require.NoError(t, dst.IntegrityCheck())
	})
}

// TestCommittedSize_BackupCopiesTheLogicalSize: a backup copies the pages of
// the source's logical size, page 1's when it is trusted, not every page of
// its file. sqlite3_backup_step takes sqlite3BtreeLastPage (backup.c:388),
// which is lockBtree's count (btree.c:2364-2369, 3294-3298). A file with a
// trailing page past that size, and no commit in its WAL, gave the pager's
// count instead, and the destination held a page nothing uses.
func TestCommittedSize_BackupCopiesTheLogicalSize(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, m := range restartModes[:2] { // a file to append to
			name := m.name + "/format6"
			if legacy {
				name = m.name + "/format5"
			}
			t.Run(name, func(t *testing.T) {
				prev := createLegacyFormat.Load()
				SetCreateLegacyFormatForTest(legacy)
				defer SetCreateLegacyFormatForTest(prev)

				srcPath := filepath.Join(t.TempDir(), "src.db")
				src, err := testOpen(t, srcPath, m.opts)
				require.NoError(t, err)
				restartPut(t, src, 0, 1, 7)
				require.NoError(t, src.Checkpoint(CheckpointTruncate))
				logical := src.pager.dbSize.Load()
				require.NoError(t, src.Close())
				f, err := os.OpenFile(srcPath, os.O_WRONLY|os.O_APPEND, 0)
				require.NoError(t, err)
				_, err = f.Write(make([]byte, m.opts.PageSize))
				require.NoError(t, err)
				require.NoError(t, f.Close())

				src, err = testOpen(t, srcPath, m.opts)
				require.NoError(t, err)
				t.Cleanup(func() { _ = src.Close() })
				require.Equal(t, logical, src.pager.dbSize.Load(), "setup: page 1's size is trusted")

				dst, _ := openRestartDB(t, m)
				b, err := dst.BackupInit(src)
				require.NoError(t, err)
				require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
				require.NoError(t, b.Finish())
				require.Equal(t, logical, dst.pager.dbSize.Load(), "the destination has the source's logical size")
				require.NoError(t, dst.IntegrityCheck())
				restartCheck(t, dst, 0, 1, 7)
			})
		}
	}
}
