package btree

// The WAL is recycled as in SQLite: a RESTART checkpoint only waits for the
// readers (wal.c:2357-2362, 2380), a TRUNCATE checkpoint resets the wal-index
// header and truncates the file (wal.c:2363-2378), and the next writer that
// finds the whole log backfilled and no reader in it starts the log over
// before its first frame (walRestartLog, wal.c:3870-3888, called from
// walFrames at wal.c:4070).
//
// Each test runs in the three WAL modes: shared memory (multi-process), heap
// shm with a WAL file (in-process), and no files at all (in-memory).

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type restartMode struct {
	name string
	opts Options
}

var restartModes = []restartMode{
	{"multi-process", Options{PageSize: 4096, CacheSize: 100, DisableAutoCheckpoint: true}},
	{"in-process", Options{PageSize: 4096, CacheSize: 100, DisableAutoCheckpoint: true, InProcess: true}},
	{"in-memory", Options{PageSize: 4096, CacheSize: 100, DisableAutoCheckpoint: true, InMemory: true}},
}

func forEachRestartMode(t *testing.T, f func(t *testing.T, m restartMode)) {
	for _, m := range restartModes {
		t.Run(m.name, func(t *testing.T) { f(t, m) })
	}
}

func openRestartDB(t *testing.T, m restartMode) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, m.opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

// restartPutTx writes rows [from, to), each 300 bytes of fill, in wtx.
func restartPutTx(t *testing.T, wtx *WriteTx, from, to int, fill byte) {
	t.Helper()
	ns, err := wtx.GetNamespace("t1")
	if err != nil {
		ns, err = wtx.CreateNamespace("t1")
		require.NoError(t, err)
	}
	for i := from; i < to; i++ {
		require.NoError(t, wtx.Put(ns, binary.BigEndian.AppendUint32(nil, uint32(i)), bytes.Repeat([]byte{fill}, 300)))
	}
}

func restartPut(t *testing.T, db *DB, from, to int, fill byte) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	restartPutTx(t, wtx, from, to, fill)
	require.NoError(t, wtx.Commit())
}

func restartCheck(t *testing.T, db *DB, from, to int, fill byte) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := from; i < to; i++ {
		val, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
		require.NoError(t, err, "row %d", i)
		require.Equal(t, bytes.Repeat([]byte{fill}, 300), val, "row %d", i)
	}
}

// restartSpillAll writes every dirty page of the open write transaction to
// the WAL without committing, as the cache does under pressure. Page 1 is
// never spilled (pagerStress).
func restartSpillAll(t *testing.T, db *DB) {
	t.Helper()
	for _, pg := range db.pager.writerCache.dirtyPages() {
		if pg.pgno == 1 {
			continue
		}
		require.NoError(t, db.pager.pagerStress(pg))
	}
}

// restartReopen closes db and opens the same files again; in-memory mode has
// nothing to reopen and keeps the handle.
func restartReopen(t *testing.T, db *DB, path string, m restartMode) *DB {
	t.Helper()
	if m.opts.InMemory {
		return db
	}
	require.NoError(t, db.Close())
	db2, err := testOpen(t, path, m.opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db2.Close() })
	return db2
}

// restartCopyFiles copies the database and its WAL, without the shm, as a
// crash at this instant would leave them, and returns the copy's path.
func restartCopyFiles(t *testing.T, path, name string) string {
	t.Helper()
	dir := filepath.Join(filepath.Dir(path), name)
	require.NoError(t, os.MkdirAll(dir, 0o755))
	dst := filepath.Join(dir, "test.db")
	copyFileForTest(t, path, dst)
	copyFileForTest(t, path+"-wal", dst+"-wal")
	return dst
}

// restartRecoverCopy opens a crash copy of the files as they are now, so
// that the WAL is recovered from its bytes: an orderly Close would
// checkpoint and truncate it first, and a log whose frames chain from the
// wrong checksums would go unnoticed. Nothing to do in in-memory mode.
func restartRecoverCopy(t *testing.T, path, name string, m restartMode, check func(db *DB)) {
	t.Helper()
	if m.opts.InMemory {
		return
	}
	cdb, err := testOpen(t, restartCopyFiles(t, path, name), m.opts)
	require.NoError(t, err)
	defer func() { require.NoError(t, cdb.Close()) }()
	check(cdb)
	require.NoError(t, cdb.IntegrityCheck())
}

func restartWalSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path + "-wal")
	require.NoError(t, err)
	return info.Size()
}

// A RESTART checkpoint leaves the log where it is. The next writer starts it
// over: its frames go to the start of the log.
func TestWalWriterRestart_AfterFullBackfill(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		w := db.pager.wal
		n := w.index.mxCommitFrame.LoadLocal()
		require.Greater(t, n, uint32(4), "setup: several frames committed")

		require.NoError(t, db.Checkpoint(CheckpointRestart))
		require.Equal(t, n, w.index.mxCommitFrame.LoadLocal(), "a RESTART checkpoint does not reset the log")
		require.Equal(t, n, w.index.liveNBackfill(), "setup: the whole log is backfilled")

		restartPut(t, db, 0, 2, 2)
		after := w.index.mxCommitFrame.LoadLocal()
		require.Less(t, after, n, "the writer started the log over: its frames are numbered from 1")
		require.Equal(t, after, w.nFrame.Load())

		restartCheck(t, db, 0, 2, 2)
		restartCheck(t, db, 2, 60, 1)
		require.NoError(t, db.IntegrityCheck())

		db = restartReopen(t, db, path, m)
		restartCheck(t, db, 0, 2, 2)
		restartCheck(t, db, 2, 60, 1)
	})
}

// A writer does not start the log over while part of it is not in the
// database file yet.
func TestWalWriterRestart_NotBeforeBackfill(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, _ := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		n := db.pager.wal.index.mxCommitFrame.LoadLocal()
		restartPut(t, db, 0, 2, 2)
		require.Greater(t, db.pager.wal.index.mxCommitFrame.LoadLocal(), n, "frames are appended")
		restartCheck(t, db, 0, 2, 2)
		restartCheck(t, db, 2, 60, 1)
	})
}

// A reader that still uses the log keeps a writer from starting it over: the
// writer appends (wal.c:3889-3890).
func TestWalWriterRestart_AppendsBehindReader(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, _ := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		n := db.pager.wal.index.mxCommitFrame.LoadLocal()

		rtx, err := db.BeginRead() // a slot 1-4 reader: the log is not backfilled yet
		require.NoError(t, err)
		done := false
		t.Cleanup(func() { // before the Close registered earlier, which waits for readers
			if !done {
				_ = rtx.Rollback()
			}
		})
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		require.Equal(t, n, db.pager.wal.index.liveNBackfill(), "setup: the whole log is backfilled")

		restartPut(t, db, 0, 2, 2)
		require.Greater(t, db.pager.wal.index.mxCommitFrame.LoadLocal(), n, "frames are appended behind the reader")

		ns, err := rtx.GetNamespace("t1")
		require.NoError(t, err)
		val, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, 0))
		require.NoError(t, err)
		require.Equal(t, byte(1), val[0], "the reader keeps its snapshot")
		done = true
		require.NoError(t, rtx.Rollback())
		restartCheck(t, db, 0, 2, 2)
	})
}

// A TRUNCATE checkpoint resets the header and empties the WAL file; the reset
// header's first salt is the previous one plus one (walRestartHdr,
// wal.c:2152).
func TestWalCheckpointTruncate_ResetsAndIncrementsSalt(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		w := db.pager.wal
		salt1 := w.header.salt1

		require.NoError(t, db.Checkpoint(CheckpointTruncate))
		require.Equal(t, uint32(0), w.index.mxCommitFrame.LoadLocal())
		require.Equal(t, salt1+1, w.header.salt1, "salt1 is incremented by a reset")
		if !m.opts.InMemory {
			require.Equal(t, int64(0), restartWalSize(t, path))
		}
		restartPut(t, db, 0, 2, 2)
		restartCheck(t, db, 0, 2, 2)
		restartCheck(t, db, 2, 60, 1)
	})
}

// A TRUNCATE checkpoint with no committed frame still truncates a WAL file
// that holds a rolled-back transaction's frames (SQLite runs the reset block
// with mxFrame and nBackfill both 0, wal.c:2352-2380).
func TestWalCheckpointTruncate_ZeroCommittedFrames(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		if m.opts.InMemory {
			t.Skip("no WAL file")
		}
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		require.NoError(t, db.Checkpoint(CheckpointTruncate))
		require.Equal(t, int64(0), restartWalSize(t, path))

		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, 30, 2)
		restartSpillAll(t, db)
		require.NoError(t, wtx.Rollback())
		require.Greater(t, restartWalSize(t, path), int64(0), "setup: the rolled-back frames are in the file")
		require.Equal(t, uint32(0), db.pager.wal.index.mxCommitFrame.LoadLocal())

		require.NoError(t, db.Checkpoint(CheckpointTruncate))
		require.Equal(t, int64(0), restartWalSize(t, path))
		restartCheck(t, db, 0, 60, 1)
	})
}

// RESTART and TRUNCATE report busy when a reader keeps them from taking the
// reader locks (walBusyLock's result is the checkpoint's, wal.c:2361).
func TestWalCheckpoint_BusyBehindReader(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		for _, mode := range []CheckpointMode{CheckpointRestart, CheckpointTruncate} {
			db, _ := openRestartDB(t, m)
			restartPut(t, db, 0, 60, 1)
			n := db.pager.wal.index.mxCommitFrame.LoadLocal()
			rtx, err := db.BeginRead() // slot 1-4
			require.NoError(t, err)
			done := false
			t.Cleanup(func() { // before the Close registered earlier, which waits for readers
				if !done {
					_ = rtx.Rollback()
				}
			})
			require.NoError(t, db.Checkpoint(CheckpointPassive))
			require.Equal(t, n, db.pager.wal.index.liveNBackfill(), "setup: the whole log is backfilled")

			db.pager.wal.busyHandler = nil
			require.ErrorIs(t, db.Checkpoint(mode), ErrBusy, "mode %d", mode)
			require.Equal(t, n, db.pager.wal.index.mxCommitFrame.LoadLocal(), "nothing was reset")
			done = true
			require.NoError(t, rtx.Rollback())
			require.NoError(t, db.Checkpoint(mode))
			require.NoError(t, db.Close())
		}
	})
}

// After the log was started over inside a transaction, the transaction's
// frames are numbered below its starting position. A transaction whose
// changed pages were all spilled, with an unchanged database header, still
// has changes and must commit them.
func TestWalWriterRestart_AllSpilledCommit(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 200, 1)
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		n := db.pager.wal.index.mxCommitFrame.LoadLocal()
		require.Greater(t, n, uint32(8))

		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, 1, 2) // one existing row, same size: one leaf, header unchanged
		restartSpillAll(t, db)
		require.Zero(t, db.pager.writerCache.nDirty, "setup: everything spilled")
		require.Less(t, db.pager.wal.nFrame.Load(), n, "setup: the log was started over")
		require.True(t, wtx.HasChanges())
		require.NoError(t, wtx.Commit())

		restartCheck(t, db, 0, 1, 2)
		restartCheck(t, db, 1, 200, 1)
		restartRecoverCopy(t, path, "crash", m, func(cdb *DB) {
			restartCheck(t, cdb, 0, 1, 2)
			restartCheck(t, cdb, 1, 200, 1)
		})
		db = restartReopen(t, db, path, m)
		restartCheck(t, db, 0, 1, 2)
		restartCheck(t, db, 1, 200, 1)
		require.NoError(t, db.IntegrityCheck())
	})
}

// A transaction that started the log over and is rolled back leaves a log
// the next transaction can write and a later open can recover.
func TestWalWriterRestart_RollbackThenCommit(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		for _, spill := range []bool{false, true} {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 200, 1)
			require.NoError(t, db.Checkpoint(CheckpointPassive))

			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 0, 100, 2)
			if spill {
				restartSpillAll(t, db)
			}
			require.NoError(t, wtx.Rollback())
			restartCheck(t, db, 0, 200, 1)

			wtx, err = db.BeginWrite()
			require.NoError(t, err)
			require.False(t, wtx.HasChanges(), "a new transaction has no changes")
			require.NoError(t, wtx.Rollback())

			// The next transaction's frames chain from the new log's seed,
			// through a spill and the commit; recovery verifies the chain.
			wtx, err = db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 0, 3, 3)
			restartSpillAll(t, db)
			require.NoError(t, wtx.Commit())
			restartCheck(t, db, 0, 3, 3)
			restartCheck(t, db, 3, 200, 1)
			restartRecoverCopy(t, path, fmt.Sprintf("crash-%v", spill), m, func(cdb *DB) {
				restartCheck(t, cdb, 0, 3, 3)
				restartCheck(t, cdb, 3, 200, 1)
			})
			db = restartReopen(t, db, path, m)
			restartCheck(t, db, 0, 3, 3)
			restartCheck(t, db, 3, 200, 1)
			require.NoError(t, db.IntegrityCheck())
			require.NoError(t, db.Close())
		}
	})
}

// A savepoint taken before the log was started over is rolled back to the
// start of the new log, not to its old frame number (sqlite3WalSavepointUndo,
// wal.c:3829-3835).
func TestWalWriterRestart_SavepointRollback(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 200, 1)
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		n := db.pager.wal.index.mxCommitFrame.LoadLocal()

		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		sp, err := wtx.Savepoint()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, 100, 2)
		restartSpillAll(t, db)
		require.Less(t, db.pager.wal.nFrame.Load(), n, "setup: the log was started over")
		require.NoError(t, wtx.RollbackToSavepoint(sp))
		restartPutTx(t, wtx, 0, 3, 3)
		restartSpillAll(t, db)
		require.NoError(t, wtx.Commit())

		restartCheck(t, db, 0, 3, 3)
		restartCheck(t, db, 3, 200, 1)
		restartRecoverCopy(t, path, "crash", m, func(cdb *DB) {
			restartCheck(t, cdb, 0, 3, 3)
			restartCheck(t, cdb, 3, 200, 1)
		})
		db = restartReopen(t, db, path, m)
		restartCheck(t, db, 0, 3, 3)
		restartCheck(t, db, 3, 200, 1)
		require.NoError(t, db.IntegrityCheck())
	})
}

// With the automatic checkpoint, which is PASSIVE, the writers keep the log
// short by starting it over.
func TestWalWriterRestart_AutoCheckpointBoundsLog(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		opts := m.opts
		opts.DisableAutoCheckpoint = false
		opts.AutoCheckpointAfter = 20
		path := filepath.Join(t.TempDir(), "test.db")
		db, err := testOpen(t, path, opts)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		restartPut(t, db, 0, 100, 1)
		var maxFrames uint32
		for i := 0; i < 200; i++ {
			restartPut(t, db, i%100, i%100+1, byte(2+i%50))
			if f := db.pager.wal.nFrame.Load(); f > maxFrames {
				maxFrames = f
			}
		}
		require.Less(t, maxFrames, uint32(80), "the log is started over, not grown")
		w := db.pager.wal
		if m.opts.InMemory {
			// The frame arena is reused from its start by each restart.
			require.LessOrEqual(t, len(w.memFrames), 80)
			require.LessOrEqual(t, w.memArenaOff, 80*int(w.pageSize))
		} else {
			// The file is reused, not shrunk: it stays as long as the
			// longest log was, and no longer.
			frameSize := int64(walFrameSize) + int64(w.pageSize)
			size := restartWalSize(t, path)
			require.Greater(t, size, int64(0))
			require.LessOrEqual(t, size, int64(walHeaderSize)+80*frameSize, "the WAL file does not grow with the number of commits")
			// Only TRUNCATE gives the space back.
			require.NoError(t, db.Checkpoint(CheckpointTruncate))
			require.Equal(t, int64(0), restartWalSize(t, path))
		}
		require.NoError(t, db.IntegrityCheck())
	})
}

// A writer does not start the log over while a checkpoint runs, in this
// process or another: it takes the checkpoint lock without waiting and
// appends if it is busy (restartLogLocked's DRIFT). Any other error of that
// lock fails the write.
func TestWalWriterRestart_CheckpointLock(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, _ := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		w := db.pager.wal
		n := w.index.mxCommitFrame.LoadLocal()
		require.Equal(t, n, w.index.liveNBackfill(), "setup: the whole log is backfilled")
		salts := [2]uint32{w.header.salt1, w.header.salt2}

		// A checkpoint holds the lock; nothing else is in the way.
		require.NoError(t, w.index.lock(lockCheckpoint, lockExclusive))
		restartPut(t, db, 0, 2, 2)
		require.NoError(t, w.index.unlock(lockCheckpoint, lockExclusive))
		n2 := w.index.mxCommitFrame.LoadLocal()
		require.Greater(t, n2, n, "frames are appended while a checkpoint runs")
		require.Equal(t, salts, [2]uint32{w.header.salt1, w.header.salt2}, "the log was not started over")

		// With the lock free and the log backfilled again, the writer
		// starts it over.
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		restartPut(t, db, 0, 2, 3)
		require.Less(t, w.index.mxCommitFrame.LoadLocal(), n2, "the writer started the log over")
		require.Equal(t, salts[0]+1, w.header.salt1)

		// An error of the checkpoint lock that is not busy fails the write
		// and leaves the log as it was.
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		n3 := w.index.mxCommitFrame.LoadLocal()
		salt1 := w.header.salt1
		errLock := errors.New("injected checkpoint lock failure")
		orig := w.index.shm
		w.index.shm = &selectiveErrorLockShm{shm: orig, failSlot: lockCheckpoint, failLockType: lockExclusive, lockErr: errLock}
		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, 2, 4)
		err = wtx.Commit()
		w.index.shm = orig
		require.ErrorIs(t, err, errLock)
		require.Equal(t, n3, w.index.mxCommitFrame.LoadLocal())
		require.Equal(t, salt1, w.header.salt1)
		restartCheck(t, db, 0, 2, 3)

		restartPut(t, db, 0, 2, 5)
		restartCheck(t, db, 0, 2, 5)
		restartCheck(t, db, 2, 60, 1)
		require.NoError(t, db.IntegrityCheck())
	})
}

// An incremental backup has copied some pages and not others when a
// transaction of this process changes both and its changed pages are spilled
// before the commit. The spilled pages are not sent to the backup, so the
// backup must start over: its result is one committed state of the source,
// not old pages next to new ones. (The automatic checkpoint, which reset the
// WAL and restarted every backup with it, used to hide this.)
func TestBackup_SpilledCommitRestartsBackup(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		src, _ := openRestartDB(t, m)
		restartPut(t, src, 0, 400, 1)
		dstMode := restartModes[0]
		dst, dstPath := openRestartDB(t, dstMode)

		b, err := dst.BackupInit(src)
		require.NoError(t, err)
		require.NoError(t, b.Step(context.Background(), 4)) // some pages copied, most not
		require.Greater(t, src.DatabaseSize(), uint32(8))

		// Every row changes; no page of the change is left for the commit to
		// send. The transaction does not mark the data changed, so the
		// page-1 counter does not move either.
		wtx, err := src.BeginWrite()
		require.NoError(t, err)
		restartPutTx(t, wtx, 0, 400, 2)
		restartSpillAll(t, src)
		require.NoError(t, wtx.Commit())

		for {
			err := b.Step(context.Background(), 16)
			if errors.Is(err, ErrBackupDone) {
				break
			}
			require.NoError(t, err)
		}
		require.NoError(t, b.Finish())

		restartCheck(t, dst, 0, 400, 2)
		dst = restartReopen(t, dst, dstPath, dstMode)
		restartCheck(t, dst, 0, 400, 2)
		require.NoError(t, dst.IntegrityCheck())
	})
}

// The heap-shm read begin falls back to reader slot 0 with part of the log
// still to read when slots 1-4 are briefly held exclusive
// (tryBeginReadInProcessHdr). A writer that began that way holds none of
// slots 1-4, so nothing but the explicit nBackfill == mxFrame test keeps it
// from starting the log over and dropping the frames that are only in the
// log. (SQLite's read lock 0 implies a full backfill, wal.c:3128-3136, and
// walRestartLog only asserts it, wal.c:3872.)
func TestWalWriterRestart_NotFromSlot0Fallback(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		if !m.opts.InProcess && !m.opts.InMemory {
			t.Skip("shared memory grants slot 0 only after a full backfill")
		}
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 60, 1)
		w := db.pager.wal
		n1 := w.index.mxCommitFrame.LoadLocal()

		rtx, err := db.BeginRead() // pins n1
		require.NoError(t, err)
		restartPut(t, db, 60, 120, 2)
		n2 := w.index.mxCommitFrame.LoadLocal()
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		require.NoError(t, rtx.Rollback())
		require.Equal(t, n1, w.index.liveNBackfill(), "setup: the backfill stopped at the reader")

		// Slots 1-4 held exclusive for the length of the write begin, as a
		// checkpoint or another begin's claim holds them for an instant.
		for i := 1; i <= 4; i++ {
			require.NoError(t, w.index.lock(lockRead0+i, lockExclusive))
		}
		wtx, err := db.BeginWrite()
		for i := 1; i <= 4; i++ {
			require.NoError(t, w.index.unlock(lockRead0+i, lockExclusive))
		}
		require.NoError(t, err)
		require.Zero(t, wtx.walSlot, "setup: the writer's snapshot fell back to slot 0")

		restartPutTx(t, wtx, 0, 2, 3)
		require.NoError(t, wtx.Commit())
		require.Greater(t, w.index.mxCommitFrame.LoadLocal(), n2, "frames are appended: part of the log is not backfilled")

		restartCheck(t, db, 0, 2, 3)
		restartCheck(t, db, 2, 60, 1)
		restartCheck(t, db, 60, 120, 2)
		db = restartReopen(t, db, path, m)
		restartCheck(t, db, 0, 2, 3)
		restartCheck(t, db, 2, 60, 1)
		restartCheck(t, db, 60, 120, 2)
		require.NoError(t, db.IntegrityCheck())
	})
}

// Starting the log over reuses its storage and gives none back: after a long
// log, a short one leaves the WAL file at its old size (a frame of the new
// log overwrites the start of the old), and in in-memory mode the frames go
// into the arena the long log allocated.
func TestWalWriterRestart_ReusesStorage(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, path := openRestartDB(t, m)
		restartPut(t, db, 0, 400, 1)
		w := db.pager.wal
		n := w.index.mxCommitFrame.LoadLocal()
		var size int64
		var arena *byte
		if m.opts.InMemory {
			require.NotEmpty(t, w.memArena)
			arena = &w.memArena[0]
		} else {
			size = restartWalSize(t, path)
			require.Equal(t, int64(walHeaderSize)+int64(n)*(int64(walFrameSize)+int64(w.pageSize)), size)
		}
		require.NoError(t, db.Checkpoint(CheckpointPassive))

		restartPut(t, db, 0, 1, 2)
		require.Less(t, w.index.mxCommitFrame.LoadLocal(), n/4, "setup: a short log replaced the long one")
		if m.opts.InMemory {
			require.Same(t, arena, &w.memArena[0], "the arena is the one the long log allocated")
			require.LessOrEqual(t, w.memArenaOff, int(n/4)*int(w.pageSize), "and it is filled from its start")
		} else {
			require.Equal(t, size, restartWalSize(t, path), "the file is reused, not shrunk")
		}
		restartCheck(t, db, 0, 1, 2)
		restartCheck(t, db, 1, 400, 1)
	})
}
