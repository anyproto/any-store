package btree

// A commit or a begin that fails on page 1, then the retry. The failed
// transaction must leave nothing behind that the next one could serialize:
// not a header, not a page count, not a change counter. SQLite's
// readDbPage fills dbFileVers with 0xff after a failed read of page 1
// (pager.c:3047-3061); the next transaction reads page 1 again before any
// commit can use it.

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedPage1Frame = errors.New("injected page-1 read failure")

// failPage1ReadOnce makes the next read of page 1's newest WAL frame fail.
func failPage1ReadOnce(t *testing.T, db *DB) {
	t.Helper()
	w := db.pager.wal
	frame, err := w.index.get(1, w.nFrame.Load(), w.index.liveMinFrame())
	require.NoError(t, err)
	require.NotZero(t, frame, "setup: page 1 is in the WAL")
	armed := true
	setWalReadFrameFaultHook(func(f uint32) error {
		if armed && f == frame {
			armed = false
			return errInjectedPage1Frame
		}
		return nil
	})
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })
}

// page1FaultCheck checks the database after a failure and its retry: every
// row, the structure, and a commit that writes page 1 with the next counter.
func page1FaultCheck(t *testing.T, db *DB, path string, m restartMode, rows int) {
	t.Helper()
	restartCheck(t, db, 0, rows, 1)
	require.NoError(t, db.IntegrityCheck())
	fcc := db.pager.header.FileChangeCount
	restartPut(t, db, rows, rows+5, 1) // grows: writes page 1
	if db.pager.page1OnChange() {
		// The retry's begin read page 1 again: dbFileVers is the committed
		// counter, never the failed read's.
		require.Equal(t, fcc+1, db.pager.header.FileChangeCount)
	}
	restartRecoverCopy(t, path, "crash", m, func(cdb *DB) { restartCheck(t, cdb, 0, rows+5, 1) })
	db = restartReopen(t, db, path, m)
	restartCheck(t, db, 0, rows+5, 1)
	require.NoError(t, db.IntegrityCheck())
}

func TestPage1Rule_FailedCommitThenRetry(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		t.Run("after-the-sync", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 50, 60, 9)
			failPublication(t)
			require.ErrorIs(t, wtx.Commit(), errInjectedPublish)
			testCommitPublishErrHook = nil
			page1FaultCheck(t, db, path, m, 50)
		})
		t.Run("page-1-load-in-the-commit", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 50, 60, 9) // grows: the commit needs page 1
			db.pager.writerCache.discard(1)
			failPage1ReadOnce(t, db)
			require.ErrorIs(t, wtx.Commit(), errInjectedPage1Frame)
			setWalReadFrameFaultHook(nil)
			page1FaultCheck(t, db, path, m, 50)
		})
		t.Run("page-1-load-at-the-begin", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			db.pager.writerCache.discard(1)
			failPage1ReadOnce(t, db)
			_, err := db.BeginWrite()
			require.ErrorIs(t, err, errInjectedPage1Frame)
			setWalReadFrameFaultHook(nil)
			page1FaultCheck(t, db, path, m, 50)
		})
	})
}
