package btree

// A commit or a begin that fails on page 1, then the retry. The failed
// transaction must leave nothing behind that the next one could serialize:
// not a header, not a page count, not a change counter. SQLite's
// readDbPage fills dbFileVers with 0xff after a failed read of page 1
// (pager.c:3047-3061); the next transaction reads page 1 again before any
// commit can use it.

import (
	"bytes"
	"encoding/binary"
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

// page1FaultState is what a failed transaction must leave as it was: the
// header, the page count and the committed position (frame and size).
type page1FaultState struct {
	header       dbHeader
	dbSize       uint32
	frame, nPage uint32
}

func page1FaultSnap(db *DB) page1FaultState {
	f, n := db.pager.wal.index.mxCommitFrame.LoadPos()
	return page1FaultState{header: db.pager.header, dbSize: db.pager.dbSize.Load(), frame: f, nPage: n}
}

// page1FaultRows reads every row of namespace t1, the whole key set.
func page1FaultRows(t *testing.T, db *DB) map[uint32][]byte {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	rows := map[uint32][]byte{}
	c := rtx.NewCursor(ns)
	defer c.Close()
	for err := c.First(); err == nil && c.Valid(); err = c.Next() {
		k, err := c.Key()
		require.NoError(t, err)
		v, err := c.Value()
		require.NoError(t, err)
		rows[binary.BigEndian.Uint32(k)] = append([]byte(nil), v...)
	}
	return rows
}

// page1FaultWant is rows 0..n-1, each 300 bytes of fill 1.
func page1FaultWant(n int) map[uint32][]byte {
	want := map[uint32][]byte{}
	for i := 0; i < n; i++ {
		want[uint32(i)] = bytes.Repeat([]byte{1}, 300)
	}
	return want
}

// page1FaultCheck checks the database after a failure and its retry. Right
// after the failure: the header, the page count and the committed position
// are those of before, and the key set is exactly rows 0..rows-1, none of
// the failed transaction's keys (rows..rows+9, fill 9). Then a commit that
// writes page 1 with the next counter, rewriting rows..rows+4 only: the
// failed keys rows+5..rows+9 stay absent, there, in a crash copy and after
// a reopen.
func page1FaultCheck(t *testing.T, db *DB, path string, m restartMode, rows int, before page1FaultState) {
	t.Helper()
	require.Equal(t, before, page1FaultSnap(db), "the failed transaction left the header, page count and committed position as they were")
	require.Equal(t, page1FaultWant(rows), page1FaultRows(t, db), "after the failure")
	require.NoError(t, db.IntegrityCheck())
	fcc := db.pager.header.FileChangeCount
	restartPut(t, db, rows, rows+5, 1) // grows: writes page 1
	if db.pager.page1OnChange() {
		// The retry's begin read page 1 again: dbFileVers is the committed
		// counter, never the failed read's.
		require.Equal(t, fcc+1, db.pager.header.FileChangeCount)
	}
	want := page1FaultWant(rows + 5)
	require.Equal(t, want, page1FaultRows(t, db), "after the retry")
	restartRecoverCopy(t, path, "crash", m, func(cdb *DB) {
		require.Equal(t, want, page1FaultRows(t, cdb), "a crash copy")
	})
	db = restartReopen(t, db, path, m)
	require.Equal(t, want, page1FaultRows(t, db), "after a reopen")
	require.NoError(t, db.IntegrityCheck())
}

func TestPage1Rule_FailedCommitThenRetry(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		t.Run("after-the-sync", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			before := page1FaultSnap(db)
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 50, 60, 9)
			failPublication(t)
			require.ErrorIs(t, wtx.Commit(), errInjectedPublish)
			testCommitPublishErrHook = nil
			page1FaultCheck(t, db, path, m, 50, before)
		})
		t.Run("page-1-load-in-the-commit", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			before := page1FaultSnap(db)
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 50, 60, 9) // grows: the commit needs page 1
			db.pager.writerCache.discard(1)
			failPage1ReadOnce(t, db)
			require.ErrorIs(t, wtx.Commit(), errInjectedPage1Frame)
			setWalReadFrameFaultHook(nil)
			page1FaultCheck(t, db, path, m, 50, before)
		})
		t.Run("page-1-load-at-the-begin", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			before := page1FaultSnap(db)
			db.pager.writerCache.discard(1)
			failPage1ReadOnce(t, db)
			_, err := db.BeginWrite()
			require.ErrorIs(t, err, errInjectedPage1Frame)
			setWalReadFrameFaultHook(nil)
			page1FaultCheck(t, db, path, m, 50, before)
		})
	})
}
