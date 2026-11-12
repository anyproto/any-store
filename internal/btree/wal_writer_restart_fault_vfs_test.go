//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// A writer that started the log over and then fails to write or sync the new
// WAL header has published a restart header and written nothing. The failed
// transaction must unwind to the start of the new log (not to the frame
// number it began at), and the next transaction must write a log a later
// open recovers.

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedWALHeader = errors.New("injected WAL header failure")

// walHeaderFaultFile fails, once, the write of the WAL header or the first
// sync after it.
type walHeaderFaultFile struct {
	File
	failWrite atomic.Bool
	failSync  atomic.Bool
}

func (f *walHeaderFaultFile) WriteAt(b []byte, off int64) (int, error) {
	if off == 0 && len(b) == walHeaderSize && f.failWrite.CompareAndSwap(true, false) {
		return 0, errInjectedWALHeader
	}
	return f.File.WriteAt(b, off)
}

func (f *walHeaderFaultFile) Sync() error {
	if f.failSync.CompareAndSwap(true, false) {
		return errInjectedWALHeader
	}
	return f.File.Sync()
}

func TestWalWriterRestart_HeaderWriteAndSyncFailure(t *testing.T) {
	for _, m := range restartModes[:2] { // the modes with a WAL file
		for _, fault := range []string{"write", "sync"} {
			t.Run(m.name+"/"+fault, func(t *testing.T) {
				db, path := openRestartDB(t, m)
				restartPut(t, db, 0, 200, 1)
				require.NoError(t, db.Checkpoint(CheckpointPassive))
				w := db.pager.wal
				n := w.index.mxCommitFrame.LoadLocal()
				ff := &walHeaderFaultFile{File: w.file}
				w.file = ff

				wtx, err := db.BeginWrite()
				require.NoError(t, err)
				restartPutTx(t, wtx, 0, 2, 2)
				if fault == "write" {
					ff.failWrite.Store(true)
				} else {
					ff.failSync.Store(true)
				}
				require.ErrorIs(t, wtx.Commit(), errInjectedWALHeader)
				require.Zero(t, w.nFrame.Load(), "the unwind went to the start of the new log (old end %d)", n)
				require.Zero(t, w.index.mxCommitFrame.LoadLocal())
				restartCheck(t, db, 0, 200, 1)

				restartPut(t, db, 0, 3, 3)
				restartCheck(t, db, 0, 3, 3)
				restartCheck(t, db, 3, 200, 1)
				db = restartReopen(t, db, path, m)
				restartCheck(t, db, 0, 3, 3)
				restartCheck(t, db, 3, 200, 1)
				require.NoError(t, db.IntegrityCheck())
			})
		}
	}
}
