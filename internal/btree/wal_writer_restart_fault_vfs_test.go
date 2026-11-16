//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// A writer that started the log over and then fails to write or sync the new
// WAL header has published a restart header and written nothing. The failed
// transaction must unwind to the start of the new log (not to the frame
// number it began at), and the next transaction must write a log a later
// open recovers.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
				size := db.pager.dbSize.Load()
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

				// A reader of the unwound state, held across the retry: its
				// snapshot is the new log's start with the last commit's size,
				// so the retry's frames 1, 2, ... are not in it.
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				held := func() error {
					defer func() { _ = rtx.Rollback() }()
					if got := db.pager.readerDbSizeBound(rtx.cache); got != size {
						return fmt.Errorf("held reader's page count %d, the last commit's %d", got, size)
					}
					restartPut(t, db, 0, 3, 3)
					ns, err := rtx.GetNamespace("t1")
					if err != nil {
						return err
					}
					for i := 0; i < 3; i++ {
						v, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
						if err != nil {
							return err
						}
						if !bytes.Equal(v, bytes.Repeat([]byte{1}, 300)) {
							return fmt.Errorf("row %d: the held reader sees the retry's commit", i)
						}
					}
					return nil
				}()
				require.NoError(t, held)
				restartCheck(t, db, 0, 3, 3)
				restartCheck(t, db, 3, 200, 1)
				// Recovered from the WAL's bytes, before any Close
				// checkpoints it away.
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
	}
}
