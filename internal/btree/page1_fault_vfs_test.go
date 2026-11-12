//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// page1_fault_test.go's failed commits with the failure in the WAL file
// itself: a frame write, the commit's sync.

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedFrameWrite = errors.New("injected WAL frame write failure")

// walFrameFaultFile fails, once, the next write of a frame header.
type walFrameFaultFile struct {
	File
	armed atomic.Bool
}

func (f *walFrameFaultFile) WriteAt(b []byte, off int64) (int, error) {
	if len(b) == walFrameSize && f.armed.CompareAndSwap(true, false) {
		return 0, errInjectedFrameWrite
	}
	return f.File.WriteAt(b, off)
}

func TestPage1Rule_FailedCommitThenRetry_WALFile(t *testing.T) {
	for _, m := range restartModes[:2] { // the modes with a WAL file
		t.Run(m.name+"/frame-write", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			ff := &walFrameFaultFile{File: db.pager.wal.file}
			db.pager.wal.file = ff
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 50, 60, 9)
			ff.armed.Store(true)
			require.ErrorIs(t, wtx.Commit(), errInjectedFrameWrite)
			page1FaultCheck(t, db, path, m, 50)
		})
		t.Run(m.name+"/sync", func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 50, 1)
			wtx, err := db.BeginWrite()
			require.NoError(t, err)
			restartPutTx(t, wtx, 50, 60, 9)
			failNextWALSync(t, db)
			require.ErrorIs(t, wtx.Commit(), errInjectedWALSync)
			ResetVFS()
			page1FaultCheck(t, db, path, m, 50)
		})
	}
}
