//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// A transaction sent an attached backup spilled pages and then failed with
// an I/O error: a later spill, the commit's write, or the commit's sync. The
// error cleanup discards the transaction without a rollback call of its own,
// and the backup must start over all the same (SQLite: pager_reset in the
// error cleanup, pager.c:1910-1912).

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedWALIO = errors.New("injected WAL I/O failure")

// walIOFaultFile fails, once, the next WriteAt or the next Sync.
type walIOFaultFile struct {
	File
	failWrite atomic.Bool
	failSync  atomic.Bool
}

func (f *walIOFaultFile) WriteAt(b []byte, off int64) (int, error) {
	if f.failWrite.CompareAndSwap(true, false) {
		return 0, errInjectedWALIO
	}
	return f.File.WriteAt(b, off)
}

func (f *walIOFaultFile) Sync() error {
	if f.failSync.CompareAndSwap(true, false) {
		return errInjectedWALIO
	}
	return f.File.Sync()
}

func TestBackupChange_SpillThenIOError(t *testing.T) {
	for _, m := range restartModes[:2] { // the modes with a WAL file
		for _, fault := range []string{"spill-write", "commit-write", "commit-sync"} {
			t.Run(m.name+"/"+fault, func(t *testing.T) {
				f := newBackupFixture(t, m)
				w := f.src.pager.wal
				ff := &walIOFaultFile{File: w.file}
				w.file = ff

				wtx, err := f.src.BeginWrite()
				require.NoError(t, err)
				restartPutTx(t, wtx, 0, backupRows/2, 2)
				restartSpillAll(t, f.src) // the backup has uncommitted pages now
				restartPutTx(t, wtx, backupRows/2, backupRows, 2)
				switch fault {
				case "spill-write":
					ff.failWrite.Store(true)
					var spillErr error
					for _, pg := range f.src.pager.writerCache.dirtyPages() {
						if pg.pgno != 1 {
							spillErr = f.src.pager.pagerStress(pg)
							break
						}
					}
					require.ErrorIs(t, spillErr, errInjectedWALIO)
					_ = wtx.Rollback()
				case "commit-write":
					ff.failWrite.Store(true)
					require.ErrorIs(t, wtx.Commit(), errInjectedWALIO)
				case "commit-sync":
					ff.failSync.Store(true)
					require.ErrorIs(t, wtx.Commit(), errInjectedWALIO)
				}
				require.Equal(t, uint32(1), f.next(t), "the backup starts over")
				restartCheck(t, f.src, 0, backupRows, 1)
				f.finish(t, 1)
			})
		}
	}
}
