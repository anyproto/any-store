//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// The shrinking-backup case of committed_size_test.go with the failure where
// it happens on a real file: the WAL sync of the commit (wal.c:4203, before
// the private header moves at wal.c:4238-4250).

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedWALSync = errors.New("injected WAL sync failure")

// failNextWALSync fails the next fdatasync of db's WAL file, once.
func failNextWALSync(t *testing.T, db *DB) {
	t.Helper()
	walFile := db.pager.wal.file
	var armed atomic.Bool
	armed.Store(true)
	SetVFS(VFS{Fdatasync: func(f File) error {
		if f == walFile && armed.CompareAndSwap(true, false) {
			return errInjectedWALSync
		}
		return defaultFdatasync(f)
	}})
	t.Cleanup(ResetVFS)
}

func TestCommittedSize_ShrinkSyncFailure(t *testing.T) {
	for _, m := range restartModes[:2] { // the modes with a WAL file
		for _, then := range []string{"retry", "truncate"} {
			t.Run(m.name+"/"+then, func(t *testing.T) {
				dst, src, dstPath := committedSizeShrinkFixture(t, m)
				b, err := dst.BackupInit(src)
				require.NoError(t, err)
				require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
				failNextWALSync(t, dst)
				require.ErrorIs(t, b.Finish(), errInjectedWALSync)
				ResetVFS()
				restartCheck(t, dst, 0, committedSizeRows, 1)

				if then == "retry" {
					restartPut(t, dst, 0, 3, 2)
				}
				require.NoError(t, dst.Checkpoint(CheckpointTruncate))
				dst = restartReopen(t, dst, dstPath, m)
				if then == "retry" {
					restartCheck(t, dst, 0, 3, 2)
					restartCheck(t, dst, 3, committedSizeRows, 1)
				} else {
					restartCheck(t, dst, 0, committedSizeRows, 1)
				}
				require.NoError(t, dst.IntegrityCheck())
			})
		}
	}
}
