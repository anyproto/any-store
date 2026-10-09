//go:build vfs

package btree

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type failingWAL struct {
	File
	mu   sync.Mutex
	fail bool
}

func (f *failingWAL) WriteAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail {
		return 0, errors.New("injected WAL write fault")
	}
	return f.File.WriteAt(b, off)
}

// A write transaction that ends in the pager's error state — its commit's
// WAL write fails — ends its cursors and forgets them: the next write
// transaction's writes save none of them.
func TestCursorSave_FailedCommitForgetsCursors(t *testing.T) {
	var wal *failingWAL
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(name, "-wal") {
				wal = &failingWAL{File: f}
				return wal, nil
			}
			return f, nil
		},
	})
	t.Cleanup(ResetVFS)
	resetPageBufferPool()
	db, err := Open(filepath.Join(t.TempDir(), "t.db"), Options{PageSize: 512, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 3000; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), []byte("v")))
	}
	require.NoError(t, tx.Commit())
	require.NotNil(t, wal)

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	c := tx.NewCursor(ns)
	require.NoError(t, c.Seek(saveKey(1000)))
	require.NoError(t, tx.Put(ns, saveKey(1), []byte("changed")))
	require.NoError(t, c.Next())
	wal.mu.Lock()
	wal.fail = true
	wal.mu.Unlock()
	require.Error(t, tx.Commit())
	wal.mu.Lock()
	wal.fail = false
	wal.mu.Unlock()
	require.Nil(t, db.pager.cursors)
	require.Equal(t, cursorFault, c.state)
	require.False(t, c.linked)
	require.Empty(t, c.stack)

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback() }()
	ns2, err := tx2.GetNamespace("data")
	require.NoError(t, err)
	c2 := tx2.NewCursor(ns2)
	require.NoError(t, c2.Seek(saveKey(5)))
	require.Same(t, c2, db.pager.cursors)
	require.Nil(t, c2.nextCursor)
	require.NoError(t, tx2.Put(ns2, saveKey(2), []byte("y")))
	require.NoError(t, c2.Next())
	cursorKeyIs(t, c2, saveKey(6))
	c2.Close()
	require.Nil(t, db.pager.cursors)
	c.Close()
	require.Nil(t, db.pager.cursors)
}
