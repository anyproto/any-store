//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// stallTruncateFile is a WAL file whose Truncate calls onTruncate first.
type stallTruncateFile struct {
	File
	onTruncate func()
}

func (f *stallTruncateFile) Truncate(size int64) error {
	f.onTruncate()
	return f.File.Truncate(size)
}

// TestOpenWaitsForLastCloserHoldingDBLock: the last client of a database is
// inside its close, holding the exclusive DB-file lock, for two seconds (it
// is stopped at the truncate of its WAL, which a close does under that
// lock). An open from another process waits for it through the busy handler
// and then opens the database.
func TestOpenWaitsForLastCloserHoldingDBLock(t *testing.T) {
	if os.Getenv(createChildRoleEnv) == "slow-closer" {
		path := os.Getenv(createChildPathEnv)
		in := bufio.NewReader(os.Stdin)
		var armed atomic.Bool
		SetVFS(VFS{OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil || !strings.HasSuffix(name, "-wal") {
				return f, err
			}
			return &stallTruncateFile{File: f, onTruncate: func() {
				if armed.CompareAndSwap(true, false) {
					childSay("STALLED")
					childWait(t, in, "GO")
				}
			}}, nil
		}})
		defer ResetVFS()
		db, err := testOpen(t, path, createOpts(""))
		require.NoError(t, err)
		createPutRow(t, db, "child")
		armed.Store(true)
		require.NoError(t, db.Close())
		require.False(t, armed.Load(), "the close truncated its WAL")
		childSay("CLOSED")
		return
	}
	if os.Getenv(createChildRoleEnv) != "" {
		return
	}

	path := filepath.Join(t.TempDir(), "closing.db")
	c := startCreateChild(t, "slow-closer", path, "", "")
	c.expect("STALLED")

	errCh := make(chan error, 1)
	var db *DB
	start := time.Now()
	go func() {
		var err error
		db, err = testOpen(t, path, createOpts(""))
		errCh <- err
	}()
	time.Sleep(dbLockHold)
	c.send("GO")
	require.NoError(t, <-errCh, "an open waits for a closing last client through the busy handler")
	defer func() { _ = db.Close() }()
	t.Logf("open returned after %v", time.Since(start).Round(time.Millisecond))

	c.expect("CLOSED")
	createRequireRow(t, db, "child")
	require.NoError(t, db.IntegrityCheck())
}
