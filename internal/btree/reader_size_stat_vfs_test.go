//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedStat = errors.New("injected database file stat failure")

// statFaultFile fails Stat while armed.
type statFaultFile struct {
	File
	armed atomic.Bool
}

func (f *statFaultFile) Stat() (os.FileInfo, error) {
	if f.armed.Load() {
		return nil, errInjectedStat
	}
	return f.File.Stat()
}

// TestReaderSizeStatErrorPropagates: the wal-index header carries no size
// (the index was built over an empty WAL), so a read begin sizes the
// database from the file. When that fails, the begin fails with the error,
// as pagerPagecount returns the fstat error (pager.c:3299-3303); it must not
// fall back to this process's own size, which a peer may have outgrown.
func TestReaderSizeStatErrorPropagates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, restartNPageOpts)
	require.NoError(t, err)
	dmsPutRange(t, db, "t1", 0, restartNPageSeedRows)
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())

	db, err = testOpen(t, path, restartNPageOpts)
	require.NoError(t, err)
	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, uint32(0), hdr.nPage, "setup: an index built over an empty WAL has no size")

	ff := &statFaultFile{File: db.pager.file}
	db.pager.file = ff
	defer func() {
		ff.armed.Store(false)
		db.pager.file = ff.File
		require.NoError(t, db.Close())
	}()

	ff.armed.Store(true)
	rtx, err := db.BeginRead()
	if err == nil {
		_ = rtx.Rollback()
	}
	require.ErrorIs(t, err, errInjectedStat)
	_, err = db.GetNamespace("t1")
	require.ErrorIs(t, err, errInjectedStat)
	_, err = db.ListNamespaces()
	require.ErrorIs(t, err, errInjectedStat)
	require.ErrorIs(t, db.IntegrityCheck(), errInjectedStat)

	ff.armed.Store(false)
	require.Equal(t, restartNPageSeedRows, countKeys(t, db, "t1"))
}
