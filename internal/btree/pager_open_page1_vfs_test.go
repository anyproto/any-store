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

var errInjectedPage1Read = errors.New("injected page 1 read failure")

// page1TailFaultFile fails, while armed, every read of page 1 that goes past
// the 100-byte header: the header is readable, the rest of the page is not.
type page1TailFaultFile struct {
	File
	armed *atomic.Bool
}

func (f *page1TailFaultFile) ReadAt(b []byte, off int64) (int, error) {
	if f.armed.Load() && off < dbHeaderSize && off+int64(len(b)) > dbHeaderSize {
		return 0, errInjectedPage1Read
	}
	return f.File.ReadAt(b, off)
}

// TestOpenPage1ReadErrorPastHeaderFails: open reads page 1 whole, as
// lockBtree fetches it (btree.c:3288, readDbPage pager.c:3039-3043), so a
// read error in the page beyond its header fails the open instead of going
// unnoticed behind a 100-byte read.
func TestOpenPage1ReadErrorPastHeaderFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, openSizeOpts)
	require.NoError(t, err)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = wtx.CreateNamespace("t")
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())
	// Page 1 now lives in the file only: the open-time read takes it from
	// there.
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())

	var armed atomic.Bool
	SetVFS(VFS{OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
		f, err := defaultOpenFile(name, flag, perm)
		if err != nil || name != path {
			return f, err
		}
		return &page1TailFaultFile{File: f, armed: &armed}, nil
	}})
	defer ResetVFS()
	// Armed after the WAL attach, so only the page-1 read of the open's
	// snapshot meets it.
	testOpenAfterWALOpenHook = func(p *pager) {
		if p.path == path {
			armed.Store(true)
		}
	}
	defer func() { testOpenAfterWALOpenHook = nil }()

	db, err = testOpen(t, path, openSizeOpts)
	if err == nil {
		armed.Store(false)
		_ = db.Close()
	}
	require.ErrorIs(t, err, errInjectedPage1Read)

	testOpenAfterWALOpenHook = nil
	armed.Store(false)
	db, err = testOpen(t, path, openSizeOpts)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}
