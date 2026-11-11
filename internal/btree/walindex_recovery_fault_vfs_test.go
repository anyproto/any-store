//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// A frame read that fails while recovery scans the WAL is an error, not the
// end of the log. SQLite's walIndexRecover keeps the read's rc
// (wal.c:1502-1503) and publishes the rebuilt header only under
// rc==SQLITE_OK (wal.c:1561-1566): a prefix of the log must never be
// published as all of it, because the next checkpoint would truncate the
// rest away.

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedWALRead = errors.New("injected WAL read failure")

// walReadFaultFile fails every ReadAt at or beyond failFrom while armed.
type walReadFaultFile struct {
	File
	armed    atomic.Bool
	failFrom atomic.Int64
	failed   atomic.Int64
}

func (f *walReadFaultFile) ReadAt(b []byte, off int64) (int, error) {
	if f.armed.Load() && off >= f.failFrom.Load() {
		f.failed.Add(1)
		return 0, errInjectedWALRead
	}
	return f.File.ReadAt(b, off)
}

// TestRecoverReadErrorNotEndOfLog_AttachedRecovery: two commits are in the
// WAL, the wal-index header is torn, and reading the second commit's frames
// fails. Neither a read begin nor a checkpoint may publish the first commit
// as the end of the log.
func TestRecoverReadErrorNotEndOfLog_AttachedRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)
	w := db.pager.wal
	firstCommitEnd := w.nFrame.Load()
	dmsPutRange(t, db, "t1", tornHdrSeedRows, tornHdrF1End)
	require.Greater(t, w.nFrame.Load(), firstCommitEnd)

	frameSize := int64(walFrameSize) + int64(w.pageSize)
	ff := &walReadFaultFile{File: w.file}
	ff.failFrom.Store(int64(walHeaderSize) + int64(firstCommitEnd)*frameSize)
	w.file = ff

	tearHeaderCopy(t, db)
	walSize := func() int64 {
		info, err := os.Stat(path + "-wal")
		require.NoError(t, err)
		return info.Size()
	}
	before := walSize()
	ff.armed.Store(true)

	_, err = db.BeginRead()
	require.ErrorIs(t, err, errInjectedWALRead)
	_, valid := w.index.readHeader()
	require.False(t, valid, "a failed recovery must not publish a header")

	err = db.Checkpoint(CheckpointTruncate)
	require.ErrorIs(t, err, errInjectedWALRead)
	_, valid = w.index.readHeader()
	require.False(t, valid, "a failed recovery must not publish a header")
	require.Equal(t, before, walSize(), "the WAL must be untouched")
	require.Greater(t, ff.failed.Load(), int64(0))

	// The fault clears: recovery sees the whole log.
	ff.armed.Store(false)
	require.Equal(t, tornHdrF1End, countKeys(t, db, "t1"))
	hdr, valid := w.index.readHeader()
	require.True(t, valid)
	require.Equal(t, uint32(before-int64(walHeaderSize))/uint32(frameSize), hdr.mxFrame)
}

// TestRecoverReadErrorNotEndOfLog_Open: the same at open, where recovery
// rebuilds the index from scratch. The open fails; a later open, with the
// fault gone, has every commit.
func TestRecoverReadErrorNotEndOfLog_Open(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)
	firstCommitEnd := db.pager.wal.nFrame.Load()
	dmsPutRange(t, db, "t1", tornHdrSeedRows, tornHdrF1End)
	frameSize := int64(walFrameSize) + int64(db.pager.pageSize)
	rawClose(db) // crash: WAL kept, shm gone

	var ff *walReadFaultFile
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(name, "-wal") {
				ff = &walReadFaultFile{File: f}
				ff.failFrom.Store(int64(walHeaderSize) + int64(firstCommitEnd)*frameSize)
				ff.armed.Store(true)
				return ff, nil
			}
			return f, nil
		},
	})
	t.Cleanup(ResetVFS)

	_, err = testOpen(t, path, tornHdrOpts)
	require.ErrorIs(t, err, errInjectedWALRead)
	require.NotNil(t, ff)
	require.Greater(t, ff.failed.Load(), int64(0))

	ResetVFS()
	db2, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	require.Equal(t, tornHdrF1End, countKeys(t, db2, "t1"))
	require.NoError(t, db2.IntegrityCheck())
}
