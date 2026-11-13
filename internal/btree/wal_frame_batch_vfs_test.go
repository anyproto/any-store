//go:build vfs

package btree

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type walWriteRec struct {
	off int64
	n   int
}

// recordingWAL records the WAL writes made while armed and can fail the
// failChunk-th write that carries whole frames (a frameBatch chunk).
type recordingWAL struct {
	File
	frameSize int
	mu        sync.Mutex
	armed     bool
	failChunk int
	chunks    int
	writes    []walWriteRec
}

func (f *recordingWAL) WriteAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	if f.armed {
		if len(b)%f.frameSize == 0 {
			f.chunks++
			if f.chunks == f.failChunk {
				f.mu.Unlock()
				return 0, errors.New("injected WAL chunk write fault")
			}
		}
		f.writes = append(f.writes, walWriteRec{off, len(b)})
	}
	f.mu.Unlock()
	return f.File.WriteAt(b, off)
}

func (f *recordingWAL) arm(failChunk int) {
	f.mu.Lock()
	f.armed, f.failChunk, f.chunks, f.writes = true, failChunk, 0, nil
	f.mu.Unlock()
}

func (f *recordingWAL) disarm() []walWriteRec {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = false
	return f.writes
}

func openRecordingWAL(t *testing.T, path string, opts Options) (*DB, *recordingWAL) {
	t.Helper()
	frameSize := int(walFrameSize) + int(opts.PageSize)
	var rec *recordingWAL
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			if strings.HasSuffix(name, "-wal") {
				rec = &recordingWAL{File: f, frameSize: frameSize}
				return rec, nil
			}
			return f, nil
		},
	})
	t.Cleanup(ResetVFS)
	resetPageBufferPool()
	db, err := Open(path, opts)
	require.NoError(t, err)
	return db, rec
}

func batchValue(i int) []byte { return bytes.Repeat([]byte{byte('a' + i%26)}, 180) }

// One small commit is one WAL write: its frames (the changed leaf and page 1)
// are staged together, not written as a header and a page each.
func TestWriteFrames_SmallCommitIsOneWrite(t *testing.T) {
	opts := Options{PageSize: 4096, DisableAutoCheckpoint: true}
	db, rec := openRecordingWAL(t, filepath.Join(t.TempDir(), "t.db"), opts)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, saveKey(0), []byte("v")))
	require.NoError(t, tx.Commit())
	require.NotNil(t, rec)

	before := db.pager.wal.nFrame.Load()
	rec.arm(0)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = tx.GetNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, saveKey(1), []byte("w")))
	require.NoError(t, tx.Commit())
	writes := rec.disarm()

	frames := int(db.pager.wal.nFrame.Load() - before)
	require.GreaterOrEqual(t, frames, 2, "a commit writes the leaf and page 1")
	require.Len(t, writes, 1)
	require.Equal(t, frames*rec.frameSize, writes[0].n)
	require.Equal(t, int64(walHeaderSize)+int64(before)*int64(rec.frameSize), writes[0].off)
}

// A commit larger than walFrameBatchMax goes out in contiguous chunks of whole
// frames, each at most walFrameBatchMax, and reads back after reopen.
func TestWriteFrames_LargeCommitChunks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	opts := Options{PageSize: 512, CacheSize: 20000, DisableAutoCheckpoint: true}
	db, rec := openRecordingWAL(t, path, opts)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	before := db.pager.wal.nFrame.Load()
	rec.arm(0)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = tx.GetNamespace("data")
	require.NoError(t, err)
	const n = 4000
	for i := 0; i < n; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), batchValue(i)))
	}
	require.NoError(t, tx.Commit())
	writes := rec.disarm()

	frames := int(db.pager.wal.nFrame.Load() - before)
	perChunk := walFrameBatchMax / rec.frameSize
	require.Greater(t, frames, 2*perChunk, "the commit must span several chunks")
	require.Len(t, writes, (frames+perChunk-1)/perChunk)
	next := int64(walHeaderSize) + int64(before)*int64(rec.frameSize)
	total := 0
	for i, w := range writes {
		require.Equal(t, next, w.off, "chunk %d not contiguous", i)
		require.Zero(t, w.n%rec.frameSize)
		require.LessOrEqual(t, w.n, walFrameBatchMax)
		if i < len(writes)-1 {
			require.Equal(t, perChunk*rec.frameSize, w.n)
		}
		next += int64(w.n)
		total += w.n
	}
	require.Equal(t, frames*rec.frameSize, total)
	require.NoError(t, db.Close())

	ResetVFS()
	db, err = Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.IntegrityCheck())
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err = rtx.GetNamespace("data")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		v, err := rtx.Get(ns, saveKey(i))
		require.NoError(t, err, "key %d", i)
		require.Equal(t, batchValue(i), v, "key %d", i)
	}
}

// A chunk that fails mid-commit fails the commit and leaves the file at the
// previous commit: after reopen the earlier data is intact, none of the failed
// transaction's keys exist, and the next commit works.
func TestWriteFrames_FailedChunkKeepsPreviousCommit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "t.db")
	opts := Options{PageSize: 512, CacheSize: 20000, DisableAutoCheckpoint: true}
	db, rec := openRecordingWAL(t, path, opts)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), []byte(fmt.Sprintf("old-%d", i))))
	}
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = tx.GetNamespace("data")
	require.NoError(t, err)
	for i := 100; i < 4100; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), batchValue(i)))
	}
	rec.arm(2)
	require.Error(t, tx.Commit())
	writes := rec.disarm()
	require.NotEmpty(t, writes, "the first chunk went out before the fault")
	_ = db.Close()

	ResetVFS()
	db, err = Open(path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.IntegrityCheck())
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err = rtx.GetNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		v, err := rtx.Get(ns, saveKey(i))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("old-%d", i), string(v))
	}
	for _, i := range []int{100, 2000, 4099} {
		_, err := rtx.Get(ns, saveKey(i))
		require.ErrorIs(t, err, ErrKeyNotFound, "key %d of the failed commit", i)
	}
	rtx.Rollback()

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = tx.GetNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, saveKey(5000), []byte("after")))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())
	db, err = Open(path, opts)
	require.NoError(t, err)
	require.NoError(t, db.IntegrityCheck())
	rtx, err = db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err = rtx.GetNamespace("data")
	require.NoError(t, err)
	v, err := rtx.Get(ns, saveKey(5000))
	require.NoError(t, err)
	require.Equal(t, "after", string(v))
}
