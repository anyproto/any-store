//go:build vfs

package anystore

// CopyToLatestFormat under faults of the file system: a link the volume
// refuses, a write or a sync of the copy that fails, and a process that
// dies after any step of the guide's replacement protocol.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A volume without hard links (FAT, exFAT) refuses the link: the call
// returns ErrPublishUnsupported with the link error, removes its copy and
// leaves nothing at the destination.
func TestCopyToLatestFormat_LinkUnsupported(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	prevLink := osLink
	osLink = func(oldname, newname string) error {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: syscall.EPERM}
	}
	defer func() { osLink = prevLink }()

	dstPath := filepath.Join(t.TempDir(), "copy.db")
	report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
	require.ErrorIs(t, err, ErrPublishUnsupported)
	require.ErrorIs(t, err, syscall.EPERM)
	assert.Zero(t, report)
	assertNoCopy(t, dstPath)
}

var errInjectedCopyIO = errors.New("injected I/O failure of the copy")

// faultyFile is a file of a staged copy, whose writes fail once failWrite
// is set if it is the copy's log.
type faultyFile struct {
	File
	name      string
	failWrite *atomic.Bool
}

func (f *faultyFile) WriteAt(b []byte, off int64) (int, error) {
	if strings.HasSuffix(f.name, "-wal") && f.failWrite.Load() {
		return 0, errInjectedCopyIO
	}
	return f.File.WriteAt(b, off)
}

// copyFaults installs a VFS that wraps the files of every copy staged
// beside dstPath: once failWrite is set the writes to a staging log fail,
// and once failSync is set so do the syncs of a staging database file.
func copyFaults(t *testing.T, dstPath string, failWrite, failSync *atomic.Bool) {
	t.Helper()
	staged := func(name string) bool { return strings.HasPrefix(name, dstPath+".tmp-") }
	SetVFS(VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil || !staged(name) {
				return f, err
			}
			return &faultyFile{File: f, name: name, failWrite: failWrite}, nil
		},
		Fdatasync: func(f File) error {
			if ff, ok := f.(*faultyFile); ok && failSync.Load() && !strings.HasSuffix(ff.name, "-wal") {
				return errInjectedCopyIO
			}
			return f.Sync()
		},
	})
	t.Cleanup(ResetVFS)
}

// An I/O fault of the copy fails the call, which closes the copy and
// removes every file of it: a write of its log while the documents are
// copied, once a first batch of them has committed, and the sync of its
// database file at the checkpoint that precedes the verification.
func TestCopyToLatestFormat_DestinationIOFault(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Fields: []string{"n"}}))
	for i := 0; i < 3*copyBatch; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"n":%d}`, i, i))))
	}
	t.Run("log write", func(t *testing.T) {
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		var failWrite atomic.Bool
		copyFaults(t, dstPath, &failWrite, new(atomic.Bool))
		var batches atomic.Int32
		// The second batch's commit is the first write after the first one.
		testHookCopiedBatch = func(int) {
			batches.Add(1)
			failWrite.Store(true)
		}
		defer func() { testHookCopiedBatch = nil }()
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, errInjectedCopyIO)
		assert.Equal(t, int32(1), batches.Load(), "one batch committed, the next failed")
		assert.Zero(t, report)
		ResetVFS()
		assertNoCopy(t, dstPath)
	})
	t.Run("database sync", func(t *testing.T) {
		dstPath := filepath.Join(t.TempDir(), "copy.db")
		var failSync atomic.Bool
		copyFaults(t, dstPath, new(atomic.Bool), &failSync)
		// Armed once the documents are in: the database file is synced
		// when the copy is created, and next by a checkpoint.
		testHookCopiedCollection = func(string) { failSync.Store(true) }
		defer func() { testHookCopiedCollection = nil }()
		report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
		require.ErrorIs(t, err, errInjectedCopyIO)
		assert.Contains(t, err.Error(), "checkpoint of the copy")
		assert.Zero(t, report)
		ResetVFS()
		assertNoCopy(t, dstPath)
	})
}

// errCrashed stops replaceWithCopy where the process would have died.
var errCrashed = errors.New("crashed")

// replaceWithCopy is the replacement protocol of
// docs/migration-format-5-to-6.md, the guide's example for one process,
// with crash(step) after each step: when it returns true the function stops
// there and leaves every handle and file as a process that died would.
// On an error before the source is closed it returns db, still open; after
// that it returns nil, and path is to be opened again.
func replaceWithCopy(ctx context.Context, db DB, path string, config *Config, crash func(step string) bool) (DB, error) {
	tmp := path + ".format6"
	// 1. The caller has stopped every writer.
	// 2. Copy. A file already at tmp is refused with ErrDestinationExists:
	// it is not this run's, and a copy from an earlier run is stale.
	if _, err := CopyToLatestFormat(ctx, db, tmp, config); err != nil {
		return db, err
	}
	if crash("copied") {
		return nil, errCrashed
	}
	// 3. Checkpoint the source and check the error: Close drops it.
	if err := db.Flush(ctx, 0, FlushModeCheckpointTruncate); err != nil {
		_ = os.Remove(tmp)
		return db, err
	}
	if crash("checkpointed") {
		return nil, errCrashed
	}
	// 4. Close the source. From here on, path has to be opened again.
	if err := db.Close(); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if crash("closed") {
		return nil, errCrashed
	}
	// 5. Remove the source's log and its index, then sync the directory. On
	// a failure path still holds the database: reopen it.
	for _, sidecar := range []string{path + "-wal", path + "-wal-shm"} {
		if err := os.Remove(sidecar); err != nil && !errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(tmp)
			return nil, err
		}
		if crash("removed " + sidecar[len(path):]) {
			return nil, errCrashed
		}
	}
	if err := exampleSyncDir(filepath.Dir(path)); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if crash("synced") {
		return nil, errCrashed
	}
	// 6. Put the copy in place, then sync the directory.
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return nil, err
	}
	if crash("renamed") {
		return nil, errCrashed
	}
	if err := exampleSyncDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if crash("synced again") {
		return nil, errCrashed
	}
	// 7. Open the new file before any writer resumes.
	newDB, err := Open(ctx, path, config)
	if err != nil {
		return nil, err
	}
	if crash("opened") {
		return nil, errCrashed
	}
	return newDB, nil
}

// exampleSyncDir is the guide's directory sync: the directory is fsynced
// on Unix, and not on Windows, where NTFS journals the names.
func exampleSyncDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// The replacement protocol of the guide, with the process dying after each
// of its steps: whatever the step, the database at the path reopens
// complete, either the old file, with the log it had, or the copy, never the
// copy beside an old log.
func TestReplaceWithCopy_CrashAfterEveryStep(t *testing.T) {
	steps := []string{"copied", "checkpointed", "closed", "removed -wal", "removed -wal-shm", "synced", "renamed", "synced again", "opened", ""}
	for i, step := range steps {
		name := step
		if name == "" {
			name = "no crash"
		}
		t.Run(name, func(t *testing.T) {
			createFormat(t, true)
			dir := t.TempDir()
			path := filepath.Join(dir, "app.db")
			// Commits stay in the log until a checkpoint: a crash before
			// step 3 leaves the source's newest data in its log.
			conf := &Config{DisableAutoCheckpoint: true}
			src, err := Open(ctx, path, conf)
			require.NoError(t, err)
			abandoned := []DB{src}
			t.Cleanup(func() { runtime.KeepAlive(abandoned) })
			coll, err := src.CreateCollection(ctx, "docs")
			require.NoError(t, err)
			require.NoError(t, coll.CreateIndex(ctx, IndexInfo{Name: "n", Fields: []string{"n"}}))
			for j := 0; j < 200; j++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"n":%d}`, j, j%13))))
			}
			reference := filepath.Join(t.TempDir(), "reference.db")
			require.NoError(t, src.Backup(ctx, reference))
			fi, err := os.Stat(path + "-wal")
			require.NoError(t, err)
			require.NotZero(t, fi.Size(), "the source's commits are in its log")

			replaced, err := replaceWithCopy(ctx, src, path, conf, func(s string) bool { return s == step })
			if step == "" {
				require.NoError(t, err)
				require.NoError(t, replaced.Close())
			} else {
				require.ErrorIs(t, err, errCrashed)
				if replaced != nil {
					abandoned = append(abandoned, replaced)
				}
			}

			// The process restarts: nothing it held is open any more.
			ResetOpenRegistry()
			reopened, err := Open(ctx, path, conf)
			require.NoError(t, err)
			defer func() { require.NoError(t, reopened.Close()) }()
			format, err := SchemaFormat(reopened)
			require.NoError(t, err)
			if i < 6 { // before the rename
				assert.Equal(t, 5, format, "the old file")
			} else {
				assert.Equal(t, LatestSchemaFormat, format, "the copy")
			}
			require.NoError(t, reopened.IntegrityCheck(ctx))
			assertSameDatabase(t, openCopy(t, reference, nil), reopened)
		})
	}
}

// The guide's example keeps the source open when it fails before closing
// it, here on a file left at its copy's path by an earlier run: the caller
// goes on with the handle it gets back, and the file is left alone.
func TestReplaceWithCopy_EarlyFailureKeepsSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.db")
	src, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	require.NoError(t, os.WriteFile(path+".format6", []byte("an earlier run's"), 0o644))

	got, err := replaceWithCopy(ctx, src, path, nil, func(string) bool { return false })
	require.ErrorIs(t, err, ErrDestinationExists)
	require.Equal(t, src, got, "the source comes back open")
	insertJSON(t, coll, `{"id":2}`)
	assertCollCount(t, coll, 2)
	data, err := os.ReadFile(path + ".format6")
	require.NoError(t, err)
	assert.Equal(t, "an earlier run's", string(data))
}
