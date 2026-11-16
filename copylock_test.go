//go:build unix || windows

package anystore

// The lock file of a copy's lock on Unix and Windows (copylock.go), and the
// lock of an open database that a copy tries on its staging file.

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// The lock file has content while it is held and goes with its holder; a
// lock file nobody holds, as a killed process leaves it, empty or not, is
// taken over and goes with its new holder.
func TestCopyLock_LockFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "copy.db.copy-lock")
	release, err := acquireCopyLock(path)
	require.NoError(t, err)
	fi, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, int64(1), fi.Size())
	release()
	assert.Empty(t, dirNames(t, dir))

	for _, left := range []string{"", "left by a killed copy"} {
		require.NoError(t, os.WriteFile(path, []byte(left), 0o644))
		release, err = acquireCopyLock(path)
		require.NoError(t, err)
		release()
		assert.Empty(t, dirNames(t, dir))
	}
}

// B opens the lock file A holds and stops before locking it; A leaves,
// removing the file, and C locks a new file at the path; then B locks the
// file it opened, which nobody holds and the path no longer names. That
// lock excludes nobody: B must find that the path names another file, open
// it again and be refused by C's lock. (On Windows A's removal fails while
// B has the file open, so C locks A's file, and B's lock is refused at
// once.)
//
// The second case models a volume that gives every empty file one
// identity, as macOS msdos does: there the check tells A's file from C's
// only because a holder gives its lock file content before comparing.
func TestCopyLock_OpenedBeforeItsRemoval(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sameFile func(a, b os.FileInfo) bool
	}{
		{"file identities", os.SameFile},
		{"empty files share one identity", func(a, b os.FileInfo) bool {
			if a.Size() == 0 && b.Size() == 0 {
				return true
			}
			return os.SameFile(a, b)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prevSameFile := copyLockSameFile
			copyLockSameFile = tc.sameFile
			defer func() { copyLockSameFile = prevSameFile }()
			dir := t.TempDir()
			path := filepath.Join(dir, "copy.db.copy-lock")

			releaseA, err := acquireCopyLock(path)
			require.NoError(t, err)
			// The first open after A's is B's: it stops there. C's open and
			// B's next ones go on.
			var opens atomic.Int32
			opened, resume := make(chan struct{}), make(chan struct{})
			testHookCopyLockOpened = func() {
				if opens.Add(1) == 1 {
					close(opened)
					<-resume
				}
			}
			defer func() { testHookCopyLockOpened = nil }()
			type result struct {
				release func()
				err     error
			}
			b := make(chan result, 1)
			go func() {
				release, err := acquireCopyLock(path)
				b <- result{release, err}
			}()
			<-opened
			releaseA()
			releaseC, err := acquireCopyLock(path)
			require.NoError(t, err)
			close(resume)

			var r result
			select {
			case r = <-b:
			case <-time.After(time.Minute):
				t.Fatal("B waits")
			}
			if r.err == nil {
				r.release()
			}
			assert.ErrorIs(t, r.err, ErrBusy, "B holds the lock while C does")
			if runtime.GOOS != "windows" {
				// B's lock of the removed file was granted, and the check
				// sent B back to the path.
				assert.Equal(t, int32(3), opens.Load())
			}
			releaseC()
			assert.Empty(t, dirNames(t, dir))
		})
	}
}

// stagingOpenAsDatabase finds the lock of an open database in both modes of
// the database file lock: shared between processes, a shared flock (Windows
// has no such mode and opens it kept in the process), and kept in the
// process, an exclusive lock. A closed database, a missing name and a
// directory are free.
func TestStagingOpenAsDatabase(t *testing.T) {
	dir := t.TempDir()
	for _, inProcess := range []bool{false, true} {
		path := filepath.Join(dir, fmt.Sprintf("inprocess-%v.db", inProcess))
		d, err := btree.Open(path, btree.Options{InProcess: inProcess})
		require.NoError(t, err)
		open, err := stagingOpenAsDatabase(path)
		require.NoError(t, err)
		assert.True(t, open, "open, InProcess=%v", inProcess)
		require.NoError(t, d.Close())
		open, err = stagingOpenAsDatabase(path)
		require.NoError(t, err)
		assert.False(t, open, "closed, InProcess=%v", inProcess)
	}
	for _, free := range []string{filepath.Join(dir, "missing"), dir} {
		open, err := stagingOpenAsDatabase(free)
		require.NoError(t, err)
		assert.False(t, open, free)
	}
}

// A database open under the staging name, which is not the source, is no
// killed copy's leftover: the copy tries the lock an open database holds on
// its file before it removes anything there, and fails with ErrBusy,
// removing nothing; the database keeps its files and its commits. Once it
// is closed, it is a file under a reserved name like any other, and the
// next copy removes it.
func TestCopyToLatestFormat_StagingOpenAsDatabase(t *testing.T) {
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)
	dir := t.TempDir()
	dstPath := filepath.Join(dir, "x")
	stage := dstPath + ".copy-staging"
	// The commits stay in the log, which the copy would remove too.
	conf := &Config{DisableAutoCheckpoint: true}
	other, err := Open(ctx, stage, conf)
	require.NoError(t, err)
	closed := false
	defer func() {
		if !closed {
			_ = other.Close()
		}
	}()
	oc, err := other.CreateCollection(ctx, "other")
	require.NoError(t, err)
	insertJSON(t, oc, `{"id":1}`, `{"id":2}`)
	before := dirNames(t, dir)

	report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
	require.ErrorIs(t, err, ErrBusy)
	assert.ErrorContains(t, err, "the staging file "+stage+" is open as a database")
	assert.Zero(t, report)
	assert.Equal(t, before, dirNames(t, dir), "the copy touches nothing")

	insertJSON(t, oc, `{"id":3}`)
	closed = true
	require.NoError(t, other.Close())
	reopened, err := Open(ctx, stage, conf)
	require.NoError(t, err)
	rc, err := reopened.OpenCollection(ctx, "other")
	require.NoError(t, err)
	n, err := rc.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	require.NoError(t, reopened.Close())

	_, err = CopyToLatestFormat(ctx, src, dstPath, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, dirNames(t, dir))
	assertSameDatabase(t, src, openCopy(t, dstPath, nil))
}
