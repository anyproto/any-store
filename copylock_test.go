//go:build unix || windows

package anystore

// The lock file of a copy's lock on Unix and Windows (copylock.go).

import (
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
