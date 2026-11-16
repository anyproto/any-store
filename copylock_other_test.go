//go:build !unix && !windows

package anystore

// The lock of a copy where there is no file lock (copylock_other.go).

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// One lock for the process, whatever the path: while it is held, a lock
// for any path, another spelling of the same or another destination's, is
// refused with ErrBusy, and no file is made.
func TestCopyLock_OneForTheProcess(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	path := filepath.Join(dir, "copy.db.copy-lock")
	release, err := acquireCopyLock(path)
	require.NoError(t, err)
	for _, other := range []string{
		path,
		"copy.db.copy-lock",
		dir + "/./copy.db.copy-lock",
		filepath.Join(dir, "other.db.copy-lock"),
	} {
		_, err = acquireCopyLock(other)
		assert.ErrorIs(t, err, ErrBusy, other)
	}
	assert.Empty(t, dirNames(t, dir))
	release()

	release, err = acquireCopyLock(filepath.Join(dir, "other.db.copy-lock"))
	require.NoError(t, err)
	release()
	assert.Empty(t, dirNames(t, dir))
}
