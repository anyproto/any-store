//go:build vfs && (linux || darwin) && (amd64 || arm64)

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

// powerLossFS is a VFS over the real file system that remembers, for every
// file opened through it, the content the file had at its last sync. image
// writes those contents out: what a power loss leaves, on a file system that
// keeps a file's name once the file is created and loses every byte written
// since the file's last sync.
type powerLossFS struct {
	mu       sync.Mutex
	synced   map[string][]byte
	failSync func(name string) error
}

type powerLossFile struct {
	*os.File
}

func (fs *powerLossFS) vfs() VFS {
	return VFS{
		OpenFile: func(name string, flag int, perm os.FileMode) (File, error) {
			f, err := os.OpenFile(name, flag, perm)
			if err != nil {
				return nil, err
			}
			fs.mu.Lock()
			if _, ok := fs.synced[name]; !ok {
				fs.synced[name] = nil
			}
			fs.mu.Unlock()
			return powerLossFile{f}, nil
		},
		Remove: func(name string) error {
			err := os.Remove(name)
			if err == nil {
				fs.mu.Lock()
				delete(fs.synced, name)
				fs.mu.Unlock()
			}
			return err
		},
		Fdatasync: func(f File) error {
			name := f.(powerLossFile).Name()
			if fs.failSync != nil {
				if err := fs.failSync(name); err != nil {
					return err
				}
			}
			content, err := os.ReadFile(name)
			if err != nil {
				return err
			}
			fs.mu.Lock()
			fs.synced[name] = content
			fs.mu.Unlock()
			return nil
		},
	}
}

// image writes what a power loss now would leave of the files in fromDir
// into toDir. The wal-index is not among them: it does not outlive a reboot.
func (fs *powerLossFS) image(t *testing.T, fromDir, toDir string) {
	t.Helper()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for name, content := range fs.synced {
		if filepath.Dir(name) != fromDir || strings.HasSuffix(name, "-shm") {
			continue
		}
		require.NoError(t, os.WriteFile(filepath.Join(toDir, filepath.Base(name)), content, 0o644))
	}
}

var errCreatorDied = errors.New("the creator dies before its page 1 is synced")

// TestCreation_CommitOverADeadCreatorsPage1SurvivesPowerLoss: a creator
// writes page 1 and dies before the page is synced. Another process opens the
// path, commits a row with a synced commit, and the power goes. The row was
// acknowledged as durable: the open after the power loss must find it.
//
// The second process must therefore not build on the dead creator's page 1,
// which the power loss takes away: the database file comes back empty, and a
// WAL beside an empty database file is deleted, the acknowledged commit with
// it.
func TestCreation_CommitOverADeadCreatorsPage1SurvivesPowerLoss(t *testing.T) {
	for _, inProcess := range []bool{false, true} {
		t.Run(map[bool]string{false: "multiprocess", true: "inprocess"}[inProcess], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.db")
			opts := DefaultOptions()
			opts.InProcess = inProcess
			opts.DisableAutoCheckpoint = true
			opts.NoCommitSync = false

			fs := &powerLossFS{synced: map[string][]byte{}}
			SetVFS(fs.vfs())
			t.Cleanup(ResetVFS)

			// The creator: its sync of the database file never happens.
			fs.failSync = func(name string) error {
				if name == path {
					return errCreatorDied
				}
				return nil
			}
			_, err := testOpen(t, path, opts)
			require.ErrorIs(t, err, errCreatorDied)
			fs.failSync = nil
			info, err := os.Stat(path)
			require.NoError(t, err)
			require.Equal(t, int64(opts.PageSize), info.Size(), "setup: the dead creator's page 1 is in the file")

			// The second process, and its acknowledged commit.
			db, err := testOpen(t, path, opts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			createPutRow(t, db, "acknowledged")

			// The power goes.
			after := t.TempDir()
			fs.image(t, dir, after)
			require.NoError(t, db.Close())
			ResetVFS()

			db, err = testOpen(t, filepath.Join(after, "test.db"), opts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			createRequireRow(t, db, "acknowledged")
			require.NoError(t, db.IntegrityCheck())
		})
	}
}
