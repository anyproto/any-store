//go:build unix

package btree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// The directory a WAL's first sync syncs is the one the database was opened
// in. A database opened by a relative path, and a working directory that
// changes before the first commit: the commit syncs the database's
// directory, not the new working directory.
func TestDirSync_DirectoryIsTheOneResolvedAtOpen(t *testing.T) {
	home, elsewhere := t.TempDir(), t.TempDir()
	homeInfo, err := os.Stat(home)
	require.NoError(t, err)
	var synced []os.FileInfo
	testSyncDirHook = func(d *os.File) {
		info, err := d.Stat()
		require.NoError(t, err)
		synced = append(synced, info)
	}
	t.Cleanup(func() { testSyncDirHook = nil })

	t.Chdir(home)
	opts := DefaultOptions()
	opts.NoCommitSync = false
	db, err := testOpen(t, "test.db", opts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.Len(t, synced, 1, "setup: the creation journal's directory sync")

	t.Chdir(elsewhere)
	createPutRow(t, db, "one")
	require.Len(t, synced, 2, "the WAL's first sync syncs its directory")
	for _, info := range synced {
		require.True(t, os.SameFile(homeInfo, info), "a directory other than the database's was synced")
	}
	require.FileExists(t, filepath.Join(home, "test.db-wal"))
}
