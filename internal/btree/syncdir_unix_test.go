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

// The database, its creation journal and its WAL are opened by the full name
// resolved once. A working directory that changes after the resolution and
// before the database file is opened: every file is in the directory the
// name was resolved against, that directory is the one synced, and the new
// working directory gets nothing.
func TestOpen_ChdirBeforeTheFilesOpenKeepsThemInTheResolvedDirectory(t *testing.T) {
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
	var journalAtHome, journalElsewhere bool
	testOpenStageHook = func(_ *pager, stage string) {
		switch stage {
		case "before-lock":
			require.NoError(t, os.Chdir(elsewhere))
		case "journal-opened":
			_, err := os.Stat(filepath.Join(home, "test.db"+creationJournalSuffix))
			journalAtHome = err == nil
			_, err = os.Stat(filepath.Join(elsewhere, "test.db"+creationJournalSuffix))
			journalElsewhere = err == nil
		}
	}
	t.Cleanup(func() { testOpenStageHook = nil })
	opts := DefaultOptions()
	opts.NoCommitSync = false
	db, err := testOpen(t, "test.db", opts)
	testOpenStageHook = nil
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	require.True(t, journalAtHome, "the creation journal was not created in the database's directory")
	require.False(t, journalElsewhere, "the creation journal was created in the working directory changed to")
	createPutRow(t, db, "one")

	require.FileExists(t, filepath.Join(home, "test.db"))
	require.FileExists(t, filepath.Join(home, "test.db-wal"))
	entries, err := os.ReadDir(elsewhere)
	require.NoError(t, err)
	require.Empty(t, entries, "a file was opened in the working directory changed to")
	require.Len(t, synced, 2, "the creation journal's directory sync and the WAL's")
	for _, info := range synced {
		require.True(t, os.SameFile(homeInfo, info), "a directory other than the database's was synced")
	}

	files, err := db.OpenFiles()
	require.NoError(t, err)
	fi, err := os.Stat(files.Path)
	require.NoError(t, err)
	require.True(t, os.SameFile(fi, files.DB), "OpenFiles.Path does not name the open database file")
}
