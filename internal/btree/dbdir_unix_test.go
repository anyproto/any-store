//go:build unix

package btree

import (
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// The directory is the full pathname up to its last separator, not cleaned:
// each ".." stays for the OS to follow.
func TestDatabaseDir_KeepsDotDot(t *testing.T) {
	t.Chdir(t.TempDir())
	wd, err := os.Getwd()
	require.NoError(t, err)
	for path, want := range map[string]string{
		"x/../db":  wd + "/x/..",
		"db":       wd,
		"/x/../db": "/x/..",
		"/db":      "/",
	} {
		got, err := databaseDir(path)
		require.NoError(t, err)
		require.Equal(t, want, got, path)
	}
}

// In a working directory whose full name is longer than the OS takes, the
// relative name "test.db" opens but its full name does not. The open fails,
// and before it has created any file: no database, journal or WAL whose
// directory sync would be skipped.
func TestDatabaseDir_NameTooLongFailsTheOpen(t *testing.T) {
	t.Chdir(t.TempDir())
	name := strings.Repeat("d", 200)
	for range 25 { // 5000 bytes, past PATH_MAX on Linux (4096) and Darwin (1024)
		require.NoError(t, os.Mkdir(name, 0o755))
		require.NoError(t, os.Chdir(name))
	}
	require.NoError(t, os.WriteFile("probe", nil, 0o644), "setup: a relative name opens")
	require.NoError(t, os.Remove("probe"))

	db, err := testOpen(t, "test.db", DefaultOptions())
	if err == nil {
		_ = db.Close()
	}
	require.ErrorIs(t, err, syscall.ENAMETOOLONG)
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	require.Empty(t, entries, "the failed open created a file")
}
