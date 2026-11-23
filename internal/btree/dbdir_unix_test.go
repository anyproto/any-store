//go:build unix

package btree

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The directory is the name up to its last separator, not cleaned: each ".."
// stays for the OS to follow. It is "/" for a file in the root directory and
// "." for a name with no separator.
func TestDatabaseDir_IsTheNameUpToItsLastSeparator(t *testing.T) {
	for name, want := range map[string]string{
		"/a/x/../db-wal": "/a/x/..",
		"/a/db-journal":  "/a",
		"/a//db":         "/a/",
		"/db":            "/",
		"db":             ".",
		"x/db":           "x",
	} {
		require.Equal(t, want, databaseDir(name), name)
	}
}

// In a working directory whose full name is longer than the OS takes, the
// relative name "test.db" opens but its full name cannot be built: getcwd
// fails, with ENAMETOOLONG or, on darwin, EINVAL, as SQLite's open fails when
// osGetcwd does (os_unix.c:7023-7024). The open fails, and it has created no
// file.
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
	require.Error(t, err)
	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	require.Empty(t, entries, "the failed open created a file")
}
