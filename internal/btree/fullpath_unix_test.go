//go:build unix

package btree

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// A relative path is joined to the working directory as written, and an
// absolute path is kept as given: each "." and ".." stays for the OS to
// follow. A full name resolves to itself.
func TestFullPathname_Unix(t *testing.T) {
	t.Chdir(t.TempDir())
	wd, err := os.Getwd()
	require.NoError(t, err)
	for path, want := range map[string]string{
		"db":         wd + "/db",
		"x/../db":    wd + "/x/../db",
		"./db":       wd + "/./db",
		"":           wd + "/",
		"/db":        "/db",
		"/x/../db":   "/x/../db",
		"/x//./db":   "/x//./db",
		wd + "/x/db": wd + "/x/db",
	} {
		got, err := FullPathname(path)
		require.NoError(t, err)
		require.Equal(t, want, got, path)

		again, err := FullPathname(got)
		require.NoError(t, err)
		require.Equal(t, got, again, "a full name resolves to itself")
	}
}

// The name is resolved against the working directory of the call, not of a
// later one.
func TestFullPathname_UnixWorkingDirectoryOfTheCall(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	t.Chdir(first)
	wd, err := os.Getwd()
	require.NoError(t, err)
	got, err := FullPathname("db")
	require.NoError(t, err)

	t.Chdir(second)
	require.Equal(t, wd+"/db", got)
	again, err := FullPathname(got)
	require.NoError(t, err)
	require.Equal(t, got, again)
}
