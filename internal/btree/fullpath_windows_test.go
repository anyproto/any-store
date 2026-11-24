package btree

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// A relative path becomes a full one under the working directory, and a
// full name resolves to itself.
func TestFullPathname_Windows(t *testing.T) {
	t.Chdir(t.TempDir())
	wd, err := os.Getwd()
	require.NoError(t, err)

	got, err := FullPathname("db")
	require.NoError(t, err)
	require.True(t, filepath.IsAbs(got), got)
	require.Equal(t, filepath.Join(wd, "db"), got)

	again, err := FullPathname(got)
	require.NoError(t, err)
	require.Equal(t, got, again, "a full name resolves to itself")
}
