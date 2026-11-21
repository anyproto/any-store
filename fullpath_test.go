package anystore

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// Outside Windows the name is not cleaned lexically: a relative path is joined
// to the working directory as written, and an absolute path is kept as given,
// so each ".." stays for the OS to follow.
func TestFullPathname_KeepsDotDot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows resolves with filepath.Abs")
	}
	t.Chdir(t.TempDir())
	wd, err := os.Getwd()
	require.NoError(t, err)

	got, err := fullPathname("x/../db")
	require.NoError(t, err)
	require.Equal(t, wd+string(filepath.Separator)+"x/../db", got)

	got, err = fullPathname("/x/../db")
	require.NoError(t, err)
	require.Equal(t, "/x/../db", got)
}

// In a working directory whose full name is longer than the OS takes, the
// relative name "test.db" opens but the full name does not. The database file
// is opened by the same full name as its sentinel file, so the open fails
// rather than going on with a sentinel file it cannot reach.
func TestFullPathname_NameTooLongFailsTheOpen(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("needs a PATH_MAX that a deep working directory exceeds")
	}
	t.Chdir(t.TempDir())
	name := strings.Repeat("d", 200)
	for range 25 { // 5000 bytes, past PATH_MAX on Linux (4096) and Darwin (1024)
		require.NoError(t, os.Mkdir(name, 0o755))
		require.NoError(t, os.Chdir(name))
	}
	f, err := os.Create("test.db.probe")
	require.NoError(t, err, "setup: a relative name opens")
	require.NoError(t, f.Close())

	db, err := Open(context.Background(), "test.db", &Config{Durability: DurabilityConfig{Sentinel: true}})
	if err == nil {
		_ = db.Close()
	}
	require.ErrorIs(t, err, syscall.ENAMETOOLONG)
}
