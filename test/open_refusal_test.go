package test

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
)

// A second Open of a file this process has open is refused with
// ErrDBAlreadyOpen, by whatever path it names the file, and the open handle
// goes on working. Backup into the open file is refused the same way.
func TestOpen_SecondOpenInProcess(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "a.db")
	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()

	// The "./" spelling is built by hand: filepath.Join would clean it away.
	dotted := dir + string(filepath.Separator) + "." + string(filepath.Separator) + "a.db"
	require.NotEqual(t, path, dotted)
	paths := []string{path, dotted}
	if runtime.GOOS != "windows" && runtime.GOOS != "js" && runtime.GOOS != "wasip1" {
		sym := filepath.Join(dir, "sym.db")
		if err := os.Symlink(path, sym); err == nil {
			paths = append(paths, sym)
		} else {
			t.Logf("no symbolic link on this filesystem: %v", err)
		}
		hard := filepath.Join(dir, "hard.db")
		if err := os.Link(path, hard); err == nil {
			paths = append(paths, hard)
		} else {
			t.Logf("no hard link on this filesystem: %v", err)
		}
	}
	for _, p := range paths {
		_, err := anystore.Open(ctx, p, nil)
		require.ErrorIs(t, err, anystore.ErrDBAlreadyOpen, p)
		require.NotErrorIs(t, err, anystore.ErrCorrupt, p)
		require.NotErrorIs(t, err, anystore.ErrBusy, p)
	}

	other, err := anystore.Open(ctx, filepath.Join(dir, "b.db"), nil)
	require.NoError(t, err)
	defer other.Close()
	require.ErrorIs(t, other.Backup(ctx, path), anystore.ErrDBAlreadyOpen)

	_, err = db.CreateCollection(ctx, "still-open")
	require.NoError(t, err)

	// Closed, the file opens again.
	require.NoError(t, db.Close())
	db2, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	require.NoError(t, db2.Close())
}
