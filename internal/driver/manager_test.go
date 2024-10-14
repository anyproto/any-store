package driver

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/registry"
	"github.com/anyproto/any-store/internal/syncpool"
)

func TestNewConnManager(t *testing.T) {
	sp := syncpool.NewSyncPool(1000)
	fr := registry.NewFilterRegistry(sp, 256)
	sr := registry.NewSortRegistry(sp, 256)
	tmpDir, tErr := os.MkdirTemp("", "")
	require.NoError(t, tErr)
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})
	t.Run("open-close", func(t *testing.T) {
		cm, err := NewConnManager(filepath.Join(tmpDir, "1.db"), nil, 1, 2, fr, sr, 1)
		require.NoError(t, err)
		require.NoError(t, cm.Close())
	})
	t.Run("empty version", func(t *testing.T) {
		conn, err := sqlite.OpenConn(filepath.Join(tmpDir, "empty.db"), sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadWrite)
		require.NoError(t, err)
		_ = conn.Close()
		_, err = NewConnManager(filepath.Join(tmpDir, "empty.db"), nil, 1, 2, fr, sr, 1)
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})
	t.Run("old version", func(t *testing.T) {
		cm, err := NewConnManager(filepath.Join(tmpDir, "old.db"), nil, 1, 2, fr, sr, 1)
		require.NoError(t, err)
		require.NoError(t, cm.Close())
		_, err = NewConnManager(filepath.Join(tmpDir, "old.db"), nil, 1, 2, fr, sr, 2)
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})
}
