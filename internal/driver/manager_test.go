package driver

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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
		cm, err := NewConnManager(filepath.Join(tmpDir, "1.db"), nil, 1, 2, 30, fr, sr, 1)
		require.NoError(t, err)
		require.NoError(t, cm.Close())
	})
	t.Run("empty version", func(t *testing.T) {
		conn, err := sqlite.OpenConn(filepath.Join(tmpDir, "empty.db"), sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadWrite)
		require.NoError(t, err)
		_ = conn.Close()
		_, err = NewConnManager(filepath.Join(tmpDir, "empty.db"), nil, 1, 2, 30, fr, sr, 1)
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})
	t.Run("old version", func(t *testing.T) {
		cm, err := NewConnManager(filepath.Join(tmpDir, "old.db"), nil, 1, 2, 30, fr, sr, 1)
		require.NoError(t, err)
		require.NoError(t, cm.Close())
		_, err = NewConnManager(filepath.Join(tmpDir, "old.db"), nil, 1, 2, 30, fr, sr, 2)
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})
}

var ctx = context.Background()

func TestConnManager_GetRead(t *testing.T) {
	fx := newFixture(t)

	var numP = 10
	for i := 0; i < numP; i++ {
		go func() {
			conn, err := fx.GetRead(ctx)
			require.NoError(t, err)
			require.NoError(t, conn.Begin(ctx))
			time.Sleep(time.Millisecond * 10)
			require.NoError(t, conn.Commit(ctx))
			fx.ReleaseRead(conn)
		}()
		time.Sleep(time.Millisecond * 100)
	}

	time.Sleep(time.Second * 5)

}

func newFixture(t *testing.T) *ConnManager {
	sp := syncpool.NewSyncPool(1000)
	fr := registry.NewFilterRegistry(sp, 256)
	sr := registry.NewSortRegistry(sp, 256)
	tmpDir, tErr := os.MkdirTemp("", "")
	require.NoError(t, tErr)
	var cm *ConnManager
	t.Cleanup(func() {
		if cm != nil {
			require.NoError(t, cm.Close())
		}
		_ = os.RemoveAll(tmpDir)
	})
	cm, err := NewConnManager(filepath.Join(tmpDir, "1.db"), nil, 1, 2, 30, fr, sr, 1)
	require.NoError(t, err)
	return cm
}
