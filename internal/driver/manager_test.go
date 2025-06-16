package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/go-sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/internal/registry"
	"github.com/anyproto/any-store/syncpool"
)

func TestNewConnManager(t *testing.T) {
	sp := syncpool.NewSyncPool(1000)
	fr := registry.NewFilterRegistry(sp, 256)
	sr := registry.NewSortRegistry(sp, 256)
	tmpDir, tErr := os.MkdirTemp("", "")
	require.NoError(t, tErr)
	conf := Config{
		ReadCount:                 2,
		PreAllocatedPageCacheSize: 30,
		SortRegistry:              sr,
		FilterRegistry:            fr,
		Version:                   1,
		ReadConnTTL:               time.Minute,
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(tmpDir)
	})
	t.Run("open-close", func(t *testing.T) {
		cm, err := NewConnManager(filepath.Join(tmpDir, "1.db"), conf)
		require.NoError(t, err)
		require.NoError(t, cm.Close())
	})
	t.Run("empty version", func(t *testing.T) {
		conn, err := sqlite.OpenConn(filepath.Join(tmpDir, "empty.db"), sqlite.OpenCreate|sqlite.OpenWAL|sqlite.OpenURI|sqlite.OpenReadWrite)
		require.NoError(t, err)
		_ = conn.Close()
		_, err = NewConnManager(filepath.Join(tmpDir, "empty.db"), conf)
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})
	t.Run("old version", func(t *testing.T) {
		cm, err := NewConnManager(filepath.Join(tmpDir, "old.db"), conf)
		require.NoError(t, err)
		require.NoError(t, cm.Close())
		conf.Version = 0
		_, err = NewConnManager(filepath.Join(tmpDir, "old.db"), conf)
		require.ErrorIs(t, err, ErrIncompatibleVersion)
	})
}

var ctx = context.Background()

func TestConnManager_GetRead(t *testing.T) {
	fx := newFixture(t)

	var assertNilORClosed = func(err error) {
		if err != nil {
			assert.True(t,
				errors.Is(err, ErrStmtIsClosed) || errors.Is(err, ErrDBIsClosed),
				"expected ErrStmtIsClosed or ErrDBIsClosed, got %v", err)
		}
	}

	var numP = 10
	for range numP {
		go func() {
			for {
				conn, err := fx.GetRead(ctx)
				if errors.Is(err, ErrDBIsClosed) {
					return
				}
				require.NoError(t, err)
				assertNilORClosed(conn.Begin(ctx))

				time.Sleep(time.Millisecond * 10)
				assertNilORClosed(conn.Commit(ctx))
				fx.ReleaseRead(conn)
			}
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
	cm, err := NewConnManager(filepath.Join(tmpDir, "1.db"), Config{
		ReadCount:                 2,
		PreAllocatedPageCacheSize: 30,
		SortRegistry:              sr,
		FilterRegistry:            fr,
		Version:                   1,
		ReadConnTTL:               time.Minute,
	})
	require.NoError(t, err)
	return cm
}
