package conn

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/internal/registry"
	"github.com/anyproto/any-store/internal/syncpool"
)

func TestConnManager(t *testing.T) {
	sp := syncpool.NewSyncPool()
	driver := NewDriver(registry.NewFilterRegistry(sp, 4), registry.NewSortRegistry(sp, 4))

	tmpDir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	defer func() {
		_ = os.RemoveAll(tmpDir)
	}()

	cm, err := NewConnManager(driver, filepath.Join(tmpDir, "test.db"), 1, 5)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, cm.Close())
	}()

	conn, err := cm.GetWrite(context.Background())
	require.NoError(t, err)

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = cm.GetWrite(canceledContext)
	assert.ErrorIs(t, err, context.Canceled)

	cm.ReleaseWrite(conn)
}
