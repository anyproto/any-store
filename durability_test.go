package anystore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// The idle flush must keep running after the context passed to Open is
// cancelled: callers open a DB with a call-scoped context and keep the
// DB for much longer.
func TestRecovery_IdleFlushOutlivesOpenCtx(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 500 * time.Millisecond,
			Sentinel:  true,
		},
	}

	openCtx, cancel := context.WithCancel(context.Background())
	db, err := Open(openCtx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()
	cancel()

	ctx := context.Background()
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1", "data":"test"}`)))

	_, err = os.Stat(sentinelPath)
	require.NoError(t, err, "sentinel file should exist after write")

	require.Eventually(t, func() bool {
		_, err := os.Stat(sentinelPath)
		return os.IsNotExist(err)
	}, 5*time.Second, 20*time.Millisecond, "idle flush should mark the sentinel clean after the open ctx is cancelled")
}
