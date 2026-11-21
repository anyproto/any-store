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

// The sentinel file is named when the database is opened. After the working
// directory changes, a flush and a write use the marker beside the database,
// and neither they nor the close touch a file of the same name in the new
// working directory.
func TestRecovery_SentinelPathResolvedAtOpen(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	markerA := filepath.Join(dirA, "test.db.lock")
	markerB := filepath.Join(dirB, "test.db.lock")
	require.NoError(t, os.WriteFile(markerB, nil, 0o644)) // another database's marker

	ctx := context.Background()
	t.Chdir(dirA)
	db, err := Open(ctx, "test.db", &Config{Durability: DurabilityConfig{Sentinel: true}})
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1"}`)))
	_, err = os.Stat(markerA)
	require.NoError(t, err, "setup: a write sets the sentinel")

	t.Chdir(dirB)
	require.NoError(t, db.Flush(ctx, 0, FlushModeCheckpointPassive))
	_, err = os.Stat(markerA)
	require.True(t, os.IsNotExist(err), "the flush left the database's sentinel: %v", err)
	_, err = os.Stat(markerB)
	require.NoError(t, err, "the flush removed a file in the new working directory")

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc2"}`)))
	_, err = os.Stat(markerA)
	require.NoError(t, err, "a write did not set the database's sentinel")

	require.NoError(t, db.Close())
	_, err = os.Stat(markerB)
	require.NoError(t, err, "the close removed a file in the new working directory")
}

// An in-memory database has no files on disk: with the sentinel on, a write
// creates no marker beside the path it was opened with, and its close, which
// persists nothing, has none to clear.
func TestRecovery_InMemoryHasNoSentinelFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "mem.db")
	config := &Config{InMemory: true, Durability: DurabilityConfig{Sentinel: true}}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1"}`)))
	_, err = os.Stat(dbPath + ".lock")
	require.True(t, os.IsNotExist(err), "a write to an in-memory database created a sentinel file: %v", err)
	require.NoError(t, db.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, entries, "an in-memory database left files on disk")

	db, err = Open(ctx, dbPath, config)
	require.NoError(t, err)
	stats, err := db.Stats(ctx)
	require.NoError(t, err)
	require.False(t, stats.DirtyOnOpen)
	require.NoError(t, db.Close())
}
