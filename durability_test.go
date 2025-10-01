package anystore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestRecovery_SentinelCleanShutdown(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 100 * time.Millisecond,
			Sentinel:  true,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)

	_, err = os.Stat(sentinelPath)
	assert.NoError(t, err, "sentinel file should exist after database open")

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1", "data":"test"}`))
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	err = db.Close()
	require.NoError(t, err)

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should be removed after clean shutdown")

	db2, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	iter, err := coll2.Find(`{"id":"doc1"}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	assert.True(t, iter.Next())
	doc, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, "test", string(doc.Value().GetStringBytes("data")))
}

func TestRecovery_SentinelDirtyShutdown(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"
	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 10 * time.Minute,
			Sentinel:  true,
		},
	}

	ctx := context.Background()

	{
		db, err := Open(ctx, dbPath, config)
		require.NoError(t, err)

		_, err = os.Stat(sentinelPath)
		assert.NoError(t, err, "sentinel file should exist")

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1", "data":"test"}`))
		require.NoError(t, err)

		db.Close()

		file, err := os.Create(sentinelPath)
		require.NoError(t, err)
		file.Close()
	}

	_, err := os.Stat(sentinelPath)
	require.NoError(t, err, "sentinel should exist to simulate dirty state")

	db2, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	iter, err := coll2.Find(`{"id":"doc1"}`).Iter(ctx)
	require.NoError(t, err)
	defer iter.Close()

	assert.True(t, iter.Next())
	doc, err := iter.Doc()
	require.NoError(t, err)
	assert.Equal(t, "test", string(doc.Value().GetStringBytes("data")))
}

func TestRecovery_IdleFlushIntegration(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 100 * time.Millisecond,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	doc := anyenc.MustParseJson(`{"id":1, "data":"test"}`)
	err = coll.Insert(ctx, doc)
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
}

func TestRecovery_DisableSentinel(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 100 * time.Millisecond,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist when DisableSentinel is true")

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist after flush when DisableSentinel is true")
}

func TestRecovery_FlushModes(t *testing.T) {
	testCases := []struct {
		name string
		mode FlushMode
	}{
		{"FsyncOnly", FlushModeFsync},
		{"Passive", FlushModeCheckpointPassive},
		{"Full", FlushModeCheckpointFull},
		{"Restart", FlushModeCheckpointRestart},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "test.db")

			config := &Config{
				Durability: DurabilityConfig{
					AutoFlush: true,
					IdleAfter: 100 * time.Millisecond,
					FlushMode: tc.mode,
				},
			}

			ctx := context.Background()
			db, err := Open(ctx, dbPath, config)
			require.NoError(t, err)

			coll, err := db.CreateCollection(ctx, "test")
			require.NoError(t, err)

			err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
			require.NoError(t, err)

			time.Sleep(300 * time.Millisecond)

			err = db.Close()
			require.NoError(t, err)
		})
	}
}

func TestRecovery_ManualFlush(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: false,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "data":"test"}`))
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	err = db.Flush(ctx, 10*time.Millisecond, FlushModeCheckpointPassive)
	require.NoError(t, err)

	err = db.Flush(ctx, 10*time.Millisecond, FlushModeCheckpointPassive)
	require.NoError(t, err)
}

func TestRecovery_ForceFlushImmediatelyAfterWrite(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	dbPath := filepath.Join(dir, "test.db")
	db, err := Open(context.Background(), dbPath, &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 10 * time.Second,
			FlushMode: FlushModeCheckpointPassive,
		},
	})
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
	require.NoError(t, err)

	start := time.Now()
	err = db.Flush(ctx, 50*time.Millisecond, FlushModeCheckpointPassive)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Less(t, elapsed, 100*time.Millisecond, "ForceFlush should complete quickly")
	assert.Greater(t, elapsed, 40*time.Millisecond, "ForceFlush should wait for idle time")
}

func TestRecovery_ForceFlushWithTimeout(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	config := &Config{
		Durability: DurabilityConfig{
			AutoFlush: true,
			IdleAfter: 10 * time.Second,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
	require.NoError(t, err)

	time.Sleep(15 * time.Millisecond)

	ctxTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err = db.Flush(ctxTimeout, 10*time.Millisecond, FlushModeCheckpointPassive)
	assert.NoError(t, err)

	stopWrites := make(chan struct{})
	go func() {
		for {
			select {
			case <-stopWrites:
				return
			default:
				_ = coll.Insert(context.Background(), anyenc.MustParseJson(`{"id":2}`))
				time.Sleep(5 * time.Millisecond)
			}
		}
	}()

	ctxTimeout2, cancel2 := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel2()
	err = db.Flush(ctxTimeout2, 50*time.Millisecond, FlushModeCheckpointPassive)
	close(stopWrites)
	assert.Error(t, err)
	if err != nil {
		assert.Contains(t, err.Error(), "cancelled")
	}
}
