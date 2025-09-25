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
	"github.com/anyproto/any-store/internal/recovery"
)

func TestRecovery_SentinelCleanShutdown(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	// Open database with recovery enabled
	config := &Config{
		Recovery: RecoveryConfig{
			Enabled:     true,
			IdleAfter:   100 * time.Millisecond,
			UseSentinel: true,
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)

	// Verify sentinel file is created
	_, err = os.Stat(sentinelPath)
	assert.NoError(t, err, "sentinel file should exist after database open")

	// Create a collection and add some data
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1", "data":"test"}`))
	require.NoError(t, err)

	// Wait for idle flush
	time.Sleep(200 * time.Millisecond)

	// Check recovery state
	state := db.RecoveryState()
	assert.True(t, state.Enabled)

	// Close database normally
	err = db.Close()
	require.NoError(t, err)

	// Verify sentinel file is removed after clean shutdown
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should be removed after clean shutdown")

	// Reopen database - should not require QuickCheck
	db2, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db2.Close()

	// Verify data is intact
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
		Recovery: RecoveryConfig{
			Enabled:           true,
			IdleAfter:         10 * time.Minute, // Long idle time to prevent flush
			UseSentinel:       true,
			QuickCheckTimeout: 5 * time.Second,
		},
	}

	ctx := context.Background()

	// First, create database and add data
	{
		db, err := Open(ctx, dbPath, config)
		require.NoError(t, err)

		// Verify sentinel file is created
		_, err = os.Stat(sentinelPath)
		assert.NoError(t, err, "sentinel file should exist")

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)

		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"doc1", "data":"test"}`))
		require.NoError(t, err)

		// Simulate dirty shutdown by manually creating sentinel
		// (normally Close() would remove it)
		db.Close()

		// Recreate sentinel to simulate crash
		file, err := os.Create(sentinelPath)
		require.NoError(t, err)
		file.Close()
	}

	// Verify sentinel exists (simulating dirty state)
	_, err := os.Stat(sentinelPath)
	require.NoError(t, err, "sentinel should exist to simulate dirty state")

	// Reopen database - should detect dirty state and run QuickCheck
	db2, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db2.Close()

	// Verify data is intact after recovery
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

	flushCount := 0
	config := &Config{
		Recovery: RecoveryConfig{
			Enabled:     true,
			IdleAfter:   100 * time.Millisecond,
			UseSentinel: true,
			OnIdleSafe: []recovery.OnIdleSafeCallback{
				func(stats recovery.Stats) {
					flushCount++
					assert.True(t, stats.Success)
				},
			},
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	// Create collection and insert data
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// Perform a write
	doc := anyenc.MustParseJson(`{"id":1, "data":"test"}`)
	err = coll.Insert(ctx, doc)
	require.NoError(t, err)

	// Wait for idle flush to trigger (needs to wait for idle period after last write)
	time.Sleep(200 * time.Millisecond)

	// Verify flush was called at least once
	assert.Greater(t, flushCount, 0, "idle flush should have been triggered")

	// Check recovery state
	state := db.RecoveryState()
	assert.True(t, state.Enabled)
	if flushCount > 0 {
		assert.True(t, state.Success)
		assert.NotZero(t, state.LastFlushTime)
	}
}

func TestRecovery_Disabled(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	// Open database with recovery disabled (default)
	config := &Config{}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	// Verify sentinel file is NOT created
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist when recovery is disabled")

	// Check recovery state shows disabled
	state := db.RecoveryState()
	assert.False(t, state.Enabled)
}

func TestRecovery_CheckpointModes(t *testing.T) {
	testCases := []struct {
		name string
		mode CheckpointMode
	}{
		{"Passive", CheckpointPassive},
		{"Full", CheckpointFull},
		{"Truncate", CheckpointTruncate},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "test.db")

			config := &Config{
				Recovery: RecoveryConfig{
					Enabled:        true,
					IdleAfter:      100 * time.Millisecond,
					CheckpointMode: tc.mode,
				},
			}

			ctx := context.Background()
			db, err := Open(ctx, dbPath, config)
			require.NoError(t, err)

			// Create collection and add data
			coll, err := db.CreateCollection(ctx, "test")
			require.NoError(t, err)

			err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
			require.NoError(t, err)

			// Wait for idle flush
			time.Sleep(300 * time.Millisecond)

			// Check recovery state
			state := db.RecoveryState()
			if state.Success {
				assert.Equal(t, string(tc.mode), state.CheckpointMode)
			}

			err = db.Close()
			require.NoError(t, err)
		})
	}
}