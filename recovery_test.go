package anystore

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/driver"
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

func TestRecovery_ForceFlush(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	flushCount := 0
	config := &Config{
		Recovery: RecoveryConfig{
			Enabled:             true,
			IdleAfter:           10 * time.Second,      // Very long idle time
			ForceFlushIdleAfter: 10 * time.Millisecond, // Short threshold for force flush
			OnIdleSafe: []recovery.OnIdleSafeCallback{
				func(stats recovery.Stats) {
					flushCount++
				},
			},
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	// Create collection and add data
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1, "data":"test"}`))
	require.NoError(t, err)

	// Wait just a bit to ensure we meet the ForceFlushIdleAfter threshold
	time.Sleep(15 * time.Millisecond)

	// Force flush immediately - should work even though not idle for regular flush
	err = db.ForceFlush(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, flushCount, "ForceFlush should trigger callback")

	// Check recovery state
	state := db.RecoveryState()
	assert.True(t, state.Enabled)
	assert.True(t, state.Success)

	// Force flush again
	err = db.ForceFlush(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, flushCount, "Second ForceFlush should also work")
}

func TestRecovery_ForceFlushNotEnabled(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Recovery disabled
	config := &Config{}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	// ForceFlush should return error when recovery is not enabled
	err = db.ForceFlush(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "recovery is not enabled")
}

func TestRecovery_ForceFlushWithTimeout(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	shouldBlock := true
	blockMutex := &sync.Mutex{}
	config := &Config{
		Recovery: RecoveryConfig{
			Enabled:             true,
			IdleAfter:           10 * time.Second,
			ForceFlushIdleAfter: 10 * time.Millisecond,
			Flush: func(ctx context.Context, conn *driver.Conn) (recovery.Stats, error) {
				blockMutex.Lock()
				block := shouldBlock
				blockMutex.Unlock()

				if block {
					// Block until context is cancelled
					<-ctx.Done()
					return recovery.Stats{}, ctx.Err()
				}
				return recovery.Stats{Success: true}, nil
			},
		},
	}

	ctx := context.Background()
	db, err := Open(ctx, dbPath, config)
	require.NoError(t, err)
	defer db.Close()

	// Do a write so we have a lastWriteTime
	coll, err := db.CreateCollection(ctx, "test")
	require.NoError(t, err)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`))
	require.NoError(t, err)

	// Wait a bit to meet ForceFlushIdleAfter threshold
	time.Sleep(15 * time.Millisecond)

	// Try force flush with timeout - should fail due to blocking
	ctxTimeout, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	err = db.ForceFlush(ctxTimeout)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")

	// Now unblock and try again
	blockMutex.Lock()
	shouldBlock = false
	blockMutex.Unlock()

	ctxTimeout2, cancel2 := context.WithTimeout(ctx, 1*time.Second)
	defer cancel2()
	err = db.ForceFlush(ctxTimeout2)
	assert.NoError(t, err)
}
