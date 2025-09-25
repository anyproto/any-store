package sentinel

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/internal/recovery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinelTracker_OnOpen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	tracker, _ := New(dbPath)
	ctx := context.Background()

	// Test clean state (no sentinel file)
	dirty, err := tracker.OnOpen(ctx)
	require.NoError(t, err)
	assert.False(t, dirty, "should not be dirty when sentinel doesn't exist")

	// Create sentinel file manually
	file, err := os.Create(sentinelPath)
	require.NoError(t, err)
	file.Close()

	// Test dirty state (sentinel exists)
	dirty, err = tracker.OnOpen(ctx)
	require.NoError(t, err)
	assert.True(t, dirty, "should be dirty when sentinel exists")

	// Clean up
	os.Remove(sentinelPath)
}

func TestSentinelTracker_MarkDirty(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	tracker, _ := New(dbPath)

	// Mark dirty
	tracker.MarkDirty()

	// Verify sentinel file was created
	_, err := os.Stat(sentinelPath)
	assert.NoError(t, err, "sentinel file should exist after MarkDirty")

	// Mark dirty again (should be idempotent)
	tracker.MarkDirty()
	_, err = os.Stat(sentinelPath)
	assert.NoError(t, err, "sentinel file should still exist")

	// Clean up
	os.Remove(sentinelPath)
}

func TestSentinelTracker_MarkClean(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	tracker, _ := New(dbPath)

	// Create sentinel first
	tracker.MarkDirty()
	_, err := os.Stat(sentinelPath)
	require.NoError(t, err)

	// Mark clean
	tracker.MarkClean()

	// Verify sentinel file was removed
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist after MarkClean")

	// Mark clean again (should be idempotent)
	tracker.MarkClean()
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err))
}

func TestSentinelTracker_OnIdleSafeCallback(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + ".lock"

	tracker, onIdleSafe := New(dbPath)

	// Create sentinel
	tracker.MarkDirty()
	_, err := os.Stat(sentinelPath)
	require.NoError(t, err)

	// Call the OnIdleSafe callback
	stats := recovery.Stats{
		Success: true,
	}
	onIdleSafe(stats)

	// Verify sentinel was removed
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel should be removed by OnIdleSafe callback")
}

func TestSentinelTracker_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	tracker, _ := New(dbPath)

	// Test concurrent marking
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			tracker.MarkDirty()
			tracker.MarkClean()
			done <- true
		}()
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// No assertion needed - test passes if no panic/race
}

func TestSentinelTracker_NestedDirectories(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "nested", "path", "test.db")
	sentinelPath := dbPath + ".lock"

	tracker, _ := New(dbPath)

	// Mark dirty should create nested directories if needed
	tracker.MarkDirty()

	// Verify sentinel file was created in nested directory
	_, err := os.Stat(sentinelPath)
	assert.NoError(t, err, "sentinel should be created even in nested directories")

	// Clean up
	os.RemoveAll(filepath.Join(dir, "nested"))
}
