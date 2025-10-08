package sentinel

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinelTracker_OnOpen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + lockFileSuffix

	tracker := New(dbPath)
	ctx := context.Background()

	dirty, err := tracker.OnOpen(ctx)
	require.NoError(t, err)
	assert.False(t, dirty, "should not be dirty when sentinel doesn't exist")

	file, err := os.Create(sentinelPath)
	require.NoError(t, err)
	file.Close()

	dirty, err = tracker.OnOpen(ctx)
	require.NoError(t, err)
	assert.True(t, dirty, "should be dirty when sentinel exists")

	os.Remove(sentinelPath)
}

func TestSentinelTracker_MarkDirty(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + lockFileSuffix

	tracker := New(dbPath)

	tracker.MarkDirty()

	_, err := os.Stat(sentinelPath)
	assert.NoError(t, err, "sentinel file should exist after MarkDirty")

	tracker.MarkDirty()
	_, err = os.Stat(sentinelPath)
	assert.NoError(t, err, "MarkDirty should be idempotent")

	os.Remove(sentinelPath)
}

func TestSentinelTracker_MarkClean(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	sentinelPath := dbPath + lockFileSuffix

	tracker := New(dbPath)

	tracker.MarkDirty()
	_, err := os.Stat(sentinelPath)
	require.NoError(t, err)

	tracker.MarkClean()

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist after MarkClean")

	tracker.MarkClean()
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "MarkClean should be idempotent")
}

func TestSentinelTracker_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	tracker := New(dbPath)

	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			tracker.MarkDirty()
			tracker.MarkClean()
			done <- true
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
