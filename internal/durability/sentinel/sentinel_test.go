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

	tracker, err := New(dbPath)
	require.NoError(t, err)
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

	tracker, err := New(dbPath)
	require.NoError(t, err)

	tracker.MarkDirty()

	_, err = os.Stat(sentinelPath)
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

	tracker, err := New(dbPath)
	require.NoError(t, err)

	tracker.MarkDirty()
	_, err = os.Stat(sentinelPath)
	require.NoError(t, err)

	tracker.MarkClean()

	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "sentinel file should not exist after MarkClean")

	tracker.MarkClean()
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "MarkClean should be idempotent")
}

// Remove does not consult the tracker's cached state: the file may be
// another process's. A file that is not there counts as removed.
func TestSentinelTracker_RemoveIgnoresCachedState(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	sentinelPath := dbPath + lockFileSuffix

	tracker, err := New(dbPath)
	require.NoError(t, err)
	dirty, err := tracker.OnOpen(context.Background())
	require.NoError(t, err)
	require.False(t, dirty)
	require.NoError(t, os.WriteFile(sentinelPath, nil, 0o644)) // another process's marker

	tracker.Remove()
	_, err = os.Stat(sentinelPath)
	assert.True(t, os.IsNotExist(err), "Remove left a marker the tracker had not seen: %v", err)

	// A marker this tracker set and another process removed: Remove finds
	// none and counts it as removed, so the next write creates it again.
	tracker.MarkDirty()
	_, err = os.Stat(sentinelPath)
	require.NoError(t, err, "setup: a write creates the marker")
	require.NoError(t, os.Remove(sentinelPath)) // another process's flush
	tracker.Remove()
	tracker.MarkDirty()
	_, err = os.Stat(sentinelPath)
	assert.NoError(t, err, "a write after Remove found no marker did not create it")
}

// The tracker's path is made absolute by New: after the working directory
// changes, it still names the marker beside the database, and leaves alone a
// file of the same name in the new working directory.
func TestSentinelTracker_PathResolvedAtNew(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	markerA := filepath.Join(dirA, "test.db"+lockFileSuffix)
	markerB := filepath.Join(dirB, "test.db"+lockFileSuffix)
	require.NoError(t, os.WriteFile(markerB, nil, 0o644)) // another database's marker

	t.Chdir(dirA)
	tracker, err := New("test.db")
	require.NoError(t, err)
	tracker.MarkDirty()
	_, err = os.Stat(markerA)
	require.NoError(t, err, "setup: a write creates the marker")

	t.Chdir(dirB)
	tracker.Remove()
	_, err = os.Stat(markerA)
	assert.True(t, os.IsNotExist(err), "Remove left the database's marker: %v", err)
	_, err = os.Stat(markerB)
	assert.NoError(t, err, "Remove removed a file in the new working directory")

	tracker.MarkDirty()
	_, err = os.Stat(markerA)
	assert.NoError(t, err, "MarkDirty did not create the database's marker")
	tracker.MarkClean()
	_, err = os.Stat(markerA)
	assert.True(t, os.IsNotExist(err), "MarkClean left the database's marker: %v", err)
	_, err = os.Stat(markerB)
	assert.NoError(t, err, "MarkClean removed a file in the new working directory")
}

func TestSentinelTracker_ConcurrentAccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	tracker, err := New(dbPath)
	require.NoError(t, err)

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
