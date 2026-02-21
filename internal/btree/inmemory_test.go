package btree

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemory_OpenClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InMemory = true
	db, err := Open(path, opts)
	require.NoError(t, err)

	// Verify no files created on disk
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err), "database file should not exist for InMemory")
	_, err = os.Stat(path + "-wal")
	assert.True(t, os.IsNotExist(err), "WAL file should not exist for InMemory")
	_, err = os.Stat(path + "-wal-shm")
	assert.True(t, os.IsNotExist(err), "SHM file should not exist for InMemory")

	require.NoError(t, db.Close())
}

func TestInMemory_WriteAndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InMemory = true
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// Create namespace and write data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("value1")))
	require.NoError(t, tx.Put(ns, []byte("key2"), []byte("value2")))
	require.NoError(t, tx.Commit())

	// Read data back
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, err := rtx.GetNamespace("test")
	require.NoError(t, err)
	val, err := rtx.Get(ns2, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value1"), val)
	val, err = rtx.Get(ns2, []byte("key2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value2"), val)
	require.NoError(t, rtx.Rollback())
}

func TestInMemory_Checkpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InMemory = true
	opts.DisableAutoCheckpoint = true
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// Write data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := range 100 {
		key := []byte("key-" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		require.NoError(t, tx.Put(ns, key, []byte("value")))
	}
	require.NoError(t, tx.Commit())

	// Manual checkpoint should succeed (moves frames to pcache)
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Data should still be readable after checkpoint
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, err := rtx.GetNamespace("test")
	require.NoError(t, err)
	count, err := rtx.Count(ns2)
	require.NoError(t, err)
	assert.Equal(t, 100, count)
	require.NoError(t, rtx.Rollback())

	// No files on disk
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestInMemory_ConcurrentReadWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InMemory = true
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// Create namespace
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Write some initial data
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = tx.GetNamespace("test")
	require.NoError(t, err)
	for i := range 50 {
		key := []byte(fmt.Sprintf("key-%04d", i))
		require.NoError(t, tx.Put(ns, key, []byte("initial")))
	}
	require.NoError(t, tx.Commit())

	// Start concurrent readers
	var wg sync.WaitGroup
	errors := make(chan error, 10)

	for r := range 5 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := range 10 {
				rtx, err := db.BeginRead()
				if err != nil {
					errors <- err
					return
				}
				rns, err := rtx.GetNamespace("test")
				if err != nil {
					errors <- err
					rtx.Rollback()
					return
				}
				_, err = rtx.Count(rns)
				if err != nil {
					errors <- err
					rtx.Rollback()
					return
				}
				rtx.Rollback()
				_ = i
			}
		}(r)
	}

	// Write more data concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; i < 100; i++ {
			wtx, err := db.BeginWrite()
			if err != nil {
				errors <- err
				return
			}
			wns, err := wtx.GetNamespace("test")
			if err != nil {
				errors <- err
				wtx.Rollback()
				return
			}
			key := []byte(fmt.Sprintf("key-%04d", i))
			if err := wtx.Put(wns, key, []byte("concurrent")); err != nil {
				errors <- err
				wtx.Rollback()
				return
			}
			if err := wtx.Commit(); err != nil {
				errors <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}

	// Verify final state
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, err := rtx.GetNamespace("test")
	require.NoError(t, err)
	count, err := rtx.Count(ns3)
	require.NoError(t, err)
	assert.Equal(t, 100, count)
	require.NoError(t, rtx.Rollback())
}

func TestInMemory_ForcesFlags(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := Options{
		InMemory: true,
		// Don't set InProcess or NoCommitSync — they should be forced
	}
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// Verify the options were forced
	assert.True(t, db.opts.InProcess, "InProcess should be forced true for InMemory")
	assert.True(t, db.opts.NoCommitSync, "NoCommitSync should be forced true for InMemory")
}

func TestInProcess_NoCommitSync_WritesToDisk(t *testing.T) {
	// Verify that InProcess+NoCommitSync (without InMemory) writes WAL frames to disk
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InProcess = true
	opts.NoCommitSync = true
	opts.DisableAutoCheckpoint = true
	db, err := Open(path, opts)
	require.NoError(t, err)

	// Write data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key"), []byte("value")))
	require.NoError(t, tx.Commit())

	// WAL file should exist and have non-zero size
	walInfo, err := os.Stat(path + "-wal")
	require.NoError(t, err, "WAL file should exist for disk-backed InProcess+NoCommitSync")
	assert.Greater(t, walInfo.Size(), int64(0), "WAL file should have data written")

	require.NoError(t, db.Close())
}

func TestInMemory_MultipleNamespaces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InMemory = true
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// Create multiple namespaces with data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns1, err := tx.CreateNamespace("ns1")
	require.NoError(t, err)
	ns2, err := tx.CreateNamespace("ns2")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns1, []byte("a"), []byte("1")))
	require.NoError(t, tx.Put(ns2, []byte("b"), []byte("2")))
	require.NoError(t, tx.Commit())

	// Verify both namespaces
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	rns1, err := rtx.GetNamespace("ns1")
	require.NoError(t, err)
	rns2, err := rtx.GetNamespace("ns2")
	require.NoError(t, err)
	v1, err := rtx.Get(rns1, []byte("a"))
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), v1)
	v2, err := rtx.Get(rns2, []byte("b"))
	require.NoError(t, err)
	assert.Equal(t, []byte("2"), v2)
	require.NoError(t, rtx.Rollback())
}

func TestInMemory_LargeData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InMemory = true
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// Write enough data to trigger multiple pages and potentially overflow
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := range 1000 {
		key := []byte(fmt.Sprintf("key-%06d", i))
		value := make([]byte, 100)
		for j := range value {
			value[j] = byte(i + j)
		}
		require.NoError(t, tx.Put(ns, key, value))
	}
	require.NoError(t, tx.Commit())

	// Checkpoint and verify data survives
	require.NoError(t, db.Checkpoint(CheckpointFull))

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, err := rtx.GetNamespace("test")
	require.NoError(t, err)
	count, err := rtx.Count(ns2)
	require.NoError(t, err)
	assert.Equal(t, 1000, count)

	// Spot-check a few values
	for _, i := range []int{0, 500, 999} {
		key := []byte(fmt.Sprintf("key-%06d", i))
		val, err := rtx.Get(ns2, key)
		require.NoError(t, err)
		assert.Len(t, val, 100)
		assert.Equal(t, byte(i), val[0])
	}
	require.NoError(t, rtx.Rollback())
}

func TestInMemory_Savepoints(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := DefaultOptions()
	opts.InMemory = true
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("v1")))

	sp, err := tx.Savepoint()
	require.NoError(t, err)

	require.NoError(t, tx.Put(ns, []byte("key2"), []byte("v2")))

	// Rollback to savepoint
	require.NoError(t, tx.RollbackToSavepoint(sp))

	// key2 should not exist
	_, err = tx.Get(ns, []byte("key2"))
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// key1 should still exist
	val, err := tx.Get(ns, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), val)

	require.NoError(t, tx.Commit())
}
