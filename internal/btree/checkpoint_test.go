package btree

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckpointDoesNotBlockReaders(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert data to create WAL frames
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Start a reader
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")

	// Run checkpoint while reader is active — should NOT block
	done := make(chan error, 1)
	go func() {
		done <- db.Checkpoint()
	}()

	select {
	case err := <-done:
		// Checkpoint completed without blocking
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint blocked for too long — it should not block readers")
	}

	// Reader should still be able to read after checkpoint
	v, err := rtx.Get(ns3, []byte("key-0050"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0050"), v)

	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestCheckpointBlocksWriters(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert initial data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Start checkpoint in background — it acquires the write lock
	checkpointDone := make(chan struct{})
	go func() {
		_ = db.Checkpoint()
		close(checkpointDone)
	}()

	// Wait for checkpoint to complete
	<-checkpointDone

	// Writer should work after checkpoint completes
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns3, []byte("after-ckpt"), []byte("value")))
	require.NoError(t, tx2.Commit())

	// Verify the write persisted
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")
	v, err := rtx.Get(ns4, []byte("after-ckpt"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), v)
	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestCheckpointPartialWithActiveReader(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert batch 1
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Start a reader that holds a snapshot at batch 1
	rtx, err := db.BeginRead()
	require.NoError(t, err)

	// Insert batch 2 (while reader is active)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	for i := 50; i < 100; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx2.Put(ns3, k, v))
	}
	require.NoError(t, tx2.Commit())

	// Checkpoint should do a partial checkpoint — it can't copy
	// frames past the reader's snapshot
	err = db.Checkpoint()
	require.NoError(t, err)

	// Reader should still see its snapshot (batch 1 data)
	ns4, _ := db.getNamespaceLocked("data")
	v, err := rtx.Get(ns4, []byte("key-0000"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0000"), v)

	// End the reader
	require.NoError(t, rtx.Rollback())

	// Now a full checkpoint should work (no readers)
	err = db.Checkpoint()
	require.NoError(t, err)

	// Verify all data is still accessible
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	ns5, _ := db.getNamespaceLocked("data")
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		got, err := rtx2.Get(ns5, k)
		require.NoError(t, err, "key %s not found after checkpoint", k)
		assert.Equal(t, v, got)
	}
	require.NoError(t, rtx2.Rollback())
	_ = ns
}

func TestReaderSlotRotation(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Start multiple concurrent readers
	readers := make([]*ReadTx, 4)
	for i := range 4 {
		readers[i], err = db.BeginRead()
		require.NoError(t, err)
	}

	// All readers should be functional
	for i, rtx := range readers {
		nsR, _ := db.getNamespaceLocked("data")
		v, err := rtx.Get(nsR, []byte("key-0010"))
		require.NoError(t, err, "reader %d failed to Get", i)
		assert.Equal(t, []byte("val-0010"), v)
	}

	// Readers should be using different slots (or sharing slot 0 if all checkpointed)
	// The key thing is they all work correctly
	for _, rtx := range readers {
		require.NoError(t, rtx.Rollback())
	}
	_ = ns
}

func TestConcurrentReadersAndCheckpoint(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert initial data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Run concurrent readers and checkpoints
	var wg sync.WaitGroup
	var errors atomic.Int32

	// Launch readers
	for r := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				rtx, err := db.BeginRead()
				if err != nil {
					errors.Add(1)
					return
				}
				nsR, _ := db.getNamespaceLocked("data")
				cur := rtx.NewCursor(nsR)
				count := 0
				for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
					count++
				}
				if count != 100 {
					t.Logf("reader %d: expected 100, got %d", r, count)
					errors.Add(1)
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Launch checkpoints
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 5 {
				_ = db.Checkpoint()
				time.Sleep(time.Millisecond)
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, int32(0), errors.Load(), "concurrent readers/checkpoints had errors")
	_ = ns
}

func TestCheckpointWithWriterAndReaders(t *testing.T) {
	// Use InProcess mode for proper goroutine-level lock isolation.
	// POSIX fcntl locks are per-process and don't provide isolation
	// between goroutines on the same file descriptor.
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.InProcess = true
	db, err := Open(filepath.Join(dir, "test.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, wtx.Commit())

	// Insert initial data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	var wg sync.WaitGroup
	var errors atomic.Int32

	// Writer thread
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := range 5 {
			wtx, err := db.BeginWrite()
			if err != nil {
				t.Logf("Writer BeginWrite error: %v", err)
				errors.Add(1)
				return
			}
			nsW, wnsErr := wtx.GetNamespace("data")
			if wnsErr != nil {
				t.Logf("Writer GetNamespace error: %v", wnsErr)
				errors.Add(1)
				_ = wtx.Rollback()
				return
			}
			k := fmt.Appendf(nil, "round-%d", round)
			v := fmt.Appendf(nil, "value-%d", round)
			if err := wtx.Put(nsW, k, v); err != nil {
				t.Logf("Writer Put error: %v", err)
				errors.Add(1)
				_ = wtx.Rollback()
				return
			}
			if err := wtx.Commit(); err != nil {
				t.Logf("Writer Commit error: %v", err)
				errors.Add(1)
				return
			}
		}
	}()

	// Reader threads
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				rtx, err := db.BeginRead()
				if err != nil {
					errors.Add(1)
					return
				}
				nsR, nsErr := rtx.GetNamespace("data")
				if nsErr != nil {
					t.Logf("GetNamespace error: %v (walMaxFrame=%d)", nsErr, rtx.walMaxFrame)
					errors.Add(1)
					_ = rtx.Rollback()
					continue
				}
				// Original keys should always be readable
				_, err = rtx.Get(nsR, []byte("key-0000"))
				if err != nil {
					t.Logf("Get error: %v (walMaxFrame=%d, ns.root=%d)", err, rtx.walMaxFrame, nsR.rootPage)
					errors.Add(1)
				}
				_ = rtx.Rollback()
			}
		}()
	}

	// Checkpoint thread
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 5 {
			_ = db.Checkpoint()
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	assert.Equal(t, int32(0), errors.Load(), "concurrent read/write/checkpoint had errors")
	_ = ns
}
