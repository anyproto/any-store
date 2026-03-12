package btree

// Reproducer for writer cache staleness after CheckpointRestart.
//
// Root cause: the checkpoint goroutine modifies w.header.salt1/salt2 and
// w.nFrame directly on the shared wal struct (via doResetWAL → writeHeader).
// When the writer calls beginWrite(), it compares the SHM header against
// these already-updated values → stateChanged=false → writerCache not cleared.

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func testVal(t *testing.T, size int) []byte {
	t.Helper()
	val := make([]byte, size)
	_, err := rand.Read(val)
	require.NoError(t, err)
	copy(val, []byte(fmt.Sprintf("v-%d-%d-", size, time.Now().UnixNano())))
	return val
}

// TestWriterCacheStaleAfterWALReset reproduces the stale writerCache bug.
// Concurrent readers + checkpoint cycling causes WAL reset without cache
// invalidation, leading to data corruption.
func TestWriterCacheStaleAfterWALReset(t *testing.T) {
	for trial := 0; trial < 3; trial++ {
		func() {
			dir := t.TempDir()
			dbPath := filepath.Join(dir, "test.db")
			db, err := Open(dbPath, Options{PageSize: 4096, InProcess: true})
			require.NoError(t, err)
			defer db.Close()
			db.pager.wal.busyHandler = DefaultBusyTimeout(200 * time.Millisecond)

			// Create namespace
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			_, err = tx.CreateNamespace("test")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			// Phase 1: Write data to populate writerCache
			expected := make(map[string][]byte)
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.GetNamespace("test")
			require.NoError(t, err)
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("key-%04d", i)
				val := testVal(t, 1500+i*50)
				require.NoError(t, tx.Put(ns, []byte(key), val))
				expected[key] = val
			}
			require.NoError(t, tx.Commit())

			// Start concurrent readers (short-lived to avoid hangs on corruption)
			var stop atomic.Bool
			var wg sync.WaitGroup

			for r := 0; r < 3; r++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for !stop.Load() {
						rtx, err := db.BeginRead()
						if err != nil {
							time.Sleep(100 * time.Microsecond)
							continue
						}
						// Just begin+rollback to trigger shmReadCkptInfo
						_ = rtx.Rollback()
						time.Sleep(100 * time.Microsecond)
					}
				}()
			}

			// Checkpoint goroutine
			wg.Add(1)
			go func() {
				defer wg.Done()
				modes := []CheckpointMode{CheckpointPassive, CheckpointFull, CheckpointRestart}
				i := 0
				for !stop.Load() {
					_ = db.Checkpoint(modes[i%len(modes)])
					i++
					time.Sleep(2 * time.Millisecond)
				}
			}()

			// Phase 2: Writer transactions with verification
			for txIdx := 0; txIdx < 100; txIdx++ {
				tx, err := db.BeginWrite()
				if err != nil {
					continue
				}
				ns, err = tx.GetNamespace("test")
				if err != nil {
					_ = tx.Rollback()
					continue
				}

				for i := txIdx % 10; i < 50; i += 10 {
					key := fmt.Sprintf("key-%04d", i)
					val := testVal(t, 2000+txIdx*7+i*30)
					if err := tx.Put(ns, []byte(key), val); err != nil {
						_ = tx.Rollback()
						goto nextTx
					}
					expected[key] = val
				}
				if err := tx.Commit(); err != nil {
					continue
				}

				// Verify via fresh read transaction
				func() {
					rtx, err := db.BeginRead()
					require.NoError(t, err)
					defer func() { _ = rtx.Rollback() }()

					rns, err := rtx.GetNamespace("test")
					require.NoError(t, err, "trial %d tx %d", trial, txIdx)

					for key, wantVal := range expected {
						gotVal, err := rtx.AppendValue(rns, []byte(key), nil)
						if err != nil {
							stop.Store(true)
							wg.Wait()
							t.Fatalf("trial %d tx %d key %s: %v", trial, txIdx, key, err)
						}
						if !bytes.Equal(gotVal, wantVal) {
							stop.Store(true)
							wg.Wait()
							t.Fatalf("trial %d tx %d key %s: value mismatch (got len=%d want len=%d)",
								trial, txIdx, key, len(gotVal), len(wantVal))
						}
					}
				}()
			nextTx:
			}

			stop.Store(true)
			wg.Wait()
		}()
	}
}
