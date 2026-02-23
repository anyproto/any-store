package btree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bug 9: Crash-recovery corruption with savepoint-stress workload
//
// STABLE EXTERNAL REPRO (100% reliable, requires the crash test harness
// in any-store-tests/crashtest/):
//
//   go test -v -timeout 60s -run TestCrashFuzz ./crashtest/ \
//     -crash.iterations=20 -crash.workload=savepoint-stress \
//     -crash.flush-mode=random -crash.seed=55443322
//
// This consistently fails within the first 20 iterations with errors like:
//   - btree: invalid page number (child/overflow pointer is 0)
//   - expected present but not found (committed keys missing)
//
// The in-process tests below exercise the same savepoint patterns but use
// rawClose instead of SIGKILL, which doesn't reproduce the exact same
// corruption because it can't interrupt mid-I/O operations (WriteAt, fdatasync).

// TestSavepointCrashRecovery exercises nested savepoints with concurrent
// readers and checkpoints, then simulates crash via rawClose at various
// points. Tests WAL recovery correctness after savepoint-style operations.
func TestSavepointCrashRecovery(t *testing.T) {
	seeds := []int64{42, 12345, 55443322, 88776655, 99887766}

	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.db")

			rng := rand.New(rand.NewSource(seed))

			numCycles := 50
			for cycle := 0; cycle < numCycles; cycle++ {
				runSavepointCrashCycle(t, path, rng, cycle)
			}

			db := openDBNoCleanup(t, path)
			defer func() { _ = db.Close() }()
			verifyDBIntegrity(t, db)
		})
	}
}

// TestSavepointCrashPartialCheckpoint simulates a crash during checkpoint
// by doing savepoint work, starting a checkpoint (writing WAL frames to DB),
// then truncating the DB file to simulate a partial write.
func TestSavepointCrashPartialCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"

	rng := rand.New(rand.NewSource(42))

	for cycle := 0; cycle < 30; cycle++ {
		db := openDBNoCleanup(t, path)

		nsName := "docs"
		func() {
			tx, err := db.BeginWrite()
			if err != nil {
				return
			}
			_, _ = tx.CreateNamespace(nsName)
			_ = tx.Commit()
		}()

		ns, err := db.getNamespaceLocked(nsName)
		if err != nil {
			rawClose(db)
			continue
		}

		// Do savepoint work
		for i := 0; i < 3+rng.Intn(5); i++ {
			tx, err := db.BeginWrite()
			if err != nil {
				break
			}

			for j := 0; j < 1+rng.Intn(10); j++ {
				key := fmt.Sprintf("key-%06d", rng.Intn(500))
				val := make([]byte, 10+rng.Intn(200))
				rng.Read(val)
				_ = tx.Put(ns, []byte(key), val)
			}

			spId, spErr := tx.Savepoint()
			if spErr != nil {
				_ = tx.Rollback()
				continue
			}

			for j := 0; j < 1+rng.Intn(10); j++ {
				key := fmt.Sprintf("key-%06d", rng.Intn(500))
				val := make([]byte, 10+rng.Intn(200))
				rng.Read(val)
				_ = tx.Put(ns, []byte(key), val)
			}

			if rng.Float64() < 0.3 {
				_ = tx.RollbackToSavepoint(spId)
			} else {
				_ = tx.ReleaseSavepoint(spId)
			}

			_ = tx.Commit()
		}

		// Do a full checkpoint to write WAL pages to DB
		_ = db.Checkpoint(CheckpointFull)

		// Do more savepoint work (creates new WAL frames after checkpoint)
		for i := 0; i < 2+rng.Intn(3); i++ {
			tx, err := db.BeginWrite()
			if err != nil {
				break
			}
			for j := 0; j < 1+rng.Intn(10); j++ {
				key := fmt.Sprintf("key-%06d", rng.Intn(500))
				val := make([]byte, 10+rng.Intn(200))
				rng.Read(val)
				_ = tx.Put(ns, []byte(key), val)
			}
			spId, _ := tx.Savepoint()
			for j := 0; j < 1+rng.Intn(5); j++ {
				key := fmt.Sprintf("key-%06d", rng.Intn(500))
				_ = tx.Delete(ns, []byte(key))
			}
			if rng.Float64() < 0.4 {
				_ = tx.RollbackToSavepoint(spId)
			} else {
				_ = tx.ReleaseSavepoint(spId)
			}
			_ = tx.Commit()
		}

		// rawClose — WAL has committed frames not yet checkpointed
		rawClose(db)

		// Simulate partial checkpoint corruption: truncate the DB file
		// to somewhere between its pre-checkpoint and post-checkpoint size.
		// This mimics SIGKILL during a checkpoint where some pages were
		// written to the DB file but not all.
		if rng.Float64() < 0.3 {
			dbInfo, err := os.Stat(path)
			if err == nil && dbInfo.Size() > int64(DefaultPageSize) {
				// Truncate to a random page boundary within the file
				numPages := dbInfo.Size() / int64(DefaultPageSize)
				if numPages > 2 {
					truncPages := 2 + rng.Int63n(numPages-1)
					truncSize := truncPages * int64(DefaultPageSize)
					if truncSize < dbInfo.Size() {
						_ = os.Truncate(path, truncSize)
					}
				}
			}
		}

		// Simulate partial WAL truncation (SIGKILL during WAL write)
		if rng.Float64() < 0.2 {
			walInfo, err := os.Stat(walPath)
			if err == nil && walInfo.Size() > int64(walHeaderSize) {
				frameSize := int64(walFrameSize) + int64(DefaultPageSize)
				// Truncate mid-frame
				truncAt := walInfo.Size() - rng.Int63n(frameSize)
				if truncAt > int64(walHeaderSize) {
					_ = os.Truncate(walPath, truncAt)
				}
			}
		}
	}

	// Final verification
	db := openDBNoCleanup(t, path)
	defer func() { _ = db.Close() }()
	verifyDBIntegrity(t, db)
}

// TestSavepointCrashConcurrentReadersCheckpoint is a high-concurrency variant
// that runs readers and checkpoints concurrently with savepoint operations,
// then crashes.
func TestSavepointCrashConcurrentReadersCheckpoint(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	rng := rand.New(rand.NewSource(31337))

	for cycle := 0; cycle < 40; cycle++ {
		runSavepointCrashCycleConcurrent(t, path, rng, cycle)
	}

	db := openDBNoCleanup(t, path)
	defer func() { _ = db.Close() }()
	verifyDBIntegrity(t, db)
}

// TestSavepointCrashHeavyFreelist combines savepoint operations with heavy
// insert/delete cycles that stress the freelist, then crashes mid-operation.
func TestSavepointCrashHeavyFreelist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	rng := rand.New(rand.NewSource(99887766))

	for cycle := 0; cycle < 30; cycle++ {
		db := openDBNoCleanup(t, path)

		nsName := "docs"
		func() {
			tx, err := db.BeginWrite()
			if err != nil {
				return
			}
			_, _ = tx.CreateNamespace(nsName)
			_ = tx.Commit()
		}()

		ns, err := db.getNamespaceLocked(nsName)
		if err != nil {
			rawClose(db)
			continue
		}

		tx, err := db.BeginWrite()
		if err != nil {
			rawClose(db)
			continue
		}
		for i := 0; i < 100+rng.Intn(200); i++ {
			key := fmt.Sprintf("key-%06d", rng.Intn(1000))
			valSize := 10 + rng.Intn(200)
			if rng.Float64() < 0.1 {
				valSize = 2048 + rng.Intn(4096)
			}
			val := make([]byte, valSize)
			rng.Read(val)
			_ = tx.Put(ns, []byte(key), val)
		}
		_ = tx.Commit()

		_ = db.Checkpoint(CheckpointFull)

		tx, err = db.BeginWrite()
		if err != nil {
			rawClose(db)
			continue
		}

		spId, _ := tx.Savepoint()
		for i := 0; i < 200; i++ {
			key := fmt.Sprintf("key-%06d", rng.Intn(1000))
			_ = tx.Delete(ns, []byte(key))
		}

		for i := 0; i < 50+rng.Intn(100); i++ {
			key := fmt.Sprintf("key-%06d", rng.Intn(1000))
			val := make([]byte, 10+rng.Intn(200))
			rng.Read(val)
			_ = tx.Put(ns, []byte(key), val)
		}

		if rng.Float64() < 0.4 {
			_ = tx.RollbackToSavepoint(spId)
		} else {
			_ = tx.ReleaseSavepoint(spId)
		}

		if rng.Float64() < 0.1 {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}

		rawClose(db)
	}

	db := openDBNoCleanup(t, path)
	defer func() { _ = db.Close() }()
	verifyDBIntegrity(t, db)
}

// TestSavepointPartialCheckpointBackfill simulates the specific scenario
// where a checkpoint partially backfills WAL frames to the DB file, then
// crashes. The WAL should still contain all committed frames for recovery.
func TestSavepointPartialCheckpointBackfill(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	walPath := path + "-wal"

	db := openDBNoCleanup(t, path)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("docs")
	require.NoError(t, err)

	// Write many keys to grow the DB
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("initial-val-%d-padding-to-increase-page-usage", i)
		require.NoError(t, tx.Put(ns, []byte(key), []byte(val)))
	}
	require.NoError(t, tx.Commit())

	// Checkpoint to push all data to DB file
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Now do a savepoint-heavy transaction that generates new WAL frames
	tx, err = db.BeginWrite()
	require.NoError(t, err)

	spId, err := tx.Savepoint()
	require.NoError(t, err)

	// Delete many keys (frees pages, modifies freelist)
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key-%06d", i)
		_ = tx.Delete(ns, []byte(key))
	}

	// Insert new keys (may reuse freed pages from freelist)
	for i := 500; i < 700; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("new-val-%d-some-data-to-fill-pages", i)
		_ = tx.Put(ns, []byte(key), []byte(val))
	}

	// Release savepoint (merge changes)
	require.NoError(t, tx.ReleaseSavepoint(spId))
	require.NoError(t, tx.Commit())

	// Record DB size before checkpoint
	dbInfo, err := os.Stat(path)
	require.NoError(t, err)
	dbSizeBefore := dbInfo.Size()

	// Record WAL size
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	walSizeBefore := walInfo.Size()

	t.Logf("Before checkpoint: DB=%d bytes, WAL=%d bytes", dbSizeBefore, walSizeBefore)

	// rawClose without checkpoint
	rawClose(db)

	// Scenario 1: DB file partially grown (simulates checkpoint writing
	// some pages to DB but not updating nBackfill/WAL header)
	for _, truncFraction := range []float64{0.3, 0.5, 0.7, 0.9} {
		t.Run(fmt.Sprintf("db_trunc_%.0f%%", truncFraction*100), func(t *testing.T) {
			snapDir := t.TempDir()
			snapPath := filepath.Join(snapDir, "test.db")
			snapWAL := snapPath + "-wal"

			// Copy DB and WAL
			copyFileForTest(t, path, snapPath)
			copyFileForTest(t, walPath, snapWAL)

			// Simulate partial DB write: truncate to a fraction of the full size
			info, _ := os.Stat(snapPath)
			if info != nil {
				truncSize := int64(float64(info.Size()) * truncFraction)
				// Align to page boundary
				truncSize = (truncSize / int64(DefaultPageSize)) * int64(DefaultPageSize)
				if truncSize >= int64(DefaultPageSize) {
					_ = os.Truncate(snapPath, truncSize)
				}
			}

			// Recovery should work — WAL still has all committed data
			snapDB := openDBNoCleanup(t, snapPath)
			verifyDBIntegrity(t, snapDB)
			_ = snapDB.Close()
		})
	}

	// Scenario 2: WAL partially truncated (mid-frame)
	for _, truncFrames := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("wal_trunc_%d_frames", truncFrames), func(t *testing.T) {
			snapDir := t.TempDir()
			snapPath := filepath.Join(snapDir, "test.db")
			snapWAL := snapPath + "-wal"

			copyFileForTest(t, path, snapPath)
			copyFileForTest(t, walPath, snapWAL)

			// Truncate WAL to remove the last N frames
			frameSize := int64(walFrameSize) + int64(DefaultPageSize)
			info, _ := os.Stat(snapWAL)
			if info != nil {
				truncSize := info.Size() - int64(truncFrames)*frameSize
				// Also cut into a frame to simulate mid-write
				truncSize -= frameSize / 2
				if truncSize > int64(walHeaderSize) {
					_ = os.Truncate(snapWAL, truncSize)
				}
			}

			snapDB := openDBNoCleanup(t, snapPath)
			verifyDBIntegrity(t, snapDB)
			_ = snapDB.Close()
		})
	}

	// Scenario 3: WAL completely removed after checkpoint
	t.Run("wal_removed_after_partial_ckpt", func(t *testing.T) {
		snapDir := t.TempDir()
		snapPath := filepath.Join(snapDir, "test.db")

		// Copy just the DB, no WAL (simulates crash after WAL truncate)
		copyFileForTest(t, path, snapPath)

		snapDB := openDBNoCleanup(t, snapPath)
		verifyDBIntegrity(t, snapDB)
		_ = snapDB.Close()
	})

	// Scenario 4: Corrupt last frame checksum in WAL
	t.Run("wal_corrupt_last_frame_cksum", func(t *testing.T) {
		snapDir := t.TempDir()
		snapPath := filepath.Join(snapDir, "test.db")
		snapWAL := snapPath + "-wal"

		copyFileForTest(t, path, snapPath)
		copyFileForTest(t, walPath, snapWAL)

		// Corrupt the checksum of the last frame
		info, _ := os.Stat(snapWAL)
		if info != nil {
			frameSize := int64(walFrameSize) + int64(DefaultPageSize)
			lastFrameOff := info.Size() - frameSize
			if lastFrameOff > int64(walHeaderSize) {
				f, _ := os.OpenFile(snapWAL, os.O_RDWR, 0)
				if f != nil {
					// Corrupt checksum1 (bytes 16-19 of frame header)
					buf := make([]byte, 4)
					binary.BigEndian.PutUint32(buf, 0xDEADBEEF)
					_, _ = f.WriteAt(buf, lastFrameOff+16)
					_ = f.Close()
				}
			}
		}

		snapDB := openDBNoCleanup(t, snapPath)
		verifyDBIntegrity(t, snapDB)
		_ = snapDB.Close()
	})
}

func runSavepointCrashCycle(t *testing.T, path string, rng *rand.Rand, cycle int) {
	t.Helper()

	db := openDBNoCleanup(t, path)

	nsName := "docs"
	func() {
		tx, err := db.BeginWrite()
		if err != nil {
			rawClose(db)
			return
		}
		_, _ = tx.CreateNamespace(nsName)
		if err := tx.Commit(); err != nil {
			rawClose(db)
			return
		}
	}()

	ns, err := db.getNamespaceLocked(nsName)
	if err != nil {
		rawClose(db)
		return
	}

	numTx := 5 + rng.Intn(15)
	for i := 0; i < numTx; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			break
		}

		outerOps := 1 + rng.Intn(10)
		for j := 0; j < outerOps; j++ {
			key := fmt.Sprintf("key-%06d", rng.Intn(500))
			val := fmt.Sprintf("val-%d-%d-%d", cycle, i, j)
			_ = tx.Put(ns, []byte(key), []byte(val))
		}

		spId, spErr := tx.Savepoint()
		if spErr != nil {
			_ = tx.Rollback()
			continue
		}

		innerOps := 1 + rng.Intn(10)
		for j := 0; j < innerOps; j++ {
			key := fmt.Sprintf("key-%06d", rng.Intn(500))
			val := fmt.Sprintf("sp-val-%d-%d-%d", cycle, i, j)
			_ = tx.Put(ns, []byte(key), []byte(val))
		}

		if rng.Float64() < 0.3 {
			spId2, sp2Err := tx.Savepoint()
			if sp2Err == nil {
				for j := 0; j < 1+rng.Intn(5); j++ {
					key := fmt.Sprintf("key-%06d", rng.Intn(500))
					val := fmt.Sprintf("sp2-val-%d-%d-%d", cycle, i, j)
					_ = tx.Put(ns, []byte(key), []byte(val))
				}
				if rng.Float64() < 0.5 {
					_ = tx.RollbackToSavepoint(spId2)
				} else {
					_ = tx.ReleaseSavepoint(spId2)
				}
			}
		}

		if rng.Float64() < 0.3 {
			_ = tx.RollbackToSavepoint(spId)
		} else {
			_ = tx.ReleaseSavepoint(spId)
		}

		if rng.Float64() < 0.2 {
			for j := 0; j < rng.Intn(20); j++ {
				key := fmt.Sprintf("key-%06d", rng.Intn(500))
				_ = tx.Delete(ns, []byte(key))
			}
		}

		if rng.Float64() < 0.1 {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}

		if rng.Float64() < 0.3 {
			modes := []CheckpointMode{CheckpointPassive, CheckpointFull, CheckpointRestart, CheckpointTruncate}
			_ = db.Checkpoint(modes[rng.Intn(len(modes))])
		}
	}

	rawClose(db)
}

func runSavepointCrashCycleConcurrent(t *testing.T, path string, rng *rand.Rand, cycle int) {
	t.Helper()

	db := openDBNoCleanup(t, path)

	nsName := "docs"
	func() {
		tx, err := db.BeginWrite()
		if err != nil {
			rawClose(db)
			return
		}
		_, _ = tx.CreateNamespace(nsName)
		_ = tx.Commit()
	}()

	ns, err := db.getNamespaceLocked(nsName)
	if err != nil {
		rawClose(db)
		return
	}

	var stop atomic.Bool
	var wg sync.WaitGroup

	for r := 0; r < 4; r++ {
		wg.Add(1)
		readerSeed := rng.Int63()
		go func(seed int64) {
			defer wg.Done()
			localRng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				rtx, err := db.BeginRead()
				if err != nil {
					return
				}
				for j := 0; j < 5; j++ {
					key := fmt.Sprintf("key-%06d", localRng.Intn(500))
					val, _ := rtx.Get(ns, []byte(key))
					_ = val
				}
				cursor := rtx.NewCursor(ns)
				if cursor.First() == nil {
					count := 0
					for cursor.Valid() {
						_, _ = cursor.Key()
						_, _ = cursor.Value()
						count++
						if count > 1000 {
							break
						}
						if cursor.Next() != nil {
							break
						}
					}
				}
				cursor.Close()
				_ = rtx.Rollback()
			}
		}(readerSeed)
	}

	for c := 0; c < 2; c++ {
		wg.Add(1)
		ckptSeed := rng.Int63() // generate seed before launching goroutine
		go func(seed int64) {
			defer wg.Done()
			modes := []CheckpointMode{CheckpointPassive, CheckpointFull, CheckpointRestart, CheckpointTruncate}
			localRng := rand.New(rand.NewSource(seed))
			for !stop.Load() {
				_ = db.Checkpoint(modes[localRng.Intn(len(modes))])
				time.Sleep(time.Duration(localRng.Intn(2)) * time.Millisecond)
			}
		}(ckptSeed)
	}

	numTx := 5 + rng.Intn(15)
	for i := 0; i < numTx; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			break
		}

		for j := 0; j < 1+rng.Intn(10); j++ {
			key := fmt.Sprintf("key-%06d", rng.Intn(500))
			val := fmt.Sprintf("val-%d-%d-%d", cycle, i, j)
			_ = tx.Put(ns, []byte(key), []byte(val))
		}

		spId, spErr := tx.Savepoint()
		if spErr != nil {
			_ = tx.Rollback()
			continue
		}

		for j := 0; j < 1+rng.Intn(10); j++ {
			key := fmt.Sprintf("key-%06d", rng.Intn(500))
			val := fmt.Sprintf("sp-val-%d-%d-%d", cycle, i, j)
			_ = tx.Put(ns, []byte(key), []byte(val))
		}

		if rng.Float64() < 0.3 {
			spId2, sp2Err := tx.Savepoint()
			if sp2Err == nil {
				for j := 0; j < 1+rng.Intn(5); j++ {
					key := fmt.Sprintf("key-%06d", rng.Intn(500))
					val := fmt.Sprintf("sp2-val-%d-%d-%d", cycle, i, j)
					_ = tx.Put(ns, []byte(key), []byte(val))
				}
				if rng.Float64() < 0.5 {
					_ = tx.RollbackToSavepoint(spId2)
				} else {
					_ = tx.ReleaseSavepoint(spId2)
				}
			}
		}

		if rng.Float64() < 0.3 {
			_ = tx.RollbackToSavepoint(spId)
		} else {
			_ = tx.ReleaseSavepoint(spId)
		}

		if rng.Float64() < 0.2 {
			for j := 0; j < rng.Intn(30); j++ {
				key := fmt.Sprintf("key-%06d", rng.Intn(500))
				_ = tx.Delete(ns, []byte(key))
			}
		}

		if rng.Float64() < 0.1 {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}

	stop.Store(true)
	wg.Wait()

	rawClose(db)
}

func verifyDBIntegrity(t *testing.T, db *DB) {
	t.Helper()

	ns, err := db.getNamespaceLocked("docs")
	if err != nil {
		return
	}

	rtx, err := db.BeginRead()
	require.NoError(t, err, "BeginRead failed during integrity check")

	cursor := rtx.NewCursor(ns)
	defer cursor.Close()

	err = cursor.First()
	if err != nil {
		_ = rtx.Rollback()
		return
	}

	count := 0
	for cursor.Valid() {
		k, kErr := cursor.Key()
		if kErr != nil {
			t.Fatalf("Key() error at position %d: %v", count, kErr)
		}
		assert.NotEmpty(t, k, "empty key at position %d", count)
		v, vErr := cursor.Value()
		if vErr != nil {
			t.Fatalf("Value() error at position %d: %v", count, vErr)
		}
		_ = v
		count++
		if err := cursor.Next(); err != nil {
			t.Fatalf("Next() error at position %d: %v", count, err)
		}
	}
	require.NoError(t, rtx.Rollback())

	t.Logf("Integrity check passed: %d keys found", count)
}

func copyFileForTest(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		return // source doesn't exist, skip
	}
	require.NoError(t, os.WriteFile(dst, data, 0644))
}
