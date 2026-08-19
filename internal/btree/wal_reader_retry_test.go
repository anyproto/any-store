package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestTryBeginReadMultiProcessHdr_AllSlotsBusyReturnsRetry asserts that when
// all four reader slots (1..4) are held shared by peer readers AND mxFrame > 0
// (so slot-0 fast path is unavailable because nBackfill < mxFrame), a new
// reader's tryBeginReadMultiProcessHdr returns errWALRetry, not ErrBusy.
//
// This mirrors SQLite walTryBeginRead's conversion at wal.c:3186-3188:
//
//	if( mxI==0 ){
//	  assert( rc==SQLITE_BUSY || (pWal->readOnly & WAL_SHM_RDONLY)!=0 );
//	  return rc==SQLITE_BUSY ? WAL_RETRY : SQLITE_READONLY_CANTINIT;
//	}
//
// SQLite never surfaces SQLITE_BUSY to its caller; the WAL_RETRY sentinel is
// consumed by the do/while loop in walBeginReadTransaction (wal.c:3391-3393).
func TestTryBeginReadMultiProcessHdr_AllSlotsBusyReturnsRetry(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Commit some frames so mxFrame > 0 and nBackfill == 0 — this forces
	// tryBeginReadMultiProcessHdr down the slot-1..4 selection path.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
	}
	require.NoError(t, tx.Commit())

	w := db.pager.wal
	require.Greater(t, w.index.maxFrame.Load(), uint32(0), "mxFrame must be > 0 to force slot-1..4 path")

	// Hold slots 1..4 shared from this goroutine to simulate peer readers.
	// Before taking the shared lock, set each readmark to a value > mxFrame
	// so step 4's candidate search rejects all of them (bestSlot == -1),
	// forcing step 5 to try exclusive-claim — which must fail because we
	// hold each slot shared ourselves.
	mxFrame := w.index.maxFrame.Load()
	for i := 1; i <= 4; i++ {
		w.index.shmWriteReadMark(i, mxFrame+1000) // out of range
		require.NoError(t, w.index.lock(lockRead0+i, lockShared), "peer reader shared lock slot %d", i)
	}
	defer func() {
		for i := 1; i <= 4; i++ {
			_ = w.index.unlock(lockRead0+i, lockShared)
		}
	}()

	_, _, _, _, err = w.tryBeginReadMultiProcessHdr()
	if !errors.Is(err, errWALRetry) {
		t.Fatalf("got err=%v (type %T), want errWALRetry — SQLite wal.c:3188 converts SQLITE_BUSY to WAL_RETRY here", err, err)
	}
}

// TestTryBeginReadInProcessHdr_Slot0FallbackClaims pins the in-process slot-0
// fallback (all reader slots 1..4 busy → claim read-0 shared with
// minFrame = nBackfill+1) still succeeding after the post-lock re-validation
// was added: with mxCommitFrame/nBackfill unchanged across the acquisition the
// fallback must claim slot 0, not spuriously retry. Guards against an
// availability regression from the re-validation fix.
func TestTryBeginReadInProcessHdr_Slot0FallbackClaims(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
	}
	require.NoError(t, tx.Commit())

	w := db.pager.wal
	mxFrame := w.index.mxCommitFrame.LoadLocal()
	nBackfill := w.index.nBackfill.Load()
	require.Greater(t, mxFrame, uint32(0))
	require.Less(t, nBackfill, mxFrame, "fallback requires an un-backfilled WAL tail")

	// Hold slots 1..4 exclusive — exactly what tryResetWALWithBusy does while
	// resetting the WAL (wal.go tryResetWALWithBusy) — so the reader can
	// neither reuse nor claim a slot and must take the slot-0 fallback.
	for i := 1; i <= 4; i++ {
		require.NoError(t, w.index.lock(lockRead0+i, lockExclusive))
	}
	defer func() {
		for i := 1; i <= 4; i++ {
			_ = w.index.unlock(lockRead0+i, lockExclusive)
		}
	}()

	hdr, gotMax, gotMin, slot, err := w.tryBeginReadInProcessHdr()
	require.NoError(t, err)
	defer func() { _ = w.index.unlock(lockRead0, lockShared) }()
	require.Equal(t, 0, slot)
	require.Equal(t, mxFrame, gotMax)
	require.Equal(t, nBackfill+1, gotMin)
	require.Equal(t, uint8(1), hdr.isInit)
}

// TestInProcessSlot0FallbackTruncateStress exercises the in-process slot-0
// fallback against concurrent TRUNCATE checkpoints. Pre-fix, the fallback
// took read-0 shared with NO post-lock re-validation (unlike its slot-reuse
// sibling and unlike C's READ_LOCK(0) re-check, wal.c:3136-3139), so a reset
// completing between the lock-free counter snapshot and the read-0
// acquisition could hand the reader a stale [minFrame, maxFrame] window that
// matches NEW-generation frames — a snapshot-isolation hole, and (once WAL
// frame read errors propagate — drift-6 resolution) a source of spurious
// ErrWALCorrupt.
//
// Readers verify every value is self-consistent (key/seq echo + fill byte),
// so serving a new-generation frame inside an old snapshot, a torn read, or
// any read error fails the test. Run with -race.
func TestInProcessSlot0FallbackTruncateStress(t *testing.T) {
	const (
		nKeys    = 24
		nReaders = 4
		valSize  = 2048 // multi-page values keep the WAL lookup path hot
	)
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	db.pager.wal.busyHandler = DefaultBusyTimeout(200 * time.Millisecond)

	mkVal := func(key uint32, seq uint64) []byte {
		v := make([]byte, valSize)
		binary.BigEndian.PutUint32(v[0:4], key)
		binary.BigEndian.PutUint64(v[4:12], seq)
		fill := byte(uint64(key)*31 + seq)
		for i := 12; i < valSize; i++ {
			v[i] = fill
		}
		return v
	}
	checkVal := func(key uint32, v []byte) error {
		if len(v) != valSize {
			return fmt.Errorf("key %d: len %d, want %d", key, len(v), valSize)
		}
		if got := binary.BigEndian.Uint32(v[0:4]); got != key {
			return fmt.Errorf("key %d: value echoes key %d", key, got)
		}
		seq := binary.BigEndian.Uint64(v[4:12])
		fill := byte(uint64(key)*31 + seq)
		for i := 12; i < valSize; i++ {
			if v[i] != fill {
				return fmt.Errorf("key %d seq %d: torn value at byte %d (got %#x want %#x)", key, seq, i, v[i], fill)
			}
		}
		return nil
	}

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := uint32(0); i < nKeys; i++ {
		key := binary.BigEndian.AppendUint32(nil, i)
		require.NoError(t, tx.Put(ns, key, mkVal(i, 0)))
	}
	require.NoError(t, tx.Commit())

	var (
		stop     atomic.Bool
		wg       sync.WaitGroup
		failOnce sync.Once
		failure  error
	)
	fail := func(err error) {
		failOnce.Do(func() { failure = err })
		stop.Store(true)
	}

	// Writer: keep committing so mxFrame stays ahead of nBackfill and reader
	// snapshots resolve WAL frames (nBackfill == mxFrame would put every
	// reader on the slot-0 fast path, which never consults the WAL).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for seq := uint64(1); !stop.Load(); seq++ {
			tx, err := db.BeginWrite()
			if err != nil {
				continue
			}
			ns, err := tx.GetNamespace("t1")
			if err != nil {
				_ = tx.Rollback()
				continue
			}
			k := uint32(seq % nKeys)
			key := binary.BigEndian.AppendUint32(nil, k)
			if err := tx.Put(ns, key, mkVal(k, seq)); err != nil {
				_ = tx.Rollback()
				continue
			}
			_ = tx.Commit()
		}
	}()

	// TRUNCATE checkpointer: each successful pass backfills fully, resets the
	// WAL (new salts, frame numbers recycled), and briefly holds slots 1..4
	// exclusive — pushing concurrent readers into the slot-0 fallback.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db.Checkpoint(CheckpointTruncate)
			time.Sleep(500 * time.Microsecond)
		}
	}()

	// Slot squatter: the reset's own slots-1..4 exclusive window is only
	// microseconds wide, far too rare for readers to land in the fallback by
	// chance (verified by coverage). Cycle short exclusive holds of all four
	// slots so a steady share of BeginRead traffic genuinely flows through
	// the slot-0 fallback while the writer and checkpointer churn the
	// counters it re-validates. Resets are excluded during a squat window
	// (tryResetWALWithBusy needs these locks) but proceed between windows.
	wg.Add(1)
	go func() {
		defer wg.Done()
		w := db.pager.wal
		for !stop.Load() {
			held := make([]int, 0, 4)
			for i := 1; i <= 4; i++ {
				if w.index.lock(lockRead0+i, lockExclusive) == nil {
					held = append(held, i)
				}
			}
			if len(held) == 4 {
				time.Sleep(2 * time.Millisecond)
			}
			for _, i := range held {
				_ = w.index.unlock(lockRead0+i, lockExclusive)
			}
			time.Sleep(time.Millisecond)
		}
	}()

	for r := 0; r < nReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				rtx, err := db.BeginRead()
				if err != nil {
					fail(fmt.Errorf("BeginRead: %w", err))
					return
				}
				rns, err := rtx.GetNamespace("t1")
				if err != nil {
					fail(fmt.Errorf("GetNamespace: %w", err))
					_ = rtx.Rollback()
					return
				}
				for i := uint32(0); i < nKeys; i++ {
					key := binary.BigEndian.AppendUint32(nil, i)
					v, err := rtx.AppendValue(rns, key, nil)
					if err != nil {
						fail(fmt.Errorf("AppendValue key %d: %w", i, err))
						_ = rtx.Rollback()
						return
					}
					if err := checkVal(i, v); err != nil {
						fail(err)
						_ = rtx.Rollback()
						return
					}
				}
				_ = rtx.Rollback()
			}
		}()
	}

	time.Sleep(1500 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	require.NoError(t, failure)
}
