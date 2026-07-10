package btree

// Coverage for the WAL read-error propagation contract (docs/btree/NOTES.md
// drift-6, RESOLVED): once the wal-index resolves a frame for a page, a
// readFrame/readFrameRaw failure must surface as the page-get error at every
// getter — getPageWriter, readTempPage, getPageReader, readRawPage — matching
// C readDbPage (pager.c:3035-3046, sqlitec 3.52.0). See
// TestWALReadFrameErrorPropagates (wal_test.go) for the getPageReader cached
// path and the structural argument for why a failure is never a benign
// WAL-reset race.

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// seedWALResolvedPage commits enough rows that several pages have live WAL
// frames, then returns a (pgno, frame) pair resolvable within maxFrame.
// DisableAutoCheckpoint on the DB keeps the frames in the WAL.
func seedWALResolvedPage(t *testing.T, db *DB, maxFrame uint32) (pgno, frame uint32) {
	t.Helper()
	for p := uint32(1); p <= db.pager.dbSize.Load(); p++ {
		if f := mustWiGet(t, db.pager.wal.index, p, maxFrame); f > 0 {
			return p, f
		}
	}
	t.Fatal("expected at least one page resolvable to a WAL frame")
	return 0, 0
}

func commitRows(t *testing.T, db *DB, ns string, n, valSize int) {
	t.Helper()
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	nsh, err := wtx.CreateNamespace(ns)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, wtx.Put(nsh, key, make([]byte, valSize)))
	}
	require.NoError(t, wtx.Commit())
}

// TestWALReadFrameErrorPropagates_Writer pins propagation on the writer path
// (getPageWriter): the write lock excludes a concurrent restart, so a
// readFrame failure there is always genuine and must abort the page get. The
// faulted page must be discarded from the writer cache (no poisoned entry) so
// a retry after the fault clears re-reads successfully.
func TestWALReadFrameErrorPropagates_Writer(t *testing.T) {
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	commitRows(t, db, "t1", 64, 200)

	pager := db.pager
	nf := pager.wal.nFrame.Load()
	require.Greater(t, nf, uint32(0))
	targetPgno, targetFrame := seedWALResolvedPage(t, db, nf)

	// Hold the write lock (open write tx) — the getPageWriter context.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	t.Cleanup(func() { _ = wtx.Rollback() })

	// Force a cache miss so getPage takes the WAL-read path.
	pager.writerCache.discard(targetPgno)

	sentinel := errors.New("injected WAL readFrame fault (writer)")
	var hookFired bool
	setWalReadFrameFaultHook(func(frame uint32) error {
		if frame == targetFrame {
			hookFired = true
			return sentinel
		}
		return nil
	})

	_, err = pager.getPage(targetPgno)
	require.True(t, hookFired, "fault hook must fire for the target frame")
	require.ErrorIs(t, err, sentinel, "writer-path readFrame failure must propagate")
	require.Contains(t, err.Error(), "WAL frame")
	require.Nil(t, pager.writerCache.fetch(targetPgno),
		"faulted page must be discarded from the writer cache, not poisoned")

	// Clearing the fault restores the read — the discarded slot re-reads.
	setWalReadFrameFaultHook(nil)
	pg, err := pager.getPage(targetPgno)
	require.NoError(t, err, "retry after the fault clears must succeed")
	require.NotNil(t, pg)
	pager.releasePage(pg)
}

// TestWALReadFrameErrorPropagates_TempPage pins propagation on the uncached
// reader path (readTempPage via getPageReader's nil-cache branch).
func TestWALReadFrameErrorPropagates_TempPage(t *testing.T) {
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	commitRows(t, db, "t1", 64, 200)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	walMaxFrame := rtx.WalMaxFrame()
	require.Greater(t, walMaxFrame, uint32(0))
	targetPgno, targetFrame := seedWALResolvedPage(t, db, walMaxFrame)

	sentinel := errors.New("injected WAL readFrame fault (temp page)")
	setWalReadFrameFaultHook(func(frame uint32) error {
		if frame == targetFrame {
			return sentinel
		}
		return nil
	})

	// cache == nil routes getPageReader through readTempPage.
	_, err = db.pager.getPageReader(targetPgno, walMaxFrame, nil)
	require.ErrorIs(t, err, sentinel, "readTempPage readFrame failure must propagate")
	require.Contains(t, err.Error(), "WAL frame")

	setWalReadFrameFaultHook(nil)
	pg, err := db.pager.getPageReader(targetPgno, walMaxFrame, nil)
	require.NoError(t, err)
	db.pager.releasePage(pg)
}

// TestReadRawPageWALErrorPropagates pins propagation on the integrity-sweep
// reader (readRawPage → readFrameRaw, which shares the fault hook), both
// directly and end-to-end through VerifyIntegrity, which must report the page
// as a SweepError wrapping the read failure instead of validating a stale
// disk copy.
func TestReadRawPageWALErrorPropagates(t *testing.T) {
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	dir := t.TempDir()
	opts := DefaultOptions()
	opts.Checksum = true
	opts.DisableAutoCheckpoint = true
	db, err := testOpen(t, filepath.Join(dir, "test.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	commitRows(t, db, "t1", 64, 200)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	walMaxFrame := rtx.WalMaxFrame()
	require.Greater(t, walMaxFrame, uint32(0))

	// Pick a WAL-resolved page other than page 1: the fault hook also covers
	// readWalFrameData, and BeginRead (used by VerifyIntegrity below) reads
	// page 1's counters through it — faulting page 1's frame would fail the
	// sweep's BeginRead instead of exercising the per-page sweep error.
	var targetPgno, targetFrame uint32
	for p := uint32(2); p <= db.pager.dbSize.Load(); p++ {
		if f := mustWiGet(t, db.pager.wal.index, p, walMaxFrame); f > 0 {
			targetPgno, targetFrame = p, f
			break
		}
	}
	require.NotZero(t, targetPgno, "expected a pgno>1 page resolvable to a WAL frame")

	sentinel := errors.New("injected WAL readFrameRaw fault")
	setWalReadFrameFaultHook(func(frame uint32) error {
		if frame == targetFrame {
			return sentinel
		}
		return nil
	})

	// Direct: readRawPage must propagate, not fall back to the disk copy.
	_, err = db.pager.readRawPage(targetPgno, walMaxFrame)
	require.ErrorIs(t, err, sentinel, "readRawPage readFrameRaw failure must propagate")
	require.Contains(t, err.Error(), "WAL frame")
	require.NoError(t, rtx.Rollback())

	// End-to-end: the sweep reports the failing page and keeps sweeping.
	res, err := db.VerifyIntegrity(context.Background())
	require.NoError(t, err, "the sweep itself must not abort on a per-page read error")
	require.Greater(t, res.Pages, 0)
	var found bool
	for _, se := range res.Errors {
		if se.PageNo == targetPgno && se.Inner != nil && errors.Is(se.Inner, sentinel) {
			found = true
			break
		}
	}
	require.True(t, found,
		"VerifyIntegrity must report a SweepError for page %d wrapping the WAL read failure; got %+v",
		targetPgno, res.Errors)
}

// TestWALDecryptFailurePropagatesFromReadFrame pins the highest-severity case
// the old fallthrough masked: an AEAD authentication failure on a WAL frame
// (readFrame's codec path) is a true integrity failure and must surface —
// serving the older DB-file plaintext instead would silently undo committed
// writes. The codec's OnError sink must observe the failing page.
func TestWALDecryptFailurePropagatesFromReadFrame(t *testing.T) {
	dir := t.TempDir()
	key := make([]byte, KeyLen)
	for i := range key {
		key[i] = byte(i + 1)
	}
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{
		PageSize:              4096,
		Key:                   key,
		DisableAutoCheckpoint: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	commitRows(t, db, "t1", 64, 200)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	walMaxFrame := rtx.WalMaxFrame()
	require.Greater(t, walMaxFrame, uint32(0))

	// Pick a WAL-resolved page other than page 1 (its plaintext prefix is
	// preserved by the codec; any offset in a pgno>1 frame is ciphertext).
	pager := db.pager
	var targetPgno, targetFrame uint32
	for p := uint32(2); p <= pager.dbSize.Load(); p++ {
		if f := mustWiGet(t, pager.wal.index, p, walMaxFrame); f > 0 {
			targetPgno, targetFrame = p, f
			break
		}
	}
	require.NotZero(t, targetPgno, "expected a pgno>1 page resolvable to a WAL frame")

	// Subscribe to the codec's OnError sink before tampering.
	sink, ok := pager.codec.(OnErrorSink)
	require.True(t, ok, "built-in AEAD codec must implement OnErrorSink")
	var fired atomic.Uint32
	var gotPgno atomic.Uint32
	sink.SetOnError(func(pgno uint32, _ error) {
		fired.Store(1)
		gotPgno.Store(pgno)
	})
	t.Cleanup(func() { sink.SetOnError(nil) })

	// Flip one ciphertext byte of the target frame in the live -wal file.
	frameSize := int64(walFrameSize) + int64(pager.pageSize)
	off := int64(walHeaderSize) + int64(targetFrame-1)*frameSize + int64(walFrameSize) + 128
	wf, err := os.OpenFile(filepath.Join(dir, "test.db")+"-wal", os.O_RDWR, 0)
	require.NoError(t, err)
	one := make([]byte, 1)
	_, err = wf.ReadAt(one, off)
	require.NoError(t, err)
	one[0] ^= 0x01
	_, err = wf.WriteAt(one, off)
	require.NoError(t, err)
	require.NoError(t, wf.Close())

	// A fresh-cache read of the tampered page must fail with the decrypt
	// error — not fall through to the stale DB-file plaintext.
	cache := newPcache(int(pager.pageSize), db.readerCacheSize, true)
	_, err = pager.getPageReader(targetPgno, walMaxFrame, cache)
	require.Error(t, err, "AEAD auth failure on a WAL frame must propagate")
	require.Contains(t, err.Error(), "WAL decrypt frame")
	require.Equal(t, uint32(1), fired.Load(), "codec OnError sink must fire")
	require.Equal(t, targetPgno, gotPgno.Load(), "OnError must report the failing page")
}

// TestWALReadFramePastTailRemapsToErrWALCorrupt pins the documented drift
// (docs/btree/NOTES.md
// #drift-2026-07-10-1-readframe-past-tail-read-failure-remapped-to-errwalcorrupt):
// a ReadAt failure at frame > nFrame returns ErrWALCorrupt, where C would
// surface the raw short-read error (sqlite3WalReadFrame is a bare
// sqlite3OsRead, wal.c:3649-3664). Now user-visible since read errors
// propagate.
func TestWALReadFramePastTailRemapsToErrWALCorrupt(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	commitRows(t, db, "t1", 8, 100)

	w := db.pager.wal
	nf := w.nFrame.Load()
	require.Greater(t, nf, uint32(0))

	// Multi-process mode: a frame beyond the local nFrame is attempted
	// against the file (a peer may have committed it); here nothing did, so
	// ReadAt fails past EOF and the failure is remapped.
	buf := make([]byte, db.pager.pageSize)
	err = w.readFrame(nf+100, buf, nil, nil)
	require.ErrorIs(t, err, ErrWALCorrupt)
	err = w.readFrameRaw(nf+100, buf)
	require.ErrorIs(t, err, ErrWALCorrupt)
}

// TestReadFrameNoBenignFailuresUnderTruncateStress is the multi-process-mode
// (default Options, single process) sibling of
// TestInProcessSlot0FallbackTruncateStress: with WAL read errors propagating,
// a writer, a TRUNCATE checkpointer, and scanning readers must produce zero
// read errors and only self-consistent values — empirically pinning that no
// benign readFrame failure exists under concurrent reset (the structural
// argument in TestWALReadFrameErrorPropagates). Run with -race.
func TestReadFrameNoBenignFailuresUnderTruncateStress(t *testing.T) {
	const (
		nKeys    = 24
		nReaders = 4
		valSize  = 2048
	)
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096})
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
				return fmt.Errorf("key %d seq %d: torn value at byte %d", key, seq, i)
			}
		}
		return nil
	}

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := uint32(0); i < nKeys; i++ {
		key := binary.BigEndian.AppendUint32(nil, i)
		require.NoError(t, wtx.Put(ns, key, mkVal(i, 0)))
	}
	require.NoError(t, wtx.Commit())

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

	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = db.Checkpoint(CheckpointTruncate)
			time.Sleep(500 * time.Microsecond)
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

// TestReadHeaderCountersWALErrorPropagates pins the same propagation contract
// on the page-1 header-counter read that BeginRead's staleness detection uses
// (readHeaderCounters → readWalFrameData, which shares the fault hook): a WAL
// read failure must fail BeginRead instead of silently serving the stale
// DB-file FileChangeCount/SchemaCookie, which would defeat cross-process
// staleness detection and backup restart detection.
func TestReadHeaderCountersWALErrorPropagates(t *testing.T) {
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	commitRows(t, db, "t1", 32, 200)

	// Page 1 carries FileChangeCount/SchemaCookie and is rewritten by every
	// committing tx, so it must resolve to a live WAL frame.
	nf := db.pager.wal.nFrame.Load()
	page1Frame := mustWiGet(t, db.pager.wal.index, 1, nf)
	require.NotZero(t, page1Frame, "page 1 must resolve to a WAL frame after a commit")

	sentinel := errors.New("injected page-1 header read fault")
	setWalReadFrameFaultHook(func(frame uint32) error {
		if frame == page1Frame {
			return sentinel
		}
		return nil
	})

	_, err = db.BeginRead()
	require.ErrorIs(t, err, sentinel,
		"BeginRead must fail when the page-1 counter read fails, not serve stale counters")
	require.Contains(t, err.Error(), "header counters")

	setWalReadFrameFaultHook(nil)
	rtx, err := db.BeginRead()
	require.NoError(t, err, "BeginRead must recover once the fault clears")
	require.NoError(t, rtx.Rollback())
}
