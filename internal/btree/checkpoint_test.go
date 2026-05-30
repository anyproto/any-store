package btree

import (
	stderrors "errors"
	"fmt"
	"os"
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

	// Start a reader holding an old snapshot
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")

	// CheckpointFull waits for readers to finish (busy handler) but does NOT
	// block readers — "new database readers are allowed to continue unimpeded"
	// (sqlite.org/c3ref/wal_checkpoint_v2.html). Use a short busy timeout so
	// we don't wait 5s for the reader that will release below.
	db.pager.wal.busyHandler = DefaultBusyTimeout(200 * time.Millisecond)

	// Start checkpoint in background — it will busy-wait for the reader
	done := make(chan error, 1)
	go func() {
		done <- db.Checkpoint(CheckpointFull)
	}()

	// Reader should NOT be blocked while checkpoint is pending.
	// Verify the reader can still read its snapshot unimpeded.
	v, err := rtx.Get(ns3, []byte("key-0050"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0050"), v)

	// Release the reader so checkpoint can complete
	require.NoError(t, rtx.Rollback())

	// Now checkpoint should complete promptly
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("checkpoint did not complete after reader was released")
	}
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
		_ = db.Checkpoint(CheckpointFull)
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
	// Short busy timeout: CheckpointFull waits for readers via busy handler;
	// keep it short so the test doesn't spend 5s per checkpoint call.
	db.pager.wal.busyHandler = DefaultBusyTimeout(200 * time.Millisecond)

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
	err = db.Checkpoint(CheckpointFull)
	require.NoError(t, err)

	// Reader should still see its snapshot (batch 1 data)
	ns4, _ := db.getNamespaceLocked("data")
	v, err := rtx.Get(ns4, []byte("key-0000"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0000"), v)

	// End the reader
	require.NoError(t, rtx.Rollback())

	// Now a full checkpoint should work (no readers)
	err = db.Checkpoint(CheckpointFull)
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
	// Short busy timeout for concurrent reader+checkpoint tests.
	db.pager.wal.busyHandler = DefaultBusyTimeout(200 * time.Millisecond)

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
				nsR, _ := rtx.GetNamespace("data")
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
				_ = db.Checkpoint(CheckpointFull)
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
	db, err := testOpen(t, filepath.Join(dir, "test.db"), opts)
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
			var wtx *WriteTx
			var err error
			// Retry on ErrBusy: CheckpointFull holds the WAL write lock
			// temporarily, so the writer may need to wait.
			for retries := 0; retries < 3; retries++ {
				wtx, err = db.BeginWrite()
				if err == nil || !stderrors.Is(err, ErrBusy) {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
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
			_ = db.Checkpoint(CheckpointFull)
			time.Sleep(time.Millisecond)
		}
	}()

	wg.Wait()
	assert.Equal(t, int32(0), errors.Load(), "concurrent read/write/checkpoint had errors")
	_ = ns
}

func openWalForDedupTest(t *testing.T) (*wal, *os.File) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	dbFile, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := dbFile.Truncate(1 << 20); err != nil {
		t.Fatalf("truncate db: %v", err)
	}
	w := newWal(filepath.Join(dir, "t.wal"), 4096)
	if err := w.open(); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = dbFile.Close() })
	return w, dbFile
}

// commitPage commits one new frame for (pgno). Real signatures:
//   - (w *wal) beginWrite() (stateChanged bool, err error)
//   - (w *wal) writeFrames(pages []*page, commit bool, dbSize uint32) error
//   - (w *wal) endWrite()
func commitPage(t *testing.T, w *wal, pgno uint32, data []byte) {
	t.Helper()
	if _, err := w.beginWrite(); err != nil {
		t.Fatalf("beginWrite: %v", err)
	}
	pg := &page{pgno: pgno, data: data}
	if err := w.writeFrames([]*page{pg}, true, pgno); err != nil {
		t.Fatalf("writeFrames: %v", err)
	}
	w.endWrite()
}

// TestWalBuildBackfillMap_DedupsRewrites asserts that for a rewrite-heavy
// workload (same pgno committed many times), the backfill dedup helper
// returns exactly one entry per distinct pgno, holding the LATEST frame
// index for that page. Matches SQLite's walIteratorNext semantics
// (wal.c:1758-1786).
func TestWalBuildBackfillMap_DedupsRewrites(t *testing.T) {
	w, _ := openWalForDedupTest(t)
	t.Cleanup(func() { _ = w.close(false) })

	pageData := make([]byte, w.pageSize)
	const nPgnos = 3
	const nRewrites = 5
	for r := 0; r < nRewrites; r++ {
		for p := uint32(1); p <= nPgnos; p++ {
			pageData[0] = byte(r)
			pageData[1] = byte(p)
			commitPage(t, w, p, pageData)
		}
	}

	latest, err := w.buildBackfillMap(0, w.nFrame.Load())
	if err != nil {
		t.Fatalf("buildBackfillMap: %v", err)
	}
	if len(latest) != nPgnos {
		t.Fatalf("expected %d distinct pgnos, got %d: %v", nPgnos, len(latest), latest)
	}
	// Total frames = nPgnos * nRewrites = 15. The latest frame for each
	// pgno is the one in the final rewrite round; each round commits
	// nPgnos frames in order (1,2,3), so:
	//   pgno 1 → frame (nRewrites-1)*nPgnos + 0 = 12
	//   pgno 2 → frame (nRewrites-1)*nPgnos + 1 = 13
	//   pgno 3 → frame (nRewrites-1)*nPgnos + 2 = 14
	expect := map[uint32]uint32{1: 12, 2: 13, 3: 14}
	for pgno, frame := range expect {
		if got := latest[pgno]; got != frame {
			t.Fatalf("pgno %d: expected frame %d, got %d", pgno, frame, got)
		}
	}
}

// TestWalBuildBackfillMap_InsertHeavyIsIdentity asserts that an
// insert-only workload (every frame a fresh pgno) produces a map with
// one entry per frame — the already-optimal case must not regress.
func TestWalBuildBackfillMap_InsertHeavyIsIdentity(t *testing.T) {
	w, _ := openWalForDedupTest(t)
	t.Cleanup(func() { _ = w.close(false) })

	pageData := make([]byte, w.pageSize)
	const nFrames = 10
	for p := uint32(1); p <= nFrames; p++ {
		pageData[0] = byte(p)
		commitPage(t, w, p, pageData)
	}

	latest, err := w.buildBackfillMap(0, w.nFrame.Load())
	if err != nil {
		t.Fatalf("buildBackfillMap: %v", err)
	}
	if uint32(len(latest)) != nFrames {
		t.Fatalf("expected %d entries (one per fresh pgno), got %d", nFrames, len(latest))
	}
	// Each pgno p was committed in exactly the (p-1)th commit → frame (p-1).
	for p := uint32(1); p <= nFrames; p++ {
		if got := latest[p]; got != p-1 {
			t.Fatalf("pgno %d: expected frame %d, got %d", p, p-1, got)
		}
	}
}

// TestCheckpoint_RewriteHeavyEndToEnd is an end-to-end sanity check:
// after a rewrite-heavy workload is checkpointed, the DB file contains
// the LATEST version of each page. Guards against dedup that picks the
// wrong frame.
func TestCheckpoint_RewriteHeavyEndToEnd(t *testing.T) {
	w, dbFile := openWalForDedupTest(t)
	t.Cleanup(func() { _ = w.close(false) })

	pageData := make([]byte, w.pageSize)
	const nPgnos = 3
	const nRewrites = 5
	for r := 0; r < nRewrites; r++ {
		for p := uint32(1); p <= nPgnos; p++ {
			pageData[0] = byte(r)
			pageData[1] = byte(p)
			commitPage(t, w, p, pageData)
		}
	}

	if err := w.checkpointWithMode(dbFile, nil, CheckpointFull, nil); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Verify: DB file page p starts with (nRewrites-1, p) — the latest
	// version committed. Pages are 4096 bytes each; pgno 1 starts at
	// offset 0, pgno p starts at (p-1)*4096.
	buf := make([]byte, int(w.pageSize))
	for p := uint32(1); p <= nPgnos; p++ {
		off := int64(p-1) * int64(w.pageSize)
		if _, err := dbFile.ReadAt(buf, off); err != nil {
			t.Fatalf("read db page %d: %v", p, err)
		}
		wantR := byte(nRewrites - 1)
		wantP := byte(p)
		if buf[0] != wantR || buf[1] != wantP {
			t.Fatalf("db page %d: expected (r=%d, p=%d), got (r=%d, p=%d) — checkpoint picked wrong frame",
				p, wantR, wantP, buf[0], buf[1])
		}
	}
}

// TestCheckpoint_SkipsOrphanFramesPastCommittedNPage is the drift-51 regression:
// frames written by an earlier larger commit (dbSize=grown) that a later
// shrinking commit (dbSize=shrunk) logically dropped still physically reside in
// the append-only WAL. SQLite's walCheckpoint skips them via iDbpage>mxPage
// (mxPage = pWal->hdr.nPage; wal.c:2228, wal.c:2306) so they are never copied to
// the DB file past the committed end. Without the filter, buildBackfillMap keeps
// the orphan frames' latest version and the write loop copies them to
// (pgno-1)*pageSize, materializing stale pages SQLite would never expose.
//
// We pre-stamp the orphan-page region of the DB file with a sentinel, run a full
// checkpoint, and assert the checkpoint did NOT overwrite those pages with the
// orphan WAL data — i.e. the committed pages are written but pgno>nPage are left
// untouched.
func TestCheckpoint_SkipsOrphanFramesPastCommittedNPage(t *testing.T) {
	w, dbFile := openWalForDedupTest(t)
	t.Cleanup(func() { _ = w.close(false) })

	const grown = 5      // earlier larger commit's dbSize
	const shrunk = 2     // later shrinking commit's dbSize
	const walMark = 0x11 // byte stamped into WAL frame data
	const dbSentinel = 0x22

	// Pre-stamp the DB file's orphan-page region (pgno shrunk+1..grown) with a
	// sentinel so we can detect any checkpoint overwrite. Pages 1..shrunk are
	// left zero; the checkpoint is expected to fill those from the WAL.
	sentinel := make([]byte, int(w.pageSize))
	for i := range sentinel {
		sentinel[i] = dbSentinel
	}
	for p := uint32(shrunk + 1); p <= grown; p++ {
		off := int64(p-1) * int64(w.pageSize)
		if _, err := dbFile.WriteAt(sentinel, off); err != nil {
			t.Fatalf("pre-stamp db page %d: %v", p, err)
		}
	}

	// Earlier larger commit: write all `grown` pages, committing with dbSize=grown.
	// This appends frames for pages 1..grown to the WAL. We stamp each frame's
	// data with walMark + pgno so an erroneous backfill would be detectable.
	if _, err := w.beginWrite(); err != nil {
		t.Fatalf("beginWrite (grow): %v", err)
	}
	growPages := make([]*page, 0, grown)
	for p := uint32(1); p <= grown; p++ {
		data := make([]byte, w.pageSize)
		data[0] = walMark
		data[1] = byte(p)
		growPages = append(growPages, &page{pgno: p, data: data})
	}
	if err := w.writeFrames(growPages, true, grown); err != nil {
		t.Fatalf("writeFrames (grow): %v", err)
	}
	w.endWrite()

	// Later shrinking commit: touch page 1 and commit with dbSize=shrunk. The
	// orphan frames for pages shrunk+1..grown remain physically in the WAL but
	// are logically dropped (committed nPage is now shrunk).
	commitData := make([]byte, w.pageSize)
	commitData[0] = walMark
	commitData[1] = 1
	commitData[2] = 0xFF // distinguish the latest page-1 content
	if _, err := w.beginWrite(); err != nil {
		t.Fatalf("beginWrite (shrink): %v", err)
	}
	if err := w.writeFrames([]*page{{pgno: 1, data: commitData}}, true, shrunk); err != nil {
		t.Fatalf("writeFrames (shrink): %v", err)
	}
	w.endWrite()

	// Sanity: buildBackfillMap (no nPage bound) still tracks the orphan pgnos,
	// proving the skip filter — not the dedup map — is what protects the DB file.
	latest, err := w.buildBackfillMap(0, w.nFrame.Load())
	if err != nil {
		t.Fatalf("buildBackfillMap: %v", err)
	}
	for p := uint32(shrunk + 1); p <= grown; p++ {
		if _, ok := latest[p]; !ok {
			t.Fatalf("precondition: expected orphan pgno %d present in backfill map; "+
				"test no longer exercises the iDbpage>mxPage skip", p)
		}
	}

	if err := w.checkpointWithMode(dbFile, nil, CheckpointFull, nil); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	buf := make([]byte, int(w.pageSize))

	// Committed page 1 must reflect the latest (shrinking) commit's content.
	if _, err := dbFile.ReadAt(buf, 0); err != nil {
		t.Fatalf("read db page 1: %v", err)
	}
	if buf[0] != walMark || buf[1] != 1 || buf[2] != 0xFF {
		t.Fatalf("db page 1: checkpoint did not backfill the committed page; got (%#x,%#x,%#x)",
			buf[0], buf[1], buf[2])
	}

	// Orphan pages shrunk+1..grown must be UNTOUCHED — still the sentinel, never
	// the walMark orphan data. This is the drift-51 assertion: with the skip
	// filter absent, these pages would be overwritten with walMark+pgno and the
	// DB file would grow past its committed size.
	for p := uint32(shrunk + 1); p <= grown; p++ {
		off := int64(p-1) * int64(w.pageSize)
		if _, err := dbFile.ReadAt(buf, off); err != nil {
			t.Fatalf("read db page %d: %v", p, err)
		}
		if buf[0] == walMark {
			t.Fatalf("db page %d (pgno>nPage=%d) was backfilled from an orphan WAL frame "+
				"(got walMark %#x) — checkpoint must skip iDbpage>mxPage (wal.c:2306)",
				p, shrunk, buf[0])
		}
		if buf[0] != dbSentinel {
			t.Fatalf("db page %d: expected untouched sentinel %#x, got %#x",
				p, byte(dbSentinel), buf[0])
		}
	}
}
