//go:build (linux || darwin) && (amd64 || arm64)

package btree

// A wal-index segment that cannot be mapped fails the page lookup, and with
// it the page read: walFindFrame returns walHashGet's error (wal.c:3576-3579
// `rc = walHashGet(pWal, iHash, &sLoc); if( rc!=SQLITE_OK ){ return rc; }`).
// Taking the segment for one that does not hold the page would send the read
// to an older frame or to the database file: the page from before a commit.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

var errSegmentMap = errors.New("injected: wal-index segment not mapped")

// mapErrorDB returns a database whose WAL reaches into the second wal-index
// segment: rows 0 and 1 were committed with fill 1 in the first segment, row
// 0 again with fill 2 in the second. failSegment makes mapping that segment
// fail, restore undoes it.
func mapErrorDB(t *testing.T) (db *DB, failSegment, restore func()) {
	t.Helper()
	opts := restartModes[0].opts // shared memory
	opts.NoCommitSync = true
	db, err := testOpen(t, filepath.Join(t.TempDir(), "test.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	restartPut(t, db, 0, 2, 1)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.GetNamespace("t1")
	require.NoError(t, err)
	big := bytes.Repeat([]byte{7}, 3000)
	for i := 0; i < htNPageOne+200; i++ { // about a page each
		require.NoError(t, wtx.Put(ns, binary.BigEndian.AppendUint32(nil, uint32(1000+i)), big))
	}
	require.NoError(t, wtx.Commit())
	require.Greater(t, db.pager.wal.index.mxCommitFrame.LoadLocal(), uint32(htNPageOne), "setup: the first segment is full")
	restartPut(t, db, 0, 1, 2)

	wi := db.pager.wal.index
	orig := wi.shm
	failSegment = func() { wi.shm = &errorRegionShm{shm: orig, failRegion: 1, regionErr: errSegmentMap} }
	restore = func() { wi.shm = orig }
	return db, failSegment, restore
}

// The reader's transaction and namespace are open before the segment fails,
// and the leaf of row 0 is not in its cache: the failure is the Get's.
func TestWalIndexMapError_ReaderFails(t *testing.T) {
	db, failSegment, restore := mapErrorDB(t)
	coldReaders(db)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	failSegment()
	_, err = rtx.Get(ns, binary.BigEndian.AppendUint32(nil, 0))
	require.ErrorIs(t, err, errSegmentMap)

	restore()
	require.NoError(t, rtx.Rollback())
	restartCheck(t, db, 0, 1, 2)
	restartCheck(t, db, 1, 2, 1)
	require.NoError(t, db.IntegrityCheck())
}

// The writer reads the leaf of rows 0 and 1 to change row 1. With the page
// from before row 0's commit it would write that commit away. The
// transaction and namespace are open before the segment fails, and the leaf
// is not in the writer's cache: the failure is the Put's page read (the
// transaction never gets as far as writing a frame, which maps the segment
// too, in shmHashWrite).
func TestWalIndexMapError_WriterFails(t *testing.T) {
	db, failSegment, restore := mapErrorDB(t)
	db.pager.writerCache.clear() // the writer has to read its pages
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.GetNamespace("t1")
	require.NoError(t, err)
	failSegment()
	err = wtx.Put(ns, binary.BigEndian.AppendUint32(nil, 1), bytes.Repeat([]byte{3}, 300))
	require.ErrorIs(t, err, errSegmentMap)

	restore()
	require.NoError(t, wtx.Rollback())
	restartCheck(t, db, 0, 1, 2)
	restartCheck(t, db, 1, 2, 1)
	restartPut(t, db, 1, 2, 3) // the writer goes on
	restartCheck(t, db, 0, 1, 2)
	restartCheck(t, db, 1, 2, 3)
	require.NoError(t, db.IntegrityCheck())
}

// The lookups themselves, and the raw page read and the page 1 counters read
// that go through them.
func TestWalIndexMapError_LookupsFail(t *testing.T) {
	db, failSegment, _ := mapErrorDB(t)
	wi := db.pager.wal.index
	mx := wi.mxCommitFrame.LoadLocal()
	region, err := wi.shm.region(1, true)
	require.NoError(t, err)
	seg, idx := htFrameSegIdx(mx)
	require.Equal(t, 1, seg)
	pgno := binary.LittleEndian.Uint32(region[htPgnoOffset(seg, idx):]) // the last commit's last page
	frame, err := wi.get(pgno, mx, 1)
	require.NoError(t, err)
	require.Equal(t, mx, frame)

	failSegment()
	_, err = wi.get(pgno, mx, 1)
	require.ErrorIs(t, err, errSegmentMap)
	_, err = wi.getInTxRange(pgno, mx, 1)
	require.ErrorIs(t, err, errSegmentMap)
	_, err = wi.getLatest(pgno)
	require.ErrorIs(t, err, errSegmentMap)
	_, err = db.pager.readRawPage(pgno, mx)
	require.ErrorIs(t, err, errSegmentMap)
	_, _, err = db.pager.headerCountersAt(mx, 1, false)
	require.ErrorIs(t, err, errSegmentMap)

	// A lookup that ends below the segment does not map it.
	_, err = wi.get(pgno, htNPageOne, 1)
	require.NoError(t, err)
}

// A snapshot that is entirely in the database file has an empty frame window
// (minFrame above maxFrame, what a reader on slot 0 gets): its lookups touch
// no segment, so one that cannot be mapped does not fail them. walFindFrame
// returns before its loop for such a reader (wal.c:3538-3541 `if( iLast==0
// || (pWal->readLock==0 && pWal->bShmUnreliable==0) ){ *piRead = 0; return
// SQLITE_OK; }`).
func TestWalIndexLookup_EmptyWindowMapsNothing(t *testing.T) {
	db, failSegment, _ := mapErrorDB(t)
	wi := db.pager.wal.index
	mx := wi.mxCommitFrame.LoadLocal()
	seg, idx := htFrameSegIdx(mx)
	require.Equal(t, 1, seg)
	require.Less(t, idx, htNPage-1, "setup: frame mx+1 is in the same segment")
	failSegment()

	frame, err := wi.get(2, mx, mx+1)
	require.NoError(t, err)
	require.Zero(t, frame)
	frame, err = wi.getInTxRange(2, mx, mx+1)
	require.NoError(t, err)
	require.Zero(t, frame)
	frame, err = wi.shmHashGet(2, mx, mx+1)
	require.NoError(t, err)
	require.Zero(t, frame)

	heap, err := newWalIndex("", true)
	require.NoError(t, err)
	defer func() { _ = heap.close(false) }()
	require.NoError(t, heap.set(2, 1))
	for _, get := range []func(pgno, maxFrame, minFrame uint32) (uint32, error){heap.get, heap.getInTxRange, heap.shmHashGet} {
		frame, err = get(2, 1, 2)
		require.NoError(t, err)
		require.Zero(t, frame)
		frame, err = get(2, 1, 1)
		require.NoError(t, err)
		require.Equal(t, uint32(1), frame)
	}
}

// The same through a read transaction: once the log is backfilled, a reader
// takes slot 0 and reads every page, page 1 included, from the database
// file, with a segment of the log that cannot be mapped.
func TestWalIndexMapError_CheckpointedReaderReads(t *testing.T) {
	db, failSegment, restore := mapErrorDB(t)
	require.NoError(t, db.Checkpoint(CheckpointRestart))
	wi := db.pager.wal.index
	require.Equal(t, wi.mxCommitFrame.LoadLocal(), wi.liveNBackfill(), "setup: the log is backfilled")
	coldReaders(db)
	failSegment()
	defer restore()
	restartCheck(t, db, 0, 1, 2)
	restartCheck(t, db, 1, 2, 1)
}

// A lookup maps a segment that is in the wal-index file and fails on one that
// is not: it never extends the file. A reader's walIndexPage passes
// pWal->writeLock as the extend flag (wal.c:784-786), unixShmMap leaves a
// region beyond the end of the file unmapped for it (os_unix.c:5163-5164 `if(
// !bExtend ){ goto shmpage_out; }`) and walHashGet turns the missing page
// into an error (wal.c:1185-1187). A segment made of zeros would answer "not
// in the WAL" for every page.
func TestWalIndexLookup_MissingSegmentFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.shm")
	writer, err := newWalIndex(path, false)
	require.NoError(t, err)
	defer func() { _ = writer.close(false) }()
	const nFrames = htNPageOne + 10
	for f := uint32(1); f <= nFrames; f++ {
		require.NoError(t, writer.set(f+1, f))
	}
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(2*shmRegionSize), info.Size(), "setup: the writer made two segments")

	// The readers attach as other processes do: they find the file in use
	// (the probe of the DMS byte reports a shared lock) and leave it as it
	// is. In one process the election would truncate it (newPlatformShm).
	stubDMSFcntl(t, func(_ func(uintptr, int, *syscall.Flock_t) syscall.Errno, _ uintptr, cmd int, flock *syscall.Flock_t) (syscall.Errno, bool) {
		if cmd == syscall.F_GETLK {
			flock.Type = syscall.F_RDLCK
			return 0, true
		}
		return 0, false
	})
	attach := func() *walIndex {
		wi, err := newWalIndex(path, false)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wi.close(false) })
		return wi
	}

	// A reader that has mapped nothing maps the second segment, which is in
	// the file, and finds the frame.
	frame, err := attach().shmHashGet(nFrames+1, nFrames, 1)
	require.NoError(t, err)
	require.Equal(t, uint32(nFrames), frame)

	// The file loses its second segment. A reader that has not mapped it
	// fails the lookup and leaves the file as it is.
	reader := attach()
	require.NoError(t, os.Truncate(path, shmRegionSize))
	frame, err = reader.shmHashGet(nFrames+1, nFrames, 1)
	require.ErrorIs(t, err, errShmSegmentMissing, "the lookup answered frame %d from a segment that is not in the file", frame)
	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(shmRegionSize), info.Size(), "the lookup extended the wal-index file")
}
