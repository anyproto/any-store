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
	"path/filepath"
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

func TestWalIndexMapError_ReaderFails(t *testing.T) {
	db, failSegment, restore := mapErrorDB(t)
	failSegment()
	err := func() error {
		rtx, err := db.BeginRead()
		if err != nil {
			return err
		}
		defer func() { _ = rtx.Rollback() }()
		ns, err := rtx.GetNamespace("t1")
		if err != nil {
			return err
		}
		_, err = rtx.Get(ns, binary.BigEndian.AppendUint32(nil, 0))
		return err
	}()
	require.ErrorIs(t, err, errSegmentMap)

	restore()
	restartCheck(t, db, 0, 1, 2)
	restartCheck(t, db, 1, 2, 1)
	require.NoError(t, db.IntegrityCheck())
}

// The writer reads the leaf of rows 0 and 1 to change row 1. With the page
// from before row 0's commit it would write that commit away.
func TestWalIndexMapError_WriterFails(t *testing.T) {
	db, failSegment, restore := mapErrorDB(t)
	db.pager.writerCache.clear() // the writer has to read its pages
	failSegment()
	// The failure is the page read's: the transaction never gets as far as
	// writing a frame (which maps the segment too, in shmHashWrite).
	err := func() error {
		wtx, err := db.BeginWrite()
		if err != nil {
			return err
		}
		defer func() { _ = wtx.Rollback() }()
		ns, err := wtx.GetNamespace("t1")
		if err != nil {
			return err
		}
		return wtx.Put(ns, binary.BigEndian.AppendUint32(nil, 1), bytes.Repeat([]byte{3}, 300))
	}()
	require.ErrorIs(t, err, errSegmentMap)

	restore()
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

// BenchmarkShmHashGet is the shared-memory page lookup over two segments: a
// page found in the newest segment, one found in the oldest, one in neither.
func BenchmarkShmHashGet(b *testing.B) {
	wi, err := newWalIndex(filepath.Join(b.TempDir(), "test.shm"), false)
	requireNoErrorB(b, err)
	defer func() { _ = wi.close(true) }()
	const nFrames = htNPageOne + 2000
	for f := uint32(1); f <= nFrames; f++ {
		requireNoErrorB(b, wi.set(f+1, f))
	}
	for _, c := range []struct {
		name string
		pgno uint32
	}{{"newest", nFrames}, {"oldest", 100}, {"miss", nFrames + 100}} {
		b.Run(c.name, func(b *testing.B) {
			var sum uint32
			for i := 0; i < b.N; i++ {
				f, err := wi.shmHashGet(c.pgno, nFrames, 1)
				if err != nil {
					b.Fatal(err)
				}
				sum += f
			}
			_ = sum
		})
	}
}
