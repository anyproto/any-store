//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// The child's database path and its operations travel in two variables: a
// path may hold any separator a single value would be split on.
const (
	sweepPeerPathEnv = "BTREE_TEST_SWEEP_PEER_PATH"
	sweepPeerOpsEnv  = "BTREE_TEST_SWEEP_PEER_OPS"
)

var sweepPeerOpts = Options{PageSize: 4096, Checksum: true, DisableAutoCheckpoint: true}

// runSweepPeer runs TestVerifyIntegritySweep_PeerChild on path in another
// process, with ops separated by ";":
//
//	grow        commit rows 1000..3000 of namespace t1 in one transaction
//	ckpt        TRUNCATE checkpoint
//	exit        exit without closing the database, which would checkpoint
//	flip:OFF    flip the byte at offset OFF of the database file, with no
//	            handle open on the database
//	flipwal:OFF the same in the WAL file
//
// The parent never opens the database's files a second time itself: closing
// any descriptor of a file drops the process's fcntl locks on it.
func runSweepPeer(t *testing.T, path, ops string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestVerifyIntegritySweep_PeerChild$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), sweepPeerPathEnv+"="+path, sweepPeerOpsEnv+"="+ops)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child: %s", out)
}

// TestVerifyIntegritySweep_PeerChild is the child of runSweepPeer.
func TestVerifyIntegritySweep_PeerChild(t *testing.T) {
	path := os.Getenv(sweepPeerPathEnv)
	if path == "" {
		t.Skip("child process only")
	}
	var db *DB
	for _, op := range strings.Split(os.Getenv(sweepPeerOpsEnv), ";") {
		f := strings.Split(op, ":")
		flip := f[0] == "flip" || f[0] == "flipwal"
		if !flip && db == nil {
			var err error
			db, err = Open(path, sweepPeerOpts)
			require.NoError(t, err)
		}
		switch f[0] {
		case "grow":
			dmsPutRange(t, db, "t1", 1000, 3000)
		case "ckpt":
			require.NoError(t, db.Checkpoint(CheckpointTruncate))
		case "exit":
			os.Exit(0)
		case "flip", "flipwal":
			require.Nil(t, db, "a flip runs with no handle on the database")
			off, err := strconv.ParseInt(f[1], 10, 64)
			require.NoError(t, err)
			name := path
			if f[0] == "flipwal" {
				name += "-wal"
			}
			file, err := os.OpenFile(name, os.O_RDWR, 0)
			require.NoError(t, err)
			var b [1]byte
			_, err = file.ReadAt(b[:], off)
			require.NoError(t, err)
			b[0] ^= 0xFF
			_, err = file.WriteAt(b[:], off)
			require.NoError(t, err)
			require.NoError(t, file.Close())
		default:
			t.Fatalf("unknown op %q", op)
		}
	}
	if db != nil {
		require.NoError(t, db.Close())
	}
}

// sweepSnapshot returns the page count of a read transaction begun now and,
// for pgno != 0, the WAL frame that transaction reads pgno from (0: the
// database file). The transaction ends with the call, whatever fails.
func sweepSnapshot(t *testing.T, db *DB, pgno uint32) (nPages, frame uint32) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	nPages, err = rtx.DatabaseSize()
	require.NoError(t, err)
	if pgno != 0 {
		frame, err = db.pager.wal.index.get(pgno, rtx.walMaxFrame, db.pager.readerMinFrame(rtx.cache))
		require.NoError(t, err)
	}
	return nPages, frame
}

// TestVerifyIntegritySweepsPagesAPeerAdded: the sweep covers the pages of
// the snapshot it reads, as a read transaction counts them (pagerPagecount,
// pager.c:3279-3306: the size the WAL's last commit recorded, else the
// file's), not the pages this process knew when it last wrote or opened. A
// process that has only read since a peer grew the database must verify the
// peer's pages too, in the database file and in the WAL, and find the
// damaged one among them.
func TestVerifyIntegritySweepsPagesAPeerAdded(t *testing.T) {
	cases := []struct {
		name string
		ckpt bool // the peer's pages are in the database file, else in the WAL
	}{
		{name: "checkpointed", ckpt: true},
		{name: "in the WAL"},
	}
	pageSize := int64(sweepPeerOpts.PageSize)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir() + "/test.db"
			db, err := testOpen(t, path, sweepPeerOpts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			dmsPutRange(t, db, "t1", 0, 10)
			require.NoError(t, db.Checkpoint(CheckpointTruncate))

			ctx := context.Background()
			res, err := db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Empty(t, res.Errors)
			before := uint32(res.Pages)
			require.NotZero(t, before)

			// From here on this process only reads.
			ops := "grow"
			if tc.ckpt {
				ops += ";ckpt"
			} else {
				ops += ";exit"
			}
			runSweepPeer(t, path, ops)

			snap, _ := sweepSnapshot(t, db, 0)
			require.Greater(t, snap, before+100, "setup: the peer grew the database")
			info, err := os.Stat(path)
			require.NoError(t, err)
			filePages := uint32(info.Size() / pageSize)

			// One byte of a page the peer added, where the snapshot reads it.
			damaged := before + (snap-before)/2
			if tc.ckpt {
				require.Equal(t, snap, filePages, "setup: the peer's pages are in the database file")
				runSweepPeer(t, path, "flip:"+strconv.FormatInt(int64(damaged-1)*pageSize+500, 10))
			} else {
				require.Equal(t, before, filePages, "setup: the peer's pages are only in the WAL")
				_, frame := sweepSnapshot(t, db, damaged)
				require.NotZero(t, frame, "setup: the page is read from a WAL frame")
				off := int64(walHeaderSize) + int64(frame-1)*(int64(walFrameSize)+pageSize) + int64(walFrameSize) + 500
				runSweepPeer(t, path, "flipwal:"+strconv.FormatInt(off, 10))
			}

			res, err = db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Equal(t, int(snap), res.Pages, "the sweep covers the snapshot's pages")
			require.Len(t, res.Errors, 1)
			require.Equal(t, damaged, res.Errors[0].PageNo)
			require.Equal(t, IntegrityChecksumMismatch, res.Errors[0].Kind)
		})
	}
}

// TestVerifyIntegritySweepBesideGrowingCommit: a sweep that begins while a
// commit which grew the database has not yet published its frames reads the
// snapshot before that commit, and that snapshot's page count. The writer's
// count already includes pages no reader can see; a sweep bounded by it read
// past its snapshot and reported a healthy database's pages as errors (and,
// with mmap on, read mapped memory past the end of the file).
func TestVerifyIntegritySweepBesideGrowingCommit(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{name: "in-process", opts: Options{PageSize: 4096, Checksum: true, DisableAutoCheckpoint: true, InProcess: true}},
		{name: "multi-process", opts: Options{PageSize: 4096, Checksum: true, DisableAutoCheckpoint: true}},
		{name: "multi-process mmap", opts: Options{PageSize: 4096, Checksum: true, DisableAutoCheckpoint: true, MmapSize: 1 << 30}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, err := testOpen(t, t.TempDir()+"/test.db", tc.opts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			dmsPutRange(t, db, "t1", 0, 50)
			require.NoError(t, db.Checkpoint(CheckpointTruncate))

			ctx := context.Background()
			res, err := db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Empty(t, res.Errors)
			before := res.Pages

			// The sweep runs to its end inside the commit, after the
			// commit has taken the grown size and before it writes a frame.
			var (
				ran      bool
				sweep    SweepResult
				sweepErr error
				writerN  uint32
			)
			testCommitPrePublishHook = func() {
				testCommitPrePublishHook = nil
				ran = true
				writerN = db.pager.dbSize.Load()
				done := make(chan struct{})
				go func() {
					defer close(done)
					sweep, sweepErr = db.VerifyIntegrity(ctx)
				}()
				<-done
			}
			defer func() { testCommitPrePublishHook = nil }()
			dmsPutRange(t, db, "t1", 1000, 1400)

			require.True(t, ran, "setup: the sweep ran inside the commit")
			require.Greater(t, writerN, uint32(before)+20, "setup: the commit grew the database")
			require.NoError(t, sweepErr)
			require.Equal(t, before, sweep.Pages, "the sweep covers its own snapshot's pages")
			require.Empty(t, sweep.Errors, "a healthy database")

			res, err = db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Equal(t, int(writerN), res.Pages)
			require.Empty(t, res.Errors)
		})
	}
}

// TestReadRawPageSlot0IgnoresNewWALGeneration: the sweep's page read looks a
// page up in the WAL within its read transaction's frame window, floor
// included, as every reader does (pWal->minFrame, walFindFrame,
// wal.c:3538-3541). A snapshot of a fully checkpointed WAL holds read slot
// 0 and reads nothing from the WAL; a WAL restart needs only slots 1-4, so
// it can run under that snapshot, and the next writer's frames are numbered
// from 1 again, below the snapshot's mxFrame. The read must keep returning
// the snapshot's pages, from the database file.
func TestReadRawPageSlot0IgnoresNewWALGeneration(t *testing.T) {
	db, err := testOpen(t, t.TempDir()+"/test.db", restartNPageOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	const rows = 200
	for i := 0; i < rows; i += 5 {
		dmsPutRange(t, db, "t1", i, i+5)
	}
	require.NoError(t, db.Checkpoint(CheckpointFull))
	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, hdr.mxFrame, db.pager.wal.index.shmNBackfill(), "setup: the WAL is fully backfilled")

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	require.Equal(t, rtx.walMaxFrame+1, rtx.cache.minFrame, "setup: the snapshot reads nothing from the WAL")
	nPages, err := rtx.DatabaseSize()
	require.NoError(t, err)
	snapshot := make([][]byte, nPages+1)
	for pgno := uint32(1); pgno <= nPages; pgno++ {
		snapshot[pgno], err = db.pager.readRawPage(pgno, rtx.walMaxFrame, rtx.cache)
		require.NoError(t, err)
	}

	// Under the snapshot: the log starts over, and a commit that empties
	// the tree rewrites its pages in frames 1..k of the new generation.
	require.NoError(t, db.Checkpoint(CheckpointRestart))
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < rows; i++ {
		require.NoError(t, wtx.Delete(ns, binary.BigEndian.AppendUint32(nil, uint32(i))))
	}
	require.NoError(t, wtx.Commit())
	after, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.NotZero(t, after.mxFrame)
	require.Less(t, after.mxFrame, hdr.mxFrame, "setup: the new frames are numbered below the snapshot's mxFrame")

	for pgno := uint32(1); pgno <= nPages; pgno++ {
		raw, err := db.pager.readRawPage(pgno, rtx.walMaxFrame, rtx.cache)
		require.NoError(t, err)
		require.True(t, bytes.Equal(snapshot[pgno], raw), "page %d is not the snapshot's", pgno)
	}
}

// sweepCountDB is a checksum database of many pages whose handle stays
// open, and its page count.
func sweepCountDB(t *testing.T, opts Options) (*DB, uint32) {
	t.Helper()
	db, err := testOpen(t, t.TempDir()+"/test.db", opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	dmsPutRange(t, db, "t1", 0, 2000)
	n := db.pager.dbSize.Load()
	require.Greater(t, n, uint32(150), "setup: a database of many pages")
	return db, n
}

// sweepPatchFile writes b into the database file at off through the pager's
// own descriptor: no second descriptor of the file is opened and closed.
func sweepPatchFile(t *testing.T, db *DB, off int64, b []byte) {
	t.Helper()
	_, err := db.pager.file.WriteAt(b, off)
	require.NoError(t, err)
}

// TestVerifyIntegritySweepDamagedPageCount: page 1's header is outside both
// codecs, so nothing tags its page count, and a smaller count with the
// change counters intact is a well-formed header (lockBtree accepts it,
// btree.c:3294-3298: a file may hold pages past the logical size). Where the
// snapshot's WAL commit recorded the database's size, that is a second count
// the sweep has: it lists page 1 for the disagreement and sweeps the pages
// the commit recorded. A TRUNCATE checkpoint empties the WAL and keeps that
// size, as walRestartHdr keeps nPage (wal.c:2146-2160).
func TestVerifyIntegritySweepDamagedPageCount(t *testing.T) {
	inProcess := Options{PageSize: 4096, Checksum: true, DisableAutoCheckpoint: true, InProcess: true}
	cases := []struct {
		name string
		opts Options
		ckpt CheckpointMode
	}{
		{name: "multi-process/full", opts: sweepPeerOpts, ckpt: CheckpointFull},
		{name: "multi-process/truncate", opts: sweepPeerOpts, ckpt: CheckpointTruncate},
		{name: "in-process/full", opts: inProcess, ckpt: CheckpointFull},
		{name: "in-process/truncate", opts: inProcess, ckpt: CheckpointTruncate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, n := sweepCountDB(t, tc.opts)
			// Everything in the database file; the size the last commit
			// recorded is still the snapshot's.
			require.NoError(t, db.Checkpoint(tc.ckpt))
			ctx := context.Background()
			res, err := db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Equal(t, int(n), res.Pages)
			require.Empty(t, res.Errors)

			// One bit of the count, and a byte of a page past the new count.
			small := n &^ 128
			require.Less(t, small, n)
			sweepPatchFile(t, db, 28, binary.BigEndian.AppendUint32(nil, small))
			damaged := small + 10
			off := int64(damaged-1)*int64(tc.opts.PageSize) + 500
			sweepPatchFile(t, db, off, []byte{0xA5, 0x5A, 0xA5, 0x5A})

			res, err = db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Equal(t, int(n), res.Pages, "the sweep covers the pages the WAL's commit recorded")
			require.Len(t, res.Errors, 2)
			require.Equal(t, uint32(1), res.Errors[0].PageNo)
			require.Equal(t, IntegrityKindUnknown, res.Errors[0].Kind)
			require.ErrorIs(t, res.Errors[0].Inner, ErrHeaderCorrupt)
			require.Equal(t, damaged, res.Errors[1].PageNo)
			require.Equal(t, IntegrityChecksumMismatch, res.Errors[1].Kind)
		})
	}
}

// TestVerifyIntegritySweepLogicalSizeBelowFile: with no commit in the WAL
// the pager's count is the file's length, and a file may be longer than the
// database (lockBtree, btree.c:3294-3298). The sweep covers the pages page 1
// counts, as sqlite_dbpage and the integrity check do; the pages past them
// are no part of the database. A page count damaged downwards is not told
// apart from this, here or in SQLite. In every mode a snapshot carries a
// size only once a commit has recorded one (walIndexRecover zeroes the
// header and sets nPage from a commit frame, wal.c:1408, 1512), so the
// file's length is never taken for a commit's.
func TestVerifyIntegritySweepLogicalSizeBelowFile(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{name: "multi-process", opts: sweepPeerOpts},
		{name: "in-process", opts: Options{PageSize: 4096, Checksum: true, DisableAutoCheckpoint: true, InProcess: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := t.TempDir() + "/test.db"
			db, err := testOpen(t, path, tc.opts)
			require.NoError(t, err)
			dmsPutRange(t, db, "t1", 0, 2000)
			n := db.pager.dbSize.Load()
			require.NoError(t, db.Checkpoint(CheckpointTruncate))
			require.NoError(t, db.Close())

			// Three pages past the database's end, with no valid tag.
			file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			require.NoError(t, err)
			_, err = file.Write(bytes.Repeat([]byte{0xEE}, 3*int(tc.opts.PageSize)))
			require.NoError(t, err)
			require.NoError(t, file.Close())

			db, err = testOpen(t, path, tc.opts)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			bound := db.pager.readerDbSizeBound(rtx.cache)
			require.NoError(t, rtx.Rollback())
			require.Equal(t, n+3, bound, "setup: the pager's count is the file's length")

			res, err := db.VerifyIntegrity(context.Background())
			require.NoError(t, err)
			require.Equal(t, int(n), res.Pages)
			require.Empty(t, res.Errors)
		})
	}
}
