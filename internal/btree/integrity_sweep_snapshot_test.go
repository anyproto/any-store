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

const sweepPeerEnv = "BTREE_TEST_SWEEP_PEER"

var sweepPeerOpts = Options{PageSize: 4096, Checksum: true, DisableAutoCheckpoint: true}

// runSweepPeer runs TestVerifyIntegritySweep_PeerChild on path in another
// process, with ops separated by ";":
//
//	grow       commit rows 1000..3000 of namespace t1 in one transaction
//	ckpt       TRUNCATE checkpoint
//	exit       exit without closing the database, which would checkpoint
//	flip:P     flip one byte of page P in the database file, with no
//	           handle open on the database
//
// The parent never opens the database file a second time itself: closing
// any descriptor of a file drops the process's fcntl locks on it.
func runSweepPeer(t *testing.T, path, ops string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestVerifyIntegritySweep_PeerChild$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), sweepPeerEnv+"="+path+"|"+ops)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child: %s", out)
}

// TestVerifyIntegritySweep_PeerChild is the child of runSweepPeer.
func TestVerifyIntegritySweep_PeerChild(t *testing.T) {
	arg := os.Getenv(sweepPeerEnv)
	if arg == "" {
		t.Skip("child process only")
	}
	parts := strings.SplitN(arg, "|", 2)
	path := parts[0]
	var db *DB
	for _, op := range strings.Split(parts[1], ";") {
		f := strings.Split(op, ":")
		if f[0] != "flip" && db == nil {
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
		case "flip":
			require.Nil(t, db, "flip runs with no handle on the database")
			pgno, err := strconv.Atoi(f[1])
			require.NoError(t, err)
			file, err := os.OpenFile(path, os.O_RDWR, 0)
			require.NoError(t, err)
			off := int64(pgno-1)*int64(sweepPeerOpts.PageSize) + 500
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

// TestVerifyIntegritySweepsPagesAPeerAdded: the sweep covers the pages of
// the snapshot it reads, as a read transaction counts them (pagerPagecount,
// pager.c:3279-3306: the size the WAL's last commit recorded, else the
// file's), not the pages this process knew when it last wrote or opened. A
// process that has only read since a peer grew the database must verify the
// peer's pages too, and find the damaged one among them.
func TestVerifyIntegritySweepsPagesAPeerAdded(t *testing.T) {
	cases := []struct {
		name string
		ckpt bool // the peer's pages are in the database file, else in the WAL
	}{
		{name: "checkpointed", ckpt: true},
		{name: "in the WAL"},
	}
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

			rtx, err := db.BeginRead()
			require.NoError(t, err)
			snap, err := rtx.DatabaseSize()
			require.NoError(t, err)
			require.NoError(t, rtx.Rollback())
			require.Greater(t, snap, before+100, "setup: the peer grew the database")
			info, err := os.Stat(path)
			require.NoError(t, err)
			filePages := uint32(info.Size() / int64(sweepPeerOpts.PageSize))

			var damaged uint32
			if tc.ckpt {
				require.Equal(t, snap, filePages, "setup: the peer's pages are in the database file")
				damaged = before + (snap-before)/2
				runSweepPeer(t, path, "flip:"+strconv.Itoa(int(damaged)))
			} else {
				require.Equal(t, before, filePages, "setup: the peer's pages are only in the WAL")
			}

			res, err = db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Equal(t, int(snap), res.Pages, "the sweep covers the snapshot's pages")
			if !tc.ckpt {
				require.Empty(t, res.Errors)
				return
			}
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
