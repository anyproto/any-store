//go:build (linux || darwin) && (amd64 || arm64)

package btree

// pager.open settles the database size as SQLite does when a transaction
// starts: the pager's count is the size recorded by the WAL's last commit,
// or the file's when the WAL has none (pagerPagecount, pager.c:3279-3306),
// page 1 is read at the same snapshot (lockBtree, btree.c:3284-3289), and
// lockBtree's rule decides between its size and the pager's count
// (btree.c:3294-3298, 3401-3408).
//
// The "peer attached" case uses a second process: a second handle in this
// process would not see the first one's DMS lock (fcntl locks are per
// process) and would elect itself first attacher and reset the shm under
// the first handle.

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var openSizeOpts = Options{PageSize: 4096, DisableAutoCheckpoint: true}

// shrinkDstInWAL builds a destination whose DB file holds many pages and
// whose WAL then commits a much smaller database (a backup from a small
// source truncates the destination at commit, and nothing checkpoints it).
// Returns the open destination, its path and the committed (small) size.
func shrinkDstInWAL(t *testing.T, opts Options) (dst *DB, dstPath string, committed uint32) {
	t.Helper()
	dir := t.TempDir()
	opts.DisableAutoCheckpoint = true

	src, err := testOpen(t, filepath.Join(dir, "src.db"), opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = src.Close() })
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	sns, err := stx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, stx.Put(sns, []byte("small"), []byte("v")))
	require.NoError(t, stx.Commit())

	dstPath = filepath.Join(dir, "dst.db")
	dst, err = testOpen(t, dstPath, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dst.Close() })
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	dns, err := dtx.CreateNamespace("data")
	require.NoError(t, err)
	fat := make([]byte, 1000)
	for i := 0; i < 400; i++ {
		require.NoError(t, dtx.Put(dns, fmt.Appendf(nil, "dst-%04d", i), fat))
	}
	require.NoError(t, dtx.Commit())
	// The large image reaches the DB file, page-1 header included.
	require.NoError(t, dst.Checkpoint(CheckpointFull))
	large := dst.DatabaseSize()

	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		err := b.Step(context.Background(), -1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())
	committed = dst.DatabaseSize()
	require.Equal(t, src.DatabaseSize(), committed)
	require.Less(t, committed*10, large, "test setup: the WAL must commit a much smaller database")

	info, err := os.Stat(dstPath)
	require.NoError(t, err)
	require.Equal(t, int64(large)*int64(dst.pager.pageSize), info.Size(),
		"test setup: the DB file must still hold the large image")
	return dst, dstPath, committed
}

// assertOpensAtCommittedSize checks a freshly opened handle on a database
// whose WAL committed a shrink: it starts at the WAL's committed size, and
// its first commit does not bring the dropped pages back.
func assertOpensAtCommittedSize(t *testing.T, db *DB, committed uint32) {
	t.Helper()
	assert.Equal(t, committed, db.DatabaseSize(),
		"open must take the database size from the WAL's last commit, not from the larger DB file")
	assert.Equal(t, committed, db.pager.header.DatabaseSize)

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.GetNamespace("data")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("after-open"), []byte("v")))
	require.NoError(t, wtx.Commit())

	// One small row: the size may not grow by more than a page or two.
	require.LessOrEqual(t, db.DatabaseSize(), committed+2,
		"the first commit after open resurrected the pages the shrink dropped")
	require.Equal(t, db.DatabaseSize(), db.pager.header.DatabaseSize)
	require.Equal(t, db.DatabaseSize(), db.pager.wal.index.maxPage.Load(), "the commit frame carries the same size")
	require.NoError(t, db.IntegrityCheck())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	rns, err := rtx.GetNamespace("data")
	require.NoError(t, err)
	got, err := rtx.Get(rns, []byte("small"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	require.NoError(t, rtx.Rollback())
}

const (
	openSizeEnvPath   = "TEST_OPENSIZE_DB_PATH"
	openSizeEnvMode   = "TEST_OPENSIZE_MODE"
	openSizeEnvExpect = "TEST_OPENSIZE_EXPECT"
)

// openSizeChild is the second process of the re-exec tests. Modes:
//
//	open-write: open, expect DatabaseSize()==TEST_OPENSIZE_EXPECT, write, and
//	            check the size stays put
//	checkpoint: open, run a FULL checkpoint, exit without closing
//
// It exits without closing the database, so it never runs a close-time
// checkpoint under the parent.
func openSizeChild(t *testing.T) bool {
	path := os.Getenv(openSizeEnvPath)
	if path == "" {
		return false
	}
	mode := os.Getenv(openSizeEnvMode)
	db, err := testOpen(t, path, openSizeOpts)
	switch mode {
	case "open-write":
		require.NoError(t, err)
		want, perr := strconv.ParseUint(os.Getenv(openSizeEnvExpect), 10, 32)
		require.NoError(t, perr)
		assertOpensAtCommittedSize(t, db, uint32(want))
	case "checkpoint":
		require.NoError(t, err)
		require.NoError(t, db.Checkpoint(CheckpointFull))
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	if t.Failed() {
		return true
	}
	os.Exit(0)
	return true
}

func runOpenSizeChild(t *testing.T, testName, path, mode string, expect uint32) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(),
		openSizeEnvPath+"="+path, openSizeEnvMode+"="+mode,
		openSizeEnvExpect+"="+strconv.FormatUint(uint64(expect), 10))
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child (%s) failed:\n%s", mode, out)
}

// patchNewestPage1Frame overwrites 4 bytes of the page-1 header carried by
// the newest page-1 frame of the WAL file at walPath, then re-derives the
// checksum chain from the start so the WAL still recovers in full.
func patchNewestPage1Frame(t *testing.T, walPath string, pageSize uint32, off int, v uint32) {
	t.Helper()
	wal, err := os.ReadFile(walPath)
	require.NoError(t, err)
	frameSize := walFrameSize + int(pageSize)
	nFrames := (len(wal) - walHeaderSize) / frameSize
	newest := -1
	for i := 0; i < nFrames; i++ {
		fh := wal[walHeaderSize+i*frameSize:]
		if binary.BigEndian.Uint32(fh[0:4]) == 1 {
			newest = i
		}
	}
	require.GreaterOrEqual(t, newest, 0, "the WAL must hold a page-1 frame")
	payload := walHeaderSize + newest*frameSize + walFrameSize
	binary.BigEndian.PutUint32(wal[payload+off:], v)

	s1, s2 := walChecksum(wal[0:24], 0, 0)
	for i := 0; i < nFrames; i++ {
		fh := wal[walHeaderSize+i*frameSize:]
		s1, s2 = walChecksum(fh[0:8], s1, s2)
		s1, s2 = walChecksum(fh[walFrameSize:frameSize], s1, s2)
		binary.BigEndian.PutUint32(fh[16:20], s1)
		binary.BigEndian.PutUint32(fh[20:24], s2)
	}
	require.NoError(t, os.WriteFile(walPath, wal, 0o644))
}

// crashCopyOfShrunkDst returns the path of a copy of a database as its only
// process left it when it died: the DB file holds the large image, the WAL's
// last commit the small one. The copy has no shm.
func crashCopyOfShrunkDst(t *testing.T) (crashPath string, committed uint32) {
	t.Helper()
	_, dstPath, committed := shrinkDstInWAL(t, Options{PageSize: 4096, InProcess: true})
	crashPath = filepath.Join(t.TempDir(), "dst.db")
	// Copy from a child process: closing a descriptor of these files in
	// this process would drop its own locks on them.
	require.NoError(t, exec.Command("cp", dstPath, crashPath).Run())
	require.NoError(t, exec.Command("cp", dstPath+"-wal", crashPath+"-wal").Run())
	return crashPath, committed
}

// TestOpenAfterWALCommittedShrink: the WAL's last commit made the database
// smaller than the not-yet-checkpointed DB file. Open must use the WAL's
// committed size even though it is the smaller one.
func TestOpenAfterWALCommittedShrink(t *testing.T) {
	if openSizeChild(t) {
		return
	}
	const name = "TestOpenAfterWALCommittedShrink"

	// A second process attaches while the first is still open.
	t.Run("peer attached", func(t *testing.T) {
		_, dstPath, committed := shrinkDstInWAL(t, openSizeOpts)
		runOpenSizeChild(t, name, dstPath, "open-write", committed)
	})

	// The first process dies before any checkpoint: the next open recovers
	// the WAL.
	t.Run("after crash", func(t *testing.T) {
		crashPath, committed := crashCopyOfShrunkDst(t)
		db2, err := testOpen(t, crashPath, Options{PageSize: 4096, InProcess: true})
		require.NoError(t, err)
		defer db2.Close()
		assertOpensAtCommittedSize(t, db2, committed)
	})

	// The WAL's page 1 carries no size (offset 28 is zero). The size is then
	// the pager's count, which is the WAL commit's 2 pages: not page 1's 0,
	// and not the file's 452.
	t.Run("page-1 size zero uses the WAL commit size", func(t *testing.T) {
		crashPath, committed := crashCopyOfShrunkDst(t)
		patchNewestPage1Frame(t, crashPath+"-wal", 4096, 28, 0)
		db2, err := testOpen(t, crashPath, Options{PageSize: 4096, InProcess: true})
		require.NoError(t, err)
		defer db2.Close()
		require.Equal(t, committed, db2.DatabaseSize())
		require.Equal(t, committed, db2.pager.wal.index.maxPage.Load(), "setup: the patched WAL must recover in full")
	})

	// A trusted page-1 size above the WAL commit's size is corruption
	// (btree.c:3401-3408), even though the DB file is long enough for it.
	t.Run("page-1 size above the WAL commit size is corrupt", func(t *testing.T) {
		crashPath, committed := crashCopyOfShrunkDst(t)
		patchNewestPage1Frame(t, crashPath+"-wal", 4096, 28, committed+5)
		_, err := testOpen(t, crashPath, Options{PageSize: 4096, InProcess: true})
		require.ErrorIs(t, err, ErrHeaderCorrupt)
	})

	// A non-zero change-counter mismatch in the database file's page 1 does
	// not matter when the WAL holds a newer, consistent page 1: the rule is
	// applied to the page 1 the snapshot reads.
	t.Run("stale file page 1 superseded by the WAL", func(t *testing.T) {
		crashPath, committed := crashCopyOfShrunkDst(t)
		f, err := os.OpenFile(crashPath, os.O_RDWR, 0)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte{0x7f, 0xff, 0xff, 0xff}, 92)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		db2, err := testOpen(t, crashPath, Options{PageSize: 4096, InProcess: true})
		require.NoError(t, err)
		defer db2.Close()
		require.Equal(t, committed, db2.DatabaseSize())
	})
}

// TestOpenReadsPage1AndSizeAtOneSnapshot: between the opener's WAL attach
// and its page-1 read, a peer checkpoints the WAL (whose last commit shrank
// the database). The opener must still pair page 1 with the size of the same
// snapshot. Read apart, the pre-checkpoint file header (452 pages) meets the
// WAL's size (2) and a healthy database is refused as corrupt.
func TestOpenReadsPage1AndSizeAtOneSnapshot(t *testing.T) {
	if openSizeChild(t) {
		return
	}
	crashPath, committed := crashCopyOfShrunkDst(t)

	fired := 0
	testOpenAfterWALOpenHook = func(p *pager) {
		if p.path != crashPath {
			return
		}
		fired++
		// The peer: attach, checkpoint everything into the file, die.
		runOpenSizeChild(t, "TestOpenReadsPage1AndSizeAtOneSnapshot", crashPath, "checkpoint", 0)
	}
	t.Cleanup(func() { testOpenAfterWALOpenHook = nil })

	db, err := testOpen(t, crashPath, openSizeOpts)
	require.NoError(t, err, "a peer's checkpoint during open must not make the database look corrupt")
	defer db.Close()
	require.Equal(t, 1, fired)
	assertOpensAtCommittedSize(t, db, committed)
}
