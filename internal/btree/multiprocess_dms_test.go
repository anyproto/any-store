//go:build (linux || darwin) && (amd64 || arm64)

package btree

// Real cross-process tests for the DMS first-attacher election in
// newPlatformShm (re-exec pattern, like multiprocess_test.go). The in-process
// shm_dms_test.go variants stub the fcntl probe; these run the genuine
// per-(process,inode) POSIX lock protocol across OS processes.

import (
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var dmsMPOpts = Options{
	PageSize:              4096,
	CacheSize:             100,
	InProcess:             false,
	DisableAutoCheckpoint: true,
}

// TestMultiProcessDMSLiveAttachDoesNotTruncate: a second process attaching to
// a LIVE database must lose the election (the parent holds the DMS shared),
// join as a non-first attacher, and leave the shm intact — an erroneous
// truncate would destroy the live wal-index under the parent's mapping.
//
// Timeline:
//  1. Parent opens the DB (holds DMS SHARED for the attachment's lifetime),
//     commits rows, keeps the DB open.
//  2. Child opens the same DB: its F_GETLK probe reports F_RDLCK → join
//     branch. It verifies the parent's rows (a truncated shm would have
//     wiped the wal-index the frames are resolved through), writes one row,
//     exits.
//  3. Parent verifies the shm was never reset (size still > 3), reads the
//     child's row, and commits again over the intact index.
func TestMultiProcessDMSLiveAttachDoesNotTruncate(t *testing.T) {
	if dbPath := os.Getenv("TEST_DMS_LIVE_DB_PATH"); dbPath != "" {
		// === CHILD ===
		db, err := testOpen(t, dbPath, dmsMPOpts)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		require.Equal(t, 20, countKeys(t, db, "t1"),
			"child must see the parent's committed rows through the live shm")

		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := wtx.GetNamespace("t1")
		require.NoError(t, err)
		require.NoError(t, wtx.Put(ns, []byte("child-row"), []byte("from-child")))
		wtx.MarkDataChanged()
		require.NoError(t, wtx.Commit())
		return
	}

	// === PARENT ===
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	shmPath := path + "-wal-shm"

	db, err := testOpen(t, path, dmsMPOpts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	dmsPutRange(t, db, "t1", 0, 20)

	info, err := os.Stat(shmPath)
	require.NoError(t, err)
	require.Greater(t, info.Size(), int64(3), "live shm must be grown past the stump")
	liveSize := info.Size()

	cmd := exec.Command(os.Args[0],
		"-test.run=^TestMultiProcessDMSLiveAttachDoesNotTruncate$",
		"-test.v",
		"-test.timeout=30s",
	)
	cmd.Env = append(os.Environ(), "TEST_DMS_LIVE_DB_PATH="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run(), "child process failed")

	info, err = os.Stat(shmPath)
	require.NoError(t, err)
	require.Equal(t, liveSize, info.Size(),
		"a live-attach must never truncate the shm (the parent held the DMS shared)")

	// The parent reads the child's row through the intact index and can
	// still commit on top of the child's frames.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	val, err := rtx.Get(ns, []byte("child-row"))
	require.NoError(t, err)
	require.Equal(t, []byte("from-child"), val)
	require.NoError(t, rtx.Rollback())

	dmsPutRange(t, db, "t1", 20, 25)
	require.Equal(t, 26, countKeys(t, db, "t1")) // 25 range keys + child-row
}

// TestMultiProcessDMSColdStartRace: two processes race a cold start against a
// crash-stale (garbage-scribbled) shm. Exactly the election's job: one child
// wins the DMS exclusive and resets the shm, the other observes the winner
// (mid-election F_WRLCK → back off/retry, or post-downgrade F_RDLCK → join);
// both must recover the same committed data from the WAL and be able to
// write. The parent then reopens and verifies everything.
func TestMultiProcessDMSColdStartRace(t *testing.T) {
	if dbPath := os.Getenv("TEST_DMS_COLD_DB_PATH"); dbPath != "" {
		// === CHILD ===
		marker := os.Getenv("TEST_DMS_COLD_MARKER")
		db, err := testOpen(t, dbPath, dmsMPOpts)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()

		// Verify every seed key individually — the sibling child races us and
		// may have already committed its own marker row, so a bare count is
		// not stable here.
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err := rtx.GetNamespace("t1")
		require.NoError(t, err)
		for i := 0; i < 30; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_, err := rtx.Get(ns, key)
			require.NoError(t, err,
				"child %s: seed key %d must be recovered from the WAL, not lost to the garbage shm", marker, i)
		}
		require.NoError(t, rtx.Rollback())

		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		wns, err := wtx.GetNamespace("t1")
		require.NoError(t, err)
		require.NoError(t, wtx.Put(wns, []byte("cold-child-"+marker), []byte(marker)))
		wtx.MarkDataChanged()
		require.NoError(t, wtx.Commit())
		return
	}

	// === PARENT ===
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	shmPath := path + "-wal-shm"

	db, err := testOpen(t, path, dmsMPOpts)
	require.NoError(t, err)
	dmsPutRange(t, db, "t1", 0, 30)
	rawClose(db)

	// Crash-stale image: garbage shm survives, WAL holds the truth.
	writeGarbageShm(t, shmPath)

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := exec.Command(os.Args[0],
				"-test.run=^TestMultiProcessDMSColdStartRace$",
				"-test.v",
				"-test.timeout=30s",
			)
			cmd.Env = append(os.Environ(),
				"TEST_DMS_COLD_DB_PATH="+path,
				fmt.Sprintf("TEST_DMS_COLD_MARKER=%d", i),
			)
			out, err := cmd.CombinedOutput()
			if err != nil {
				errs[i] = fmt.Errorf("cold-start child %d: %w\n%s", i, err, out)
			}
		}(i)
	}
	wg.Wait()
	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	// Reopen in the parent: 30 seed rows + one marker row per child.
	db2, err := testOpen(t, path, dmsMPOpts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	require.Equal(t, 32, countKeys(t, db2, "t1"))

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		val, err := rtx.Get(ns, []byte(fmt.Sprintf("cold-child-%d", i)))
		require.NoError(t, err, "child %d's write must survive", i)
		require.Equal(t, []byte(fmt.Sprintf("%d", i)), val)
	}
}
