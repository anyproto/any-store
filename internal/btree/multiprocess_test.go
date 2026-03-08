//go:build (linux || darwin) && (amd64 || arm64)

package btree

// This test reproduces the multi-process WAL write safety gaps documented in
// NOTES.md ("Multi-Process WAL Write Safety -- Severity: Critical (latent)").
//
// The test PASSES when corruption IS reproduced (proving the gaps exist).
// If the gaps are fixed (e.g. by adding a BUSY_SNAPSHOT check), this test
// must be updated to verify the fix instead.

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiProcessWALCorruption demonstrates that when two OS processes open
// the same database file (InProcess=false), writes from a second process are
// silently lost because the first process's wal.nFrame and wal.cksum1/cksum2
// become stale.
//
// Timeline:
//  1. Process A opens DB, writes "parent-1", commits  → WAL frames 1..N
//  2. Process B opens DB, writes "child-1/2", commits → WAL frames N+1..M
//  3. Process A writes "parent-2", commits — its stale nFrame causes it to
//     overwrite frames starting at N+1, and stale checksums break the chain
//     for any remaining child frames M > N+K
//  4. On reopen + WAL recovery, child's data is lost
func TestMultiProcessWALCorruption(t *testing.T) {
	dbPath := os.Getenv("TEST_MULTIPROCESS_DB_PATH")
	if dbPath != "" {
		// === CHILD PROCESS ===
		multiProcessChild(t, dbPath)
		return
	}

	// === PARENT PROCESS ===
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}

	// Step 1: Open DB, write initial data
	db, err := Open(path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("parent-1"), []byte("value-parent-1")))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	// Capture parent's WAL state before child writes
	parentNFrame := db.pager.wal.nFrame.Load()
	parentCksum1 := db.pager.wal.cksum1
	parentCksum2 := db.pager.wal.cksum2
	t.Logf("parent after first commit: nFrame=%d cksum1=%08x cksum2=%08x",
		parentNFrame, parentCksum1, parentCksum2)

	// Step 2: Spawn child process to write to the same DB
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestMultiProcessWALCorruption$",
		"-test.v",
		"-test.timeout=30s",
	)
	cmd.Env = append(os.Environ(), "TEST_MULTIPROCESS_DB_PATH="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	require.NoError(t, err, "child process failed")

	// Step 3: Verify parent's WAL state is stale
	assert.Equal(t, parentNFrame, db.pager.wal.nFrame.Load(),
		"parent nFrame should be stale (unchanged by child process)")
	assert.Equal(t, parentCksum1, db.pager.wal.cksum1,
		"parent cksum1 should be stale")
	assert.Equal(t, parentCksum2, db.pager.wal.cksum2,
		"parent cksum2 should be stale")

	// The child's close() checkpoints and truncates the WAL, writing frames
	// to the DB file. But the parent's in-memory state is completely unaware:
	// - writerCache still has stale pages from before the child's writes
	// - wal.nFrame, wal.cksum1/cksum2 reflect the parent's last commit only
	// - walIndex.mxCommitFrame is stale (process-local atomic)
	// - wal header/salts may have been reset by the child's checkpoint

	// Step 4: Parent writes with stale WAL state
	// The parent's beginRead() uses mxCommitFrame.Load() which is process-local
	// and stale. It doesn't see the child's committed data. Then writeFrames
	// uses stale nFrame for offset and stale checksums for the chain.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := tx2.GetNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx2.Put(ns2, []byte("parent-2"), []byte("value-parent-2")))
	tx2.MarkDataChanged()
	require.NoError(t, tx2.Commit())

	t.Logf("parent after second commit: nFrame=%d cksum1=%08x cksum2=%08x",
		db.pager.wal.nFrame.Load(), db.pager.wal.cksum1, db.pager.wal.cksum2)

	require.NoError(t, db.Close())

	// Step 5: Reopen and verify corruption — child's data should be lost
	// The child wrote to the DB file via checkpoint, but the parent's second
	// write either:
	//   a) Wrote WAL frames with stale salts (if child reset the WAL), making
	//      the parent's frames invalid on recovery
	//   b) Wrote WAL frames at wrong offsets (if WAL wasn't truncated), and
	//      the parent's commit overwrites the child's DB file changes on the
	//      next checkpoint
	// Either way, the child's data is lost.
	db2, err := Open(path, opts)
	require.NoError(t, err)
	defer db2.Close()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	ns3, err := rtx.GetNamespace("test")
	require.NoError(t, err)

	// Parent's first write should survive (checkpointed by child before its writes)
	val, err := rtx.Get(ns3, []byte("parent-1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-parent-1"), val)

	// Check what survived
	_, childErr1 := rtx.Get(ns3, []byte("child-1"))
	_, childErr2 := rtx.Get(ns3, []byte("child-2"))
	parentVal2, parentErr2 := rtx.Get(ns3, []byte("parent-2"))

	childDataLost := childErr1 != nil || childErr2 != nil
	parentDataLost := parentErr2 != nil

	t.Logf("child-1: %v, child-2: %v, parent-2: %v", childErr1, childErr2, parentErr2)
	if parentErr2 == nil {
		t.Logf("parent-2 value: %s", parentVal2)
	}

	if childDataLost || parentDataLost {
		t.Logf("CORRUPTION REPRODUCED: data lost (child lost: %v, parent-2 lost: %v)",
			childDataLost, parentDataLost)
	} else {
		t.Fatal("BUG NOT REPRODUCED: all data survived — the multi-process WAL gaps may have been fixed. Update this test.")
	}
}

func multiProcessChild(t *testing.T, dbPath string) {
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}

	db, err := Open(dbPath, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)

	ns, err := tx.GetNamespace("test")
	require.NoError(t, err)

	require.NoError(t, tx.Put(ns, []byte("child-1"), []byte("value-child-1")))
	require.NoError(t, tx.Put(ns, []byte("child-2"), []byte("value-child-2")))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	// Verify child can read its own data before closing
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	val, err := rtx.Get(ns, []byte("child-1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-child-1"), val)
	require.NoError(t, rtx.Rollback())

	t.Logf("child: nFrame=%d after commit", db.pager.wal.nFrame.Load())
	require.NoError(t, db.Close())
}
