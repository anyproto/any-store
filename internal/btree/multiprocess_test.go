//go:build (linux || darwin) && (amd64 || arm64)

package btree

// This test verifies that the multi-process WAL write safety fix works:
// BUSY_SNAPSHOT check + WAL state re-sync ensures that writes from multiple
// OS processes all survive correctly.

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMultiProcessWALCorruption verifies that when two OS processes open
// the same database file (InProcess=false), writes from both processes
// are correctly preserved thanks to the BUSY_SNAPSHOT check and WAL state
// re-sync in beginWrite.
//
// Timeline:
//  1. Process A opens DB, writes "parent-1", commits  → WAL frames 1..N
//  2. Process B opens DB, writes "child-1/2", commits → WAL frames N+1..M
//  3. Process A writes "parent-2", commits — beginWrite re-syncs WAL state
//     from SHM so frames chain correctly after M
//  4. On reopen + WAL recovery, all data survives
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
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("parent-1"), []byte("value-parent-1")))
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	t.Logf("parent after first commit: nFrame=%d", db.pager.wal.nFrame.Load())

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

	// Step 3: Parent writes after child committed.
	// With the BUSY_SNAPSHOT fix, beginWrite re-syncs WAL state from SHM,
	// so the parent correctly chains onto the child's frames.
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

	// Step 4: Reopen and verify ALL data survives
	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db2.Close()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	ns3, err := rtx.GetNamespace("test")
	require.NoError(t, err)

	// All 4 keys must survive
	val, err := rtx.Get(ns3, []byte("parent-1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-parent-1"), val)

	val, err = rtx.Get(ns3, []byte("child-1"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-child-1"), val)

	val, err = rtx.Get(ns3, []byte("child-2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-child-2"), val)

	val, err = rtx.Get(ns3, []byte("parent-2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value-parent-2"), val)

	t.Logf("all data survived: parent-1, child-1, child-2, parent-2")
}

func multiProcessChild(t *testing.T, dbPath string) {
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             false,
		DisableAutoCheckpoint: true,
	}

	db, err := testOpen(t, dbPath, opts)
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
