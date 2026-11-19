//go:build (linux || darwin) && (amd64 || arm64)

package btree

// Two-process tests of an open's wait for the DB-file lock (re-exec pattern,
// helpers in multiprocess_create_test.go). The holder is a child stopped at
// a named point; the parent keeps it there for longer than a second, the
// whole wait an open used to allow, and well short of BusyTimeout.

import (
	"bufio"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// dbLockHold is how long a holder keeps the exclusive DB-file lock while the
// other process's open waits for it.
const dbLockHold = 2 * time.Second

// TestOpenWaitsForCreatorHoldingDBLock: a creator has written and synced
// page 1 and still holds the exclusive DB-file lock, for two seconds. An open
// of the same path from another process waits for it through the busy
// handler and then opens the creator's database.
func TestOpenWaitsForCreatorHoldingDBLock(t *testing.T) {
	if os.Getenv(createChildRoleEnv) == "slow-creator" {
		path := os.Getenv(createChildPathEnv)
		in := bufio.NewReader(os.Stdin)
		testOpenStageHook = func(_ *pager, stage string) {
			if stage == "after-page1-sync" {
				testOpenStageHook = nil
				childSay("STALLED")
				childWait(t, in, "GO")
			}
		}
		db, err := testOpen(t, path, createOpts(""))
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		createPutRow(t, db, "child")
		childSay("WROTE")
		childWait(t, in, "EXIT")
		require.NoError(t, db.Close())
		childSay("CLOSED")
		return
	}
	if os.Getenv(createChildRoleEnv) != "" {
		return
	}

	path := filepath.Join(t.TempDir(), "slow.db")
	c := startCreateChild(t, "slow-creator", path, "", "")
	c.expect("STALLED")

	errCh := make(chan error, 1)
	var db *DB
	start := time.Now()
	go func() {
		var err error
		db, err = testOpen(t, path, createOpts(""))
		errCh <- err
	}()
	time.Sleep(dbLockHold)
	c.send("GO")
	require.NoError(t, <-errCh, "an open waits for a creator through the busy handler")
	defer func() { _ = db.Close() }()
	t.Logf("open returned after %v", time.Since(start).Round(time.Millisecond))

	c.expect("WROTE")
	createRequireRow(t, db, "child")
	require.NoError(t, db.IntegrityCheck())
	c.send("EXIT")
	c.expect("CLOSED")
}

// TestOpenAgainstInProcessHolder: another process holds the file in
// in-process mode, which keeps the exclusive DB-file lock for the handle's
// life. An open in in-process mode is refused at once with
// ErrInProcessLocked: that is a refusal, not a wait. An open in
// multi-process mode cannot tell this holder from a peer inside its close;
// it waits out the busy handler and fails with an error that matches
// ErrBusy.
func TestOpenAgainstInProcessHolder(t *testing.T) {
	if os.Getenv(createChildRoleEnv) == "inprocess-holder" {
		path := os.Getenv(createChildPathEnv)
		in := bufio.NewReader(os.Stdin)
		opts := createOpts("")
		opts.InProcess = true
		db, err := testOpen(t, path, opts)
		require.NoError(t, err)
		defer func() { _ = db.Close() }()
		createPutRow(t, db, "child")
		childSay("READY")
		childWait(t, in, "EXIT")
		require.NoError(t, db.Close())
		childSay("CLOSED")
		return
	}
	if os.Getenv(createChildRoleEnv) != "" {
		return
	}

	path := filepath.Join(t.TempDir(), "held.db")
	c := startCreateChild(t, "inprocess-holder", path, "", "")
	c.expect("READY")

	t.Run("in-process open is refused at once", func(t *testing.T) {
		opts := createOpts("")
		opts.InProcess = true
		start := time.Now()
		db, err := testOpen(t, path, opts)
		elapsed := time.Since(start)
		if err == nil {
			_ = db.Close()
		}
		require.ErrorIs(t, err, ErrInProcessLocked)
		require.Less(t, elapsed, time.Second, "a refusal, not a wait")
		t.Logf("refused after %v", elapsed.Round(time.Microsecond))
	})

	t.Run("multi-process open is busy", func(t *testing.T) {
		start := time.Now()
		db, err := testOpen(t, path, createOpts(""))
		elapsed := time.Since(start)
		if err == nil {
			_ = db.Close()
		}
		require.ErrorIs(t, err, ErrBusy)
		require.Less(t, elapsed, BusyTimeout+5*time.Second)
		t.Logf("busy after %v", elapsed.Round(time.Millisecond))
	})

	c.send("EXIT")
	c.expect("CLOSED")

	db, err := testOpen(t, path, createOpts(""))
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	createRequireRow(t, db, "child")
}
