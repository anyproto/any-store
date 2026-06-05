//go:build unix

package btree

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func inProcessOpts() Options {
	opts := DefaultOptions()
	opts.InProcess = true
	return opts
}

// TestInProcess_CrossProcessLockRejected is the canonical regression: a
// file-backed in-process DB uses heap-backed (process-local) SHM and is NOT
// multi-process safe, so a second process opening the same file must be rejected
// with ErrInProcessLocked — not silently allowed to corrupt via a separate heap
// SHM. We simulate "another process" with an independent fd holding an exclusive
// flock (flock treats separate file descriptions as separate holders, even in
// the same process). Pre-fix, in-process mode skipped DB-file locking entirely,
// so this Open SUCCEEDED (the bug); with the fix it returns ErrInProcessLocked.
func TestInProcess_CrossProcessLockRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ip.db")

	// Materialize a real DB, then close it (releases its lock + openDBs entry).
	db, err := testOpen(t, path, inProcessOpts())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// "Another process" grabs the exclusive lock on the file.
	peer, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, syscall.Flock(int(peer.Fd()), syscall.LOCK_EX|syscall.LOCK_NB),
		"peer should acquire exclusive lock on the closed DB")

	// Opening in-process now must fail with the clear cross-process error.
	_, err = testOpen(t, path, inProcessOpts())
	require.ErrorIs(t, err, ErrInProcessLocked)

	// And it must NOT be misreported as the same-process error.
	require.NotErrorIs(t, err, ErrDatabaseOpen)
}

// TestInProcess_SameProcessDoubleOpenRejected: a second open of the same
// in-process DB within this process is rejected with the established
// ErrDatabaseOpen (the openDBs guard), distinct from the cross-process error.
func TestInProcess_SameProcessDoubleOpenRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ip.db")

	db1, err := testOpen(t, path, inProcessOpts())
	require.NoError(t, err)
	defer db1.Close()

	_, err = testOpen(t, path, inProcessOpts())
	require.ErrorIs(t, err, ErrDatabaseOpen)
	require.NotErrorIs(t, err, ErrInProcessLocked)
}

// TestInProcess_LockReleasedOnClose: the exclusive lock is held only for the
// pager's lifetime, so after Close a fresh open (or a peer) can acquire it.
func TestInProcess_LockReleasedOnClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ip.db")

	db1, err := testOpen(t, path, inProcessOpts())
	require.NoError(t, err)
	require.NoError(t, db1.Close())

	db2, err := testOpen(t, path, inProcessOpts())
	require.NoError(t, err, "reopen after close must succeed (lock released)")
	require.NoError(t, db2.Close())
}

// TestInProcess_PeerCanOpenAfterClose: once the in-process holder closes, a peer
// (another fd) can take the exclusive lock — proving the lock is not leaked.
func TestInProcess_PeerCanOpenAfterClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "ip.db")

	db, err := testOpen(t, path, inProcessOpts())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	peer, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, syscall.Flock(int(peer.Fd()), syscall.LOCK_EX|syscall.LOCK_NB),
		"peer must acquire the lock after the in-process holder closed")
}

// --- Mixed mode (in-process vs multi-process mmap) ---------------------------
//
// anystore never opens the same file in both modes (Config exposes no InProcess;
// mode is platform-derived: unix->mmap, Windows->in-process), so this is not a
// user-reachable scenario. But if it ever happened — an mmap-mode process and an
// in-process process on one file — it would be UNSAFE (one coordinates via the
// mmap'd -shm, the other via heap SHM, with no shared coordination). The DB-file
// locks must therefore be mutually exclusive: the second opener of either mode is
// rejected rather than allowed to dual-open. (Before the exclusive-lock fix the
// in-process side took no lock, so this dual-opened and corrupted.)

// TestInProcess_RejectedWhilePeerHoldsShared: a multi-process (mmap) opener holds
// a SHARED lock; an in-process open must be rejected with ErrInProcessLocked
// (its exclusive lock cannot coexist with the peer's shared lock).
func TestInProcess_RejectedWhilePeerHoldsShared(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.db")

	// Materialize a real DB, then close it.
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Peer holds SHARED, as a multi-process mmap-mode opener would.
	peer, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, syscall.Flock(int(peer.Fd()), syscall.LOCK_SH|syscall.LOCK_NB))

	_, err = testOpen(t, path, inProcessOpts())
	require.ErrorIs(t, err, ErrInProcessLocked,
		"in-process open must be rejected while an mmap-mode peer holds the shared lock")
}

// TestInProcess_MmapRejectedWhilePeerHoldsExclusive: an in-process opener holds an
// EXCLUSIVE lock; a multi-process (mmap) open must be rejected (no dual-open).
// The mmap path surfaces a generic busy-after-retries error rather than
// ErrInProcessLocked, so we assert only that it is rejected — the safety property
// is "no concurrent open across modes", not the specific message.
func TestInProcess_MmapRejectedWhilePeerHoldsExclusive(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mixed.db")

	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	// Peer holds EXCLUSIVE, as an in-process opener would.
	peer, err := os.OpenFile(path, os.O_RDWR, 0)
	require.NoError(t, err)
	defer peer.Close()
	require.NoError(t, syscall.Flock(int(peer.Fd()), syscall.LOCK_EX|syscall.LOCK_NB))

	got, err := testOpen(t, path, DefaultOptions()) // InProcess=false (mmap mode)
	require.Error(t, err, "mmap open must be rejected while an in-process peer holds the exclusive lock")
	require.Nil(t, got, "no DB handle must be returned on a rejected open")
}
