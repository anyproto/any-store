//go:build (linux || darwin) && (amd64 || arm64)

package btree

// The tests of the mmap shared-memory index (shm_mmap.go), which exists on
// the targets of its build constraint only.

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMmapShm_FcntlLockError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the underlying file to force fcntl to fail
	savedFile := ms.file
	_ = savedFile.Close()
	ms.file = savedFile // fd is now invalid

	// lock should fail with bad fd — fcntl called because in-process counter is 0
	err = ms.lock(0, lockShared)
	assert.Error(t, err)

	// unlock is a no-op: the lock above failed so the in-process counter
	// was never incremented. No fcntl call is made, so no error.
	err = ms.unlock(0, lockShared)
	assert.NoError(t, err)

	// Verify fcntl error propagates through unlock: manually set the
	// counter to simulate a held lock, then unlock triggers fcntl on bad fd.
	ms.locks[0] = 1
	err = ms.unlock(0, lockShared)
	assert.Error(t, err)

	ms.file = nil // prevent close() from panicking
}

func TestMmapShm_FcntlLock_NonEACCES(t *testing.T) {
	// Test fcntlLock returning a non-EACCES/EAGAIN error (line 144-146)
	// by closing the file and using the invalid fd
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)
	// Close the file to make the fd invalid
	origFile := ms.file
	_ = origFile.Close()
	ms.file = origFile // fd is now closed

	// This should get EBADF (not EACCES or EAGAIN) and hit line 147
	err = ms.fcntlLock(0, 0) // F_RDLCK = 0, offset 0
	assert.Error(t, err)
	// The error should NOT be ErrBusy (since it's EBADF, not EAGAIN/EACCES)
	assert.NotErrorIs(t, err, ErrBusy)
	assert.Contains(t, err.Error(), "fcntl lock")

	ms.file = nil // prevent double close
}

func TestMmapShm_RegionStatError(t *testing.T) {
	// Test the stat error in region (line 74-76).
	// Close the underlying fd via syscall to make Stat return EBADF.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the file so region()'s Stat() fails. Use the *os.File's own Close
	// rather than syscall.Close on the raw fd: a raw-fd close leaves the
	// *os.File believing its fd is still open, so the GC finalizer later closes
	// that fd number — which may have been recycled to an unrelated file by
	// then, corrupting it (observed as spurious "bad file descriptor" failures
	// in later tests).
	require.NoError(t, ms.file.Close())

	// Stat() on the closed file now fails, exercising region()'s Stat-error path.
	_, err = ms.region(0, true)
	assert.Error(t, err)
}

func TestMmapShm_RegionMmapError(t *testing.T) {
	// Test the mmap error in region (line 87-89).
	// Replace the file with a read-only one so mmap with PROT_WRITE fails.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the original file and reopen as read-only
	origFile := ms.file
	_ = origFile.Close()

	// Reopen the file as read-only
	roFile, err := os.Open(path)
	require.NoError(t, err)
	ms.file = roFile

	// Extend the file so stat shows enough size (bypasses truncate)
	// We need info.Size() >= requiredSize to skip the Truncate call
	_ = os.WriteFile(path, make([]byte, shmRegionSize*2), 0666)
	// Reopen after writing
	_ = roFile.Close()
	roFile, err = os.Open(path)
	require.NoError(t, err)
	ms.file = roFile

	// mmap with PROT_WRITE on a read-only fd should fail
	_, err = ms.region(0, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mmap shm region")

	_ = roFile.Close()
	ms.file = nil
}

func TestMmapShm_RegionTruncateError(t *testing.T) {
	// Test the truncate error in region (line 78-80).
	// Use a read-only file that passes Stat but fails on Truncate.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the original file and reopen as read-only
	origFile := ms.file
	_ = origFile.Close()

	roFile, err := os.Open(path)
	require.NoError(t, err)
	ms.file = roFile

	// The file is 0 bytes, so info.Size() < requiredSize, and Truncate will be called.
	// Truncate on a read-only fd fails.
	_, err = ms.region(0, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extend shm file")

	_ = roFile.Close()
	ms.file = nil
}

// --- shm_mmap.go L144-146: fcntlLock non-EACCES/EAGAIN error ---
// Already tested by TestMmapShm_FcntlLock_NonEACCES.
func TestCov2_FcntlLock_OtherError(t *testing.T) {
	// Test that fcntlLock returns a formatted error for non-EACCES/EAGAIN errors.
	// We can trigger this by using an invalid file descriptor.
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")

	ms, err := newPlatformShm(shmPath)
	require.NoError(t, err)

	// Close the file to invalidate the fd
	s := ms.(*mmapShm)
	fd := s.file.Fd()
	_ = fd

	// Close the underlying file
	_ = s.file.Close()

	// Now try to lock - should fail with EBADF
	err = s.fcntlLock(syscall.F_RDLCK, 0)
	assert.Error(t, err)
	// The error should NOT be ErrBusy (it's EBADF, not EACCES/EAGAIN)
	assert.NotErrorIs(t, err, ErrBusy)

	s.file = nil
}

// TestP3_1_ExistingShmNotTruncated verifies the 3-byte truncate does NOT
// fire when a second opener attaches to a shm that live peers maintain.
//
// Since the DMS first-attacher election (see newPlatformShm), "live peers"
// is decided by the F_GETLK probe of the DMS byte, not by file size. A real
// second PROCESS would observe opener A's shared DMS lock; POSIX record
// locks never conflict within one process, so simulating that here requires
// stubbing the probe to report F_RDLCK (see stubDMSFcntl). A same-process
// second attach without the stub would win the election and truncate — which
// is exactly why openDBs forbids same-process double-open by file identity.
func TestP3_1_ExistingShmNotTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "populated.shm")

	sA, err := newPlatformShm(path)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	defer sA.close(false)
	if _, err := sA.region(0, true); err != nil {
		t.Fatalf("region: %v", err)
	}
	grownInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after grow: %v", err)
	}
	if grownInfo.Size() <= 3 {
		t.Fatalf("after region(0, true), size should exceed 3, got %d", grownInfo.Size())
	}

	// Opener B "from another process": the DMS probe reports A's shared lock.
	stubDMSFcntl(t, func(real func(uintptr, int, *syscall.Flock_t) syscall.Errno, fd uintptr, cmd int, flock *syscall.Flock_t) (syscall.Errno, bool) {
		if cmd == syscall.F_GETLK {
			flock.Type = syscall.F_RDLCK
			return 0, true
		}
		return 0, false
	})
	sB, err := newPlatformShm(path)
	if err != nil {
		t.Fatalf("open B: %v", err)
	}
	defer sB.close(false)
	sizeB, _ := os.Stat(path)
	if sizeB.Size() != grownInfo.Size() {
		t.Fatalf("opener B changed shm size %d → %d; should leave it alone", grownInfo.Size(), sizeB.Size())
	}
}

// --- shm_mmap.go L144-146: fcntlLock non-EACCES/EAGAIN error ---
// Trigger by using an invalid file descriptor (close the file first).
func TestGap_ShmMmap_FcntlLock_BadFd(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")

	ms, err := newPlatformShm(shmPath)
	require.NoError(t, err)

	s := ms.(*mmapShm)
	// Close the underlying file to invalidate the fd
	_ = s.file.Close()

	// Now try to lock - should fail with EBADF (not EACCES/EAGAIN)
	err = s.fcntlLock(0x00, 0) // F_RDLCK = 0 on Linux
	assert.Error(t, err)
	assert.NotErrorIs(t, err, ErrBusy) // L144-146: non-EACCES/EAGAIN maps to generic error

	// Prevent double-close in cleanup
	s.file = nil
}
