//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

var (
	sysMmap   = syscall.Mmap
	sysMunmap = syscall.Munmap
	sysFcntl  = func(fd, cmd, arg uintptr) (uintptr, uintptr, syscall.Errno) {
		return syscall.Syscall(syscall.SYS_FCNTL, fd, cmd, arg)
	}
)

// hasMmapShm indicates this platform supports mmap-based shared memory
// for multi-process WAL coordination. When false, only heap SHM is
// available and InProcess mode is forced.
const hasMmapShm = true

// shmDMSOffset is the byte offset of the "dead man switch" (DMS) lock in the
// SHM file. Each open connection holds a shared lock on this byte. On close,
// we try to acquire an exclusive lock: if successful, we're the last connection
// and can safely delete the SHM file. This matches SQLite's UNIX_SHM_DMS pattern.
const shmDMSOffset = 120 + int64(lockSlotCount) // right after the per-slot lock bytes

// mmapShm implements the shm interface using mmap on a .shm file.
// This enables multi-process access to the WAL index, matching SQLite's approach.
//
// In-process lock tracking: POSIX fcntl advisory locks are per-(process, inode),
// NOT per-file-descriptor or per-goroutine. Within a single process, a shared
// lock from one goroutine does not block an exclusive lock from another goroutine
// on the same file — fcntl silently "upgrades" the lock. This breaks checkpoint's
// reader-detection logic: the checkpoint acquires exclusive locks on reader slots
// to check if readers are active, but within the same process these locks always
// succeed regardless of active readers.
//
// To fix this, mmapShm maintains in-process lock counters (like SQLite's
// unixInodeInfo.aLock[] in os_unix.c) that track per-goroutine shared/exclusive
// state. The in-process layer provides correct intra-process lock semantics,
// while fcntl provides inter-process coordination.
type mmapShm struct {
	mu      sync.Mutex
	file    fileHandle
	path    string
	regions [][]byte // mmap'd regions
	locks   [lockSlotCount]int // in-process lock state: 0=unlocked, >0=shared count, -1=exclusive
}

// newPlatformShm creates a new mmap-backed shm and acquires a shared DMS
// fcntl lock (kept for intra-process lock-slot coordination via the fcntl
// layer). Single-shot — no retries, no inode verify.
//
// The pager.open caller holds a SHARED flock on the DB file before we are
// invoked. That flock is what serializes "last-client-unlink" vs. "new-
// opener-attach": any closer that could unlink our shm file must hold DB-file
// EXCLUSIVE, which means our pager's SHARED acquisition must have completed
// before that closer ever unlinked — so by the time we are here, the shm
// path→inode mapping is either fresh (closer already unlinked and we/peer
// created a new one) or stably attached. Either way the old inode-verify
// retry loop is obsolete — see docs/btree/NOTES.md §SHM open/
// close protocol drift.
func newPlatformShm(path string) (shm, error) {
	f, err := osOpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("btree: open shm file: %w", err)
	}
	s := &mmapShm{
		file:    f,
		path:    path,
		regions: make([][]byte, 0, shmMaxRegions),
	}
	if err := s.fcntlLock(syscall.F_RDLCK, shmDMSOffset); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("btree: acquire DMS shared lock: %w", err)
	}

	// If the file is freshly created (size == 0), truncate to 3 bytes as
	// a "known-fresh" diagnostic marker — matches SQLite's unixOpenSharedMemory
	// (os_unix.c:4902 robust_ftruncate(hShm, 3)). The marker size is
	// deliberately smaller than walIndexHdrSize (48), so any subsequent
	// opener that mmaps a 3-byte file knows it's a blank-slate shm and not
	// a corruption. The first region() call will Truncate to shmRegionSize,
	// so this 3-byte state is transient.
	if info, err := f.Stat(); err == nil && info.Size() == 0 {
		if err := f.Truncate(3); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("btree: ftruncate(shm, 3) marker: %w", err)
		}
	}

	return s, nil
}

func (s *mmapShm) region(index int, create bool) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Return existing region if already mapped.
	if index < len(s.regions) && s.regions[index] != nil {
		return s.regions[index], nil
	}

	if !create {
		return nil, fmt.Errorf("btree: shm region %d not available", index)
	}

	// Extend the file if needed.
	requiredSize := int64((index + 1) * shmRegionSize)
	info, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < requiredSize {
		if err := s.file.Truncate(requiredSize); err != nil {
			return nil, fmt.Errorf("btree: extend shm file: %w", err)
		}
	}

	// mmap the region.
	offset := int64(index) * int64(shmRegionSize)
	data, err := sysMmap(int(s.file.Fd()), offset, shmRegionSize,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("btree: mmap shm region %d: %w", index, err)
	}

	// Grow the regions slice if needed.
	for len(s.regions) <= index {
		s.regions = append(s.regions, nil)
	}
	s.regions[index] = data
	return data, nil
}

// lock acquires a lock on the given slot. It first checks the in-process lock
// counters (for intra-process correctness), then acquires the fcntl lock (for
// inter-process coordination). If the in-process check fails, fcntl is not called.
//
// This matches SQLite's unixShmLock() (os_unix.c) which checks the
// unixInodeInfo.aLock[] counters before calling fcntl.
func (s *mmapShm) lock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}

	s.mu.Lock()
	current := s.locks[slot]

	switch lockType {
	case lockShared:
		if current < 0 {
			// Exclusive lock held in-process — cannot acquire shared.
			s.mu.Unlock()
			return ErrBusy
		}
		// Only call fcntl when transitioning from unlocked to shared (first
		// shared holder). Subsequent same-process shared locks are tracked
		// purely in-process — fcntl already holds the shared lock from the
		// first acquisition. This matches SQLite's unixShmLock optimization:
		// "p->sharedMask |= mask" without calling fcntl when the OS lock is
		// already held.
		if current == 0 {
			if err := s.fcntlLock(syscall.F_RDLCK, shmLockOffset(slot)); err != nil {
				s.mu.Unlock()
				return err
			}
		}
		s.locks[slot] = current + 1

	case lockExclusive:
		if current != 0 {
			// Any lock held in-process — cannot acquire exclusive.
			s.mu.Unlock()
			return ErrBusy
		}
		if err := s.fcntlLock(syscall.F_WRLCK, shmLockOffset(slot)); err != nil {
			s.mu.Unlock()
			return err
		}
		s.locks[slot] = -1
	}

	s.mu.Unlock()
	return nil
}

// unlock releases the lock on the given slot. It updates the in-process lock
// counters and calls fcntl only when the last holder releases.
func (s *mmapShm) unlock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}

	s.mu.Lock()
	current := s.locks[slot]

	switch lockType {
	case lockShared:
		if current > 0 {
			// Only release fcntl lock when last shared holder releases.
			// Update the counter AFTER fcntl succeeds to keep in-process
			// state consistent on failure (matches SQLite's unixShmLock).
			if current == 1 {
				if err := s.fcntlLock(syscall.F_UNLCK, shmLockOffset(slot)); err != nil {
					s.mu.Unlock()
					return err
				}
			}
			s.locks[slot] = current - 1
		}
	case lockExclusive:
		if current == -1 {
			if err := s.fcntlLock(syscall.F_UNLCK, shmLockOffset(slot)); err != nil {
				s.mu.Unlock()
				return err
			}
			s.locks[slot] = 0
		}
	}

	s.mu.Unlock()
	return nil
}

// shmLockOffset returns the byte offset in the shm file for the given lock slot.
// Lock bytes start at offset 120 in the shm file (matching SQLite).
func shmLockOffset(slot int) int64 {
	return 120 + int64(slot)
}

// fcntlLock performs a POSIX fcntl lock operation on a 1-byte range.
func (s *mmapShm) fcntlLock(lockType int, offset int64) error {
	if s.file == nil {
		return fmt.Errorf("btree: shm file closed")
	}
	flock := syscall.Flock_t{
		Type:   int16(lockType),
		Whence: 0, // SEEK_SET
		Start:  offset,
		Len:    1,
	}
	// Use F_SETLK for non-blocking lock attempts.
	_, _, errno := sysFcntl(
		s.file.Fd(),
		uintptr(syscall.F_SETLK),
		uintptr(unsafe.Pointer(&flock)))
	if errno != 0 {
		if errno == syscall.EACCES || errno == syscall.EAGAIN {
			return ErrBusy
		}
		return fmt.Errorf("btree: fcntl lock: %w", errno)
	}
	return nil
}

func (s *mmapShm) close(isLastClient bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, region := range s.regions {
		if region != nil {
			_ = sysMunmap(region)
			s.regions[i] = nil
		}
	}

	// Unlink the shm file iff the caller (pager.close) tells us we are the
	// last client. The decision is made at the DB-file lock level — pager.close
	// upgraded its shared flock to exclusive, which guarantees no peer can be
	// mid-open against this DB.
	//
	// CRITICAL ordering: unlink BEFORE closing the fd. A new opener that got
	// as far as osOpenFile before our unlink will still reach its
	// acquireSharedDBLock call and BUSY-retry there, never touching a stale
	// inode. (We also still hold the DB-file exclusive flock until pager.close's
	// Close() returns, providing the outer serialization.)
	if isLastClient && s.file != nil {
		_ = osRemove(s.path)
	}

	var err error
	if s.file != nil {
		err = s.file.Close()
		s.file = nil
	}

	return err
}
