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
type mmapShm struct {
	mu      sync.Mutex
	file    File
	path    string
	regions [][]byte // mmap'd regions
}

// newPlatformShm creates a new mmap-backed shm.
// Acquires a shared DMS lock to track that this connection is using the SHM file.
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

	// Acquire a shared DMS lock. All open connections hold this lock.
	// On close, we try to upgrade to exclusive: if successful, we're the last
	// connection and can safely delete the SHM file.
	if err := s.fcntlLock(syscall.F_RDLCK, shmDMSOffset); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("btree: acquire DMS lock: %w", err)
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

// lock acquires a POSIX advisory lock on the shm file for the given slot.
// Each slot corresponds to a 1-byte range in the file (matching SQLite's approach).
// The lock range starts at byte offset 120 (after the WAL index header area).
func (s *mmapShm) lock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}

	lt := syscall.F_RDLCK
	if lockType == lockExclusive {
		lt = syscall.F_WRLCK
	}

	return s.fcntlLock(lt, shmLockOffset(slot))
}

// unlock releases the POSIX advisory lock on the given slot.
func (s *mmapShm) unlock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}
	_ = lockType // unlock is the same regardless of type
	return s.fcntlLock(syscall.F_UNLCK, shmLockOffset(slot))
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

func (s *mmapShm) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, region := range s.regions {
		if region != nil {
			_ = sysMunmap(region)
			s.regions[i] = nil
		}
	}

	// Determine if we're the last connection by trying to acquire an exclusive
	// DMS lock. If successful, no other process holds the SHM file open, so we
	// can safely delete it. This matches SQLite's unixShmUnmap() behavior.
	//
	// We must do this BEFORE closing the file descriptor, since closing the fd
	// releases all our fcntl locks.
	deleteFile := false
	if s.file != nil {
		// Try to upgrade our shared DMS lock to exclusive.
		if err := s.fcntlLock(syscall.F_WRLCK, shmDMSOffset); err == nil {
			deleteFile = true
		}
	}

	var err error
	if s.file != nil {
		err = s.file.Close()
		s.file = nil
	}

	if deleteFile {
		_ = osRemove(s.path)
	}
	return err
}
