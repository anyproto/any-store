//go:build linux && amd64

package btree

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

// mmapShm implements the shm interface using mmap on a .shm file.
// This enables multi-process access to the WAL index, matching SQLite's approach.
type mmapShm struct {
	mu      sync.Mutex
	file    *os.File
	path    string
	regions [][]byte // mmap'd regions
}

// newPlatformShm creates a new mmap-backed shm.
func newPlatformShm(path string) (shm, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("btree: open shm file: %w", err)
	}

	return &mmapShm{
		file:    f,
		path:    path,
		regions: make([][]byte, 0, shmMaxRegions),
	}, nil
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
	data, err := syscall.Mmap(int(s.file.Fd()), offset, shmRegionSize,
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
	flock := syscall.Flock_t{
		Type:   int16(lockType),
		Whence: 0, // SEEK_SET
		Start:  offset,
		Len:    1,
	}
	// Use F_SETLK for non-blocking lock attempts.
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL,
		uintptr(s.file.Fd()),
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
			_ = syscall.Munmap(region)
			s.regions[i] = nil
		}
	}

	var err error
	if s.file != nil {
		err = s.file.Close()
		s.file = nil
	}

	// Remove the shm file on close (like SQLite does).
	_ = os.Remove(s.path)
	return err
}
