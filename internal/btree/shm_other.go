//go:build !((linux || darwin) && (amd64 || arm64))

package btree

import (
	"fmt"
	"sync"
)

// heapShm implements the shm interface using heap memory.
// This fallback supports only single-process access.
// Multi-process access requires platform-specific mmap implementation.
//
// Planned platform support:
//   - linux/arm64: mmap via syscall (same as amd64, different Flock_t layout)
//   - darwin/amd64, darwin/arm64: mmap via syscall (BSD fcntl semantics)
//   - windows/amd64: CreateFileMapping + MapViewOfFile + LockFileEx
//   - freebsd, openbsd, netbsd: mmap via syscall (BSD semantics)
type heapShm struct {
	mu      sync.Mutex
	regions [][]byte
	locks   [lockSlotCount]int // 0=unlocked, >0=shared count, -1=exclusive
}

// newPlatformShm creates a heap-backed shm (single-process fallback).
func newPlatformShm(_ string) (shm, error) {
	return &heapShm{
		regions: make([][]byte, 0, shmMaxRegions),
	}, nil
}

func (s *heapShm) region(index int, create bool) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index < len(s.regions) && s.regions[index] != nil {
		return s.regions[index], nil
	}

	if !create {
		return nil, fmt.Errorf("btree: shm region %d not available", index)
	}

	for len(s.regions) <= index {
		s.regions = append(s.regions, nil)
	}
	s.regions[index] = make([]byte, shmRegionSize)
	return s.regions[index], nil
}

func (s *heapShm) lock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.locks[slot]
	switch lockType {
	case lockShared:
		if current < 0 {
			return ErrBusy // exclusive lock held
		}
		s.locks[slot] = current + 1
	case lockExclusive:
		if current != 0 {
			return ErrBusy // any lock held
		}
		s.locks[slot] = -1
	}
	return nil
}

func (s *heapShm) unlock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	current := s.locks[slot]
	switch lockType {
	case lockShared:
		if current > 0 {
			s.locks[slot] = current - 1
		}
	case lockExclusive:
		if current == -1 {
			s.locks[slot] = 0
		}
	}
	return nil
}

func (s *heapShm) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.regions = nil
	return nil
}
