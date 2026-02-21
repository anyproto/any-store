//go:build !((linux || darwin) && (amd64 || arm64))

package btree

import (
	"fmt"
	"sync"
)

// hasMmapShm indicates this platform lacks mmap-based shared memory.
// Only heap SHM is available; InProcess mode is forced.
const hasMmapShm = false

// heapShm implements the shm interface using heap memory with simple locking.
// This fallback supports only single-process access.
// Multi-process access requires platform-specific mmap implementation.
type heapShm struct {
	shmRegions
	locks [lockSlotCount]int // 0=unlocked, >0=shared count, -1=exclusive
	lockMu sync.Mutex
}

// newPlatformShm creates a heap-backed shm (single-process fallback).
func newPlatformShm(_ string) (shm, error) {
	return &heapShm{shmRegions: newShmRegions()}, nil
}

func (s *heapShm) lock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}
	s.lockMu.Lock()
	defer s.lockMu.Unlock()

	current := s.locks[slot]
	switch lockType {
	case lockShared:
		if current < 0 {
			return ErrBusy
		}
		s.locks[slot] = current + 1
	case lockExclusive:
		if current != 0 {
			return ErrBusy
		}
		s.locks[slot] = -1
	}
	return nil
}

func (s *heapShm) unlock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}
	s.lockMu.Lock()
	defer s.lockMu.Unlock()

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
	s.shmRegions.close()
	return nil
}
