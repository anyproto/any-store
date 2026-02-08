package btree

// shm defines the interface for shared memory used by the WAL index.
// The WAL index (equivalent to SQLite's .shm file) tracks which pages
// are stored in which WAL frames, enabling snapshot isolation for readers.
//
// Platform-specific implementations:
//   - Linux amd64: mmap-backed .shm file for multi-process access
//   - Other platforms: heap-backed fallback (single-process only)
//
// The shm is divided into fixed-size regions. Region 0 contains the
// WAL index header. Subsequent regions contain hash tables mapping
// page numbers to WAL frame positions.

import (
	"fmt"
	"sync"
)

const (
	// shmRegionSize is the size of each shared memory region (32 KB, matching SQLite).
	shmRegionSize = 32768

	// shmHeaderOffset is the byte offset of the WAL index header in region 0.
	shmHeaderOffset = 0

	// shmHeaderSize is the size of the WAL index header (two copies for atomicity).
	shmHeaderSize = 136

	// shmMaxRegions is the maximum number of shm regions.
	shmMaxRegions = 32
)

// newHeapShm creates a heap-backed shm that uses only in-process locking.
// This is faster than mmap+fcntl but only supports single-process access.
func newHeapShm() shm {
	return &inProcessShm{
		regions: make([][]byte, 0, shmMaxRegions),
	}
}

// inProcessShm implements shm using heap memory with in-process locks only.
// No file I/O or syscalls are needed, making it much faster for single-process use.
type inProcessShm struct {
	mu      [lockSlotCount]shmLock
	regions [][]byte
	regMu   sync.Mutex
}

type shmLock struct {
	mu    sync.Mutex
	state int // 0=unlocked, >0=shared count, -1=exclusive
}

func (s *inProcessShm) region(index int, create bool) ([]byte, error) {
	s.regMu.Lock()
	defer s.regMu.Unlock()

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

func (s *inProcessShm) lock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}
	l := &s.mu[slot]
	l.mu.Lock()
	defer l.mu.Unlock()
	switch lockType {
	case lockShared:
		if l.state < 0 {
			return ErrBusy
		}
		l.state++
	case lockExclusive:
		if l.state != 0 {
			return ErrBusy
		}
		l.state = -1
	}
	return nil
}

func (s *inProcessShm) unlock(slot int, lockType int) error {
	if slot < 0 || slot >= lockSlotCount {
		return fmt.Errorf("btree: invalid lock slot %d", slot)
	}
	l := &s.mu[slot]
	l.mu.Lock()
	defer l.mu.Unlock()
	switch lockType {
	case lockShared:
		if l.state > 0 {
			l.state--
		}
	case lockExclusive:
		if l.state == -1 {
			l.state = 0
		}
	}
	return nil
}

func (s *inProcessShm) close() error {
	s.regMu.Lock()
	s.regions = nil
	s.regMu.Unlock()
	return nil
}

// shm is the interface for shared memory access.
// Implementations must be safe for concurrent use within one process.
type shm interface {
	// region returns a byte slice for the given region index.
	// If the region doesn't exist and create is true, it is allocated.
	// The returned slice is valid until close is called.
	region(index int, create bool) ([]byte, error)

	// lock acquires a lock on the given lock slot.
	// lockType: lockShared or lockExclusive.
	lock(slot int, lockType int) error

	// unlock releases the lock on the given lock slot.
	unlock(slot int, lockType int) error

	// close releases all resources.
	close() error
}

// Lock types for shm.lock/unlock.
const (
	lockShared    = 0
	lockExclusive = 1
)

// Lock slot indices (matching SQLite's WAL lock slots).
const (
	// lockWrite is held by the writer during write transactions.
	lockWrite = 0

	// lockCheckpoint is held during checkpoint.
	lockCheckpoint = 1

	// lockRecover is held during WAL recovery.
	lockRecover = 2

	// lockRead0 through lockRead4 are the reader lock slots (5 total).
	// Each reader acquires a shared lock on one of these slots.
	lockRead0 = 3
	lockRead1 = 4
	lockRead2 = 5
	lockRead3 = 6
	lockRead4 = 7

	// lockSlotCount is the total number of lock slots.
	lockSlotCount = 8
)
