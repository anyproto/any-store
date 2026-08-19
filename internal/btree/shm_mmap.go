//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

var (
	sysMmap   = syscall.Mmap
	sysMunmap = syscall.Munmap
	// sysFcntl performs an fcntl record-lock call (F_SETLK / F_GETLK). The
	// typed *Flock_t parameter lets tests stub lock conflicts and probe
	// responses (impossible with real fcntl intra-process, since POSIX locks
	// never conflict within one process); for F_GETLK the kernel — or a stub
	// — rewrites flock.Type with the answer.
	sysFcntl = func(fd uintptr, cmd int, flock *syscall.Flock_t) syscall.Errno {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, uintptr(cmd), uintptr(unsafe.Pointer(flock)))
		return errno
	}
)

// hasMmapShm indicates this platform supports mmap-based shared memory
// for multi-process WAL coordination. When false, only heap SHM is
// available and InProcess mode is forced.
const hasMmapShm = true

// shmDMSOffset is the byte offset of the "dead man switch" (DMS) lock in the
// SHM file, matching SQLite's UNIX_SHM_DMS = UNIX_SHM_BASE+SQLITE_SHM_NLOCK =
// 128 (os_unix.c:4602-4603, 3.52.0). Each attached process holds a SHARED
// fcntl lock on this byte for the lifetime of the attachment (released when
// the shm fd closes, including on process death); the first attacher briefly
// holds it EXCLUSIVE during the open-time election that resets a stale shm
// (see newPlatformShm). Last-client detection at close does NOT use the DMS —
// it moved to the DB-file flock upgrade (pager.close); the DMS's remaining
// job is the first-attacher election.
//
// Byte 128 overlaps the nBackfillAttempted field of the mmap'd wal-index
// data (htNBackfillAttemptedOff, wal.go) — SQLite has the identical overlap.
// Advisory fcntl locks never touch file bytes, so this is inert.
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
	mu       sync.Mutex
	file     fileHandle
	path     string
	regions  [][]byte           // mmap'd regions (each exactly shmRegionSize bytes)
	baseMaps [][]byte           // underlying mmap'd backing maps, keyed by group-base region index; entries are nil except at group bases. close() munmaps these (one per real mmap), never the sub-region slices in regions.
	locks    [lockSlotCount]int // in-process lock state: 0=unlocked, >0=shared count, -1=exclusive
}

// shmRegionsPerMap mirrors SQLite's unixShmRegionPerMap() (os_unix.c:4797-4803).
// Each mmap must cover an integer multiple of the OS page size. When the page
// size is >= shmRegionSize (e.g. 64KB pages), a single mapping must span
// multiple shm regions so that every mmap offset stays page-aligned.
// On 4KB/16KB-page systems this returns 1, keeping behavior byte-identical.
func shmRegionsPerMap() int {
	pg := os.Getpagesize()
	if pg < shmRegionSize {
		return 1
	}
	return pg / shmRegionSize
}

// newPlatformShm creates or attaches the mmap-backed shm and runs SQLite's
// dead-man-switch (DMS) first-attacher election, mirroring unixLockSharedMemory
// (os_unix.c:4860-4913, 3.52.0):
//
//   - Probe the DMS byte with F_GETLK (os_unix.c:4876). Three-way branch:
//   - F_UNLCK (no holder): take the DMS EXCLUSIVE non-blocking
//     (os_unix.c:4893). Success proves no other process is attached, so any
//     existing shm content is cross-crash garbage — a *-shm that survived
//     power loss reflects the dead writer's in-memory view, not the on-disk
//     WAL, and adopting it can resurrect uncommitted frames or hide the
//     committed tail. UNCONDITIONALLY reset it to the 3-byte stump
//     (robust_ftruncate(hShm, 3), os_unix.c:4903), then downgrade to SHARED
//     (os_unix.c:4910-4913). POSIX F_SETLK conversion of a held lock is
//     atomic — there is no unlock window in which a raced peer could attach
//     between the truncate and the downgrade.
//   - F_RDLCK (live attachers): join with a SHARED lock and do not touch
//     the file — a live process is maintaining the shm.
//   - F_WRLCK (peer mid-election): back off and retry the whole probe.
//     NEVER fall back to the shared lock here: SQLite documents
//     (os_unix.c:4864-4871) that an earlier version did exactly that, and
//     if the exclusive holder dies just before truncating, the joiner
//     attaches an untruncated — possibly crash-corrupted — shm.
//
// The 3-byte stump is deliberately smaller than walIndexHdrSize (48); the
// first region() call re-grows the file with zero-fill, so readHeader's
// dual-copy compare (and the isInit/aCksum checks behind it) can never
// validate stale content, and recoverLocked rebuilds the wal-index from the
// authoritative WAL file.
//
// Retry policy: bounded 100×10ms in-open loop, mirroring pager.open's
// DB-flock retry; exhaustion wraps ErrBusy. (C surfaces SQLITE_BUSY to the
// caller instead — DRIFT below.) The exclusive window is microseconds
// (WRLCK → ftruncate → downgrade), so a single retry normally suffices.
//
// Intra-process safety: POSIX fcntl record locks are per-(process, inode). A
// second attachment in the SAME process would not conflict with our locks —
// its F_WRLCK would silently convert our F_RDLCK and truncate a LIVE shm.
// That cannot happen because openDBs (db.go) forbids same-process double-open
// by file identity — the analog of SQLite's per-inode unixShmNode singleton.
// For the same reason, no code may open+close this *-shm path in a process
// that has it attached: closing ANY fd for the inode drops all of the
// process's record locks on it (the shm slot locks and the DMS byte).
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
//
// The election also excludes the mmap/ftruncate SIGBUS hazard: the truncate
// runs only when zero other processes hold the DMS, every attacher takes the
// DMS before its first region() mapping, and close() munmaps before closing
// the fd that holds the lock — so no live mapping can exist over a page the
// truncate discards.
//
// DRIFT: DMS BUSY retried in-line (100x10ms) instead of surfacing to the caller See docs/btree/NOTES.md#drift-2026-07-10-2-dms-busy-retried-in-open-instead-of-surfacing
func newPlatformShm(path string) (shm, error) {
	f, err := osOpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("btree: open shm file: %w", err)
	}
	s := &mmapShm{
		file:     f,
		path:     path,
		regions:  make([][]byte, 0, shmMaxRegions),
		baseMaps: make([][]byte, 0, shmMaxRegions),
	}

	for attempt := 0; ; attempt++ {
		typ, perr := s.fcntlGetLock(shmDMSOffset)
		if perr != nil {
			_ = f.Close()
			return nil, fmt.Errorf("btree: probe DMS lock: %w", perr)
		}
		switch typ {
		case syscall.F_UNLCK:
			// No holder: run the election.
			werr := s.fcntlLock(syscall.F_WRLCK, shmDMSOffset)
			if werr != nil {
				if errors.Is(werr, ErrBusy) {
					break // lost the race to another opener → re-probe
				}
				_ = f.Close()
				return nil, fmt.Errorf("btree: acquire DMS exclusive lock: %w", werr)
			}
			// First attacher: reset the (possibly crash-stale) shm.
			if terr := f.Truncate(3); terr != nil {
				_ = f.Close()
				return nil, fmt.Errorf("btree: ftruncate(shm, 3) first-attacher reset: %w", terr)
			}
			// Atomic downgrade to the lifetime SHARED lock. A conversion of
			// a lock we hold cannot legitimately be denied — treat even
			// ErrBusy as a hard protocol error, not contention.
			if derr := s.fcntlLock(syscall.F_RDLCK, shmDMSOffset); derr != nil {
				_ = f.Close()
				return nil, fmt.Errorf("btree: downgrade DMS lock to shared: %w", derr)
			}
			return s, nil

		case syscall.F_RDLCK:
			// Live attachers: join shared; the shm is live-maintained.
			rerr := s.fcntlLock(syscall.F_RDLCK, shmDMSOffset)
			if rerr == nil {
				return s, nil
			}
			if !errors.Is(rerr, ErrBusy) {
				_ = f.Close()
				return nil, fmt.Errorf("btree: acquire DMS shared lock: %w", rerr)
			}
			// Holder changed between probe and lock (readers left, a new
			// opener elected itself) → re-probe.

		case syscall.F_WRLCK:
			// Peer mid-election: back off, never join (os_unix.c:4864-4871).
		}

		if attempt >= 99 {
			_ = f.Close()
			return nil, fmt.Errorf("btree: DMS lock busy after retries (peer mid-election?): %w", ErrBusy)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fcntlGetLock probes the 1-byte range at offset with F_GETLK, asking whether
// an exclusive lock could be placed there (l_type=F_WRLCK query, matching
// unixLockSharedMemory's probe, os_unix.c:4872-4877). The kernel rewrites
// l_type to the answer: F_UNLCK (no conflicting holder), or the type of the
// conflicting lock held by another process (F_RDLCK / F_WRLCK). Locks held by
// THIS process never conflict and report F_UNLCK — see the intra-process
// invariant on newPlatformShm.
func (s *mmapShm) fcntlGetLock(offset int64) (int16, error) {
	if s.file == nil {
		return 0, fmt.Errorf("btree: shm file closed")
	}
	flock := syscall.Flock_t{
		Type:   int16(syscall.F_WRLCK),
		Whence: 0, // SEEK_SET
		Start:  offset,
		Len:    1,
	}
	if errno := sysFcntl(s.file.Fd(), syscall.F_GETLK, &flock); errno != 0 {
		return 0, fmt.Errorf("btree: fcntl getlk: %w", errno)
	}
	return flock.Type, nil
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

	// On systems whose page size is >= shmRegionSize, a single mmap must span
	// multiple shm regions to keep the mmap offset page-aligned. Map the whole
	// page-aligned group containing `index` at once and slice it into the
	// per-region 32KB windows, mirroring SQLite's unixShmRegionPerMap grouping
	// (os_unix.c:5202-5224). On 4KB/16KB pages regionsPerMap==1, so this is a
	// single 32KB mapping at offset index*32KB (unchanged behavior).
	regionsPerMap := shmRegionsPerMap()
	base := (index / regionsPerMap) * regionsPerMap
	nMap := regionsPerMap * shmRegionSize
	offset := int64(base) * int64(shmRegionSize) // always a multiple of os.Getpagesize()

	// Extend the file to back the whole group before mapping.
	requiredSize := int64(base+regionsPerMap) * int64(shmRegionSize)
	info, err := s.file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < requiredSize {
		oldSize := info.Size()
		if err := s.file.Truncate(requiredSize); err != nil {
			return nil, fmt.Errorf("btree: extend shm file: %w", err)
		}
		// Pre-touch each newly allocated page by writing a single byte to its
		// last byte. Technically only the final page must be written to grow the
		// file, but writing to all new pages forces the OS to allocate them
		// immediately, which reduces the chances of SIGBUS while accessing the
		// mapped region later on. Mirrors SQLite's unixShmMap bExtend loop
		// (os_unix.c:5180-5187), using a fixed 4096 page size (shmPageSize) to
		// match C's `static const int pgsz = 4096;`. requiredSize is a multiple
		// of shmRegionSize (32768), hence of 4096, so the loop covers full pages
		// (matches C assert `(nByte % pgsz)==0`, os_unix.c:5179).
		var oneByte [1]byte
		for iPg := oldSize / shmPageSize; iPg < requiredSize/shmPageSize; iPg++ {
			if _, err := s.file.WriteAt(oneByte[:], iPg*shmPageSize+shmPageSize-1); err != nil {
				return nil, fmt.Errorf("btree: pre-touch shm page: %w", err)
			}
		}
	}

	// Map the whole group once.
	data, err := sysMmap(int(s.file.Fd()), offset, nMap,
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("btree: mmap shm region %d: %w", index, err)
	}

	// Grow the slices to cover the whole group.
	for len(s.regions) <= base+regionsPerMap-1 {
		s.regions = append(s.regions, nil)
		s.baseMaps = append(s.baseMaps, nil)
	}

	// Slice the single mapping into regionsPerMap per-region 32KB windows,
	// matching C's apRegion[nRegion+i] = &pMem[szRegion*i] (os_unix.c:5223-5224).
	// Record the full backing map at the group base so close() munmaps it once.
	s.baseMaps[base] = data
	for i := 0; i < regionsPerMap; i++ {
		s.regions[base+i] = data[i*shmRegionSize : (i+1)*shmRegionSize : (i+1)*shmRegionSize]
	}

	return s.regions[index], nil
}

// lock acquires a lock on the given slot. It first checks the in-process lock
// counters (for intra-process correctness), then acquires the fcntl lock (for
// inter-process coordination). If the in-process check fails, fcntl is not called.
//
// This matches SQLite's unixShmLock() (os_unix.c) which checks the
// unixInodeInfo.aLock[] counters before calling fcntl.
// DRIFT: in-process/mmap shm collapse per-conn masks to one refcount; repeat shared not no-op See docs/btree/NOTES.md#drift-81-in-process-shm-lock-collapses-per-connection-masks-to-single
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
	errno := sysFcntl(s.file.Fd(), syscall.F_SETLK, &flock)
	if errno != 0 {
		// Mirror sqliteErrorFromPosixError (os_unix.c:1029-1038): the documented
		// set of transient/NFS-retry errnos that SQLite collapses to SQLITE_BUSY
		// so the WAL busy-handler loop retries instead of aborting. EPERM is
		// intentionally excluded (SQLite maps it to SQLITE_PERM), and genuine
		// hard errors (EBADF/EFAULT) keep returning the wrapped error to surface
		// closed-fd bugs.
		switch errno {
		case syscall.EACCES, syscall.EAGAIN, syscall.ETIMEDOUT,
			syscall.EBUSY, syscall.EINTR, syscall.ENOLCK:
			return ErrBusy
		}
		return fmt.Errorf("btree: fcntl lock: %w", errno)
	}
	return nil
}

func (s *mmapShm) close(isLastClient bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// munmap each real backing mapping exactly once. Sub-region slices in
	// s.regions share a backing map (see region()); munmapping a sub-slice
	// rather than the full mapping would be incorrect, so we munmap the
	// group-base maps tracked in s.baseMaps instead.
	for i, m := range s.baseMaps {
		if m != nil {
			_ = sysMunmap(m)
			s.baseMaps[i] = nil
		}
	}
	for i := range s.regions {
		s.regions[i] = nil
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
