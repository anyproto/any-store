//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

// errMmapNotRealFile signals that the active fileHandle is not the real OS
// file (e.g. a custom VFS File injected for fault injection), so there is no
// raw fd whose bytes are guaranteed to equal its ReadAt. Mmap is disabled and
// reads fall back to File.ReadAt — the analogue of SQLite leaving bUseFetch=0
// when the VFS lacks xFetch (pager.c:3536-3539 pagerFixMaplimit).
var errMmapNotRealFile = errors.New("btree: mmap unavailable: file is not a real OS file")

// dbMmap holds the optional mmap region for the database file. When
// enabled, reads of any offset within [0, curSize) memcpy from the
// mapping instead of calling pread. Writes go through WriteAt and are
// coherent via the OS unified page cache (linux/darwin).
//
// Modeled on SQLite's unixFile mmap fields (sqlitec/src/os_unix.c:272-275):
//   - maxSize   ~ mmapSizeMax   (configured cap; 0 disables)
//   - region    ~ pMapRegion    (the base pointer of the mapping)
//   - len(region) ~ mmapSize    (bytes currently addressable)
//
// Unlike SQLite, we do NOT expose a zero-copy fetch — callers copy bytes
// out. That skips SQLite's nFetchOut reference counting
// (os_unix.c:5750-5772 unixUnfetch) at the cost of not saving the memcpy.
// Measurement shows pread-syscall overhead dominates on large-blob reads,
// so the memcpy cost is secondary.
type dbMmap struct {
	mu      sync.RWMutex
	file    fileHandle
	region  []byte // mmap'd bytes; len() == curSize
	maxSize int64  // configured cap (Config.MmapSize)
}

// newDBMmap returns a disabled dbMmap when maxSize <= 0 (SQLite
// PRAGMA mmap_size = 0 equivalent). Callers invoke fetch() which
// returns (nil, false) when disabled, and fall back to ReadAt.
func newDBMmap(file fileHandle, maxSize int64) *dbMmap {
	return &dbMmap{file: file, maxSize: maxSize}
}

// readAt copies [off, off+len(dst)) from the mapping into dst, returning
// ok=true on success. The copy happens WHILE the read lock is held, so
// a concurrent remap()/unmap() cannot munmap the backing memory out from
// under us. Returns ok=false on: disabled, mapping not yet created, or
// offset outside current window — caller falls back to ReadAt.
//
// Mirrors SQLite's unixFetch (os_unix.c:5714-5738) but eliminates the
// nFetchOut reference counting (which SQLite needs for its zero-copy
// contract — pointer into the mapping returned to the caller): we
// memcpy inside the critical section so there is never an outstanding
// pointer into the mapping past the lock release.
func (m *dbMmap) readAt(dst []byte, off int64) (ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.maxSize == 0 || m.region == nil {
		return false
	}
	if off < 0 {
		return false
	}
	end := off + int64(len(dst))
	if end > int64(len(m.region)) {
		return false
	}
	copy(dst, m.region[off:end])
	return true
}

// remap resizes the mapping to cover at least [0, need) bytes, capped
// at maxSize. Called lazily on first fetch and after DB-file growth.
// Matches SQLite's unixRemapfile (os_unix.c:5570-5640+). No-op when
// disabled (maxSize == 0).
func (m *dbMmap) remap(need int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.maxSize == 0 {
		return nil
	}
	target := need
	if target > m.maxSize {
		target = m.maxSize
	}
	if target <= int64(len(m.region)) {
		// Already covered.
		return nil
	}
	// Unmap the old region if any (linux/darwin have mremap but Go's
	// syscall package doesn't expose it; unmap+remap is simpler).
	// SQLite's non-HAVE_MREMAP branch does the same: os_unix.c:5610-5625.
	if m.region != nil {
		if err := syscall.Munmap(m.region); err != nil {
			return fmt.Errorf("btree: munmap old db region: %w", err)
		}
		m.region = nil
	}
	fd, err := fdFromFile(m.file)
	if err != nil {
		if errors.Is(err, errMmapNotRealFile) {
			// The active file is a custom VFS File (no real raw fd whose
			// bytes equal its ReadAt). Permanently disable mmap for this
			// dbMmap so every read falls through to File.ReadAt. This is
			// the exact analogue of SQLite leaving bUseFetch=0 when the VFS
			// lacks xFetch (pager.c:3536-3539): all reads then go through
			// the injected File.ReadAt, restoring interception/fault
			// injection.
			m.maxSize = 0
			return nil
		}
		return err
	}
	b, err := syscall.Mmap(fd, 0, int(target), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		// Non-fatal: caller falls back to ReadAt. Matches SQLite's
		// "continue accessing the database using the xRead() and
		// xWrite() methods" note at os_unix.c:5579-5582.
		return fmt.Errorf("btree: mmap db %d bytes: %w", target, err)
	}
	m.region = b
	return nil
}

// unmap tears down the mapping. Matches SQLite's unixUnmapfile
// (os_unix.c:5562-5566).
func (m *dbMmap) unmap() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.region == nil {
		return nil
	}
	err := syscall.Munmap(m.region)
	m.region = nil
	return err
}

// enabled reports whether mmap reads are active.
func (m *dbMmap) enabled() bool {
	return m != nil && m.maxSize > 0
}

// fdFromFile extracts the OS fd from a fileHandle, but ONLY when the handle
// is the real OS file (*os.File), whose raw-fd bytes are guaranteed to equal
// its ReadAt. On the default build (fileHandle = *os.File) the assert always
// holds. On the vfs build (fileHandle = File interface), the default
// os.OpenFile still returns an *os.File so the real-OS path keeps mmap; an
// injected custom File fails the assert and yields errMmapNotRealFile so the
// caller falls back to File.ReadAt. This mirrors SQLite gating mmap on the
// VFS providing its own xFetch (bUseFetch only set when iVersion>=3,
// pager.c:3536-3539); SQLite never reaches around an active VFS to mmap a raw
// fd (getPageMMap fetches via the SAME VFS, pager.c:5682).
func fdFromFile(f fileHandle) (int, error) {
	if f == nil {
		return -1, os.ErrClosed
	}
	if of, ok := any(f).(*os.File); ok {
		return int(of.Fd()), nil
	}
	return -1, errMmapNotRealFile // custom VFS file: no equivalent raw fd
}
