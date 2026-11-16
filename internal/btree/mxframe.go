package btree

import "sync/atomic"

// commitFrameCounter is the storage for walIndex.mxCommitFrame.
//
// It deliberately does NOT embed atomic.Uint32. Embedding would expose a
// plain .Load() method — and a plain Load is a footgun: in multi-process
// mode the process-local value can be stale (a peer may have committed
// since), so checkpoint / truncate / close decisions that gate on "last
// committed frame" must read from the SHM header, not from this counter.
//
// By offering only .LoadLocal() here, callers that want the local value
// type the word "Local" (affirmative choice). Callers that need the
// cross-process truth use (*wal).authoritativeMxFrame(). The compiler
// enforces the distinction.
//
// SQLite sidesteps this issue because its pWal->hdr.mxFrame is
// per-connection (populated by walIndexTryHdr, wal.c:2570-2620), so the
// "right" value is naturally at hand. Our walIndex is shared across
// goroutines in the same process, which is why the guard is needed.
//
// The counter also carries the database size, in pages, that the commit
// ending on that frame recorded: SQLite's WalIndexHdr.nPage next to its
// mxFrame (wal.c:328-329), which a reader takes in one consistent header
// read and uses as its page count (sqlite3WalDbsize, wal.c:3669-3674). The
// two are one word here so that a read begin of the in-process and
// in-memory modes, which publish no WAL-index header, loads the pair as
// one: with two atomics a begin racing a commit could pair one commit's
// frame with another's size. Only those modes read the size
// (tryBeginReadInProcessHdr); a multi-process reader has the header's.
type commitFrameCounter struct {
	v atomic.Uint64 // frame<<32 | nPage
}

// LoadLocal returns the process-local commit-frame cursor. Do NOT use
// this for cross-process decisions (checkpoint truncate gate, close-time
// truncate gate, etc.); use (*wal).authoritativeMxFrame() instead.
func (c *commitFrameCounter) LoadLocal() uint32 { return uint32(c.v.Load() >> 32) }

// LoadCommit returns the commit-frame cursor and the database size the
// last commit recorded (0 when none has been recorded), as one read.
func (c *commitFrameCounter) LoadCommit() (frame, nPage uint32) {
	v := c.v.Load()
	return uint32(v >> 32), uint32(v)
}

// Store sets the commit-frame cursor and keeps the recorded database size:
// for the sites that move the cursor without a commit (a WAL restart, which
// keeps the size as doResetWAL keeps hdr.nPage; a cursor re-synced from the
// shared header).
func (c *commitFrameCounter) Store(v uint32) {
	for {
		old := c.v.Load()
		if c.v.CompareAndSwap(old, uint64(v)<<32|old&0xffffffff) {
			return
		}
	}
}

// StoreCommit sets the commit-frame cursor together with the database size
// of the commit that ends on it (commit path, recovery).
func (c *commitFrameCounter) StoreCommit(frame, nPage uint32) {
	c.v.Store(uint64(frame)<<32 | uint64(nPage))
}

// StoreSize sets the recorded database size and keeps the cursor: open,
// where the size of a database whose WAL holds no commit comes from page 1
// and the file.
func (c *commitFrameCounter) StoreSize(nPage uint32) {
	for {
		old := c.v.Load()
		if c.v.CompareAndSwap(old, old&^0xffffffff|uint64(nPage)) {
			return
		}
	}
}

// CompareAndSwap is a CAS on the local cursor; the recorded size is kept.
func (c *commitFrameCounter) CompareAndSwap(old, new uint32) bool {
	for {
		cur := c.v.Load()
		if uint32(cur>>32) != old {
			return false
		}
		if c.v.CompareAndSwap(cur, uint64(new)<<32|cur&0xffffffff) {
			return true
		}
	}
}
