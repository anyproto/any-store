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
// It holds the committed position: the commit frame and the database size
// that commit recorded, in one word, as SQLite's private header holds mxFrame
// and nPage together and moves them together once a commit is written and
// synced (wal.c:4238-4250). In the in-process and in-memory modes, which keep
// no wal-index header, this is the committed state: a read begin takes its
// frame ceiling and its page count from one load (sqlite3WalDbsize,
// wal.c:3669-3674), and nothing an open write transaction does moves either.
// In multi-process mode it mirrors the shared header, which stays the
// authority there.
type commitFrameCounter struct {
	v atomic.Uint64 // frame | nPage<<32
}

// LoadLocal returns the process-local commit-frame cursor. Do NOT use
// this for cross-process decisions (checkpoint truncate gate, close-time
// truncate gate, etc.); use (*wal).authoritativeMxFrame() instead.
func (c *commitFrameCounter) LoadLocal() uint32 { return uint32(c.v.Load()) }

// LoadPos returns the committed position: the commit frame and the database
// size in pages at that commit, from one load.
func (c *commitFrameCounter) LoadPos() (frame, nPage uint32) {
	v := c.v.Load()
	return uint32(v), uint32(v >> 32)
}

// StorePos sets the committed position.
func (c *commitFrameCounter) StorePos(frame, nPage uint32) {
	c.v.Store(uint64(frame) | uint64(nPage)<<32)
}

// Store sets the commit frame and keeps the size, as a WAL restart does
// (walRestartHdr keeps nPage, wal.c:2146-2160).
func (c *commitFrameCounter) Store(frame uint32) {
	for {
		old := c.v.Load()
		if c.v.CompareAndSwap(old, old&^0xffffffff|uint64(frame)) {
			return
		}
	}
}
