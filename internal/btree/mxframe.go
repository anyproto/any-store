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
type commitFrameCounter struct {
	v atomic.Uint32
}

// LoadLocal returns the process-local commit-frame cursor. Do NOT use
// this for cross-process decisions (checkpoint truncate gate, close-time
// truncate gate, etc.); use (*wal).authoritativeMxFrame() instead.
func (c *commitFrameCounter) LoadLocal() uint32 { return c.v.Load() }

// Store sets the commit-frame cursor. Intended for writers that have
// just advanced the WAL frame ceiling (commit path, recovery, reset).
func (c *commitFrameCounter) Store(v uint32) { c.v.Store(v) }

// CompareAndSwap is a CAS on the local cursor. Used by the monotonic
// advance path in syncFromSHMLocked.
func (c *commitFrameCounter) CompareAndSwap(old, new uint32) bool {
	return c.v.CompareAndSwap(old, new)
}
