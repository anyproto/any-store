package btree

// How one process learns that another committed.
//
// SQLite compares the wal-index header it read for a transaction with the
// one its connection last saw, and resets the pager when they differ:
// walIndexTryHdr sets *pChanged and copies the header into pWal->hdr
// (wal.c:2624-2626), and pagerBeginReadTransaction then calls pager_reset
// (pager.c:3261-3263), which raises iDataVersion, restarts backups and
// clears the cache (pager.c:1772-1776). The connection's own commits and
// restarts write pWal->hdr before they publish it (wal.c:4243-4250,
// 2151-2154), so they are never a change; a recovery always is
// (wal.c:2724-2725).
//
// One *wal here serves every transaction of the process, at many snapshots
// at once, so "the header last seen" and "the count of resets" cannot be two
// fields changed by one thread. They are one immutable value behind one
// pointer, changed by compare-and-swap only.
// DRIFT: one process-wide {header, epoch, pending} value under CAS stands in for C's per-connection pWal->hdr and iDataVersion See docs/btree/NOTES.md#drift-2026-11-13-1-the-change-state-is-one-value-under-compare-and-swap

// walChange is that value.
type walChange struct {
	// hdr is the newest wal-index header this process produced (a commit, a
	// WAL restart or reset, a recovery) or consumed (a begin that found
	// another process's). It only ever moves to a header that was read from,
	// or written to, shared memory after the previous one was current.
	hdr WalIndexHdr
	// epoch counts the changes this process did not make: SQLite's
	// iDataVersion with the connection's own commits cancelled
	// (pager.c:6709, btree.c:4417). A change made by another process enters
	// hdr or pending only in a swap that also raises epoch.
	epoch uint64
	// pending is the header an own publication is writing to shared memory,
	// from before its first byte until it is settled. A snapshot of it is
	// this process's own, and its epoch step, if it owes one, is already in
	// epoch.
	pending *WalIndexHdr
}

// exact reports that a transaction at snapshot snap sees exactly the changes
// c.epoch counts: no fewer and no more. While a publication is pending only
// its header qualifies. The retained hdr does not: epoch may already hold
// the step the pending header owes, for a change hdr does not contain.
func (c *walChange) exact(snap *WalIndexHdr) bool {
	if c.pending != nil {
		return *c.pending == *snap
	}
	return c.hdr == *snap
}

// noteSnapshot is the begin's half: snap is the header a read begin just
// took its snapshot at, pre the change state loaded before it did.
//
// A snapshot equal to the state's header, or to a pending own header, is no
// news. Any other is a change made elsewhere. If the state's header is still
// the one loaded before the snapshot was taken, the snapshot is the newer of
// the two and becomes the header; if it moved in the meantime (an own
// commit, another begin), their order is unknown: the change is counted and
// the header left alone, for the next begin to move.
func (w *wal) noteSnapshot(pre *walChange, snap *WalIndexHdr) {
	if pre.hdr == *snap {
		return
	}
	for {
		cur := w.chg.Load()
		if cur.hdr == *snap || (cur.pending != nil && *cur.pending == *snap) {
			return
		}
		next := &walChange{hdr: cur.hdr, epoch: cur.epoch + 1, pending: cur.pending}
		if cur.hdr == pre.hdr {
			next.hdr = *snap
		}
		if testChangeSwapHook != nil {
			testChangeSwapHook()
		}
		if w.chg.CompareAndSwap(cur, next) {
			return
		}
	}
}

// announce is the first half of an own publication: post is about to be
// written to shared memory in place of replaced, the header last published
// there, which the caller knows because it holds the write lock. If the
// state's header is not replaced, a change made elsewhere lies between them
// and post will contain it: its step is owed and taken here, before the
// header can be seen. A recovery always owes one (wal.c:2724-2725).
func (w *wal) announce(replaced, post *WalIndexHdr, recovery bool) {
	p := *post
	for {
		cur := w.chg.Load()
		next := &walChange{hdr: cur.hdr, epoch: cur.epoch, pending: &p}
		if recovery || cur.hdr != *replaced {
			next.epoch++
		}
		if w.chg.CompareAndSwap(cur, next) {
			return
		}
	}
}

// settle is the second half, run on every way out of the publication with
// the write lock still held: it decides from what shared memory holds. If
// that is post, post is the header. If not (the write failed, or unwound
// partway), nothing is known about what readers saw: the pending header is
// dropped and one step taken, so no consumer trusts a snapshot it certified.
func (w *wal) settle(post *WalIndexHdr) {
	live, valid := w.index.readHeader()
	published := valid && live == *post
	for {
		cur := w.chg.Load()
		next := &walChange{hdr: cur.hdr, epoch: cur.epoch}
		if published {
			next.hdr = *post
		} else {
			next.epoch++
		}
		if w.chg.CompareAndSwap(cur, next) {
			return
		}
	}
}

// publishHeader writes a wal-index header this process made, as walIndex
// .writeHeader does, between announce and settle. replaced and recovery: see
// announce. In the heap-shm modes, and while the WAL is being opened, there
// is no change state and it is writeHeader alone.
func (w *wal) publishHeader(replaced *WalIndexHdr, recovery bool, iChange, maxFrame, maxPage uint32, frameCksum, salt [2]uint32) error {
	region, err := w.index.prepareHeader(iChange, maxFrame, maxPage, frameCksum, salt)
	if err != nil {
		return err
	}
	if w.chg.Load() != nil {
		post := w.index.hdr
		w.announce(replaced, &post, recovery)
		defer w.settle(&post)
	}
	if testPublishHeaderHook != nil {
		testPublishHeaderHook("announced")
	}
	w.index.storeHeader(region)
	if testPublishHeaderHook != nil {
		testPublishHeaderHook("stored")
	}
	return nil
}

// Test hooks. testPublishHeaderHook runs inside publishHeader with the step
// just completed: "announced" (nothing written yet), "between" (copy 2
// written, copy 1 not), "stored" (both written, not settled).
// testChangeSwapHook runs in noteSnapshot before each swap attempt.
var (
	testPublishHeaderHook func(step string)
	testChangeSwapHook    func()
)

// changeEpoch returns the invalidation epoch and whether a transaction at
// snapshot snap is exact for it (walChange.exact). In the heap-shm modes no
// other process can be attached: the epoch never moves and every snapshot is
// exact.
func (w *wal) changeEpoch(snap *WalIndexHdr) (epoch uint64, exact bool) {
	cur := w.chg.Load()
	if cur == nil {
		return 0, true
	}
	return cur.epoch, cur.exact(snap)
}
