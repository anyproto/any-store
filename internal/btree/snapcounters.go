package btree

import (
	"sync"
	"sync/atomic"
)

// snapCountersMemo remembers the page-1 counters (FileChangeCount and
// SchemaCookie) of one snapshot, so that a read transaction pinning the same
// snapshot learns them without reading page 1. A commit of this process
// stores the snapshot it produces as it becomes visible; a read begin that
// had to read the counters stores what it read.
//
// The key is what identifies a snapshot to this process (see DB.beginRead):
// the WAL-index header, which is unique per commit when it comes from the
// shared memory — change counter, frame checksum and salts — and for the
// header synthesized in-process, which is a frame number only, the
// dataVersion with it.
//
// Readers take no lock: every field is an atomic, framed by a sequence
// number that is odd while a writer is inside and changes with every write.
type snapCountersMemo struct {
	seq      atomic.Uint64
	dv       atomic.Uint64
	counters atomic.Uint64 // FileChangeCount<<32 | SchemaCookie
	hdr      [5]atomic.Uint64
	mu       sync.Mutex // serializes writers
}

// packWalIndexHdr packs what tells one header from another; iVersion and
// the padding are the same in all.
func packWalIndexHdr(h *WalIndexHdr) [5]uint64 {
	return [5]uint64{
		uint64(h.iChange)<<32 | uint64(h.isInit)<<24 | uint64(h.bigEndCksum)<<16 | uint64(h.szPage),
		uint64(h.mxFrame)<<32 | uint64(h.nPage),
		uint64(h.aFrameCksum[0])<<32 | uint64(h.aFrameCksum[1]),
		uint64(h.aSalt[0])<<32 | uint64(h.aSalt[1]),
		uint64(h.aCksum[0])<<32 | uint64(h.aCksum[1]),
	}
}

// synthesized reports that hdr is the frame-number-only header of the
// in-process and in-memory modes: no salts.
func (h *WalIndexHdr) synthesized() bool {
	return h.aSalt[0]|h.aSalt[1] == 0
}

// get returns the counters of the snapshot (dv, hdr) if that is the one
// remembered.
func (m *snapCountersMemo) get(dv uint64, hdr *WalIndexHdr) (fcc, sc uint32, ok bool) {
	seq := m.seq.Load()
	if seq == 0 || seq&1 != 0 || (hdr.synthesized() && m.dv.Load() != dv) {
		return 0, 0, false
	}
	want := packWalIndexHdr(hdr)
	for i := range want {
		if m.hdr[i].Load() != want[i] {
			return 0, 0, false
		}
	}
	c := m.counters.Load()
	if m.seq.Load() != seq {
		return 0, 0, false
	}
	return uint32(c >> 32), uint32(c), true
}

// put remembers the counters of the snapshot (dv, hdr).
func (m *snapCountersMemo) put(dv uint64, hdr *WalIndexHdr, fcc, sc uint32) {
	if _, _, ok := m.get(dv, hdr); ok {
		return
	}
	words := packWalIndexHdr(hdr)
	m.mu.Lock()
	m.seq.Add(1)
	m.dv.Store(dv)
	for i := range words {
		m.hdr[i].Store(words[i])
	}
	m.counters.Store(uint64(fcc)<<32 | uint64(sc))
	m.seq.Add(1)
	m.mu.Unlock()
}
