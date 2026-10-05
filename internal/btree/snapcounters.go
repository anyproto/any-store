package btree

import (
	"sync"
	"sync/atomic"
)

// snapCountersMemo remembers the page-1 counters (FileChangeCount and
// SchemaCookie) of one snapshot, so that a read transaction pinning the same
// snapshot learns them without reading page 1. A commit of this process
// stores the snapshot it produced; a read begin that had to read the
// counters stores what it read.
//
// The key is what identifies a snapshot to this process: dataVersion plus
// the WAL-index header (see DB.beginRead). Readers take no lock: every field
// is an atomic, framed by a sequence number that is odd while a writer is
// inside and changes with every write.
type snapCountersMemo struct {
	mu       sync.Mutex // serializes writers
	seq      atomic.Uint64
	dv       atomic.Uint64
	hdr      [6]atomic.Uint64
	counters atomic.Uint64 // FileChangeCount<<32 | SchemaCookie
}

func packWalIndexHdr(h *WalIndexHdr) [6]uint64 {
	return [6]uint64{
		uint64(h.iVersion)<<32 | uint64(h.unused),
		uint64(h.iChange)<<32 | uint64(h.isInit)<<24 | uint64(h.bigEndCksum)<<16 | uint64(h.szPage),
		uint64(h.mxFrame)<<32 | uint64(h.nPage),
		uint64(h.aFrameCksum[0])<<32 | uint64(h.aFrameCksum[1]),
		uint64(h.aSalt[0])<<32 | uint64(h.aSalt[1]),
		uint64(h.aCksum[0])<<32 | uint64(h.aCksum[1]),
	}
}

// get returns the counters of the snapshot (dv, hdr) if that is the one
// remembered.
func (m *snapCountersMemo) get(dv uint64, hdr *WalIndexHdr) (fcc, sc uint32, ok bool) {
	seq := m.seq.Load()
	if seq == 0 || seq&1 != 0 || m.dv.Load() != dv {
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
