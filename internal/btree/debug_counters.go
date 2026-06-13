package btree

import (
	"sync"
	"sync/atomic"
)

// BtreeCounters are coarse page-level counters for diagnosing the read path
// (e.g. the per-row data-namespace point lookups done by the query FetchIter).
// DEBUG ONLY — env/flag gated, off by default; this file is for local profiling
// and is not meant to ship.
type BtreeCounters struct {
	GetPageReaderCalls uint64 // reader-cache page fetches (getPageReader, cache!=nil)
	PcacheHits         uint64 // served from the per-connection reader cache
	PcacheMisses       uint64 // not in the reader cache -> WAL or disk read
	DiskReads          uint64 // actual readDBPage (the pread syscall)
	DescendChild       uint64 // interior->child btree descents
	OverflowReads      uint64 // readOverflowAt chains (large values)

	DistinctReaderPages int // distinct pgnos seen at getPageReader (working set)
	DistinctDiskPages   int // distinct pgnos actually read from disk (pread set)
}

var btreeCtr struct {
	enabled atomic.Bool

	getPageReaderCalls atomic.Uint64
	pcacheHits         atomic.Uint64
	pcacheMisses       atomic.Uint64
	diskReads          atomic.Uint64
	descendChild       atomic.Uint64
	overflowReads      atomic.Uint64

	distMu        sync.Mutex
	distReaderPgs map[uint32]struct{}
	distDiskPgs   map[uint32]struct{}
	diskSeq       []uint32 // ordered pgnos of disk reads (for sequentiality analysis)
	recordSeq     bool
}

// SetRecordDiskSeq toggles recording the ordered sequence of disk-read pgnos.
func SetRecordDiskSeq(b bool) {
	btreeCtr.distMu.Lock()
	btreeCtr.recordSeq = b
	btreeCtr.distMu.Unlock()
}

// SnapshotDiskSeq returns a copy of the ordered disk-read pgno sequence.
func SnapshotDiskSeq() []uint32 {
	btreeCtr.distMu.Lock()
	defer btreeCtr.distMu.Unlock()
	return append([]uint32(nil), btreeCtr.diskSeq...)
}

func btreeCtrNoteReaderPage(pgno uint32) {
	btreeCtr.distMu.Lock()
	if btreeCtr.distReaderPgs == nil {
		btreeCtr.distReaderPgs = make(map[uint32]struct{})
	}
	btreeCtr.distReaderPgs[pgno] = struct{}{}
	btreeCtr.distMu.Unlock()
}

func btreeCtrNoteDiskPage(pgno uint32) {
	btreeCtr.distMu.Lock()
	if btreeCtr.distDiskPgs == nil {
		btreeCtr.distDiskPgs = make(map[uint32]struct{})
	}
	btreeCtr.distDiskPgs[pgno] = struct{}{}
	if btreeCtr.recordSeq {
		btreeCtr.diskSeq = append(btreeCtr.diskSeq, pgno)
	}
	btreeCtr.distMu.Unlock()
}

func btreeCtrEnabled() bool { return btreeCtr.enabled.Load() }

// EnableBtreeCounters toggles the debug page counters.
func EnableBtreeCounters(b bool) { btreeCtr.enabled.Store(b) }

// ResetBtreeCounters zeroes all debug page counters.
func ResetBtreeCounters() {
	btreeCtr.getPageReaderCalls.Store(0)
	btreeCtr.pcacheHits.Store(0)
	btreeCtr.pcacheMisses.Store(0)
	btreeCtr.diskReads.Store(0)
	btreeCtr.descendChild.Store(0)
	btreeCtr.overflowReads.Store(0)
	btreeCtr.distMu.Lock()
	btreeCtr.distReaderPgs = nil
	btreeCtr.distDiskPgs = nil
	btreeCtr.diskSeq = nil
	btreeCtr.distMu.Unlock()
}

// SnapshotBtreeCounters returns the current debug page counters.
func SnapshotBtreeCounters() BtreeCounters {
	btreeCtr.distMu.Lock()
	dr := len(btreeCtr.distReaderPgs)
	dd := len(btreeCtr.distDiskPgs)
	btreeCtr.distMu.Unlock()
	return BtreeCounters{
		GetPageReaderCalls:  btreeCtr.getPageReaderCalls.Load(),
		PcacheHits:          btreeCtr.pcacheHits.Load(),
		PcacheMisses:        btreeCtr.pcacheMisses.Load(),
		DiskReads:           btreeCtr.diskReads.Load(),
		DescendChild:        btreeCtr.descendChild.Load(),
		OverflowReads:       btreeCtr.overflowReads.Load(),
		DistinctReaderPages: dr,
		DistinctDiskPages:   dd,
	}
}
