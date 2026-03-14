package btree

// WAL (Write-Ahead Log) implementation modeled after SQLite's wal.c.
//
// WAL file format:
//
//	WAL Header (32 bytes):
//	  Offset  Size  Description
//	  0       4     Magic number (0x42540601 for big-endian checksums)
//	  4       4     File format version (1000000)
//	  8       4     Database page size
//	  12      4     Checkpoint sequence number
//	  16      4     Salt-1 (random value, changes with each WAL reset)
//	  20      4     Salt-2 (random value)
//	  24      4     Checksum-1 (of first 24 bytes)
//	  28      4     Checksum-2
//
//	Frame Header (24 bytes):
//	  Offset  Size  Description
//	  0       4     Page number
//	  4       4     For commit frames: size of database in pages after commit
//	  8       4     Salt-1 (must match WAL header)
//	  12      4     Salt-2 (must match WAL header)
//	  16      4     Checksum-1 (cumulative of frame header + page data)
//	  20      4     Checksum-2
//
// A frame is a "commit frame" if the database-size field (offset 4) is non-zero.
// The WAL is read during recovery and by readers to find the latest version of pages.
//
// Shared memory (.shm file):
//
// The WAL index is stored in a memory-mapped .shm file, enabling multiple
// processes to coordinate access to the WAL. On linux/amd64, this uses mmap
// with POSIX fcntl locks. On other platforms, a heap-based fallback is used
// (single-process only).
//
// The shm contains:
//   - WAL index header (region 0): metadata about WAL state
//   - Hash tables: mapping page numbers to WAL frame positions
//   - Lock slots: coordinating readers, writer, and checkpoint
//
// Same-process reads (issue 7.9):
//
// walIndex.get() uses an in-process Go map (pageMap) rather than reading
// the SHM hash tables. The SHM hash tables ARE written on every frame
// (shmHashWrite) for cross-process visibility, but same-process readers
// use the faster Go map. This is acceptable because:
//
//  1. Our primary deployment is single-process (InProcess mode with heap shm).
//     Multi-process access via mmap'd shm is a secondary feature.
//  2. The Go map provides O(1) lookup without the linear-probing overhead of
//     the hash table, giving better read performance.
//  3. The SHM hash tables are still populated correctly, so if multi-process
//     readers are added in the future, they can use shmHashGet() for recovery.
//  4. In SQLite, walHashGet/walFramePage are only used during recovery and
//     checkpoint iteration. Normal same-process reads also use in-memory state
//     (the aSegment array in walIterator). Our pageMap serves the same role.
//
// If multi-process readers become a requirement, the get() method should be
// updated to fall back to shmHashGet() when !inProcess and the pageMap is
// empty (indicating a fresh process that hasn't done recovery yet).

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math/bits"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	walHeaderSize = 32
	walFrameSize  = 24 // frame header only, page data follows

	walMagic   = 0x42540601 // "BT" prefix + version, distinct from SQLite's 0x377f0682
	walVersion = 1000000    // our own version, distinct from SQLite's 3007000

	// WAL index (shared memory) constants
	walIndexHeaderSize = 48
	walHashSize        = 4096 // entries per hash table segment

	// SHM hash table layout constants (matching SQLite's wal-index hash tables).
	// Each shm region (32KB) stores a hash table segment mapping page numbers
	// to WAL frame positions. Region 0 also contains the WAL index header.
	htNPage = 4096 // max frame entries per hash segment (power of 2)
	htNSlot = 8192 // hash slots per segment (2 * htNPage, power of 2)
	htHash1 = 383  // hash multiplier (prime)

	htHdrSize = 136 // header area in region 0 (two copies of WalIndexHdr + WalCkptInfo)

	// Checkpoint info offsets in region 0
	htCkptOff     = 96  // nBackfill (4 bytes)
	htReadMarkOff = 100 // aReadMark[0..4] (5 * 4 = 20 bytes)

	// nBackfillAttempted offset in region 0 (after aReadMark + aLock).
	// Layout: nBackfill(4) + aReadMark(20) + aLock(8) = 32 bytes from offset 96.
	// nBackfillAttempted is at offset 128, matching SQLite's WalCkptInfo layout.
	htNBackfillAttemptedOff = 128

	// Hash table data offsets
	htPgnoOff0     = htHdrSize   // aPgno start in region 0 (byte 136)
	htHashArrayOff = htNPage * 4 // aHash start in all regions (byte 16384)

	// Number of frame entries in region 0 (reduced by header)
	htNPageOne = htNPage - (htHdrSize / 4) // 4062
)

// CheckpointMode specifies the type of checkpoint to perform.
// Modeled after SQLite's SQLITE_CHECKPOINT_* constants.
type CheckpointMode int

const (
	// CheckpointPassive checkpoints as many frames as possible without waiting
	// for any database readers or writers to finish. The busy handler is never
	// invoked. Might leave the checkpoint unfinished if there are concurrent
	// readers or writers.
	CheckpointPassive CheckpointMode = iota

	// CheckpointFull blocks (invokes the busy handler) until there is no
	// database writer and all readers are reading from the most recent
	// database snapshot. It then checkpoints all frames in the log file and
	// syncs the database file. Blocks new writers while pending, but new
	// readers are allowed to continue. Does not reset the WAL.
	CheckpointFull

	// CheckpointRestart works the same as CheckpointFull with the addition
	// that after checkpointing it blocks (calls the busy handler) until all
	// readers are reading from the database file only. This ensures that the
	// next writer will restart the log file from the beginning. Blocks new
	// writers while pending, but does not impede readers.
	CheckpointRestart

	// CheckpointTruncate works the same as CheckpointRestart with the addition
	// that it also truncates the log file to zero bytes.
	CheckpointTruncate
)

// BusyHandler is a callback invoked when a lock cannot be acquired.
// It is called repeatedly with an incrementing count (starting at 0)
// until it returns false (give up) or the lock is acquired.
// Modeled after SQLite's sqlite3_busy_handler() callback.
type BusyHandler func(count int) bool

// DefaultBusyTimeout returns a BusyHandler that retries with exponential
// backoff up to the given total timeout duration. Modeled after SQLite's
// sqliteDefaultBusyCallback (main.c): the handler is stateless, computing
// the cumulative sleep from the count parameter alone. This makes it safe
// for concurrent use from multiple walBusyLock call sites.
//
// Delay schedule (ms): 1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100, ...
// Matches SQLite's delays[] table; after the table is exhausted the last
// delay (100ms) repeats.
func DefaultBusyTimeout(timeout time.Duration) BusyHandler {
	if timeout <= 0 {
		return nil
	}
	tmout := timeout.Milliseconds()
	return func(count int) bool {
		// SQLite's delays[] and totals[] tables (main.c:1717-1720).
		// delays[i] is the sleep for attempt i; totals[i] is the
		// cumulative sleep before attempt i.
		var delays = [...]int64{1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50, 100}
		var totals = [...]int64{0, 1, 3, 8, 18, 33, 53, 78, 103, 128, 178, 228}
		nDelay := len(delays)

		var delay, prior int64
		if count < nDelay {
			delay = delays[count]
			prior = totals[count]
		} else {
			delay = delays[nDelay-1]
			prior = totals[nDelay-1] + delay*int64(count-(nDelay-1))
		}
		if prior+delay > tmout {
			delay = tmout - prior
			if delay <= 0 {
				return false
			}
		}
		time.Sleep(time.Duration(delay) * time.Millisecond)
		return true
	}
}

// walBusyLock attempts to acquire a lock on the given slot, retrying via
// the busy handler if the lock is busy.
// If xBusy is nil, returns ErrBusy immediately on failure.
// Modeled after SQLite's walBusyLock().
func walBusyLock(wi *walIndex, xBusy BusyHandler, slot int, lockType int) error {
	var count int
	for {
		err := wi.lock(slot, lockType)
		if err == nil {
			return nil
		}
		if err != ErrBusy {
			return err
		}
		if xBusy == nil || !xBusy(count) {
			return ErrBusy
		}
		count++
	}
}

// walHeader represents the WAL file header.
type walHeader struct {
	magic      uint32
	version    uint32
	pageSize   uint32
	checkpoint uint32
	salt1      uint32
	salt2      uint32
	checksum1  uint32
	checksum2  uint32
}

func (wh *walHeader) serialize(buf []byte) {
	binary.BigEndian.PutUint32(buf[0:4], wh.magic)
	binary.BigEndian.PutUint32(buf[4:8], wh.version)
	binary.BigEndian.PutUint32(buf[8:12], wh.pageSize)
	binary.BigEndian.PutUint32(buf[12:16], wh.checkpoint)
	binary.BigEndian.PutUint32(buf[16:20], wh.salt1)
	binary.BigEndian.PutUint32(buf[20:24], wh.salt2)
	c1, c2 := walChecksum(buf[0:24], 0, 0)
	binary.BigEndian.PutUint32(buf[24:28], c1)
	binary.BigEndian.PutUint32(buf[28:32], c2)
}

func (wh *walHeader) deserialize(buf []byte) error {
	if len(buf) < walHeaderSize {
		return ErrWALCorrupt
	}
	wh.magic = binary.BigEndian.Uint32(buf[0:4])
	if wh.magic != walMagic {
		return ErrWALCorrupt
	}
	wh.version = binary.BigEndian.Uint32(buf[4:8])
	wh.pageSize = binary.BigEndian.Uint32(buf[8:12])
	wh.checkpoint = binary.BigEndian.Uint32(buf[12:16])
	wh.salt1 = binary.BigEndian.Uint32(buf[16:20])
	wh.salt2 = binary.BigEndian.Uint32(buf[20:24])
	wh.checksum1 = binary.BigEndian.Uint32(buf[24:28])
	wh.checksum2 = binary.BigEndian.Uint32(buf[28:32])

	// Verify header checksum
	c1, c2 := walChecksum(buf[0:24], 0, 0)
	if c1 != wh.checksum1 || c2 != wh.checksum2 {
		return ErrWALCorrupt
	}
	return nil
}

// walFrame represents a single WAL frame header.
type walFrame struct {
	pgno      uint32
	dbSize    uint32 // non-zero for commit frames
	salt1     uint32
	salt2     uint32
	checksum1 uint32
	checksum2 uint32
}

func (wf *walFrame) serialize(buf []byte) {
	binary.BigEndian.PutUint32(buf[0:4], wf.pgno)
	binary.BigEndian.PutUint32(buf[4:8], wf.dbSize)
	binary.BigEndian.PutUint32(buf[8:12], wf.salt1)
	binary.BigEndian.PutUint32(buf[12:16], wf.salt2)
	binary.BigEndian.PutUint32(buf[16:20], wf.checksum1)
	binary.BigEndian.PutUint32(buf[20:24], wf.checksum2)
}

func (wf *walFrame) deserialize(buf []byte) {
	wf.pgno = binary.BigEndian.Uint32(buf[0:4])
	wf.dbSize = binary.BigEndian.Uint32(buf[4:8])
	wf.salt1 = binary.BigEndian.Uint32(buf[8:12])
	wf.salt2 = binary.BigEndian.Uint32(buf[12:16])
	wf.checksum1 = binary.BigEndian.Uint32(buf[16:20])
	wf.checksum2 = binary.BigEndian.Uint32(buf[20:24])
}

// walChecksum computes the WAL checksum over 32-bit big-endian words.
// Matches SQLite's walChecksumBytes() paired-word recurrence:
//
//	s1 += x[i]   + s2;
//	s2 += x[i+1] + s1;
//
// Data is interpreted as big-endian 32-bit words (since our walMagic has the
// big-endian checksum bit set), processed in pairs. The input length must be
// a multiple of 8 bytes.
//
// Uses unsafe pointer arithmetic to eliminate bounds checking in the hot loop.
func walChecksum(data []byte, s1, s2 uint32) (uint32, uint32) {
	if len(data) < 8 {
		return s1, s2
	}
	n := len(data) / 4
	p := unsafe.Pointer(&data[0])
	i := 0
	// Unrolled 8x (4 pairs) for throughput
	for ; i+7 < n; i += 8 {
		base := unsafe.Add(p, uintptr(i)*4)
		w0 := bits.ReverseBytes32(*(*uint32)(base))
		w1 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 4)))
		s1 += w0 + s2
		s2 += w1 + s1

		w2 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 8)))
		w3 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 12)))
		s1 += w2 + s2
		s2 += w3 + s1

		w4 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 16)))
		w5 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 20)))
		s1 += w4 + s2
		s2 += w5 + s1

		w6 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 24)))
		w7 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 28)))
		s1 += w6 + s2
		s2 += w7 + s1
	}
	// Handle remaining pairs (n is always even since len is multiple of 8)
	for ; i+1 < n; i += 2 {
		w0 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(p, uintptr(i)*4)))
		w1 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(p, uintptr(i+1)*4)))
		s1 += w0 + s2
		s2 += w1 + s1
	}
	return s1, s2
}

// readMarkNotUsed is the sentinel value for an unused read mark slot.
const readMarkNotUsed = uint32(0xFFFFFFFF)

// errWALRetry is an internal sentinel signaling that beginRead should retry.
// It is never returned to callers; the retry loop in beginRead handles it.
var errWALRetry = errors.New("btree: WAL retry")

// walIndexHdrSize is the size of one WalIndexHdr in the SHM (48 bytes),
// matching SQLite's struct WalIndexHdr.
const walIndexHdrSize = 48

// WalIndexHdr mirrors SQLite's WalIndexHdr struct (48 bytes).
// Two copies are stored at the start of SHM region 0, followed by WalCkptInfo.
// The dual-copy design allows readers to detect torn writes by comparing both copies.
//
// Layout (matching SQLite):
//
//	Offset  Size  Field
//	0       4     iVersion      -- wal-index format version
//	4       4     unused        -- padding
//	8       4     iChange       -- counter incremented each transaction
//	12      1     isInit        -- 1 when initialized
//	13      1     bigEndCksum   -- 1 if WAL checksums are big-endian
//	14      2     szPage        -- database page size (1 means 65536)
//	16      4     mxFrame       -- index of last valid frame in WAL
//	20      4     nPage         -- size of database in pages
//	24      8     aFrameCksum   -- checksum of last frame in log
//	32      8     aSalt         -- salt values from WAL header
//	40      8     aCksum        -- checksum over all prior fields (bytes 0..39)
type WalIndexHdr struct {
	iVersion    uint32
	unused      uint32
	iChange     uint32
	isInit      uint8
	bigEndCksum uint8
	szPage      uint16
	mxFrame     uint32
	nPage       uint32
	aFrameCksum [2]uint32
	aSalt       [2]uint32
	aCksum      [2]uint32
}

// serialize writes the WalIndexHdr into a 48-byte buffer (little-endian,
// matching SQLite's native byte order for the wal-index on little-endian platforms).
func (h *WalIndexHdr) serialize(buf []byte) {
	binary.LittleEndian.PutUint32(buf[0:4], h.iVersion)
	binary.LittleEndian.PutUint32(buf[4:8], h.unused)
	binary.LittleEndian.PutUint32(buf[8:12], h.iChange)
	buf[12] = h.isInit
	buf[13] = h.bigEndCksum
	binary.LittleEndian.PutUint16(buf[14:16], h.szPage)
	binary.LittleEndian.PutUint32(buf[16:20], h.mxFrame)
	binary.LittleEndian.PutUint32(buf[20:24], h.nPage)
	binary.LittleEndian.PutUint32(buf[24:28], h.aFrameCksum[0])
	binary.LittleEndian.PutUint32(buf[28:32], h.aFrameCksum[1])
	binary.LittleEndian.PutUint32(buf[32:36], h.aSalt[0])
	binary.LittleEndian.PutUint32(buf[36:40], h.aSalt[1])
	binary.LittleEndian.PutUint32(buf[40:44], h.aCksum[0])
	binary.LittleEndian.PutUint32(buf[44:48], h.aCksum[1])
}

// deserialize reads a WalIndexHdr from a 48-byte buffer.
func (h *WalIndexHdr) deserialize(buf []byte) {
	h.iVersion = binary.LittleEndian.Uint32(buf[0:4])
	h.unused = binary.LittleEndian.Uint32(buf[4:8])
	h.iChange = binary.LittleEndian.Uint32(buf[8:12])
	h.isInit = buf[12]
	h.bigEndCksum = buf[13]
	h.szPage = binary.LittleEndian.Uint16(buf[14:16])
	h.mxFrame = binary.LittleEndian.Uint32(buf[16:20])
	h.nPage = binary.LittleEndian.Uint32(buf[20:24])
	h.aFrameCksum[0] = binary.LittleEndian.Uint32(buf[24:28])
	h.aFrameCksum[1] = binary.LittleEndian.Uint32(buf[28:32])
	h.aSalt[0] = binary.LittleEndian.Uint32(buf[32:36])
	h.aSalt[1] = binary.LittleEndian.Uint32(buf[36:40])
	h.aCksum[0] = binary.LittleEndian.Uint32(buf[40:44])
	h.aCksum[1] = binary.LittleEndian.Uint32(buf[44:48])
}

// computeCksum computes the aCksum field over bytes 0..39 of the header.
// Uses the WAL checksum with native byte order (nativeCksum=true in SQLite,
// which on little-endian means we interpret as native u32 words).
func (h *WalIndexHdr) computeCksum() {
	var buf [40]byte
	binary.LittleEndian.PutUint32(buf[0:4], h.iVersion)
	binary.LittleEndian.PutUint32(buf[4:8], h.unused)
	binary.LittleEndian.PutUint32(buf[8:12], h.iChange)
	buf[12] = h.isInit
	buf[13] = h.bigEndCksum
	binary.LittleEndian.PutUint16(buf[14:16], h.szPage)
	binary.LittleEndian.PutUint32(buf[16:20], h.mxFrame)
	binary.LittleEndian.PutUint32(buf[20:24], h.nPage)
	binary.LittleEndian.PutUint32(buf[24:28], h.aFrameCksum[0])
	binary.LittleEndian.PutUint32(buf[28:32], h.aFrameCksum[1])
	binary.LittleEndian.PutUint32(buf[32:36], h.aSalt[0])
	binary.LittleEndian.PutUint32(buf[36:40], h.aSalt[1])
	// Use walChecksumNative for native-endian word processing (like SQLite does
	// for the wal-index header checksum).
	h.aCksum[0], h.aCksum[1] = walChecksumNative(buf[:], 0, 0)
}

// walChecksumNative computes the WAL checksum treating data as native-endian
// 32-bit words (no byte swap). Used for wal-index header checksums.
// Same paired-word recurrence as walChecksum but without byte swapping.
func walChecksumNative(data []byte, s1, s2 uint32) (uint32, uint32) {
	if len(data) < 8 {
		return s1, s2
	}
	n := len(data) / 4
	p := unsafe.Pointer(&data[0])
	i := 0
	for ; i+1 < n; i += 2 {
		w0 := *(*uint32)(unsafe.Add(p, uintptr(i)*4))
		w1 := *(*uint32)(unsafe.Add(p, uintptr(i+1)*4))
		s1 += w0 + s2
		s2 += w1 + s1
	}
	return s1, s2
}

// walIndex manages the WAL index stored in shared memory.
// It maps page numbers to their latest WAL frame positions.
// The index is backed by the shm interface, which may be mmap'd
// for multi-process access or heap-backed for single-process.
type walIndex struct {
	// mu protects pageMap only. All scalar fields (maxFrame, maxPage,
	// nBackfill, nBackfillAttempted, aReadMark) are atomic.Uint32 and
	// must be accessed via Load/Store without holding mu. This matches
	// SQLite's lock-free walTryBeginRead design. Do NOT add mu.RLock/Lock
	// around atomic field accesses.
	//
	// DRIFT from SQLite (NOTES.md §20, drifts 2-3): SQLite has no process-local
	// copies of mxCommitFrame, nBackfill, or aReadMark. These values live ONLY
	// in the mmap'd SHM region (via volatile WalCkptInfo* pointer). We maintain
	// process-local atomic.Uint32 copies alongside SHM because:
	//   - In-process mode (heapShm) has no mmap'd region — process-local atomics
	//     ARE the single source of truth.
	//   - walIndex.get() reads nBackfill.Load()+1 as minFrame — this code path is
	//     shared by in-process and multi-process modes.
	//   - Multi-process tryBeginReadMultiProcess reads from SHM (shmNBackfill,
	//     shmReadMark) then syncs nBackfill to process-local at the end for get().
	mu      sync.RWMutex
	shm     shm               // platform-specific shared memory
	pageMap map[uint32][]uint32 // pgno -> sorted list of frame indices (1-based)
	maxFrame      atomic.Uint32 // highest valid frame (committed + spilled)
	mxCommitFrame atomic.Uint32 // highest COMMITTED frame — visible to readers
	maxPage       atomic.Uint32 // database size at last commit
	nBackfill atomic.Uint32    // frames already checkpointed

	// nBackfillAttempted is the highest frame that a checkpoint has attempted
	// to copy back to the database. It is set BEFORE backfilling begins, so
	// that after a crash during checkpoint, recovery knows which frames may
	// have been partially written. nBackfillAttempted >= nBackfill always.
	// Matches SQLite's WalCkptInfo.nBackfillAttempted (issue 7.7).
	nBackfillAttempted atomic.Uint32

	// hdr is the current WalIndexHdr, matching SQLite's pWal->hdr.
	// Contains all 11 fields from the SQLite WalIndexHdr struct.
	hdr WalIndexHdr

	// iChange is incremented on each write transaction.
	iChange uint32

	// inProcess indicates heap-backed shm (no multi-process coordination needed).
	inProcess bool

	// pendingShmFrames accumulates frame->pgno pairs for deferred SHM hash writes.
	// During spill (writeFrames with commit=false), SHM hash writes are deferred
	// to prevent cross-process readers from seeing uncommitted frames.
	// Flushed to SHM hash tables on commit via flushPendingShmFrames().
	// DRIFT from SQLite: SQLite always calls walIndexAppend (writes SHM hash) even
	// for non-commit frames, relying on walIndexWriteHdr not being called to hide them.
	// We defer the SHM writes entirely for stronger isolation.
	pendingShmFrames []struct{ pgno, frame uint32 }

	// aReadMark tracks each reader's WAL snapshot position.
	// Slot 0 is special: readers on slot 0 read entirely from the DB (nBackfill == maxFrame).
	// Slots 1-4 are for readers that need WAL frames.
	aReadMark [5]atomic.Uint32
}

func newWalIndex(shmPath string, inProcess bool) (*walIndex, error) {
	var s shm
	if inProcess {
		s = newHeapShm()
	} else {
		var err error
		s, err = newPlatformShm(shmPath)
		if err != nil {
			return nil, err
		}
	}
	wi := &walIndex{
		shm:       s,
		pageMap:   make(map[uint32][]uint32),
		inProcess: inProcess,
	}
	// Initialize all read marks as unused
	for i := range wi.aReadMark {
		wi.aReadMark[i].Store(readMarkNotUsed)
	}
	return wi, nil
}

// set records a page at a given frame position.
func (wi *walIndex) set(pgno, frame uint32) {
	wi.mu.Lock()
	frames := wi.pageMap[pgno]
	if frames == nil {
		frames = make([]uint32, 0, 4)
	}
	wi.pageMap[pgno] = append(frames, frame)
	wi.mu.Unlock()
	if frame > wi.maxFrame.Load() {
		wi.maxFrame.Store(frame)
	}
	wi.shmHashWrite(pgno, frame)
}

// setBatch records multiple page->frame mappings under a single lock.
// When commit is false (spill), SHM hash writes are deferred to prevent
// cross-process readers from seeing uncommitted frames. The in-process
// pageMap is always updated immediately so the writer can see its own pages.
func (wi *walIndex) setBatch(pages []*page, startFrame uint32, commit bool) {
	wi.mu.Lock()
	for i, p := range pages {
		frame := startFrame + uint32(i)
		frames := wi.pageMap[p.pgno]
		if frames == nil {
			frames = make([]uint32, 0, 4)
		}
		wi.pageMap[p.pgno] = append(frames, frame)
	}
	wi.mu.Unlock()
	if f := startFrame + uint32(len(pages)) - 1; f > wi.maxFrame.Load() {
		wi.maxFrame.Store(f)
	}
	if commit {
		// Write to shm hash tables immediately for cross-process visibility
		for i, p := range pages {
			wi.shmHashWrite(p.pgno, startFrame+uint32(i))
		}
	} else {
		// DRIFT from SQLite: SQLite writes SHM hash entries immediately in
		// walFrames() via walIndexAppend(), then cleans them up with
		// walCleanupHash() on rollback. We defer SHM writes for spill frames
		// and flush on commit, avoiding cross-process readers seeing
		// uncommitted frames without needing post-rollback cleanup.
		for i, p := range pages {
			wi.pendingShmFrames = append(wi.pendingShmFrames, struct{ pgno, frame uint32 }{
				pgno:  p.pgno,
				frame: startFrame + uint32(i),
			})
		}
	}
}

// flushPendingShmFrames writes all deferred SHM hash entries accumulated during
// spill (non-commit writeFrames calls). Called on commit to make spilled frames
// visible to cross-process readers.
func (wi *walIndex) flushPendingShmFrames() {
	for _, pf := range wi.pendingShmFrames {
		wi.shmHashWrite(pf.pgno, pf.frame)
	}
	wi.pendingShmFrames = wi.pendingShmFrames[:0]
}

// rollbackToFrame removes all pageMap entries with frame > target and restores
// maxFrame to target. Also clears pendingShmFrames since spilled frames are
// being discarded. Called on transaction rollback or savepoint rollback to
// clean up frames written by spill (writeFrames with commit=false).
func (wi *walIndex) rollbackToFrame(target uint32) {
	wi.mu.Lock()
	for pgno, frames := range wi.pageMap {
		// Find cutoff: keep only frames <= target
		n := len(frames)
		for n > 0 && frames[n-1] > target {
			n--
		}
		if n == 0 {
			delete(wi.pageMap, pgno)
		} else if n < len(frames) {
			wi.pageMap[pgno] = frames[:n]
		}
	}
	wi.mu.Unlock()
	wi.maxFrame.Store(target)
	// Only discard pending SHM entries for frames beyond the rollback target.
	// Entries at or below the target belong to spills from before the savepoint
	// and must be preserved for flushPendingShmFrames at commit time.
	n := 0
	for _, pf := range wi.pendingShmFrames {
		if pf.frame <= target {
			wi.pendingShmFrames[n] = pf
			n++
		}
	}
	wi.pendingShmFrames = wi.pendingShmFrames[:n]
}

// get returns the frame containing the latest version of pgno that is
// within the given maxFrame snapshot, or 0 if not in WAL.
// The maxFrame parameter limits which frames are visible (for snapshot isolation).
func (wi *walIndex) get(pgno, maxFrame uint32) uint32 {
	wi.mu.RLock()
	// Skip frames that have been checkpointed back to the DB.
	// This matches SQLite's minFrame filter (wal.c:3571) and prevents
	// readers from seeing stale WAL frames after a checkpoint + truncate.
	minFrame := wi.nBackfill.Load() + 1
	frames := wi.pageMap[pgno]
	wi.mu.RUnlock()

	// Frames are appended in order, so the list is sorted ascending.
	// Search backwards for the highest frame in [minFrame, maxFrame].
	for i := len(frames) - 1; i >= 0; i-- {
		if frames[i] <= maxFrame && frames[i] >= minFrame {
			return frames[i]
		}
	}

	// Cross-process fallback: read from SHM hash tables when not in
	// single-process mode. Another process may have written frames
	// that are not in our pageMap.
	if !wi.inProcess {
		if frame := wi.shmHashGet(pgno, maxFrame, minFrame); frame > 0 {
			return frame
		}
	}

	return 0
}

// getLatest returns the latest WAL frame for pgno, or 0 if the page is not
// in the WAL. Used to detect whether a cached page may have been updated
// beyond a reader's snapshot. The caller compares the result against its
// walMaxFrame (which is bounded by mxCommitFrame from beginRead) to decide
// whether the cached page is still valid.
//
// Note: We now use per-connection page caches matching SQLite's model, but
// we still share one pageMap across all goroutines. Spill frames are visible
// to getLatest, causing transient cache misses during active spill when
// latestFrame > walMaxFrame. Each reader's private cache absorbs repeated
// misses within a transaction. This is correct and short-lived.
func (wi *walIndex) getLatest(pgno uint32) uint32 {
	wi.mu.RLock()
	frames := wi.pageMap[pgno]
	wi.mu.RUnlock()

	if len(frames) > 0 {
		return frames[len(frames)-1]
	}

	// Cross-process fallback: check SHM hash tables.
	// Use mxCommitFrame so spilled uncommitted frames (which are not
	// written to SHM hash during spill) are bounded correctly.
	// getLatest is used for cache invalidation, not snapshot reads,
	// so minFrame=1 (search all non-checkpointed segments).
	if !wi.inProcess {
		minFrame := wi.nBackfill.Load() + 1
		return wi.shmHashGet(pgno, wi.mxCommitFrame.Load(), minFrame)
	}

	return 0
}

// reset clears the WAL index (after a checkpoint + WAL truncate).
func (wi *walIndex) reset() {
	wi.mu.Lock()
	clear(wi.pageMap)
	wi.mu.Unlock()
	wi.maxFrame.Store(0)
	wi.mxCommitFrame.Store(0)
	wi.nBackfill.Store(0)
	wi.nBackfillAttempted.Store(0)
	for i := range wi.aReadMark {
		wi.aReadMark[i].Store(readMarkNotUsed)
	}
	wi.pendingShmFrames = wi.pendingShmFrames[:0]
	wi.shmClearHash()
	wi.shmWriteCkptInfo()
}

// writeHeader writes the WAL index header to region 0 of the shm.
// Matches SQLite's walIndexWriteHdr():
//   - Computes aCksum over the header fields
//   - Writes copy 2 first (offset 48)
//   - Issues a memory barrier (walShmBarrier)
//   - Writes copy 1 (offset 0)
//
// The dual-copy + barrier design allows readers to detect torn writes
// by comparing both copies.
func (wi *walIndex) writeHeader(maxFrame, maxPage, nBackfill uint32, frameCksum, salt [2]uint32) error {
	region, err := wi.shm.region(0, true)
	if err != nil {
		return err
	}

	// Populate the header struct.
	// DRIFT from SQLite: SQLite's walIndexWriteHdr (wal.c:942-954) copies
	// pWal->hdr directly to SHM — no parameters. We pass explicit values
	// because walIndex.hdr and wal fields are separate structs.
	wi.hdr.isInit = 1
	wi.hdr.iVersion = walVersion
	wi.hdr.mxFrame = maxFrame
	wi.hdr.nPage = maxPage
	wi.hdr.iChange = wi.iChange
	wi.hdr.aFrameCksum = frameCksum
	wi.hdr.aSalt = salt

	// Compute the header checksum over bytes 0..39 (all fields except aCksum)
	wi.hdr.computeCksum()

	// Serialize header to a buffer
	var buf [walIndexHdrSize]byte
	wi.hdr.serialize(buf[:])

	// Write copy 2 first (offset walIndexHdrSize = 48), then barrier, then copy 1
	// This matches SQLite's write order: copy 2 first, barrier, copy 1.
	copy(region[walIndexHdrSize:walIndexHdrSize*2], buf[:])

	// Memory barrier between the two copies (7.2 + 7.5).
	// For mmap'd shared memory, this ensures that the second copy is fully
	// visible to other processes before we start writing the first copy.
	if !wi.inProcess {
		walShmBarrier()
	}

	copy(region[0:walIndexHdrSize], buf[:])

	return nil
}

// readHeader reads and validates the WAL index header from SHM region 0.
// It compares both copies of the header to detect torn writes (7.3).
// Returns the header and true if valid, or a zero header and false if
// the header is corrupt or the two copies don't match.
func (wi *walIndex) readHeader() (WalIndexHdr, bool) {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return WalIndexHdr{}, false
	}

	if len(region) < walIndexHdrSize*2 {
		return WalIndexHdr{}, false
	}

	// Read copy 1 (offset 0)
	var hdr1 WalIndexHdr
	hdr1.deserialize(region[0:walIndexHdrSize])

	// Memory barrier before reading copy 2
	if !wi.inProcess {
		walShmBarrier()
	}

	// Read copy 2 (offset walIndexHdrSize = 48)
	var hdr2 WalIndexHdr
	hdr2.deserialize(region[walIndexHdrSize : walIndexHdrSize*2])

	// Both copies must match
	var buf1, buf2 [walIndexHdrSize]byte
	hdr1.serialize(buf1[:])
	hdr2.serialize(buf2[:])
	if buf1 != buf2 {
		return WalIndexHdr{}, false
	}

	// Verify isInit flag
	if hdr1.isInit != 1 {
		return WalIndexHdr{}, false
	}

	// Verify checksum: recompute aCksum over bytes 0..39 and compare
	saved := hdr1.aCksum
	hdr1.computeCksum()
	if hdr1.aCksum != saved {
		return WalIndexHdr{}, false
	}

	return hdr1, true
}

// walShmBarrier issues a memory barrier for cross-process mmap'd memory.
// On x86/amd64, stores are already ordered (TSO), but we need a compiler barrier
// to prevent the Go compiler from reordering. On ARM64, we need a full fence.
// This matches SQLite's walShmBarrier() / sqlite3OsShmBarrier().
func walShmBarrier() {
	// atomic.StoreUint32 on a dummy variable acts as both a compiler barrier
	// and a memory fence (uses MFENCE on x86, DMB on ARM64).
	var dummy uint32
	atomic.StoreUint32(&dummy, 0)
	// Also yield to help with cache coherency visibility across processes.
	runtime.Gosched()
}

// lock acquires a shm lock.
func (wi *walIndex) lock(slot int, lockType int) error {
	return wi.shm.lock(slot, lockType)
}

// unlock releases a shm lock.
func (wi *walIndex) unlock(slot int, lockType int) error {
	return wi.shm.unlock(slot, lockType)
}

// close closes the shm.
func (wi *walIndex) close() error {
	if wi.shm != nil {
		return wi.shm.close()
	}
	return nil
}

// --- SHM hash table operations ---
//
// The WAL index hash tables reside in shm regions, enabling multi-process
// readers to find page->frame mappings without scanning the WAL file.
//
// Layout per region:
//
//	Region 0: [header 136B][aPgno 4062x4B][aHash 8192x2B] = 32768 bytes
//	Region i: [aPgno 4096x4B][aHash 8192x2B] = 32768 bytes
//
// aPgno[idx] stores the page number for the frame at position (iZero + idx + 1).
// aHash is a linear-probing hash table: hash(pgno) -> (idx+1), where 0 = empty.

// htFrameSegIdx returns the segment (region) index and entry index for a frame number.
func htFrameSegIdx(frame uint32) (seg int, idx int) {
	if frame <= htNPageOne {
		return 0, int(frame) - 1
	}
	f := int(frame) - htNPageOne - 1
	return 1 + f/htNPage, f % htNPage
}

// htPgnoOffset returns the byte offset of aPgno[idx] within the segment's region.
func htPgnoOffset(seg, idx int) int {
	if seg == 0 {
		return htPgnoOff0 + idx*4
	}
	return idx * 4
}

// htSegmentInfo returns the aPgno base offset, entry count, and iZero for a segment.
// iZero is the frame number that maps to aPgno[0] minus 1 (so frame = iZero + idx + 1).
func htSegmentInfo(seg int) (pgnoBase int, nEntry int, iZero uint32) {
	if seg == 0 {
		return htPgnoOff0, int(htNPageOne), 0
	}
	return 0, htNPage, uint32(htNPageOne + (seg-1)*htNPage)
}

// shmHashWrite records a page->frame mapping in the shm hash table.
// Best-effort: errors are ignored since the Go map is authoritative for same-process reads.
func (wi *walIndex) shmHashWrite(pgno, frame uint32) {
	seg, idx := htFrameSegIdx(frame)

	region, err := wi.shm.region(seg, true)
	if err != nil {
		return
	}

	// Write pgno to aPgno[idx]
	pgnoOff := htPgnoOffset(seg, idx)
	binary.LittleEndian.PutUint32(region[pgnoOff:], pgno)

	// Insert into hash table (linear probing)
	h := int(pgno*htHash1) & (htNSlot - 1)
	for range htNSlot {
		slotOff := htHashArrayOff + h*2
		if binary.LittleEndian.Uint16(region[slotOff:]) == 0 {
			binary.LittleEndian.PutUint16(region[slotOff:], uint16(idx+1))
			return
		}
		h = (h + 1) & (htNSlot - 1)
	}
}

// shmHashGet looks up the latest frame for pgno from shm hash tables.
// Returns 0 if not found. Only frames <= maxFrame are considered.
// This is the cross-process read path; same-process readers use the Go map.
// shmHashGet searches SHM hash tables for the latest frame of pgno within
// [minFrame, maxFrame]. This is the cross-process fallback when pageMap
// doesn't contain the page — another process may have written WAL frames.
//
// Matches SQLite's walFindFrame (wal.c:3554-3581): only scan hash segments
// that could contain frames in [minFrame, maxFrame], searching backwards
// from the newest segment. This avoids scanning already-checkpointed
// segments, which is critical for performance on large WALs.
func (wi *walIndex) shmHashGet(pgno, maxFrame, minFrame uint32) uint32 {
	if maxFrame == 0 {
		return 0
	}

	lastSeg, _ := htFrameSegIdx(maxFrame)
	// Match SQLite wal.c:3554 — skip segments before minFrame.
	minSeg, _ := htFrameSegIdx(minFrame)

	for seg := lastSeg; seg >= minSeg; seg-- {
		region, err := wi.shm.region(seg, false)
		if err != nil {
			continue
		}

		pgnoBase, nEntry, iZero := htSegmentInfo(seg)
		h := int(pgno*htHash1) & (htNSlot - 1)

		var bestFrame uint32
		for range htNSlot {
			slotOff := htHashArrayOff + h*2
			entry := binary.LittleEndian.Uint16(region[slotOff:])
			if entry == 0 {
				break // end of probe chain
			}
			idx := int(entry) - 1
			if idx < nEntry {
				storedPgno := binary.LittleEndian.Uint32(region[pgnoBase+idx*4:])
				if storedPgno == pgno {
					frame := iZero + uint32(idx) + 1
					if frame <= maxFrame && frame >= minFrame && frame > bestFrame {
						bestFrame = frame
					}
				}
			}
			h = (h + 1) & (htNSlot - 1)
		}

		if bestFrame > 0 {
			return bestFrame
		}
	}

	return 0
}

// shmClearHash zeros out all hash table data in shm regions.
// Called during WAL reset. Preserves the header area in region 0.
func (wi *walIndex) shmClearHash() {
	for seg := range shmMaxRegions {
		region, err := wi.shm.region(seg, false)
		if err != nil {
			break
		}
		if seg == 0 {
			clear(region[htPgnoOff0:])
		} else {
			clear(region[:])
		}
	}
}

// shmWriteCkptInfo writes checkpoint info (nBackfill, aReadMark, nBackfillAttempted)
// to shm region 0. Matches SQLite's WalCkptInfo layout.
func (wi *walIndex) shmWriteCkptInfo() {
	region, err := wi.shm.region(0, true)
	if err != nil {
		return
	}
	binary.LittleEndian.PutUint32(region[htCkptOff:], wi.nBackfill.Load())
	for i := range 5 {
		binary.LittleEndian.PutUint32(region[htReadMarkOff+i*4:], wi.aReadMark[i].Load())
	}
	// Write nBackfillAttempted at offset 128 (issue 7.7)
	binary.LittleEndian.PutUint32(region[htNBackfillAttemptedOff:], wi.nBackfillAttempted.Load())
}

// shmReadCkptInfo reads checkpoint info (nBackfill, aReadMark, nBackfillAttempted)
// from shm region 0 into process-local atomics.
//
// WARNING: This function bulk-copies SHM values into shared process-local atomics.
// It must NOT be called from concurrent reader goroutines — multiple goroutines
// calling this concurrently will clobber each other's values, causing stale
// snapshot reads. Use shmNBackfill/shmReadMark for per-call SHM reads instead.
// This function is retained for use in tests and single-goroutine contexts only.
// See NOTES.md §20, drift 6.
func (wi *walIndex) shmReadCkptInfo() {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return
	}
	wi.nBackfill.Store(binary.LittleEndian.Uint32(region[htCkptOff:]))
	for i := range 5 {
		wi.aReadMark[i].Store(binary.LittleEndian.Uint32(region[htReadMarkOff+i*4:]))
	}
	// Read nBackfillAttempted at offset 128 (issue 7.7)
	wi.nBackfillAttempted.Store(binary.LittleEndian.Uint32(region[htNBackfillAttemptedOff:]))
}

// shmNBackfill reads nBackfill directly from SHM region 0 without updating
// process-local atomics. Matches SQLite's AtomicLoad(&pInfo->nBackfill) pattern
// where nBackfill is always read from shared memory, not from a per-connection copy.
func (wi *walIndex) shmNBackfill() uint32 {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return 0
	}
	return binary.LittleEndian.Uint32(region[htCkptOff:])
}

// shmReadMark reads a single aReadMark[i] directly from SHM region 0 without
// updating process-local atomics. Matches SQLite's AtomicLoad(pInfo->aReadMark+i).
func (wi *walIndex) shmReadMark(i int) uint32 {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return readMarkNotUsed
	}
	return binary.LittleEndian.Uint32(region[htReadMarkOff+i*4:])
}

// shmWriteReadMark writes a single aReadMark[i] directly to SHM region 0.
// Matches SQLite's AtomicStore(pInfo->aReadMark+i, mxFrame).
func (wi *walIndex) shmWriteReadMark(i int, val uint32) {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return
	}
	binary.LittleEndian.PutUint32(region[htReadMarkOff+i*4:], val)
}

// wal manages the Write-Ahead Log.
type wal struct {
	mu       sync.RWMutex // protects memFrames slice; readers use RLock, writer uses Lock
	file     fileHandle
	header   walHeader
	index    *walIndex
	pageSize uint32
	path     string
	nFrame atomic.Uint32 // total frames written (atomic: read by readFrame, written by writeFrames)

	// Cumulative checksum state for appending frames
	cksum1 uint32
	cksum2 uint32

	// Reusable write buffer to avoid per-commit allocations
	writeBuf []byte

	// ckptBuf is a reusable page-sized buffer for reading WAL frames during
	// checkpoint backfill. Allocated lazily on first checkpoint. Matches
	// SQLite's use of pTmpSpace in walCheckpoint (wal.c:2285-2304), though
	// SQLite passes pTmpSpace from the pager rather than storing it on the WAL.
	ckptBuf []byte

	// headerOnDisk is false when the WAL file is empty (0 bytes) and the
	// header has not yet been written to disk. The header is written lazily
	// on the first writeFrames call. This matches SQLite's behavior where
	// after a clean close the WAL file is empty/deleted and only recreated
	// when the first write transaction commits.
	headerOnDisk bool

	// inProcess uses heap-backed shm instead of mmap+fcntl (faster, single-process only)
	inProcess bool

	// noCommitSync skips fdatasync on WAL commit (like SQLite synchronous=normal)
	noCommitSync bool

	// inMemory keeps the entire WAL in memory with no WAL file
	inMemory bool

	// busyHandler is an optional callback invoked when lock acquisition fails.
	// If nil, lock failures return ErrBusy immediately (issue 1.7).
	busyHandler BusyHandler

	// readSnapshot is the SHM header snapshot saved during beginRead (multi-process only).
	// Used by beginWrite for BUSY_SNAPSHOT comparison.
	// DRIFT from SQLite: SQLite uses pWal->hdr for both snapshot comparison and
	// checksum chaining in walEncodeFrame. We keep them separate (readSnapshot for
	// snapshot, wal.cksum1/cksum2 for chaining) because our checksum fields are
	// separate from the SHM header struct.
	// Only accessed by writer goroutine, no synchronization needed.
	readSnapshot WalIndexHdr

	// writerHdr is the writer's private copy of the SHM header, updated after
	// each successful commit (writeFrames) and after re-sync in beginWrite().
	// Used to detect external state changes: if the live SHM header differs from
	// writerHdr, another process committed or checkpointed since our last write.
	// DRIFT from SQLite: SQLite stores this in pWal->hdr (shared with readers).
	// We keep it separate because our readers use per-connection pcache isolation
	// and never share pWal->hdr with the writer.
	writerHdr WalIndexHdr

	// memFrames stores page data in memory for InMemory mode,
	// eliminating per-commit file I/O. Frames are flushed to pcache on checkpoint.
	memFrames []memFrame

	// memArena is a pre-allocated byte pool for memFrame data to avoid
	// per-frame allocations and reduce GC pressure.
	memArena    []byte
	memArenaOff int
}

// memFrame stores a single WAL frame's page data in memory.
type memFrame struct {
	pgno   uint32
	dbSize uint32
	data   []byte // page data (slice into memArena)
}

func newWal(path string, pageSize uint32) *wal {
	return &wal{
		path:     path,
		pageSize: pageSize,
	}
}

// open opens or creates the WAL file and recovers any committed frames.
func (w *wal) open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.inMemory {
		// InMemory mode: no WAL file, heap-backed everything
		idx, err := newWalIndex(w.path+"-shm", true)
		if err != nil {
			return err
		}
		w.index = idx
		w.initHeaderState()
		w.memFrames = make([]memFrame, 0, 1024)
		return nil
	}

	f, err := osOpenFile(w.path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	w.file = f

	// Initialize shared memory for WAL index.
	idx, err := newWalIndex(w.path+"-shm", w.inProcess)
	if err != nil {
		return err
	}
	w.index = idx

	// Acquire exclusive locks on all lock slots except WAL_WRITE_LOCK (slot 0)
	// and WAL_READ_LOCK(0) (slot 3), matching SQLite's walIndexRecover().
	// This means locking WAL_CKPT_LOCK (1) and WAL_RECOVER_LOCK (2) exclusively,
	// which prevents concurrent checkpoints and other recoveries.
	if err := w.index.lock(lockCheckpoint, lockExclusive); err != nil {
		return err
	}
	defer func() { _ = w.index.unlock(lockCheckpoint, lockExclusive) }()

	if err := w.index.lock(lockRecover, lockExclusive); err != nil {
		return err
	}
	defer func() { _ = w.index.unlock(lockRecover, lockExclusive) }()

	info, err := f.Stat()
	if err != nil {
		return err
	}

	if info.Size() >= walHeaderSize {
		return w.recover()
	}

	// New/empty WAL: initialize in-memory state without writing to disk.
	// The header will be written lazily on the first writeFrames call.
	// This matches SQLite's behavior where after a clean close the WAL file
	// is empty/deleted and only populated when the first write commits.
	w.initHeaderState()

	return nil
}

// initHeaderState initializes the in-memory WAL header state without writing
// to disk. Used when the WAL file is empty (after a clean close). The header
// will be flushed to disk lazily on the first writeFrames call.
func (w *wal) initHeaderState() {
	w.header = walHeader{
		magic:      walMagic,
		version:    walVersion,
		pageSize:   w.pageSize,
		checkpoint: 0,
		salt1:      rand.Uint32(),
		salt2:      rand.Uint32(),
	}

	// Compute checksum state from the header fields
	var buf [walHeaderSize]byte
	w.header.serialize(buf[:])
	w.cksum1, w.cksum2 = walChecksum(buf[0:24], 0, 0)
	w.nFrame.Store(0)
	w.headerOnDisk = false
	w.index.reset()

	// Update shm header with initial checksums and salts
	_ = w.index.writeHeader(0, 0, 0,
		[2]uint32{w.cksum1, w.cksum2},
		[2]uint32{w.header.salt1, w.header.salt2})
}

// flushHeader writes the already-initialized in-memory header to disk.
// Called lazily on the first writeFrames when headerOnDisk is false.
// Must be called with w.mu held.
func (w *wal) flushHeader() error {
	if w.file == nil {
		return errors.New("btree: WAL file closed")
	}
	buf := make([]byte, walHeaderSize)
	w.header.serialize(buf)

	if _, err := w.file.WriteAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(w.file); err != nil {
		return err
	}
	w.headerOnDisk = true
	return nil
}

// writeHeader writes a fresh WAL header to disk.
func (w *wal) writeHeader() error {
	w.header = walHeader{
		magic:      walMagic,
		version:    walVersion,
		pageSize:   w.pageSize,
		checkpoint: 0,
		salt1:      rand.Uint32(),
		salt2:      rand.Uint32(),
	}

	buf := make([]byte, walHeaderSize)
	w.header.serialize(buf)

	if _, err := w.file.WriteAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(w.file); err != nil {
		return err
	}

	// Initialize checksum state from header
	w.cksum1, w.cksum2 = walChecksum(buf[0:24], 0, 0)
	w.nFrame.Store(0)
	w.headerOnDisk = true
	w.index.reset()

	// Update shm header
	return w.index.writeHeader(0, 0, 0,
		[2]uint32{w.cksum1, w.cksum2},
		[2]uint32{w.header.salt1, w.header.salt2})
}

// recover reads the WAL file and rebuilds the in-memory index from committed frames.
//
// Matches SQLite's walIndexRecover(): the WAL file is never modified during
// recovery. Uncommitted trailing frames are simply ignored by setting mxFrame
// to the last committed frame. The WAL file retains its full on-disk size.
func (w *wal) recover() error {
	w.headerOnDisk = true // WAL file already has a header on disk

	buf := make([]byte, walHeaderSize)
	if _, err := w.file.ReadAt(buf, 0); err != nil {
		return err
	}
	if err := w.header.deserialize(buf); err != nil {
		// Invalid WAL, start fresh
		if err := w.file.Truncate(0); err != nil {
			return err
		}
		return w.writeHeader()
	}

	w.pageSize = w.header.pageSize
	w.index.reset()

	// Initialize checksum from header
	w.cksum1, w.cksum2 = walChecksum(buf[0:24], 0, 0)

	// Read frames
	frameHeaderBuf := make([]byte, walFrameSize)
	pageBuf := make([]byte, w.pageSize)
	offset := int64(walHeaderSize)
	frameSize := int64(walFrameSize) + int64(w.pageSize)

	info, err := w.file.Stat()
	if err != nil {
		return err
	}

	var frame walFrame
	var nFrame uint32
	var lastCommitFrame uint32
	var lastCommitDbSize uint32
	var lastCommitCksum1, lastCommitCksum2 uint32

	s1, s2 := w.cksum1, w.cksum2

	for offset+frameSize <= info.Size() {
		if _, err := w.file.ReadAt(frameHeaderBuf, offset); err != nil {
			break
		}
		if _, err := w.file.ReadAt(pageBuf, offset+walFrameSize); err != nil {
			break
		}

		frame.deserialize(frameHeaderBuf)

		// Verify salt
		if frame.salt1 != w.header.salt1 || frame.salt2 != w.header.salt2 {
			break
		}

		// Verify checksum: checksum covers frame header (first 8 bytes) + page data
		s1, s2 = walChecksum(frameHeaderBuf[0:8], s1, s2)
		s1, s2 = walChecksum(pageBuf, s1, s2)

		if s1 != frame.checksum1 || s2 != frame.checksum2 {
			break
		}

		nFrame++
		w.index.set(frame.pgno, nFrame)

		if frame.dbSize > 0 {
			lastCommitFrame = nFrame
			lastCommitDbSize = frame.dbSize
			lastCommitCksum1 = s1
			lastCommitCksum2 = s2
		}

		offset += frameSize
	}

	// Only index frames up to the last commit (like SQLite: set mxFrame,
	// do NOT truncate the WAL file). Uncommitted trailing frames are ignored.
	if lastCommitFrame > 0 {
		// Rebuild index with only committed frames (clear and re-add).
		w.index.reset()

		offset = int64(walHeaderSize)
		for i := uint32(1); i <= lastCommitFrame; i++ {
			if _, err := w.file.ReadAt(frameHeaderBuf, offset); err != nil {
				return err
			}
			frame.deserialize(frameHeaderBuf)
			w.index.set(frame.pgno, i)
			offset += frameSize
		}

		w.nFrame.Store(lastCommitFrame)
		w.cksum1 = lastCommitCksum1
		w.cksum2 = lastCommitCksum2
		w.index.maxFrame.Store(lastCommitFrame)
		w.index.mxCommitFrame.Store(lastCommitFrame)
		w.index.maxPage.Store(lastCommitDbSize)

		// Set nBackfillAttempted to mxFrame during recovery (issue 7.7).
		// This matches SQLite's walIndexRecover() which sets
		// pInfo->nBackfillAttempted = pWal->hdr.mxFrame.
		w.index.nBackfillAttempted.Store(lastCommitFrame)
		w.index.shmWriteCkptInfo()
	} else {
		// No committed frames -- set empty state, do NOT truncate WAL file.
		w.nFrame.Store(0)
		w.index.reset()
	}

	// Update shm header with recovered state (use mxCommitFrame for reader visibility)
	return w.index.writeHeader(w.index.mxCommitFrame.Load(), w.index.maxPage.Load(), 0,
		[2]uint32{w.cksum1, w.cksum2},
		[2]uint32{w.header.salt1, w.header.salt2})
}

// writeFrames appends frames to the WAL. If commit is true, the last frame
// is marked as a commit frame with the given dbSize.
// All frames are batched into a single write call for performance.
func (w *wal) writeFrames(pages []*page, commit bool, dbSize uint32) error {
	if len(pages) == 0 {
		return nil
	}

	// Fast path: in-memory WAL for InMemory mode.
	// Stores page data in memory, skipping file I/O and checksums entirely.
	if w.inMemory {
		return w.writeFramesMem(pages, commit, dbSize)
	}

	if w.file == nil {
		return errors.New("btree: WAL file closed")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Lazily write the WAL header to disk on first frame write.
	// The header was initialized in memory during open() but not flushed
	// to keep the WAL file empty until actual writes occur.
	if !w.headerOnDisk {
		if err := w.flushHeader(); err != nil {
			return err
		}
	}

	frameSize := int(walFrameSize) + int(w.pageSize)
	nf := w.nFrame.Load()
	offset := int64(walHeaderSize) + int64(nf)*int64(frameSize)

	// Reuse write buffer to avoid per-commit allocations
	needSize := len(pages) * frameSize
	if cap(w.writeBuf) >= needSize {
		w.writeBuf = w.writeBuf[:needSize]
	} else {
		w.writeBuf = make([]byte, needSize)
	}
	buf := w.writeBuf

	s1, s2 := w.cksum1, w.cksum2
	startFrame := nf + 1

	for i, p := range pages {
		pos := i * frameSize
		frameBuf := buf[pos : pos+walFrameSize]

		binary.BigEndian.PutUint32(frameBuf[0:4], p.pgno)
		var dbSizeField uint32
		if commit && i == len(pages)-1 {
			dbSizeField = dbSize
		}
		binary.BigEndian.PutUint32(frameBuf[4:8], dbSizeField)
		binary.BigEndian.PutUint32(frameBuf[8:12], w.header.salt1)
		binary.BigEndian.PutUint32(frameBuf[12:16], w.header.salt2)

		// Compute checksum over frame header (first 8 bytes) + page data
		s1, s2 = walChecksum(frameBuf[0:8], s1, s2)
		s1, s2 = walChecksum(p.data, s1, s2)

		binary.BigEndian.PutUint32(frameBuf[16:20], s1)
		binary.BigEndian.PutUint32(frameBuf[20:24], s2)

		// Copy page data into buffer
		copy(buf[pos+walFrameSize:pos+frameSize], p.data)
	}

	// Single write call for all frames
	n, err := w.file.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return io.ErrShortWrite
	}

	// Only advance nFrame and checksums after successful write. If WriteAt
	// fails (disk full, I/O error), the WAL state remains at the pre-write
	// position so subsequent writes use the correct offset and checksum chain.
	w.nFrame.Store(nf + uint32(len(pages)))
	w.cksum1 = s1
	w.cksum2 = s2

	// Batch update walIndex under a single lock.
	// Pass commit so setBatch defers SHM hash writes for non-commit (spill) frames.
	w.index.setBatch(pages, startFrame, commit)

	if commit {
		// Flush any previously deferred SHM hash entries from spill frames.
		w.index.flushPendingShmFrames()
		if dbSize > 0 {
			w.index.maxPage.Store(dbSize)
		}
		if !w.noCommitSync {
			if err := fdatasync(w.file); err != nil {
				return err
			}
		}
		// Advance mxCommitFrame AFTER fdatasync so in-process readers only
		// see committed frames once they are durable on disk. This matches
		// SQLite where walIndexWriteHdr (which publishes mxFrame) is called
		// after fdatasync.
		mxCommit := w.index.maxFrame.Load()
		w.index.mxCommitFrame.Store(mxCommit)
		if !w.inProcess {
			// Use dbSize directly instead of maxPage.Load() because a
			// concurrent reader's tryBeginRead may have overwritten maxPage
			// with a stale value from the SHM header (race between the
			// Store above and this writeHeader call).
			nPage := dbSize
			if nPage == 0 {
				nPage = w.index.maxPage.Load()
			}
			if err := w.index.writeHeader(mxCommit, nPage, w.index.nBackfill.Load(),
				[2]uint32{w.cksum1, w.cksum2},
				[2]uint32{w.header.salt1, w.header.salt2}); err != nil {
				return err
			}
			// Snapshot the header we just wrote so beginWrite can detect
			// external state changes without false positives from our own
			// commits (53f68eb fix).
			w.writerHdr = w.index.hdr
		}
	}

	return nil
}

// writeFramesMem is the fast in-memory path for writeFrames.
// No file I/O, no checksums -- just copy page data into a pre-allocated arena.
// Acquires w.mu to synchronize with readFrame which reads w.memFrames.
// nFrame is updated atomically after the slice is populated, ensuring readers
// only see the new nFrame after the memFrames data is visible.
func (w *wal) writeFramesMem(pages []*page, commit bool, dbSize uint32) error {
	w.mu.Lock()

	nf := w.nFrame.Load()
	startFrame := nf + 1
	pageSz := int(w.pageSize)

	// Ensure arena has enough space for all pages in this batch
	needed := len(pages) * pageSz
	if w.memArenaOff+needed > len(w.memArena) {
		// Allocate new arena: at least 1MB or enough for 256 pages
		arenaSize := max(1<<20, pageSz*256)
		if needed > arenaSize {
			arenaSize = needed
		}
		w.memArena = make([]byte, arenaSize)
		w.memArenaOff = 0
	}

	for _, p := range pages {
		// Slice page data from the arena (no individual allocation)
		dataCopy := w.memArena[w.memArenaOff : w.memArenaOff+pageSz]
		w.memArenaOff += pageSz
		copy(dataCopy, p.data)

		var dbSizeField uint32
		if commit && p == pages[len(pages)-1] {
			dbSizeField = dbSize
		}

		w.memFrames = append(w.memFrames, memFrame{
			pgno:   p.pgno,
			dbSize: dbSizeField,
			data:   dataCopy,
		})
		nf++
	}

	// Store nFrame while still holding the lock so readers see consistent
	// memFrames slice + nFrame via their RLock.
	w.nFrame.Store(nf)
	w.mu.Unlock()

	// Batch update walIndex (has its own lock).
	// Pass commit so setBatch defers SHM hash writes for non-commit (spill) frames.
	w.index.setBatch(pages, startFrame, commit)

	if commit {
		// Flush any previously deferred SHM hash entries from spill frames.
		w.index.flushPendingShmFrames()
		// Advance mxCommitFrame so readers can see the committed frames.
		w.index.mxCommitFrame.Store(w.index.maxFrame.Load())
		if dbSize > 0 {
			w.index.maxPage.Store(dbSize)
		}
	}

	return nil
}

// readFrame reads the page data for a given frame number.
// For the file-based path, only an atomic load of nFrame is needed (WAL frames
// on disk are immutable once written). For the memFrames path, RLock protects
// the slice from concurrent append by writeFramesMem.
func (w *wal) readFrame(frame uint32, buf []byte) error {
	nf := w.nFrame.Load()
	if frame == 0 || frame > nf {
		return ErrWALCorrupt
	}
	// Fast path: read from in-memory frames (needs RLock for slice access).
	// Use the immutable inMemory flag instead of checking the mutable memFrames
	// slice header to avoid a data race with writeFramesMem's append.
	if w.inMemory {
		w.mu.RLock()
		idx := frame - 1
		if idx < uint32(len(w.memFrames)) {
			copy(buf[:w.pageSize], w.memFrames[idx].data)
			w.mu.RUnlock()
			return nil
		}
		w.mu.RUnlock()
		return ErrWALCorrupt
	}
	// File path: no lock needed. WAL frames are immutable once written,
	// and nFrame was already validated atomically above.
	if w.file == nil {
		return ErrWALCorrupt
	}
	frameSize := int64(walFrameSize) + int64(w.pageSize)
	offset := int64(walHeaderSize) + int64(frame-1)*frameSize + walFrameSize
	_, err := w.file.ReadAt(buf[:w.pageSize], offset)
	return err
}

// beginRead acquires a shared read lock on a reader slot and returns the
// current max frame number for snapshot isolation plus the slot number.
// Reader slot rotation (like SQLite):
//   - Slot 0: used when nBackfill == maxFrame (read everything from DB, skip WAL)
//   - Slots 1-4: used for readers that need WAL frames. Best slot is the one
//     with the largest readmark <= current maxFrame.
func (w *wal) beginRead() (maxFrame uint32, slot int, err error) {
	// Retry loop matching SQLite's WAL_RETRY protocol (wal.c:3239-3245).
	// If a checkpoint resets the WAL between our metadata read and lock
	// acquisition, tryBeginRead returns errWALRetry and we try again.
	// DRIFT from SQLite (NOTES.md §20, drift 5): SQLite does not use a
	// *pChanged output signal — pWal->hdr already reflects the current SHM
	// state after walIndexReadHdr(). Our per-connection caches are invalidated
	// via dataVersion in DB.beginRead(), not via a signal from tryBeginRead.
	// SQLite uses an unbounded retry loop. We use a generous limit to
	// detect true bugs while tolerating heavy contention (e.g., concurrent
	// readers + checkpoint cycling in multi-process mode).
	const retryLimit = 5000
	for i := 0; i < retryLimit; i++ {
		maxFrame, slot, err = w.tryBeginRead()
		if !errors.Is(err, errWALRetry) {
			return maxFrame, slot, err
		}
		if i >= 5 {
			runtime.Gosched()
		}
	}
	return 0, 0, ErrProtocol
}

// tryBeginRead attempts to acquire a reader slot and returns the current
// max frame number. Returns errWALRetry if the WAL state changed between
// reading metadata and acquiring the lock, signaling the caller to retry.
func (w *wal) tryBeginRead() (maxFrame uint32, slot int, err error) {
	if w.inProcess || w.inMemory {
		return w.tryBeginReadInProcess()
	}
	return w.tryBeginReadMultiProcess()
}

// tryBeginReadInProcess handles the in-process/in-memory path where all
// state is in process-local atomics and there are no cross-goroutine SHM races.
// No re-validation needed because writes go through process-local atomics
// (mxCommitFrame, nBackfill, aReadMark) which are always consistent within
// a single process. No WAL_RETRY — the in-process path never races with
// external checkpoint or writer state changes.
func (w *wal) tryBeginReadInProcess() (maxFrame uint32, slot int, err error) {
	mxFrame := w.index.mxCommitFrame.Load()
	nBackfill := w.index.nBackfill.Load()

	if mxFrame == 0 || nBackfill == mxFrame {
		if err := w.index.lock(lockRead0, lockShared); err != nil {
			return 0, 0, err
		}
		w.index.aReadMark[0].Store(mxFrame)
		return mxFrame, 0, nil
	}

	bestSlot := -1
	bestMark := uint32(0)
	for i := 1; i <= 4; i++ {
		mark := w.index.aReadMark[i].Load()
		if mark != readMarkNotUsed && mark <= mxFrame && mark > bestMark {
			bestSlot = i
			bestMark = mark
		}
	}

	if bestSlot != -1 {
		lockSlot := lockRead0 + bestSlot
		if err := w.index.lock(lockSlot, lockShared); err == nil {
			return mxFrame, bestSlot, nil
		}
	}

	for i := 1; i <= 4; i++ {
		lockSlot := lockRead0 + i
		if err := w.index.lock(lockSlot, lockShared); err == nil {
			w.index.aReadMark[i].Store(mxFrame)
			return mxFrame, i, nil
		}
	}

	if err := w.index.lock(lockRead0, lockShared); err != nil {
		return 0, 0, err
	}
	w.index.aReadMark[0].Store(mxFrame)
	return mxFrame, 0, nil
}

// tryBeginReadMultiProcess handles the multi-process path, matching SQLite's
// walTryBeginRead() (wal.c:3000-3252) step by step:
//
//  1. Read SHM header into a local copy (SQLite: walIndexReadHdr → walIndexTryHdr
//     copies SHM into pWal->hdr). We use a function-scoped local variable instead
//     of a persistent per-connection field. (NOTES.md §20, drift 4)
//  2. Read nBackfill and aReadMark directly from SHM each time (SQLite: volatile
//     WalCkptInfo *pInfo = walCkptInfo(pWal), then AtomicLoad). We use shmNBackfill()
//     and shmReadMark() helper functions. (NOTES.md §20, drift 1)
//  3. Claim a reader slot using exclusive lock → write readmark → unlock → shared lock.
//     Matches SQLite wal.c:3170-3185 exactly.
//  4. Re-validate after acquiring shared lock: compare live SHM header against local
//     copy AND live readmark against saved value. If either changed → WAL_RETRY.
//     Matches SQLite wal.c:3239-3249 (memcmp + AtomicLoad re-check).
//  5. Sync nBackfill to process-local atomic for walIndex.get() minFrame filter.
//     (NOTES.md §20, drift 3 — SQLite doesn't need this sync step because it reads
//     nBackfill directly from SHM via pInfo pointer everywhere)
func (w *wal) tryBeginReadMultiProcess() (maxFrame uint32, slot int, err error) {
	// Step 1: Read SHM header into local copy (SQLite: walIndexReadHdr → walIndexTryHdr)
	hdr, valid := w.index.readHeader()
	if !valid {
		// Can't read a valid SHM header — fall back to process-local mxCommitFrame.
		// This happens when the SHM hasn't been initialized yet.
		hdr.mxFrame = w.index.mxCommitFrame.Load()
	}
	mxFrame := hdr.mxFrame

	// Step 2: Read nBackfill directly from SHM (SQLite: AtomicLoad(&pInfo->nBackfill))
	nBackfill := w.index.shmNBackfill()

	// Step 3: Slot 0 path — WAL fully checkpointed (SQLite wal.c:3114-3147)
	if mxFrame == 0 || nBackfill == mxFrame {
		if err := w.index.lock(lockRead0, lockShared); err != nil {
			return 0, 0, err
		}
		walShmBarrier()
		// Re-validate: compare live SHM header against our local copy
		// (SQLite wal.c:3125: memcmp(walIndexHdr(pWal), &pWal->hdr, sizeof(WalIndexHdr)))
		if liveHdr, ok := w.index.readHeader(); ok && liveHdr != hdr {
			_ = w.index.unlock(lockRead0, lockShared)
			return 0, 0, errWALRetry
		}
		w.index.shmWriteReadMark(0, mxFrame)
		w.index.aReadMark[0].Store(mxFrame) // keep process-local in sync
		return mxFrame, 0, nil
	}

	// Step 4: Find best reader slot 1-4 (SQLite wal.c:3154-3169)
	// Read aReadMark directly from SHM (SQLite: AtomicLoad(pInfo->aReadMark+i))
	bestSlot := -1
	bestMark := uint32(0)
	for i := 1; i <= 4; i++ {
		mark := w.index.shmReadMark(i)
		if mark != readMarkNotUsed && mark <= mxFrame && mark > bestMark {
			bestSlot = i
			bestMark = mark
		}
	}

	// Step 5: If no good slot or bestMark < mxFrame, try to claim an unused slot
	// and set its readmark to mxFrame (SQLite wal.c:3170-3185)
	if bestSlot == -1 || bestMark < mxFrame {
		for i := 1; i <= 4; i++ {
			lockSlot := lockRead0 + i
			if err := w.index.lock(lockSlot, lockExclusive); err == nil {
				w.index.shmWriteReadMark(i, mxFrame)
				w.index.aReadMark[i].Store(mxFrame) // keep process-local in sync
				bestMark = mxFrame
				bestSlot = i
				_ = w.index.unlock(lockSlot, lockExclusive)
				break
			}
		}
	}

	if bestSlot == -1 {
		return 0, 0, ErrBusy
	}

	// Step 6: Acquire shared lock on the chosen slot (SQLite wal.c:3192)
	lockSlot := lockRead0 + bestSlot
	if err := w.index.lock(lockSlot, lockShared); err != nil {
		return 0, 0, errWALRetry
	}

	// Step 7: Re-validate after lock (SQLite wal.c:3239-3249)
	// Read minFrame (nBackfill+1) from SHM, then barrier, then check header + readmark
	nBackfill = w.index.shmNBackfill()
	walShmBarrier()

	liveMark := w.index.shmReadMark(bestSlot)
	liveHdr, liveValid := w.index.readHeader()
	if liveMark != bestMark || (liveValid && liveHdr != hdr) {
		_ = w.index.unlock(lockSlot, lockShared)
		return 0, 0, errWALRetry
	}

	// Sync nBackfill to process-local for walIndex.get() minFrame filter
	w.index.nBackfill.Store(nBackfill)

	return mxFrame, bestSlot, nil
}

// saveReadSnapshot saves the current SHM header as the read snapshot for
// the BUSY_SNAPSHOT check in beginWrite. Must be called by the writer
// goroutine only (under writeMu), NOT from concurrent reader goroutines.
func (w *wal) saveReadSnapshot() {
	if w.inProcess || w.inMemory {
		return
	}
	if hdr, valid := w.index.readHeader(); valid {
		w.readSnapshot = hdr
	}
}

// endRead releases the reader lock for the given slot.
func (w *wal) endRead(slot int) {
	_ = w.index.unlock(lockRead0+slot, lockShared)
}

// beginWrite acquires the exclusive write lock.
// Uses the busy handler for retry/backoff if configured (issue 1.7).
//
// For multi-process mode, performs a BUSY_SNAPSHOT check after acquiring the
// lock: compares the saved SHM header snapshot (from tryBeginRead) against the
// current SHM header. If they differ, another process committed since our last
// read, so we reject the write to prevent stale-state corruption.
// Matches sqlite3WalBeginWriteTransaction (wal.c:3700-3714).
//
// Returns stateChanged=true if the WAL state was re-synced from SHM (indicating
// another process committed or checkpointed since the last local write).
// Callers should invalidate any stale page caches when stateChanged is true.
func (w *wal) beginWrite() (stateChanged bool, err error) {
	if err := walBusyLock(w.index, w.busyHandler, lockWrite, lockExclusive); err != nil {
		return false, err
	}
	if w.inProcess || w.inMemory {
		return false, nil
	}

	// BUSY_SNAPSHOT: compare saved snapshot against current SHM header.
	// Matches sqlite3WalBeginWriteTransaction (wal.c:3712):
	//   memcmp(&pWal->hdr, walIndexHdr(pWal), sizeof(WalIndexHdr))
	// Only check if readSnapshot was populated (isInit==1 from saveReadSnapshot);
	// raw wal.beginWrite() callers that skip saveReadSnapshot get isInit==0.
	hdr, valid := w.index.readHeader()
	if valid && w.readSnapshot.isInit != 0 && hdr != w.readSnapshot {
		_ = w.index.unlock(lockWrite, lockExclusive)
		return false, ErrBusySnapshot
	}

	// Re-sync WAL state from SHM header for writeFrames correctness.
	// DRIFT from SQLite: SQLite doesn't re-sync in beginWriteTransaction —
	// if headers match, pWal->hdr is already correct (because walIndexTryHdr
	// populated it). We re-sync because our WAL state (nFrame, cksum1/2,
	// salts) is separate from the SHM header struct. When BUSY_SNAPSHOT
	// passes (headers match), this is a no-op in practice.
	if valid {
		// Detect external state change: compare current SHM header against
		// the snapshot saved after our last writeFrames commit. writerHdr
		// reflects what WE wrote, so any difference means another process
		// committed or checkpointed since then (53f68eb fix).
		// When writerHdr.isInit==0 (first write or after restart), treat as
		// state-changed to force a full re-sync.
		if w.writerHdr.isInit != 0 && hdr != w.writerHdr {
			stateChanged = true
		} else if w.writerHdr.isInit == 0 {
			stateChanged = true
		}
		// If WAL was reset (mxFrame==0) or state changed externally, force
		// WAL header rewrite
		if hdr.mxFrame == 0 || stateChanged {
			w.headerOnDisk = false
		}
		w.nFrame.Store(hdr.mxFrame)
		w.cksum1 = hdr.aFrameCksum[0]
		w.cksum2 = hdr.aFrameCksum[1]
		w.header.salt1 = hdr.aSalt[0]
		w.header.salt2 = hdr.aSalt[1]
		w.index.mxCommitFrame.Store(hdr.mxFrame)
		// Monotonic update: same guard as tryBeginRead — prevent concurrent
		// reader from racing with the previous commit's writeHeader.
		for {
			old := w.index.maxPage.Load()
			if hdr.nPage <= old || w.index.maxPage.CompareAndSwap(old, hdr.nPage) {
				break
			}
		}
		// Snapshot the re-synced header so future beginWrite calls can
		// detect external changes relative to this baseline (53f68eb fix).
		w.writerHdr = hdr
	}

	return stateChanged, nil
}

// endWrite releases the exclusive write lock.
func (w *wal) endWrite() {
	_ = w.index.unlock(lockWrite, lockExclusive)
}

// checkpoint writes WAL frames back to the database file.
// For InMemory databases, master is used to store checkpointed pages instead of dbFile.
func (w *wal) checkpoint(dbFile fileHandle, master *masterStore) error {
	return w.checkpointWithMode(dbFile, master, CheckpointFull, nil)
}

// checkpointPassive performs a passive checkpoint that never blocks.
// Used by auto-checkpoint (issue 6.2) to avoid blocking writers or readers.
// Returns ErrBusy if not all frames were copied (partial checkpoint),
// matching SQLite's SQLITE_BUSY return from sqlite3WalCheckpoint in
// PASSIVE mode when readers block progress. Callers must not truncate
// the WAL when ErrBusy is returned.
func (w *wal) checkpointPassive(dbFile fileHandle, master *masterStore) error {
	err := w.checkpointWithMode(dbFile, master, CheckpointPassive, nil)
	if err != nil {
		return err
	}
	// Check if all frames were backfilled. A partial checkpoint means
	// some frames remain only in the WAL and must not be discarded.
	// Use mxCommitFrame (not maxFrame) so spilled uncommitted frames don't
	// prevent WAL reset. During active spill maxFrame > mxCommitFrame.
	complete := w.index.nBackfill.Load() >= w.index.mxCommitFrame.Load()
	if !complete {
		return ErrBusy
	}
	return nil
}

// checkpointWithMode writes WAL frames back to the database file using
// the specified checkpoint mode and optional busy handler.
//
// Checkpoint modes (issue 6.2):
//   - CheckpointPassive: never blocks, skips frames locked by readers.
//     Does NOT acquire the write lock. The busy handler is never invoked.
//   - CheckpointFull: acquires write lock to block new writers, uses busy
//     handler to wait for readers blocking progress.
//   - CheckpointRestart: like Full, but also resets the WAL.
//   - CheckpointTruncate: like Restart, but also truncates the WAL file.
//
// The busy handler (issue 1.7 + 6.3) is invoked when waiting for reader locks
// in FULL/RESTART/TRUNCATE modes. In PASSIVE mode it is never called.
func (w *wal) checkpointWithMode(dbFile fileHandle, master *masterStore, mode CheckpointMode, xBusy BusyHandler) error {
	// Acquire checkpoint lock -- serialize concurrent checkpoints.
	// The busy handler is NOT used for the checkpoint lock itself, matching
	// SQLite: "Even if there is a busy-handler configured, it will not be
	// invoked in this case."
	if err := w.index.lock(lockCheckpoint, lockExclusive); err != nil {
		return err
	}
	defer func() { _ = w.index.unlock(lockCheckpoint, lockExclusive) }()

	// In PASSIVE mode, do NOT acquire the write lock (don't block writers).
	// In FULL/RESTART/TRUNCATE modes, acquire the write lock using the busy handler.
	hasWriteLock := false
	if mode != CheckpointPassive {
		err := walBusyLock(w.index, xBusy, lockWrite, lockExclusive)
		if err == nil {
			hasWriteLock = true
		} else if err == ErrBusy {
			// If we can't get the write lock, downgrade to PASSIVE mode,
			// matching SQLite's behavior: "eMode2 = SQLITE_CHECKPOINT_PASSIVE"
			mode = CheckpointPassive
			xBusy = nil
		} else {
			return err
		}
	}
	if hasWriteLock {
		defer func() { _ = w.index.unlock(lockWrite, lockExclusive) }()
	}

	// PASSIVE mode never uses the busy handler (issue 6.3).
	if mode == CheckpointPassive {
		xBusy = nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Use mxCommitFrame (not nFrame) so spilled uncommitted frames are
	// excluded. nFrame may be ahead of mxCommitFrame during an active spill.
	// SQLite's walCheckpoint uses pWal->hdr.mxFrame which is only updated
	// at commit time via walIndexWriteHdr.
	nf := w.index.mxCommitFrame.Load()
	if nf == 0 {
		return nil
	}

	// Compute mxSafeFrame: the highest frame we can safely copy to DB.
	// Start with all committed frames, then lower based on active readers.
	//
	// For each reader slot (1-4), check its readmark. If the readmark
	// is below mxSafeFrame, try to acquire an exclusive lock on that slot.
	// If we get the lock, the slot is unused and we can clear its readmark.
	// If the lock fails (reader active), use the busy handler to wait
	// (in FULL/RESTART/TRUNCATE modes) or just lower mxSafeFrame (PASSIVE).
	// This matches SQLite's walCheckpoint() reader-lock loop (issue 6.3).
	mxSafeFrame := nf

	multiProcess := !w.inProcess && !w.inMemory
	for i := 1; i < 5; i++ {
		var mark uint32
		if multiProcess {
			mark = w.index.shmReadMark(i)
		} else {
			mark = w.index.aReadMark[i].Load()
		}

		if mark == readMarkNotUsed || mark >= mxSafeFrame {
			continue
		}

		// This reader's mark is less than mxSafeFrame, which would block us.
		// Try to acquire exclusive lock on this reader slot.
		lockSlot := lockRead0 + i
		rc := walBusyLock(w.index, xBusy, lockSlot, lockExclusive)
		if rc == nil {
			// Got the lock -- no reader on this slot anymore.
			// Reset the readmark: slot 1 gets mxSafeFrame, others get NOT_USED.
			// This matches SQLite: "iMark = (i==1 ? mxSafeFrame : READMARK_NOT_USED)"
			if i == 1 {
				w.index.aReadMark[i].Store(mxSafeFrame)
				if multiProcess {
					w.index.shmWriteReadMark(i, mxSafeFrame)
				}
			} else {
				w.index.aReadMark[i].Store(readMarkNotUsed)
				if multiProcess {
					w.index.shmWriteReadMark(i, readMarkNotUsed)
				}
			}
			_ = w.index.unlock(lockSlot, lockExclusive)
		} else if rc == ErrBusy {
			// Reader still active -- lower mxSafeFrame to this reader's mark.
			mxSafeFrame = mark
			// After hitting a busy reader, disable the busy handler for
			// subsequent slots to avoid indefinite waiting, matching SQLite:
			// "xBusy = 0" after SQLITE_BUSY in the reader loop.
			xBusy = nil
		} else {
			return rc
		}
	}

	// nBackfill is the number of frames already copied to DB
	var nBackfill uint32
	if multiProcess {
		nBackfill = w.index.shmNBackfill()
	} else {
		nBackfill = w.index.nBackfill.Load()
	}

	if mxSafeFrame <= nBackfill {
		// Nothing new to checkpoint.
		// For non-PASSIVE modes, check if we need to wait for readers
		// to finish before attempting RESTART/TRUNCATE.
		return w.checkpointPost(mode, xBusy)
	}

	// Acquire exclusive lock on reader slot 0 before backfilling,
	// matching SQLite's walCheckpoint() which calls
	// walBusyLock(pWal, xBusy, pBusyArg, WAL_READ_LOCK(0), 1).
	lockErr := walBusyLock(w.index, xBusy, lockRead0, lockExclusive)
	if lockErr == ErrBusy {
		// Reader on slot 0 active; can't backfill but not a fatal error.
		return w.checkpointPost(mode, xBusy)
	}
	if lockErr != nil {
		return lockErr
	}

	// Set nBackfillAttempted BEFORE starting the backfill (issue 7.7).
	// This provides a crash-safety hint: if we crash during backfill,
	// recovery knows that frames up to nBackfillAttempted may have been
	// partially written to the database.
	w.index.nBackfillAttempted.Store(mxSafeFrame)
	w.index.shmWriteCkptInfo()

	// Sync the WAL file before copying frames to the database, matching
	// SQLite's walCheckpoint() (CKPT_SYNC_FLAGS). This ensures all WAL data
	// is durable on disk before we start overwriting the database file with it.
	// Always sync regardless of noCommitSync: noCommitSync only skips per-commit
	// syncs (like SQLite synchronous=NORMAL), but checkpoint always syncs.
	if w.file != nil {
		if err := fdatasync(w.file); err != nil {
			_ = w.index.unlock(lockRead0, lockExclusive)
			return err
		}
	}

	// Copy frames (nBackfill+1)..mxSafeFrame to DB
	pageSz := int64(w.pageSize)
	var backfillErr error

	if w.inMemory {
		for i := nBackfill; i < mxSafeFrame; i++ {
			mf := &w.memFrames[i]
			if dbFile != nil {
				// Disk mode: write to DB file
				pageOffset := int64(mf.pgno-1) * pageSz
				n, err := dbFile.WriteAt(mf.data, pageOffset)
				if err != nil {
					backfillErr = err
					break
				}
				if n != len(mf.data) {
					backfillErr = io.ErrShortWrite
					break
				}
			} else if master != nil {
				// InMemory mode: write to masterStore
				master.writePage(mf.pgno, mf.data)
			}
		}
	} else {
		if w.file == nil {
			_ = w.index.unlock(lockRead0, lockExclusive)
			return errors.New("btree: WAL file closed")
		}
		frameSize := int64(walFrameSize) + int64(w.pageSize)
		var frame walFrame
		frameBuf := make([]byte, walFrameSize)
		// Reuse w.ckptBuf for page data reads, avoiding per-checkpoint allocations.
		// Matches SQLite's walCheckpoint (wal.c:2285-2304) which reuses the
		// pager's pTmpSpace buffer across the entire backfill loop.
		if w.ckptBuf == nil {
			w.ckptBuf = make([]byte, w.pageSize)
		}
		pageData := w.ckptBuf

		for i := nBackfill; i < mxSafeFrame; i++ {
			off := int64(walHeaderSize) + int64(i)*frameSize
			n, err := w.file.ReadAt(frameBuf, off)
			if err != nil {
				backfillErr = err
				break
			}
			if n != len(frameBuf) {
				backfillErr = io.ErrUnexpectedEOF
				break
			}
			frame.deserialize(frameBuf)

			n, err = w.file.ReadAt(pageData, off+walFrameSize)
			if err != nil {
				backfillErr = err
				break
			}
			if n != len(pageData) {
				backfillErr = io.ErrUnexpectedEOF
				break
			}

			pageOffset := int64(frame.pgno-1) * pageSz
			n, err = dbFile.WriteAt(pageData, pageOffset)
			if err != nil {
				backfillErr = err
				break
			}
			if n != len(pageData) {
				backfillErr = io.ErrShortWrite
				break
			}
		}
	}

	if backfillErr != nil {
		_ = w.index.unlock(lockRead0, lockExclusive)
		return backfillErr
	}

	// Sync the database file (skip for InMemory)
	if dbFile != nil {
		if err := fdatasync(dbFile); err != nil {
			_ = w.index.unlock(lockRead0, lockExclusive)
			return err
		}
	}

	// Update nBackfill
	w.index.nBackfill.Store(mxSafeFrame)
	w.index.shmWriteCkptInfo()

	// Release the reader lock held while backfilling
	_ = w.index.unlock(lockRead0, lockExclusive)

	return w.checkpointPost(mode, xBusy)
}

// checkpointPost handles post-backfill logic: WAL reset for modes that
// completed a full checkpoint.
func (w *wal) checkpointPost(mode CheckpointMode, xBusy BusyHandler) error {
	backfill := w.index.nBackfill.Load()

	// Use mxCommitFrame (not nFrame) so spilled uncommitted frames don't
	// prevent WAL reset when all committed frames are checkpointed.
	if backfill < w.index.mxCommitFrame.Load() {
		// Not everything was checkpointed. Can't reset the WAL.
		return nil
	}

	// All frames are checkpointed. Only RESTART/TRUNCATE modes reset the WAL.
	// FULL mode preserves the WAL as a crash-safety net: if a subsequent
	// checkpoint partially writes pages to the DB and SIGKILL hits mid-backfill,
	// the WAL data is still available for recovery.
	if mode >= CheckpointRestart {
		return w.tryResetWALWithBusy(xBusy, mode == CheckpointTruncate)
	}

	return nil
}

// tryResetWALWithBusy attempts to reset the WAL, using the busy handler
// to wait for reader locks on slots 1-4 (issue 6.3).
// Used by CheckpointRestart and CheckpointTruncate modes.
// The truncate parameter controls whether to truncate the WAL file to zero
// (CheckpointTruncate) or just reset it (CheckpointRestart).
func (w *wal) tryResetWALWithBusy(xBusy BusyHandler, truncate bool) error {
	// Try to acquire exclusive locks on all reader slots 1-4,
	// using the busy handler. Matching SQLite:
	// "rc = walBusyLock(pWal, xBusy, pBusyArg, WAL_READ_LOCK(1), WAL_NREADER-1)"
	//
	// SQLite locks slots 1..4 as a single range. We lock them individually
	// since our shm interface works per-slot.
	for i := 1; i <= 4; i++ {
		lockSlot := lockRead0 + i
		if err := walBusyLock(w.index, xBusy, lockSlot, lockExclusive); err != nil {
			// Release any locks we already acquired
			for j := 1; j < i; j++ {
				_ = w.index.unlock(lockRead0+j, lockExclusive)
			}
			if err == ErrBusy {
				return nil // can't reset, but not fatal
			}
			return err
		}
	}
	// Release all reader locks after reset
	defer func() {
		for i := 1; i <= 4; i++ {
			_ = w.index.unlock(lockRead0+i, lockExclusive)
		}
	}()

	return w.doResetWAL(truncate)
}

// doResetWAL performs the actual WAL reset.
// Must be called with w.mu held and all necessary locks acquired.
// If truncate is true, the WAL file is truncated to zero bytes (CheckpointTruncate).
// If false, the WAL is just restarted with a new header (CheckpointRestart).
func (w *wal) doResetWAL(truncate bool) error {
	if debugTrace {
		trace("doResetWAL: truncate=%v nFrame=%d maxFrame=%d nBackfill=%d",
			truncate, w.nFrame.Load(), w.index.maxFrame.Load(), w.index.nBackfill.Load())
	}
	// Reset metadata BEFORE truncating the file. This ensures concurrent
	// readers see nFrame=0 / empty pageMap before the file shrinks,
	// preventing TOCTOU races where readFrame passes the nFrame check
	// but gets EOF from the truncated file. Matches SQLite's ordering:
	// walRestartHdr (reset header) → sqlite3OsTruncate (wal.c:2363-2364).
	w.index.reset()
	w.nFrame.Store(0)

	if truncate && w.file != nil {
		// CheckpointTruncate: truncate WAL file to zero bytes
		if err := w.file.Truncate(0); err != nil {
			return err
		}
	}

	// Reset memFrames and arena for reuse
	if w.inMemory {
		w.memFrames = w.memFrames[:0]
		w.memArenaOff = 0
	}

	if w.inMemory {
		// InMemory: just reinitialize header state, no disk write
		w.initHeaderState()
		return nil
	}

	return w.writeHeader()
}

// truncateFile truncates the WAL file to zero bytes under the WAL mutex.
func (w *wal) truncateFile() {
	w.mu.Lock()
	if w.file != nil {
		_ = w.file.Truncate(0)
	}
	w.mu.Unlock()
}

// close closes the WAL file and shared memory.
func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	if w.index != nil {
		if err := w.index.close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.file = nil
	}
	return firstErr
}

// walFrameChecksum computes a frame's checksum for validation (exported for testing).
func walFrameChecksum(headerFirst8 []byte, pageData []byte, prevS1, prevS2 uint32) (uint32, uint32) {
	s1, s2 := walChecksum(headerFirst8, prevS1, prevS2)
	s1, s2 = walChecksum(pageData, s1, s2)
	return s1, s2
}

// walPageChecksum computes a simple checksum of page data.
func walPageChecksum(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
