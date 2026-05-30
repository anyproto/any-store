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
// Frame lookup — in-process map vs. SHM hash (issue 7.9):
//
// walIndex.get()/getLatest() select their source of truth based on mode:
//
//  1. InProcess mode (single-process, heap shm): the in-process Go map
//     (pageMap) is the sole source of truth. There is no mmap'd SHM to
//     consult, and the Go map gives O(1) lookup without the linear-probing
//     overhead of the hash table.
//  2. Multi-process mode (mmap'd shm): get()/getLatest() consult the SHM
//     hash tables exclusively via shmHashGet(), matching SQLite's
//     walFindFrame (wal.c:3554-3582). A peer process may have written WAL
//     frames that never touched this process's pageMap, so the shared SHM
//     hash — populated on every frame by shmHashWrite — is authoritative.
//     nBackfill is likewise read from SHM (shmNBackfill) since a peer
//     checkpoint may advance it without updating our process-local copy.
//
// In SQLite, walHashGet/walFramePage are used during recovery, checkpoint
// iteration, AND normal cross-process reads (all frame lookups go through
// the shared SHM). Our pageMap mirrors SQLite's in-memory state only for the
// InProcess fast path; cross-process readers use the SHM hash directly.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/bits"
	"math/rand/v2"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// DiagReuseFrames and DiagAppendFrames are diagnostic counters incremented
// from writeFrames. Used by benchmarks to verify the in-tx frame-reuse path
// fires under a given workload. Cheap atomic adds; lifetime totals.
//
// DiagDisableReuse, when true, skips the in-tx frame-reuse branch in
// writeFrames so benchmarks can A/B-compare against the no-reuse baseline
// without checking out a separate revision.
var (
	DiagReuseFrames  atomic.Uint64
	DiagAppendFrames atomic.Uint64
	DiagDisableReuse atomic.Bool
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
// DRIFT: busy-handler retry count resets per walBusyLock call vs C per-connection nBusy See docs/btree/NOTES.md#drift-108-busy-handler-retry-count-resets-per-call
// DRIFT: walBusyLock locks single slot; C locks n consecutive slots atomically See docs/btree/NOTES.md#drift-109-walbusylock-locks-single-slot-not-n-consecutive
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

	// Validate version — matches SQLite walIndexRecover (wal.c:1406-1410).
	// A mismatch means the WAL was written by an incompatible any-store
	// version; continuing would replay frames we don't know how to read.
	if wh.version != walVersion {
		return ErrWALCorrupt
	}

	// Validate page size — matches SQLite walIndexRecover (wal.c:1414-1419).
	// Must be a power of two within [MinPageSize, MaxPageSize]. A corrupted
	// value would mislead recovery's frame-size arithmetic and could cause
	// out-of-bounds allocation.
	if wh.pageSize < MinPageSize || wh.pageSize > MaxPageSize || wh.pageSize&(wh.pageSize-1) != 0 {
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

// errWALRetry is an internal sentinel signaling that the caller should retry.
// Used by (a) tryBeginRead when SHM state changed between metadata read and
// lock acquisition (beginRead's 5000-attempt loop consumes it), and (b) the
// ensureHeaderInitialized helper when a peer holds a blocking lock and the
// operation must be re-attempted. Callers outside beginRead must implement
// their own retry loop or propagate as BUSY-equivalent.
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
	// DRIFT from SQLite (docs/btree/NOTES.md §20, drifts 2-3): SQLite has no process-local
	// copies of mxCommitFrame, nBackfill, or aReadMark. These values live ONLY
	// in the mmap'd SHM region (via volatile WalCkptInfo* pointer). We maintain
	// process-local atomic.Uint32 copies alongside SHM because:
	//   - In-process mode (heapShm) has no mmap'd region — process-local atomics
	//     ARE the single source of truth.
	//   - walIndex.get() reads nBackfill.Load()+1 as minFrame — this code path is
	//     shared by in-process and multi-process modes.
	//   - Multi-process tryBeginReadMultiProcess reads from SHM (shmNBackfill,
	//     shmReadMark) then syncs nBackfill to process-local at the end for get().
	mu       sync.RWMutex
	shm      shm                 // platform-specific shared memory
	pageMap  map[uint32][]uint32 // pgno -> sorted list of frame indices (1-based)
	maxFrame atomic.Uint32       // highest valid frame (committed + spilled)
	// mxCommitFrame is the process-local commit-frame cursor. Do NOT read
	// via bare .Load — the compiler blocks that. For local reads use
	// mxCommitFrame.LoadLocal(); for cross-process authoritative reads
	// use (*wal).authoritativeMxFrame(). See mxframe.go for rationale.
	mxCommitFrame commitFrameCounter
	maxPage       atomic.Uint32 // database size at last commit
	nBackfill     atomic.Uint32 // frames already checkpointed

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
// In-process mode writes the process-local pageMap (which is the sole source
// of truth when there is no multi-process SHM to consult). Multi-process mode
// relies on the SHM hash written below — matching SQLite's walFrames →
// walIndexAppend (wal.c:2900, 1295-1338) where SHM hash is the authoritative
// page→frame index.
// DRIFT: shmHash write/get drop walIndexAppend/walFindFrame nCollide CORRUPT + cleanup/zero-init See docs/btree/NOTES.md#drift-91-wal-index-hash-append-missing-collision-limit-corruption-det
func (wi *walIndex) set(pgno, frame uint32) {
	if wi.inProcess {
		wi.mu.Lock()
		frames := wi.pageMap[pgno]
		if frames == nil {
			frames = make([]uint32, 0, 4)
		}
		wi.pageMap[pgno] = append(frames, frame)
		wi.mu.Unlock()
	}
	if frame > wi.maxFrame.Load() {
		wi.maxFrame.Store(frame)
	}
	wi.shmHashWrite(pgno, frame)
}

// setBatch records multiple page->frame mappings under a single lock and
// publishes them to the SHM hash tables. Matches SQLite's walFrames loop
// (wal.c:2900-ish) calling walIndexAppend (wal.c:1295-1338) for every frame
// regardless of commit: peer readers are still gated by hdr.mxFrame (only
// advanced on commit via walIndexWriteHdr), so uncommitted hash entries are
// present but unreachable. Rollback uses shmCleanupFromFrame (analog of
// walCleanupHash, wal.c:1247-1282) to zero out dangling hash entries.
// DRIFT: shmHash write/get drop walIndexAppend/walFindFrame nCollide CORRUPT + cleanup/zero-init See docs/btree/NOTES.md#drift-91-wal-index-hash-append-missing-collision-limit-corruption-det
func (wi *walIndex) setBatch(pages []*page, startFrame uint32, commit bool) {
	_ = commit
	if wi.inProcess {
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
	}
	if f := startFrame + uint32(len(pages)) - 1; f > wi.maxFrame.Load() {
		wi.maxFrame.Store(f)
	}
	for i, p := range pages {
		wi.shmHashWrite(p.pgno, startFrame+uint32(i))
	}
}

// rollbackToFrame removes all pageMap entries with frame > target and restores
// maxFrame to target. Also cleans up SHM hash entries for the rolled-back
// frames — matches SQLite's walCleanupHash (wal.c:1247-1282) invoked on
// savepoint rollback from sqlite3WalSavepointUndo.
// Called under w.index exclusive write lock.
// DRIFT: shmCleanupFromFrame zeros all segments above target; C zeros only mxFrame's segment See docs/btree/NOTES.md#drift-95-shmcleanupfromframe-zeros-all-segments-above-target
func (wi *walIndex) rollbackToFrame(target uint32) {
	oldMax := wi.maxFrame.Load()
	if wi.inProcess {
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
	}
	wi.maxFrame.Store(target)
	if oldMax > target {
		wi.shmCleanupFromFrame(target, oldMax)
	}
}

// resetIReCksumIfPast clears w.iReCksum if it refers to a frame that
// has been rolled back past newMxFrame. Mirrors SQLite wal.c:3832-3834
// in sqlite3WalSavepointUndo.
func (w *wal) resetIReCksumIfPast(newMxFrame uint32) {
	if w.iReCksum > newMxFrame {
		w.iReCksum = 0
	}
}

// shmCleanupFromFrame zeros out SHM hash entries for frames in (target, maxFrame].
// Caller must hold lockWrite exclusively so no peer can read or write SHM hash
// concurrently — this matches SQLite's walCleanupHash invariant (wal.c:1236,
// "assert( pWal->writeLock )").
//
// For the segment containing target+1: SQLite's selective approach —
// `if aHash[i] > iLimit then aHash[i] = 0`, then `memset(&aPgno[iLimit], 0, …)`
// (wal.c:1258-1271). This preserves linear-probe chains for the idx <= iLimit
// entries that remain valid.
//
// For strictly-later segments: zero the full region body (aPgno + aHash).
// SQLite does not need this step because `walIndexAppend` zero-inits a new
// segment on its first write (wal.c:1315-1319). any-store's setBatch does not
// have that zero-init step, so trailing segments would retain dangling entries
// from the rolled-back writes without this cleanup.
func (wi *walIndex) shmCleanupFromFrame(target, maxFrame uint32) {
	if maxFrame <= target {
		return
	}
	startSeg, _ := htFrameSegIdx(target + 1)
	endSeg, _ := htFrameSegIdx(maxFrame)
	for seg := startSeg; seg <= endSeg; seg++ {
		region, err := wi.shm.region(seg, false)
		if err != nil {
			continue
		}
		pgnoBase, nEntry, iZero := htSegmentInfo(seg)
		if target <= iZero {
			// Trailing segment: every frame indexed here is > target. Zero both
			// aPgno and aHash wholesale. The preceding `target == iZero` case
			// means this segment's aPgno[0] == frame (iZero+1) > target.
			for i := 0; i < nEntry; i++ {
				binary.LittleEndian.PutUint32(region[pgnoBase+i*4:], 0)
			}
			clear(region[htHashArrayOff : htHashArrayOff+htNSlot*2])
			continue
		}
		// SQLite's walCleanupHash: selectively zero slots whose indexed frame
		// is beyond iLimit, then memset the tail of aPgno.
		iLimit := uint16(target - iZero) // target's idx within this segment, 1-based
		for h := 0; h < htNSlot; h++ {
			slotOff := htHashArrayOff + h*2
			v := binary.LittleEndian.Uint16(region[slotOff:])
			if v > iLimit {
				binary.LittleEndian.PutUint16(region[slotOff:], 0)
			}
		}
		for i := int(iLimit); i < nEntry; i++ {
			binary.LittleEndian.PutUint32(region[pgnoBase+i*4:], 0)
		}
	}
}

// getInTxRange returns the frame for pgno within [minFrame, maxFrame],
// or 0 if none exists. Used by the frame-reuse path in writeFrames to
// locate an existing frame from the current transaction's range. Direct
// analog of SQLite's walFindFrame with a caller-supplied minFrame
// (wal.c:3554 uses pWal->minFrame; we let the caller pass iFirst).
//
// In-process mode walks pageMap; multi-process mode consults the SHM
// hash via shmHashGet.
func (wi *walIndex) getInTxRange(pgno, maxFrame, minFrame uint32) uint32 {
	if wi.inProcess {
		wi.mu.RLock()
		frames := wi.pageMap[pgno]
		wi.mu.RUnlock()
		for i := len(frames) - 1; i >= 0; i-- {
			if frames[i] <= maxFrame && frames[i] >= minFrame {
				return frames[i]
			}
		}
		return 0
	}
	return wi.shmHashGet(pgno, maxFrame, minFrame)
}

// get returns the frame containing the latest version of pgno that is
// within the given maxFrame snapshot, or 0 if not in WAL.
// The maxFrame parameter limits which frames are visible (for snapshot isolation).
// DRIFT: WAL hash-probe full-chain (nCollide) CORRUPT signal dropped in get/shmHashGet See docs/btree/NOTES.md#drift-93-wal-hash-probe-full-chain-corruption-signal-dropped
func (wi *walIndex) get(pgno, maxFrame uint32) uint32 {
	if wi.inProcess {
		// In-process: no SHM to consult; pageMap is the sole source of truth.
		// minFrame filters out frames already checkpointed back to the DB
		// (SQLite wal.c:3571).
		minFrame := wi.nBackfill.Load() + 1
		wi.mu.RLock()
		frames := wi.pageMap[pgno]
		wi.mu.RUnlock()
		for i := len(frames) - 1; i >= 0; i-- {
			if frames[i] <= maxFrame && frames[i] >= minFrame {
				return frames[i]
			}
		}
		return 0
	}
	// Multi-process: SHM hash tables are the sole source of truth, matching
	// SQLite's walFindFrame (wal.c:3554-3582). nBackfill lives in SHM
	// (wal.c:3114, 3571) since a peer checkpoint may advance it without
	// touching our process-local copy — use shmNBackfill() rather than
	// the cached atomic.
	minFrame := wi.shmNBackfill() + 1
	return wi.shmHashGet(pgno, maxFrame, minFrame)
}

// getLatest returns the latest WAL frame for pgno, or 0 if the page is not
// in the WAL. Used to detect whether a cached page may have been updated
// beyond a reader's snapshot. The caller compares the result against its
// walMaxFrame (which is bounded by mxCommitFrame from beginRead) to decide
// whether the cached page is still valid.
//
// In multi-process mode this consults SHM hash exclusively (the SQLite-
// aligned source of truth per walFindFrame). In in-process mode it walks
// pageMap — there is no SHM to query in that mode.
func (wi *walIndex) getLatest(pgno uint32) uint32 {
	if wi.inProcess {
		wi.mu.RLock()
		frames := wi.pageMap[pgno]
		wi.mu.RUnlock()
		if len(frames) > 0 {
			return frames[len(frames)-1]
		}
		return 0
	}
	// Multi-process: SHM hash only. Use mxCommitFrame as upper bound so
	// cross-process readers never observe uncommitted spill frames (readers
	// gate on hdr.mxFrame / mxCommitFrame, not raw hash entries).
	minFrame := wi.shmNBackfill() + 1
	return wi.shmHashGet(pgno, wi.mxCommitFrame.LoadLocal(), minFrame)
}

// reset clears the WAL index (after a checkpoint + WAL truncate).
// DRIFT: reset() clobbers aReadMark[0]/[1] to NOT_USED + drops nCkpt/salt increments See docs/btree/NOTES.md#drift-99-wal-restart-read-mark-reset-diverges-from-walrestarthdr
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
// DRIFT: wal-index header iChange counter never incremented (always 0) See docs/btree/NOTES.md#drift-96-wal-index-change-counter-ichange-never-incremented
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

	// Barrier before publishing the header: ensures all prior SHM hash-slot
	// writes (shmHashWrite / flushPendingShmFrames) are visible to peer
	// processes before they observe the new mxFrame in the header. Without
	// this fence, on weakly-ordered arches a reader can see the new header
	// but stale hash slots → shmHashGet returns 0 for an indexed page.
	// Matches SQLite's per-slot AtomicStore semantics in walIndexAppend
	// (wal.c:1341) + the walIndexWriteHdr ordering contract.
	if !wi.inProcess {
		walShmBarrier()
	}

	// Write copy 2 first (offset walIndexHdrSize = 48), then barrier, then
	// copy 1. Matches SQLite's walIndexWriteHdr (wal.c:942-954) which uses
	// a plain memcpy per copy under WAL_WRITE_LOCK: the dual-copy + memcmp
	// in readers + the aCksum covered by writers both guard against torn
	// reads. Per-word atomic stores are unnecessary here (the writer holds
	// an exclusive lock) and subtly wrong — a reader's atomic load could
	// observe mixed-version u32s whose checksum happens to match both
	// generations at the interleave.
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
// DRIFT: wal-index szPage neither encoded nor decoded (szPage transform absent) See docs/btree/NOTES.md#drift-90-wal-index-szpage-field-not-encoded-or-decoded
func (wi *walIndex) readHeader() (WalIndexHdr, bool) {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return WalIndexHdr{}, false
	}

	if len(region) < walIndexHdrSize*2 {
		return WalIndexHdr{}, false
	}

	// Read copy 1 (offset 0). Plain copy under readHeader's dual-copy +
	// memcmp protocol, matching walIndexTryHdr (wal.c:2570-2620). The
	// subsequent memcmp against copy 2 is the torn-read detector; per-word
	// atomic loads add no safety and defeat the design.
	var hdr1 WalIndexHdr
	var buf1 [walIndexHdrSize]byte
	copy(buf1[:], region[0:walIndexHdrSize])
	hdr1.deserialize(buf1[:])

	// Memory barrier before reading copy 2
	if !wi.inProcess {
		walShmBarrier()
	}

	// Read copy 2 (offset walIndexHdrSize = 48)
	var hdr2 WalIndexHdr
	var buf2 [walIndexHdrSize]byte
	copy(buf2[:], region[walIndexHdrSize:walIndexHdrSize*2])
	hdr2.deserialize(buf2[:])

	// Both copies must match
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

	// Reject a SHM wal-index header written with an incompatible/future
	// format version. Matches SQLite's walIndexReadHdr gate
	// (wal.c:2740-2742: iVersion != WALINDEX_MAX_VERSION -> SQLITE_CANTOPEN).
	// any-store uses its own walVersion (1000000) instead of 3007000.
	if hdr1.iVersion != walVersion {
		return WalIndexHdr{}, false
	}

	return hdr1, true
}

// walShmBarrier issues a full memory barrier for cross-process mmap'd memory.
// On x86/amd64, stores are already ordered (TSO), but we need a compiler barrier
// to prevent the Go compiler from reordering. On ARM64, we need a full fence.
// This matches SQLite's walShmBarrier() / sqlite3OsShmBarrier().
func walShmBarrier() {
	// atomic.SwapUint32 on a dummy variable acts as both a compiler barrier
	// and a full bidirectional memory fence. Unlike atomic.StoreUint32 (which
	// lowers to a one-way store-release STLRW on ARM64), the atomic
	// read-modify-write lowers to SWPALW (or LDAXRW+STLXRW on pre-LSE) on ARM64
	// and XCHGL (implicit LOCK prefix) on amd64, both providing the full
	// LoadLoad+LoadStore+StoreLoad+StoreStore ordering SQLite requires. This is
	// the sole mechanism providing cross-process publish ordering, matching
	// SQLite's sqlite3MemoryBarrier (mutex_unix.c:98-104) = pure
	// __sync_synchronize() (= DMB ISH on ARM64), which performs no scheduler
	// yield. SQLite's redundant unixEnterMutex/unixLeaveMutex fallback is only
	// needed when sqlite3MemoryBarrier() compiles to a no-op; Go's atomic RMW
	// never degrades to a no-op, so the fallback is intentionally dropped.
	var dummy uint32
	atomic.SwapUint32(&dummy, 0)
}

// lock acquires a shm lock.
func (wi *walIndex) lock(slot int, lockType int) error {
	return wi.shm.lock(slot, lockType)
}

// unlock releases a shm lock.
func (wi *walIndex) unlock(slot int, lockType int) error {
	return wi.shm.unlock(slot, lockType)
}

// close closes the shm. If isLastClient is true the backing shm file is unlinked.
func (wi *walIndex) close(isLastClient bool) error {
	if wi.shm != nil {
		return wi.shm.close(isLastClient)
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
// DRIFT: htSegmentInfo adds nEntry bound replacing C's index mask in hash lookup See docs/btree/NOTES.md#drift-94-htsegmentinfo-adds-nentry-bound-replacing-c-mask
func htSegmentInfo(seg int) (pgnoBase int, nEntry int, iZero uint32) {
	if seg == 0 {
		return htPgnoOff0, int(htNPageOne), 0
	}
	return 0, htNPage, uint32(htNPageOne + (seg-1)*htNPage)
}

// shmHashWrite records a page->frame mapping in the shm hash table.
// Matches SQLite's walIndexAppend (wal.c:1295-1338): writes aPgno[idx],
// barriers, then publishes the aHash[h] slot. Protected by WAL_WRITE_LOCK
// (any-store's lockWrite) so linear probing does not race another writer.
// The write ordering + barrier guarantees cross-process readers either see
// a fully-populated entry or none at all.
// DRIFT: shmHash write/get drop walIndexAppend/walFindFrame nCollide CORRUPT + cleanup/zero-init See docs/btree/NOTES.md#drift-91-wal-index-hash-append-missing-collision-limit-corruption-det
func (wi *walIndex) shmHashWrite(pgno, frame uint32) {
	seg, idx := htFrameSegIdx(frame)

	region, err := wi.shm.region(seg, true)
	if err != nil {
		return
	}

	// Write pgno to aPgno[idx] first (SQLite wal.c:1337). u32-aligned.
	pgnoOff := htPgnoOffset(seg, idx)
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&region[pgnoOff])), pgno)

	// Release barrier between aPgno and the hash slot publish, matching
	// the implicit release on SQLite's AtomicStore(aHash, idx) at wal.c:1338.
	if !wi.inProcess {
		walShmBarrier()
	}

	// Insert into hash table (linear probing). aHash entries are u16; we
	// publish via a plain store here because writes are serialized under
	// lockWrite and the preceding barrier ordered aPgno before us.
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
// DRIFT: shmHash write/get drop walIndexAppend/walFindFrame nCollide CORRUPT + cleanup/zero-init See docs/btree/NOTES.md#drift-91-wal-index-hash-append-missing-collision-limit-corruption-det
// DRIFT: shmHashGet silently skips segment on SHM map/IO error vs C abort See docs/btree/NOTES.md#drift-92-shmhashget-skips-segment-on-map-io-error
func (wi *walIndex) shmHashGet(pgno, maxFrame, minFrame uint32) uint32 {
	if maxFrame == 0 {
		return 0
	}

	lastSeg, _ := htFrameSegIdx(maxFrame)
	// Match SQLite wal.c:3554 — skip segments before minFrame.
	minSeg, _ := htFrameSegIdx(minFrame)

	for seg := lastSeg; seg >= minSeg; seg-- {
		// Map segment on demand (create=true). A peer writer may have just
		// extended SHM with a new segment containing frames <= maxFrame that
		// this reader hasn't yet mapped. SQLite's walFindFrame (wal.c:3562)
		// always calls walHashGet → walIndexPage → sqlite3OsShmMap, which
		// maps/extends the wal-index page — it never silently skips a
		// segment. Skipping here was the root cause of cross-process
		// row-loss: hdr.mxFrame advertised frames in a segment we hadn't
		// mapped, shmHashGet returned 0, and the read path fell through to
		// the DB file which still held the pre-commit page.
		region, err := wi.shm.region(seg, true)
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
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&region[htCkptOff])), wi.nBackfill.Load())
	for i := range 5 {
		atomic.StoreUint32((*uint32)(unsafe.Pointer(&region[htReadMarkOff+i*4])), wi.aReadMark[i].Load())
	}
	// Write nBackfillAttempted at offset 128 (issue 7.7)
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&region[htNBackfillAttemptedOff])), wi.nBackfillAttempted.Load())
}

// shmReadCkptInfo reads checkpoint info (nBackfill, aReadMark, nBackfillAttempted)
// from shm region 0 into process-local atomics.
//
// WARNING: This function bulk-copies SHM values into shared process-local atomics.
// It must NOT be called from concurrent reader goroutines — multiple goroutines
// calling this concurrently will clobber each other's values, causing stale
// snapshot reads. Use shmNBackfill/shmReadMark for per-call SHM reads instead.
// This function is retained for use in tests and single-goroutine contexts only.
// See docs/btree/NOTES.md §20, drift 6.
func (wi *walIndex) shmReadCkptInfo() {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return
	}
	wi.nBackfill.Store(atomic.LoadUint32((*uint32)(unsafe.Pointer(&region[htCkptOff]))))
	for i := range 5 {
		wi.aReadMark[i].Store(atomic.LoadUint32((*uint32)(unsafe.Pointer(&region[htReadMarkOff+i*4]))))
	}
	// Read nBackfillAttempted at offset 128 (issue 7.7)
	wi.nBackfillAttempted.Store(atomic.LoadUint32((*uint32)(unsafe.Pointer(&region[htNBackfillAttemptedOff]))))
}

// shmNBackfill reads nBackfill directly from SHM region 0 without updating
// process-local atomics. Matches SQLite's AtomicLoad(&pInfo->nBackfill) pattern
// where nBackfill is always read from shared memory, not from a per-connection copy.
func (wi *walIndex) shmNBackfill() uint32 {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return 0
	}
	return atomic.LoadUint32((*uint32)(unsafe.Pointer(&region[htCkptOff])))
}

// shmReadMark reads a single aReadMark[i] directly from SHM region 0 without
// updating process-local atomics. Matches SQLite's AtomicLoad(pInfo->aReadMark+i).
func (wi *walIndex) shmReadMark(i int) uint32 {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return readMarkNotUsed
	}
	return atomic.LoadUint32((*uint32)(unsafe.Pointer(&region[htReadMarkOff+i*4])))
}

// shmWriteReadMark writes a single aReadMark[i] directly to SHM region 0.
// Matches SQLite's AtomicStore(pInfo->aReadMark+i, mxFrame).
func (wi *walIndex) shmWriteReadMark(i int, val uint32) {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return
	}
	atomic.StoreUint32((*uint32)(unsafe.Pointer(&region[htReadMarkOff+i*4])), val)
}

// wal manages the Write-Ahead Log.
type wal struct {
	mu       sync.RWMutex // protects memFrames slice; readers use RLock, writer uses Lock
	file     fileHandle
	header   walHeader
	index    *walIndex
	pageSize uint32
	path     string
	nFrame   atomic.Uint32 // total frames written (atomic: read by readFrame, written by writeFrames)

	// Cumulative checksum state for appending frames
	cksum1 uint32
	cksum2 uint32

	// ckptBuf is a reusable page-sized buffer for reading WAL frames during
	// checkpoint backfill. Allocated lazily on first checkpoint. Matches
	// SQLite's use of pTmpSpace in walCheckpoint (wal.c:2285-2304), though
	// SQLite passes pTmpSpace from the pager rather than storing it on the WAL.
	ckptBuf []byte

	// ckptLatest and ckptPgnos are scratch for the checkpoint dedup pass
	// (buildBackfillMap + pgno-sorted iterate). Reused across checkpoints
	// because only one checkpoint runs at a time (lockCheckpoint exclusive).
	// Clear-and-reuse avoids the per-checkpoint map/slice allocation that
	// otherwise shows up in BatchUpdate benches.
	ckptLatest map[uint32]uint32
	ckptPgnos  []uint32

	// headerOnDisk is false when the WAL file is empty (0 bytes) and the
	// header has not yet been written to disk. The header is written lazily
	// on the first writeFrames call. This matches SQLite's behavior where
	// after a clean close the WAL file is empty/deleted and only recreated
	// when the first write transaction commits.
	headerOnDisk bool

	// inProcessInit gates the in-process lazy init path in
	// ensureHeaderInitializedInProcess so we only run initHeaderState /
	// recover once per wal instance. Heap-backed SHM is zero-initialized
	// per process, so the first call always needs to publish a header;
	// subsequent calls are no-ops.
	inProcessInit bool

	// inProcess uses heap-backed shm instead of mmap+fcntl (faster, single-process only)
	inProcess bool

	// noCommitSync skips fdatasync on WAL commit (like SQLite synchronous=normal)
	noCommitSync bool

	// inMemory keeps the entire WAL in memory with no WAL file
	inMemory bool

	// busyHandler is an optional callback invoked when lock acquisition fails.
	// If nil, lock failures return ErrBusy immediately (issue 1.7).
	busyHandler BusyHandler

	// Test hook — intentionally commented out to avoid a per-BeginWrite
	// atomic.Load on the production hot path. Uncomment together with the
	// matching check in beginWriteWithSnapshot to re-enable the two tests
	// in busy_test.go (TestBeginWrite_SurfacesBusySnapshotAfterBoundedRetries,
	// TestBeginWrite_BusySnapshotRoutesThroughBusyHandler).
	// forceBusySnapshotForTest atomic.Bool

	// writerHdr is the writer's private copy of the SHM header, updated after
	// each successful commit (writeFrames) and after re-sync in beginWrite().
	// Used to detect external state changes: if the live SHM header differs from
	// writerHdr, another process committed or checkpointed since our last write.
	// DRIFT from SQLite: SQLite stores this in pWal->hdr (shared with readers).
	// We keep it separate because our readers use per-connection pcache isolation
	// and never share pWal->hdr with the writer.
	writerHdr WalIndexHdr

	// iReCksum is the earliest WAL frame overwritten by the current
	// write transaction via frame-reuse. Zero when no overwrites are
	// pending. On commit, if non-zero, rewriteChecksums re-reads
	// frames [iReCksum..iLast] and rewrites their headers to restore
	// the cumulative checksum chain. Reset after successful commit,
	// on recovery, on endWrite, and on savepoint rollback past the
	// overwrite. Mirrors SQLite's Wal.iReCksum (wal.c:533).
	iReCksum uint32

	// memFrames stores page data in memory for InMemory mode,
	// eliminating per-commit file I/O. Frames are flushed to pcache on checkpoint.
	memFrames []memFrame

	// memArena is a pre-allocated byte pool for memFrame data to avoid
	// per-frame allocations and reduce GC pressure.
	memArena    []byte
	memArenaOff int

	// BEGIN ENCRYPTION
	// codec, when non-nil, encrypts frame payloads on write and decrypts
	// them on read. Propagated from the pager after wal creation via
	// p.wal.codec = p.codec; see pager.open / pager.initNewDB.
	codec Codec
	// codecScratch is a page-sized scratch used by writeFrame to hold
	// ciphertext. Allocated lazily on first use. Writes are
	// single-writer-serialized, so one buffer is safe.
	codecScratch []byte
	// codecAEAD is the nonce/AAD scratch for writeFrame's codec call.
	// Embedded by value so nonce/aad slices live on the wal's heap
	// allocation rather than escaping per-call.
	codecAEAD aeadScratch
	// END ENCRYPTION
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
// DRIFT: padToSectorBoundary commit-frame sector padding not ported See docs/btree/NOTES.md#drift-104-padtosectorboundary-sector-padding-of-commit-frames-not-port
// DRIFT: no read-only WAL fallback (readOnly=WAL_RDONLY) when open downgraded See docs/btree/NOTES.md#drift-106-wal-read-only-fallback-not-ported
// DRIFT: syncHeader device-characteristic tuning not ported (always-on) See docs/btree/NOTES.md#drift-107-syncheader-device-characteristic-tuning-not-ported
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
		w.initHeaderStateLocked()
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

	// Fast path: optimistic lock-free readHeader. The dual-copy + memcmp +
	// checksum protocol (walIndexTryHdr wal.c:2570-2620) detects torn reads
	// without requiring a write lock. If a valid SHM hdr exists we adopt it
	// and return — no reason to serialize with peer writers in the common
	// case, and acquiring lockWrite here under RESTART-checkpoint load
	// starves openers (wait for lockWrite → BUSY → open retries).
	//
	// Multi-process mode: pageMap is unused — get()/getLatest() consult
	// SHM hash exclusively (matches walFindFrame at wal.c:3554-3582).
	// No per-process rebuild is needed; peer writes land directly in SHM.
	if !w.inProcess {
		if hdr, valid := w.index.readHeader(); valid {
			info, err := f.Stat()
			if err != nil {
				return err
			}
			w.adoptSHMState(hdr, info.Size() >= walHeaderSize)
			return nil
		}
	}

	// Slow path: SHM hdr was invalid (torn read OR no peer has initialized
	// it yet). We must take lockWrite before falling through to recoverLocked
	// so a peer's in-flight writeHeader cannot produce a stale lastCommitFrame
	// scan that orphans the peer's just-committed frames (tail-of-one-writer
	// rowloss regression, fixed at 8138a22).
	//
	// Lock order write → checkpoint → recover matches SQLite's
	// walIndexRecover (wal.c:1395-1401 `assert( pWal->writeLock )`) and
	// any-store's ensureHeaderInitialized slow path — the two open-class
	// paths cannot deadlock against each other.
	//
	// In-process mode: no SHM coordination needed; w.mu serializes all
	// writers in the same process and heap SHM has no cross-process races.
	if !w.inProcess {
		// Use walBusyLock (with the configured busy handler) so an opener
		// that lands here during an in-progress peer checkpoint-restart
		// waits briefly instead of failing immediately — matches SQLite's
		// walIndexRecover caller (sqlite3WalBeginReadTransaction) which
		// retries under the busy handler on WAL_RETRY.
		if err := walBusyLock(w.index, w.busyHandler, lockWrite, lockExclusive); err != nil {
			return err
		}
		defer func() { _ = w.index.unlock(lockWrite, lockExclusive) }()
	}

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

	// Re-check SHM hdr under locks in case a peer completed writeHeader
	// while we were contending for lockWrite. Adopting it here is strictly
	// safer than falling through to recoverLocked.
	if !w.inProcess {
		if hdr, valid := w.index.readHeader(); valid {
			w.adoptSHMState(hdr, info.Size() >= walHeaderSize)
			return nil
		}
	}

	if info.Size() >= walHeaderSize {
		return w.recoverLocked()
	}

	// No SHM to adopt and no on-disk WAL. Initialize fresh state. The on-disk
	// header is written lazily on the first writeFrames call (flushHeader).
	w.initHeaderStateLocked()

	return nil
}

// ensureHeaderInitialized is the lazy counterpart to wal.open — it guarantees
// that w.index has a valid SHM header and that w's process-local state (salts,
// nFrame, cksum1/2, writerHdr) is synced to it. Safe to call repeatedly; idempotent.
//
// Matches SQLite's walIndexReadHdr (wal.c:2640-2745):
//   - Non-blocking readHeader first (walIndexTryHdr equivalent).
//   - On !valid, take WAL_WRITE_LOCK exclusive, retry readHeader.
//   - If still !valid under WRITE lock, run recover() to reconstruct from the
//     WAL file. Recovery requires WAL_WRITE_LOCK per wal.c:1399, and additionally
//     WAL_CKPT_LOCK + WAL_RECOVER_LOCK exclusive to serialize against siblings
//     that may be trying to initialize concurrently (wal.c:1400-1404).
//
// DRIFT from SQLite (docs/btree/NOTES.md): SQLite's walIndexReadHdr uses WAL_WRITE_LOCK
// alone. We additionally take WAL_CKPT_LOCK + WAL_RECOVER_LOCK so a reader
// triggering recovery fences peers via the same barrier that our Item-1
// reader handshake uses.
// ensureHeaderInitialized guarantees the SHM header is published and returns
// a snapshot of it so callers can stamp it onto their per-tx walHdr. During
// the per-connection-hdr migration (spec:
// docs/superpowers/specs/2026-04-18-per-connection-hdr-design.md), the helper
// still calls syncFromSHMLocked to update process-global writer state
// (w.header.salt*, w.cksum1/2, w.nFrame, w.writerHdr). Later steps will
// narrow that to writer paths only.
//
// The returned hdr is the live SHM header value at the moment of observation.
// On error, the zero hdr is returned and the error signals the retry class.
func (w *wal) ensureHeaderInitialized() (WalIndexHdr, error) {
	if w.inProcess || w.inMemory {
		return w.ensureHeaderInitializedInProcess()
	}

	// Fast path: a peer (or we) already published a valid header.
	if hdr, valid := w.index.readHeader(); valid {
		w.mu.Lock()
		w.syncFromSHMLocked()
		w.mu.Unlock()
		return hdr, nil
	}

	// Slow path: acquire WAL_WRITE_LOCK exclusive, retry, then recover if still bad.
	if err := w.index.lock(lockWrite, lockExclusive); err != nil {
		if err == ErrBusy {
			if e := w.index.lock(lockRecover, lockShared); e == nil {
				_ = w.index.unlock(lockRecover, lockShared)
			}
			if hdr, valid := w.index.readHeader(); valid {
				w.mu.Lock()
				w.syncFromSHMLocked()
				w.mu.Unlock()
				return hdr, nil
			}
			return WalIndexHdr{}, ErrBusyRecovery
		}
		return WalIndexHdr{}, err
	}
	defer func() { _ = w.index.unlock(lockWrite, lockExclusive) }()

	// Re-check under WAL_WRITE_LOCK: a peer may have just published.
	if hdr, valid := w.index.readHeader(); valid {
		w.mu.Lock()
		w.syncFromSHMLocked()
		w.mu.Unlock()
		return hdr, nil
	}

	// Header still bad → run recovery. Acquire CKPT + RECOVER exclusive too.
	if err := w.index.lock(lockCheckpoint, lockExclusive); err != nil {
		if err == ErrBusy {
			return WalIndexHdr{}, ErrBusyRecovery
		}
		return WalIndexHdr{}, err
	}
	defer func() { _ = w.index.unlock(lockCheckpoint, lockExclusive) }()
	if err := w.index.lock(lockRecover, lockExclusive); err != nil {
		if err == ErrBusy {
			return WalIndexHdr{}, ErrBusyRecovery
		}
		return WalIndexHdr{}, err
	}
	defer func() { _ = w.index.unlock(lockRecover, lockExclusive) }()

	w.mu.Lock()
	defer w.mu.Unlock()

	// Final race check under all three locks.
	if hdr, valid := w.index.readHeader(); valid {
		w.syncFromSHMLocked()
		return hdr, nil
	}

	info, err := w.file.Stat()
	if err != nil {
		return WalIndexHdr{}, err
	}
	if info.Size() >= walHeaderSize {
		if err := w.recoverLocked(); err != nil {
			return WalIndexHdr{}, err
		}
	} else {
		w.initHeaderStateLocked()
	}
	hdr, _ := w.index.readHeader()
	return hdr, nil
}

// ensureHeaderInitializedInProcess is the in-process analog. Heap SHM is
// zero-initialized per process, so on first call we always run initHeaderState.
func (w *wal) ensureHeaderInitializedInProcess() (WalIndexHdr, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.inProcessInit {
		if !w.inMemory && w.file != nil {
			info, err := w.file.Stat()
			if err == nil && info.Size() >= walHeaderSize {
				if err := w.recoverLocked(); err != nil {
					return WalIndexHdr{}, err
				}
				w.inProcessInit = true
				hdr, _ := w.index.readHeader()
				return hdr, nil
			}
		}
		w.initHeaderStateLocked()
		w.inProcessInit = true
	}
	hdr, _ := w.index.readHeader()
	return hdr, nil
}

// syncFromSHMLocked synchronizes process-global state from a valid SHM hdr.
// After the per-tx walHdr migration (step 4), writer-private fields
// (w.header.salt*, w.cksum1/2, w.writerHdr) are owned by beginWrite's
// inline sync under lockWrite — this helper only updates the shared
// atomics on w.index and w.nFrame that both readers and writers consult
// for WAL offset + backfill tracking.
//
// Caller must hold w.mu and have observed a valid SHM header.
func (w *wal) syncFromSHMLocked() {
	hdr, valid := w.index.readHeader()
	if !valid {
		return
	}
	// w.nFrame is the shared physical append cursor; readers and writers
	// both look at it. Safe to advance monotonically (the writer in peer
	// process always writes higher values).
	if hdr.mxFrame > w.nFrame.Load() {
		w.nFrame.Store(hdr.mxFrame)
	}
	if !w.headerOnDisk && hdr.mxFrame > 0 {
		w.headerOnDisk = true
	}
	// Monotonic updates on shared walIndex atomics. These are safe under
	// concurrent readers because the writer (single, via writeMu) is the
	// only source of increments.
	w.index.maxFrame.Store(hdr.mxFrame)
	w.index.mxCommitFrame.Store(hdr.mxFrame)
	for {
		old := w.index.maxPage.Load()
		if hdr.nPage <= old || w.index.maxPage.CompareAndSwap(old, hdr.nPage) {
			break
		}
	}
	w.index.nBackfill.Store(w.index.shmNBackfill())
}

// adoptSHMState synchronizes the process-local WAL state (salts, nFrame,
// checksums, header-on-disk flag) from an already-initialized SHM header.
// Called from wal.open when another process has already written SHM.
//
// `walHasHeader` is true when the on-disk WAL file already has a header,
// meaning flushHeader should not rewrite it — our salts come from SHM and
// must match what the sibling wrote to disk.
func (w *wal) adoptSHMState(hdr WalIndexHdr, walHasHeader bool) {
	w.header.salt1 = hdr.aSalt[0]
	w.header.salt2 = hdr.aSalt[1]
	w.header.magic = walMagic
	w.header.version = walVersion
	w.header.pageSize = w.pageSize
	w.cksum1 = hdr.aFrameCksum[0]
	w.cksum2 = hdr.aFrameCksum[1]
	w.nFrame.Store(hdr.mxFrame)
	w.headerOnDisk = walHasHeader
	w.index.maxFrame.Store(hdr.mxFrame)
	w.index.mxCommitFrame.Store(hdr.mxFrame)
	w.index.maxPage.Store(hdr.nPage)
	w.index.nBackfill.Store(w.index.shmNBackfill())
	// Adopt the snapshot so the first beginWrite's stateChanged logic
	// compares apples to apples.
	w.writerHdr = hdr
}

// initHeaderStateLocked initializes the in-memory WAL header state without
// writing to disk. Used when the WAL file is empty (after a clean close). The
// header will be flushed to disk lazily on the first writeFrames call.
//
// Caller must hold w.mu.
func (w *wal) initHeaderStateLocked() {
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
// DRIFT: WAL restart randomizes both salts; C increments salt0 (+1), randomizes salt1 only See docs/btree/NOTES.md#drift-97-wal-restart-randomizes-both-salts-instead-of-incrementing-sa
// DRIFT: WAL header checkpoint-seq nCkpt hardcoded 0, never incremented on restart See docs/btree/NOTES.md#drift-98-wal-checkpoint-sequence-number-nckpt-never-incremented
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

// recoverLocked reads the WAL file and rebuilds the in-memory index from
// committed frames.
//
// Matches SQLite's walIndexRecover() (wal.c:1384-1611). Invariants preserved:
//   - The WAL file itself is never modified. Uncommitted trailing frames are
//     ignored by stopping at lastCommitFrame.
//   - SHM hash regions are published as a single per-region memcpy from a
//     heap-private []byte, never zeroed in place. A peer process observing
//     the pre-publish window sees the OLD valid header + the OLD hash slots;
//     only when the final writeHeader publishes does mxFrame advance.
//   - Region 0's memcpy skips the 136-byte header area (htHdrSize), matching
//     SQLite wal.c:1533: `memcpy(&aShare[nHdr32], &aPrivate[nHdr32], WALINDEX_PGSZ-nHdr)`.
//
// Caller must hold w.mu AND lockCheckpoint + lockRecover exclusive (see
// wal.open and ensureHeaderInitialized). SQLite additionally acquires
// WAL_ALL_BUT_WRITE..READ_LOCK(0) exclusive during recovery (wal.c:1401);
// any-store relies on recoverLocked only being invoked from wal.open (before
// any peer reader/writer has attached) or from ensureHeaderInitialized
// (which holds lockWrite + lockCheckpoint + lockRecover exclusive).
// DRIFT: recoverLocked sets all read-marks NOT_USED; C pre-seeds slot 1 to mxFrame See docs/btree/NOTES.md#drift-88-wal-recovery-does-not-pre-seed-read-mark-slot-1
func (w *wal) recoverLocked() error {
	w.headerOnDisk = true

	buf := make([]byte, walHeaderSize)
	if _, err := w.file.ReadAt(buf, 0); err != nil {
		return err
	}
	if err := w.header.deserialize(buf); err != nil {
		// Invalid WAL header — start fresh.
		if err := w.file.Truncate(0); err != nil {
			return err
		}
		return w.writeHeader()
	}

	w.pageSize = w.header.pageSize
	headerCksum1, headerCksum2 := walChecksum(buf[0:24], 0, 0)

	info, err := w.file.Stat()
	if err != nil {
		return err
	}
	frameSize := int64(walFrameSize) + int64(w.pageSize)

	// Phase 1: parse WAL into a private []uint32 of pgnos. One ReadAt per
	// frame (header+page coalesced). No SHM writes.
	est := int((info.Size() - walHeaderSize) / frameSize)
	if est < 0 {
		est = 0
	}
	parsed := make([]uint32, 0, est)
	scratch := make([]byte, frameSize)

	var (
		fr                                 walFrame
		lastCommitFrame, lastCommitDbSize  uint32
		lastCommitCksum1, lastCommitCksum2 uint32
		s1, s2                             = headerCksum1, headerCksum2
		offset                             = int64(walHeaderSize)
	)
	for offset+frameSize <= info.Size() {
		if _, err := w.file.ReadAt(scratch, offset); err != nil {
			break
		}
		fr.deserialize(scratch[:walFrameSize])
		if fr.salt1 != w.header.salt1 || fr.salt2 != w.header.salt2 {
			break
		}
		// A frame is only valid if its page number is greater than zero.
		// Matches SQLite walDecodeFrame (wal.c:1019-1024), placed after the
		// salt match and before the running-checksum fold so a pgno==0 frame
		// terminates recovery without polluting the checksum state.
		if fr.pgno == 0 {
			break
		}
		s1, s2 = walChecksum(scratch[0:8], s1, s2)
		s1, s2 = walChecksum(scratch[walFrameSize:], s1, s2)
		if s1 != fr.checksum1 || s2 != fr.checksum2 {
			break
		}
		parsed = append(parsed, fr.pgno)
		if fr.dbSize > 0 {
			lastCommitFrame = uint32(len(parsed))
			lastCommitDbSize = fr.dbSize
			lastCommitCksum1 = s1
			lastCommitCksum2 = s2
		}
		offset += frameSize
	}

	// Phase 2: build SQLite-style aPrivate buffers. One []byte per touched
	// SHM region, mutated entirely in process heap — shared SHM is untouched.
	// Also rebuild pageMap locally.
	priv := make(map[int][]byte)
	getPriv := func(seg int) []byte {
		if b, ok := priv[seg]; ok {
			return b
		}
		b := make([]byte, shmRegionSize)
		priv[seg] = b
		return b
	}
	// walIndexAppend-equivalent on a private region buffer (wal.c:1295-1345).
	appendPriv := func(seg int, pgno, frame uint32) {
		region := getPriv(seg)
		_, _, iZero := htSegmentInfo(seg)
		idx := int(frame-iZero) - 1
		pgnoOff := htPgnoOffset(seg, idx)
		binary.LittleEndian.PutUint32(region[pgnoOff:], pgno)
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
	newMap := make(map[uint32][]uint32, len(parsed))
	for i := uint32(1); i <= lastCommitFrame; i++ {
		pgno := parsed[i-1]
		seg, _ := htFrameSegIdx(i)
		appendPriv(seg, pgno, i)
		newMap[pgno] = append(newMap[pgno], i)
	}

	// Phase 3: single-shot publish of all private buffers to SHM. For region 0
	// we skip the first htHdrSize (=136) bytes so the OLD header + ckpt info
	// stay in place; they are overwritten only by writeHeader / shmWriteCkptInfo
	// at the end, matching SQLite wal.c:1524-1533 commentary.
	for seg, b := range priv {
		region, err := w.index.shm.region(seg, true)
		if err != nil {
			return err
		}
		skip := 0
		if seg == 0 {
			skip = htHdrSize
		}
		copy(region[skip:], b[skip:])
	}

	// Swap pageMap under walIndex.mu — serializes with same-process readers.
	w.index.mu.Lock()
	w.index.pageMap = newMap
	w.index.mu.Unlock()

	// Phase 4: scalar state. Use locals directly (no atomic-load-after-store).
	if lastCommitFrame > 0 {
		w.nFrame.Store(lastCommitFrame)
		w.cksum1 = lastCommitCksum1
		w.cksum2 = lastCommitCksum2
		w.index.maxFrame.Store(lastCommitFrame)
		w.index.mxCommitFrame.Store(lastCommitFrame)
		w.index.maxPage.Store(lastCommitDbSize)
		// SQLite wal.c:1598: nBackfillAttempted = mxFrame after recovery.
		w.index.nBackfillAttempted.Store(lastCommitFrame)
	} else {
		w.nFrame.Store(0)
		w.cksum1 = headerCksum1
		w.cksum2 = headerCksum2
		w.index.maxFrame.Store(0)
		w.index.mxCommitFrame.Store(0)
		w.index.maxPage.Store(0)
		w.index.nBackfillAttempted.Store(0)
	}
	w.index.nBackfill.Store(0)
	for i := range w.index.aReadMark {
		w.index.aReadMark[i].Store(readMarkNotUsed)
	}
	w.index.aReadMark[0].Store(0)

	// Phase 5: publish header FIRST, then ckpt info. writeHeader issues the
	// release barrier between the hash copy above and the new mxFrame. The
	// SQLite order (wal.c:1566 → 1572-1574) is writeHdr, then aReadMark and
	// nBackfillAttempted — matched here.
	if err := w.index.writeHeader(lastCommitFrame, lastCommitDbSize, 0,
		[2]uint32{w.cksum1, w.cksum2},
		[2]uint32{w.header.salt1, w.header.salt2}); err != nil {
		return err
	}
	w.index.shmWriteCkptInfo()

	// Recovery reestablishes the canonical WAL state; no overwrites
	// are pending. Matches SQLite wal.c:3733 walIndexTryHdr, which
	// implicitly resets via the memcmp-fresh hdr copy.
	w.iReCksum = 0
	return nil
}

// writeFrames appends frames to the WAL. If commit is true, the last frame
// is marked as a commit frame with the given dbSize.
// All frames are batched into a single write call for performance.
// DRIFT: wal-index header iChange counter never incremented (always 0) See docs/btree/NOTES.md#drift-96-wal-index-change-counter-ichange-never-incremented
// DRIFT: padToSectorBoundary commit-frame sector padding not ported See docs/btree/NOTES.md#drift-104-padtosectorboundary-sector-padding-of-commit-frames-not-port
// DRIFT: journal_size_limit/mxWalSize WAL truncation unimplemented (truncateOnCommit/walLimitSize) See docs/btree/NOTES.md#drift-105-journal-size-limit-wal-truncation-feature-unimplemented
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

	s1, s2 := w.cksum1, w.cksum2
	startFrame := nf + 1

	// Compute iFirst — the first frame written by the current tx.
	// Frames at iFirst or later are candidates for in-place reuse;
	// earlier frames (<= last-committed mxFrame) are visible to other
	// readers/processes and must not be touched. Matches SQLite
	// wal.c:4048-4051 (iFirst = pLive->mxFrame+1).
	//
	// We use mxCommitFrame rather than writerHdr.mxFrame because
	// writerHdr is only maintained in multi-process mode (gated by
	// !w.inProcess in the commit path); mxCommitFrame is updated in
	// both modes on every commit and therefore always reflects the
	// last-committed state at tx begin. Within a tx, spills don't
	// update mxCommitFrame (commit=false skips the Store), so iFirst
	// stays stable across spills.
	iFirst := w.index.mxCommitFrame.LoadLocal() + 1

	// Track appended frames separately from the input slice index.
	// Reused pages don't advance the append cursor nor the checksum
	// chain. `appended` records exactly the pages that were appended (in
	// frame order) so setBatch registers the correct frame numbers and
	// maxFrame — recording inline avoids re-deriving the reuse predicate
	// (which previously dropped the force-appended commit frame).
	var nAppended uint32
	appended := make([]*page, 0, len(pages))

	// Per-frame writes matching SQLite's walWriteOneFrame pattern:
	// 24-byte stack header + direct page data write — zero heap allocation.
	var frameHdr [walFrameSize]byte

	for i, p := range pages {
		isLast := i == len(pages)-1

		// Reuse check — mirrors SQLite wal.c:4124-4140. Only allowed
		// for non-last-of-commit pages; the last commit frame must
		// be a fresh append carrying the dbSize commit marker.
		if !(commit && isLast) && iFirst > 0 && !DiagDisableReuse.Load() {
			iWrite := w.index.getInTxRange(p.pgno, nf+nAppended, iFirst)
			if iWrite > 0 {
				// Overwrite the page data at frame iWrite. The frame
				// header (pgno, dbSize, salts, checksums) is NOT
				// rewritten here — rewriteChecksums on commit
				// recomputes the chain. The pgno/salts are still
				// valid; only cksum1/cksum2 become stale and must
				// be fixed before the tx is visible.
				dataOff := int64(walHeaderSize) + int64(iWrite-1)*int64(frameSize) + int64(walFrameSize)

				// BEGIN ENCRYPTION
				// Run the codec on the reused payload exactly like the
				// fresh-append branch below — otherwise plaintext lands
				// in a frame the reader will try to decrypt, and the
				// cksum codec's trailer (or AEAD tag) will not match.
				// Mirrors SQLCipher wal.c:4152-4156, which gates the
				// reuse-path WriteAt on sqlcipherPagerCodec(p).
				payload := p.data
				if w.codec != nil {
					if w.codecScratch == nil {
						w.codecScratch = make([]byte, w.pageSize)
					}
					ct, eerr := encryptPageWithCodec(w.codec, w.codecScratch, p.data, p.pgno, &w.codecAEAD)
					if eerr != nil {
						return eerr
					}
					payload = ct
				}
				// END ENCRYPTION

				if _, err := w.file.WriteAt(payload, dataOff); err != nil {
					return err
				}
				if w.iReCksum == 0 || iWrite < w.iReCksum {
					w.iReCksum = iWrite
				}
				DiagReuseFrames.Add(1)
				// SHM hash entry already points at iWrite — unchanged.
				continue
			}
		}

		// Fresh append — original path.
		DiagAppendFrames.Add(1)
		off := offset + int64(nAppended)*int64(frameSize)

		binary.BigEndian.PutUint32(frameHdr[0:4], p.pgno)
		var dbSizeField uint32
		if commit && isLast {
			dbSizeField = dbSize
		}
		binary.BigEndian.PutUint32(frameHdr[4:8], dbSizeField)
		binary.BigEndian.PutUint32(frameHdr[8:12], w.header.salt1)
		binary.BigEndian.PutUint32(frameHdr[12:16], w.header.salt2)

		// BEGIN ENCRYPTION
		// Encrypt page payload before checksum (per encryption.md §4.3: the
		// WAL checksum covers ciphertext). When no codec is installed this
		// is a zero-cost pass-through — payload := p.data.
		payload := p.data
		if w.codec != nil {
			if w.codecScratch == nil {
				w.codecScratch = make([]byte, w.pageSize)
			}
			ct, eerr := encryptPageWithCodec(w.codec, w.codecScratch, p.data, p.pgno, &w.codecAEAD)
			if eerr != nil {
				return eerr
			}
			payload = ct
		}
		// END ENCRYPTION

		// Compute checksum over frame header (first 8 bytes) + page data
		s1, s2 = walChecksum(frameHdr[0:8], s1, s2)
		s1, s2 = walChecksum(payload, s1, s2)

		binary.BigEndian.PutUint32(frameHdr[16:20], s1)
		binary.BigEndian.PutUint32(frameHdr[20:24], s2)

		// Write frame header
		if _, err := w.file.WriteAt(frameHdr[:], off); err != nil {
			return err
		}
		// Write page data (plaintext or ciphertext depending on codec).
		if _, err := w.file.WriteAt(payload, off+walFrameSize); err != nil {
			return err
		}
		appended = append(appended, p)
		nAppended++
	}

	// Only advance nFrame and checksums after successful write. If
	// WriteAt fails (disk full, I/O error), the WAL state remains at
	// the pre-write position so subsequent writes use the correct
	// offset and checksum chain.
	//
	// Use nAppended, not len(pages): reused frames don't advance the
	// append cursor. The running checksums (s1, s2) already reflect
	// only appended frames because the reuse branch skipped the
	// accumulation via `continue`.
	w.nFrame.Store(nf + nAppended)
	w.cksum1 = s1
	w.cksum2 = s2

	// Batch update walIndex for APPENDED frames only — reused frames
	// already have their hash entries at the reused index (setBatch
	// would overwrite with the wrong frame number otherwise).
	if nAppended > 0 {
		w.index.setBatch(appended, startFrame, commit)
	}

	// Invariant (defense-in-depth): every physically-appended frame must be
	// registered in the wal-index. A shortfall means a frame — typically the
	// force-appended commit frame — was dropped from the index batch, which
	// desyncs mxCommitFrame and silently corrupts crash recovery (committed
	// data lost). Fail the write loudly instead. registered maxFrame and the
	// physical append cursor w.nFrame must agree after every writeFrames.
	if mf := w.index.maxFrame.Load(); mf < w.nFrame.Load() {
		return fmt.Errorf("btree: wal-index frame desync: registered maxFrame=%d < %d frames written (dropped frame)", mf, w.nFrame.Load())
	}

	if commit {
		// Rewrite checksum chain if any frame was overwritten by in-tx
		// reuse. Must happen BEFORE fdatasync so the durable state has
		// a consistent chain — matches SQLite wal.c:4152-4156 where
		// walRewriteChecksums is called before the commit-time sync.
		if w.iReCksum > 0 {
			if err := w.rewriteChecksums(nf + nAppended); err != nil {
				return err
			}
		}

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
//
// BEGIN ENCRYPTION
// InMemory mode skips encryption per encryption.md §7: encryption protects
// data at rest, and an InMemory DB has no on-disk file to protect. The WAL
// memory arena stays plaintext, readFrame's in-memory branch returns
// plaintext, and checkpoint to masterStore is also plaintext. If a codec
// is installed with an InMemory DB, ReservedSpace still accounts for
// per-page overhead (so page format stays consistent with a future
// serialization) but the codec itself is not invoked on this fast path.
// END ENCRYPTION
func (w *wal) writeFramesMem(pages []*page, commit bool, dbSize uint32) error {
	w.mu.Lock()

	nf := w.nFrame.Load()
	startFrame := nf + 1
	pageSz := int(w.pageSize)

	// iFirst — first frame written by the current tx. Same logic as
	// disk-mode writeFrames; reuse only allowed for frames in this tx.
	iFirst := w.index.mxCommitFrame.LoadLocal() + 1

	// Ensure arena has enough space for all pages in this batch (worst
	// case: no reuse). We may over-reserve when reuse fires; that's fine
	// — the arena recycles across txs.
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

	var nAppended uint32
	appended := make([]*page, 0, len(pages))
	for i, p := range pages {
		isLast := i == len(pages)-1

		// Reuse check — same semantics as disk-mode writeFrames.
		if !(commit && isLast) && iFirst > 0 {
			iWrite := w.index.getInTxRange(p.pgno, nf+nAppended, iFirst)
			if iWrite > 0 {
				// Overwrite the existing memFrame slot in place.
				// memFrames is 0-indexed; frames are 1-indexed.
				copy(w.memFrames[iWrite-1].data, p.data)
				continue
			}
		}

		// Fresh append — slice page data from the arena.
		dataCopy := w.memArena[w.memArenaOff : w.memArenaOff+pageSz]
		w.memArenaOff += pageSz
		copy(dataCopy, p.data)

		var dbSizeField uint32
		if commit && isLast {
			dbSizeField = dbSize
		}

		w.memFrames = append(w.memFrames, memFrame{
			pgno:   p.pgno,
			dbSize: dbSizeField,
			data:   dataCopy,
		})
		appended = append(appended, p)
		nAppended++
	}

	// Store nFrame while still holding the lock so readers see consistent
	// memFrames slice + nFrame via their RLock.
	w.nFrame.Store(nf + nAppended)
	w.mu.Unlock()

	// Batch update walIndex for APPENDED frames only — `appended` was
	// recorded inline in the write loop above (never re-derived), so the
	// force-appended commit frame is always present.
	if nAppended > 0 {
		w.index.setBatch(appended, startFrame, commit)
	}

	// Invariant (defense-in-depth): registered maxFrame must cover every
	// physically-appended frame; a shortfall = a dropped (commit) frame =
	// silent recovery corruption. See disk-path writeFrames for rationale.
	if mf := w.index.maxFrame.Load(); mf < w.nFrame.Load() {
		return fmt.Errorf("btree: wal-index frame desync (mem): registered maxFrame=%d < %d frames written (dropped frame)", mf, w.nFrame.Load())
	}

	if commit {
		// Advance mxCommitFrame so readers can see the committed frames.
		w.index.mxCommitFrame.Store(w.index.maxFrame.Load())
		if dbSize > 0 {
			w.index.maxPage.Store(dbSize)
		}
	}

	return nil
}

// BEGIN ENCRYPTION

// readFrameRaw reads frame `frame` into buf without invoking the codec.
// For file-backed WALs that means the on-disk ciphertext-with-trailer
// bytes; for in-memory WALs the bytes are already plaintext (no codec
// is invoked on the in-memory write path either — see writeFramesMem).
//
// Used by the VerifyIntegrity sweep, which needs the post-Encrypt bytes
// to verify the trailer (cksum) or attempt decrypt (AEAD). Concurrency
// contract matches readFrame.
func (w *wal) readFrameRaw(frame uint32, buf []byte) error {
	if frame == 0 {
		return ErrWALCorrupt
	}
	nf := w.nFrame.Load()
	if w.inMemory {
		if frame > nf {
			return ErrWALCorrupt
		}
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
	if w.inProcess && frame > nf {
		return ErrWALCorrupt
	}
	if w.file == nil {
		return ErrWALCorrupt
	}
	frameSize := int64(walFrameSize) + int64(w.pageSize)
	offset := int64(walHeaderSize) + int64(frame-1)*frameSize + walFrameSize
	if _, err := w.file.ReadAt(buf[:w.pageSize], offset); err != nil {
		if frame > nf {
			return ErrWALCorrupt
		}
		return err
	}
	return nil
}

// END ENCRYPTION

// readFrame reads the page data for a given frame number.
// For the file-based path, only an atomic load of nFrame is needed (WAL frames
// on disk are immutable once written). For the memFrames path, RLock protects
// the slice from concurrent append by writeFramesMem.
//
// When the codec is installed, scratchBuf (if non-nil) is used as the
// decryption scratch to avoid a per-call allocPageBuffer/freePageBuffer
// round-trip. scratchBuf must be at least w.pageSize bytes and must not
// alias buf. aeadScratchBuf (if non-nil) holds the nonce/AAD buffers to
// keep them off-heap. Callers that pass nil for either get a per-call
// alloc (rare paths).
func (w *wal) readFrame(frame uint32, buf, scratchBuf []byte, aeadScratchBuf *aeadScratch) error {
	if frame == 0 {
		return ErrWALCorrupt
	}
	nf := w.nFrame.Load()
	// Fast path: read from in-memory frames (needs RLock for slice access).
	// Use the immutable inMemory flag instead of checking the mutable memFrames
	// slice header to avoid a data race with writeFramesMem's append.
	if w.inMemory {
		if frame > nf {
			return ErrWALCorrupt
		}
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
	// File path: in multi-process mode, a peer may have committed frames beyond
	// our process-local nFrame. In that case, do not reject the frame number up
	// front — attempt the immutable file read directly and let ReadAt validate
	// whether the frame exists on disk.
	if w.inProcess && frame > nf {
		return ErrWALCorrupt
	}
	if w.file == nil {
		return ErrWALCorrupt
	}
	frameSize := int64(walFrameSize) + int64(w.pageSize)
	offset := int64(walHeaderSize) + int64(frame-1)*frameSize + walFrameSize
	if _, err := w.file.ReadAt(buf[:w.pageSize], offset); err != nil {
		// Multi-process: a stale nFrame view may have let us try to read
		// past the peer's actual WAL tail. Treat that as corruption
		// rather than leaking the raw I/O error.
		if frame > nf {
			return ErrWALCorrupt
		}
		return err
	}
	// BEGIN ENCRYPTION
	// Decrypt frame payload if a codec is installed. The caller needs the
	// frame's page number to bind into AAD; we extract it from the frame
	// header which lives at offset - walFrameSize.
	if w.codec != nil {
		pgno, perr := w.readFramePgno(frame)
		if perr != nil {
			return perr
		}
		scratch := scratchBuf
		ownScratch := false
		if scratch == nil {
			scratch = allocPageBuffer(int(w.pageSize), false)
			ownScratch = true
		}
		aeadS := aeadScratchBuf
		if aeadS == nil {
			aeadS = new(aeadScratch)
		}
		plain, derr := decryptPageWithCodec(w.codec, scratch, buf[:w.pageSize], pgno, aeadS)
		if derr != nil {
			if ownScratch {
				freePageBuffer(scratch, false)
			}
			return fmt.Errorf("btree: WAL decrypt frame %d (pgno %d): %w", frame, pgno, derr)
		}
		copy(buf[:w.pageSize], plain)
		if ownScratch {
			freePageBuffer(scratch, false)
		}
	}
	// END ENCRYPTION
	return nil
}

// BEGIN ENCRYPTION

// readFramePgno returns the page number encoded in the frame header at
// position (frame-1). Only called on the file-backed path; the caller
// must have validated frame >= 1 and frame <= nFrame.
func (w *wal) readFramePgno(frame uint32) (uint32, error) {
	frameSize := int64(walFrameSize) + int64(w.pageSize)
	headerOffset := int64(walHeaderSize) + int64(frame-1)*frameSize
	var hdr [4]byte
	if _, err := w.file.ReadAt(hdr[:], headerOffset); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(hdr[:4]), nil
}

// END ENCRYPTION

// beginRead acquires a shared read lock on a reader slot and returns the
// current max frame number for snapshot isolation plus the slot number.
// Reader slot rotation (like SQLite):
//   - Slot 0: used when nBackfill == maxFrame (read everything from DB, skip WAL)
//   - Slots 1-4: used for readers that need WAL frames. Best slot is the one
//     with the largest readmark <= current maxFrame.
func (w *wal) beginRead() (maxFrame uint32, slot int, err error) {
	_, maxFrame, slot, err = w.beginReadHdr()
	return maxFrame, slot, err
}

// beginReadHdr is beginRead plus the exact WAL header snapshot used to claim
// the reader slot. The caller can thread this hdr into BUSY_SNAPSHOT checks so
// beginWrite compares against the same snapshot that beginRead actually used.
func (w *wal) beginReadHdr() (hdr WalIndexHdr, maxFrame uint32, slot int, err error) {
	// Retry loop matching SQLite's WAL_RETRY protocol (wal.c:3022-3056,
	// 3391-3393). After 5 retries we begin sleeping; from the 10th retry
	// onward the delay grows quadratically up to ~10s total before
	// returning ErrProtocol at WAL_RETRY_PROTOCOL_LIMIT iterations.
	//
	// Constants match SQLite literally:
	//   WAL_RETRY_PROTOCOL_LIMIT = 100  (wal.c:2943)
	//   first 5 retries:           Gosched only
	//   retries 6..9:              1 µs sleep
	//   retries 10..100:           (cnt-9)² × 39 µs (≈ 323 ms at cnt=100)
	//
	// DRIFT from SQLite (docs/btree/NOTES.md §20, drift 5): SQLite does not use a
	// *pChanged output signal — pWal->hdr already reflects the current SHM
	// state after walIndexReadHdr(). Our per-connection caches are invalidated
	// via dataVersion in DB.beginRead(), not via a signal from tryBeginRead.
	const protocolLimit = 100
	for cnt := 0; cnt <= protocolLimit; cnt++ {
		hdr, maxFrame, slot, err = w.tryBeginReadHdr()
		if !errors.Is(err, errWALRetry) {
			return hdr, maxFrame, slot, err
		}
		if cnt < 5 {
			runtime.Gosched()
			continue
		}
		nDelay := time.Microsecond // 1 µs base (retries 6..9)
		if cnt >= 10 {
			d := cnt - 9
			nDelay = time.Duration(d*d*39) * time.Microsecond
		}
		time.Sleep(nDelay)
	}
	return WalIndexHdr{}, 0, 0, ErrProtocol
}

// tryBeginRead attempts to acquire a reader slot and returns the current
// max frame number. Returns errWALRetry if the WAL state changed between
// reading metadata and acquiring the lock, signaling the caller to retry.
// DRIFT: slot-0 read path writes mxFrame into aReadMark[0] (must stay 0) See docs/btree/NOTES.md#drift-100-slot-0-read-path-writes-mxframe-violating-areadmark0-invaria
func (w *wal) tryBeginRead() (maxFrame uint32, slot int, err error) {
	_, maxFrame, slot, err = w.tryBeginReadHdr()
	return maxFrame, slot, err
}

// tryBeginReadHdr is tryBeginRead plus the exact WAL header snapshot used to
// choose/validate the reader slot.
func (w *wal) tryBeginReadHdr() (hdr WalIndexHdr, maxFrame uint32, slot int, err error) {
	if w.inProcess || w.inMemory {
		return w.tryBeginReadInProcessHdr()
	}
	return w.tryBeginReadMultiProcessHdr()
}

// tryBeginReadInProcess handles the in-process/in-memory path where all state
// is in process-local atomics. There are no cross-PROCESS SHM races, but an
// INTERNAL concurrent checkpoint (auto-checkpoint from Commit via
// pager.tryCheckpoint, or DB.Checkpoint) runs WITHOUT pager.mu and can advance
// mxCommitFrame / nBackfill and grab+clear free reader slots while a reader is
// mid-acquire. The slot-0 fast path and the fresh-slot claim both publish a
// mark equal to mxFrame, but the slot-REUSE branch publishes no mark, so it
// re-validates mxCommitFrame and nBackfill after taking the shared lock and
// returns errWALRetry if either moved (mirrors SQLite walTryBeginRead's
// post-lock re-check of aReadMark[mxI], wal.c:3239-3249). On retry the
// nBackfill==mxFrame slot-0 fast path stays safe.
func (w *wal) tryBeginReadInProcess() (maxFrame uint32, slot int, err error) {
	_, maxFrame, slot, err = w.tryBeginReadInProcessHdr()
	return maxFrame, slot, err
}

func (w *wal) tryBeginReadInProcessHdr() (hdr WalIndexHdr, maxFrame uint32, slot int, err error) {
	mxFrame := w.index.mxCommitFrame.LoadLocal()
	nBackfill := w.index.nBackfill.Load()
	hdr = WalIndexHdr{isInit: 1, mxFrame: mxFrame}

	if mxFrame == 0 || nBackfill == mxFrame {
		if err := w.index.lock(lockRead0, lockShared); err != nil {
			return WalIndexHdr{}, 0, 0, err
		}
		w.index.aReadMark[0].Store(mxFrame)
		return hdr, mxFrame, 0, nil
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
			// Post-lock re-validation. An internal concurrent checkpoint
			// (auto-checkpoint from Commit / DB.Checkpoint) runs WITHOUT
			// pager.mu and may, between our lock-free scan above and the
			// shared-lock acquisition here, advance mxCommitFrame and/or
			// nBackfill (and exclusively grab+clear a now-free reader slot).
			// The reused slot's stored mark may then lag our snapshot
			// (endRead never resets a slot's mark), so a checkpointer that
			// read that under-reported mark could backfill frames past the
			// snapshot this slot still pins. Mirror SQLite walTryBeginRead's
			// post-lock re-check of aReadMark[mxI] (wal.c:3239-3249): if
			// either value changed, drop the lock and retry. On retry the
			// nBackfill==mxFrame slot-0 fast path stays safe.
			if w.index.mxCommitFrame.LoadLocal() != mxFrame || w.index.nBackfill.Load() != nBackfill {
				_ = w.index.unlock(lockSlot, lockShared)
				return WalIndexHdr{}, 0, 0, errWALRetry
			}
			return hdr, mxFrame, bestSlot, nil
		}
	}

	for i := 1; i <= 4; i++ {
		lockSlot := lockRead0 + i
		if err := w.index.lock(lockSlot, lockShared); err == nil {
			w.index.aReadMark[i].Store(mxFrame)
			return hdr, mxFrame, i, nil
		}
	}

	if err := w.index.lock(lockRead0, lockShared); err != nil {
		// Slot 0 fallback also busy: every slot 1..4 was busy AND slot 0
		// is busy too. Convert to errWALRetry so beginReadHdr loops.
		// Mirrors the multi-process site's SQLite wal.c:3186-3188
		// conversion. In InProcess mode this is rare (process-local
		// locks rarely race) but kept symmetric.
		if errors.Is(err, ErrBusy) {
			return WalIndexHdr{}, 0, 0, errWALRetry
		}
		return WalIndexHdr{}, 0, 0, err
	}
	w.index.aReadMark[0].Store(mxFrame)
	return hdr, mxFrame, 0, nil
}

// tryBeginReadMultiProcess handles the multi-process path, matching SQLite's
// walTryBeginRead() (wal.c:3000-3252) step by step:
//
//  1. Read SHM header into a local copy (SQLite: walIndexReadHdr → walIndexTryHdr
//     copies SHM into pWal->hdr). We use a function-scoped local variable instead
//     of a persistent per-connection field. (docs/btree/NOTES.md §20, drift 4)
//  2. Read nBackfill and aReadMark directly from SHM each time (SQLite: volatile
//     WalCkptInfo *pInfo = walCkptInfo(pWal), then AtomicLoad). We use shmNBackfill()
//     and shmReadMark() helper functions. (docs/btree/NOTES.md §20, drift 1)
//  3. Claim a reader slot using exclusive lock → write readmark → unlock → shared lock.
//     Matches SQLite wal.c:3170-3185 exactly.
//  4. Re-validate after acquiring shared lock: compare live SHM header against local
//     copy AND live readmark against saved value. If either changed → WAL_RETRY.
//     Matches SQLite wal.c:3239-3249 (memcmp + AtomicLoad re-check).
//  5. Sync nBackfill to process-local atomic for walIndex.get() minFrame filter.
//     (docs/btree/NOTES.md §20, drift 3 — SQLite doesn't need this sync step because it reads
//     nBackfill directly from SHM via pInfo pointer everywhere)
//
// DRIFT: reader-slot tie-break selects lowest slot; SQLite selects highest on equal marks See docs/btree/NOTES.md#drift-102-reader-slot-tie-break-selects-lowest-not-highest
func (w *wal) tryBeginReadMultiProcess() (maxFrame uint32, slot int, err error) {
	_, maxFrame, slot, err = w.tryBeginReadMultiProcessHdr()
	return maxFrame, slot, err
}

func (w *wal) tryBeginReadMultiProcessHdr() (hdr WalIndexHdr, maxFrame uint32, slot int, err error) {
	// Step 1: Read SHM header into local copy (SQLite: walIndexReadHdr → walIndexTryHdr)
	hdr, valid := w.index.readHeader()
	if !valid {
		// SHM header is invalid — a sibling process may be in walIndexRecover()
		// (which zeroes SHM in-place) or SHM hasn't been initialized yet.
		// Matches SQLite wal.c:3089: acquire WAL_RECOVER_LOCK shared as a fence,
		// blocking until any in-progress exclusive recoverer releases, then
		// release and retry from the top of the loop.
		if err := w.index.lock(lockRecover, lockShared); err == nil {
			_ = w.index.unlock(lockRecover, lockShared)
		}
		return WalIndexHdr{}, 0, 0, errWALRetry
	}
	mxFrame := hdr.mxFrame

	// Step 2: Read nBackfill directly from SHM (SQLite: AtomicLoad(&pInfo->nBackfill))
	nBackfill := w.index.shmNBackfill()

	// Step 3: Slot 0 path — WAL fully checkpointed (SQLite wal.c:3114-3147).
	// On BUSY, fall through to the slot-1..4 selection path matching
	// SQLite wal.c:3144-3146:
	//   }else if( rc!=SQLITE_BUSY ){
	//     return rc;
	//   }
	// SQLite intentionally lets BUSY here flow into the read-mark loop, so
	// a reader doesn't bail out just because a checkpointer briefly holds
	// slot 0 exclusive.
	if mxFrame == 0 || nBackfill == mxFrame {
		err := w.index.lock(lockRead0, lockShared)
		if err == nil {
			walShmBarrier()
			// Re-validate: compare live SHM header against our local copy
			// (SQLite wal.c:3139: memcmp(walIndexHdr(pWal), &pWal->hdr, sizeof(WalIndexHdr)))
			// C does an unconditional raw memcmp against the validated cached
			// hdr, so an invalid/zeroed live header (!ok, e.g. a recoverer
			// mid-zeroing SHM) always differs and forces WAL_RETRY.
			if liveHdr, ok := w.index.readHeader(); !ok || liveHdr != hdr {
				_ = w.index.unlock(lockRead0, lockShared)
				return WalIndexHdr{}, 0, 0, errWALRetry
			}
			w.index.shmWriteReadMark(0, mxFrame)
			w.index.aReadMark[0].Store(mxFrame) // keep process-local in sync
			return hdr, mxFrame, 0, nil
		}
		if !errors.Is(err, ErrBusy) {
			return WalIndexHdr{}, 0, 0, err
		}
		// BUSY: fall through to slot 1..4 selection below.
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
		// No reader slot could be claimed — every slot 1..4 is held shared
		// by a peer reader, so step 5's exclusive lock attempts all
		// returned ErrBusy. Convert to errWALRetry so the outer
		// beginReadHdr loop can spin until a slot frees up.
		// Matches SQLite wal.c:3186-3188:
		//   if( mxI==0 ){
		//     assert( rc==SQLITE_BUSY || (pWal->readOnly & WAL_SHM_RDONLY)!=0 );
		//     return rc==SQLITE_BUSY ? WAL_RETRY : SQLITE_READONLY_CANTINIT;
		//   }
		// any-store has no read-only-shm mode, so the conversion is
		// unconditional.
		return WalIndexHdr{}, 0, 0, errWALRetry
	}

	// Step 6: Acquire shared lock on the chosen slot (SQLite wal.c:3192)
	lockSlot := lockRead0 + bestSlot
	if err := w.index.lock(lockSlot, lockShared); err != nil {
		return WalIndexHdr{}, 0, 0, errWALRetry
	}

	// Step 7: Re-validate after lock (SQLite wal.c:3239-3249)
	// Read minFrame (nBackfill+1) from SHM, then barrier, then check header + readmark
	nBackfill = w.index.shmNBackfill()
	walShmBarrier()

	liveMark := w.index.shmReadMark(bestSlot)
	liveHdr, liveValid := w.index.readHeader()
	// C does an unconditional raw memcmp against the validated cached hdr
	// (wal.c:3255-3256), so an invalid/zeroed live header (!liveValid) always
	// differs and forces WAL_RETRY.
	if liveMark != bestMark || !liveValid || liveHdr != hdr {
		_ = w.index.unlock(lockSlot, lockShared)
		return WalIndexHdr{}, 0, 0, errWALRetry
	}

	// Sync nBackfill to process-local for walIndex.get() minFrame filter
	w.index.nBackfill.Store(nBackfill)

	return hdr, mxFrame, bestSlot, nil
}

// endRead releases the reader lock for the given slot.
func (w *wal) endRead(slot int) {
	_ = w.index.unlock(lockRead0+slot, lockShared)
}

// beginWrite acquires the exclusive write lock.
// Uses the busy handler for retry/backoff if configured (issue 1.7).
//
// For multi-process mode, performs a BUSY_SNAPSHOT check after acquiring the
// lock: compares the caller's read snapshot (tx.walHdr, captured at BeginRead
// time) against the current SHM header. If they differ, another process
// committed since our last read, so we reject the write to prevent stale-state
// corruption. Matches sqlite3WalBeginWriteTransaction (wal.c:3700-3714).
//
// `readSnap` is the reader's snapshot. Pass zero-value hdr (isInit=0) to skip
// the BUSY_SNAPSHOT check — used by raw tests that don't hold a tx.
//
// Returns stateChanged=true if the WAL state was re-synced from SHM (indicating
// another process committed or checkpointed since the last local write).
// Callers should invalidate any stale page caches when stateChanged is true.
func (w *wal) beginWrite() (stateChanged bool, err error) {
	// Legacy entry for raw-wal tests. Passes zero snapshot → BUSY_SNAPSHOT
	// check is skipped (acceptable; these tests don't exercise the race).
	return w.beginWriteWithSnapshot(WalIndexHdr{})
}

// beginWriteWithSnapshot is the generalized form: caller supplies the
// read snapshot for the BUSY_SNAPSHOT check. See beginWrite for semantics.
func (w *wal) beginWriteWithSnapshot(readSnap WalIndexHdr) (stateChanged bool, err error) {
	// Test hook — kept commented to avoid an atomic.Load on every BeginWrite.
	// Uncomment with the matching field in the wal struct to run the two
	// ErrBusySnapshot tests in busy_test.go.
	// if w.forceBusySnapshotForTest.Load() {
	// 	return false, ErrBusySnapshot
	// }
	if err := walBusyLock(w.index, w.busyHandler, lockWrite, lockExclusive); err != nil {
		return false, err
	}
	if w.inProcess || w.inMemory {
		return false, nil
	}

	// BUSY_SNAPSHOT: compare caller's snapshot against current SHM header.
	// Only check if readSnap is populated (isInit==1); zero hdr skips.
	hdr, valid := w.index.readHeader()
	if debugTrace {
		trace("beginWriteWithSnapshot: valid=%v readSnap.init=%d readSnap.mx=%d live.mx=%d writerHdr.init=%d writerHdr.mx=%d",
			valid, readSnap.isInit, readSnap.mxFrame, hdr.mxFrame, w.writerHdr.isInit, w.writerHdr.mxFrame)
	}
	// C does an unconditional raw memcmp of the validated reader snapshot
	// (pWal->hdr == readSnap) against the live header (wal.c:3729), so an
	// invalid/zeroed live header (!valid) with a populated reader snapshot
	// also differs and yields SQLITE_BUSY_SNAPSHOT. We return before the
	// valid-gated re-sync block below, which must only read from a valid
	// live header. A zero snapshot (isInit==0, raw-wal tests) still skips.
	if readSnap.isInit != 0 && (!valid || hdr != readSnap) {
		if debugTrace {
			trace("beginWriteWithSnapshot: busy_snapshot live.mx=%d readSnap.mx=%d", hdr.mxFrame, readSnap.mxFrame)
		}
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
		if debugTrace {
			trace("beginWriteWithSnapshot: stateChanged=%v live.mx=%d writerHdr.mx=%d", stateChanged, hdr.mxFrame, w.writerHdr.mxFrame)
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
		// Re-sync maxFrame from the authoritative SHM hdr. Without this, a
		// peer's CheckpointRestart (doResetWAL — wal.go:2712, matching
		// SQLite's walRestartLog at wal.c:3852-3884) that drops mxFrame back
		// to 0 leaves our local maxFrame stale at the pre-reset high-water
		// mark. setBatch's monotonic guard `if f > maxFrame.Load()` then
		// never lowers it, and writeFrames (line 1849) publishes the stale
		// value to SHM as the new commit's mxFrame — larger than the frames
		// actually written, producing ghost frame references for peer
		// readers and the "row count mismatch: got 606/602/320" signatures.
		w.index.maxFrame.Store(hdr.mxFrame)
		// Re-sync nBackfill + nBackfillAttempted + aReadMark[] from SHM so
		// the process-local ckpt snapshot mirrors a peer's RESTART or
		// checkpoint. Same rationale as adoptSHMState (wal.go:1488) —
		// without this, walIndex.get's minFrame filter (uses nBackfill)
		// and any future checkpoint-path reads that consult the local
		// atomics instead of shmNBackfill()/shmReadCkptInfo() would see
		// stale pre-reset values.
		w.index.shmReadCkptInfo()
		// Monotonic update: same guard as tryBeginRead — prevent concurrent
		// reader from racing with the previous commit's writeHeader.
		for {
			old := w.index.maxPage.Load()
			if hdr.nPage <= old || w.index.maxPage.CompareAndSwap(old, hdr.nPage) {
				break
			}
		}
		// Snapshot the re-synced header so future beginWrite calls can
		// detect external changes relative to this baseline. With pageMap
		// removed from the multi-process read path, stateChanged no longer
		// triggers a pageMap rebuild — pager-layer invalidation of the
		// writerCache / cached page 1 is handled by the caller via the
		// returned stateChanged flag.
		w.writerHdr = hdr
	}

	return stateChanged, nil
}

// endWrite releases the exclusive write lock.
// DRIFT: journal_size_limit/mxWalSize WAL truncation unimplemented (truncateOnCommit/walLimitSize) See docs/btree/NOTES.md#drift-105-journal-size-limit-wal-truncation-feature-unimplemented
func (w *wal) endWrite() {
	// Tx boundary: any pending in-tx frame rewrite is moot. Either
	// the tx committed (rewrite happened in writeFrames and iReCksum
	// is already 0) or it rolled back (no commit will ever consume
	// the pending rewrite). Reset unconditionally so a future write
	// tx starts with a clean slate.
	w.iReCksum = 0
	_ = w.index.unlock(lockWrite, lockExclusive)
}

// authoritativeMxFrame returns the highest committed WAL frame from the most
// authoritative source available. In multi-process mode it reads the SHM header
// (written under lockWrite by any process on commit); in single-process or
// in-memory mode it uses the in-process atomic. Falls back to the atomic if the
// SHM read yields a torn/invalid header.
//
// Callers should prefer this over w.index.mxCommitFrame.LoadLocal() whenever the
// result influences truncate or WAL-reset decisions: a stale process-local
// value that undercounts a peer's committed frames can make us reset or
// truncate the WAL while those frames are still only in the WAL file,
// destroying the peer's data. Mirrors SQLite's use of pWal->hdr.mxFrame after
// walIndexReadHdr throughout wal.c (see walCheckpoint, wal.c:2166).
func (w *wal) authoritativeMxFrame() uint32 {
	if !w.inProcess && !w.inMemory {
		if hdr, valid := w.index.readHeader(); valid {
			return hdr.mxFrame
		}
	}
	return w.index.mxCommitFrame.LoadLocal()
}

// checkpoint writes WAL frames back to the database file.
// For InMemory databases, master is used to store checkpointed pages instead of dbFile.
// DRIFT: checkpoint never truncates DB file to committed nPage after full backfill See docs/btree/NOTES.md#drift-50-checkpoint-never-physically-truncates-db-file-after-full-bac
// DRIFT: checkpoint backfill lacks iDbpage>mxPage (committed nPage) skip filter See docs/btree/NOTES.md#drift-51-checkpoint-backfill-missing-idbpage-greater-than-mxpage-filt
// DRIFT: checkpoint omits page-size-mismatch and over-grow corruption guards See docs/btree/NOTES.md#drift-52-checkpoint-missing-page-size-mismatch-and-over-grow-corrupti
func (w *wal) checkpoint(dbFile fileHandle, master *masterStore) error {
	return w.checkpointWithMode(dbFile, master, CheckpointFull, nil)
}

// checkpointPassive performs a passive checkpoint that never blocks.
// Used by auto-checkpoint (issue 6.2) to avoid blocking writers or readers.
// Returns ErrBusy if not all frames were copied (partial checkpoint),
// matching SQLite's SQLITE_BUSY return from sqlite3WalCheckpoint in
// PASSIVE mode when readers block progress. Callers must not truncate
// the WAL when ErrBusy is returned.
// DRIFT: checkpoint never truncates DB file to committed nPage after full backfill See docs/btree/NOTES.md#drift-50-checkpoint-never-physically-truncates-db-file-after-full-bac
func (w *wal) checkpointPassive(dbFile fileHandle, master *masterStore) error {
	err := w.checkpointWithMode(dbFile, master, CheckpointPassive, nil)
	if err != nil {
		return err
	}
	// Check if all frames were backfilled. A partial checkpoint means
	// some frames remain only in the WAL and must not be discarded.
	// Use mxCommitFrame (not maxFrame) so spilled uncommitted frames don't
	// prevent WAL reset. During active spill maxFrame > mxCommitFrame.
	//
	// Source mxCommitFrame from SHM in multi-process mode: a stale
	// process-local value would falsely report "complete" when a peer has
	// committed frames we haven't backfilled. Callers (notably pager.close)
	// then truncate the WAL and destroy the peer's data. See
	// authoritativeMxFrame for rationale.
	complete := w.index.nBackfill.Load() >= w.authoritativeMxFrame()
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
//
// buildBackfillMap scans WAL frame headers in the range [lo, hi) and
// populates w.ckptLatest with pgno → latest frame index. Reuses the
// scratch map across checkpoints — safe because lockCheckpoint is held
// exclusive while buildBackfillMap runs. Disk-mode WAL only.
// Matches SQLite's walIteratorNext (wal.c:1758-1786).
// Complexity: O((hi-lo) * walFrameSize) I/O.
func (w *wal) buildBackfillMap(lo, hi uint32) (map[uint32]uint32, error) {
	if w.ckptLatest == nil {
		// First checkpoint: allocate with a capacity hint.
		w.ckptLatest = make(map[uint32]uint32, hi-lo)
	} else {
		clear(w.ckptLatest)
	}
	if hi <= lo {
		return w.ckptLatest, nil
	}
	frameSize := int64(walFrameSize) + int64(w.pageSize)
	frameBuf := make([]byte, walFrameSize)
	var frame walFrame
	for i := lo; i < hi; i++ {
		off := int64(walHeaderSize) + int64(i)*frameSize
		n, err := w.file.ReadAt(frameBuf, off)
		if err != nil {
			return nil, err
		}
		if n != len(frameBuf) {
			return nil, io.ErrUnexpectedEOF
		}
		frame.deserialize(frameBuf)
		// Overwrite keeps the latest frame, since i increases monotonically.
		w.ckptLatest[frame.pgno] = i
	}
	return w.ckptLatest, nil
}

// rewriteChecksums re-reads frames [w.iReCksum..iLast] and rewrites
// each frame header with a freshly computed checksum chain. Called
// from writeFrames on commit when w.iReCksum > 0.
//
// Mirrors SQLite's walRewriteChecksums (wal.c:3966-4009):
//   - iReCksum == 1: seed chain from the WAL-header checksums at
//     file offset 24 (bytes 24..31 of walHeader).
//   - iReCksum >  1: seed from frame (iReCksum-1)'s header at offset
//     walFrameOffset(iReCksum-1)+16 (the cksum1/cksum2 fields).
//
// Then for each frame i in [iReCksum..iLast]: read existing frame
// header (preserves pgno/dbSize/salts; its cksum fields are stale),
// read the page data (current — possibly overwritten), recompute
// (cksum1, cksum2) over (frameHdr[0:8] + pageData), write the 24-byte
// frame header back with updated cksum1/cksum2 at bytes 16..23.
//
// On success, sets w.iReCksum = 0. On I/O error, w.iReCksum is left
// unchanged; the caller should surface the error as commit failure.
//
// Caller must hold w.mu.
func (w *wal) rewriteChecksums(iLast uint32) error {
	if w.iReCksum == 0 {
		return nil
	}
	if w.file == nil {
		return errors.New("btree: WAL file closed")
	}
	frameSize := int64(walFrameSize) + int64(w.pageSize)

	// Seed the checksum chain. SQLite wal.c:3982-3990.
	var s1, s2 uint32
	if w.iReCksum == 1 {
		// Read cksum1/cksum2 from the WAL header (bytes 24..31 of the
		// 32-byte walHeader).
		var hdrCksumBuf [8]byte
		if _, err := w.file.ReadAt(hdrCksumBuf[:], 24); err != nil {
			return err
		}
		s1 = binary.BigEndian.Uint32(hdrCksumBuf[0:4])
		s2 = binary.BigEndian.Uint32(hdrCksumBuf[4:8])
	} else {
		// Read cksum1/cksum2 from the previous frame's header at bytes
		// 16..23 of the 24-byte walFrame struct.
		prevOff := int64(walHeaderSize) + int64(w.iReCksum-2)*frameSize
		var prevCksumBuf [8]byte
		if _, err := w.file.ReadAt(prevCksumBuf[:], prevOff+16); err != nil {
			return err
		}
		s1 = binary.BigEndian.Uint32(prevCksumBuf[0:4])
		s2 = binary.BigEndian.Uint32(prevCksumBuf[4:8])
	}

	// Scratch buffers: frame header + page data.
	var frameHdr [walFrameSize]byte
	pageData := make([]byte, w.pageSize)

	// Walk frames iReCksum..iLast (inclusive). SQLite wal.c:3992-4005.
	for i := w.iReCksum; i <= iLast; i++ {
		off := int64(walHeaderSize) + int64(i-1)*frameSize

		// Read the existing frame header + page data.
		if _, err := w.file.ReadAt(frameHdr[:], off); err != nil {
			return err
		}
		if _, err := w.file.ReadAt(pageData, off+walFrameSize); err != nil {
			return err
		}

		// Recompute checksums over (header-first-8 + page data).
		s1, s2 = walChecksum(frameHdr[0:8], s1, s2)
		s1, s2 = walChecksum(pageData, s1, s2)

		// Rewrite the cksum fields in the header at bytes [16..24].
		binary.BigEndian.PutUint32(frameHdr[16:20], s1)
		binary.BigEndian.PutUint32(frameHdr[20:24], s2)

		// Write back the 24-byte header only (page data unchanged).
		if _, err := w.file.WriteAt(frameHdr[:], off); err != nil {
			return err
		}
	}

	// Success — propagate the final chain cksum to w.cksum1/cksum2 so
	// the NEXT tx's first appended frame chains from the correct seed.
	// Without this update, w.cksum1/cksum2 would still hold the stale
	// in-memory accumulator that skipped reused frames, breaking the
	// chain on the next tx (recovery would reject the next-tx frames).
	w.cksum1 = s1
	w.cksum2 = s2

	// Reset for next transaction. SQLite wal.c:3993.
	w.iReCksum = 0
	return nil
}

// mxFrame source (multi-process): `nf` is read via authoritativeMxFrame,
// which consults the SHM header under lockCheckpoint. This mirrors SQLite's
// walCheckpoint using pWal->hdr.mxFrame. Reading the process-local
// mxCommitFrame would undercount committed frames written by peer
// processes, causing a close-time checkpoint to only backfill our own
// frames before truncate — losing the peer's data (commit 9023f5b).
// DRIFT: FULL/RESTART/TRUNCATE checkpoint returns nil vs SQLITE_BUSY on incomplete backfill See docs/btree/NOTES.md#drift-49-non-passive-checkpoint-returns-success-instead-of-busy-on-in
// DRIFT: checkpoint never truncates DB file to committed nPage after full backfill See docs/btree/NOTES.md#drift-50-checkpoint-never-physically-truncates-db-file-after-full-bac
// DRIFT: checkpoint backfill lacks iDbpage>mxPage (committed nPage) skip filter See docs/btree/NOTES.md#drift-51-checkpoint-backfill-missing-idbpage-greater-than-mxpage-filt
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
	//
	// In multi-process mode, read mxFrame from the SHM header via
	// authoritativeMxFrame, not the process-local mxCommitFrame. A sibling
	// process may have committed frames we haven't synced — without this,
	// our close-time checkpoint would only backfill our own frames, then
	// truncate the WAL and lose the sibling's data (measured as the
	// residual ~4% failure in TestMultiProcessIndex_ConcurrentSketchUpdates).
	nf := w.authoritativeMxFrame()
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
		// Phase 1: latest frame per pgno. Same reasoning as disk mode
		// (see SQLite walIteratorNext wal.c:1758-1786). Reuse the same
		// w.ckptLatest / w.ckptPgnos scratch the disk path uses —
		// lockCheckpoint serializes this caller.
		if w.ckptLatest == nil {
			w.ckptLatest = make(map[uint32]uint32, mxSafeFrame-nBackfill)
		} else {
			clear(w.ckptLatest)
		}
		latest := w.ckptLatest
		for i := nBackfill; i < mxSafeFrame; i++ {
			latest[w.memFrames[i].pgno] = i
		}

		// Phase 2: pgno-sorted iteration for write locality.
		pgnos := w.ckptPgnos[:0]
		if cap(pgnos) < len(latest) {
			pgnos = make([]uint32, 0, len(latest))
		}
		for pgno := range latest {
			pgnos = append(pgnos, pgno)
		}
		w.ckptPgnos = pgnos
		sort.Slice(pgnos, func(a, b int) bool { return pgnos[a] < pgnos[b] })

		for _, pgno := range pgnos {
			mf := &w.memFrames[latest[pgno]]
			if dbFile != nil {
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
				master.writePage(mf.pgno, mf.data)
			}
		}
	} else {
		if w.file == nil {
			_ = w.index.unlock(lockRead0, lockExclusive)
			return errors.New("btree: WAL file closed")
		}
		frameSize := int64(walFrameSize) + int64(w.pageSize)
		// Reuse w.ckptBuf for page data reads, avoiding per-checkpoint allocations.
		// Matches SQLite's walCheckpoint (wal.c:2285-2304) which reuses the
		// pager's pTmpSpace buffer across the entire backfill loop.
		if w.ckptBuf == nil {
			w.ckptBuf = make([]byte, w.pageSize)
		}
		pageData := w.ckptBuf

		// Phase 1: scan frames (nBackfill..mxSafeFrame), keep the latest
		// frame per pgno. Matches SQLite walIteratorNext semantics
		// (wal.c:1758-1786) — a page that appears multiple times in the
		// WAL only needs its last version written to the DB file.
		latest, err := w.buildBackfillMap(nBackfill, mxSafeFrame)
		if err != nil {
			backfillErr = err
		} else {
			// Phase 2: iterate pgnos in ascending order for sequential
			// DB-file write locality (matches SQLite walIteratorNext which
			// sorts per-segment and emits in pgno asc). Reuse w.ckptPgnos
			// scratch — lockCheckpoint serializes this caller.
			pgnos := w.ckptPgnos[:0]
			if cap(pgnos) < len(latest) {
				pgnos = make([]uint32, 0, len(latest))
			}
			for pgno := range latest {
				pgnos = append(pgnos, pgno)
			}
			w.ckptPgnos = pgnos
			sort.Slice(pgnos, func(a, b int) bool { return pgnos[a] < pgnos[b] })

			// BEGIN ENCRYPTION
			// Checkpoint uses copy-verbatim: if a codec is installed, the WAL
			// frame payload is already ciphertext produced by the same codec,
			// with the same per-page-1 plaintext-prefix handling as the DB
			// file expects. Writing the bytes unchanged to the DB file
			// produces a valid encrypted DB page. No decrypt-then-re-encrypt
			// roundtrip is needed because (a) nonce reuse is safe here — each
			// (key, pgno) pair only sees a given plaintext once per frame,
			// since each new modification gets a fresh WAL frame with a fresh
			// nonce — and (b) WAL ciphertext and DB-file ciphertext share
			// codec, key, and layout. See docs/btree/plans/encryption-plan.md Task 8.
			// Matches SQLCipher wal.c:2309-2315 (OsRead → OsWrite with no
			// codec hook between them).
			// END ENCRYPTION
			for _, pgno := range pgnos {
				frameIdx := latest[pgno]
				off := int64(walHeaderSize) + int64(frameIdx)*frameSize
				n, err := w.file.ReadAt(pageData, off+walFrameSize)
				if err != nil {
					backfillErr = err
					break
				}
				if n != len(pageData) {
					backfillErr = io.ErrUnexpectedEOF
					break
				}
				pageOffset := int64(pgno-1) * pageSz
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
// DRIFT: FULL/RESTART/TRUNCATE checkpoint returns nil vs SQLITE_BUSY on incomplete backfill See docs/btree/NOTES.md#drift-49-non-passive-checkpoint-returns-success-instead-of-busy-on-in
func (w *wal) checkpointPost(mode CheckpointMode, xBusy BusyHandler) error {
	backfill := w.index.nBackfill.Load()

	// Use mxCommitFrame (not nFrame) so spilled uncommitted frames don't
	// prevent WAL reset when all committed frames are checkpointed.
	//
	// Source mxCommitFrame from SHM in multi-process mode: see
	// authoritativeMxFrame. A stale process-local value could make us
	// proceed to tryResetWAL after only backfilling our own frames, wiping
	// a peer's committed frames that we never saw.
	if backfill < w.authoritativeMxFrame() {
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
// DRIFT: WAL header checkpoint-seq nCkpt hardcoded 0, never incremented on restart See docs/btree/NOTES.md#drift-98-wal-checkpoint-sequence-number-nckpt-never-incremented
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
		w.initHeaderStateLocked()
		return nil
	}

	return w.writeHeader()
}

// truncateFile truncates the WAL file to zero bytes under the WAL mutex.
// DRIFT: journal_size_limit/mxWalSize WAL truncation unimplemented (truncateOnCommit/walLimitSize) See docs/btree/NOTES.md#drift-105-journal-size-limit-wal-truncation-feature-unimplemented
func (w *wal) truncateFile() {
	w.mu.Lock()
	if w.file != nil {
		_ = w.file.Truncate(0)
	}
	w.mu.Unlock()
}

// close closes the WAL file and shared memory. If isLastClient is true, the
// shm backing file is unlinked (see shm.close).
func (w *wal) close(isLastClient bool) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var firstErr error
	if w.index != nil {
		if err := w.index.close(isLastClient); err != nil && firstErr == nil {
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
