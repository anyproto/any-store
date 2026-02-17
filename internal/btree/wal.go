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

import (
	"encoding/binary"
	"hash/crc32"
	"math/bits"
	"math/rand/v2"
	"os"
	"sync"
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
	htNPage    = 4096  // max frame entries per hash segment (power of 2)
	htNSlot    = 8192  // hash slots per segment (2 * htNPage, power of 2)
	htHash1    = 383   // hash multiplier (prime)
	htHdrSize  = 136   // header area in region 0 (two copies of WalIndexHdr + WalCkptInfo)

	// Checkpoint info offsets in region 0
	htCkptOff     = 96  // nBackfill (4 bytes)
	htReadMarkOff = 100 // aReadMark[0..4] (5 * 4 = 20 bytes)

	// Hash table data offsets
	htPgnoOff0     = htHdrSize   // aPgno start in region 0 (byte 136)
	htHashArrayOff = htNPage * 4 // aHash start in all regions (byte 16384)

	// Number of frame entries in region 0 (reduced by header)
	htNPageOne = htNPage - (htHdrSize / 4) // 4062
)

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
// Uses unsafe pointer arithmetic to eliminate bounds checking in the hot loop.
func walChecksum(data []byte, s1, s2 uint32) (uint32, uint32) {
	if len(data) < 4 {
		return s1, s2
	}
	n := len(data) / 4
	p := unsafe.Pointer(&data[0])
	i := 0
	// Unrolled 8x for throughput
	for ; i+7 < n; i += 8 {
		base := unsafe.Add(p, uintptr(i)*4)
		w0 := bits.ReverseBytes32(*(*uint32)(base))
		s1 += w0 + s2
		s2 += s1
		w1 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 4)))
		s1 += w1 + s2
		s2 += s1
		w2 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 8)))
		s1 += w2 + s2
		s2 += s1
		w3 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 12)))
		s1 += w3 + s2
		s2 += s1
		w4 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 16)))
		s1 += w4 + s2
		s2 += s1
		w5 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 20)))
		s1 += w5 + s2
		s2 += s1
		w6 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 24)))
		s1 += w6 + s2
		s2 += s1
		w7 := bits.ReverseBytes32(*(*uint32)(unsafe.Add(base, 28)))
		s1 += w7 + s2
		s2 += s1
	}
	for ; i < n; i++ {
		w := bits.ReverseBytes32(*(*uint32)(unsafe.Add(p, uintptr(i)*4)))
		s1 += w + s2
		s2 += s1
	}
	return s1, s2
}

// readMarkNotUsed is the sentinel value for an unused read mark slot.
const readMarkNotUsed = uint32(0xFFFFFFFF)

// walIndex manages the WAL index stored in shared memory.
// It maps page numbers to their latest WAL frame positions.
// The index is backed by the shm interface, which may be mmap'd
// for multi-process access or heap-backed for single-process.
type walIndex struct {
	mu        sync.RWMutex
	shm       shm               // platform-specific shared memory
	pageMap   map[uint32]uint32 // pgno -> frame index (1-based), in-process cache
	maxFrame  uint32            // highest valid frame
	maxPage   uint32            // database size at last commit
	nBackfill uint32            // frames already checkpointed

	// aReadMark tracks each reader's WAL snapshot position.
	// Slot 0 is special: readers on slot 0 read entirely from the DB (nBackfill == maxFrame).
	// Slots 1-4 are for readers that need WAL frames.
	aReadMark [5]uint32
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
		shm:     s,
		pageMap: make(map[uint32]uint32),
	}
	// Initialize all read marks as unused
	for i := range wi.aReadMark {
		wi.aReadMark[i] = readMarkNotUsed
	}
	return wi, nil
}

// set records a page at a given frame position.
func (wi *walIndex) set(pgno, frame uint32) {
	wi.mu.Lock()
	wi.pageMap[pgno] = frame
	if frame > wi.maxFrame {
		wi.maxFrame = frame
	}
	wi.mu.Unlock()
	wi.shmHashWrite(pgno, frame)
}

// setBatch records multiple page→frame mappings under a single lock.
func (wi *walIndex) setBatch(pages []*page, startFrame uint32) {
	wi.mu.Lock()
	for i, p := range pages {
		frame := startFrame + uint32(i)
		wi.pageMap[p.pgno] = frame
	}
	if f := startFrame + uint32(len(pages)) - 1; f > wi.maxFrame {
		wi.maxFrame = f
	}
	wi.mu.Unlock()
	// Write to shm hash tables for cross-process visibility
	for i, p := range pages {
		wi.shmHashWrite(p.pgno, startFrame+uint32(i))
	}
}

// get returns the frame containing the latest version of pgno, or 0 if not in WAL.
// The maxFrame parameter limits which frames are visible (for snapshot isolation).
func (wi *walIndex) get(pgno, maxFrame uint32) uint32 {
	wi.mu.RLock()
	defer wi.mu.RUnlock()
	frame := wi.pageMap[pgno]
	if frame > 0 && frame <= maxFrame {
		return frame
	}
	return 0
}

// reset clears the WAL index (after a checkpoint + WAL truncate).
func (wi *walIndex) reset() {
	wi.mu.Lock()
	defer wi.mu.Unlock()
	clear(wi.pageMap)
	wi.maxFrame = 0
	wi.nBackfill = 0
	for i := range wi.aReadMark {
		wi.aReadMark[i] = readMarkNotUsed
	}
	wi.shmClearHash()
	wi.shmWriteCkptInfo()
}

// writeHeader writes the WAL index header to region 0 of the shm.
// This allows other processes to discover the WAL state.
func (wi *walIndex) writeHeader(maxFrame, maxPage, nBackfill uint32) error {
	region, err := wi.shm.region(0, true)
	if err != nil {
		return err
	}

	// Write header at offset 0 in region 0.
	// Format: maxFrame(4) + maxPage(4) + nBackfill(4) + reserved(36) = 48 bytes
	binary.LittleEndian.PutUint32(region[0:4], maxFrame)
	binary.LittleEndian.PutUint32(region[4:8], maxPage)
	binary.LittleEndian.PutUint32(region[8:12], nBackfill)

	// Write a second copy for atomicity detection (at offset 48).
	binary.LittleEndian.PutUint32(region[48:52], maxFrame)
	binary.LittleEndian.PutUint32(region[52:56], maxPage)
	binary.LittleEndian.PutUint32(region[56:60], nBackfill)
	return nil
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
// readers to find page→frame mappings without scanning the WAL file.
//
// Layout per region:
//   Region 0: [header 136B][aPgno 4062×4B][aHash 8192×2B] = 32768 bytes
//   Region i: [aPgno 4096×4B][aHash 8192×2B] = 32768 bytes
//
// aPgno[idx] stores the page number for the frame at position (iZero + idx + 1).
// aHash is a linear-probing hash table: hash(pgno) → (idx+1), where 0 = empty.

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

// shmHashWrite records a page→frame mapping in the shm hash table.
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
func (wi *walIndex) shmHashGet(pgno, maxFrame uint32) uint32 {
	if maxFrame == 0 {
		return 0
	}

	lastSeg, _ := htFrameSegIdx(maxFrame)

	for seg := lastSeg; seg >= 0; seg-- {
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
					if frame <= maxFrame && frame > bestFrame {
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

// shmWriteCkptInfo writes checkpoint info (nBackfill, aReadMark) to shm region 0.
func (wi *walIndex) shmWriteCkptInfo() {
	region, err := wi.shm.region(0, true)
	if err != nil {
		return
	}
	binary.LittleEndian.PutUint32(region[htCkptOff:], wi.nBackfill)
	for i := range 5 {
		binary.LittleEndian.PutUint32(region[htReadMarkOff+i*4:], wi.aReadMark[i])
	}
}

// shmReadCkptInfo reads checkpoint info (nBackfill, aReadMark) from shm region 0.
func (wi *walIndex) shmReadCkptInfo() {
	region, err := wi.shm.region(0, false)
	if err != nil {
		return
	}
	wi.nBackfill = binary.LittleEndian.Uint32(region[htCkptOff:])
	for i := range 5 {
		wi.aReadMark[i] = binary.LittleEndian.Uint32(region[htReadMarkOff+i*4:])
	}
}

// wal manages the Write-Ahead Log.
type wal struct {
	mu       sync.Mutex
	file     *os.File
	header   walHeader
	index    *walIndex
	pageSize uint32
	path     string
	nFrame   uint32     // total frames written
	readers  sync.Mutex // protects reader slot allocation

	// Cumulative checksum state for appending frames
	cksum1 uint32
	cksum2 uint32

	// Reusable write buffer to avoid per-commit allocations
	writeBuf []byte

	// inProcess uses heap-backed shm instead of mmap+fcntl (faster, single-process only)
	inProcess bool

	// noSync skips fdatasync on WAL commit (like SQLite synchronous=normal)
	noSync bool

	// memFrames stores page data in memory for InProcess+NoSync mode,
	// eliminating per-commit file I/O. Frames are flushed to disk on checkpoint.
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

	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE, 0666)
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

	// Acquire recover lock to prevent concurrent recovery.
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

	// New WAL - write header
	if err := w.writeHeader(); err != nil {
		return err
	}
	// Enable in-memory WAL for InProcess+NoSync (no per-commit file I/O)
	if w.inProcess && w.noSync {
		w.memFrames = make([]memFrame, 0, 1024)
	}
	return nil
}

// writeHeader writes a fresh WAL header.
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
	if err := w.file.Sync(); err != nil {
		return err
	}

	// Initialize checksum state from header
	w.cksum1, w.cksum2 = walChecksum(buf[0:24], 0, 0)
	w.nFrame = 0
	w.index.reset()

	// Update shm header
	return w.index.writeHeader(0, 0, 0)
}

// recover reads the WAL file and rebuilds the index from committed frames.
func (w *wal) recover() error {
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
		}

		offset += frameSize
	}

	// Only keep frames up to the last commit
	if lastCommitFrame > 0 {
		w.nFrame = lastCommitFrame
		w.index.maxPage = lastCommitDbSize

		// Rebuild index with only committed frames
		w.index.reset()
		offset = int64(walHeaderSize)
		s1, s2 = w.cksum1, w.cksum2

		for i := uint32(1); i <= lastCommitFrame; i++ {
			if _, err := w.file.ReadAt(frameHeaderBuf, offset); err != nil {
				return err
			}
			if _, err := w.file.ReadAt(pageBuf, offset+walFrameSize); err != nil {
				return err
			}
			frame.deserialize(frameHeaderBuf)
			s1, s2 = walChecksum(frameHeaderBuf[0:8], s1, s2)
			s1, s2 = walChecksum(pageBuf, s1, s2)

			w.index.set(frame.pgno, i)
			offset += frameSize
		}

		w.cksum1 = s1
		w.cksum2 = s2
		w.index.maxFrame = lastCommitFrame
		w.index.maxPage = lastCommitDbSize

		// Truncate uncommitted trailing frames
		truncAt := int64(walHeaderSize) + int64(lastCommitFrame)*frameSize
		if err := w.file.Truncate(truncAt); err != nil {
			return err
		}
	} else {
		// No committed frames - reset WAL
		w.nFrame = 0
		w.index.reset()
	}

	// Update shm header with recovered state
	return w.index.writeHeader(w.index.maxFrame, w.index.maxPage, 0)
}

// writeFrames appends frames to the WAL. If commit is true, the last frame
// is marked as a commit frame with the given dbSize.
// All frames are batched into a single write call for performance.
func (w *wal) writeFrames(pages []*page, commit bool, dbSize uint32) error {
	if len(pages) == 0 {
		return nil
	}

	// Fast path: in-memory WAL for InProcess+NoSync mode.
	// Stores page data in memory, skipping file I/O and checksums entirely.
	if w.memFrames != nil {
		return w.writeFramesMem(pages, commit, dbSize)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	frameSize := int(walFrameSize) + int(w.pageSize)
	offset := int64(walHeaderSize) + int64(w.nFrame)*int64(frameSize)

	// Reuse write buffer to avoid per-commit allocations
	needSize := len(pages) * frameSize
	if cap(w.writeBuf) >= needSize {
		w.writeBuf = w.writeBuf[:needSize]
	} else {
		w.writeBuf = make([]byte, needSize)
	}
	buf := w.writeBuf

	s1, s2 := w.cksum1, w.cksum2
	startFrame := w.nFrame + 1

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

	w.nFrame += uint32(len(pages))
	w.cksum1 = s1
	w.cksum2 = s2

	// Single write call for all frames
	if _, err := w.file.WriteAt(buf, offset); err != nil {
		return err
	}

	// Batch update walIndex under a single lock
	w.index.setBatch(pages, startFrame)

	if commit {
		if dbSize > 0 {
			w.index.mu.Lock()
			w.index.maxPage = dbSize
			w.index.mu.Unlock()
		}
		if !w.noSync {
			if err := fdatasync(w.file); err != nil {
				return err
			}
		}
		if !w.inProcess {
			return w.index.writeHeader(w.index.maxFrame, w.index.maxPage, w.index.nBackfill)
		}
	}

	return nil
}

// writeFramesMem is the fast in-memory path for writeFrames.
// No file I/O, no checksums — just copy page data into a pre-allocated arena.
func (w *wal) writeFramesMem(pages []*page, commit bool, dbSize uint32) error {
	startFrame := w.nFrame + 1
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
		w.nFrame++
	}

	// Batch update walIndex
	w.index.setBatch(pages, startFrame)

	if commit && dbSize > 0 {
		w.index.mu.Lock()
		w.index.maxPage = dbSize
		w.index.mu.Unlock()
	}

	return nil
}

// readFrame reads the page data for a given frame number.
func (w *wal) readFrame(frame uint32, buf []byte) error {
	if frame == 0 || frame > w.nFrame {
		return ErrWALCorrupt
	}
	// Fast path: read from in-memory frames
	if w.memFrames != nil {
		idx := frame - 1
		if idx < uint32(len(w.memFrames)) {
			copy(buf[:w.pageSize], w.memFrames[idx].data)
			return nil
		}
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
	w.index.mu.RLock()
	mxFrame := w.index.maxFrame
	nBackfill := w.index.nBackfill
	w.index.mu.RUnlock()

	if mxFrame == 0 || nBackfill == mxFrame {
		// All frames are checkpointed. Use slot 0 (read from DB, skip WAL).
		if err := w.index.lock(lockRead0, lockShared); err != nil {
			return 0, 0, err
		}
		w.index.mu.Lock()
		w.index.aReadMark[0] = mxFrame
		w.index.mu.Unlock()
		return mxFrame, 0, nil
	}

	// Find the best reader slot (1-4).
	// Best slot = one with largest readmark <= mxFrame.
	// If none has a valid mark, find an unused slot.
	bestSlot := -1
	bestMark := uint32(0)

	w.index.mu.RLock()
	for i := 1; i <= 4; i++ {
		mark := w.index.aReadMark[i]
		if mark != readMarkNotUsed && mark <= mxFrame && mark > bestMark {
			bestSlot = i
			bestMark = mark
		}
	}
	w.index.mu.RUnlock()

	if bestSlot != -1 {
		// Try to acquire the best slot
		lockSlot := lockRead0 + bestSlot
		if err := w.index.lock(lockSlot, lockShared); err == nil {
			return mxFrame, bestSlot, nil
		}
		// If lock fails, fall through to find an unused slot
	}

	// Find an unused slot and set its mark to mxFrame
	for i := 1; i <= 4; i++ {
		lockSlot := lockRead0 + i
		if err := w.index.lock(lockSlot, lockShared); err == nil {
			w.index.mu.Lock()
			w.index.aReadMark[i] = mxFrame
			w.index.mu.Unlock()
			return mxFrame, i, nil
		}
	}

	// All slots busy — fall back to slot 0
	if err := w.index.lock(lockRead0, lockShared); err != nil {
		return 0, 0, err
	}
	w.index.mu.Lock()
	w.index.aReadMark[0] = mxFrame
	w.index.mu.Unlock()
	return mxFrame, 0, nil
}

// endRead releases the reader lock for the given slot.
func (w *wal) endRead(slot int) {
	_ = w.index.unlock(lockRead0+slot, lockShared)
}

// beginWrite acquires the exclusive write lock.
func (w *wal) beginWrite() error {
	return w.index.lock(lockWrite, lockExclusive)
}

// endWrite releases the exclusive write lock.
func (w *wal) endWrite() {
	_ = w.index.unlock(lockWrite, lockExclusive)
}

// checkpoint writes WAL frames back to the database file.
// It implements SQLite's FULL checkpoint mode:
//   - Blocks new writers (acquires lockWrite)
//   - Does NOT block readers
//   - Computes mxSafeFrame: the highest frame that can be safely copied,
//     limited by the oldest active reader's readmark
//   - Only resets the WAL when ALL frames are checkpointed AND no readers
//     are active on slots 1-4
func (w *wal) checkpoint(dbFile *os.File) error {
	// Acquire checkpoint lock — serialize concurrent checkpoints
	if err := w.index.lock(lockCheckpoint, lockExclusive); err != nil {
		return err
	}
	defer func() { _ = w.index.unlock(lockCheckpoint, lockExclusive) }()

	// Acquire write lock — block new writers during checkpoint
	if err := w.index.lock(lockWrite, lockExclusive); err != nil {
		_ = w.index.unlock(lockCheckpoint, lockExclusive)
		return err
	}
	defer func() { _ = w.index.unlock(lockWrite, lockExclusive) }()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.nFrame == 0 {
		return nil
	}

	// Compute mxSafeFrame: the highest frame we can safely copy to DB.
	// Start with all frames, then lower based on active readers.
	mxSafeFrame := w.nFrame

	for i := 0; i < 5; i++ {
		lockSlot := lockRead0 + i
		// Try exclusive lock on this reader slot
		if err := w.index.lock(lockSlot, lockExclusive); err == nil {
			// No reader on this slot — release immediately
			_ = w.index.unlock(lockSlot, lockExclusive)
			continue
		}
		// Reader active on this slot — check its readmark
		w.index.mu.RLock()
		mark := w.index.aReadMark[i]
		w.index.mu.RUnlock()
		if mark != readMarkNotUsed && mark < mxSafeFrame {
			mxSafeFrame = mark
		}
	}

	// nBackfill is the number of frames already copied to DB
	w.index.mu.RLock()
	nBackfill := w.index.nBackfill
	w.index.mu.RUnlock()

	if mxSafeFrame <= nBackfill {
		// Nothing new to checkpoint
		return nil
	}

	// Copy frames (nBackfill+1)..mxSafeFrame to DB
	pageSz := int64(w.pageSize)

	if w.memFrames != nil {
		for i := nBackfill; i < mxSafeFrame; i++ {
			mf := &w.memFrames[i]
			pageOffset := int64(mf.pgno-1) * pageSz
			if _, err := dbFile.WriteAt(mf.data, pageOffset); err != nil {
				return err
			}
		}
	} else {
		frameSize := int64(walFrameSize) + int64(w.pageSize)
		var frame walFrame
		frameBuf := make([]byte, walFrameSize)

		for i := nBackfill; i < mxSafeFrame; i++ {
			off := int64(walHeaderSize) + int64(i)*frameSize
			if _, err := w.file.ReadAt(frameBuf, off); err != nil {
				return err
			}
			frame.deserialize(frameBuf)

			pageData := make([]byte, w.pageSize)
			if _, err := w.file.ReadAt(pageData, off+walFrameSize); err != nil {
				return err
			}

			pageOffset := int64(frame.pgno-1) * pageSz
			if _, err := dbFile.WriteAt(pageData, pageOffset); err != nil {
				return err
			}
		}
	}

	// Sync the database file
	if err := fdatasync(dbFile); err != nil {
		return err
	}

	// Update nBackfill
	w.index.mu.Lock()
	w.index.nBackfill = mxSafeFrame
	w.index.mu.Unlock()
	w.index.shmWriteCkptInfo()

	// If all frames are checkpointed, try to reset the WAL
	if mxSafeFrame == w.nFrame {
		return w.tryResetWAL()
	}

	return nil
}

// tryResetWAL attempts to reset the WAL file after a full checkpoint.
// Only succeeds if no readers are active on slots 1-4.
// Must be called with w.mu held and lockCheckpoint + lockWrite acquired.
func (w *wal) tryResetWAL() error {
	// Check that no readers are active on slots 1-4.
	// Slot 0 readers are OK — they read from DB only.
	allFree := true
	for i := 1; i <= 4; i++ {
		lockSlot := lockRead0 + i
		if err := w.index.lock(lockSlot, lockExclusive); err == nil {
			_ = w.index.unlock(lockSlot, lockExclusive)
		} else {
			allFree = false
			break
		}
	}

	if !allFree {
		return nil // readers still active, can't reset WAL
	}

	// Reset WAL file
	if err := w.file.Truncate(0); err != nil {
		return err
	}

	w.index.reset()
	w.nFrame = 0

	// Reset memFrames and arena for reuse
	if w.memFrames != nil {
		w.memFrames = w.memFrames[:0]
		w.memArenaOff = 0
	}

	return w.writeHeader()
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
