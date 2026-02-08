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
	return &walIndex{
		shm:     s,
		pageMap: make(map[uint32]uint32),
	}, nil
}

// set records a page at a given frame position.
func (wi *walIndex) set(pgno, frame uint32) {
	wi.mu.Lock()
	wi.pageMap[pgno] = frame
	if frame > wi.maxFrame {
		wi.maxFrame = frame
	}
	wi.mu.Unlock()
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

// reset clears the WAL index (after a checkpoint).
func (wi *walIndex) reset() {
	wi.mu.Lock()
	defer wi.mu.Unlock()
	clear(wi.pageMap)
	wi.maxFrame = 0
	wi.nBackfill = 0
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

// beginRead acquires a shared read lock and returns the current max frame
// number for snapshot isolation.
func (w *wal) beginRead() (uint32, error) {
	// Acquire a shared lock on a reader slot.
	// For simplicity, we use lockRead0. A full implementation would
	// cycle through reader slots like SQLite does.
	if err := w.index.lock(lockRead0, lockShared); err != nil {
		return 0, err
	}

	w.index.mu.RLock()
	maxFrame := w.index.maxFrame
	w.index.mu.RUnlock()
	return maxFrame, nil
}

// endRead releases the reader lock.
func (w *wal) endRead() {
	_ = w.index.unlock(lockRead0, lockShared)
}

// beginWrite acquires the exclusive write lock.
func (w *wal) beginWrite() error {
	return w.index.lock(lockWrite, lockExclusive)
}

// endWrite releases the exclusive write lock.
func (w *wal) endWrite() {
	_ = w.index.unlock(lockWrite, lockExclusive)
}

// checkpoint writes WAL frames back to the database file (passive checkpoint).
// It only checkpoints frames that are not needed by any active reader.
func (w *wal) checkpoint(dbFile *os.File) error {
	// Acquire checkpoint lock
	if err := w.index.lock(lockCheckpoint, lockExclusive); err != nil {
		return err
	}
	defer func() { _ = w.index.unlock(lockCheckpoint, lockExclusive) }()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.nFrame == 0 {
		return nil
	}

	// Try to get exclusive lock on reader slot to ensure no active readers.
	// This is the passive checkpoint approach - if readers exist, skip.
	if err := w.index.lock(lockRead0, lockExclusive); err != nil {
		return nil // readers active, skip checkpoint (passive mode)
	}
	defer func() { _ = w.index.unlock(lockRead0, lockExclusive) }()

	pageSz := int64(w.pageSize)

	if w.memFrames != nil {
		// In-memory WAL: write directly from memFrames to DB file
		for i := uint32(0); i < w.nFrame; i++ {
			mf := &w.memFrames[i]
			pageOffset := int64(mf.pgno-1) * pageSz
			if _, err := dbFile.WriteAt(mf.data, pageOffset); err != nil {
				return err
			}
		}
	} else {
		// File-based WAL: read frames then write pages to DB
		frameSize := int64(walFrameSize) + int64(w.pageSize)
		walDataSize := int64(w.nFrame) * frameSize
		walData := make([]byte, walDataSize)
		if _, err := w.file.ReadAt(walData, int64(walHeaderSize)); err != nil {
			return err
		}

		var frame walFrame
		for i := uint32(0); i < w.nFrame; i++ {
			off := int64(i) * frameSize
			frame.deserialize(walData[off:])
			pageData := walData[off+walFrameSize : off+frameSize]
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
