package btree

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWALHeaderSerializeDeserialize(t *testing.T) {
	h := walHeader{
		magic:      walMagic,
		version:    walVersion,
		pageSize:   4096,
		checkpoint: 5,
		salt1:      0xdeadbeef,
		salt2:      0xcafebabe,
	}

	buf := make([]byte, walHeaderSize)
	h.serialize(buf)

	var h2 walHeader
	require.NoError(t, h2.deserialize(buf))
	assert.Equal(t, h.magic, h2.magic)
	assert.Equal(t, h.version, h2.version)
	assert.Equal(t, h.pageSize, h2.pageSize)
	assert.Equal(t, h.checkpoint, h2.checkpoint)
	assert.Equal(t, h.salt1, h2.salt1)
	assert.Equal(t, h.salt2, h2.salt2)
}

func TestWALHeaderNotSQLiteCompatible(t *testing.T) {
	assert.NotEqual(t, uint32(0x377f0682), walMagic)
	assert.NotEqual(t, uint32(3007000), walVersion)
}

func TestWALHeaderDeserializeCorrupt(t *testing.T) {
	var h walHeader

	// Too short
	assert.ErrorIs(t, h.deserialize(make([]byte, 10)), ErrWALCorrupt)

	// Wrong magic
	buf := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], 0x12345678)
	assert.ErrorIs(t, h.deserialize(buf), ErrWALCorrupt)

	// Bad checksum
	buf2 := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(buf2[0:4], walMagic)
	binary.BigEndian.PutUint32(buf2[24:28], 0xbad)
	binary.BigEndian.PutUint32(buf2[28:32], 0xbad)
	assert.ErrorIs(t, h.deserialize(buf2), ErrWALCorrupt)
}

func TestWALChecksum(t *testing.T) {
	data := make([]byte, 16)
	binary.BigEndian.PutUint32(data[0:4], 1)
	binary.BigEndian.PutUint32(data[4:8], 2)
	binary.BigEndian.PutUint32(data[8:12], 3)
	binary.BigEndian.PutUint32(data[12:16], 4)

	s1, s2 := walChecksum(data, 0, 0)
	assert.NotEqual(t, uint32(0), s1)
	assert.NotEqual(t, uint32(0), s2)

	// Same data should produce same checksum
	s1b, s2b := walChecksum(data, 0, 0)
	assert.Equal(t, s1, s1b)
	assert.Equal(t, s2, s2b)

	// Different data should produce different checksum
	binary.BigEndian.PutUint32(data[0:4], 99)
	s1c, s2c := walChecksum(data, 0, 0)
	assert.NotEqual(t, s1, s1c)
	assert.NotEqual(t, s2, s2c)
}

func TestWALChecksumCumulative(t *testing.T) {
	data1 := make([]byte, 8)
	data2 := make([]byte, 8)
	binary.BigEndian.PutUint32(data1[0:4], 10)
	binary.BigEndian.PutUint32(data1[4:8], 20)
	binary.BigEndian.PutUint32(data2[0:4], 30)
	binary.BigEndian.PutUint32(data2[4:8], 40)

	// Cumulative: checksum data1 then data2
	s1, s2 := walChecksum(data1, 0, 0)
	s1, s2 = walChecksum(data2, s1, s2)

	// Should differ from checksum of data2 alone
	s1b, s2b := walChecksum(data2, 0, 0)
	assert.NotEqual(t, s1, s1b)
	assert.NotEqual(t, s2, s2b)
}

func TestWALFrameSerializeDeserialize(t *testing.T) {
	f := walFrame{
		pgno:      42,
		dbSize:    10,
		salt1:     0xaabbccdd,
		salt2:     0x11223344,
		checksum1: 0x55667788,
		checksum2: 0x99aabbcc,
	}

	buf := make([]byte, walFrameSize)
	f.serialize(buf)

	var f2 walFrame
	f2.deserialize(buf)
	assert.Equal(t, f.pgno, f2.pgno)
	assert.Equal(t, f.dbSize, f2.dbSize)
	assert.Equal(t, f.salt1, f2.salt1)
	assert.Equal(t, f.salt2, f2.salt2)
	assert.Equal(t, f.checksum1, f2.checksum1)
	assert.Equal(t, f.checksum2, f2.checksum2)
}

func TestWALOpenClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	require.NoError(t, w.close(false))

	// WAL file should exist
	_, err := os.Stat(path)
	require.NoError(t, err)
}

func TestWALWriteReadFrames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())

	// Acquire write lock
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Create test pages
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "page1 data here")
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "page2 data here")

	// Write frames with commit
	require.NoError(t, w.writeFrames([]*page{pg1, pg2}, true, 2))
	assert.Equal(t, uint32(2), w.nFrame.Load())
	assert.Equal(t, uint32(2), w.index.maxFrame.Load())

	// Read frame 1
	buf := make([]byte, 4096)
	require.NoError(t, w.readFrame(1, buf, nil, nil))
	assert.Equal(t, pg1.data, buf)

	// Read frame 2
	require.NoError(t, w.readFrame(2, buf, nil, nil))
	assert.Equal(t, pg2.data, buf)

	w.endWrite()
	require.NoError(t, w.close(false))
}

func TestWALReadFrameInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())

	buf := make([]byte, 4096)
	assert.ErrorIs(t, w.readFrame(0, buf, nil, nil), ErrWALCorrupt)
	assert.ErrorIs(t, w.readFrame(999, buf, nil, nil), ErrWALCorrupt)

	require.NoError(t, w.close(false))
}

func TestWALWriteNoCommit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	pg := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg.data, "uncommitted")

	// Write without commit
	require.NoError(t, w.writeFrames([]*page{pg}, false, 1))
	assert.Equal(t, uint32(1), w.nFrame.Load())
	// walIndex.maxFrame tracks highest frame set (used internally),
	// but the commit-visible maxFrame is only updated on commit.
	// The key behavior is that recovery will NOT replay uncommitted frames.
	assert.Equal(t, uint32(1), w.index.maxFrame.Load())

	w.endWrite()
	require.NoError(t, w.close(false))
}

func TestWALWriteEmptyFrames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	require.NoError(t, w.writeFrames(nil, true, 0))
	assert.Equal(t, uint32(0), w.nFrame.Load())

	w.endWrite()
	require.NoError(t, w.close(false))
}

func TestWALRecoveryCommitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Write some committed data
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	pg := &page{pgno: 5, data: make([]byte, 4096)}
	copy(pg.data, "committed data")
	require.NoError(t, w.writeFrames([]*page{pg}, true, 5))

	w.endWrite()
	require.NoError(t, w.close(false))

	// Reopen and verify recovery
	w2 := newWal(path, 4096)
	require.NoError(t, w2.open())

	assert.Equal(t, uint32(1), w2.nFrame.Load())
	assert.Equal(t, uint32(5), w2.index.maxPage.Load())

	// Verify we can read the recovered frame
	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(1, buf, nil, nil))
	assert.Equal(t, pg.data, buf)

	require.NoError(t, w2.close(false))
}

func TestWALRecoveryUncommittedTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Write committed + uncommitted frames
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))

	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "uncommitted")
	require.NoError(t, w.writeFrames([]*page{pg2}, false, 2))

	w.endWrite()
	require.NoError(t, w.close(false))

	// Reopen - recovery should only see committed frame
	w2 := newWal(path, 4096)
	require.NoError(t, w2.open())

	assert.Equal(t, uint32(1), w2.nFrame.Load())
	assert.Equal(t, uint32(1), w2.index.maxPage.Load())

	// Frame for page 2 should not be in index
	frame := mustWiGet(t, w2.index, 2, w2.nFrame.Load())
	assert.Equal(t, uint32(0), frame)

	require.NoError(t, w2.close(false))
}

// TestWALRecoveryStopsAtPgnoZeroFrame verifies recovery halts at a frame whose
// page number is zero, even when its salt matches the WAL header and its
// checksum correctly continues the running chain. This mirrors SQLite's
// walDecodeFrame pgno==0 rejection (wal.c:1019-1024) and the walIndexRecover
// call-site termination (wal.c:1504-1505): the first invalid frame ends the
// scan, so only the prior committed frames are recovered.
func TestWALRecoveryStopsAtPgnoZeroFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	const pageSize = 4096

	// Write one committed frame (page 5) via the normal API.
	w := newWal(path, pageSize)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)
	pg := &page{pgno: 5, data: make([]byte, pageSize)}
	copy(pg.data, "committed data")
	require.NoError(t, w.writeFrames([]*page{pg}, true, 5))
	w.endWrite()
	salt1, salt2 := w.header.salt1, w.header.salt2
	require.NoError(t, w.close(false))

	frameSize := int64(walFrameSize) + int64(pageSize)

	// Reconstruct the running checksum state after the first committed frame
	// exactly as recoverLocked does: seed from header words [0:24], then fold
	// in frame-header bytes [0:8] and the full page payload.
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.GreaterOrEqual(t, int64(len(contents)), walHeaderSize+frameSize)

	s1, s2 := walChecksum(contents[0:24], 0, 0)
	frame0 := contents[walHeaderSize : walHeaderSize+frameSize]
	s1, s2 = walChecksum(frame0[0:8], s1, s2)
	s1, s2 = walChecksum(frame0[walFrameSize:], s1, s2)

	// Hand-craft a second frame with pgno==0 but a matching salt and a checksum
	// that correctly continues the chain — so only the pgno==0 guard can reject
	// it. dbSize is set as a commit marker to prove recovery still stops here.
	var hdr [walFrameSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], 0) // pgno == 0 (invalid frame)
	binary.BigEndian.PutUint32(hdr[4:8], 7) // dbSize commit marker
	binary.BigEndian.PutUint32(hdr[8:12], salt1)
	binary.BigEndian.PutUint32(hdr[12:16], salt2)
	data := make([]byte, pageSize)
	copy(data, "page-zero frame")
	cs1, cs2 := walChecksum(hdr[0:8], s1, s2)
	cs1, cs2 = walChecksum(data, cs1, cs2)
	binary.BigEndian.PutUint32(hdr[16:20], cs1)
	binary.BigEndian.PutUint32(hdr[20:24], cs2)

	f, err := os.OpenFile(path, os.O_WRONLY, 0666)
	require.NoError(t, err)
	off := walHeaderSize + frameSize
	_, err = f.WriteAt(hdr[:], off)
	require.NoError(t, err)
	_, err = f.WriteAt(data, off+walFrameSize)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Reopen — recovery must stop at the pgno==0 frame, recovering only the
	// first committed frame.
	w2 := newWal(path, pageSize)
	require.NoError(t, w2.open())

	assert.Equal(t, uint32(1), w2.nFrame.Load(), "recovery must stop at the pgno==0 frame")
	assert.Equal(t, uint32(5), w2.index.maxPage.Load())

	// The pgno==0 frame lives at frame index 2; it must not be indexed.
	assert.Equal(t, uint32(0), mustWiGet(t, w2.index, 0, w2.nFrame.Load()))

	// The prior committed frame is recoverable.
	buf := make([]byte, pageSize)
	require.NoError(t, w2.readFrame(1, buf, nil, nil))
	assert.Equal(t, pg.data, buf)

	require.NoError(t, w2.close(false))
}

func TestWALRecoveryCorruptHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Write garbage to WAL file
	require.NoError(t, os.WriteFile(path, make([]byte, walHeaderSize+100), 0666))

	w := newWal(path, 4096)
	require.NoError(t, w.open()) // Should handle gracefully by resetting

	assert.Equal(t, uint32(0), w.nFrame.Load())
	require.NoError(t, w.close(false))
}

func TestWALIndexSetGet(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")
	idx, err := newWalIndex(shmPath, false)
	require.NoError(t, err)
	defer idx.close(false)

	mustWiSet(t, idx, 1, 1)
	mustWiSet(t, idx, 2, 2)
	mustWiSet(t, idx, 1, 3) // Update page 1 to frame 3

	// Get with full visibility
	assert.Equal(t, uint32(3), mustWiGet(t, idx, 1, 10))
	assert.Equal(t, uint32(2), mustWiGet(t, idx, 2, 10))

	// Snapshot isolation: max frame limits visibility
	assert.Equal(t, uint32(0), mustWiGet(t, idx, 1, 0))
	assert.Equal(t, uint32(2), mustWiGet(t, idx, 2, 5))

	// Page 1 has frames [1, 3]. With maxFrame=2, we see frame 1 (latest <= 2).
	// With maxFrame >= 3, we see frame 3 (the newest version).
	assert.Equal(t, uint32(1), mustWiGet(t, idx, 1, 2)) // frame 1 <= maxFrame 2
	assert.Equal(t, uint32(3), mustWiGet(t, idx, 1, 3))

	// Non-existent page
	assert.Equal(t, uint32(0), mustWiGet(t, idx, 999, 100))
}

func TestWALIndexReset(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")
	idx, err := newWalIndex(shmPath, false)
	require.NoError(t, err)
	defer idx.close(false)

	mustWiSet(t, idx, 1, 1)
	mustWiSet(t, idx, 2, 2)
	assert.Equal(t, uint32(2), idx.maxFrame.Load())

	idx.reset()
	assert.Equal(t, uint32(0), idx.maxFrame.Load())
	assert.Equal(t, uint32(0), mustWiGet(t, idx, 1, 100))
	assert.Equal(t, uint32(0), mustWiGet(t, idx, 2, 100))
}

func TestWALIndexWriteHeader(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")
	idx, err := newWalIndex(shmPath, false)
	require.NoError(t, err)
	defer idx.close(false)

	idx.nBackfill.Store(5)
	require.NoError(t, idx.writeHeader(10, 20, 5, [2]uint32{}, [2]uint32{}))
	idx.shmWriteCkptInfo()

	// Read back from shm region 0
	region, err := idx.shm.region(0, false)
	require.NoError(t, err)

	// WalIndexHdr copy 1: mxFrame at offset 16, nPage at offset 20
	assert.Equal(t, uint32(10), binary.LittleEndian.Uint32(region[16:20]))
	assert.Equal(t, uint32(20), binary.LittleEndian.Uint32(region[20:24]))

	// WalIndexHdr copy 2: starts at offset 48
	assert.Equal(t, uint32(10), binary.LittleEndian.Uint32(region[64:68]))
	assert.Equal(t, uint32(20), binary.LittleEndian.Uint32(region[68:72]))

	// nBackfill is in checkpoint info area at offset 96
	assert.Equal(t, uint32(5), binary.LittleEndian.Uint32(region[96:100]))
}

func TestWALCheckpointEmpty(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	dbFile, err := os.Create(dbPath)
	require.NoError(t, err)
	defer dbFile.Close()

	walPath := filepath.Join(dir, "test.wal")
	w := newWal(walPath, 4096)
	require.NoError(t, w.open())

	// Checkpoint with no frames should be no-op
	require.NoError(t, w.checkpoint(dbFile, nil))

	require.NoError(t, w.close(false))
}

func TestWALCheckpointWritesBack(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	// Create a minimal DB file
	dbFile, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0666)
	require.NoError(t, err)
	buf := make([]byte, 4096)
	_, err = dbFile.Write(buf)
	require.NoError(t, err)

	walPath := filepath.Join(dir, "test.wal")
	w := newWal(walPath, 4096)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Write a frame
	pg := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg.data, "checkpointed data")
	require.NoError(t, w.writeFrames([]*page{pg}, true, 1))
	w.endWrite()

	// Checkpoint (FULL mode no longer resets the WAL)
	require.NoError(t, w.checkpoint(dbFile, nil))

	// WAL frames should still exist (FULL mode preserves WAL for crash safety)
	assert.Equal(t, uint32(1), w.nFrame.Load())

	// Read DB file and verify data was written
	readBuf := make([]byte, 4096)
	_, err = dbFile.ReadAt(readBuf, 0)
	require.NoError(t, err)
	assert.Equal(t, pg.data, readBuf)

	dbFile.Close()
	require.NoError(t, w.close(false))
}

func TestWALMultipleCommits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())

	// First commit
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "commit 1")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))
	w.endWrite()

	// Second commit (updates same page)
	_, bwErr = w.beginWrite()
	require.NoError(t, bwErr)
	pg2 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg2.data, "commit 2")
	require.NoError(t, w.writeFrames([]*page{pg2}, true, 1))
	w.endWrite()

	assert.Equal(t, uint32(2), w.nFrame.Load())

	// Latest frame for page 1 should be frame 2
	frame := mustWiGet(t, w.index, 1, w.nFrame.Load())
	assert.Equal(t, uint32(2), frame)

	// Read latest frame
	buf := make([]byte, 4096)
	require.NoError(t, w.readFrame(2, buf, nil, nil))
	assert.Equal(t, pg2.data, buf)

	require.NoError(t, w.close(false))
}

func TestWALBeginReadEndRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())

	maxFrame, slot, err := w.beginRead()
	require.NoError(t, err)
	assert.Equal(t, uint32(0), maxFrame)
	w.endRead(slot)

	// Write some data, then read
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)
	pg := &page{pgno: 1, data: make([]byte, 4096)}
	require.NoError(t, w.writeFrames([]*page{pg}, true, 1))
	w.endWrite()

	maxFrame, slot, err = w.beginRead()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), maxFrame)
	w.endRead(slot)

	require.NoError(t, w.close(false))
}

func TestWALFrameChecksum(t *testing.T) {
	hdr := make([]byte, 8)
	binary.BigEndian.PutUint32(hdr[0:4], 1) // pgno
	binary.BigEndian.PutUint32(hdr[4:8], 0) // dbSize

	pageData := make([]byte, 4096)
	copy(pageData, "test page content")

	s1, s2 := walFrameChecksum(hdr, pageData, 0, 0)
	assert.NotEqual(t, uint32(0), s1)
	assert.NotEqual(t, uint32(0), s2)

	// Deterministic
	s1b, s2b := walFrameChecksum(hdr, pageData, 0, 0)
	assert.Equal(t, s1, s1b)
	assert.Equal(t, s2, s2b)
}

func TestWALRecoveryMultipleCommits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w := newWal(path, 4096)
	require.NoError(t, w.open())

	// Two commits
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "first commit")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))
	w.endWrite()

	_, bwErr = w.beginWrite()
	require.NoError(t, bwErr)
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "second commit")
	require.NoError(t, w.writeFrames([]*page{pg2}, true, 2))
	w.endWrite()

	require.NoError(t, w.close(false))

	// Reopen and recover
	w2 := newWal(path, 4096)
	require.NoError(t, w2.open())

	assert.Equal(t, uint32(2), w2.nFrame.Load())
	assert.Equal(t, uint32(2), w2.index.maxPage.Load())

	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(1, buf, nil, nil))
	assert.Equal(t, pg1.data, buf)

	require.NoError(t, w2.readFrame(2, buf, nil, nil))
	assert.Equal(t, pg2.data, buf)

	require.NoError(t, w2.close(false))
}

func TestWALReadFrameAllowsPeerCommittedFrameBeyondLocalNFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Open a second WAL handle before the first writer commits so its local
	// nFrame remains stale. This simulates another process discovering a newer
	// frame via SHM/page lookup and then reading the frame data from disk.
	wWriter := newWal(path, 4096)
	wWriter.inProcess = false
	require.NoError(t, wWriter.open())
	defer wWriter.close(false)

	wReader := newWal(path, 4096)
	wReader.inProcess = false
	require.NoError(t, wReader.open())
	defer wReader.close(false)

	_, bwErr := wWriter.beginWrite()
	require.NoError(t, bwErr)
	pg := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg.data, "peer-committed-page")
	require.NoError(t, wWriter.writeFrames([]*page{pg}, true, 1))
	wWriter.endWrite()

	require.Equal(t, uint32(0), wReader.nFrame.Load(), "reader handle should still have stale local nFrame")

	buf := make([]byte, 4096)
	require.NoError(t, wReader.readFrame(1, buf, nil, nil), "reader should read peer frame directly from file even with stale local nFrame")
	assert.Equal(t, pg.data, buf)
}

func TestWalIndexGetCrossProcessFallback(t *testing.T) {
	// Use a heap SHM but with inProcess=false to test the fallback logic.
	// This simulates a second process whose pageMap is empty for frames
	// written by another process, but the SHM hash tables have them.
	wi := &walIndex{
		shm:       newHeapShm(),
		pageMap:   make(map[uint32][]uint32),
		inProcess: false,
	}

	// Write frames to SHM hash tables only (simulating another process)
	mustShmHashWrite(t, wi, 5, 1)
	wi.maxFrame.Store(1)
	wi.mxCommitFrame.Store(1) // getLatest() uses mxCommitFrame for SHM fallback bound
	wi.nBackfill.Store(0)

	// get() should find frame 1 for page 5 via SHM fallback
	frame := mustWiGet(t, wi, 5, 1)
	assert.Equal(t, uint32(1), frame, "get() SHM fallback: expected frame 1")

	// getLatest() should also find it
	frame = mustWiGetLatest(t, wi, 5)
	assert.Equal(t, uint32(1), frame, "getLatest() SHM fallback: expected frame 1")

	// With inProcess=true, should NOT fall back to SHM
	wi.inProcess = true
	frame = mustWiGet(t, wi, 5, 1)
	assert.Equal(t, uint32(0), frame, "get() inProcess should not use SHM fallback")

	frame = mustWiGetLatest(t, wi, 5)
	assert.Equal(t, uint32(0), frame, "getLatest() inProcess should not use SHM fallback")
}

func TestWriteFramesCommitFalseDoesNotAdvanceMxCommitFrame(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// First commit some frames so mxCommitFrame is non-zero
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))
	assert.Equal(t, uint32(1), w.index.maxFrame.Load())
	assert.Equal(t, uint32(1), w.index.mxCommitFrame.LoadLocal())

	// Now write frames WITHOUT commit (simulating spill)
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "spilled page 2")
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "spilled page 3")
	require.NoError(t, w.writeFrames([]*page{pg2, pg3}, false, 0))

	// maxFrame should advance to 3 (writer sees all frames)
	assert.Equal(t, uint32(3), w.index.maxFrame.Load())
	// mxCommitFrame should still be 1 (readers only see committed)
	assert.Equal(t, uint32(1), w.index.mxCommitFrame.LoadLocal())

	// Writer can find spilled pages via pageMap
	assert.Equal(t, uint32(2), mustWiGet(t, w.index, 2, 3))
	assert.Equal(t, uint32(3), mustWiGet(t, w.index, 3, 3))

	// Now commit — mxCommitFrame should catch up
	pg4 := &page{pgno: 4, data: make([]byte, 4096)}
	copy(pg4.data, "committed page 4")
	require.NoError(t, w.writeFrames([]*page{pg4}, true, 4))
	assert.Equal(t, uint32(4), w.index.maxFrame.Load())
	assert.Equal(t, uint32(4), w.index.mxCommitFrame.LoadLocal())

	w.endWrite()
	require.NoError(t, w.close(false))
}

func TestGetLatestIgnoresSpilledFrames(t *testing.T) {
	// Test that getLatest() uses mxCommitFrame for SHM hash fallback,
	// so cross-process readers don't see spilled frames.
	wi := &walIndex{
		shm:       newHeapShm(),
		pageMap:   make(map[uint32][]uint32),
		inProcess: false,
	}

	// Simulate a commit: page 1 at frame 1
	mustShmHashWrite(t, wi, 1, 1)
	wi.maxFrame.Store(1)
	wi.mxCommitFrame.Store(1)

	// Simulate spill: page 2 at frame 2, written to SHM hash but not committed.
	// In real code, spill does NOT write SHM hash (deferred), but here we test
	// that getLatest bounds the SHM search to mxCommitFrame even if SHM has the entry.
	mustShmHashWrite(t, wi, 2, 2)
	wi.maxFrame.Store(2)
	// mxCommitFrame stays at 1

	// getLatest for page 2: pageMap is empty (cross-process scenario),
	// SHM fallback uses mxCommitFrame=1, so frame 2 is invisible.
	frame := mustWiGetLatest(t, wi, 2)
	assert.Equal(t, uint32(0), frame, "getLatest should not see spilled frame beyond mxCommitFrame")

	// getLatest for page 1: should still find frame 1 (within mxCommitFrame)
	frame = mustWiGetLatest(t, wi, 1)
	assert.Equal(t, uint32(1), frame, "getLatest should find committed frame")

	// After commit, advancing mxCommitFrame makes frame 2 visible
	wi.mxCommitFrame.Store(2)
	frame = mustWiGetLatest(t, wi, 2)
	assert.Equal(t, uint32(2), frame, "getLatest should see frame after mxCommitFrame advances")
}

func TestRollbackCleansUpSpilledFrames(t *testing.T) {
	// After spilling frames (commit=false), rollbackToFrame should remove
	// pageMap entries for spilled frames and restore maxFrame.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Commit pages 1 and 2 (frames 1 and 2)
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "committed page 2")
	require.NoError(t, w.writeFrames([]*page{pg1, pg2}, true, 2))

	// Verify baseline state
	savedFrame := w.nFrame.Load()
	assert.Equal(t, uint32(2), savedFrame)
	assert.Equal(t, uint32(2), w.index.maxFrame.Load())
	assert.Equal(t, uint32(2), w.index.mxCommitFrame.LoadLocal())

	// Spill pages 3 and 4 (frames 3 and 4, commit=false)
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "spilled page 3")
	pg4 := &page{pgno: 4, data: make([]byte, 4096)}
	copy(pg4.data, "spilled page 4")
	require.NoError(t, w.writeFrames([]*page{pg3, pg4}, false, 0))

	// Verify spilled state: maxFrame advanced but mxCommitFrame did not
	assert.Equal(t, uint32(4), w.index.maxFrame.Load())
	assert.Equal(t, uint32(2), w.index.mxCommitFrame.LoadLocal())
	assert.Equal(t, uint32(4), w.nFrame.Load())

	// Writer can see spilled frames in pageMap
	assert.Equal(t, uint32(3), mustWiGet(t, w.index, 3, 4))
	assert.Equal(t, uint32(4), mustWiGet(t, w.index, 4, 4))

	// Rollback to savedFrame (discard spilled frames)
	w.index.rollbackToFrame(savedFrame)

	// maxFrame should be restored
	assert.Equal(t, uint32(2), w.index.maxFrame.Load())

	// Spilled pages should be gone from pageMap
	assert.Equal(t, uint32(0), mustWiGet(t, w.index, 3, 10))
	assert.Equal(t, uint32(0), mustWiGet(t, w.index, 4, 10))

	// Committed pages should still be visible
	assert.Equal(t, uint32(1), mustWiGet(t, w.index, 1, 10))
	assert.Equal(t, uint32(2), mustWiGet(t, w.index, 2, 10))

	w.endWrite()
	require.NoError(t, w.close(false))
}

func TestRollbackToSavepointWithSpilledFrames(t *testing.T) {
	// Savepoint, spill frames, rollback to savepoint — WAL index should
	// be restored to the savepoint's walFrame position.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Commit pages 1 and 2 (frames 1 and 2)
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "committed page 2")
	require.NoError(t, w.writeFrames([]*page{pg1, pg2}, true, 2))

	// Record the savepoint WAL position
	savepointFrame := w.nFrame.Load()
	assert.Equal(t, uint32(2), savepointFrame)

	// Commit page 3 (frame 3) — this is after the savepoint
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "committed page 3")
	require.NoError(t, w.writeFrames([]*page{pg3}, true, 3))

	// Spill page 4 (frame 4, commit=false)
	pg4 := &page{pgno: 4, data: make([]byte, 4096)}
	copy(pg4.data, "spilled page 4")
	require.NoError(t, w.writeFrames([]*page{pg4}, false, 0))

	// Verify state before rollback
	assert.Equal(t, uint32(4), w.index.maxFrame.Load())
	assert.Equal(t, uint32(3), w.index.mxCommitFrame.LoadLocal())

	// Rollback to savepoint (frame 2)
	w.index.rollbackToFrame(savepointFrame)

	// maxFrame should be restored to savepoint position
	assert.Equal(t, uint32(2), w.index.maxFrame.Load())

	// Pages written after savepoint should be gone
	assert.Equal(t, uint32(0), mustWiGet(t, w.index, 3, 10))
	assert.Equal(t, uint32(0), mustWiGet(t, w.index, 4, 10))

	// Pages from before savepoint should still be visible
	assert.Equal(t, uint32(1), mustWiGet(t, w.index, 1, 10))
	assert.Equal(t, uint32(2), mustWiGet(t, w.index, 2, 10))

	w.endWrite()
	require.NoError(t, w.close(false))
}

func TestCrossProcessReaderDoesNotSeeSpilledFrames(t *testing.T) {
	// Multi-process mode: spill frames (no commit), verify SHM header mxFrame
	// is unchanged and shmHashGet doesn't find spilled pages. This ensures
	// cross-process readers are fully isolated from uncommitted spills.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = false
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Commit pages 1 and 2
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "committed page 2")
	require.NoError(t, w.writeFrames([]*page{pg1, pg2}, true, 2))

	// Read SHM header after commit — mxFrame should be 2
	hdr1, ok := w.index.readHeader()
	require.True(t, ok, "SHM header should be valid after commit")
	assert.Equal(t, uint32(2), hdr1.mxFrame, "SHM header mxFrame should be 2 after commit")
	assert.Equal(t, uint32(2), hdr1.nPage, "SHM header nPage should be 2 after commit")

	// Verify committed pages are in SHM hash
	assert.Equal(t, uint32(1), mustShmHashGet(t, w.index, 1, 10, 1), "committed page 1 should be in SHM hash")
	assert.Equal(t, uint32(2), mustShmHashGet(t, w.index, 2, 10, 1), "committed page 2 should be in SHM hash")

	// Spill pages 3 and 4 (commit=false)
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "spilled page 3")
	pg4 := &page{pgno: 4, data: make([]byte, 4096)}
	copy(pg4.data, "spilled page 4")
	require.NoError(t, w.writeFrames([]*page{pg3, pg4}, false, 0))

	// SHM header mxFrame should still be 2 (no writeHeader on non-commit)
	hdr2, ok := w.index.readHeader()
	require.True(t, ok, "SHM header should still be valid after spill")
	assert.Equal(t, uint32(2), hdr2.mxFrame, "SHM header mxFrame must not advance on spill")
	assert.Equal(t, uint32(2), hdr2.nPage, "SHM header nPage must not change on spill")

	// mxCommitFrame should still be 2
	assert.Equal(t, uint32(2), w.index.mxCommitFrame.LoadLocal(), "mxCommitFrame must not advance on spill")
	// maxFrame (writer-internal) should be 4
	assert.Equal(t, uint32(4), w.index.maxFrame.Load(), "maxFrame should include spilled frames")

	// Committed pages still accessible via SHM hash
	assert.Equal(t, uint32(1), mustShmHashGet(t, w.index, 1, 10, 1), "committed page 1 still in SHM hash")
	assert.Equal(t, uint32(2), mustShmHashGet(t, w.index, 2, 10, 1), "committed page 2 still in SHM hash")

	// A cross-process reader gated by mxCommitFrame must not see spilled pages:
	// SHM hash is written eagerly (SQLite-aligned), but readers bound by
	// mxCommitFrame in getLatest cannot reach frames > mxCommitFrame.
	reader := &walIndex{
		shm:       w.index.shm,
		pageMap:   make(map[uint32][]uint32),
		inProcess: false,
	}
	reader.mxCommitFrame.Store(w.index.mxCommitFrame.LoadLocal())
	reader.nBackfill.Store(0)

	// Reader should see committed pages via SHM hash
	assert.Equal(t, uint32(1), mustWiGetLatest(t, reader, 1), "cross-process reader sees committed page 1")
	assert.Equal(t, uint32(2), mustWiGetLatest(t, reader, 2), "cross-process reader sees committed page 2")
	// Reader should NOT see spilled pages (frame > mxCommitFrame)
	assert.Equal(t, uint32(0), mustWiGetLatest(t, reader, 3), "cross-process reader must not see spilled page 3")
	assert.Equal(t, uint32(0), mustWiGetLatest(t, reader, 4), "cross-process reader must not see spilled page 4")

	w.endWrite()
	require.NoError(t, w.close(false))
}

func TestRecoveryIgnoresSpilledFrames(t *testing.T) {
	// Write committed frames followed by spilled (non-commit) frames to WAL,
	// close, reopen — recovery should only index committed frames.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w := newWal(path, 4096)
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Commit pages 1, 2, 3 (frames 1-3)
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "committed page 2")
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "committed page 3")
	require.NoError(t, w.writeFrames([]*page{pg1, pg2, pg3}, true, 3))

	// Spill pages 4 and 5, plus update page 1 (frames 4-6, no commit)
	pg4 := &page{pgno: 4, data: make([]byte, 4096)}
	copy(pg4.data, "spilled page 4")
	pg5 := &page{pgno: 5, data: make([]byte, 4096)}
	copy(pg5.data, "spilled page 5")
	pg1v2 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1v2.data, "spilled update to page 1")
	require.NoError(t, w.writeFrames([]*page{pg4, pg5, pg1v2}, false, 0))

	// Verify pre-close state: 6 total frames, only 3 committed
	assert.Equal(t, uint32(6), w.nFrame.Load())
	assert.Equal(t, uint32(6), w.index.maxFrame.Load())
	assert.Equal(t, uint32(3), w.index.mxCommitFrame.LoadLocal())

	w.endWrite()
	require.NoError(t, w.close(false))

	// Reopen — recovery should only see committed frames 1-3
	w2 := newWal(path, 4096)
	require.NoError(t, w2.open())

	// nFrame should be 3 (only committed frames)
	assert.Equal(t, uint32(3), w2.nFrame.Load(), "recovery: nFrame should be 3")
	assert.Equal(t, uint32(3), w2.index.maxFrame.Load(), "recovery: maxFrame should be 3")
	assert.Equal(t, uint32(3), w2.index.mxCommitFrame.LoadLocal(), "recovery: mxCommitFrame should be 3")
	assert.Equal(t, uint32(3), w2.index.maxPage.Load(), "recovery: maxPage should be 3")

	// Committed pages should be in index
	assert.Equal(t, uint32(1), mustWiGet(t, w2.index, 1, 3), "recovery: page 1 at frame 1")
	assert.Equal(t, uint32(2), mustWiGet(t, w2.index, 2, 3), "recovery: page 2 at frame 2")
	assert.Equal(t, uint32(3), mustWiGet(t, w2.index, 3, 3), "recovery: page 3 at frame 3")

	// Spilled pages should NOT be in index
	assert.Equal(t, uint32(0), mustWiGet(t, w2.index, 4, 10), "recovery: spilled page 4 not visible")
	assert.Equal(t, uint32(0), mustWiGet(t, w2.index, 5, 10), "recovery: spilled page 5 not visible")

	// Page 1 should be at frame 1 (committed version), NOT frame 6 (spilled update)
	assert.Equal(t, uint32(1), mustWiGet(t, w2.index, 1, 10), "recovery: page 1 should be at committed frame, not spilled")

	// Verify data integrity of committed frames
	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(1, buf, nil, nil))
	assert.Equal(t, pg1.data, buf, "recovery: page 1 data should be committed version")
	require.NoError(t, w2.readFrame(2, buf, nil, nil))
	assert.Equal(t, pg2.data, buf, "recovery: page 2 data intact")
	require.NoError(t, w2.readFrame(3, buf, nil, nil))
	assert.Equal(t, pg3.data, buf, "recovery: page 3 data intact")

	// SHM header should reflect committed state
	hdr, ok := w2.index.readHeader()
	require.True(t, ok, "recovery: SHM header should be valid")
	assert.Equal(t, uint32(3), hdr.mxFrame, "recovery: SHM header mxFrame should be 3")
	assert.Equal(t, uint32(3), hdr.nPage, "recovery: SHM header nPage should be 3")

	require.NoError(t, w2.close(false))
}

func TestWriteFramesCommitFlushesToShm(t *testing.T) {
	// SHM hash is populated eagerly (SQLite-aligned): every frame writes SHM
	// hash immediately regardless of commit. Readers are gated via mxCommitFrame,
	// not via the presence of the SHM hash entry itself.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = false
	require.NoError(t, w.open())
	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Spill pages 1 and 2 (commit=false)
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "spilled page 1")
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "spilled page 2")
	require.NoError(t, w.writeFrames([]*page{pg1, pg2}, false, 0))

	// SHM hash holds entries eagerly (invisible to peer readers via mxCommitFrame)
	assert.Equal(t, uint32(1), mustShmHashGet(t, w.index, 1, 10, 1))
	assert.Equal(t, uint32(2), mustShmHashGet(t, w.index, 2, 10, 1))

	// Now commit page 3
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "committed page 3")
	require.NoError(t, w.writeFrames([]*page{pg3}, true, 3))

	// All frames visible in SHM hash
	frame := mustShmHashGet(t, w.index, 1, 10, 1)
	assert.Equal(t, uint32(1), frame, "page 1 in SHM hash")
	frame = mustShmHashGet(t, w.index, 2, 10, 1)
	assert.Equal(t, uint32(2), frame, "page 2 in SHM hash")
	frame = mustShmHashGet(t, w.index, 3, 10, 1)
	assert.Equal(t, uint32(3), frame, "page 3 in SHM hash")

	// mxCommitFrame should include all frames
	assert.Equal(t, uint32(3), w.index.mxCommitFrame.LoadLocal())

	w.endWrite()
	require.NoError(t, w.close(false))
}

// BenchmarkWalOpen_CleanReopen measures per-reopen cost after a clean close
// with 50 pre-existing WAL frames. Baseline for the wal.open SQLite-alignment
// refactor: current code eagerly touches SHM on open (either initHeaderState
// or adoptSHMState + recover). Post-refactor target: <20% of current time,
// matching SQLite's sqlite3WalOpen which never touches SHM on open.
func BenchmarkWalOpen_CleanReopen(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(b, path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(b, err)
	tx, err := db.BeginWrite()
	require.NoError(b, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(b, err)
	for i := 0; i < 50; i++ {
		require.NoError(b, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), make([]byte, 512)))
	}
	require.NoError(b, tx.Commit())
	require.NoError(b, db.Close())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db2, err := testOpen(b, path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
		if err != nil {
			b.Fatal(err)
		}
		_ = db2.Close()
	}
}

func TestEnsureHeaderInitialized_FreshSHM(t *testing.T) {
	dir := t.TempDir()
	w := newWal(filepath.Join(dir, "t.db-wal"), 4096)
	w.inProcess = false
	require.NoError(t, w.open())
	defer w.close(false)

	// Simulate cold-open: clear SHM header bytes so readHeader returns !valid.
	region, err := w.index.shm.region(0, true)
	require.NoError(t, err)
	clear(region[:walIndexHdrSize*2])

	// ensureHeaderInitialized must leave SHM in a valid state with isInit==1
	// and return the published hdr.
	_, err = w.ensureHeaderInitialized()
	require.NoError(t, err)
	hdr, valid := w.index.readHeader()
	require.True(t, valid)
	require.Equal(t, uint8(1), hdr.isInit)
	require.Equal(t, uint32(0), hdr.mxFrame)
	require.Equal(t, w.header.salt1, hdr.aSalt[0])
	require.Equal(t, w.header.salt2, hdr.aSalt[1])
}

func TestEnsureHeaderInitialized_AdoptsValidSHM(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")

	// Process A: create DB, commit 10 frames, rawClose to keep WAL+SHM intact
	// (normal DB.Close would checkpoint, truncate WAL and remove -shm).
	db1, err := testOpen(t, dbPath, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	tx, err := db1.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%d", i)), []byte("v")))
	}
	require.NoError(t, tx.Commit())
	saltsA := [2]uint32{db1.pager.wal.header.salt1, db1.pager.wal.header.salt2}
	// 10 Puts with tiny values pack into a single commit batch — capture the
	// real frame count rather than hard-coding a per-row assumption.
	expectedFrames := db1.pager.wal.nFrame.Load()
	require.Greater(t, expectedFrames, uint32(0), "test must seed at least one frame")
	rawClose(db1)

	// Open a raw wal against the existing -wal file. rawClose deletes -shm
	// (last-connection DMS upgrade succeeds), so w2 gets a fresh SHM mapped
	// and the current eager wal.open will run recoverLocked to rebuild it
	// from the on-disk WAL header. ensureHeaderInitialized must be a no-op
	// idempotent follow-up that keeps the adopted state intact.
	w2 := newWal(dbPath+"-wal", 4096)
	w2.inProcess = false
	require.NoError(t, w2.open())
	defer w2.close(false)
	_, err = w2.ensureHeaderInitialized()
	require.NoError(t, err)

	require.Equal(t, saltsA[0], w2.header.salt1, "must adopt salts, not generate new ones")
	require.Equal(t, saltsA[1], w2.header.salt2)
	require.Equal(t, expectedFrames, w2.index.maxFrame.Load(), "mxFrame must reflect recovered frames")
	require.Equal(t, expectedFrames, w2.nFrame.Load(), "nFrame must reflect recovered frames")
}

func TestEnsureHeaderInitialized_TriggersRecoveryWhenSHMInvalid(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")

	// Seed a DB with frames and rawClose to keep the WAL file intact
	// (normal DB.Close would truncate via checkpoint).
	db, err := testOpen(t, dbPath, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%d", i)), []byte("v")))
	}
	require.NoError(t, tx.Commit())
	expectedFrames := db.pager.wal.nFrame.Load()
	require.Greater(t, expectedFrames, uint32(0), "test must seed at least one frame")
	rawClose(db)

	// rawClose removed -shm; w2 creates a new SHM mapping. The current eager
	// wal.open already recovers from the WAL file into SHM — so to exercise
	// ensureHeaderInitialized's recovery path we clear the SHM header after
	// open and then invoke the helper explicitly.
	w2 := newWal(dbPath+"-wal", 4096)
	w2.inProcess = false
	require.NoError(t, w2.open())
	defer w2.close(false)

	// Clear the SHM header to force ensureHeaderInitialized into the slow path.
	region, err := w2.index.shm.region(0, true)
	require.NoError(t, err)
	clear(region[:walIndexHdrSize*2])

	_, validBefore := w2.index.readHeader()
	require.False(t, validBefore, "SHM should be invalid after manual clear")

	_, err = w2.ensureHeaderInitialized()
	require.NoError(t, err)

	// After, SHM must be valid and mxFrame must match recovered frames.
	hdr, validAfter := w2.index.readHeader()
	require.True(t, validAfter)
	require.Equal(t, expectedFrames, hdr.mxFrame)
}

// TestRollbackCleanupZerosShmHashEntries exercises shmCleanupFromFrame: a
// cross-process reader consulting shmHashGet directly (bypassing the
// mxCommitFrame gate on getLatest) must find zero entries for frames that
// were rolled back. Matches SQLite's walCleanupHash invariant (wal.c:1247-1282):
// after savepoint rollback, the SHM hash slots pointing at frames > hdr.mxFrame
// are observably zeroed — not just invisible via mxCommitFrame.
func TestRollbackCleanupZerosShmHashEntries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = false
	require.NoError(t, w.open())
	defer w.close(false)

	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Commit one frame to establish a stable baseline.
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))
	committedFrame := w.nFrame.Load()

	// Verify SHM hash has frame 1 for page 1.
	assert.Equal(t, uint32(1), mustShmHashGet(t, w.index, 1, 10, 1),
		"committed frame 1 should be in SHM hash")

	// Spill two more pages (commit=false). With eager SHM writes, they land
	// in the hash tables immediately.
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "spilled page 2")
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "spilled page 3")
	require.NoError(t, w.writeFrames([]*page{pg2, pg3}, false, 0))

	// Pre-rollback: spilled frames are in SHM hash (caller must use
	// mxCommitFrame to gate visibility).
	assert.Equal(t, uint32(2), mustShmHashGet(t, w.index, 2, 10, 1),
		"spilled frame 2 should be present in SHM hash (eager)")
	assert.Equal(t, uint32(3), mustShmHashGet(t, w.index, 3, 10, 1),
		"spilled frame 3 should be present in SHM hash (eager)")

	// Rollback to the commit boundary — this must invoke shmCleanupFromFrame.
	w.index.rollbackToFrame(committedFrame)

	// Post-rollback: shmHashGet with maxFrame=10 (large enough to see spilled
	// if they remained) must now return 0 for pages 2 and 3. A reader
	// unaware of mxCommitFrame (e.g. a fresh peer process scanning SHM
	// after the writer died) must NOT find dangling entries for rolled-back
	// frames.
	assert.Equal(t, uint32(0), mustShmHashGet(t, w.index, 2, 10, 1),
		"frame 2 hash entry must be zeroed by rollback cleanup")
	assert.Equal(t, uint32(0), mustShmHashGet(t, w.index, 3, 10, 1),
		"frame 3 hash entry must be zeroed by rollback cleanup")

	// Committed frame must still be reachable — cleanup preserves the
	// probe chain for idx <= iLimit (wal.c:1258-1264).
	assert.Equal(t, uint32(1), mustShmHashGet(t, w.index, 1, 10, 1),
		"committed frame 1 must remain reachable after rollback cleanup")
}

// TestRollbackCleanupAcrossSegments verifies shmCleanupFromFrame handles the
// case where spilled frames cross a segment boundary. any-store-specific
// extension over SQLite's walCleanupHash (which only touches the segment of
// hdr.mxFrame because walIndexAppend zero-inits new segments at idx==1).
// Our setBatch has no zero-init, so a trailing segment's aHash must be
// fully cleared by rollback.
func TestRollbackCleanupAcrossSegments(t *testing.T) {
	wi := &walIndex{
		shm:       newHeapShm(),
		pageMap:   make(map[uint32][]uint32),
		inProcess: false,
	}
	// Force entries into segment 0 and segment 1 by writing at indices
	// around htNPageOne. Commit up through frame htNPageOne, spill the
	// next frame (which lands in segment 1), then roll back.
	committed := uint32(htNPageOne)
	mustShmHashWrite(t, wi, 100, committed-1)
	mustShmHashWrite(t, wi, 200, committed)
	wi.maxFrame.Store(committed)

	// Spill two frames into segment 1.
	spill1 := committed + 1
	spill2 := committed + 2
	mustShmHashWrite(t, wi, 300, spill1)
	mustShmHashWrite(t, wi, 400, spill2)
	wi.maxFrame.Store(spill2)

	// Pre-rollback: segment-1 entries are present.
	assert.NotZero(t, mustShmHashGet(t, wi, 300, spill2, 1), "spill frame 300 in SHM")
	assert.NotZero(t, mustShmHashGet(t, wi, 400, spill2, 1), "spill frame 400 in SHM")

	wi.rollbackToFrame(committed)

	// Post-rollback: segment-1 hash entries zeroed.
	assert.Equal(t, uint32(0), mustShmHashGet(t, wi, 300, spill2, 1),
		"trailing-segment frame 300 hash must be zeroed")
	assert.Equal(t, uint32(0), mustShmHashGet(t, wi, 400, spill2, 1),
		"trailing-segment frame 400 hash must be zeroed")

	// Pre-boundary committed frames in segment 0 must remain.
	assert.Equal(t, committed-1, mustShmHashGet(t, wi, 100, committed, 1),
		"segment-0 frame 100 must still be reachable")
	assert.Equal(t, committed, mustShmHashGet(t, wi, 200, committed, 1),
		"segment-0 frame 200 must still be reachable")
}

// TestRollbackReadvanceTrailingSegmentConsistency pins the wal-index hash
// consistency invariant across an mxFrame DECREASE followed by a RE-ADVANCE
// into a trailing segment that shmCleanupFromFrame wholesale-zeroed.
//
// This is the load-bearing equivalence behind drift-95
// (docs/btree/NOTES.md#drift-95-shmcleanupfromframe-zeros-all-segments-above-target):
// SQLite's walCleanupHash cleans only the boundary segment and relies on
// walIndexAppend's idx==1 memset (wal.c:1315-1319) to lazily re-clear higher
// segments when they are reused on re-advance. any-store deliberately omits
// that idx==1 zero-init (drift-91) and instead eagerly scrubs every trailing
// segment in shmCleanupFromFrame. For the two strategies to be observationally
// equivalent, after mxFrame decreases past a segment boundary and then
// re-advances back into that same (now-reused) segment, hash reads must be
// exactly correct.
//
// The adversarial shape that distinguishes eager-scrub from a no-op is a
// PARTIAL re-advance: gen2 re-advances mxFrame far enough to make a stale gen1
// slot's frame fall back within [minFrame, maxFrame], but writes FEWER frames
// than gen1, so it does NOT overwrite the trailing gen1 aPgno[idx]. Without the
// trailing-segment scrub, that surviving aPgno[idx]+aHash slot would resurrect
// a rolled-back page (exactly the corruption C's idx==1 memset prevents lazily
// and the EXPENSIVE_ASSERT at wal.c:1277-1286 guards). With the scrub, the slot
// is zeroed, so the read is correct. This test FAILS if the trailing-segment
// wholesale-zero is removed.
func TestRollbackReadvanceTrailingSegmentConsistency(t *testing.T) {
	wi := &walIndex{
		shm:       newHeapShm(),
		pageMap:   make(map[uint32][]uint32),
		inProcess: false,
	}

	// Commit one frame at the very end of segment 0 so the next frame spills
	// into trailing segment 1 (the wholesale-zeroed case in
	// shmCleanupFromFrame, target <= iZero).
	committed := uint32(htNPageOne)
	mustShmHashWrite(t, wi, 100, committed)
	wi.maxFrame.Store(committed)

	// First generation: spill FOUR frames into trailing segment 1 at indices
	// 0..3 (frames committed+1..committed+4), then roll them all back.
	gen1 := []uint32{300, 400, 500, 600}
	for i, pgno := range gen1 {
		mustShmHashWrite(t, wi, pgno, committed+1+uint32(i))
	}
	wi.maxFrame.Store(committed + uint32(len(gen1)))

	// Sanity: first-generation frames are present before rollback.
	for _, pgno := range gen1 {
		require.NotZero(t, mustShmHashGet(t, wi, pgno, wi.maxFrame.Load(), 1),
			"gen1 page %d should be present pre-rollback", pgno)
	}

	// mxFrame DECREASE: roll back to the boundary. shmCleanupFromFrame
	// wholesale-zeroes trailing segment 1.
	wi.rollbackToFrame(committed)
	require.Equal(t, committed, wi.maxFrame.Load(), "rollback must restore mxFrame")

	// mxFrame RE-ADVANCE, but only PARTIALLY: write TWO frames (gen2) into
	// segment-1 indices 0..1 (frames committed+1, committed+2). This overwrites
	// only gen1's first two aPgno slots; gen1's pages 500 (idx 2, frame
	// committed+3) and 600 (idx 3, frame committed+4) are NOT overwritten.
	gen2 := []uint32{700, 800}
	for i, pgno := range gen2 {
		mustShmHashWrite(t, wi, pgno, committed+1+uint32(i))
	}
	// Advance mxFrame to committed+4 — far enough that the stale gen1 frames
	// for pages 500 and 600 (committed+3, committed+4) would be IN RANGE and
	// thus resurrectable by shmHashGet if their slots were not scrubbed.
	wi.maxFrame.Store(committed + 4)
	maxF := wi.maxFrame.Load()

	// Re-advanced gen2 frames resolve to their new frame numbers.
	for i, pgno := range gen2 {
		want := committed + 1 + uint32(i)
		assert.Equal(t, want, mustShmHashGet(t, wi, pgno, maxF, 1),
			"re-advanced page %d must resolve to its new frame", pgno)
	}

	// THE INVARIANT: rolled-back gen1 pages whose slots were NOT overwritten by
	// the partial re-advance (500, 600) must still be invisible. Their frames
	// (committed+3, committed+4) are <= maxF, so only the eager trailing-segment
	// scrub keeps them from being resurrected. A no-op scrub fails here.
	assert.Equal(t, uint32(0), mustShmHashGet(t, wi, 500, maxF, 1),
		"rolled-back page 500 must not be resurrected by partial re-advance")
	assert.Equal(t, uint32(0), mustShmHashGet(t, wi, 600, maxF, 1),
		"rolled-back page 600 must not be resurrected by partial re-advance")

	// Overwritten gen1 pages (300, 400) likewise stay invisible.
	assert.Equal(t, uint32(0), mustShmHashGet(t, wi, 300, maxF, 1),
		"rolled-back page 300 must not be resurrected after re-advance")
	assert.Equal(t, uint32(0), mustShmHashGet(t, wi, 400, maxF, 1),
		"rolled-back page 400 must not be resurrected after re-advance")

	// Boundary-segment committed frame is untouched throughout.
	assert.Equal(t, committed, mustShmHashGet(t, wi, 100, maxF, 1),
		"boundary-segment committed page 100 must remain reachable")
}

// clearHeaderForTest zeroes the shm header so the next reader's
// readHeader() returns valid=false and ensureHeaderInitialized takes
// the slow path. Test-only helper.
func (wi *walIndex) clearHeaderForTest() error {
	region, err := wi.shm.region(0, true)
	if err != nil {
		return err
	}
	for i := 0; i < walIndexHdrSize*2; i++ {
		region[i] = 0
	}
	return nil
}

// TestEnsureHeaderInitialized_SurfacesBusyRecovery exercises the
// slow-path in ensureHeaderInitialized where the RECOVER lock is held
// exclusive (simulating "peer is mid-walIndexRecover"). The helper
// must return ErrBusyRecovery (not the generic errWALRetry) so upstream
// callers can back off via busyHandler instead of spinning.
//
// Uses a single wal instance: the in-process mmapShm.locks[] counters
// serialize exclusive-vs-exclusive on the same slot even within the
// same process, so preholding RECOVER exclusive is enough to trip the
// helper's third branch.
func TestEnsureHeaderInitialized_SurfacesBusyRecovery(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")

	w := newWal(dbPath, 4096)
	if err := w.open(); err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = w.close(false) })

	// Force slow path by zeroing the shm header.
	if err := w.index.clearHeaderForTest(); err != nil {
		t.Fatalf("clear shm header: %v", err)
	}

	// Prehold RECOVER exclusive — representing "peer is mid-recovery".
	if err := w.index.lock(lockRecover, lockExclusive); err != nil {
		t.Fatalf("acquire RECOVER exclusive: %v", err)
	}
	defer func() { _ = w.index.unlock(lockRecover, lockExclusive) }()

	_, err := w.ensureHeaderInitialized()
	if !errors.Is(err, ErrBusyRecovery) {
		t.Fatalf("expected ErrBusyRecovery, got %v", err)
	}
}

// buildWalHeader builds a serialized WAL header with configurable fields.
// Callers override the defaults to craft malformed inputs.
func buildWalHeader(t *testing.T, version, pageSize uint32) []byte {
	t.Helper()
	buf := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], walMagic)
	binary.BigEndian.PutUint32(buf[4:8], version)
	binary.BigEndian.PutUint32(buf[8:12], pageSize)
	binary.BigEndian.PutUint32(buf[12:16], 0)          // checkpoint
	binary.BigEndian.PutUint32(buf[16:20], 0xdeadbeef) // salt1
	binary.BigEndian.PutUint32(buf[20:24], 0xcafef00d) // salt2
	c1, c2 := walChecksum(buf[0:24], 0, 0)
	binary.BigEndian.PutUint32(buf[24:28], c1)
	binary.BigEndian.PutUint32(buf[28:32], c2)
	return buf
}

// TestWalHeaderDeserialize_GoodHeader sanity-checks the helper — a
// well-formed header of the current version and default page size must
// deserialize cleanly.
func TestWalHeaderDeserialize_GoodHeader(t *testing.T) {
	buf := buildWalHeader(t, walVersion, DefaultPageSize)
	var h walHeader
	if err := h.deserialize(buf); err != nil {
		t.Fatalf("good header rejected: %v", err)
	}
	if h.version != walVersion || h.pageSize != DefaultPageSize {
		t.Fatalf("fields mis-parsed: version=%d pageSize=%d", h.version, h.pageSize)
	}
}

// TestWalHeaderDeserialize_RejectsBadVersion mirrors SQLite's
// walIndexRecover rejection at wal.c:1441-1446 (SQLITE_CANTOPEN). The error
// must be ErrWALVersion, NOT ErrWALCorrupt: recoverLocked truncates the WAL
// on corruption but must refuse to open on a version mismatch (a newer
// binary's WAL holds committed transactions this binary cannot read).
func TestWalHeaderDeserialize_RejectsBadVersion(t *testing.T) {
	buf := buildWalHeader(t, walVersion+1, DefaultPageSize)
	var h walHeader
	err := h.deserialize(buf)
	if !errors.Is(err, ErrWALVersion) {
		t.Fatalf("bad version should be ErrWALVersion, got %v", err)
	}
	if errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("version mismatch must not map to ErrWALCorrupt (would trigger destructive recovery), got %v", err)
	}
}

// TestWalHeaderDeserialize_RejectsBadPageSize mirrors SQLite's
// walIndexRecover rejection at wal.c:1414-1419 (non-power-of-2, too
// small, too large, zero).
func TestWalHeaderDeserialize_RejectsBadPageSize(t *testing.T) {
	cases := []struct {
		name string
		ps   uint32
	}{
		{"zero", 0},
		{"non-power-of-2", 4097},
		{"too small", MinPageSize / 2},
		{"too large", MaxPageSize * 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := buildWalHeader(t, walVersion, tc.ps)
			var h walHeader
			err := h.deserialize(buf)
			if !errors.Is(err, ErrWALCorrupt) {
				t.Fatalf("bad pageSize %d should be ErrWALCorrupt, got %v", tc.ps, err)
			}
		})
	}
}

// TestWriteFrames_ReuseOnRedirtied writes the same pgno twice in one
// write transaction and asserts that the second write OVERWRITES the
// first frame instead of appending a new one.
// Reference: SQLite wal.c:4117-4140 (walFrames reuse branch).
func TestWriteFrames_ReuseOnRedirtied(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)
	t.Cleanup(w.endWrite)

	// First spill: pgno 5, data V1. Not a commit.
	pg1 := &page{pgno: 5, data: make([]byte, 4096)}
	pg1.data[0] = 0xA1
	require.NoError(t, w.writeFrames([]*page{pg1}, false, 0))
	assert.Equal(t, uint32(1), w.nFrame.Load(), "first spill should append one frame")

	// Second spill: same pgno 5, data V1'. Not a commit. Reuse must fire.
	pg1b := &page{pgno: 5, data: make([]byte, 4096)}
	pg1b.data[0] = 0xA2
	require.NoError(t, w.writeFrames([]*page{pg1b}, false, 0))

	// Frame count must NOT have advanced — pgno 5's frame was overwritten.
	assert.Equal(t, uint32(1), w.nFrame.Load(), "second write should reuse the existing frame, not append")
	assert.Equal(t, uint32(1), w.iReCksum, "iReCksum should point at the overwritten frame")
}

// TestWriteFrames_CommitLastFrameNotReused — commit's LAST page is
// always appended fresh even when reuse-eligible, so it carries the
// dbSize commit marker. Reference: SQLite wal.c:4124 guard.
func TestWriteFrames_CommitLastFrameNotReused(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Spill pgno 7 → frame 1.
	pg7a := &page{pgno: 7, data: make([]byte, 4096)}
	require.NoError(t, w.writeFrames([]*page{pg7a}, false, 0))
	require.Equal(t, uint32(1), w.nFrame.Load())

	// Commit with pgno 7 again as the only (and therefore last) page.
	// Must append fresh (frame 2) carrying dbSize.
	pg7b := &page{pgno: 7, data: make([]byte, 4096)}
	pg7b.data[0] = 0xFF
	require.NoError(t, w.writeFrames([]*page{pg7b}, true, 7))

	assert.Equal(t, uint32(2), w.nFrame.Load(), "commit's last frame must be a fresh append with dbSize marker")

	w.endWrite()
}

// TestWriteFrames_ChecksumChainConsistentAfterRewrite forces a reuse
// followed by an append commit, then reopens to trigger recovery and
// verifies the latest data is read. Recovery would reject the WAL on
// a broken checksum chain.
func TestWriteFrames_ChecksumChainConsistentAfterRewrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())

	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)

	// Spill: pgno 3 (V1), pgno 5.
	pg3a := &page{pgno: 3, data: make([]byte, 4096)}
	pg3a.data[0] = 0x33
	pg5 := &page{pgno: 5, data: make([]byte, 4096)}
	pg5.data[0] = 0x55
	require.NoError(t, w.writeFrames([]*page{pg3a, pg5}, false, 0))

	// Commit: pgno 3 again (V1' → triggers reuse on frame 1) + pgno 9
	// as the last (fresh-appended commit-marker bearer).
	pg3b := &page{pgno: 3, data: make([]byte, 4096)}
	pg3b.data[0] = 0x77
	pg9 := &page{pgno: 9, data: make([]byte, 4096)}
	pg9.data[0] = 0x99
	require.NoError(t, w.writeFrames([]*page{pg3b, pg9}, true, 9))
	w.endWrite()
	require.NoError(t, w.close(false))

	// Reopen → recoverLocked validates the checksum chain across all
	// surviving frames including the one whose data was overwritten.
	w2 := newWal(path, 4096)
	require.NoError(t, w2.open())
	t.Cleanup(func() { _ = w2.close(false) })

	// Latest frame for pgno 3 should be the overwritten one (frame 1)
	// with V1' = 0x77 in its data.
	frame := mustWiGet(t, w2.index, 3, w2.index.maxFrame.Load())
	require.NotZero(t, frame, "pgno 3 should have a frame after recovery")
	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(frame, buf, nil, nil))
	assert.Equal(t, byte(0x77), buf[0], "recovery should yield the overwritten data, not the original")
}

// TestWriteFrames_SavepointRollbackResetsIReCksum confirms that
// rolling back past iReCksum clears it. Mirrors SQLite wal.c:3832.
func TestWriteFrames_SavepointRollbackResetsIReCksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	_, bwErr := w.beginWrite()
	require.NoError(t, bwErr)
	t.Cleanup(w.endWrite)

	// Spill pgno 2 → frame 1.
	pg2a := &page{pgno: 2, data: make([]byte, 4096)}
	require.NoError(t, w.writeFrames([]*page{pg2a}, false, 0))
	savedMxFrame := w.nFrame.Load() // would be a savepoint

	// Spill pgno 3 + reuse pgno 2.
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	pg2b := &page{pgno: 2, data: make([]byte, 4096)}
	pg2b.data[0] = 0xEE
	require.NoError(t, w.writeFrames([]*page{pg3, pg2b}, false, 0))

	require.Equal(t, uint32(2), w.nFrame.Load(), "frame 2 appended (pgno 3); pgno 2 reused frame 1")
	require.Equal(t, uint32(1), w.iReCksum, "iReCksum should point at overwritten frame 1")

	// Savepoint rollback to savedMxFrame=1: iReCksum (1) is NOT past
	// the new mxFrame, so it stays set.
	w.index.rollbackToFrame(savedMxFrame)
	w.nFrame.Store(savedMxFrame)
	w.resetIReCksumIfPast(savedMxFrame)
	assert.Equal(t, uint32(1), w.iReCksum, "rollback NOT past iReCksum should preserve it")

	// Rollback to mxFrame=0 (past iReCksum): should clear it.
	w.index.rollbackToFrame(0)
	w.nFrame.Store(0)
	w.resetIReCksumIfPast(0)
	assert.Equal(t, uint32(0), w.iReCksum, "rollback past iReCksum should clear it")
}

// fileSize returns the physical on-disk size of the database file in bytes.
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	require.NoError(t, err)
	return fi.Size()
}

// TestCheckpoint_PhysicalTruncateAfterShrink asserts that a full-backfill
// checkpoint physically shrinks the DB file down to the committed page count
// after a shrinking commit (here driven by backup's truncateTo, the same path
// used by sqlite3PagerTruncateImage / VACUUM). Mirrors SQLite walCheckpoint
// (wal.c:2320-2329): when mxSafeFrame==hdr.mxFrame the DB file is truncated to
// hdr.nPage*szPage.
//
// Before the fix, checkpointWithMode only wrote backfilled pages via WriteAt +
// fdatasync and never called dbFile.Truncate, so the file only ever grew and
// this test failed (the file stayed at its pre-shrink size).
func TestCheckpoint_PhysicalTruncateAfterShrink(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	srcPath := filepath.Join(dir, "src.db")
	dstPath := filepath.Join(dir, "dst.db")

	src, err := testOpen(t, srcPath, opts)
	require.NoError(t, err)
	defer func() { _ = src.Close() }()
	dst, err := testOpen(t, dstPath, opts)
	require.NoError(t, err)
	defer func() { _ = dst.Close() }()

	// src: a tiny database.
	stx, err := src.BeginWrite()
	require.NoError(t, err)
	sns, err := stx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, stx.Put(sns, []byte("small"), []byte("v")))
	require.NoError(t, stx.Commit())

	// dst: a much larger database (many pages).
	dtx, err := dst.BeginWrite()
	require.NoError(t, err)
	dns, err := dtx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, dtx.Commit())
	dtx2, err := dst.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 256)
	for i := 0; i < 800; i++ {
		require.NoError(t, dtx2.Put(dns, fmt.Appendf(nil, "dst-%05d", i), fat))
	}
	require.NoError(t, dtx2.Commit())

	// Checkpoint dst so the LARGE image is physically materialized on disk.
	require.NoError(t, dst.Checkpoint(CheckpointFull))
	bigPages := dst.DatabaseSize()
	pageSize := int64(dst.PageSize())
	bigFileSize := fileSize(t, dstPath)
	require.Equal(t, int64(bigPages)*pageSize, bigFileSize,
		"after checkpointing the large image the file should be bigPages*pageSize")
	require.Greater(t, bigPages, uint32(50), "test setup: dst must be many pages")

	// Backup the tiny src over the large dst. backup.finalize calls
	// pager.truncateTo(nSrcPage), committing a SHRINKING image to dst's WAL.
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	for {
		err := b.Step(-1)
		if err == ErrBackupDone {
			break
		}
		require.NoError(t, err)
	}
	require.NoError(t, b.Finish())

	smallPages := dst.DatabaseSize()
	require.Less(t, smallPages, bigPages, "backup must have shrunk the logical page count")

	// At this point the shrink lives in dst's WAL; the physical file is still
	// the large size (truncation is deferred to checkpoint).
	require.Equal(t, bigFileSize, fileSize(t, dstPath),
		"file should still be large before the post-shrink checkpoint")

	// Full-backfill checkpoint: must physically shrink the file to the
	// committed page count.
	require.NoError(t, dst.Checkpoint(CheckpointFull))

	wantSize := int64(smallPages) * pageSize
	gotSize := fileSize(t, dstPath)
	require.Equal(t, wantSize, gotSize,
		"checkpoint must physically truncate the DB file to smallPages*pageSize (SQLite wal.c:2320-2329)")
	require.Less(t, gotSize, bigFileSize, "file must be physically smaller after the shrinking checkpoint")
}

// TestCheckpoint_NoTruncateWithOlderSnapshotReader asserts that the
// full-backfill truncate is SKIPPED while a concurrent reader is pinned to an
// older (pre-shrink) snapshot, then performed once that reader closes.
//
// This is the concurrent-reader-safety edge case SQLite protects (wal.c:2229-2245
// + 2322): a reader holding readmark==its mxFrame lowers mxSafeFrame below the
// live mxFrame, so mxSafeFrame!=authoritativeMxFrame() at the truncate guard and
// the trailing pages the reader still reads from the DB file are preserved.
func TestCheckpoint_NoTruncateWithOlderSnapshotReader(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.DisableAutoCheckpoint = true
	opts.InProcess = true

	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Grow to many pages.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	fat := make([]byte, 256)
	for i := 0; i < 800; i++ {
		require.NoError(t, tx2.Put(ns, fmt.Appendf(nil, "k-%05d", i), fat))
	}
	require.NoError(t, tx2.Commit())

	// Checkpoint so the large image is on disk, then write one more commit so
	// nBackfill < mxFrame and a new reader pins a REAL readmark slot (1-4)
	// rather than the slot-0 "read nothing from WAL" fast path.
	require.NoError(t, db.Checkpoint(CheckpointFull))
	bigPages := db.DatabaseSize()
	pageSize := int64(db.PageSize())
	bigFileSize := fileSize(t, path)
	require.Equal(t, int64(bigPages)*pageSize, bigFileSize)
	require.Greater(t, bigPages, uint32(50))

	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx3.Put(ns, []byte("marker"), []byte("1")))
	require.NoError(t, tx3.Commit())

	// Open a reader pinned to this PRE-shrink snapshot. It takes a readmark
	// slot at the current mxFrame.
	reader, err := db.BeginRead()
	require.NoError(t, err)
	readerOpen := true
	defer func() {
		if readerOpen {
			_ = reader.Rollback()
		}
	}()

	// Now commit a SHRINKING image via truncateTo. This drives the pager's
	// dbSize down and commits the smaller page-1 header to the WAL, advancing
	// mxFrame past the reader's pinned snapshot.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	smallPages := uint32(8)
	require.Less(t, smallPages, bigPages)
	require.NoError(t, db.pager.truncateTo(smallPages))
	require.NoError(t, wtx.Commit())
	require.Equal(t, smallPages, db.DatabaseSize())

	// Checkpoint while the older-snapshot reader is still open. The reader's
	// readmark lowers mxSafeFrame below the live mxFrame, so the truncate guard
	// (mxSafeFrame==authoritativeMxFrame()) is FALSE and the file is NOT shrunk:
	// the trailing pages the reader still reads must remain on disk.
	//
	// The non-PASSIVE checkpoint reports ErrBusy because the active reader
	// blocked a complete backfill (BUSY-means-retry, wal.c:2352-2356); the
	// data-path guard below (file must NOT shrink) is what this test verifies
	// and is unaffected by the error return.
	require.ErrorIs(t, db.Checkpoint(CheckpointFull), ErrBusy)
	require.Equal(t, bigFileSize, fileSize(t, path),
		"file must NOT shrink while an older-snapshot reader is open (concurrent-reader safety, wal.c:2322)")

	// Close the reader, then checkpoint again. Now mxSafeFrame can reach the
	// live mxFrame, the guard passes, and the file physically shrinks.
	require.NoError(t, reader.Rollback())
	readerOpen = false

	require.NoError(t, db.Checkpoint(CheckpointFull))
	wantSize := int64(smallPages) * pageSize
	require.Equal(t, wantSize, fileSize(t, path),
		"file must physically shrink to smallPages*pageSize once no older reader pins the old size")
	require.Less(t, fileSize(t, path), bigFileSize)
}

// TestInProcessSHMHeaderFrozenButReadsSeeCommits pins the by-design drift
// documented in docs/btree/NOTES.md#old-drift-inprocess-skips-shm-hdr-on-commit:
//
//	"In-process mode skips SHM hdr updates on commit (writeFrames !w.inProcess
//	 guard). db.BeginRead/BeginWrite synthesize a minimal walHdr{isInit:1,
//	 mxFrame:maxFrame} in that mode so read paths consuming tx.walHdr.mxFrame
//	 see the correct frame ceiling."
//
// The drift relies on TWO invariants that this test pins:
//
//  1. FROZEN HEADER: in in-process mode the heap-SHM region-0 WAL-index header
//     is written exactly once at open (initHeaderStateLocked -> writeHeader(0,
//     0,0,...), wal.go:1718, NOT gated by inProcess) and is NEVER refreshed on
//     commit (writeFrames gates w.index.writeHeader behind `if !w.inProcess`,
//     wal.go:2204). So for the DB's lifetime readHeader() returns the frozen
//     {isInit:1, mxFrame:0, nPage:0} regardless of how many commits land.
//
//  2. SYNTHESIZED CEILING: despite the frozen header, BeginRead (db.go:797) and
//     BeginWrite (db.go:930) synthesize walHdr{isInit:1, mxFrame:maxFrame} from
//     the live process-local mxCommitFrame atomic (beginReadHdr ->
//     tryBeginReadInProcessHdr, wal.go:2613), so reads observe every committed
//     frame.
//
// Why this fails loudly under a future refactor:
//
//   - If someone "fixes" the commit path to publish the SHM header in-process
//     (drops the `!w.inProcess` guard at wal.go:2204), readHeader().mxFrame
//     advances past 0 and the FROZEN HEADER assertion below fails. That guard
//     is load-bearing: the heap SHM hash slots are written without the
//     cross-process barrier (writeHeader's walShmBarrier is itself gated on
//     !inProcess), so publishing a non-zero mxFrame there would expose readers
//     to a header that promises frames the in-process get() path does not route
//     through. The test makes that silent re-coupling impossible.
//
//   - If someone breaks the synthesis (e.g. drops the else-branch at
//     db.go:797/930 so tx.walHdr stays the zero/frozen header), the SYNTHESIZED
//     CEILING assertions fail: WalMaxFrame() reads 0 and the post-commit reader
//     cannot see the committed rows.
func TestInProcessSHMHeaderFrozenButReadsSeeCommits(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := testOpen(t, dbPath, Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)
	defer db.Close()

	// Precondition: this DB really is in in-process (heap-SHM) mode, otherwise
	// the drift under test does not apply and the assertions would be vacuous.
	require.True(t, db.pager.inProcess, "test requires InProcess mode")
	require.True(t, db.pager.wal.inProcess, "wal must be in-process")
	require.True(t, db.pager.wal.index.inProcess, "wal index must be in-process")

	// readSHMHeader reads the RAW heap-SHM region-0 WAL-index header — the only
	// thing a cross-process peer (or any non-synthesizing reader) could observe.
	// This bypasses the BeginRead/BeginWrite synthesis entirely.
	readSHMHeader := func() WalIndexHdr {
		hdr, valid := db.pager.wal.index.readHeader()
		require.True(t, valid, "heap-SHM region-0 header must be valid (written once at open)")
		return hdr
	}

	// At open, initHeaderStateLocked has written the frozen header.
	openHdr := readSHMHeader()
	require.Equal(t, uint8(1), openHdr.isInit, "header initialized at open")
	require.Equal(t, uint32(0), openHdr.mxFrame, "open header mxFrame is 0")
	require.Equal(t, uint32(0), openHdr.nPage, "open header nPage is 0")

	// Perform several committed writes. Each commit appends WAL frames and
	// advances the process-local mxCommitFrame, but (in-process) must NOT touch
	// the SHM region-0 header.
	const nsName = "drift"
	const nRows = 64
	const valSize = 200 // forces multi-page growth so nPage would change if published

	want := make(map[uint32][]byte, nRows)
	for round := 0; round < 3; round++ {
		wtx, err := db.BeginWrite()
		require.NoError(t, err)

		// The writer's synthesized walHdr must already carry a non-zero ceiling
		// once frames are committed (invariant 2, BeginWrite path, db.go:930).
		if round > 0 {
			require.NotZero(t, wtx.WalMaxFrame(),
				"BeginWrite must synthesize a non-zero mxFrame after prior commits")
			require.Equal(t, db.pager.wal.index.mxCommitFrame.LoadLocal(), wtx.WalMaxFrame(),
				"synthesized writer mxFrame must equal live committed frame ceiling")
		}

		ns, err := wtx.CreateNamespace(nsName)
		if err != nil {
			// Namespace already exists after the first round; re-resolve it.
			ns, err = wtx.GetNamespace(nsName)
		}
		require.NoError(t, err)

		for i := 0; i < nRows; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(round*nRows+i))
			val := []byte(fmt.Sprintf("r%d-row%d-", round, i))
			for len(val) < valSize {
				val = append(val, byte(i))
			}
			require.NoError(t, wtx.Put(ns, key, val))
			want[binary.BigEndian.Uint32(key)] = val
		}
		require.NoError(t, wtx.Commit())

		// INVARIANT 1 (FROZEN HEADER): the raw SHM header is unchanged by the
		// commit. mxCommitFrame has advanced, yet the published header is still
		// frozen at the open-time {isInit:1, mxFrame:0, nPage:0}.
		require.Positive(t, db.pager.wal.index.mxCommitFrame.LoadLocal(),
			"commit must advance the process-local committed-frame cursor")
		postHdr := readSHMHeader()
		require.Equal(t, uint8(1), postHdr.isInit, "header stays initialized")
		require.Equal(t, uint32(0), postHdr.mxFrame,
			"in-process commit must NOT publish mxFrame to the SHM header")
		require.Equal(t, uint32(0), postHdr.nPage,
			"in-process commit must NOT publish nPage to the SHM header")
	}

	committedFrame := db.pager.wal.index.mxCommitFrame.LoadLocal()
	require.Positive(t, committedFrame)

	// INVARIANT 2 (SYNTHESIZED CEILING): a fresh reader synthesizes a correct
	// minimal walHdr from the live committed-frame cursor (NOT from the frozen
	// SHM header), and therefore observes every committed row across the
	// commit->read cycle.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	require.Equal(t, uint8(1), rtx.walHdr.isInit,
		"reader walHdr must be synthesized as initialized")
	require.Equal(t, committedFrame, rtx.WalMaxFrame(),
		"reader's synthesized mxFrame must equal the committed frame ceiling")
	// Sanity: the synthesized reader ceiling is NOT what the raw SHM header says.
	require.NotEqual(t, readSHMHeader().mxFrame, rtx.WalMaxFrame(),
		"reader ceiling must come from synthesis, not the frozen SHM header")

	ns, err := rtx.GetNamespace(nsName)
	require.NoError(t, err)
	for k, v := range want {
		key := binary.BigEndian.AppendUint32(nil, k)
		got, err := rtx.Get(ns, key)
		require.NoError(t, err, "committed key %d must be visible to a fresh reader", k)
		require.Equal(t, v, got, "value for key %d must match the committed write", k)
	}
}

// These regression tests pin SQLite's bounded-probe corruption detection on
// both SHM-hash paths (drift-91). Before the fix, a full / cyclically-corrupt
// hash segment was NOT detected: shmHashWrite fell out of `for range htNSlot`
// having written aPgno[idx] with no reachable aHash slot (silent
// committed-but-unindexed frame), and shmHashGet fell out of the loop
// returning 0 (a silent miss that masks corruption and serves a stale page
// from the DB file). After the fix both surface ErrCorrupt, mirroring C's
// walIndexAppend (wal.c:1333-1336) and walFindFrame (wal.c:3580,3592-3595)
// returning SQLITE_CORRUPT_BKPT.

// hashSlotOff returns the byte offset of aHash[h] within a (non-region-0)
// segment region. Region 0 shares the same aHash layout offset.
func hashSlotOff(h int) int { return htHashArrayOff + h*2 }

// TestShmHashWrite_CollisionLimit_Corrupt verifies the WRITE-side bound:
// walking past nCollide = idx+1 occupied slots returns ErrCorrupt instead of
// silently inserting nothing. Mirrors walIndexAppend's
// `nCollide = idx; for(...){ if((nCollide--)==0) return SQLITE_CORRUPT_BKPT; }`.
func TestShmHashWrite_CollisionLimit_Corrupt(t *testing.T) {
	wi, err := newWalIndex("", true) // in-process shm
	require.NoError(t, err)
	defer wi.close(false)

	region, err := wi.shm.region(0, true)
	require.NoError(t, err)

	// Target frame 2 => idx 1 => nCollide = idx+1 = 2. The probe may pass over
	// at most two occupied slots; a third occupied slot is provably corrupt.
	// (Frame 1 / idx 0 cannot exercise the bound: the segment's first write
	// lazily zeroes the whole table first, matching SQLite walIndexAppend
	// wal.c:1324-1330.)
	const pgno = uint32(12345)
	const frame = uint32(2)
	h0 := int(pgno*htHash1) & (htNSlot - 1)

	// Occupy the first THREE slots of pgno's probe chain with non-matching,
	// valid entries so there is no empty slot within nCollide+1 probes. (No
	// fourth slot is occupied, so a fix that mis-bounds the probe would still
	// find a free slot — only the correctly-bounded check fires here.)
	binary.LittleEndian.PutUint16(region[hashSlotOff(h0):], 7)
	binary.LittleEndian.PutUint16(region[hashSlotOff((h0+1)&(htNSlot-1)):], 9)
	binary.LittleEndian.PutUint16(region[hashSlotOff((h0+2)&(htNSlot-1)):], 11)

	err = wi.shmHashWrite(pgno, frame)
	require.ErrorIs(t, err, ErrCorrupt,
		"shmHashWrite must return ErrCorrupt when the probe walks past nCollide=idx+1 occupied slots")
}

// TestShmHashWrite_CollisionLimit_PropagatesThroughSetBatch verifies the
// ErrCorrupt aborts the commit via setBatch (the production write path),
// matching C's walFrames loop stopping on SQLITE_CORRUPT and skipping the
// mxFrame advance (wal.c:4229-4257).
func TestShmHashWrite_CollisionLimit_PropagatesThroughSetBatch(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close(false)

	region, err := wi.shm.region(0, true)
	require.NoError(t, err)

	const pgno = uint32(54321)
	h0 := int(pgno*htHash1) & (htNSlot - 1)
	// frame 2 => idx 1 => nCollide = 2: three occupied slots => corrupt.
	// (Frame 1 would lazily zero the segment first — see walIndexAppend's
	// idx==1 memset, mirrored in shmHashWrite.)
	binary.LittleEndian.PutUint16(region[hashSlotOff(h0):], 3)
	binary.LittleEndian.PutUint16(region[hashSlotOff((h0+1)&(htNSlot-1)):], 5)
	binary.LittleEndian.PutUint16(region[hashSlotOff((h0+2)&(htNSlot-1)):], 7)

	p := &page{pgno: pgno}
	err = wi.setBatch([]*page{p}, 2, true)
	require.ErrorIs(t, err, ErrCorrupt,
		"setBatch must propagate shmHashWrite's ErrCorrupt so the commit aborts")
}

// TestShmHashWrite_FirstFrameLazilyClearsSegment verifies that writing the
// FIRST entry of a hash segment zeroes the whole segment first, matching
// SQLite walIndexAppend (wal.c:1324-1330 `if( idx==1 ) memset(...)`). This is
// what makes stale entries from a previous WAL generation harmless: a WAL
// reset only clears the segments the resetting connection has mapped, so a
// regrowing WAL must be able to reclaim a junk-filled segment. Regression
// test for the multi-process writer aborting with ErrCorrupt at the first
// frame of segment 32 after a reset of a larger WAL.
func TestShmHashWrite_FirstFrameLazilyClearsSegment(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close(false)

	region, err := wi.shm.region(0, true)
	require.NoError(t, err)

	// Simulate a previous WAL generation: fill EVERY hash slot and a few
	// aPgno entries with junk, as if the segment had been fully used and
	// never cleared by reset.
	for h := 0; h < htNSlot; h++ {
		binary.LittleEndian.PutUint16(region[hashSlotOff(h):], uint16(h%htNPage)+1)
	}
	for i := 0; i < 16; i++ {
		binary.LittleEndian.PutUint32(region[htPgnoOff0+i*4:], uint32(100000+i))
	}

	// Writing frame 1 (the segment's first entry of the new generation) must
	// succeed — not ErrCorrupt — because the segment is zeroed first.
	const pgno = uint32(777)
	require.NoError(t, wi.shmHashWrite(pgno, 1),
		"first frame of a segment must lazily clear stale entries, not trip the collision bound")

	// The new entry is findable, and the junk is gone: a lookup for one of
	// the junk page numbers misses cleanly.
	frame := mustShmHashGet(t, wi, pgno, 1, 1)
	require.Equal(t, uint32(1), frame)
	miss := mustShmHashGet(t, wi, 100003, 1, 1)
	require.Zero(t, miss, "stale pre-reset entries must not survive the segment's first write")
}

// TestShmHashGet_FullChain_Corrupt verifies the READ-side bound: a probe chain
// that walks past nCollide = htNSlot occupied slots without an empty slot
// returns ErrCorrupt instead of a silent 0/miss. Mirrors walFindFrame's
// `nCollide = HASHTABLE_NSLOT; ... if((nCollide--)==0){ *piRead=0; return
// SQLITE_CORRUPT_BKPT; }`.
func TestShmHashGet_FullChain_Corrupt(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close(false)

	region, err := wi.shm.region(0, true)
	require.NoError(t, err)

	// Fill EVERY aHash slot of segment 0 with a non-zero entry pointing at a
	// valid-but-non-matching aPgno index, so the probe chain for any pgno never
	// hits an empty slot. Using entry index 1 (=> aPgno[0]) which we set to a
	// page number that won't match the looked-up pgno.
	for h := 0; h < htNSlot; h++ {
		binary.LittleEndian.PutUint16(region[hashSlotOff(h):], 1)
	}
	binary.LittleEndian.PutUint32(region[htPgnoOff0:], 999999) // aPgno[0]

	frame, err := wi.shmHashGet(7, 100, 1)
	require.ErrorIs(t, err, ErrCorrupt,
		"shmHashGet must return ErrCorrupt when the probe walks a full chain of occupied slots")
	require.Zero(t, frame, "corrupt read must not return a frame (C sets *piRead = 0)")
}

// TestShmHashGet_FullChain_Corrupt_PropagatesThroughGet verifies ErrCorrupt
// reaches the multi-process get() wrapper (the read path used by the pager),
// rather than being swallowed into a 0 miss that would read a stale DB page.
func TestShmHashGet_FullChain_Corrupt_PropagatesThroughGet(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close(false)

	// get() in multi-process mode consults shmHashGet exclusively.
	wi.inProcess = false

	region, err := wi.shm.region(0, true)
	require.NoError(t, err)
	for h := 0; h < htNSlot; h++ {
		binary.LittleEndian.PutUint16(region[hashSlotOff(h):], 1)
	}
	binary.LittleEndian.PutUint32(region[htPgnoOff0:], 999999)

	frame, err := wi.get(7, 100, wi.liveMinFrame())
	require.ErrorIs(t, err, ErrCorrupt,
		"walIndex.get must forward shmHashGet's ErrCorrupt to the pager")
	require.Zero(t, frame)
}

// Test-only thin wrappers that assert the (now error-returning) walIndex
// lookup/insert methods succeed, so the many existing positive-path tests can
// keep their compact `assert.Equal(t, want, wi.getX(...))` form without each
// site re-deriving the (value, error) pair. A real ErrCorrupt still fails the
// test loudly via t.Fatal. Tests that specifically exercise the error path
// call the error-returning methods directly.

func mustShmHashGet(t *testing.T, wi *walIndex, pgno, maxFrame, minFrame uint32) uint32 {
	t.Helper()
	v, err := wi.shmHashGet(pgno, maxFrame, minFrame)
	if err != nil {
		t.Fatalf("shmHashGet(%d, %d, %d): unexpected error: %v", pgno, maxFrame, minFrame, err)
	}
	return v
}

func mustShmHashWrite(t *testing.T, wi *walIndex, pgno, frame uint32) {
	t.Helper()
	if err := wi.shmHashWrite(pgno, frame); err != nil {
		t.Fatalf("shmHashWrite(%d, %d): unexpected error: %v", pgno, frame, err)
	}
}

func mustWiGet(t *testing.T, wi *walIndex, pgno, maxFrame uint32) uint32 {
	t.Helper()
	v, err := wi.get(pgno, maxFrame, wi.liveMinFrame())
	if err != nil {
		t.Fatalf("walIndex.get(%d, %d): unexpected error: %v", pgno, maxFrame, err)
	}
	return v
}

func mustWiGetLatest(t *testing.T, wi *walIndex, pgno uint32) uint32 {
	t.Helper()
	v, err := wi.getLatest(pgno)
	if err != nil {
		t.Fatalf("walIndex.getLatest(%d): unexpected error: %v", pgno, err)
	}
	return v
}

func mustWiSet(t *testing.T, wi *walIndex, pgno, frame uint32) {
	t.Helper()
	if err := wi.set(pgno, frame); err != nil {
		t.Fatalf("walIndex.set(%d, %d): unexpected error: %v", pgno, frame, err)
	}
}

// hookShm wraps a real shm and runs onLock(slot, lockType) just before each
// lock acquisition is granted. It lets a test deterministically inject a
// concurrent-checkpointer/writer state change into the precise window between
// tryBeginReadInProcessHdr's pre-lock metadata scan and its shared-lock
// acquisition on the reused reader slot — reproducing the cross-goroutine race
// (internal auto-checkpoint runs without pager.mu) without sleeps or timing.
type hookShm struct {
	shm
	onLock func(slot, lockType int)
}

func (h *hookShm) lock(slot, lockType int) error {
	if h.onLock != nil {
		h.onLock(slot, lockType)
	}
	return h.shm.lock(slot, lockType)
}

// TestWALReaderSlotReuseRevalidatesOnStaleMark reproduces the read-safety
// violation that arises when the in-process reader-slot REUSE branch of
// tryBeginReadInProcessHdr skips post-lock re-validation.
//
// Scenario:
//
//  1. A reader claimed slot 1 at mxFrame=10, then ended. endRead never resets a
//     slot's mark, so slot 1 keeps the stale mark 10 while nBackfill stays < 10.
//  2. A reusing reader runs the lock-free scan and picks slot 1 (mark 10 <=
//     mxFrame 10). BUT between that scan and acquiring the shared lock, an
//     internal concurrent checkpoint advances nBackfill past the snapshot the
//     reusing reader is about to adopt (and a commit advances mxCommitFrame).
//  3. If the reuse branch returns without re-validating, the reader proceeds
//     with maxFrame=10 while nBackfill has moved to 10 — the checkpointer is
//     now free to backfill/overwrite frames the reader believes it can read,
//     corrupting the reader's snapshot.
//
// With the fix the reuse branch re-loads mxCommitFrame/nBackfill after taking
// the shared lock; because they changed it drops the lock and returns
// errWALRetry. On retry the nBackfill==mxFrame slot-0 fast path is safe.
func TestWALReaderSlotReuseRevalidatesOnStaleMark(t *testing.T) {
	dir := t.TempDir()
	w := newWal(filepath.Join(dir, "test.wal"), 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	// Establish the precondition directly on the WAL index (the established
	// pattern for WAL unit tests): committed up to frame 10, only frames <= 3
	// backfilled, and slot 1 holding a stale mark of 10 from a departed reader.
	w.index.mxCommitFrame.Store(10)
	w.index.maxFrame.Store(10)
	w.index.nBackfill.Store(3)
	w.index.aReadMark[1].Store(10) // stale mark left by an ended reader

	// Inject the concurrent checkpoint + writer that runs in the race window:
	// when the reusing reader takes the SHARED lock on reused slot 1
	// (lockRead0+1), advance nBackfill up to (and past) the stale mark and bump
	// the commit frame, exactly as an internal auto-checkpoint + commit would.
	var injected bool
	hooked := &hookShm{shm: w.index.shm}
	hooked.onLock = func(slot, lockType int) {
		if slot == lockRead0+1 && lockType == lockShared && !injected {
			injected = true
			// Checkpointer backfilled the WAL up to frame 10 and a writer
			// committed new frames, raising the commit ceiling to 20.
			w.index.nBackfill.Store(10)
			w.index.mxCommitFrame.Store(20)
			w.index.maxFrame.Store(20)
		}
	}
	w.index.shm = hooked

	// The reuse path must detect the changed state and signal retry rather than
	// silently adopting the stale (now-unsafe) snapshot.
	_, _, _, _, err := w.tryBeginReadInProcessHdr()
	require.True(t, injected, "test must exercise the slot-reuse shared-lock path")
	require.ErrorIs(t, err, errWALRetry,
		"reuse branch must re-validate and retry when the checkpointer advanced "+
			"nBackfill/mxCommitFrame past the reused slot's stale mark")

	// The retry attempt (state now stable: nBackfill=10, mxCommitFrame=20) must
	// adopt the LIVE commit ceiling (20), never the stale snapshot (10). A
	// snapshot that floored at the stale mark while nBackfill had already
	// advanced to 10 is exactly the corruption window the re-validation closes.
	hdr, maxFrame, _, slot, err := w.tryBeginReadInProcessHdr()
	require.NoError(t, err)
	assert.Equal(t, uint32(20), maxFrame, "retry must adopt the live commit ceiling, not the stale 10")
	assert.Equal(t, uint32(20), hdr.mxFrame)
	// The reader's snapshot ceiling (20) must stay strictly above the
	// checkpointer's backfill floor (nBackfill==10): walIndex.get() floors reads
	// at nBackfill+1==11, so no frame the checkpointer already backfilled past is
	// readable through this snapshot. A held reused slot's mark may lag the
	// snapshot — that is conservative-safe because the checkpointer cannot
	// exclusively lock a still-held slot and so keeps frames above the mark live.
	assert.Greater(t, maxFrame, w.index.nBackfill.Load(),
		"reader snapshot must stay above the checkpointer's backfill floor")
	w.endRead(slot)
}

// TestWALReaderSlotReuseFastPathSafeAfterBackfill verifies the companion
// guarantee referenced by the fix: once the checkpointer has backfilled the
// whole WAL (nBackfill == mxFrame), a reusing reader takes the slot-0 fast path
// and adopts a correct, fully-backfilled snapshot — the fix must not break it.
func TestWALReaderSlotReuseFastPathSafeAfterBackfill(t *testing.T) {
	dir := t.TempDir()
	w := newWal(filepath.Join(dir, "test.wal"), 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	// mxFrame==nBackfill: WAL fully checkpointed. A stale slot-1 mark of 10 must
	// be ignored in favor of the slot-0 fast path.
	w.index.mxCommitFrame.Store(10)
	w.index.maxFrame.Store(10)
	w.index.nBackfill.Store(10)
	w.index.aReadMark[1].Store(10)

	hdr, maxFrame, _, slot, err := w.tryBeginReadInProcessHdr()
	require.NoError(t, err)
	assert.Equal(t, 0, slot, "fully-backfilled WAL must use the slot-0 fast path")
	assert.Equal(t, uint32(10), maxFrame)
	assert.Equal(t, uint32(10), hdr.mxFrame)
	w.endRead(slot)
}

// TestWALReaderSlotReuseSucceedsWhenStateStable confirms the fix does not
// over-retry: when no checkpoint/writer advances state during slot reuse, a
// reusing reader on a valid slot proceeds normally on the reused slot.
func TestWALReaderSlotReuseSucceedsWhenStateStable(t *testing.T) {
	dir := t.TempDir()
	w := newWal(filepath.Join(dir, "test.wal"), 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	t.Cleanup(func() { _ = w.close(false) })

	// Committed to 10, backfilled to 3, slot 1 holds mark 10 == mxFrame, and
	// no concurrent state change happens during acquisition.
	w.index.mxCommitFrame.Store(10)
	w.index.maxFrame.Store(10)
	w.index.nBackfill.Store(3)
	w.index.aReadMark[1].Store(10)

	hdr, maxFrame, _, slot, err := w.tryBeginReadInProcessHdr()
	require.NoError(t, err)
	require.False(t, errors.Is(err, errWALRetry))
	assert.Equal(t, 1, slot, "stable state must reuse the existing slot 1")
	assert.Equal(t, uint32(10), maxFrame)
	assert.Equal(t, uint32(10), hdr.mxFrame)
	w.endRead(slot)
}

// TestWALReadFrameFaultFallsThroughToDisk pins the by-design behavior documented
// at docs/btree/NOTES.md#drift-6-wal-frame-read-failure-falls-through-to-disk-read.
//
// In C readDbPage (pager.c:3035-3045) a WAL-resolved page is read ONLY from the
// WAL frame and a sqlite3WalReadFrame failure propagates as the page-get error.
// The Go getters (getPageWriter / readTempPage / getPageReader) instead, on a
// readFrame failure, deliberately ignore the error and fall through to a DB-file
// (or masterStore) read. This is a deliberate drift, NOT a bug, because the
// primary protections that C relies on are preserved in Go:
//
//	(1) A reader holds its WAL read-mark slot for the whole transaction, so the
//	    authoritative WAL frame within the reader's snapshot is served from the
//	    WAL (the normal, non-faulting path).
//	(2) A checkpoint cannot truncate / reset the WAL out from under a held reader
//	    slot, and the backfill-before-truncate ordering guarantees that any frame
//	    a reader could resolve is already durable in the DB file before it could
//	    ever be truncated. So the disk fallthrough can only return content at
//	    least as new as the faulting WAL frame — never an older committed copy.
//
// This test installs the minimal test-only readFrame fault-injection hook and
// asserts: the frame is normally served from the WAL; truncation cannot occur
// under a held reader slot; and on an injected readFrame error the getter falls
// through (no error surfaced) rather than propagating the WAL read failure.
//
// There is no production fault-injection hook on wal.readFrame — this hook is the
// minimal test-only surface required to make the invariant deterministically
// observable.
func TestWALReadFrameFaultFallsThroughToDisk(t *testing.T) {
	// Ensure any hook installed by this (or a prior) test is cleared on exit so
	// the production no-op path is restored for every other test.
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")
	// DisableAutoCheckpoint keeps committed frames in the WAL so the reader's
	// snapshot resolves pages to live WAL frames (the precondition for the drift).
	db, err := testOpen(t, dbPath, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// Seed a namespace and rows so several pages (incl. page 1) gain WAL frames.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 1; i <= 64; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, wtx.Put(ns, key, make([]byte, 200)))
	}
	require.NoError(t, wtx.Commit())

	// Open a read transaction. The reader claims a WAL read-mark slot and freezes
	// its snapshot ceiling (walMaxFrame); it holds the slot for the whole tx.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })

	walMaxFrame := rtx.WalMaxFrame()
	require.Greater(t, walMaxFrame, uint32(0),
		"reader snapshot must include the just-committed WAL frames")

	pager := db.pager

	// Find a page that resolves to a live WAL frame within the reader's snapshot.
	// This is exactly the C `iFrame != 0` case where C reads ONLY from the WAL.
	var targetPgno, targetFrame uint32
	for pgno := uint32(1); pgno <= pager.dbSize.Load(); pgno++ {
		if f := mustWiGet(t, pager.wal.index, pgno, walMaxFrame); f > 0 {
			targetPgno, targetFrame = pgno, f
			break
		}
	}
	require.NotZero(t, targetPgno, "expected at least one page resolvable to a WAL frame")
	require.NotZero(t, targetFrame)

	// readPgnoFreshCache reads targetPgno through getPageReader with a brand-new
	// reader cache, forcing a true cache miss so wal.readFrame is invoked every
	// call (a cache hit would bypass readFrame and defeat the fault injection).
	readPgnoFreshCache := func() ([]byte, error) {
		cache := newPcache(int(pager.pageSize), db.readerCacheSize, true)
		pg, gerr := pager.getPageReader(targetPgno, walMaxFrame, cache)
		if gerr != nil {
			return nil, gerr
		}
		out := make([]byte, pager.pageSize)
		copy(out, pg.data[:pager.pageSize])
		pager.releasePage(pg)
		return out, nil
	}

	// --- Protection (1): the frame within the snapshot is served from the WAL. ---
	// With no fault injected, getPageReader must succeed and return the WAL-frame
	// content. Capture it as the authoritative bytes for the fallthrough check.
	require.Nil(t, walReadFrameFaultHook.Load(), "no hook installed yet")
	walServed, err := readPgnoFreshCache()
	require.NoError(t, err, "frame within snapshot must be served from the WAL without error")

	// --- The drift: an injected readFrame error falls through, not propagates. ---
	// Inject a failure for exactly the target frame. C would propagate this as the
	// page-get error; Go must instead fall through to the DB-file/masterStore read
	// and return successfully (the documented, intentional behavior). This is the
	// core invariant this regression test pins.
	sentinel := errors.New("injected WAL readFrame fault")
	var hookFired bool
	setWalReadFrameFaultHook(func(frame uint32) error {
		if frame == targetFrame {
			hookFired = true
			return sentinel
		}
		return nil
	})

	fellThrough, err := readPgnoFreshCache()
	require.True(t, hookFired, "fault hook must have been exercised for the target frame")
	require.NoError(t, err,
		"by design, a readFrame failure must fall through to a disk read, not surface the error")
	require.NotNil(t, fellThrough)
	// Note: at this point (frame NOT yet backfilled to the DB file) the fallthrough
	// can legitimately return content OLDER than the WAL frame — that is exactly the
	// drift hazard the NOTES entry documents. We deliberately do NOT assert the
	// fallthrough equals the WAL bytes here: the safety argument is not "the disk
	// copy is always current" but "this fallthrough is unreachable in production",
	// proven by the two protections asserted below. Asserting current-content here
	// would falsely claim the drift is harmless even before backfill.

	// Clearing the hook restores the WAL-served path with no error, reconfirming the
	// non-faulting production path returns the authoritative WAL frame content.
	setWalReadFrameFaultHook(nil)
	require.Nil(t, walReadFrameFaultHook.Load())
	afterClear, err := readPgnoFreshCache()
	require.NoError(t, err)
	assert.Equal(t, walServed, afterClear,
		"clearing the fault must restore the normal WAL-served read")

	// --- Protection (2): truncation/reset cannot occur under a held reader slot. ---
	// The structural guarantee that makes the (unreachable-in-prod) fallthrough safe:
	// a reader holding its read-mark slot blocks the checkpointer from RESET/TRUNCATE
	// (tryResetWALWithBusy must exclusively lock the reader slots, which ErrBusy-fails
	// while one is held), so the WAL frame numbering and salt are NOT recycled under
	// the reader. A passive backfill may copy frames to the DB file and raise
	// nBackfill (transparently shifting the reader's reads to the now-durable disk
	// copy), but the WAL is never reset out from under the snapshot — so readFrame
	// cannot fail due to truncation, and the only way to reach the fallthrough is a
	// genuine I/O error (which this test simulates).
	saltBefore := [2]uint32{pager.wal.header.salt1, pager.wal.header.salt2}
	nFrameBefore := pager.wal.nFrame.Load()
	require.Greater(t, nFrameBefore, uint32(0), "WAL must hold frames before the checkpoint")

	require.NoError(t, db.Checkpoint(CheckpointTruncate),
		"truncate checkpoint must not error while a reader holds a slot")

	saltAfter := [2]uint32{pager.wal.header.salt1, pager.wal.header.salt2}
	assert.Equal(t, saltBefore, saltAfter,
		"WAL salt must be unchanged: a held reader slot blocks WAL reset/truncation")
	assert.Equal(t, nFrameBefore, pager.wal.nFrame.Load(),
		"WAL frame numbering must not be recycled while a reader holds its slot")

	// The reader's snapshot remains internally consistent across the checkpoint:
	// the same page reads back with identical content (no error).
	afterCkpt, err := readPgnoFreshCache()
	require.NoError(t, err, "page must remain readable across a checkpoint under a held slot")
	assert.Equal(t, walServed, afterCkpt,
		"snapshot content must be stable while the reader holds its slot")

	// --- Claim (3): backfill-before-truncate makes the fallthrough content safe. ---
	// The checkpoint above ran its PASSIVE backfill, which copies (and fdatasyncs)
	// each frame to the DB file BEFORE advancing nBackfill — and a truncate can only
	// follow a completed backfill. So once a frame's content is eligible to be
	// truncated, that content is already durably in the DB file. We prove the
	// ordering directly: after the backfill, the raw DB-file copy of targetPgno now
	// equals the WAL-frame content. Hence in the only window the fallthrough could be
	// reached (frame still live but unreadable due to a real I/O fault), the disk
	// copy it falls back to is at least as new as the frame — never an older version.
	rawDisk := make([]byte, pager.pageSize)
	require.NoError(t, pager.readDBPage(targetPgno, rawDisk),
		"backfill must have written the page to the DB file")
	assert.Equal(t, walServed, rawDisk,
		"backfill-before-truncate: DB-file copy must match the WAL frame once backfilled")
}

// TestWALReadFrameFaultHookDefaultsToNoop guards the production contract: the
// fault-injection hook is nil unless a test installs it, so production readFrame
// behavior is unchanged (the by-design fallthrough is never triggered in prod by
// the hook).
func TestWALReadFrameFaultHookDefaultsToNoop(t *testing.T) {
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	require.Nil(t, walReadFrameFaultHook.Load(),
		"readFrame fault hook must be nil by default (production no-op)")

	// Installing then clearing must round-trip cleanly back to nil.
	setWalReadFrameFaultHook(func(uint32) error { return errors.New("x") })
	require.NotNil(t, walReadFrameFaultHook.Load())
	setWalReadFrameFaultHook(nil)
	require.Nil(t, walReadFrameFaultHook.Load())
}

// TestWALVersionMismatchRefusesOpenWithoutTruncate guards Bug 10 (pre-beta
// catalog): a checksum-valid WAL header whose version differs was previously
// collapsed into the corrupt-header path, which truncates the WAL — an older
// binary opening a newer database would silently destroy committed
// transactions. SQLite returns SQLITE_CANTOPEN here (walIndexRecover,
// wal.c:1441-1446); we must refuse to open and leave the WAL bytes intact.
func TestWALVersionMismatchRefusesOpenWithoutTruncate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db := openDBNoCleanup(t, path)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, intKey(1), intVal(1)))
	require.NoError(t, tx.Commit())
	rawClose(db) // crash-style close: committed frames stay in the WAL

	// Force recovery to read the WAL file header rather than adopt the shm.
	require.NoError(t, os.Remove(path+"-wal-shm"))

	// Stamp a newer version into the otherwise-valid header (serialize
	// recomputes the header checksum, so it stays checksum-valid).
	walPath := path + "-wal"
	f, err := os.OpenFile(walPath, os.O_RDWR, 0)
	require.NoError(t, err)
	hdrBuf := make([]byte, walHeaderSize)
	_, err = f.ReadAt(hdrBuf, 0)
	require.NoError(t, err)
	var hdr walHeader
	require.NoError(t, hdr.deserialize(hdrBuf))
	hdr.version = walVersion + 1
	hdr.serialize(hdrBuf)
	_, err = f.WriteAt(hdrBuf, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	before, err := os.Stat(walPath)
	require.NoError(t, err)

	_, err = testOpen(t, path, DefaultOptions())
	require.Error(t, err)
	require.ErrorIs(t, err, ErrWALVersion)

	after, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Equal(t, before.Size(), after.Size(),
		"refusing a newer-version WAL must not truncate it")

	// Restoring the version restores access to the committed data — nothing
	// was destroyed by the refused open.
	f, err = os.OpenFile(walPath, os.O_RDWR, 0)
	require.NoError(t, err)
	hdr.version = walVersion
	hdr.serialize(hdrBuf)
	_, err = f.WriteAt(hdrBuf, 0)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	db2 := openDBNoCleanup(t, path)
	assert.Equal(t, 1, countKeys(t, db2, "t1"))
	require.NoError(t, db2.Close())
}

// TestOpenPage1WALReadErrorFailsOpen guards Bug 5 (pre-beta catalog):
// pager.open's post-recovery page-1 header refresh used to swallow a
// readFrame error and continue with the stale DB-file header — the next
// commit would then serialize stale freelist pointers back into page 1,
// double-allocating pages. A read failure at open must fail the open (as in
// SQLite, where lockBtree's page-1 read propagates its error).
func TestOpenPage1WALReadErrorFailsOpen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	db := openDBNoCleanup(t, path)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, intKey(1), intVal(1)))
	require.NoError(t, tx.Commit())
	rawClose(db) // committed frames (incl. page 1) remain in the WAL

	// Fail only the FIRST readFrame after recovery: recovery itself reads the
	// WAL via raw ReadAt, so the first readFrame is pager.open's page-1
	// refresh. Subsequent reads succeed, isolating the injected fault.
	injected := errors.New("injected transient page-1 read error")
	var fired bool
	setWalReadFrameFaultHook(func(uint32) error {
		if !fired {
			fired = true
			return injected
		}
		return nil
	})
	t.Cleanup(func() { setWalReadFrameFaultHook(nil) })

	_, err = testOpen(t, path, DefaultOptions())
	require.Error(t, err, "open must fail when the page-1 WAL frame cannot be read")
	require.ErrorIs(t, err, injected)
	require.True(t, fired, "fault hook must have been exercised")

	// The error was transient: with the fault cleared the same files open
	// fine and the committed data is intact.
	setWalReadFrameFaultHook(nil)
	db2 := openDBNoCleanup(t, path)
	assert.Equal(t, 1, countKeys(t, db2, "t1"))
	require.NoError(t, db2.Close())
}
