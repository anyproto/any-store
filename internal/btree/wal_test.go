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
	require.NoError(t, w.readFrame(1, buf))
	assert.Equal(t, pg1.data, buf)

	// Read frame 2
	require.NoError(t, w.readFrame(2, buf))
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
	assert.ErrorIs(t, w.readFrame(0, buf), ErrWALCorrupt)
	assert.ErrorIs(t, w.readFrame(999, buf), ErrWALCorrupt)

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
	require.NoError(t, w2.readFrame(1, buf))
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
	frame := w2.index.get(2, w2.nFrame.Load())
	assert.Equal(t, uint32(0), frame)

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

	idx.set(1, 1)
	idx.set(2, 2)
	idx.set(1, 3) // Update page 1 to frame 3

	// Get with full visibility
	assert.Equal(t, uint32(3), idx.get(1, 10))
	assert.Equal(t, uint32(2), idx.get(2, 10))

	// Snapshot isolation: max frame limits visibility
	assert.Equal(t, uint32(0), idx.get(1, 0))
	assert.Equal(t, uint32(2), idx.get(2, 5))

	// Page 1 has frames [1, 3]. With maxFrame=2, we see frame 1 (latest <= 2).
	// With maxFrame >= 3, we see frame 3 (the newest version).
	assert.Equal(t, uint32(1), idx.get(1, 2)) // frame 1 <= maxFrame 2
	assert.Equal(t, uint32(3), idx.get(1, 3))

	// Non-existent page
	assert.Equal(t, uint32(0), idx.get(999, 100))
}

func TestWALIndexReset(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")
	idx, err := newWalIndex(shmPath, false)
	require.NoError(t, err)
	defer idx.close(false)

	idx.set(1, 1)
	idx.set(2, 2)
	assert.Equal(t, uint32(2), idx.maxFrame.Load())

	idx.reset()
	assert.Equal(t, uint32(0), idx.maxFrame.Load())
	assert.Equal(t, uint32(0), idx.get(1, 100))
	assert.Equal(t, uint32(0), idx.get(2, 100))
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
	frame := w.index.get(1, w.nFrame.Load())
	assert.Equal(t, uint32(2), frame)

	// Read latest frame
	buf := make([]byte, 4096)
	require.NoError(t, w.readFrame(2, buf))
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
	require.NoError(t, w2.readFrame(1, buf))
	assert.Equal(t, pg1.data, buf)

	require.NoError(t, w2.readFrame(2, buf))
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
	require.NoError(t, wReader.readFrame(1, buf), "reader should read peer frame directly from file even with stale local nFrame")
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
	wi.shmHashWrite(5, 1)
	wi.maxFrame.Store(1)
	wi.mxCommitFrame.Store(1) // getLatest() uses mxCommitFrame for SHM fallback bound
	wi.nBackfill.Store(0)

	// get() should find frame 1 for page 5 via SHM fallback
	frame := wi.get(5, 1)
	assert.Equal(t, uint32(1), frame, "get() SHM fallback: expected frame 1")

	// getLatest() should also find it
	frame = wi.getLatest(5)
	assert.Equal(t, uint32(1), frame, "getLatest() SHM fallback: expected frame 1")

	// With inProcess=true, should NOT fall back to SHM
	wi.inProcess = true
	frame = wi.get(5, 1)
	assert.Equal(t, uint32(0), frame, "get() inProcess should not use SHM fallback")

	frame = wi.getLatest(5)
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
	assert.Equal(t, uint32(2), w.index.get(2, 3))
	assert.Equal(t, uint32(3), w.index.get(3, 3))

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
	wi.shmHashWrite(1, 1)
	wi.maxFrame.Store(1)
	wi.mxCommitFrame.Store(1)

	// Simulate spill: page 2 at frame 2, written to SHM hash but not committed.
	// In real code, spill does NOT write SHM hash (deferred), but here we test
	// that getLatest bounds the SHM search to mxCommitFrame even if SHM has the entry.
	wi.shmHashWrite(2, 2)
	wi.maxFrame.Store(2)
	// mxCommitFrame stays at 1

	// getLatest for page 2: pageMap is empty (cross-process scenario),
	// SHM fallback uses mxCommitFrame=1, so frame 2 is invisible.
	frame := wi.getLatest(2)
	assert.Equal(t, uint32(0), frame, "getLatest should not see spilled frame beyond mxCommitFrame")

	// getLatest for page 1: should still find frame 1 (within mxCommitFrame)
	frame = wi.getLatest(1)
	assert.Equal(t, uint32(1), frame, "getLatest should find committed frame")

	// After commit, advancing mxCommitFrame makes frame 2 visible
	wi.mxCommitFrame.Store(2)
	frame = wi.getLatest(2)
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
	assert.Equal(t, uint32(3), w.index.get(3, 4))
	assert.Equal(t, uint32(4), w.index.get(4, 4))

	// Rollback to savedFrame (discard spilled frames)
	w.index.rollbackToFrame(savedFrame)

	// maxFrame should be restored
	assert.Equal(t, uint32(2), w.index.maxFrame.Load())

	// Spilled pages should be gone from pageMap
	assert.Equal(t, uint32(0), w.index.get(3, 10))
	assert.Equal(t, uint32(0), w.index.get(4, 10))

	// Committed pages should still be visible
	assert.Equal(t, uint32(1), w.index.get(1, 10))
	assert.Equal(t, uint32(2), w.index.get(2, 10))

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
	assert.Equal(t, uint32(0), w.index.get(3, 10))
	assert.Equal(t, uint32(0), w.index.get(4, 10))

	// Pages from before savepoint should still be visible
	assert.Equal(t, uint32(1), w.index.get(1, 10))
	assert.Equal(t, uint32(2), w.index.get(2, 10))

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
	assert.Equal(t, uint32(1), w.index.shmHashGet(1, 10, 1), "committed page 1 should be in SHM hash")
	assert.Equal(t, uint32(2), w.index.shmHashGet(2, 10, 1), "committed page 2 should be in SHM hash")

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
	assert.Equal(t, uint32(1), w.index.shmHashGet(1, 10, 1), "committed page 1 still in SHM hash")
	assert.Equal(t, uint32(2), w.index.shmHashGet(2, 10, 1), "committed page 2 still in SHM hash")

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
	assert.Equal(t, uint32(1), reader.getLatest(1), "cross-process reader sees committed page 1")
	assert.Equal(t, uint32(2), reader.getLatest(2), "cross-process reader sees committed page 2")
	// Reader should NOT see spilled pages (frame > mxCommitFrame)
	assert.Equal(t, uint32(0), reader.getLatest(3), "cross-process reader must not see spilled page 3")
	assert.Equal(t, uint32(0), reader.getLatest(4), "cross-process reader must not see spilled page 4")

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
	assert.Equal(t, uint32(1), w2.index.get(1, 3), "recovery: page 1 at frame 1")
	assert.Equal(t, uint32(2), w2.index.get(2, 3), "recovery: page 2 at frame 2")
	assert.Equal(t, uint32(3), w2.index.get(3, 3), "recovery: page 3 at frame 3")

	// Spilled pages should NOT be in index
	assert.Equal(t, uint32(0), w2.index.get(4, 10), "recovery: spilled page 4 not visible")
	assert.Equal(t, uint32(0), w2.index.get(5, 10), "recovery: spilled page 5 not visible")

	// Page 1 should be at frame 1 (committed version), NOT frame 6 (spilled update)
	assert.Equal(t, uint32(1), w2.index.get(1, 10), "recovery: page 1 should be at committed frame, not spilled")

	// Verify data integrity of committed frames
	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(1, buf))
	assert.Equal(t, pg1.data, buf, "recovery: page 1 data should be committed version")
	require.NoError(t, w2.readFrame(2, buf))
	assert.Equal(t, pg2.data, buf, "recovery: page 2 data intact")
	require.NoError(t, w2.readFrame(3, buf))
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
	assert.Equal(t, uint32(1), w.index.shmHashGet(1, 10, 1))
	assert.Equal(t, uint32(2), w.index.shmHashGet(2, 10, 1))

	// Now commit page 3
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "committed page 3")
	require.NoError(t, w.writeFrames([]*page{pg3}, true, 3))

	// All frames visible in SHM hash
	frame := w.index.shmHashGet(1, 10, 1)
	assert.Equal(t, uint32(1), frame, "page 1 in SHM hash")
	frame = w.index.shmHashGet(2, 10, 1)
	assert.Equal(t, uint32(2), frame, "page 2 in SHM hash")
	frame = w.index.shmHashGet(3, 10, 1)
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
	assert.Equal(t, uint32(1), w.index.shmHashGet(1, 10, 1),
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
	assert.Equal(t, uint32(2), w.index.shmHashGet(2, 10, 1),
		"spilled frame 2 should be present in SHM hash (eager)")
	assert.Equal(t, uint32(3), w.index.shmHashGet(3, 10, 1),
		"spilled frame 3 should be present in SHM hash (eager)")

	// Rollback to the commit boundary — this must invoke shmCleanupFromFrame.
	w.index.rollbackToFrame(committedFrame)

	// Post-rollback: shmHashGet with maxFrame=10 (large enough to see spilled
	// if they remained) must now return 0 for pages 2 and 3. A reader
	// unaware of mxCommitFrame (e.g. a fresh peer process scanning SHM
	// after the writer died) must NOT find dangling entries for rolled-back
	// frames.
	assert.Equal(t, uint32(0), w.index.shmHashGet(2, 10, 1),
		"frame 2 hash entry must be zeroed by rollback cleanup")
	assert.Equal(t, uint32(0), w.index.shmHashGet(3, 10, 1),
		"frame 3 hash entry must be zeroed by rollback cleanup")

	// Committed frame must still be reachable — cleanup preserves the
	// probe chain for idx <= iLimit (wal.c:1258-1264).
	assert.Equal(t, uint32(1), w.index.shmHashGet(1, 10, 1),
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
	wi.shmHashWrite(100, committed-1)
	wi.shmHashWrite(200, committed)
	wi.maxFrame.Store(committed)

	// Spill two frames into segment 1.
	spill1 := committed + 1
	spill2 := committed + 2
	wi.shmHashWrite(300, spill1)
	wi.shmHashWrite(400, spill2)
	wi.maxFrame.Store(spill2)

	// Pre-rollback: segment-1 entries are present.
	assert.NotZero(t, wi.shmHashGet(300, spill2, 1), "spill frame 300 in SHM")
	assert.NotZero(t, wi.shmHashGet(400, spill2, 1), "spill frame 400 in SHM")

	wi.rollbackToFrame(committed)

	// Post-rollback: segment-1 hash entries zeroed.
	assert.Equal(t, uint32(0), wi.shmHashGet(300, spill2, 1),
		"trailing-segment frame 300 hash must be zeroed")
	assert.Equal(t, uint32(0), wi.shmHashGet(400, spill2, 1),
		"trailing-segment frame 400 hash must be zeroed")

	// Pre-boundary committed frames in segment 0 must remain.
	assert.Equal(t, committed-1, wi.shmHashGet(100, committed, 1),
		"segment-0 frame 100 must still be reachable")
	assert.Equal(t, committed, wi.shmHashGet(200, committed, 1),
		"segment-0 frame 200 must still be reachable")
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
// walIndexRecover rejection at wal.c:1406-1410.
func TestWalHeaderDeserialize_RejectsBadVersion(t *testing.T) {
	buf := buildWalHeader(t, walVersion+1, DefaultPageSize)
	var h walHeader
	err := h.deserialize(buf)
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("bad version should be ErrWALCorrupt, got %v", err)
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
	frame := w2.index.get(3, w2.index.maxFrame.Load())
	require.NotZero(t, frame, "pgno 3 should have a frame after recovery")
	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(frame, buf))
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
