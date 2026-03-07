package btree

import (
	"encoding/binary"
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
	require.NoError(t, w.close())

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
	require.NoError(t, w.beginWrite())

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
	require.NoError(t, w.close())
}

func TestWALReadFrameInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())

	buf := make([]byte, 4096)
	assert.ErrorIs(t, w.readFrame(0, buf), ErrWALCorrupt)
	assert.ErrorIs(t, w.readFrame(999, buf), ErrWALCorrupt)

	require.NoError(t, w.close())
}

func TestWALWriteNoCommit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

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
	require.NoError(t, w.close())
}

func TestWALWriteEmptyFrames(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

	require.NoError(t, w.writeFrames(nil, true, 0))
	assert.Equal(t, uint32(0), w.nFrame.Load())

	w.endWrite()
	require.NoError(t, w.close())
}

func TestWALRecoveryCommitted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Write some committed data
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

	pg := &page{pgno: 5, data: make([]byte, 4096)}
	copy(pg.data, "committed data")
	require.NoError(t, w.writeFrames([]*page{pg}, true, 5))

	w.endWrite()
	require.NoError(t, w.close())

	// Reopen and verify recovery
	w2 := newWal(path, 4096)
	require.NoError(t, w2.open())

	assert.Equal(t, uint32(1), w2.nFrame.Load())
	assert.Equal(t, uint32(5), w2.index.maxPage.Load())

	// Verify we can read the recovered frame
	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(1, buf))
	assert.Equal(t, pg.data, buf)

	require.NoError(t, w2.close())
}

func TestWALRecoveryUncommittedTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Write committed + uncommitted frames
	w := newWal(path, 4096)
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))

	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "uncommitted")
	require.NoError(t, w.writeFrames([]*page{pg2}, false, 2))

	w.endWrite()
	require.NoError(t, w.close())

	// Reopen - recovery should only see committed frame
	w2 := newWal(path, 4096)
	require.NoError(t, w2.open())

	assert.Equal(t, uint32(1), w2.nFrame.Load())
	assert.Equal(t, uint32(1), w2.index.maxPage.Load())

	// Frame for page 2 should not be in index
	frame := w2.index.get(2, w2.nFrame.Load())
	assert.Equal(t, uint32(0), frame)

	require.NoError(t, w2.close())
}

func TestWALRecoveryCorruptHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Write garbage to WAL file
	require.NoError(t, os.WriteFile(path, make([]byte, walHeaderSize+100), 0666))

	w := newWal(path, 4096)
	require.NoError(t, w.open()) // Should handle gracefully by resetting

	assert.Equal(t, uint32(0), w.nFrame.Load())
	require.NoError(t, w.close())
}

func TestWALIndexSetGet(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")
	idx, err := newWalIndex(shmPath, false)
	require.NoError(t, err)
	defer idx.close()

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
	defer idx.close()

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
	defer idx.close()

	idx.nBackfill.Store(5)
	require.NoError(t, idx.writeHeader(10, 20, 5))
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

	require.NoError(t, w.close())
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
	require.NoError(t, w.beginWrite())

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
	require.NoError(t, w.close())
}

func TestWALMultipleCommits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	require.NoError(t, w.open())

	// First commit
	require.NoError(t, w.beginWrite())
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "commit 1")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))
	w.endWrite()

	// Second commit (updates same page)
	require.NoError(t, w.beginWrite())
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

	require.NoError(t, w.close())
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
	require.NoError(t, w.beginWrite())
	pg := &page{pgno: 1, data: make([]byte, 4096)}
	require.NoError(t, w.writeFrames([]*page{pg}, true, 1))
	w.endWrite()

	maxFrame, slot, err = w.beginRead()
	require.NoError(t, err)
	assert.Equal(t, uint32(1), maxFrame)
	w.endRead(slot)

	require.NoError(t, w.close())
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
	require.NoError(t, w.beginWrite())
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "first commit")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))
	w.endWrite()

	require.NoError(t, w.beginWrite())
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "second commit")
	require.NoError(t, w.writeFrames([]*page{pg2}, true, 2))
	w.endWrite()

	require.NoError(t, w.close())

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

	require.NoError(t, w2.close())
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
	require.NoError(t, w.beginWrite())

	// First commit some frames so mxCommitFrame is non-zero
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))
	assert.Equal(t, uint32(1), w.index.maxFrame.Load())
	assert.Equal(t, uint32(1), w.index.mxCommitFrame.Load())

	// Now write frames WITHOUT commit (simulating spill)
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "spilled page 2")
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "spilled page 3")
	require.NoError(t, w.writeFrames([]*page{pg2, pg3}, false, 0))

	// maxFrame should advance to 3 (writer sees all frames)
	assert.Equal(t, uint32(3), w.index.maxFrame.Load())
	// mxCommitFrame should still be 1 (readers only see committed)
	assert.Equal(t, uint32(1), w.index.mxCommitFrame.Load())

	// Writer can find spilled pages via pageMap
	assert.Equal(t, uint32(2), w.index.get(2, 3))
	assert.Equal(t, uint32(3), w.index.get(3, 3))

	// Now commit — mxCommitFrame should catch up
	pg4 := &page{pgno: 4, data: make([]byte, 4096)}
	copy(pg4.data, "committed page 4")
	require.NoError(t, w.writeFrames([]*page{pg4}, true, 4))
	assert.Equal(t, uint32(4), w.index.maxFrame.Load())
	assert.Equal(t, uint32(4), w.index.mxCommitFrame.Load())

	w.endWrite()
	require.NoError(t, w.close())
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

func TestWriteFramesCommitFalseDoesNotWriteShmHash(t *testing.T) {
	// Spill frames (commit=false) should NOT write to SHM hash tables.
	// Cross-process readers use SHM hash to find pages, so deferred writes
	// prevent them from seeing uncommitted spilled frames.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	// Use non-inProcess mode to exercise SHM hash path
	w.inProcess = false
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

	// First commit page 1 so we have a baseline
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "committed page 1")
	require.NoError(t, w.writeFrames([]*page{pg1}, true, 1))

	// Verify committed page 1 is in SHM hash
	frame := w.index.shmHashGet(1, 10)
	assert.Equal(t, uint32(1), frame, "committed page should be in SHM hash")

	// Now spill pages 2 and 3 (commit=false)
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "spilled page 2")
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "spilled page 3")
	require.NoError(t, w.writeFrames([]*page{pg2, pg3}, false, 0))

	// Spilled pages should NOT be in SHM hash
	frame = w.index.shmHashGet(2, 10)
	assert.Equal(t, uint32(0), frame, "spilled page 2 should not be in SHM hash")
	frame = w.index.shmHashGet(3, 10)
	assert.Equal(t, uint32(0), frame, "spilled page 3 should not be in SHM hash")

	// But writer can still find them via pageMap
	assert.Equal(t, uint32(2), w.index.get(2, 3))
	assert.Equal(t, uint32(3), w.index.get(3, 3))

	// Verify pending SHM frames were accumulated
	assert.Len(t, w.index.pendingShmFrames, 2, "should have 2 pending SHM frames")

	w.endWrite()
	require.NoError(t, w.close())
}

func TestRollbackCleansUpSpilledFrames(t *testing.T) {
	// After spilling frames (commit=false), rollbackToFrame should remove
	// pageMap entries for spilled frames and restore maxFrame.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

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
	assert.Equal(t, uint32(2), w.index.mxCommitFrame.Load())

	// Spill pages 3 and 4 (frames 3 and 4, commit=false)
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "spilled page 3")
	pg4 := &page{pgno: 4, data: make([]byte, 4096)}
	copy(pg4.data, "spilled page 4")
	require.NoError(t, w.writeFrames([]*page{pg3, pg4}, false, 0))

	// Verify spilled state: maxFrame advanced but mxCommitFrame did not
	assert.Equal(t, uint32(4), w.index.maxFrame.Load())
	assert.Equal(t, uint32(2), w.index.mxCommitFrame.Load())
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

	// pendingShmFrames should be cleared
	assert.Len(t, w.index.pendingShmFrames, 0)

	w.endWrite()
	require.NoError(t, w.close())
}

func TestRollbackToSavepointWithSpilledFrames(t *testing.T) {
	// Savepoint, spill frames, rollback to savepoint — WAL index should
	// be restored to the savepoint's walFrame position.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = true
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

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
	assert.Equal(t, uint32(3), w.index.mxCommitFrame.Load())

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

	// pendingShmFrames should be cleared
	assert.Len(t, w.index.pendingShmFrames, 0)

	w.endWrite()
	require.NoError(t, w.close())
}

func TestWriteFramesCommitFlushesToShm(t *testing.T) {
	// After spill + commit, all frames (spilled and committed) should be
	// visible in SHM hash tables.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")
	w := newWal(path, 4096)
	w.inProcess = false
	require.NoError(t, w.open())
	require.NoError(t, w.beginWrite())

	// Spill pages 1 and 2 (commit=false)
	pg1 := &page{pgno: 1, data: make([]byte, 4096)}
	copy(pg1.data, "spilled page 1")
	pg2 := &page{pgno: 2, data: make([]byte, 4096)}
	copy(pg2.data, "spilled page 2")
	require.NoError(t, w.writeFrames([]*page{pg1, pg2}, false, 0))

	// Verify NOT in SHM hash yet
	assert.Equal(t, uint32(0), w.index.shmHashGet(1, 10))
	assert.Equal(t, uint32(0), w.index.shmHashGet(2, 10))

	// Now commit page 3
	pg3 := &page{pgno: 3, data: make([]byte, 4096)}
	copy(pg3.data, "committed page 3")
	require.NoError(t, w.writeFrames([]*page{pg3}, true, 3))

	// All frames should now be in SHM hash (pending flushed + commit batch)
	frame := w.index.shmHashGet(1, 10)
	assert.Equal(t, uint32(1), frame, "spilled page 1 should be in SHM hash after commit")
	frame = w.index.shmHashGet(2, 10)
	assert.Equal(t, uint32(2), frame, "spilled page 2 should be in SHM hash after commit")
	frame = w.index.shmHashGet(3, 10)
	assert.Equal(t, uint32(3), frame, "committed page 3 should be in SHM hash after commit")

	// Pending SHM frames should be cleared
	assert.Len(t, w.index.pendingShmFrames, 0, "pending SHM frames should be empty after commit")

	// mxCommitFrame should include all frames
	assert.Equal(t, uint32(3), w.index.mxCommitFrame.Load())

	w.endWrite()
	require.NoError(t, w.close())
}
