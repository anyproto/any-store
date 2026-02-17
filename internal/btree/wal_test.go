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
	assert.Equal(t, uint32(2), w.nFrame)
	assert.Equal(t, uint32(2), w.index.maxFrame)

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
	assert.Equal(t, uint32(1), w.nFrame)
	// walIndex.maxFrame tracks highest frame set (used internally),
	// but the commit-visible maxFrame is only updated on commit.
	// The key behavior is that recovery will NOT replay uncommitted frames.
	assert.Equal(t, uint32(1), w.index.maxFrame)

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
	assert.Equal(t, uint32(0), w.nFrame)

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

	assert.Equal(t, uint32(1), w2.nFrame)
	assert.Equal(t, uint32(5), w2.index.maxPage)

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

	assert.Equal(t, uint32(1), w2.nFrame)
	assert.Equal(t, uint32(1), w2.index.maxPage)

	// Frame for page 2 should not be in index
	frame := w2.index.get(2, w2.nFrame)
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

	assert.Equal(t, uint32(0), w.nFrame)
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

	// Frame 3 is visible only when maxFrame >= 3
	assert.Equal(t, uint32(0), idx.get(1, 2)) // frame 3 > maxFrame 2
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
	assert.Equal(t, uint32(2), idx.maxFrame)

	idx.reset()
	assert.Equal(t, uint32(0), idx.maxFrame)
	assert.Equal(t, uint32(0), idx.get(1, 100))
	assert.Equal(t, uint32(0), idx.get(2, 100))
}

func TestWALIndexWriteHeader(t *testing.T) {
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")
	idx, err := newWalIndex(shmPath, false)
	require.NoError(t, err)
	defer idx.close()

	require.NoError(t, idx.writeHeader(10, 20, 5))

	// Read back from shm region 0
	region, err := idx.shm.region(0, false)
	require.NoError(t, err)

	assert.Equal(t, uint32(10), binary.LittleEndian.Uint32(region[0:4]))
	assert.Equal(t, uint32(20), binary.LittleEndian.Uint32(region[4:8]))
	assert.Equal(t, uint32(5), binary.LittleEndian.Uint32(region[8:12]))

	// Second copy
	assert.Equal(t, uint32(10), binary.LittleEndian.Uint32(region[48:52]))
	assert.Equal(t, uint32(20), binary.LittleEndian.Uint32(region[52:56]))
	assert.Equal(t, uint32(5), binary.LittleEndian.Uint32(region[56:60]))
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
	require.NoError(t, w.checkpoint(dbFile))

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

	// Checkpoint
	require.NoError(t, w.checkpoint(dbFile))

	// WAL should be reset
	assert.Equal(t, uint32(0), w.nFrame)

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

	assert.Equal(t, uint32(2), w.nFrame)

	// Latest frame for page 1 should be frame 2
	frame := w.index.get(1, w.nFrame)
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

	assert.Equal(t, uint32(2), w2.nFrame)
	assert.Equal(t, uint32(2), w2.index.maxPage)

	buf := make([]byte, 4096)
	require.NoError(t, w2.readFrame(1, buf))
	assert.Equal(t, pg1.data, buf)

	require.NoError(t, w2.readFrame(2, buf))
	assert.Equal(t, pg2.data, buf)

	require.NoError(t, w2.close())
}
