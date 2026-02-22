package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShmHashTableBasic(t *testing.T) {
	wi, err := newWalIndex("", true) // in-process shm
	require.NoError(t, err)
	defer wi.close()

	// Write some page→frame mappings
	wi.shmHashWrite(10, 1)
	wi.shmHashWrite(20, 2)
	wi.shmHashWrite(30, 3)

	// Look them up via shm
	assert.Equal(t, uint32(1), wi.shmHashGet(10, 10))
	assert.Equal(t, uint32(2), wi.shmHashGet(20, 10))
	assert.Equal(t, uint32(3), wi.shmHashGet(30, 10))

	// Non-existent page
	assert.Equal(t, uint32(0), wi.shmHashGet(99, 10))

	// maxFrame limit: frame 3 is invisible when maxFrame=2
	assert.Equal(t, uint32(0), wi.shmHashGet(30, 2))
	assert.Equal(t, uint32(2), wi.shmHashGet(20, 2))
}

func TestShmHashTableOverwrite(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close()

	// Page 10 written at frame 1, then overwritten at frame 5
	wi.shmHashWrite(10, 1)
	wi.shmHashWrite(10, 5)

	// Should find the latest frame
	assert.Equal(t, uint32(5), wi.shmHashGet(10, 10))

	// With maxFrame=3, should find frame 1
	assert.Equal(t, uint32(1), wi.shmHashGet(10, 3))
}

func TestShmHashTableCollision(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close()

	// Insert many pages to force hash collisions
	for i := uint32(1); i <= 1000; i++ {
		wi.shmHashWrite(i*100, i)
	}

	// All should be findable
	for i := uint32(1); i <= 1000; i++ {
		got := wi.shmHashGet(i*100, 1000)
		assert.Equal(t, i, got, "page %d", i*100)
	}
}

func TestShmHashTableMultiSegment(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close()

	// Write enough frames to span multiple segments.
	// Segment 0 holds frames 1..4062, segment 1 holds 4063..8158.
	totalFrames := uint32(5000)
	for f := uint32(1); f <= totalFrames; f++ {
		pgno := f + 100 // arbitrary page numbers
		wi.shmHashWrite(pgno, f)
	}

	// Verify all lookups work
	for f := uint32(1); f <= totalFrames; f++ {
		pgno := f + 100
		got := wi.shmHashGet(pgno, totalFrames)
		assert.Equal(t, f, got, "frame for page %d", pgno)
	}

	// Verify maxFrame boundary at segment edge
	assert.Equal(t, uint32(4062), wi.shmHashGet(4062+100, 4062))
	assert.Equal(t, uint32(4063), wi.shmHashGet(4063+100, totalFrames))
	assert.Equal(t, uint32(0), wi.shmHashGet(4063+100, 4062)) // beyond maxFrame
}

func TestShmHashTableClear(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close()

	// Populate
	for f := uint32(1); f <= 100; f++ {
		wi.shmHashWrite(f, f)
	}
	assert.Equal(t, uint32(50), wi.shmHashGet(50, 100))

	// Clear
	wi.shmClearHash()

	// Should not find anything
	assert.Equal(t, uint32(0), wi.shmHashGet(50, 100))
}

func TestShmCkptInfo(t *testing.T) {
	wi, err := newWalIndex("", true)
	require.NoError(t, err)
	defer wi.close()

	// Set some values
	wi.nBackfill = 42
	wi.aReadMark = [5]uint32{0, 100, 200, readMarkNotUsed, readMarkNotUsed}
	wi.shmWriteCkptInfo()

	// Reset in-memory values
	wi.nBackfill = 0
	wi.aReadMark = [5]uint32{}

	// Read back from shm
	wi.shmReadCkptInfo()
	assert.Equal(t, uint32(42), wi.nBackfill)
	assert.Equal(t, uint32(100), wi.aReadMark[1])
	assert.Equal(t, uint32(200), wi.aReadMark[2])
	assert.Equal(t, readMarkNotUsed, wi.aReadMark[3])
}

func TestShmHashIntegrationWithSetBatch(t *testing.T) {
	// Verify that set/setBatch write to shm hash tables
	db, ns := tempDBWithNS(t, "data")

	// Insert data which calls setBatch internally
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Verify we can look up pages via the shm hash tables
	wi := db.pager.wal.index
	wi.mu.RLock()
	maxFrame := wi.maxFrame
	wi.mu.RUnlock()

	// The shm hash table returns the latest frame for a page (within maxFrame),
	// which should match the last entry in the Go map's frame list.
	wi.mu.RLock()
	for pgno, frames := range wi.pageMap {
		if len(frames) == 0 {
			continue
		}
		latestFrame := frames[len(frames)-1]
		shmFrame := wi.shmHashGet(pgno, maxFrame)
		assert.Equal(t, latestFrame, shmFrame, "page %d: map latest=%d shm=%d", pgno, latestFrame, shmFrame)
	}
	wi.mu.RUnlock()
	_ = ns
}

func TestShmHashAfterCheckpoint(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Checkpoint should clear hash tables on WAL reset
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// After checkpoint + WAL reset, hash tables should be cleared
	wi := db.pager.wal.index
	wi.mu.RLock()
	maxFrame := wi.maxFrame
	wi.mu.RUnlock()

	// If WAL was reset, maxFrame should be 0 and lookups return 0
	if maxFrame == 0 {
		assert.Equal(t, uint32(0), wi.shmHashGet(1, 100))
	}

	// Data should still be readable from DB
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	v, err := rtx.Get(ns3, []byte("key-0010"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0010"), v)
	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestHtFrameSegIdx(t *testing.T) {
	// Frame 1 → segment 0, index 0
	seg, idx := htFrameSegIdx(1)
	assert.Equal(t, 0, seg)
	assert.Equal(t, 0, idx)

	// Frame 4062 (htNPageOne) → segment 0, last index
	seg, idx = htFrameSegIdx(htNPageOne)
	assert.Equal(t, 0, seg)
	assert.Equal(t, int(htNPageOne)-1, idx)

	// Frame 4063 → segment 1, index 0
	seg, idx = htFrameSegIdx(htNPageOne + 1)
	assert.Equal(t, 1, seg)
	assert.Equal(t, 0, idx)

	// Frame 8158 (4062 + 4096) → segment 1, last index
	seg, idx = htFrameSegIdx(htNPageOne + htNPage)
	assert.Equal(t, 1, seg)
	assert.Equal(t, htNPage-1, idx)

	// Frame 8159 → segment 2, index 0
	seg, idx = htFrameSegIdx(htNPageOne + htNPage + 1)
	assert.Equal(t, 2, seg)
	assert.Equal(t, 0, idx)
}
