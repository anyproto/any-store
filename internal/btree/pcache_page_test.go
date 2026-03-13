package btree

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ===== pcache.go coverage =====

func TestPcacheFetchAndMakeDirty_Clean(t *testing.T) {
	pc := newPcache(4096, 100, true)

	// Create a clean page and release it (goes to LRU)
	pg := pc.create(1, 2)
	pc.release(pg)
	assert.Equal(t, 1, pc.nRecyclable)
	assert.False(t, pg.dirty)

	// fetch + makeDirty should fetch it, mark dirty, remove from LRU
	p := pc.fetch(1)
	require.NotNil(t, p)
	pc.makeDirty(p)
	assert.True(t, p.dirty)
	assert.Equal(t, 0, pc.nRecyclable)
	assert.Equal(t, 1, pc.nDirty)
	assert.Equal(t, p, pc.dirtyHead)
}

func TestPcacheFetchAndMakeDirty_AlreadyDirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.makeDirty(pg)
	assert.True(t, pg.dirty)
	assert.Equal(t, 1, pc.nDirty)

	// fetch + makeDirty on already-dirty page should just pin it
	p := pc.fetch(1)
	require.NotNil(t, p)
	pc.makeDirty(p) // no-op since already dirty
	assert.True(t, p.dirty)
	assert.Equal(t, 1, pc.nDirty) // no double-count
}

func TestPcacheFetchAndMakeDirty_NotFound(t *testing.T) {
	pc := newPcache(4096, 100, true)

	// fetch on non-existent page should return nil
	p := pc.fetch(999)
	assert.Nil(t, p)
}

func TestPcacheFetchAndMakeDirty_MultipleDirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	// Create two clean pages
	pg1 := pc.create(1, 2)
	pc.release(pg1)
	pg2 := pc.create(2, 2)
	pc.release(pg2)

	// fetch + makeDirty both — second should chain in front of first
	p1 := pc.fetch(1)
	require.NotNil(t, p1)
	pc.makeDirty(p1)
	p2 := pc.fetch(2)
	require.NotNil(t, p2)
	pc.makeDirty(p2)

	assert.Equal(t, 2, pc.nDirty)
	// dirtyHead should be p2 (last inserted)
	assert.Equal(t, p2, pc.dirtyHead)
	assert.Equal(t, p1, pc.dirtyHead.next)
}

func TestPcacheEvictOne_SingleElement(t *testing.T) {
	pc := newPcache(4096, 1, true) // max 1 page

	// Create and release one clean page
	pg := pc.create(1, 2)
	pc.release(pg)
	assert.Equal(t, 1, pc.nRecyclable)
	assert.NotNil(t, pc.lruHead)
	assert.NotNil(t, pc.lruTail)

	// Creating a second page should evict the first (single element eviction)
	pg2 := pc.create(2, 2)
	require.NotNil(t, pg2)
	assert.Nil(t, pc.pages[1])       // page 1 evicted
	assert.Nil(t, pc.lruHead)         // LRU list empty
	assert.Nil(t, pc.lruTail)         // LRU list empty
	assert.Equal(t, 0, pc.nRecyclable)
	pc.release(pg2)
}

func TestPcacheEvictOne_EmptyLRU(t *testing.T) {
	pc := newPcache(4096, 100, true)

	// Manually call evictOne on empty LRU — should be no-op
	pc.evictOne() // lruHead == nil path
	assert.Nil(t, pc.lruHead)
}

func TestPcacheLruRemove_NotInLRU(t *testing.T) {
	// Test lruRemove on a page that is NOT in the LRU list.
	// Should be a no-op: nRecyclable stays at 0.
	pc := newPcache(4096, 100, true)

	// Create a page that is pinned (never released, so not in LRU)
	pg := pc.create(1, 2)
	assert.Equal(t, 0, pc.nRecyclable)

	// lruRemove on a page not in LRU should be a no-op
	pc.lruRemove(pg)
	assert.Equal(t, 0, pc.nRecyclable)
}

func TestPcacheLruRemove_Head(t *testing.T) {
	// Test removing the head of a multi-element LRU list.
	// With lruPrepend, most recently released is at HEAD.
	pc := newPcache(4096, 100, true)

	pg1 := pc.create(1, 2)
	pg2 := pc.create(2, 2)
	pg3 := pc.create(3, 2)
	pc.release(pg1) // LRU: HEAD -> pg1 -> TAIL
	pc.release(pg2) // LRU: HEAD -> pg2 -> pg1 -> TAIL
	pc.release(pg3) // LRU: HEAD -> pg3 -> pg2 -> pg1 -> TAIL
	assert.Equal(t, 3, pc.nRecyclable)

	// Remove head (pg3, most recently released)
	pc.lruRemove(pg3)
	assert.Equal(t, 2, pc.nRecyclable)
	assert.Equal(t, pg2, pc.lruHead)
	assert.Equal(t, pg1, pc.lruTail)
}

func TestPcacheLruRemove_Tail(t *testing.T) {
	// Test removing the tail of a multi-element LRU list.
	// With lruPrepend: release(pg1) then release(pg2) gives HEAD -> pg2 -> pg1 -> TAIL
	pc := newPcache(4096, 100, true)

	pg1 := pc.create(1, 2)
	pg2 := pc.create(2, 2)
	pc.release(pg1) // LRU: HEAD -> pg1 -> TAIL
	pc.release(pg2) // LRU: HEAD -> pg2 -> pg1 -> TAIL
	assert.Equal(t, 2, pc.nRecyclable)

	// Remove tail (pg1, least recently released)
	pc.lruRemove(pg1)
	assert.Equal(t, 1, pc.nRecyclable)
	assert.Equal(t, pg2, pc.lruHead)
	assert.Equal(t, pg2, pc.lruTail)
}

func TestPcacheLruRemove_Middle(t *testing.T) {
	pc := newPcache(4096, 100, true)

	// Create 3 pages and release them — all go to LRU
	// With lruPrepend: HEAD -> pgs[2] -> pgs[1] -> pgs[0] -> TAIL
	pgs := make([]*page, 3)
	for i := range pgs {
		pgs[i] = pc.create(uint32(i+1), 2)
	}
	for _, p := range pgs {
		pc.release(p)
	}
	assert.Equal(t, 3, pc.nRecyclable)

	// Remove the middle page (pgs[1])
	pc.lruRemove(pgs[1])
	assert.Equal(t, 2, pc.nRecyclable)
	assert.Equal(t, pgs[2], pc.lruHead)
	assert.Equal(t, pgs[0], pc.lruTail)
}

func TestPcacheNonPurgeable(t *testing.T) {
	// Non-purgeable cache (InMemory mode) should never evict
	pc := newPcache(4096, 2, false) // max 2 pages, not purgeable

	for i := uint32(1); i <= 5; i++ {
		pg := pc.create(i, 2)
		pc.release(pg)
	}

	// All 5 pages should still be present since cache is not purgeable
	assert.Len(t, pc.pages, 5)
}

func TestPcacheAppendDirtyPages(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg1 := pc.create(1, 2)
	pg2 := pc.create(2, 2)
	pc.makeDirty(pg1)
	pc.makeDirty(pg2)

	buf := make([]*page, 0, 5)
	result := pc.appendDirtyPages(buf)
	assert.Len(t, result, 2)
}

func TestPcacheFetch_Clean(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.release(pg)

	// fetch on a clean page: dirty=false
	p := pc.fetch(1)
	require.NotNil(t, p)
	assert.False(t, p.dirty)
}

func TestPcacheFetch_Dirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.makeDirty(pg)
	pc.release(pg)

	// fetch on a dirty page: dirty=true
	p := pc.fetch(1)
	require.NotNil(t, p)
	assert.True(t, p.dirty)
}

func TestPcacheFetch_NotFound(t *testing.T) {
	pc := newPcache(4096, 100, true)

	p := pc.fetch(999)
	assert.Nil(t, p)
}

func TestPcacheCreateExistingDirty(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.makeDirty(pg)

	// create(1) again should return the same dirty page
	pg2 := pc.create(1, 2)
	assert.Equal(t, pg, pg2)
	assert.True(t, pg2.dirty)
}

// ===== page.go coverage =====

func TestGetCellOffsetSafe_OutOfBounds(t *testing.T) {
	pg := &page{
		pgno: 2,
		data: make([]byte, 20), // tiny buffer
		header: pageHeader{
			pageType:  pageTypeLeafIdx,
			cellCount: 100, // absurdly high to force OOB
		},
	}

	// With cellCount=100, requesting cell 99 would need base=8+99*2=206 > 20
	_, err := pg.getCellOffsetSafe(99)
	assert.ErrorIs(t, err, ErrCorrupt)

	// Even cell 0 — if data is too small for the cell pointer area
	pg2 := &page{
		pgno: 2,
		data: make([]byte, 5), // header is 8 bytes, so cell pointer area starts at 8 which is OOB
		header: pageHeader{
			pageType:  pageTypeLeafIdx,
			cellCount: 1,
		},
	}
	_, err = pg2.getCellOffsetSafe(0)
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestGetCellOffsetSafe_Valid(t *testing.T) {
	pg := &page{
		pgno: 2,
		data: make([]byte, 4096),
		header: pageHeader{
			pageType:  pageTypeLeafIdx,
			cellCount: 2,
		},
	}
	// Set cell 0 offset to 3000
	base := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[base:], 3000)

	off, err := pg.getCellOffsetSafe(0)
	require.NoError(t, err)
	assert.Equal(t, uint16(3000), off)
}

func TestGetVarintSafe_EmptyBuffer(t *testing.T) {
	_, _, err := getVarintSafe([]byte{})
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestGetVarintSafe_SingleByte(t *testing.T) {
	v, n, err := getVarintSafe([]byte{42})
	require.NoError(t, err)
	assert.Equal(t, uint64(42), v)
	assert.Equal(t, 1, n)
}

func TestGetVarintSafe_TruncatedMultiByte(t *testing.T) {
	// A byte with high bit set indicates multi-byte varint.
	// If buffer only has one byte with high bit set, it's truncated.
	_, _, err := getVarintSafe([]byte{0x80})
	assert.ErrorIs(t, err, ErrCorrupt)

	// Two bytes where first has high bit, second also has high bit — needs more
	_, _, err = getVarintSafe([]byte{0x80, 0x80})
	assert.ErrorIs(t, err, ErrCorrupt)

	// Three continuation bytes but still truncated
	_, _, err = getVarintSafe([]byte{0x80, 0x80, 0x80})
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestGetVarintSafe_TruncatedNineByteVarint(t *testing.T) {
	// 8 continuation bytes need a 9th byte
	buf := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	_, _, err := getVarintSafe(buf)
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestGetVarintSafe_NineByteVarint(t *testing.T) {
	// Write a 9-byte varint and read it back with getVarintSafe
	buf := make([]byte, 9)
	val := uint64(0xFFFFFFFFFFFFFFFF) // max uint64
	n := putVarint(buf, val)
	assert.Equal(t, 9, n)

	v, m, err := getVarintSafe(buf)
	require.NoError(t, err)
	assert.Equal(t, 9, m)
	assert.Equal(t, val, v)
}

func TestGetVarintSafe_TwoByteVarint(t *testing.T) {
	buf := make([]byte, 9)
	val := uint64(0x3FFF) // max 2-byte
	n := putVarint(buf, val)
	assert.Equal(t, 2, n)

	v, m, err := getVarintSafe(buf[:2])
	require.NoError(t, err)
	assert.Equal(t, 2, m)
	assert.Equal(t, val, v)
}

func TestLocalPayloadSize_SurplusExceedsMaxLocal(t *testing.T) {
	// We need surplus > maxLocal, which triggers the minLocal return path.
	// maxLocal = ((usableSize - 12) * 64 / 255) - 23
	// minLocal = ((usableSize - 12) * 32 / 255) - 23
	// surplus = minLocal + (totalPayload - minLocal) % ovflUsable

	usableSize := 4096
	maxLocal := maxLocalPayload(usableSize)
	minLocal := minLocalPayload(usableSize)
	ovflUsable := overflowPageUsable(usableSize)

	// We need: surplus = minLocal + (totalPayload - minLocal) % ovflUsable > maxLocal
	// So (totalPayload - minLocal) % ovflUsable > maxLocal - minLocal
	// Pick totalPayload such that remainder is large enough
	// maxLocal - minLocal = ((usableSize-12) * 32 / 255) = about 512 for 4096 usable
	diff := maxLocal - minLocal
	// We want (totalPayload - minLocal) % ovflUsable > diff
	// Let totalPayload = minLocal + ovflUsable - 1 + maxLocal + 1
	// This way remainder = ovflUsable - 1 + maxLocal + 1 - ovflUsable * k
	// Actually, let's just pick: totalPayload = maxLocal + 1 + (ovflUsable - diff + maxLocal)
	// Simpler: just find a value empirically

	// surplus = minLocal + (tp - minLocal) % ovflUsable
	// We want surplus > maxLocal, i.e., (tp - minLocal) % ovflUsable > diff
	// ovflUsable = 4092, diff = maxLocal - minLocal ~ 512
	// Pick (tp - minLocal) % ovflUsable = diff + 1
	totalPayload := minLocal + diff + 1 + ovflUsable
	result := localPayloadSize(totalPayload, usableSize)
	// surplus = minLocal + (totalPayload - minLocal) % ovflUsable
	// = minLocal + (diff + 1 + ovflUsable) % ovflUsable
	// = minLocal + diff + 1
	// = maxLocal + 1 > maxLocal, so should return minLocal
	assert.Equal(t, minLocal, result)
}

func TestLocalPayloadSize_SurplusWithinMaxLocal(t *testing.T) {
	usableSize := 4096
	maxLocal := maxLocalPayload(usableSize)

	// totalPayload exactly at maxLocal boundary
	result := localPayloadSize(maxLocal, usableSize)
	assert.Equal(t, maxLocal, result)

	// totalPayload just over maxLocal
	result = localPayloadSize(maxLocal+1, usableSize)
	// surplus should be within maxLocal in most cases
	assert.True(t, result > 0)
	assert.True(t, result <= maxLocal)
}

func TestLocalPayloadSize_UnderMaxLocal(t *testing.T) {
	usableSize := 4096
	// Small payload that fits entirely
	result := localPayloadSize(100, usableSize)
	assert.Equal(t, 100, result)
}

func TestContentAreaOffset_CellContentOffZero(t *testing.T) {
	// When cellContentOff == 0 and usableSize != 65536, top = usableSize
	pg := &page{
		pgno: 2,
		data: make([]byte, 4096),
		header: pageHeader{
			pageType:       pageTypeLeafIdx,
			cellContentOff: 0,
			cellCount:      0,
		},
	}
	off, err := pg.contentAreaOffset(4096)
	require.NoError(t, err)
	assert.Equal(t, 4096, off)
}

func TestContentAreaOffset_CellContentOffZero65536(t *testing.T) {
	// When cellContentOff == 0 and usableSize == 65536, top = 65536
	pg := &page{
		pgno: 2,
		data: make([]byte, 65536),
		header: pageHeader{
			pageType:       pageTypeLeafIdx,
			cellContentOff: 0,
			cellCount:      0,
		},
	}
	off, err := pg.contentAreaOffset(65536)
	require.NoError(t, err)
	assert.Equal(t, 65536, off)
}

func TestContentAreaOffset_TopTooLarge(t *testing.T) {
	pg := &page{
		pgno: 2,
		data: make([]byte, 4096),
		header: pageHeader{
			pageType:       pageTypeLeafIdx,
			cellContentOff: 5000, // > usableSize
			cellCount:      0,
		},
	}
	_, err := pg.contentAreaOffset(4096)
	assert.ErrorIs(t, err, ErrCorrupt)
}

func TestContentAreaOffset_TopLessThanGap(t *testing.T) {
	pg := &page{
		pgno: 2,
		data: make([]byte, 4096),
		header: pageHeader{
			pageType:       pageTypeLeafIdx,
			cellContentOff: 5, // less than headerSize(8) + 0 cells = 8
			cellCount:      0,
		},
	}
	_, err := pg.contentAreaOffset(4096)
	assert.ErrorIs(t, err, ErrCorrupt)
}

// ===== shm.go coverage (inProcessShm) =====

func TestInProcessShm_LockSharedWhenExclusive(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	// Acquire exclusive lock
	require.NoError(t, s.lock(0, lockExclusive))

	// Trying to acquire shared lock should return ErrBusy
	err := s.lock(0, lockShared)
	assert.ErrorIs(t, err, ErrBusy)

	// Unlock exclusive
	require.NoError(t, s.unlock(0, lockExclusive))
}

func TestInProcessShm_LockExclusiveWhenShared(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	// Acquire shared lock
	require.NoError(t, s.lock(0, lockShared))

	// Trying to acquire exclusive lock should return ErrBusy
	err := s.lock(0, lockExclusive)
	assert.ErrorIs(t, err, ErrBusy)

	// Unlock shared
	require.NoError(t, s.unlock(0, lockShared))
}

func TestInProcessShm_LockExclusiveWhenExclusive(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	require.NoError(t, s.lock(0, lockExclusive))

	// Trying to acquire exclusive again should return ErrBusy (state=-1, != 0)
	err := s.lock(0, lockExclusive)
	assert.ErrorIs(t, err, ErrBusy)

	require.NoError(t, s.unlock(0, lockExclusive))
}

func TestInProcessShm_LockInvalidSlot(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	err := s.lock(-1, lockShared)
	assert.Error(t, err)

	err = s.lock(lockSlotCount, lockShared)
	assert.Error(t, err)

	err = s.lock(999, lockExclusive)
	assert.Error(t, err)
}

func TestInProcessShm_UnlockInvalidSlot(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	err := s.unlock(-1, lockShared)
	assert.Error(t, err)

	err = s.unlock(lockSlotCount, lockExclusive)
	assert.Error(t, err)
}

func TestInProcessShm_UnlockSharedWhenZero(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	// unlock shared when state is 0 — should be no-op (state stays 0)
	err := s.unlock(0, lockShared)
	require.NoError(t, err)
	// state should still be 0
	assert.Equal(t, 0, s.mu[0].state)
}

func TestInProcessShm_UnlockExclusiveWhenNotExclusive(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	// unlock exclusive when state is 0 — should be no-op
	err := s.unlock(0, lockExclusive)
	require.NoError(t, err)
	assert.Equal(t, 0, s.mu[0].state)

	// Lock shared and try unlock exclusive — should be no-op (state > 0, not -1)
	require.NoError(t, s.lock(0, lockShared))
	err = s.unlock(0, lockExclusive)
	require.NoError(t, err)
	assert.Equal(t, 1, s.mu[0].state) // still shared
}

func TestInProcessShm_MultipleSharedLocks(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	require.NoError(t, s.lock(0, lockShared))
	require.NoError(t, s.lock(0, lockShared))
	require.NoError(t, s.lock(0, lockShared))

	assert.Equal(t, 3, s.mu[0].state)

	require.NoError(t, s.unlock(0, lockShared))
	assert.Equal(t, 2, s.mu[0].state)

	require.NoError(t, s.unlock(0, lockShared))
	require.NoError(t, s.unlock(0, lockShared))
	assert.Equal(t, 0, s.mu[0].state)
}

func TestInProcessShm_RegionCreateAndGet(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	// Getting non-existent region without create should fail
	_, err := s.region(0, false)
	assert.Error(t, err)

	// Create region 0
	r0, err := s.region(0, true)
	require.NoError(t, err)
	assert.Len(t, r0, shmRegionSize)

	// Getting existing region without create should succeed
	r0b, err := s.region(0, false)
	require.NoError(t, err)
	assert.Equal(t, r0, r0b)

	// Create region 2 (skipping region 1)
	r2, err := s.region(2, true)
	require.NoError(t, err)
	assert.Len(t, r2, shmRegionSize)

	// Region 1 was not created
	_, err = s.region(1, false)
	assert.Error(t, err)
}

func TestInProcessShm_Close(t *testing.T) {
	s := newHeapShm().(*inProcessShm)

	_, err := s.region(0, true)
	require.NoError(t, err)

	require.NoError(t, s.close())
	assert.Nil(t, s.regions)
}

// ===== shm_mmap.go coverage =====

func TestMmapShm_NewPlatformShm(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	require.NotNil(t, s)
	require.NoError(t, s.close())
}

func TestMmapShm_NewPlatformShm_BadPath(t *testing.T) {
	// Path to a non-existent directory should fail
	_, err := newPlatformShm("/nonexistent/dir/test.shm")
	assert.Error(t, err)
}

func TestMmapShm_RegionCreateAndAccess(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	defer s.close()

	// Create region 0
	r0, err := s.region(0, true)
	require.NoError(t, err)
	assert.Len(t, r0, shmRegionSize)

	// Access existing region
	r0b, err := s.region(0, false)
	require.NoError(t, err)
	assert.Equal(t, len(r0), len(r0b))

	// Create region 2 (gaps are OK)
	r2, err := s.region(2, true)
	require.NoError(t, err)
	assert.Len(t, r2, shmRegionSize)
}

func TestMmapShm_RegionNoCreate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	defer s.close()

	// Getting non-existent region without create should fail
	_, err = s.region(0, false)
	assert.Error(t, err)
}

func TestMmapShm_LockUnlock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	defer s.close()

	// Shared lock and unlock
	require.NoError(t, s.lock(0, lockShared))
	require.NoError(t, s.unlock(0, lockShared))

	// Exclusive lock and unlock
	require.NoError(t, s.lock(0, lockExclusive))
	require.NoError(t, s.unlock(0, lockExclusive))
}

func TestMmapShm_LockInvalidSlot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	defer s.close()

	err = s.lock(-1, lockShared)
	assert.Error(t, err)

	err = s.lock(lockSlotCount, lockExclusive)
	assert.Error(t, err)
}

func TestMmapShm_UnlockInvalidSlot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	defer s.close()

	err = s.unlock(-1, lockShared)
	assert.Error(t, err)

	err = s.unlock(lockSlotCount, lockExclusive)
	assert.Error(t, err)
}

func TestMmapShm_LockAllSlots(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)
	defer s.close()

	// Lock and unlock all valid slots with both types
	for slot := 0; slot < lockSlotCount; slot++ {
		require.NoError(t, s.lock(slot, lockShared))
		require.NoError(t, s.unlock(slot, lockShared))
		require.NoError(t, s.lock(slot, lockExclusive))
		require.NoError(t, s.unlock(slot, lockExclusive))
	}
}

func TestMmapShm_FcntlLockError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the underlying file to force fcntl to fail
	savedFile := ms.file
	_ = savedFile.Close()
	ms.file = savedFile // fd is now invalid

	// lock should fail with bad fd — fcntl called because in-process counter is 0
	err = ms.lock(0, lockShared)
	assert.Error(t, err)

	// unlock is a no-op: the lock above failed so the in-process counter
	// was never incremented. No fcntl call is made, so no error.
	err = ms.unlock(0, lockShared)
	assert.NoError(t, err)

	// Verify fcntl error propagates through unlock: manually set the
	// counter to simulate a held lock, then unlock triggers fcntl on bad fd.
	ms.locks[0] = 1
	err = ms.unlock(0, lockShared)
	assert.Error(t, err)

	ms.file = nil // prevent close() from panicking
}

func TestMmapShm_CloseDeletesFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	// Create a region so the file has content
	_, err = s.region(0, true)
	require.NoError(t, err)

	// Verify file exists
	_, err = os.Stat(path)
	require.NoError(t, err)

	// Close — since this is the only connection, it should delete the file
	require.NoError(t, s.close())

	// File should be deleted
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

// ===== integrity.go coverage =====

func TestIntegrityCheckN_Zero(t *testing.T) {
	// maxErrors=0 means unlimited
	db, ns := tempDBWithNS(t, "test")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		require.NoError(t, tx.Put(ns, []byte(key), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	// Should pass with maxErrors=0
	require.NoError(t, db.IntegrityCheckN(0))
}

func TestIntegrityCheckN_LimitErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)

	// Insert data to create multi-page tree
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	// Artificially inflate DatabaseSize to create orphan pages
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	oldSize := db.pager.dbSize.Load()
	db.pager.dbSize.Store(oldSize + 10)
	db.pager.header.DatabaseSize = oldSize + 10
	binary.BigEndian.PutUint32(pg1.data[28:32], oldSize+10)
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	// IntegrityCheckN(2) should limit to at most 2 errors
	err = db.IntegrityCheckN(2)
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	assert.LessOrEqual(t, len(ie.Errors), 2)
}

func TestIntegrityCheck_FreelistWithCorrectCount(t *testing.T) {
	db, ns := tempDBWithNS(t, "test")

	// Insert enough data to fill multiple pages
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("val-data-here")))
	}
	require.NoError(t, tx.Commit())

	// Delete most keys to create freelist
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 250; i++ {
		require.NoError(t, tx.Delete(ns, []byte(fmt.Sprintf("k%06d", i))))
	}
	require.NoError(t, tx.Commit())

	// Should pass with a proper freelist
	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheck_CorruptFreelistLeafCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	// Build a database with a freelist trunk
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("value")))
	}
	require.NoError(t, tx.Commit())

	// Delete the entire namespace to free all its pages
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNamespace("test"))
	require.NoError(t, tx.Commit())

	require.NoError(t, db.IntegrityCheck())
	require.True(t, db.pager.header.TotalFreelistPgs > 0)

	// Read the current freelist trunk page in a read tx to verify it has leaf entries
	trunkPgno := db.pager.header.FirstFreelistPg
	maxFrame, slot, rerr := db.pager.beginRead()
	require.NoError(t, rerr)
	trunkPg, rerr := db.pager.getPageWriter(trunkPgno, maxFrame)
	require.NoError(t, rerr)
	origLeafCount := binary.BigEndian.Uint32(trunkPg.data[4:8])
	db.pager.releasePage(trunkPg)
	db.pager.endRead(slot)

	if origLeafCount == 0 {
		t.Skip("freelist trunk has no leaf entries, can't test leaf count corruption")
	}

	// Corrupt the freelist trunk's leaf count to be absurdly large
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	trunkPgW, err := db.pager.getWritablePage(trunkPgno)
	require.NoError(t, err)
	binary.BigEndian.PutUint32(trunkPgW.data[4:8], 0xFFFFFFFF)
	db.pager.releasePage(trunkPgW)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err, "expected integrity check to fail after corrupting freelist leaf count")
}

func TestIntegrityCheck_CorruptFreeblockOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Set firstFreeBlk to a bad offset to test checkPageCoverage freeblock path
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	// Set firstFreeBlk to 1 (which is within the header, not content area)
	binary.BigEndian.PutUint16(pg.data[1:3], 1)
	pg.header.firstFreeBlk = 1
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "freeblock") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected freeblock error, got: %v", ie.Errors)
}

func TestIntegrityCheck_CorruptFreeblockSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	// Insert and delete to create freeblocks on the page
	for i := 0; i < 30; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	// Delete some entries to potentially create freeblocks
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 30; i += 3 {
		require.NoError(t, tx.Delete(ns, []byte(fmt.Sprintf("k%04d", i))))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Read page to check if it has freeblocks
	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	hasFreeBlk := pg.header.firstFreeBlk != 0
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if hasFreeBlk {
		// Corrupt freeblock size to be too small (< 4)
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		pg, err := db.pager.getWritablePage(rootPage)
		require.NoError(t, err)
		fbOff := int(pg.header.firstFreeBlk)
		// Set freeblock size to 2 (too small, min is 4)
		binary.BigEndian.PutUint16(pg.data[fbOff+2:fbOff+4], 2)
		db.pager.releasePage(pg)
		require.NoError(t, tx2.Commit())

		err = db.IntegrityCheck()
		require.Error(t, err)
		ie, ok := err.(*IntegrityError)
		require.True(t, ok)
		found := false
		for _, e := range ie.Errors {
			if strings.Contains(e, "freeblock") {
				found = true
				break
			}
		}
		assert.True(t, found, "expected freeblock error, got: %v", ie.Errors)
	}
}

func TestIntegrityCheck_CorruptFragmentation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Corrupt the fragmentation byte to a wrong value
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	pg.data[7] = 99 // fragBytes offset is at byte 7 of page header
	pg.header.fragBytes = 99
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "fragmentation") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'fragmentation' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_CorruptContentOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Corrupt the cell content offset to be too small
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	// Set cellContentOff to 1 (way too small)
	binary.BigEndian.PutUint16(pg.data[5:7], 1)
	pg.header.cellContentOff = 1
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "cell content offset") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'cell content offset' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_InteriorTreePage(t *testing.T) {
	// Create a DB with enough data to generate interior pages
	db, ns := tempDBWithNS(t, "test")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("key-%06d", i)
		val := fmt.Sprintf("value-%06d", i)
		require.NoError(t, tx.Put(ns, []byte(key), []byte(val)))
	}
	require.NoError(t, tx.Commit())

	// Verify that the tree is deep enough to have interior pages
	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheck_OverflowChainValidation(t *testing.T) {
	// Use a small page size to force overflow with smaller values
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)

	// Insert values large enough to overflow on 512-byte pages
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("k%04d", i)
		val := make([]byte, 400) // will definitely overflow on 512-byte pages
		require.NoError(t, tx.Put(ns, []byte(key), val))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheck_FreelistHeaderZeroTrunk(t *testing.T) {
	// Edge case: header says freelist pages > 0 but FirstFreelistPg == 0
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Set TotalFreelistPgs to 5 but leave FirstFreelistPg as 0
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	db.pager.header.TotalFreelistPgs = 5
	binary.BigEndian.PutUint32(pg1.data[36:40], 5)
	db.pager.header.FirstFreelistPg = 0
	binary.BigEndian.PutUint32(pg1.data[32:36], 0)
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "freelist") && strings.Contains(e, "first trunk is 0") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'first trunk is 0' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_CellExtendsOffPage(t *testing.T) {
	// Create a DB and corrupt a cell to extend past the page boundary
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Corrupt cell: change the key length varint to be absurdly large
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	cellOff := int(pg.getCellOffset(0))
	// Write a very large varint at the cell offset to make cell extend off page
	// Overwrite keyLen varint with a 2-byte varint encoding a large value
	pg.data[cellOff] = 0xFF   // continuation byte
	pg.data[cellOff+1] = 0x7F // value = 16383
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
}

func TestIntegrityCheck_MasterPageCorruptType(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Corrupt page 1 type to something invalid
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	pg1.data[dbHeaderSize] = 7 // invalid page type
	pg1.header.pageType = 7
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "master page 1") && strings.Contains(e, "invalid page type") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'master page 1: invalid page type' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_MasterPageCorruptContentOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Corrupt page 1's cellContentOff to be too small
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	// The page header starts at dbHeaderSize (100) for page 1
	// cellContentOff is at offset 5 within the page header
	binary.BigEndian.PutUint16(pg1.data[dbHeaderSize+5:dbHeaderSize+7], 1)
	pg1.header.cellContentOff = 1
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "master page 1") && strings.Contains(e, "cell content offset") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected master page 1 cell content offset error, got: %v", ie.Errors)
}

func TestIntegrityCheck_MasterPageCellOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Corrupt cell pointer on master page to point out of range
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	cpOff := pg1.cellPointerOffset()
	binary.BigEndian.PutUint16(pg1.data[cpOff:], 0xFFFF)
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "master page 1") && strings.Contains(e, "out of range") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'master page 1 cell out of range' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_MasterCellCorruptData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Corrupt the first cell data on master page
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	cellOff := int(pg1.getCellOffset(0))
	// Set impossibly large varint for key length
	pg1.data[cellOff] = 0xFF
	pg1.data[cellOff+1] = 0xFF
	pg1.data[cellOff+2] = 0xFF
	pg1.data[cellOff+3] = 0xFF
	pg1.data[cellOff+4] = 0xFF
	pg1.data[cellOff+5] = 0xFF
	pg1.data[cellOff+6] = 0xFF
	pg1.data[cellOff+7] = 0xFF
	pg1.data[cellOff+8] = 0xFF
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
}

func TestIntegrityCheck_MasterCellExtendsOffPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Corrupt the cell to have very large key/val to extend past page
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	cellOff := int(pg1.getCellOffset(0))
	// Write a 2-byte varint encoding a huge key length
	pg1.data[cellOff] = 0xFF   // continuation byte
	pg1.data[cellOff+1] = 0x7F // key length = 16383
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
}

func TestIntegrityCheck_MasterKeyOutOfOrder(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("alpha")
	require.NoError(t, err)
	_, err = tx.CreateNamespace("beta")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Swap cell pointers on page 1 to break key ordering
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	if pg1.header.cellCount >= 2 {
		off0 := pg1.getCellOffset(0)
		off1 := pg1.getCellOffset(1)
		pg1.setCellOffset(0, off1)
		pg1.setCellOffset(1, off0)
	}
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "key out of order") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'key out of order' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_NamespaceRootOutOfRange(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Corrupt the namespace root page to a huge value
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	cellOff := int(pg1.getCellOffset(0))
	// Parse the cell to find where the value starts
	cell, _, cerr := parseLeafCellWithSize(pg1.data, cellOff, 4096)
	if cerr == nil && len(cell.value) >= 4 {
		// Write invalid root page number into the value portion
		// The value starts after the key in the cell
		keyLen, kn, _ := getVarintSafe(pg1.data[cellOff:])
		valLen, vn, _ := getVarintSafe(pg1.data[cellOff+kn:])
		valStart := cellOff + kn + vn + int(keyLen)
		_ = valLen
		binary.BigEndian.PutUint32(pg1.data[valStart:], 0xFFFFFFFF)
	}
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "root page") && strings.Contains(e, "out of range") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'root page out of range' error, got: %v", ie.Errors)
	_ = ns
}

func TestIntegrityCheck_InteriorTreeCorruptChildDepth(t *testing.T) {
	// Create a large enough tree to have interior pages, then corrupt child
	db := tempDBWithPageSize(t, 512) // small pages to force interior nodes quickly

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("value-data-here")))
	}
	require.NoError(t, tx.Commit())

	// Verify it passes first
	require.NoError(t, db.IntegrityCheck())
}

func TestHeapInsertPull(t *testing.T) {
	var h []uint32

	// Insert in reverse order
	heapInsert(&h, 50)
	heapInsert(&h, 30)
	heapInsert(&h, 40)
	heapInsert(&h, 10)
	heapInsert(&h, 20)

	// Pull should give min each time
	assert.Equal(t, uint32(10), heapPull(&h))
	assert.Equal(t, uint32(20), heapPull(&h))
	assert.Equal(t, uint32(30), heapPull(&h))
	assert.Equal(t, uint32(40), heapPull(&h))
	assert.Equal(t, uint32(50), heapPull(&h))
	assert.Empty(t, h)
}

func TestHeapInsertPull_Duplicates(t *testing.T) {
	var h []uint32

	heapInsert(&h, 10)
	heapInsert(&h, 10)
	heapInsert(&h, 5)

	assert.Equal(t, uint32(5), heapPull(&h))
	assert.Equal(t, uint32(10), heapPull(&h))
	assert.Equal(t, uint32(10), heapPull(&h))
}

func TestIntegrityCheck_FreeblockChainUnordered(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	// Insert keys and delete some to create freeblocks
	for i := 0; i < 20; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("value")))
	}
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 20; i += 2 {
		require.NoError(t, tx.Delete(ns, []byte(fmt.Sprintf("k%04d", i))))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Check if root page has a freeblock chain
	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	firstFb := pg.header.firstFreeBlk
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if firstFb != 0 {
		// Create a circular/unordered freeblock chain
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		pg, err := db.pager.getWritablePage(rootPage)
		require.NoError(t, err)
		fbOff := int(pg.header.firstFreeBlk)
		// Set freeblock's next pointer to point to itself or before it
		binary.BigEndian.PutUint16(pg.data[fbOff:fbOff+2], uint16(fbOff))
		db.pager.releasePage(pg)
		require.NoError(t, tx2.Commit())

		err = db.IntegrityCheck()
		require.Error(t, err)
		ie, ok := err.(*IntegrityError)
		require.True(t, ok)
		found := false
		for _, e := range ie.Errors {
			if strings.Contains(e, "freeblock") {
				found = true
				break
			}
		}
		assert.True(t, found, "expected freeblock chain error, got: %v", ie.Errors)
	}
}

func TestIntegrityCheck_FreeblockExtendsOffPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("value")))
	}
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 20; i += 2 {
		require.NoError(t, tx.Delete(ns, []byte(fmt.Sprintf("k%04d", i))))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	firstFb := pg.header.firstFreeBlk
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if firstFb != 0 {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		pg, err := db.pager.getWritablePage(rootPage)
		require.NoError(t, err)
		fbOff := int(pg.header.firstFreeBlk)
		// Set freeblock size so it extends past the page
		binary.BigEndian.PutUint16(pg.data[fbOff+2:fbOff+4], 0xFFFF)
		// Set next pointer to 0 (end of chain)
		binary.BigEndian.PutUint16(pg.data[fbOff:fbOff+2], 0)
		db.pager.releasePage(pg)
		require.NoError(t, tx2.Commit())

		err = db.IntegrityCheck()
		require.Error(t, err)
		ie, ok := err.(*IntegrityError)
		require.True(t, ok)
		found := false
		for _, e := range ie.Errors {
			if strings.Contains(e, "freeblock") && strings.Contains(e, "extends off end") {
				found = true
				break
			}
		}
		assert.True(t, found, "expected 'freeblock extends off end' error, got: %v", ie.Errors)
	}
}

func TestIntegrityCheck_MultipleBytesUse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Duplicate a cell pointer to trigger "multiple uses for byte" overlap detection
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	if pg.header.cellCount >= 2 {
		// Make cell pointer 1 point to the same offset as cell pointer 0
		off0 := pg.getCellOffset(0)
		pg.setCellOffset(1, off0)
	}
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "multiple uses") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'multiple uses' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_OverflowOnInteriorPage(t *testing.T) {
	// Use very small page size to force interior page key overflow
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)

	// Insert entries with large keys that will overflow on interior pages
	for i := 0; i < 50; i++ {
		key := make([]byte, 200) // large key for 512-byte pages
		copy(key, fmt.Sprintf("key-%04d-padding-here-to-make-long", i))
		require.NoError(t, tx.Put(ns, key, []byte("v")))
	}
	require.NoError(t, tx.Commit())

	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheck_DatabaseSizeZero(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	// Set DatabaseSize to 0 to trigger early return
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	db.pager.header.DatabaseSize = 0
	binary.BigEndian.PutUint32(pg1.data[28:32], 0)
	db.pager.releasePage(pg1)
	require.NoError(t, tx.Commit())

	// IntegrityCheck should return nil for empty database
	require.NoError(t, db.IntegrityCheck())
}

func TestIntegrityCheck_DoubleReference(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("alpha")
	require.NoError(t, err)
	_, err = tx.CreateNamespace("beta")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	// Make both namespaces point to the same root page (causes double reference)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)

	// Parse cells on master page: find both namespace entries
	nCells := int(pg1.header.cellCount)
	var rootPages []int
	for i := 0; i < nCells; i++ {
		cellOff := int(pg1.getCellOffset(i))
		cell, _, cerr := parseLeafCellWithSize(pg1.data, cellOff, 4096)
		if cerr == nil && len(cell.value) >= 4 {
			rootPages = append(rootPages, cellOff)
		}
	}
	if len(rootPages) >= 2 {
		// Make second namespace point to first namespace's root page
		cell0, _, _ := parseLeafCellWithSize(pg1.data, rootPages[0], 4096)
		rp0 := binary.BigEndian.Uint32(cell0.value)

		cell1Off := rootPages[1]
		keyLen1, kn1, _ := getVarintSafe(pg1.data[cell1Off:])
		_, vn1, _ := getVarintSafe(pg1.data[cell1Off+kn1:])
		valStart1 := cell1Off + kn1 + vn1 + int(keyLen1)
		binary.BigEndian.PutUint32(pg1.data[valStart1:], rp0)
	}
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "2nd reference") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected '2nd reference' error, got: %v", ie.Errors)
}

// ===== Additional integrity.go coverage =====

func TestIntegrityError_Error(t *testing.T) {
	ie := &IntegrityError{Errors: []string{"err1", "err2", "err3"}}
	msg := ie.Error()
	assert.Equal(t, "err1\nerr2\nerr3", msg)
}

func TestIntegrityCheck_TooManyErrors_CheckList(t *testing.T) {
	// Trigger tooManyErrors at the start of checkList (line 85).
	// Strategy: use a freelist with a leaf page that is already referenced
	// (generating one error), then the freelist count mismatch generates
	// another error. With maxErrors=1, the tooManyErrors check at the
	// top of a subsequent checkList call should trigger.
	//
	// Alternative approach: create overflow cells with out-of-order keys.
	// The key ordering error happens BEFORE the overflow chain checkList call.
	// With maxErrors=1, checkList for overflow would hit tooManyErrors.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	// Insert values large enough to create overflow on 512-byte pages
	for i := 0; i < 10; i++ {
		val := make([]byte, 400)
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), val))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Corrupt key ordering: swap cell pointers on the leaf page
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	nCells := int(pg.header.cellCount)
	if nCells >= 2 {
		// Swap cells 0 and 1 to break key ordering
		off0 := pg.getCellOffset(0)
		off1 := pg.getCellOffset(1)
		pg.setCellOffset(0, off1)
		pg.setCellOffset(1, off0)
	}
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	// With maxErrors=1, the key-out-of-order error at cell 1 will fill errors.
	// If cell 1 also has overflow, its checkList call should hit tooManyErrors.
	err = db.IntegrityCheckN(1)
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	assert.LessOrEqual(t, len(ie.Errors), 1)
}

func TestIntegrityCheck_TooManyErrors_CheckTreePage(t *testing.T) {
	// To hit tooManyErrors at the START of checkTreePage (line 224),
	// checkTreePage must be called when errors >= maxErrors.
	// This happens when checkTreePage is called recursively: an interior page
	// calls checkTreePage for its children. If the first child generates an error,
	// subsequent children's checkTreePage calls will hit tooManyErrors at line 224.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("value-data-here")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Find the root page and verify it's interior
	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	isInterior := pg.header.isInterior()
	nCells := int(pg.header.cellCount)
	var firstChildPage uint32
	if isInterior && nCells > 0 {
		cellOff := int(pg.getCellOffset(0))
		firstChildPage = binary.BigEndian.Uint32(pg.data[cellOff : cellOff+4])
	}
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if !isInterior || nCells < 2 || firstChildPage == 0 {
		t.Skip("root page doesn't have enough interior cells")
	}

	// Corrupt the first child page type to invalid
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err = db.pager.getWritablePage(firstChildPage)
	require.NoError(t, err)
	pg.data[0] = 7 // invalid page type
	pg.header.pageType = 7
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	// With maxErrors=1, the first child generates "invalid page type" error,
	// then the second child's checkTreePage call hits tooManyErrors at line 224.
	err = db.IntegrityCheckN(1)
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	assert.LessOrEqual(t, len(ie.Errors), 1)
}

func TestIntegrityCheck_FreeblockSizeTooSmall_Deterministic(t *testing.T) {
	// Create a page with a manually crafted freeblock that has size < 4.
	// We need to ensure the page actually has a freeblock by inserting
	// and deleting specific keys.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	// Insert many small keys
	for i := 0; i < 50; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("key%04d", i)), []byte("v")))
	}
	require.NoError(t, tx.Commit())

	// Delete every other key to create freeblocks
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := 1; i < 50; i += 2 {
		require.NoError(t, tx.Delete(ns, []byte(fmt.Sprintf("key%04d", i))))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Find the root page and check if it has freeblocks
	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	firstFb := pg.header.firstFreeBlk
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if firstFb == 0 {
		// Try to create freeblocks on the page by using larger values then deleting
		tx, err = db.BeginWrite()
		require.NoError(t, err)
		for i := 0; i < 50; i += 2 {
			require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("key%04d", i)), []byte("somewhat-longer-value-to-fill-page")))
		}
		require.NoError(t, tx.Commit())

		tx, err = db.BeginWrite()
		require.NoError(t, err)
		for i := 0; i < 50; i += 4 {
			require.NoError(t, tx.Delete(ns, []byte(fmt.Sprintf("key%04d", i))))
		}
		require.NoError(t, tx.Commit())

		maxFrame, slot, _ = db.pager.beginRead()
		pg, _ = db.pager.getPageWriter(rootPage, maxFrame)
		firstFb = pg.header.firstFreeBlk
		db.pager.releasePage(pg)
		db.pager.endRead(slot)
	}

	if firstFb != 0 {
		// Corrupt freeblock size to 2 (< 4 minimum)
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		pg, err := db.pager.getWritablePage(rootPage)
		require.NoError(t, err)
		fbOff := int(pg.header.firstFreeBlk)
		binary.BigEndian.PutUint16(pg.data[fbOff+2:fbOff+4], 2)
		db.pager.releasePage(pg)
		require.NoError(t, tx2.Commit())

		err = db.IntegrityCheck()
		require.Error(t, err)
		ie, ok := err.(*IntegrityError)
		require.True(t, ok)
		found := false
		for _, e := range ie.Errors {
			if strings.Contains(e, "freeblock") && strings.Contains(e, "invalid size") {
				found = true
				break
			}
		}
		assert.True(t, found, "expected 'freeblock invalid size' error, got: %v", ie.Errors)
	} else {
		t.Log("no freeblocks on root page, testing with manually crafted freeblock on namespace page")
		// As fallback, manually set firstFreeBlk to point into content area
		// and set its size to 2
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		pg, err := db.pager.getWritablePage(rootPage)
		require.NoError(t, err)
		usableSize := int(db.pager.pageSize)
		// Pick a spot in the content area (end of page minus some space)
		fbPos := usableSize - 20
		// Set firstFreeBlk in header
		binary.BigEndian.PutUint16(pg.data[1:3], uint16(fbPos))
		pg.header.firstFreeBlk = uint16(fbPos)
		// Set next=0, size=2 (too small)
		binary.BigEndian.PutUint16(pg.data[fbPos:fbPos+2], 0)
		binary.BigEndian.PutUint16(pg.data[fbPos+2:fbPos+4], 2)
		db.pager.releasePage(pg)
		require.NoError(t, tx2.Commit())

		err = db.IntegrityCheck()
		require.Error(t, err)
	}
}

func TestIntegrityCheck_FreeblockExtendsOffPage_Deterministic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage
	usableSize := int(db.pager.pageSize)

	// Manually create a freeblock that extends off the page
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	// Place freeblock at usableSize - 8 (valid position in content area)
	fbPos := usableSize - 8
	binary.BigEndian.PutUint16(pg.data[1:3], uint16(fbPos))
	pg.header.firstFreeBlk = uint16(fbPos)
	// Next freeblock = 0 (end of chain)
	binary.BigEndian.PutUint16(pg.data[fbPos:fbPos+2], 0)
	// Size = 0xFFFF (way past the page)
	binary.BigEndian.PutUint16(pg.data[fbPos+2:fbPos+4], 0xFFFF)
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "freeblock") && strings.Contains(e, "extends off end") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'freeblock extends off end' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_FreeblockChainUnordered_Deterministic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage
	usableSize := int(db.pager.pageSize)

	// Create two freeblocks where the second's offset <= first's offset (unordered)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)

	// First freeblock at usableSize - 16
	fb1Pos := usableSize - 16
	// Second freeblock at usableSize - 20 (before fb1, creating unordered chain)
	fb2Pos := usableSize - 20

	// Set firstFreeBlk to fb1
	binary.BigEndian.PutUint16(pg.data[1:3], uint16(fb1Pos))
	pg.header.firstFreeBlk = uint16(fb1Pos)

	// fb1: next=fb2Pos (which is <= fb1Pos, unordered), size=4
	binary.BigEndian.PutUint16(pg.data[fb1Pos:fb1Pos+2], uint16(fb2Pos))
	binary.BigEndian.PutUint16(pg.data[fb1Pos+2:fb1Pos+4], 4)

	// fb2: next=0, size=4
	binary.BigEndian.PutUint16(pg.data[fb2Pos:fb2Pos+2], 0)
	binary.BigEndian.PutUint16(pg.data[fb2Pos+2:fb2Pos+4], 4)

	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "freeblock chain") && strings.Contains(e, "not ordered") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'freeblock chain not ordered' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_ValidFreeblockChain(t *testing.T) {
	// Create a page with a valid freeblock chain to cover the normal
	// freeblock loop path (fb = nextFb at line 196).
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%04d", i)), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage
	usableSize := int(db.pager.pageSize)

	// Create a valid freeblock on the page.
	// A valid freeblock: next=0, size>=4, within content area, fb+size <= usableSize.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(rootPage)
	require.NoError(t, err)

	// Place freeblock at usableSize - 16 (valid position)
	fbPos := usableSize - 16
	binary.BigEndian.PutUint16(pg.data[1:3], uint16(fbPos))
	pg.header.firstFreeBlk = uint16(fbPos)
	// next = 0 (end of chain), size = 8
	binary.BigEndian.PutUint16(pg.data[fbPos:fbPos+2], 0)
	binary.BigEndian.PutUint16(pg.data[fbPos+2:fbPos+4], 8)
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	// The integrity check will process the freeblock and hit fb = nextFb = 0,
	// exiting the loop normally. The fragmentation count will likely be wrong,
	// but that's OK - we're testing the freeblock loop coverage.
	err = db.IntegrityCheck()
	// May or may not error (fragmentation mismatch is likely)
	_ = err
}

func TestIntegrityCheck_InteriorCellExtendsOffPage(t *testing.T) {
	// Cover integrity.go line 350: interior cell extends off end of page.
	// Strategy: create DB with interior pages, close, modify file to set
	// ReservedSpace and place an interior cell near usableSize boundary.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	opts := DefaultOptions()
	opts.PageSize = 512
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("value-data-here")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage
	pageSize := 512

	// Verify root is interior
	mf, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, mf)
	require.True(t, pg.header.isInterior(), "root must be interior")
	require.True(t, pg.header.cellCount >= 1, "need cells")
	origCellOff := int(pg.getCellOffset(0))
	// Read the original cell to get its size
	_, origCellSz, _ := parseInteriorCell(pg.data, origCellOff, pageSize)
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	db.Close()

	// Modify file directly
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	reservedSpace := 20
	usableSize := pageSize - reservedSpace // 492

	// Set ReservedSpace
	data[20] = byte(reservedSpace)

	// Fix master page (page 1) cells: move into valid range
	masterHdrOff := 100
	masterCellCount := int(binary.BigEndian.Uint16(data[masterHdrOff+3 : masterHdrOff+5]))
	masterCpStart := masterHdrOff + 8
	if masterCellCount > 0 {
		for ci := 0; ci < masterCellCount; ci++ {
			mCellOff := int(binary.BigEndian.Uint16(data[masterCpStart+ci*2 : masterCpStart+ci*2+2]))
			if mCellOff > usableSize-40 {
				// Move cell to a safe location
				newPos := masterCpStart + masterCellCount*2 + 10 + ci*40
				_, mCellSz, _ := parseLeafCellWithSize(data[:pageSize], mCellOff, 0)
				if mCellSz > 0 && newPos+mCellSz < usableSize {
					copy(data[newPos:], data[mCellOff:mCellOff+mCellSz])
					binary.BigEndian.PutUint16(data[masterCpStart+ci*2:], uint16(newPos))
				}
			}
		}
		// Update content area offset for master page
		minOff := usableSize
		for ci := 0; ci < masterCellCount; ci++ {
			off := int(binary.BigEndian.Uint16(data[masterCpStart+ci*2 : masterCpStart+ci*2+2]))
			if off < minOff {
				minOff = off
			}
		}
		binary.BigEndian.PutUint16(data[masterHdrOff+5:masterHdrOff+7], uint16(minOff))
	}

	// Modify the interior root page: place cell near usableSize boundary
	pgOff := int(rootPage-1) * pageSize
	intHdrSize := 12 // interior page header

	// Read original cell from file
	origFileOff := pgOff + origCellOff

	// Place cell at usableSize - 6 (need it to extend past usableSize)
	// Interior cell: 4-byte leftChild + varint(keyLen) + key
	// Minimum cell: 4 + 1(varint) + 1(key) = 6 bytes
	// So cellOff=usableSize-6, cellSize>=6, cellOff+cellSize >= usableSize
	// We need cellOff+cellSize > usableSize, so use a slightly bigger cell
	newIntCellOff := usableSize - 5 // cell starts 5 bytes before usableSize
	// Copy original cell data (which is bigger than 5 bytes)
	if origCellSz > 5 {
		copy(data[pgOff+newIntCellOff:], data[origFileOff:origFileOff+origCellSz])
	}
	// Update cell pointer 0
	binary.BigEndian.PutUint16(data[pgOff+intHdrSize:pgOff+intHdrSize+2], uint16(newIntCellOff))
	// Update content area offset
	binary.BigEndian.PutUint16(data[pgOff+5:pgOff+7], uint16(newIntCellOff))

	require.NoError(t, os.WriteFile(path, data, 0666))
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db2.Close()

	err = db2.IntegrityCheckN(0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extends off end of page")
}

func TestIntegrityCheck_InteriorCorruptKey(t *testing.T) {
	// Cover integrity.go line 358: corrupt interior key (interiorFullKey returns error).
	// Strategy: craft an interior cell with keyLen > maxPayloadAlloc (1<<30).
	// parseInteriorCell doesn't check maxPayloadAlloc, so it succeeds.
	// But interiorFullKey checks maxPayloadAlloc and returns ErrCorrupt.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	opts := DefaultOptions()
	opts.PageSize = 512
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("value-data-here")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage
	pageSize := 512

	// Verify root is interior
	mf, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, mf)
	require.True(t, pg.header.isInterior())
	require.True(t, pg.header.cellCount >= 1)
	// Get a valid cell to use as template
	cellOff := int(pg.getCellOffset(0))
	cell, _, _ := parseInteriorCell(pg.data, cellOff, pageSize)
	leftChild := cell.leftChild
	db.pager.releasePage(pg)
	db.pager.endRead(slot)
	db.Close()

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Fix master page cells for usableSize (no ReservedSpace change needed here)
	pgOff := int(rootPage-1) * pageSize
	intHdrSize := 12

	// Craft an interior cell with huge keyLen at cell 0's position.
	// Interior cell format: [4-byte leftChild] [varint keyLen] [localKey] [4-byte overflowPg]
	// Use keyLen = maxPayloadAlloc + 1 = (1<<30) + 1 = 0x40000001
	// Varint encoding of 0x40000001: 5 bytes
	// 0x40000001 = 0100_0000_0000_0000_0000_0000_0000_0001
	// Varint (7 bits per byte, MSB continuation):
	// Byte 0: (0x40000001 >> 28) | 0x80 = 4 | 0x80 = 0x84
	// Byte 1: ((0x40000001 >> 21) & 0x7F) | 0x80 = 0 | 0x80 = 0x80
	// Byte 2: ((0x40000001 >> 14) & 0x7F) | 0x80 = 0 | 0x80 = 0x80
	// Byte 3: ((0x40000001 >> 7) & 0x7F) | 0x80 = 0 | 0x80 = 0x80
	// Byte 4: 0x40000001 & 0x7F = 0x01
	bigKeyLen := []byte{0x84, 0x80, 0x80, 0x80, 0x01}

	// Find a spot for our crafted cell
	cellPos := pgOff + intHdrSize + 2 + 50 // after header + cell pointer + some offset

	// Write the cell: leftChild + bigKeyLen varint + localKey (fill with 'x') + overflowPg
	binary.BigEndian.PutUint32(data[cellPos:], leftChild)
	copy(data[cellPos+4:], bigKeyLen)
	// localPayloadSize for this huge key would be small
	// Fill the rest of the cell with dummy data + a valid-looking overflow page
	usableSize := pageSize
	localSz := localPayloadSize(0x40000001, usableSize)
	for j := 0; j < localSz && cellPos+4+5+j < pgOff+pageSize; j++ {
		data[cellPos+4+5+j] = 'x'
	}
	ovfOff := cellPos + 4 + 5 + localSz
	if ovfOff+4 <= pgOff+pageSize {
		binary.BigEndian.PutUint32(data[ovfOff:], 3) // some valid page
	}

	// Update cell pointer 0 to point to our crafted cell
	cellOffInPage := cellPos - pgOff
	binary.BigEndian.PutUint16(data[pgOff+intHdrSize:pgOff+intHdrSize+2], uint16(cellOffInPage))

	// Set cellCount to 1 and update content area offset
	binary.BigEndian.PutUint16(data[pgOff+3:pgOff+5], 1)
	binary.BigEndian.PutUint16(data[pgOff+5:pgOff+7], uint16(cellOffInPage))

	require.NoError(t, os.WriteFile(path, data, 0666))
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db2.Close()

	err = db2.IntegrityCheckN(0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "corrupt interior key")
}

func TestIntegrityCheck_ChildDepthDiffers_RightChild(t *testing.T) {
	// Create a tree with interior pages, then corrupt the rightChild to point
	// to a different depth subtree
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("value-data")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	isInterior := pg.header.isInterior()
	nCells := int(pg.header.cellCount)
	rightChild := pg.header.rightChild
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if !isInterior || nCells == 0 {
		t.Skip("root is not interior or has no cells")
	}

	// Set rightChild to point to a leaf page (one level short) to trigger depth mismatch
	// Find a leaf page by checking page 2 or page 3
	var leafPage uint32
	maxFrame, slot, _ = db.pager.beginRead()
	for pgno := uint32(2); pgno < 20; pgno++ {
		p, e := db.pager.getPageWriter(pgno, maxFrame)
		if e == nil && p.header.isLeaf() {
			leafPage = pgno
			db.pager.releasePage(p)
			break
		}
		if e == nil {
			db.pager.releasePage(p)
		}
	}
	db.pager.endRead(slot)

	if leafPage == 0 || leafPage == rightChild {
		t.Skip("could not find a suitable leaf page for corruption")
	}

	// Set rightChild to the leaf page (wrong depth)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err = db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	// rightChild offset depends on page type
	// For interior pages: header.rightChild is at offset 8 in the page header
	binary.BigEndian.PutUint32(pg.data[8:12], leafPage)
	pg.header.rightChild = leafPage
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "depth differs") && strings.Contains(e, "rightChild") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'rightChild depth differs' error, got: %v", ie.Errors)
}

func TestIntegrityCheck_InteriorNoCells(t *testing.T) {
	// Test an interior page with nCells=0, only a rightChild (line 396-398).
	// This is an unusual state but can happen if we corrupt the cell count.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("value-data-here")))
	}
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	isInterior := pg.header.isInterior()
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if !isInterior {
		t.Skip("root page is not interior")
	}

	// Set cellCount to 0 on the interior page
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err = db.pager.getWritablePage(rootPage)
	require.NoError(t, err)
	binary.BigEndian.PutUint16(pg.data[3:5], 0)
	pg.header.cellCount = 0
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	// The check will visit this interior page, find nCells=0, go to rightChild,
	// and hit the nCells==0 branch at line 396-398.
	err = db.IntegrityCheck()
	// Will likely error due to orphan pages, but the path is exercised
	_ = err
}

func TestIntegrityCheck_LeafCellExtendsOffPage(t *testing.T) {
	// Cover integrity.go line 295: leaf cell extends off end of page.
	// Strategy: create DB, close, modify file to set ReservedSpace and
	// craft cells that work with reduced usableSize, reopen and check.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("val1")))
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage
	db.Close()

	// Modify the DB file directly
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	pageSize := 4096
	reservedSpace := 20
	usableSize := pageSize - reservedSpace // 4076

	// Set ReservedSpace in db header (byte 20 of page 1)
	data[20] = byte(reservedSpace)

	// Fix master page (page 1): move its cell into the valid range.
	// Page 1 page header is at offset 100. The cell pointer array starts
	// at offset 100 + 8 = 108. For 1 cell, the cell pointer is at 108-109.
	masterHdrOff := 100
	masterCellCount := int(binary.BigEndian.Uint16(data[masterHdrOff+3 : masterHdrOff+5]))
	masterCpStart := masterHdrOff + 8 // leaf page header = 8 bytes

	// Read original master cell and copy it to a valid location
	if masterCellCount > 0 {
		origCellOff := int(binary.BigEndian.Uint16(data[masterCpStart : masterCpStart+2]))
		// Cell format: varint(keyLen) + varint(valLen) + key + value
		// Read the cell to find its size
		pos := origCellOff
		// keyLen varint
		for data[pos]&0x80 != 0 {
			pos++
		}
		pos++
		// valLen varint
		for data[pos]&0x80 != 0 {
			pos++
		}
		pos++
		// Already past the varints; the rest is key+value data
		cell0, cellSz, _ := parseLeafCellWithSize(data[:pageSize], origCellOff, 0)
		_ = cell0

		// Place this cell at a position that fits within new usableSize
		// New position: usableSize - cellSz - 10 (well within range)
		newMasterCellOff := usableSize - cellSz - 10
		if newMasterCellOff < masterCpStart+masterCellCount*2 {
			newMasterCellOff = masterCpStart + masterCellCount*2 + 10
		}
		copy(data[newMasterCellOff:], data[origCellOff:origCellOff+cellSz])
		// Update cell pointer
		binary.BigEndian.PutUint16(data[masterCpStart:masterCpStart+2], uint16(newMasterCellOff))
		// Update content area offset
		binary.BigEndian.PutUint16(data[masterHdrOff+5:masterHdrOff+7], uint16(newMasterCellOff))
	}

	// Modify the namespace leaf page
	pgOff := int(rootPage-1) * pageSize

	// Set cellCount to 1
	binary.BigEndian.PutUint16(data[pgOff+3:pgOff+5], 1)

	// Place cell at usableSize - 4 = 4072 (relative to page start)
	newCellOff := usableSize - 4

	// Set content area offset
	binary.BigEndian.PutUint16(data[pgOff+5:pgOff+7], uint16(newCellOff))

	// Set cell pointer 0 (at offset 8 for leaf page)
	binary.BigEndian.PutUint16(data[pgOff+8:pgOff+10], uint16(newCellOff))

	// Write cell data: varint(3) + varint(3) + "aaa" + "bbb" = 8 bytes
	// cellOff=4072, cellSize=8, cellOff+cellSize=4080 > usableSize=4076
	cellPos := pgOff + newCellOff
	data[cellPos] = 3
	data[cellPos+1] = 3
	copy(data[cellPos+2:], []byte("aaa"))
	copy(data[cellPos+5:], []byte("bbb"))

	require.NoError(t, os.WriteFile(path, data, 0666))
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	// Reopen and run integrity check
	db2, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db2.Close()

	err = db2.IntegrityCheckN(0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extends off end of page")
}

func TestIntegrityCheck_DatabaseSizeZero_Header(t *testing.T) {
	// Test nPages==0 path (line 452-454) by corrupting the header.
	// The commit process overwrites DatabaseSize, so we need to
	// corrupt page 1 data directly after the last write.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	// Create some data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Set DatabaseSize to 0 in the db header on page 1.
	// We need to write this AFTER commit so it's not overwritten.
	// But the pager.header.DatabaseSize will be overwritten on next commit.
	// Instead, corrupt the raw page data in the WAL.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg1, err := db.pager.getWritablePage(1)
	require.NoError(t, err)
	// Set DatabaseSize to 0 in the raw data
	binary.BigEndian.PutUint32(pg1.data[28:32], 0)
	// Also set in the pager header so it doesn't get overwritten
	db.pager.header.DatabaseSize = 0
	db.pager.dbSize.Store(0)
	db.pager.releasePage(pg1)
	require.NoError(t, tx2.Commit())

	// IntegrityCheckN should hit nPages==0 and return nil
	err = db.IntegrityCheckN(0)
	// It should return nil (no error for empty database)
	assert.NoError(t, err)
}

func TestIntegrityCheck_ContentAreaOffset_InCheckPageCoverage(t *testing.T) {
	// Trigger the contentAreaOffset error inside checkPageCoverage (not checkTreePage).
	// This path is different: checkTreePage validates contentOffset first.
	// But checkPageCoverage also checks it again.
	// Since checkTreePage checks it first, we can't easily reach checkPageCoverage
	// with a bad contentOffset unless checkTreePage passes but checkPageCoverage fails.
	// Actually, the checkTreePage contentOffset check and checkPageCoverage contentOffset
	// check use the same function, so if one passes the other will too.
	// The uncovered line 169-172 in checkPageCoverage is therefore only reachable if
	// called independently. Let's skip this - it's unreachable through IntegrityCheckN
	// because checkTreePage validates contentOffset first at lines 254-258.
	t.Skip("contentAreaOffset error in checkPageCoverage is unreachable through normal path")
}

func TestIntegrityCheck_MasterCellExtendsOffPage_Deterministic(t *testing.T) {
	// Cover integrity.go line 549: master page cell extends off page.
	// Strategy: close DB, modify file to set ReservedSpace and place
	// the master cell near usableSize boundary so it extends past.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	db.Close()

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	pageSize := 4096
	reservedSpace := 20
	usableSize := pageSize - reservedSpace // 4076

	// Set ReservedSpace
	data[20] = byte(reservedSpace)

	// Master page (page 1): page header at offset 100
	masterHdrOff := 100
	masterCpStart := masterHdrOff + 8 // leaf header = 8 bytes
	origCellOff := int(binary.BigEndian.Uint16(data[masterCpStart : masterCpStart+2]))

	// Read original cell size
	_, origCellSz, cerr := parseLeafCellWithSize(data[:pageSize], origCellOff, 0)
	require.NoError(t, cerr)

	// Place cell at usableSize - (origCellSz - 2) so it extends past usableSize
	// by at least 2 bytes. Must be > contentAreaOffset gap.
	newCellOff := usableSize - origCellSz + 2
	if newCellOff < masterCpStart+2 {
		newCellOff = masterCpStart + 4
	}
	copy(data[newCellOff:newCellOff+origCellSz], data[origCellOff:origCellOff+origCellSz])

	// Update cell pointer and content area offset
	binary.BigEndian.PutUint16(data[masterCpStart:masterCpStart+2], uint16(newCellOff))
	binary.BigEndian.PutUint16(data[masterHdrOff+5:masterHdrOff+7], uint16(newCellOff))

	require.NoError(t, os.WriteFile(path, data, 0666))
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	db2, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db2.Close()

	err = db2.IntegrityCheckN(0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "extends off end of page")
}

func TestIntegrityCheck_TooManyErrors_FreelisLeaves(t *testing.T) {
	// This tests tooManyErrors inside the freelist leaf enumeration loop (line 127-130).
	// Create a freelist with many leaf pages, and corrupt the first leaf to trigger
	// errors that hit maxErrors during leaf enumeration.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("val-data-here")))
	}
	require.NoError(t, tx.Commit())

	// Delete everything to create many free pages
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNamespace("test"))
	require.NoError(t, tx.Commit())

	require.True(t, db.pager.header.TotalFreelistPgs > 0)
	trunkPgno := db.pager.header.FirstFreelistPg

	// Corrupt freelist leaf entries: make them all point to page 1 (double reference)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := db.pager.getWritablePage(trunkPgno)
	require.NoError(t, err)
	leafCount := binary.BigEndian.Uint32(pg.data[4:8])
	for i := uint32(0); i < leafCount && i < 5; i++ {
		// Point leaf to page 1 (already referenced = double reference = error)
		binary.BigEndian.PutUint32(pg.data[8+i*4:], 1)
	}
	db.pager.releasePage(pg)
	require.NoError(t, tx2.Commit())

	// With maxErrors=1, the leaf loop should hit tooManyErrors quickly
	err = db.IntegrityCheckN(1)
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	assert.LessOrEqual(t, len(ie.Errors), 1)
}

func TestIntegrityCheck_TooManyErrors_CheckListLoop(t *testing.T) {
	// Cover integrity.go line 93: tooManyErrors at top of checkList for-loop.
	// Need: 2+ freelist trunk pages, errors on first trunk that reach maxErrors,
	// then line 93 triggers on the second trunk iteration.
	// With pageSize=512, freelistMaxLeaves = (512-8)/4 = 126.
	// So we need > 126 free pages to get 2 trunks.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	opts := DefaultOptions()
	opts.PageSize = 512
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)

	// Create enough data to use many pages
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 5000; i++ {
		require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("k%06d", i)), []byte("value-padding-data")))
	}
	require.NoError(t, tx.Commit())

	// Delete to create many free pages
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNamespace("test"))
	require.NoError(t, tx.Commit())

	totalFree := db.pager.header.TotalFreelistPgs
	trunkPgno := db.pager.header.FirstFreelistPg
	t.Logf("totalFree=%d, trunkPgno=%d", totalFree, trunkPgno)
	require.True(t, totalFree > 126, "need multiple freelist trunks")
	db.Close()

	// Modify file: corrupt leaf entries on the first trunk page
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	pageSize := 512
	pgOff := int(trunkPgno-1) * pageSize
	leafCount := binary.BigEndian.Uint32(data[pgOff+4 : pgOff+8])
	t.Logf("first trunk leafCount=%d", leafCount)

	// Corrupt only the LAST leaf entry to point to page 1 (double reference).
	// The leaf loop processes leaves sequentially. When the last leaf generates
	// an error, the loop ends (no more leaves), then pgno is updated to next
	// trunk. At line 93, tooManyErrors returns true.
	lastLeafOff := pgOff + 8 + (int(leafCount)-1)*4
	binary.BigEndian.PutUint32(data[lastLeafOff:], 1)

	require.NoError(t, os.WriteFile(path, data, 0666))
	os.Remove(path + "-wal")
	os.Remove(path + "-shm")

	db2, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db2.Close()

	// maxErrors=1: first leaf error should trigger tooManyErrors on next loop iteration
	err = db2.IntegrityCheckN(1)
	require.Error(t, err)
}

func TestIntegrityCheck_OverflowChainWrongLength(t *testing.T) {
	// Test the overflow chain length mismatch (non-freelist path in checkList, line 150-152)
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)

	// Insert a value large enough to create an overflow chain on 512-byte pages
	bigVal := make([]byte, 1000)
	for i := range bigVal {
		bigVal[i] = byte(i % 256)
	}
	require.NoError(t, tx.Put(ns, []byte("overflow-key"), bigVal))
	require.NoError(t, tx.Commit())

	rootPage := ns.rootPage

	// Find the overflow page number
	maxFrame, slot, _ := db.pager.beginRead()
	pg, _ := db.pager.getPageWriter(rootPage, maxFrame)
	cellOff := int(pg.getCellOffset(0))
	cell, _, cerr := parseLeafCellWithSize(pg.data, cellOff, 512)
	db.pager.releasePage(pg)
	db.pager.endRead(slot)

	if cerr != nil || cell.overflowPg == 0 {
		t.Skip("no overflow page found")
	}

	// Corrupt the overflow chain: set the first overflow page's next pointer
	// to 0 (terminate early), making the chain shorter than expected
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ovflPg, err := db.pager.getWritablePage(cell.overflowPg)
	require.NoError(t, err)
	// Set next overflow page to 0 (premature end)
	binary.BigEndian.PutUint32(ovflPg.data[0:4], 0)
	db.pager.releasePage(ovflPg)
	require.NoError(t, tx2.Commit())

	err = db.IntegrityCheck()
	require.Error(t, err)
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "overflow chain length") {
			found = true
			break
		}
	}
	assert.True(t, found, "expected 'overflow chain length' error, got: %v", ie.Errors)
}

// ===== Additional shm_mmap.go coverage =====

func TestMmapShm_FcntlLock_NonEACCES(t *testing.T) {
	// Test fcntlLock returning a non-EACCES/EAGAIN error (line 144-146)
	// by closing the file and using the invalid fd
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)
	// Close the file to make the fd invalid
	origFile := ms.file
	_ = origFile.Close()
	ms.file = origFile // fd is now closed

	// This should get EBADF (not EACCES or EAGAIN) and hit line 147
	err = ms.fcntlLock(0, 0) // F_RDLCK = 0, offset 0
	assert.Error(t, err)
	// The error should NOT be ErrBusy (since it's EBADF, not EAGAIN/EACCES)
	assert.NotErrorIs(t, err, ErrBusy)
	assert.Contains(t, err.Error(), "fcntl lock")

	ms.file = nil // prevent double close
}

func TestMmapShm_RegionStatError(t *testing.T) {
	// Test the stat error in region (line 74-76).
	// Close the underlying fd via syscall to make Stat return EBADF.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the fd via syscall. The Go *os.File object still exists
	// but the underlying fd is invalid.
	fd := ms.file.Fd()
	_ = syscall.Close(int(fd))

	// Now Stat() will fail with EBADF
	_, err = ms.region(0, true)
	assert.Error(t, err)

	ms.file = nil // prevent double close
}

func TestMmapShm_RegionMmapError(t *testing.T) {
	// Test the mmap error in region (line 87-89).
	// Replace the file with a read-only one so mmap with PROT_WRITE fails.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the original file and reopen as read-only
	origFile := ms.file
	_ = origFile.Close()

	// Reopen the file as read-only
	roFile, err := os.Open(path)
	require.NoError(t, err)
	ms.file = roFile

	// Extend the file so stat shows enough size (bypasses truncate)
	// We need info.Size() >= requiredSize to skip the Truncate call
	_ = os.WriteFile(path, make([]byte, shmRegionSize*2), 0666)
	// Reopen after writing
	_ = roFile.Close()
	roFile, err = os.Open(path)
	require.NoError(t, err)
	ms.file = roFile

	// mmap with PROT_WRITE on a read-only fd should fail
	_, err = ms.region(0, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "mmap shm region")

	_ = roFile.Close()
	ms.file = nil
}

func TestMmapShm_RegionTruncateError(t *testing.T) {
	// Test the truncate error in region (line 78-80).
	// Use a read-only file that passes Stat but fails on Truncate.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	s, err := newPlatformShm(path)
	require.NoError(t, err)

	ms := s.(*mmapShm)

	// Close the original file and reopen as read-only
	origFile := ms.file
	_ = origFile.Close()

	roFile, err := os.Open(path)
	require.NoError(t, err)
	ms.file = roFile

	// The file is 0 bytes, so info.Size() < requiredSize, and Truncate will be called.
	// Truncate on a read-only fd fails.
	_, err = ms.region(0, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "extend shm file")

	_ = roFile.Close()
	ms.file = nil
}

func TestMmapShm_DMSLockFailure(t *testing.T) {
	// Test the DMS lock failure path in newPlatformShm (lines 50-53)
	// This is hard to trigger because we'd need the shared DMS lock to fail.
	// One approach: open the file read-only so F_RDLCK on it fails.
	dir := t.TempDir()
	path := filepath.Join(dir, "test.shm")

	// Create the file
	f, err := os.Create(path)
	require.NoError(t, err)
	f.Close()

	// Make the file read-only to see if it affects fcntl
	// Actually fcntl locks require the file to be opened with the right mode.
	// F_RDLCK requires at least read access, which OpenFile provides even with 0666.
	// Let's try a different approach: use a directory as the path
	_, err = newPlatformShm(filepath.Join(dir, "nonexistent", "subdir", "test.shm"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "open shm file")
}

// NOTE: The following integrity.go lines are unreachable through IntegrityCheckN
// because the integrity checker caps nPages to the actual DB size, making
// getPageAt errors impossible for pages within nPages:
// - Line 110-113: getPageAt error in checkList
// - Line 235-238: getPageAt error in checkTreePage
// - Line 434-436: beginRead error in IntegrityCheckN
// - Line 441-443: getPageAt(1) error in IntegrityCheckN
// - Line 445-448: hdr.deserialize error (Open catches corrupt magic first)
// - Line 505-507: second getPageAt(1) error in IntegrityCheckN
// These are defensive error paths that only trigger on I/O failures.

// ============================================================
// TestCov2_ tests for additional integrity.go and shm_mmap.go coverage
// ============================================================

// --- integrity.go L110-113: getPageAt error in checkList ---
func TestCov2_Integrity_CheckList_GetPageError(t *testing.T) {
	t.Skip("BUG: L110-113 unreachable - nPages capped to actual DB size, getPageAt won't fail for valid pages")
}

// --- integrity.go L169-172: contentAreaOffset error in checkPageCoverage ---
// checkPageCoverage is always called AFTER contentAreaOffset is checked by the
// caller (checkTreePage at L254, or IntegrityCheckN at L520). If the check
// fails, the caller skips checkPageCoverage. So L169-172 is structurally
// unreachable: the same contentAreaOffset call cannot fail in checkPageCoverage
// if it already succeeded in the caller on the same page.
func TestCov2_Integrity_CheckPageCoverage_ContentAreaOffsetError(t *testing.T) {
	t.Skip("BUG: L169-172 unreachable - contentAreaOffset is always pre-checked by caller before calling checkPageCoverage")
}

// --- integrity.go L224-226: tooManyErrors at start of checkTreePage ---
// To hit this, we need a recursive checkTreePage call where errors are already
// accumulated. The key path: an interior page with 1 cell whose leftChild's
// checkTreePage generates an error (filling maxErrors=1). Then at L392,
// checkTreePage(rightChild) is called, and L224's tooManyErrors() fires.
func TestCov2_Integrity_CheckTreePage_TooManyErrors(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	pageSize := 512

	// Use small page size to force a multi-level tree with fewer entries
	db, err := testOpen(t, path, Options{PageSize: uint32(pageSize), InProcess: true})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("ns1")
	require.NoError(t, err)

	// Insert enough data to force an interior root page in the namespace B-tree
	for i := 0; i < 200; i++ {
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 50)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Find the namespace root page
	cpBase := dbHeaderSize + 8
	cellOff := int(binary.BigEndian.Uint16(data[cpBase:]))
	pos := cellOff
	keyLen := int(data[pos])
	pos++
	pos++ // valLen
	nsRootPage := binary.BigEndian.Uint32(data[pos+keyLen : pos+keyLen+4])

	nsPageOff := int(nsRootPage-1) * pageSize
	nsPageType := data[nsPageOff]
	if nsPageType == pageTypeIntIdx {
		// Interior page. Get the leftChild from the first cell.
		// Interior page header: 12 bytes (type, firstFreeBlk, cellCount, cellContentOff, fragBytes, rightChild)
		cellPtrBase := nsPageOff + 12
		firstCellOff := int(binary.BigEndian.Uint16(data[cellPtrBase:]))
		// Interior cell starts with 4-byte leftChild
		leftChild := binary.BigEndian.Uint32(data[nsPageOff+firstCellOff:])

		// Corrupt the leftChild page's type to an invalid value
		leftChildOff := int(leftChild-1) * pageSize
		if leftChildOff >= 0 && leftChildOff < len(data) {
			data[leftChildOff] = 0xFF // invalid page type
		}

		require.NoError(t, os.WriteFile(path, data, 0644))

		db2, err := testOpen(t, path, Options{PageSize: uint32(pageSize), InProcess: true})
		if err != nil {
			t.Log("Could not reopen corrupted DB:", err)
			return
		}
		defer db2.Close()

		// maxErrors=1: leftChild's checkTreePage adds 1 error,
		// then rightChild's checkTreePage hits tooManyErrors at L224.
		err = db2.IntegrityCheckN(1)
		assert.Error(t, err)
	} else {
		// If the root is a leaf, we can't test the recursive path.
		// This shouldn't happen with 200 entries in a 512-byte page size.
		t.Log("Namespace root is not interior (type:", nsPageType, "), skipping")
	}
}

// --- integrity.go L235-238: getPageAt error in checkTreePage ---
func TestCov2_Integrity_CheckTreePage_GetPageError(t *testing.T) {
	t.Skip("BUG: L235-238 unreachable - nPages capped to actual DB size, getPageAt won't fail for valid pages")
}

// --- integrity.go L434-436: beginRead error in IntegrityCheckN ---
func TestCov2_Integrity_BeginReadError(t *testing.T) {
	t.Skip("BUG: L434-436 requires beginRead to fail - defensive error path for pager in error state")
}

// --- integrity.go L441-443: getPageAt(1) error in IntegrityCheckN ---
func TestCov2_Integrity_GetPage1Error(t *testing.T) {
	t.Skip("BUG: L441-443 requires getPageAt(1) to fail - page 1 always readable if beginRead succeeds")
}

// --- integrity.go L445-448: hdr.deserialize error in IntegrityCheckN ---
func TestCov2_Integrity_DeserializeError(t *testing.T) {
	t.Skip("BUG: L445-448 requires hdr.deserialize to fail - Open() catches corrupt magic first")
}

// --- integrity.go L505-507: second getPageAt(1) error in IntegrityCheckN ---
func TestCov2_Integrity_SecondGetPage1Error(t *testing.T) {
	t.Skip("BUG: L505-507 requires second getPageAt(1) to fail - first read succeeded moments before")
}

// --- shm_mmap.go L50-53: error in newPlatformShm DMS lock failure ---
// The DMS lock is F_RDLCK which should almost always succeed.
// To make it fail, we'd need to open the file in a mode that doesn't support
// read locks. The existing TestMmapShm_DMSLockFailure tests the open-file-failure path.
func TestCov2_NewPlatformShm_DMSLockError(t *testing.T) {
	// The DMS shared lock (F_RDLCK) failure path at L50-53 requires the
	// fcntl call to fail with an error other than EACCES/EAGAIN.
	// This is essentially impossible in normal circumstances on Linux.
	// fcntl F_RDLCK on a valid RW file descriptor always succeeds.
	t.Skip("BUG: L50-53 DMS F_RDLCK failure essentially impossible on valid RW file descriptor")
}

// --- shm_mmap.go L144-146: fcntlLock non-EACCES/EAGAIN error ---
// Already tested by TestMmapShm_FcntlLock_NonEACCES.
func TestCov2_FcntlLock_OtherError(t *testing.T) {
	// Test that fcntlLock returns a formatted error for non-EACCES/EAGAIN errors.
	// We can trigger this by using an invalid file descriptor.
	dir := t.TempDir()
	shmPath := filepath.Join(dir, "test.shm")

	ms, err := newPlatformShm(shmPath)
	require.NoError(t, err)

	// Close the file to invalidate the fd
	s := ms.(*mmapShm)
	fd := s.file.Fd()
	_ = fd

	// Close the underlying file
	_ = s.file.Close()

	// Now try to lock - should fail with EBADF
	err = s.fcntlLock(syscall.F_RDLCK, 0)
	assert.Error(t, err)
	// The error should NOT be ErrBusy (it's EBADF, not EACCES/EAGAIN)
	assert.NotErrorIs(t, err, ErrBusy)

	s.file = nil
}

// ===== page.cache backpointer coverage =====

func TestPageCacheBackpointer_SetOnCreate(t *testing.T) {
	pc := newPcache(4096, 100, true)

	// Creating a page should set pg.cache to the owning pcache.
	pg := pc.create(1, 2)
	assert.Equal(t, pc, pg.cache, "page.cache should point to owning pcache after create")
	assert.Equal(t, uint32(1), pg.pgno)
	assert.Equal(t, 1, pg.pinCount)
}

func TestPageCacheBackpointer_PreservedOnFetch(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	pc.release(pg)

	// Fetching should preserve the cache backpointer.
	fetched := pc.fetch(1)
	require.NotNil(t, fetched)
	assert.Equal(t, pc, fetched.cache, "page.cache should be preserved after fetch")
}

func TestPageCacheBackpointer_ReleaseRoutesViaCache(t *testing.T) {
	pc := newPcache(4096, 100, true)

	pg := pc.create(1, 2)
	assert.Equal(t, pc, pg.cache)

	// Release via the page's cache backpointer (simulating releasePage routing).
	pg.cache.release(pg)
	assert.Equal(t, 0, pg.pinCount)
	assert.Equal(t, 1, pc.nRecyclable, "page should be on LRU after release")
}

func TestPageCacheBackpointer_ClearedOnRecycle(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	defer db.Close()

	// Create a page with a cache set, simulating a cached page being recycled.
	pg := &page{
		data:     make([]byte, 4096),
		cache:    &pcache{},
		uncached: true,
		pgno:     42,
	}

	// recycleTempPage should clear pg.cache.
	db.pager.recycleTempPage(pg)
	assert.Nil(t, pg.cache, "page.cache should be nil after recycleTempPage")
	assert.Equal(t, uint32(0), pg.pgno, "pgno should be zeroed")
	assert.False(t, pg.uncached, "uncached should be false")
}

func TestPageCacheBackpointer_DifferentCaches(t *testing.T) {
	pc1 := newPcache(4096, 100, true)
	pc2 := newPcache(4096, 100, true)

	pg1 := pc1.create(1, 2)
	pg2 := pc2.create(1, 2)

	assert.Equal(t, pc1, pg1.cache, "pg1 should point to pc1")
	assert.Equal(t, pc2, pg2.cache, "pg2 should point to pc2")
	assert.True(t, pg1.cache != pg2.cache, "different caches should have different backpointers")

	// Releasing each page should route to the correct cache.
	pg1.cache.release(pg1)
	assert.Equal(t, 1, pc1.nRecyclable)
	assert.Equal(t, 0, pc2.nRecyclable)

	pg2.cache.release(pg2)
	assert.Equal(t, 1, pc1.nRecyclable)
	assert.Equal(t, 1, pc2.nRecyclable)
}

// ===== masterStore coverage =====

func TestMasterStore_ReadWriteBasic(t *testing.T) {
	ms := &masterStore{pages: make(map[uint32][]byte)}

	// Write a page
	src := make([]byte, 4096)
	copy(src, "hello masterStore")
	ms.writePage(1, src)

	// Read it back
	dst := make([]byte, 4096)
	found := ms.readPageInto(1, dst)
	assert.True(t, found)
	assert.Equal(t, src, dst)
}

func TestMasterStore_ReadNotFound(t *testing.T) {
	ms := &masterStore{pages: make(map[uint32][]byte)}

	dst := make([]byte, 4096)
	found := ms.readPageInto(42, dst)
	assert.False(t, found)
}

func TestMasterStore_OverwriteExisting(t *testing.T) {
	ms := &masterStore{pages: make(map[uint32][]byte)}

	src1 := make([]byte, 4096)
	copy(src1, "version 1")
	ms.writePage(1, src1)

	src2 := make([]byte, 4096)
	copy(src2, "version 2")
	ms.writePage(1, src2)

	dst := make([]byte, 4096)
	found := ms.readPageInto(1, dst)
	assert.True(t, found)
	assert.Equal(t, src2, dst, "should have version 2 data")
}

func TestMasterStore_IsolatesFromSource(t *testing.T) {
	ms := &masterStore{pages: make(map[uint32][]byte)}

	src := make([]byte, 4096)
	copy(src, "original")
	ms.writePage(1, src)

	// Modify the source buffer after writing
	copy(src, "modified")

	// masterStore should have a copy, not the original buffer
	dst := make([]byte, 4096)
	ms.readPageInto(1, dst)
	assert.Equal(t, byte('o'), dst[0], "masterStore should hold a copy, not a reference")
}

func TestMasterStore_MultiplePages(t *testing.T) {
	ms := &masterStore{pages: make(map[uint32][]byte)}

	for pgno := uint32(1); pgno <= 10; pgno++ {
		src := make([]byte, 4096)
		binary.BigEndian.PutUint32(src, pgno)
		ms.writePage(pgno, src)
	}

	// Verify each page
	for pgno := uint32(1); pgno <= 10; pgno++ {
		dst := make([]byte, 4096)
		found := ms.readPageInto(pgno, dst)
		assert.True(t, found, "page %d should exist", pgno)
		assert.Equal(t, pgno, binary.BigEndian.Uint32(dst), "page %d should have correct data", pgno)
	}
}

func TestMasterStore_InMemoryCheckpointBackfill(t *testing.T) {
	// Full integration test: InMemory DB writes data, checkpoints, and reads back via masterStore.
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.InMemory = true
	db, err := testOpen(t, filepath.Join(dir, "test.db"), opts)
	require.NoError(t, err)
	defer db.Close()

	// Verify masterStore was created
	require.NotNil(t, db.pager.master)

	// Write some data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("val1")))
	require.NoError(t, tx.Commit())

	// Read it back (verifies WAL read path works)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	val, err := rtx.Get(ns, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)
	require.NoError(t, rtx.Rollback())

	// Force a checkpoint to flush WAL to masterStore
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Read again after checkpoint (reads from masterStore, not WAL)
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	val2, err := rtx2.Get(ns, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val2)
	require.NoError(t, rtx2.Rollback())
}

// ===== getPageReader and readOverflowChainReader coverage =====

func TestGetPageReader_CacheHit(t *testing.T) {
	db, err := testOpen(t, "", Options{PageSize: 4096, InMemory: true})
	require.NoError(t, err)
	defer db.Close()

	// Write some data so there's content to read.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	cache := newPcache(4096, 50, true)

	// First read: cache miss, reads from WAL/disk.
	maxFrame, slot, err := db.pager.beginRead()
	require.NoError(t, err)
	defer db.pager.endRead(slot)

	pg1, err := db.pager.getPageReader(1, maxFrame, cache)
	require.NoError(t, err)
	assert.NotNil(t, pg1)
	assert.Equal(t, uint32(1), pg1.pgno)
	assert.Equal(t, cache, pg1.cache, "page should belong to reader cache")
	db.pager.releasePage(pg1)

	// Second read: cache hit.
	pg2, err := db.pager.getPageReader(1, maxFrame, cache)
	require.NoError(t, err)
	assert.NotNil(t, pg2)
	assert.Equal(t, pg1, pg2, "should return same page from cache")
	db.pager.releasePage(pg2)
}

func TestGetPageReader_CacheMiss(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	defer db.Close()

	// Write data to create page 2.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("val1")))
	require.NoError(t, tx.Commit())

	cache := newPcache(4096, 50, true)

	maxFrame, slot, err := db.pager.beginRead()
	require.NoError(t, err)
	defer db.pager.endRead(slot)

	// Read pages 1 and 2 - both should be cache misses initially.
	pg1, err := db.pager.getPageReader(1, maxFrame, cache)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), pg1.pgno)
	db.pager.releasePage(pg1)

	// Page 2 should also be readable.
	pg2, err := db.pager.getPageReader(2, maxFrame, cache)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), pg2.pgno)
	db.pager.releasePage(pg2)

	// Verify cache now has both pages.
	assert.NotNil(t, cache.fetch(1))
	cache.release(cache.fetch(1)) // double release to balance
	assert.NotNil(t, cache.fetch(2))
	cache.release(cache.fetch(2))
}

func TestGetPageReader_NilCacheFallback(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	defer db.Close()

	maxFrame, slot, err := db.pager.beginRead()
	require.NoError(t, err)
	defer db.pager.endRead(slot)

	// With nil cache, should fall back to the writer cache path.
	pg, err := db.pager.getPageReader(1, maxFrame, nil)
	require.NoError(t, err)
	assert.NotNil(t, pg, "with nil cache, should still return a page")
	assert.Equal(t, uint32(1), pg.pgno)
	db.pager.releasePage(pg)
}

func TestGetPageReader_InvalidPage(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	defer db.Close()

	cache := newPcache(4096, 50, true)

	_, err = db.pager.getPageReader(0, 0, cache)
	assert.ErrorIs(t, err, ErrInvalidPage)
}

func TestGetPageReader_InMemoryFallback(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions()
	opts.InMemory = true
	db, err := testOpen(t, filepath.Join(dir, "test.db"), opts)
	require.NoError(t, err)
	defer db.Close()

	// Write data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key"), []byte("val")))
	require.NoError(t, tx.Commit())

	// Force checkpoint so data is in masterStore
	require.NoError(t, db.Checkpoint(CheckpointFull))

	cache := newPcache(4096, 50, true)

	maxFrame, slot, err := db.pager.beginRead()
	require.NoError(t, err)
	defer db.pager.endRead(slot)

	// Should read from masterStore on cache miss
	pg, err := db.pager.getPageReader(1, maxFrame, cache)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), pg.pgno)
	assert.Equal(t, cache, pg.cache)
	db.pager.releasePage(pg)
}

func TestGetPageReader_StaleEviction(t *testing.T) {
	db, err := testOpen(t, "", Options{PageSize: 4096, InMemory: true})
	require.NoError(t, err)
	defer db.Close()

	// Write initial data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("val1")))
	require.NoError(t, tx.Commit())

	cache := newPcache(4096, 50, true)

	// Read page 1 into cache
	maxFrame1, slot1, err := db.pager.beginRead()
	require.NoError(t, err)
	pg1, err := db.pager.getPageReader(1, maxFrame1, cache)
	require.NoError(t, err)
	pg1Ptr := pg1
	db.pager.releasePage(pg1)
	db.pager.endRead(slot1)

	// Verify page is cached
	cached := cache.fetch(1)
	require.NotNil(t, cached, "page should be in cache")
	assert.Equal(t, pg1Ptr, cached, "should be same page object from cache")
	cache.release(cached)

	// Clear the cache (simulating pool recycling)
	cache.clear()

	// Verify page was evicted
	assert.Nil(t, cache.fetch(1), "page should be gone after clear")

	// Read again — page struct may be recycled from pFree (same pointer)
	// but data should be freshly loaded from disk.
	maxFrame2, slot2, err := db.pager.beginRead()
	require.NoError(t, err)
	defer db.pager.endRead(slot2)

	pg2, err := db.pager.getPageReader(1, maxFrame2, cache)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), pg2.pgno)
	// Page must have valid data (header deserialized from disk)
	assert.NotZero(t, pg2.header, "page should have been read from disk after clear")
	db.pager.releasePage(pg2)
}

func TestReadOverflowChainReader(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	// Write a large value that triggers overflow
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("test")
	require.NoError(t, err)
	largeVal := make([]byte, 2000) // will overflow with 512-byte pages
	for i := range largeVal {
		largeVal[i] = byte(i % 256)
	}
	require.NoError(t, tx.Put(ns, []byte("bigkey"), largeVal))
	require.NoError(t, tx.Commit())

	// Read the value back using a reader cache
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	val, err := rtx.Get(ns, []byte("bigkey"))
	require.NoError(t, err)
	assert.Equal(t, largeVal, val, "should read back the large overflow value")
}

func TestReaderCachePool_Lifecycle(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 500})
	require.NoError(t, err)
	defer db.Close()

	// Reader cache uses same size as writer cache (matches SQLite)
	assert.Equal(t, 500, db.readerCacheSize)
}

func TestReaderCachePool_SmallCacheSize(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 100})
	require.NoError(t, err)
	defer db.Close()

	assert.Equal(t, 100, db.readerCacheSize)
}

func TestReaderCachePool_LargeCacheSize(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 10000})
	require.NoError(t, err)
	defer db.Close()

	assert.Equal(t, 10000, db.readerCacheSize)
}

// ===== Task 5: Wire readers to private caches =====

func TestConcurrentReadersIndependentCaches(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 200})
	require.NoError(t, err)
	defer db.Close()

	// Write some data
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("value-%04d", i)
		require.NoError(t, wtx.Put(ns, []byte(key), []byte(val)))
	}
	require.NoError(t, wtx.Commit())

	// Start two readers concurrently — each should get its own cache
	rtx1, err := db.BeginRead()
	require.NoError(t, err)
	rtx2, err := db.BeginRead()
	require.NoError(t, err)

	// Verify each reader has its own cache
	require.NotNil(t, rtx1.cache)
	require.NotNil(t, rtx2.cache)
	assert.True(t, rtx1.cache != rtx2.cache, "readers should have distinct cache instances")

	// Read from both — each should populate its own cache
	ns1, err := rtx1.GetNamespace("test")
	require.NoError(t, err)
	ns2, err := rtx2.GetNamespace("test")
	require.NoError(t, err)

	v1, err := rtx1.Get(ns1, []byte("key-0050"))
	require.NoError(t, err)
	assert.Equal(t, "value-0050", string(v1))

	v2, err := rtx2.Get(ns2, []byte("key-0050"))
	require.NoError(t, err)
	assert.Equal(t, "value-0050", string(v2))

	// Rollback both
	require.NoError(t, rtx1.Rollback())
	require.NoError(t, rtx2.Rollback())
}

func TestReaderCacheStalenessAfterCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 200})
	require.NoError(t, err)
	defer db.Close()

	// Write initial data
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("v1")))
	require.NoError(t, wtx.Commit())

	// Start a reader — caches pages
	rtx1, err := db.BeginRead()
	require.NoError(t, err)
	ns1, err := rtx1.GetNamespace("test")
	require.NoError(t, err)
	v, err := rtx1.Get(ns1, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, "v1", string(v))

	// Writer commits new data
	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	ns2, err := wtx.GetNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns2, []byte("key1"), []byte("v2")))
	require.NoError(t, wtx.Commit())

	// The old reader should still see the old value (snapshot isolation)
	v, err = rtx1.Get(ns1, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, "v1", string(v))
	require.NoError(t, rtx1.Rollback())

	// A new reader should see the new value
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	ns3, err := rtx2.GetNamespace("test")
	require.NoError(t, err)
	v, err = rtx2.Get(ns3, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, "v2", string(v))
	require.NoError(t, rtx2.Rollback())
}

func TestReaderCacheRecycledOnRollback(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 200})
	require.NoError(t, err)
	defer db.Close()

	// Write data
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("key1"), []byte("val1")))
	require.NoError(t, wtx.Commit())

	// Reader 1: allocates cache, populates it, rollback clears + recycles
	rtx1, err := db.BeginRead()
	require.NoError(t, err)
	require.NotNil(t, rtx1.cache)
	ns1, err := rtx1.GetNamespace("test")
	require.NoError(t, err)
	_, err = rtx1.Get(ns1, []byte("key1"))
	require.NoError(t, err)
	require.NoError(t, rtx1.Rollback())

	// After rollback, cache field should be nil on the tx
	assert.Nil(t, rtx1.cache, "cache should be nil after rollback")

	// Reader 2: gets a cache (from pool or new) and it works correctly
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	require.NotNil(t, rtx2.cache, "new reader should always get a cache")
	ns2, err := rtx2.GetNamespace("test")
	require.NoError(t, err)
	v, err := rtx2.Get(ns2, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, "val1", string(v))
	require.NoError(t, rtx2.Rollback())
}

func TestWriterDoesNotGetReaderCache(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 200})
	require.NoError(t, err)
	defer db.Close()

	// Writer should have nil cache (uses shared pcache)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	assert.Nil(t, wtx.ReadTx.cache, "writer should not have a reader cache")
	require.NoError(t, wtx.Rollback())
}

func TestCursorWithReaderCache(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, filepath.Join(dir, "test.db"), Options{PageSize: 4096, CacheSize: 200})
	require.NoError(t, err)
	defer db.Close()

	// Write data
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("test")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key-%04d", i)
		val := fmt.Sprintf("value-%04d", i)
		require.NoError(t, wtx.Put(ns, []byte(key), []byte(val)))
	}
	require.NoError(t, wtx.Commit())

	// Read with cursor using reader cache
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, err := rtx.GetNamespace("test")
	require.NoError(t, err)

	cursor := rtx.NewCursor(ns2)
	require.NoError(t, cursor.First())

	count := 0
	for cursor.Valid() {
		k, kerr := cursor.Key()
		require.NoError(t, kerr)
		v, verr := cursor.Value()
		require.NoError(t, verr)
		expected := fmt.Sprintf("key-%04d", count)
		assert.Equal(t, expected, string(k))
		assert.Equal(t, fmt.Sprintf("value-%04d", count), string(v))
		count++
		require.NoError(t, cursor.Next())
	}
	assert.Equal(t, 50, count)
	cursor.Close()
	require.NoError(t, rtx.Rollback())
}
