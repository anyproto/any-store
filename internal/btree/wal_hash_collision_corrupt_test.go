package btree

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

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

	// Target frame 1 => idx 0 => nCollide = idx+1 = 1. The probe may pass over
	// at most one occupied slot; a second occupied slot is provably corrupt.
	const pgno = uint32(12345)
	const frame = uint32(1)
	h0 := int(pgno*htHash1) & (htNSlot - 1)

	// Occupy the first TWO slots of pgno's probe chain with non-matching, valid
	// entries so there is no empty slot within nCollide+1 probes. (No third
	// slot is occupied, so a fix that mis-bounds the probe would still find a
	// free slot — only the correctly-bounded check fires here.)
	binary.LittleEndian.PutUint16(region[hashSlotOff(h0):], 7)
	binary.LittleEndian.PutUint16(region[hashSlotOff((h0+1)&(htNSlot-1)):], 9)

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
	// frame 1 => idx 0 => nCollide = 1: two occupied slots => corrupt.
	binary.LittleEndian.PutUint16(region[hashSlotOff(h0):], 3)
	binary.LittleEndian.PutUint16(region[hashSlotOff((h0+1)&(htNSlot-1)):], 5)

	p := &page{pgno: pgno}
	err = wi.setBatch([]*page{p}, 1, true)
	require.ErrorIs(t, err, ErrCorrupt,
		"setBatch must propagate shmHashWrite's ErrCorrupt so the commit aborts")
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

	frame, err := wi.get(7, 100)
	require.ErrorIs(t, err, ErrCorrupt,
		"walIndex.get must forward shmHashGet's ErrCorrupt to the pager")
	require.Zero(t, frame)
}
