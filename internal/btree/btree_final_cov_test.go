package btree

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---- updateLeafCell with nil path causing split → insertIntoParent (L1376-1411) ----
// When updateLeafCell is called via the old insertIntoLeaf path (path=nil),
// and the new value causes page overflow, it calls insertIntoParent (old path).

func TestFinal_UpdateLeafCellNilPathSplit(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Fill the leaf page with small values via the old insert path
	for i := 0; i < 20; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		pg, err := p.getWritablePage(bt.rootPage)
		require.NoError(t, err)
		err = bt.insertIntoLeaf(pg, key, make([]byte, 10))
		p.releasePage(pg)
		require.NoError(t, err)
	}

	// Now update a key with a much larger value that will cause the page to overflow.
	// This triggers the slow path in updateLeafCell (L1365+) → split with path=nil (L1408-1411)
	key := binary.BigEndian.AppendUint32(nil, uint32(10))
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	err = bt.insertIntoLeaf(pg, key, make([]byte, 400))
	p.releasePage(pg)
	require.NoError(t, err)
}

// ---- insertIntoLeaf defrag path (L1202-1207) ----
// When insertIntoLeaf detects that totalFree >= cellSize+2 but gapSpace < cellSize+2,
// it triggers defragmentation via rebuildLeafPage.

func TestFinal_InsertIntoLeafDefragPath(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Fill the leaf page with cells
	for i := 0; i < 15; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		pg, err := p.getWritablePage(bt.rootPage)
		require.NoError(t, err)
		err = bt.insertIntoLeaf(pg, key, make([]byte, 20))
		p.releasePage(pg)
		require.NoError(t, err)
	}

	// Delete some cells via Drop (not through btree Delete, but by directly manipulating)
	// Actually, we can call insertIntoLeaf to update existing cells with smaller values,
	// which creates fragmentation in the page header.
	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		pg, err := p.getWritablePage(bt.rootPage)
		require.NoError(t, err)
		// Update with a much smaller value - creates waste tracked as fragBytes
		err = bt.insertIntoLeaf(pg, key, make([]byte, 5))
		p.releasePage(pg)
		require.NoError(t, err)
	}

	// Now insert a new key that fits in totalFree but not gapSpace
	// This should trigger the defrag path
	key := binary.BigEndian.AppendUint32(nil, uint32(100))
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	err = bt.insertIntoLeaf(pg, key, make([]byte, 20))
	p.releasePage(pg)
	require.NoError(t, err)
}

// ---- insertLeafCellAt overflow path (L1237-1239, L1250-1256) ----
// When inserting a leaf cell that needs overflow pages via insertLeafCellAt

func TestFinal_InsertLeafCellAtOverflow(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert a cell with overflow-sized value via old path
	key := binary.BigEndian.AppendUint32(nil, uint32(1))
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	err = bt.insertIntoLeaf(pg, key, make([]byte, 400))
	p.releasePage(pg)
	require.NoError(t, err)

	// Verify by reading
	pg2, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	n := int(pg2.header.cellCount)
	require.Equal(t, 1, n)
	bt.pager.releasePage(pg2)
}

// ---- collectLeafCells contentAreaOffset error (L1426-1428) ----
// When contentAreaOffset returns error, contentOff is set to usableSize

func TestFinal_CollectLeafCellsContentAreaError(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Insert some data
	for i := 0; i < 5; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		pg, err := p.getWritablePage(bt.rootPage)
		require.NoError(t, err)
		err = bt.insertIntoLeaf(pg, key, make([]byte, 20))
		p.releasePage(pg)
		require.NoError(t, err)
	}

	// Corrupt the cellContentOff to trigger contentAreaOffset error
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	// Set cellContentOff to 0 (invalid - less than header size)
	pg.header.cellContentOff = 0
	// Don't serialize to page data since collectLeafCells reads pg.header directly
	cells := bt.collectLeafCells(pg)
	p.releasePage(pg)
	// Should not panic; contentOff fallback to usableSize
	_ = cells
}

// ---- collectLeafCells contentSize < 0 (L1430-1432) ----

func TestFinal_CollectLeafCellsNegativeContentSize(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	for i := 0; i < 3; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		pg, err := p.getWritablePage(bt.rootPage)
		require.NoError(t, err)
		err = bt.insertIntoLeaf(pg, key, make([]byte, 10))
		p.releasePage(pg)
		require.NoError(t, err)
	}

	// Set cellContentOff beyond usableSize to create negative contentSize
	pg, err := p.getWritablePage(bt.rootPage)
	require.NoError(t, err)
	pg.header.cellContentOff = uint16(p.usableSize() + 10) // beyond usable → offset error → fallback
	cells := bt.collectLeafCells(pg)
	p.releasePage(pg)
	_ = cells
}

// ---- insertSepIntoInterior overflow key in parent (L1773-1776) ----
// When inserting a separator key into an interior page that requires overflow

func TestFinal_InsertSepIntoInteriorOverflowKey(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert large keys that will create overflow separator keys in interior pages
	// maxLocal for 512 = ~102 bytes, so keys > 102 bytes overflow in interior
	for i := 0; i < 50; i++ {
		key := make([]byte, 150) // > maxLocal, will overflow when promoted to interior
		binary.BigEndian.PutUint32(key, uint32(i))
		for j := 4; j < 150; j++ {
			key[j] = byte(i + j)
		}
		require.NoError(t, tx.Put(ns, key, []byte("v")))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

// ---- insertSepIntoInterior split path (L1839-1853) ----
// When inserting a separator into an already-full interior page, triggering interior split

func TestFinal_InteriorPageSplitViaInsert(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// With 512-byte pages and 4-byte keys, we need many keys to trigger
	// multiple interior page splits. At least 3 levels required.
	for i := 0; i < 800; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 4)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())

	// Verify all keys readable
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	require.NoError(t, rtx.Rollback())
	require.Equal(t, 800, count)
}

// ---- splitRoot error paths (L1955-1976) ----
// splitRoot allocates a new page and copies root content

func TestFinal_SplitRootViaOldPath(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	bt := initLeafBtree(t, p)

	// Fill the root leaf to capacity via old insertIntoLeaf, triggering splitRoot
	for i := 0; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		pg, err := p.getWritablePage(bt.rootPage)
		require.NoError(t, err)
		err = bt.insertIntoPage(pg, key, make([]byte, 15))
		p.releasePage(pg)
		require.NoError(t, err)
	}

	// After many inserts, the root should have split multiple times
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	require.True(t, pg.header.isInterior()) // Root should now be interior
	bt.pager.releasePage(pg)
}

// ---- Delete with getPage error in initial descent (L2029-2031, L2044-2046) ----
// These are I/O error paths - skip

// ---- Delete with getWritablePage error (L2054-2056) ----
// I/O error path - skip

// ---- Delete parseLeafCellWithSize error (L2071-2074) ----
// Corruption path

// ---- tryMergeLeaf freePage error (L2137-2139) ----
// I/O path - skip

// ---- removeChildFromParent rebuildInteriorPage error (L2349-2352) ----
// I/O/corruption path - skip

// ---- Cursor.Value overflow varint error (L2770-2772, L2775-2777) ----
// Corruption path - would need to corrupt page data after positioning cursor

// ---- Cursor.Value readOverflowChain error (L2795-2797) ----
// I/O path

// ---- leafSplitPoint and interiorSplitPoint clamping (L287-292, L329-331) ----
// These are purely defensive checks. Let me call them with edge-case inputs.

func TestFinal_SplitPointEdgeCases(t *testing.T) {
	// leafSplitPoint with 2 cells where first cell is huge
	// This should cause cumSize to exceed target immediately after i==0,
	// setting bestIdx = 1 at L276. The clamp at L287 would be 1, which matches.
	hugeCells := []cellData{
		{key: bytes.Repeat([]byte("x"), 3000), value: make([]byte, 1000)},
		{key: []byte("b"), value: make([]byte, 10)},
	}
	mid := leafSplitPoint(hugeCells, 4096)
	require.Equal(t, 1, mid)

	// leafSplitPoint where all cells are small → cumSize never exceeds target
	// → bestIdx = len(cells)-1 from L282 fallback
	smallCells := make([]cellData, 10)
	for i := range smallCells {
		smallCells[i] = cellData{key: []byte{byte(i)}, value: []byte{0}}
	}
	mid2 := leafSplitPoint(smallCells, 4096)
	require.LessOrEqual(t, mid2, 9)
	require.GreaterOrEqual(t, mid2, 1)

	// interiorSplitPoint with large cells → target exceeded at i=1
	largeIntCells := []cellData{
		{key: bytes.Repeat([]byte("a"), 2000), leftChild: 1},
		{key: bytes.Repeat([]byte("b"), 2000), leftChild: 2},
		{key: bytes.Repeat([]byte("c"), 2000), leftChild: 3},
	}
	intMid := interiorSplitPoint(largeIntCells, 4096)
	require.Equal(t, 1, intMid)

	// interiorSplitPoint where all cells fit → bestIdx = len(cells)-1
	// then clamp at L332 forces bestIdx to len(cells)-2
	smallIntCells := make([]cellData, 5)
	for i := range smallIntCells {
		smallIntCells[i] = cellData{key: []byte{byte(i)}, leftChild: uint32(i + 1)}
	}
	intMid2 := interiorSplitPoint(smallIntCells, 4096)
	require.LessOrEqual(t, intMid2, 3) // len-2 = 3
	require.GreaterOrEqual(t, intMid2, 1)
}

// ---- insertIntoParent getPage error (L1916-1918) ----
// This is the old non-path insertIntoParent. It calls bt.getPage(bt.rootPage)
// to traverse from root to find parent. This is I/O error.

// ---- insertIntoParent loop getPage error (L1937-1939) ----
// Same - I/O error during tree traversal

// ---- Cursor.Previous through rightChild position (L2952-2954) ----
// When frame.cellIdx == pg.header.cellCount in Previous(),
// childPgno = pg.header.rightChild. This happens when the cursor was
// positioned at the rightChild and decrements to access it.

func TestFinal_CursorPreviousFromRightChildPosition(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Create a multi-level tree
	for i := 0; i < 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 12)))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)

	// Position at the very last entry (rightChild's rightmost leaf)
	err = cur.Last()
	require.NoError(t, err)
	require.True(t, cur.Valid())

	// Move Previous. When we pop up from the leaf to the interior frame,
	// the cellIdx was at the rightChild position (n). Decrementing gives n-1.
	// If n-1 == cellCount, that's the rightChild position → L2952-2954
	err = cur.Previous()
	require.NoError(t, err)
	require.True(t, cur.Valid())

	// Continue Previous to fully traverse back
	for i := 0; i < 50; i++ {
		err = cur.Previous()
		require.NoError(t, err)
		if !cur.Valid() {
			break
		}
	}
}

// ---- Delete overflow cell causing high fragmentation → rebuild (L2100-2108, L2112-2115) ----
// Combine overflow deletion with high fragmentation

func TestFinal_DeleteOverflowWithHighFrag(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert mix of normal and overflow cells
	for i := 0; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 30)))
	}
	// Insert overflow values
	for i := 100; i < 103; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 400)))
	}
	require.NoError(t, tx.Commit())

	// Delete normal cells to accumulate fragmentation
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i < 10; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns2, key)
	}
	require.NoError(t, tx2.Commit())

	// Now delete an overflow cell — may trigger rebuild path due to high fragmentation
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	key := binary.BigEndian.AppendUint32(nil, uint32(100))
	_ = tx3.Delete(ns3, key)
	require.NoError(t, tx3.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// ---- Cursor.SeekNear fast-path idx >= n → c.Next() (L2680-2682) ----
// This is theoretically unreachable: we check key <= lastKey, so searchLeaf
// should never return idx >= n. But let's try to hit it by positioning
// cursor then searching with exact lastKey + epsilon within the same range check.

func TestFinal_SeekNearFastPathBoundary(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert sequential keys
	for i := 0; i < 50; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i*10))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)

	// Seek to a known key
	err = cur.Seek(binary.BigEndian.AppendUint32(nil, uint32(100)))
	require.NoError(t, err)
	require.True(t, cur.Valid())

	// Now SeekNear with a key slightly beyond but still in range
	// Use SeekNear with a key between existing keys on the same page
	err = cur.SeekNear(binary.BigEndian.AppendUint32(nil, uint32(105)))
	require.NoError(t, err)
}

// ---- Cursor.SeekExact Key() error (L2702-2704) ----
// This requires Key() to return error, which happens on corrupted page data.
// We can't easily trigger this through valid operations.

// ---- removeChildFromParent non-root empty interior → recursive (L2354-2360) ----
// Trigger by creating a 3+ level tree and deleting enough to empty a non-root interior

func TestFinal_RemoveChildRecursive(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert enough for a deep tree (3+ levels)
	for i := 0; i < 600; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 6)))
	}
	require.NoError(t, tx.Commit())

	// Delete most keys to trigger recursive removeChildFromParent
	// Delete from the middle to avoid simple rightChild removal
	for batch := 0; batch < 12; batch++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := batch * 50; i < (batch+1)*50; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx2.Delete(ns2, key)
		}
		require.NoError(t, tx2.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
}

// ---- parseLeafCellWithSize overflow keyLen/valLen bounds (L148-150) ----

func TestFinal_ParseLeafCellBoundsCheck(t *testing.T) {
	// Test keyLen > maxPayloadAlloc (1<<30)
	buf := make([]byte, 30)
	n1 := putVarint(buf, uint64(maxPayloadAlloc+1)) // keyLen > maxPayloadAlloc
	n2 := putVarint(buf[n1:], 0)                    // valLen = 0
	// Need enough buffer for the "pos >= dataLen" check
	fullBuf := make([]byte, n1+n2+100)
	copy(fullBuf, buf[:n1+n2])
	_, _, err := parseLeafCellWithSize(fullBuf, 0, 512)
	require.ErrorIs(t, err, ErrCorrupt)

	// Test totalPayload > maxPayloadAlloc (L148-150)
	// keyLen and valLen individually within maxPayloadAlloc but sum exceeds it
	buf2 := make([]byte, 30)
	n1 = putVarint(buf2, uint64(maxPayloadAlloc/2+1))
	n2 = putVarint(buf2[n1:], uint64(maxPayloadAlloc/2+1))
	fullBuf2 := make([]byte, n1+n2+100)
	copy(fullBuf2, buf2[:n1+n2])
	_, _, err = parseLeafCellWithSize(fullBuf2, 0, 512)
	require.ErrorIs(t, err, ErrCorrupt)
}
