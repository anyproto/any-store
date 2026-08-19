package btree

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pgHdrOff returns the header offset for a page (100 for page 1, 0 otherwise).
func pgHdrOff(pg *page) int {
	if pg.pgno == 1 {
		return dbHeaderSize
	}
	return 0
}

// makeInteriorPage0 creates an interior page with cellCount=0 and given rightChild.
// The loop in searchInterior* is skipped (lo=0, hi=0), so post-loop checks
// for lo==0 are reachable without the binary search seeing any cells.
func makeInteriorPage0(pg *page, usableSize int) {
	clear(pg.data)
	pg.header.pageType = pageTypeIntIdx
	pg.header.cellCount = 0
	pg.header.cellContentOff = uint16(usableSize)
	pg.header.rightChild = 99
	pg.header.serialize(pg.data[pgHdrOff(pg):])
}

// =============================================================================
// searchLeafPage corruption tests (L477-494)
// =============================================================================

// TestSearchCorrupt_SearchLeaf_MultiByteValLenEndBeyondData covers L477-479:
// 1-byte keyLen (< 0x80 fast path), multi-byte valLen, end = keyStart + kl > dataLen.
func TestSearchCorrupt_SearchLeaf_MultiByteValLenEndBeyondData(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	err = bt.rebuildLeafPage(pg, []cellData{{key: []byte("a"), value: bytes.Repeat([]byte("v"), 200)}})
	require.NoError(t, err)

	// Craft cell near page end: [keyLen=50] [valLen=2-byte varint(200)]
	// keyStart = newOff+3, end = newOff+53 > dataLen when newOff = dataLen-5.
	dataLen := len(pg.data)
	newOff := dataLen - 5
	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(newOff))
	pg.data[newOff] = 50     // keyLen=50 (1-byte varint)
	pg.data[newOff+1] = 0x81 // valLen multi-byte start
	pg.data[newOff+2] = 0x48 // valLen terminator (200)

	_, _, serr := searchLeafPage(pg, []byte("a"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchLeaf_KeyLenVarintError covers L484-486:
// Multi-byte keyLen varint (>= 0x80) that is truncated/unterminated.
func TestSearchCorrupt_SearchLeaf_KeyLenVarintError(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	bigKey := bytes.Repeat([]byte("k"), 200)
	err = bt.rebuildLeafPage(pg, []cellData{{key: bigKey, value: []byte("v")}})
	require.NoError(t, err)

	// Point cell pointer to last 2 bytes of page, both 0x80 (unterminated varint).
	cpOff := pg.cellPointerOffset()
	dataLen := len(pg.data)
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(dataLen-2))
	pg.data[dataLen-2] = 0x80
	pg.data[dataLen-1] = 0x80

	_, _, serr := searchLeafPage(pg, bigKey)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchLeaf_PosAfterKeyLenVarint covers L488-490:
// After decoding multi-byte keyLen varint, pos = off+kn >= dataLen.
func TestSearchCorrupt_SearchLeaf_PosAfterKeyLenVarint(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	bigKey := bytes.Repeat([]byte("k"), 200)
	err = bt.rebuildLeafPage(pg, []cellData{{key: bigKey, value: []byte("v")}})
	require.NoError(t, err)

	// 2-byte varint at dataLen-2: pos = (dataLen-2) + 2 = dataLen >= dataLen.
	cpOff := pg.cellPointerOffset()
	dataLen := len(pg.data)
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(dataLen-2))
	pg.data[dataLen-2] = 0x81
	pg.data[dataLen-1] = 0x00

	_, _, serr := searchLeafPage(pg, bigKey)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchLeaf_ValLenVarintAfterKeyLen covers L492-494:
// Multi-byte keyLen varint decodes OK but valLen varint is truncated.
func TestSearchCorrupt_SearchLeaf_ValLenVarintAfterKeyLen(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	bigKey := bytes.Repeat([]byte("k"), 200)
	err = bt.rebuildLeafPage(pg, []cellData{{key: bigKey, value: []byte("v")}})
	require.NoError(t, err)

	// 2-byte keyLen at dataLen-3, valid. valLen at dataLen-1 is 0x80 (truncated).
	cpOff := pg.cellPointerOffset()
	dataLen := len(pg.data)
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(dataLen-3))
	pg.data[dataLen-3] = 0x81
	pg.data[dataLen-2] = 0x00
	pg.data[dataLen-1] = 0x80

	_, _, serr := searchLeafPage(pg, bigKey)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// =============================================================================
// searchLeafWithOverflow corruption tests (L573-575, L597-599)
// =============================================================================

// TestSearchCorrupt_SearchLeafWithOverflow_KeyFitsLocallyEndBeyondData covers L573-575:
// Overflow cell where localKeyBytes == keyLen, end = payloadStart + localKeyBytes > dataLen.
func TestSearchCorrupt_SearchLeafWithOverflow_KeyFitsLocallyEndBeyondData(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	// Need totalPayload > maxLocal but key fits locally (keyLen < nLocal).
	// Use keyLen=10, valLen=maxLocal+100. totalPayload = maxLocal+110.
	keyLen := 10
	valLen := maxLocal + 100
	shortKey := bytes.Repeat([]byte("k"), keyLen)
	bigVal := bytes.Repeat([]byte("v"), valLen)

	err = bt.rebuildLeafPage(pg, []cellData{{key: shortKey, value: bigVal}})
	require.NoError(t, err)

	// Craft cell near page end: valid varints, but payloadStart + keyLen > dataLen.
	// varint(keyLen=10) = 1 byte, varint(valLen) = 2 bytes (since valLen > 127).
	// payloadStart = newOff + 3. Need newOff + 3 + 10 > dataLen => newOff > dataLen - 13.
	dataLen := len(pg.data)
	newOff := dataLen - 12 // payloadStart = dataLen - 9, end = dataLen - 9 + 10 = dataLen + 1

	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(newOff))
	pg.data[newOff] = byte(keyLen) // keyLen (1-byte varint)
	putVarint(pg.data[newOff+1:], uint64(valLen))

	_, _, serr := searchLeafWithOverflow(pg, shortKey, usableSize, p, 0, nil)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchLeafWithOverflow_LeafFullKeyError covers L597-599:
// Overflow cell where key overflows, prefix matches, leafFullKey returns error.
func TestSearchCorrupt_SearchLeafWithOverflow_LeafFullKeyError(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	// Key > maxLocal forces key overflow.
	bigKey := bytes.Repeat([]byte("k"), maxLocal+100)
	val := []byte("v")

	err = bt.rebuildLeafPage(pg, []cellData{{key: bigKey, value: val}})
	require.NoError(t, err)

	// Now craft the cell near page end so that searchLeafWithOverflow reads
	// the varints and prefix OK, but leafFullKey fails because
	// pos+nLocal+4 > dataLen (L831 in leafFullKey).
	// nLocal = localPayloadSize(totalPayload, usableSize).
	totalPayload := len(bigKey) + len(val)
	nLocal := localPayloadSize(totalPayload, usableSize)
	localKeyBytes := min(nLocal, len(bigKey))

	// varints: varint(bigKeyLen) = 2 bytes, varint(1) = 1 byte. payloadStart = newOff+3.
	// leafFullKey needs pos+nLocal+4 <= dataLen.
	// searchLeafWithOverflow needs only payloadStart+localKeyBytes for the prefix compare.
	// We need: payloadStart + localKeyBytes <= dataLen (prefix fits)
	// AND: payloadStart + nLocal + 4 > dataLen (leafFullKey fails at L831)
	// Since localKeyBytes <= nLocal, and nLocal+4 > localKeyBytes, this is possible.
	// newOff + 3 + nLocal + 4 > dataLen AND newOff + 3 + localKeyBytes <= dataLen
	// => newOff > dataLen - nLocal - 7 AND newOff <= dataLen - localKeyBytes - 3
	dataLen := len(pg.data)
	newOff := dataLen - localKeyBytes - 3 // exactly payloadStart + localKeyBytes = dataLen
	if newOff+3+nLocal+4 <= dataLen {
		// Adjust: need to be tighter. Actually with localKeyBytes < nLocal always true
		// for key overflow, we have room.
		newOff = dataLen - nLocal - 6 // pos+nLocal+4 = dataLen+1 > dataLen
	}

	if newOff < pg.cellPointerOffset()+2 {
		t.Skip("page too small for this test")
	}

	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(newOff))
	kn := putVarint(pg.data[newOff:], uint64(len(bigKey)))
	vn := putVarint(pg.data[newOff+kn:], uint64(len(val)))
	payloadStart := newOff + kn + vn

	// Copy local key prefix
	avail := dataLen - payloadStart
	if avail > localKeyBytes {
		avail = localKeyBytes
	}
	if avail > 0 {
		copy(pg.data[payloadStart:payloadStart+avail], bigKey[:avail])
	}

	// Search with same key prefix - prefix comparison should be 0, triggering leafFullKey.
	_, _, serr := searchLeafWithOverflow(pg, bigKey, usableSize, p, 0, nil)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// =============================================================================
// leafFullKey corruption tests (L784-786, L824-826)
// =============================================================================

// TestSearchCorrupt_LeafFullKey_KeyLenVarintError covers L784-786:
// leafFullKey gets a getVarintSafe error when reading keyLen.
func TestSearchCorrupt_LeafFullKey_KeyLenVarintError(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()

	err = bt.rebuildLeafPage(pg, []cellData{{key: []byte("abc"), value: []byte("def")}})
	require.NoError(t, err)

	off := int(pg.getCellOffset(0))
	// Corrupt keyLen varint to be unterminated (all 0xFF continuation bytes).
	for i := 0; i < 9 && off+i < len(pg.data); i++ {
		pg.data[off+i] = 0xFF
	}

	_, fkerr := leafFullKey(pg.data, off, usableSize, p, 0, nil)
	assert.ErrorIs(t, fkerr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_LeafFullKey_OverflowKeyFitsLocallyBeyondData covers L824-826:
// Overflow cell where localKeyBytes == keyLen, pos+localKeyBytes > dataLen.
func TestSearchCorrupt_LeafFullKey_OverflowKeyFitsLocallyBeyondData(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	// Need overflow where key fits locally: totalPayload > maxLocal, keyLen < nLocal.
	// keyLen=10, valLen=maxLocal+200. nLocal >= 10, so localKeyBytes = 10 = keyLen.
	keyLen := 10
	valLen := maxLocal + 200

	// Craft cell near page end: varints valid, pos+keyLen > dataLen.
	// varint(10) = 1 byte, varint(valLen) needs 2+ bytes.
	dataLen := len(pg.data)

	// Compute varint size for valLen
	valLenVarSize := varintSize(uint64(valLen))
	// payloadStart = off + 1 + valLenVarSize
	// Need payloadStart + keyLen > dataLen => off + 1 + valLenVarSize + 10 > dataLen
	off := dataLen - 1 - valLenVarSize - keyLen + 1 // payloadStart + keyLen = dataLen + 1

	if off < pg.cellPointerOffset()+2 || off < 0 {
		t.Skip("page too small")
	}

	// Clear area and write varints
	pg.data[off] = byte(keyLen) // keyLen (1-byte varint since keyLen < 128)
	putVarint(pg.data[off+1:], uint64(valLen))

	_, fkerr := leafFullKey(pg.data, off, usableSize, p, 0, nil)
	assert.ErrorIs(t, fkerr, ErrCorrupt)
	p.releasePage(pg)
}

// =============================================================================
// searchInteriorPage corruption tests (L673-675, L693-712)
// =============================================================================

// TestSearchCorrupt_SearchInteriorPage_CpBaseBeyondData covers L673-675:
// cpBase+2 > dataLen during binary search (cellCount absurdly large).
func TestSearchCorrupt_SearchInteriorPage_CpBaseBeyondData(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)

	pg.header.pageType = pageTypeIntIdx
	pg.header.cellCount = 10000
	pg.header.cellContentOff = uint16(p.usableSize())
	pg.header.rightChild = 99
	pg.header.serialize(pg.data[pgHdrOff(pg):])

	_, _, serr := searchInteriorPage(pg, []byte("any"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorPage_Lo0CpOff: cellCount=0 on a non-page-1 interior now trips the 0-cell gate
// (moveToRoot's page-1-only virtual root, btree.c:5610) before any cell-pointer
// read; the crafted layout below is kept as extra hostile input.
func TestSearchCorrupt_SearchInteriorPage_Lo0CpOff(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)

	// cellCount=0: loop skipped, lo=0. Truncate page so cpOff+2 > dataLen.
	makeInteriorPage0(pg, p.usableSize())
	origData := pg.data
	pg.data = origData[:pg.cellPointerOffset()] // cpOff+2 > len(data)

	_, _, serr := searchInteriorPage(pg, []byte("aaa"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	pg.data = origData
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorPage_Lo0InteriorCellKeyError: cellCount=0 on a non-page-1 interior now trips the 0-cell gate
// (moveToRoot's page-1-only virtual root, btree.c:5610) before any cell-pointer
// read; the crafted layout below is kept as extra hostile input.
func TestSearchCorrupt_SearchInteriorPage_Lo0InteriorCellKeyError(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)

	makeInteriorPage0(pg, p.usableSize())
	// Cell pointer 0 points to bad data (only 1 byte, need >= 5).
	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(len(pg.data)-1))
	pg.data[len(pg.data)-1] = 0

	_, _, serr := searchInteriorPage(pg, []byte("aaa"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorPage_LoLtN_CpBaseBeyondData covers L705-707:
// 0 < lo < n, cpBase+2 > dataLen. 8 cells, exact match on cell 4 => lo=5.
// Cell 4's data is relocated into the cell pointer area (overwriting ptrs 0-3).
func TestSearchCorrupt_SearchInteriorPage_LoLtN_CpBaseBeyondData(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	// Build 8-cell interior page.
	cells := []cellData{
		{key: []byte("aa"), leftChild: 2},
		{key: []byte("bb"), leftChild: 3},
		{key: []byte("cc"), leftChild: 4},
		{key: []byte("dd"), leftChild: 5},
		{key: []byte("ee"), leftChild: 6},
		{key: []byte("ff"), leftChild: 7},
		{key: []byte("gg"), leftChild: 8},
		{key: []byte("hh"), leftChild: 9},
	}
	err = bt.rebuildInteriorPage(pg, cells, 10)
	require.NoError(t, err)

	cpOff := pg.cellPointerOffset()

	// n=8. First mid = 0+(8-0)/2 = 4. Searching for "ee" => exact match.
	// lo=5, break. Post-loop: lo=5 < 8. cpBase = cpOff + 10.
	// Need cpBase+2 = cpOff+12 > dataLen.

	// Move cell 4 data into cpOff (overwriting cell ptrs 0-3, never accessed).
	// Cell format: [4-byte leftChild][varint keyLen=2][key "ee"] = 7 bytes.
	cellOff := cpOff
	binary.BigEndian.PutUint16(pg.data[cpOff+8:], uint16(cellOff)) // cell ptr 4 -> cellOff
	binary.BigEndian.PutUint32(pg.data[cellOff:], 6)               // leftChild
	pg.data[cellOff+4] = 2                                         // keyLen varint
	copy(pg.data[cellOff+5:cellOff+7], "ee")                       // key

	// Truncate: cell ptr 4 at cpOff+8 readable, cell ptr 5 at cpOff+10 partially cut.
	origData := pg.data
	pg.data = origData[:cpOff+11]

	_, _, serr := searchInteriorPage(pg, []byte("ee"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	pg.data = origData
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorPage_LoLtN_InteriorCellKeyError covers L710-712:
// 0 < lo < n, interiorCellKey fails for the lo-th cell.
// Same strategy: 4 cells, exact match on cell 2, corrupt cell 3's data.
func TestSearchCorrupt_SearchInteriorPage_LoLtN_InteriorCellKeyError(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	cells := []cellData{
		{key: []byte("aaa"), leftChild: 2},
		{key: []byte("ccc"), leftChild: 3},
		{key: []byte("eee"), leftChild: 4},
		{key: []byte("ggg"), leftChild: 5},
	}
	err = bt.rebuildInteriorPage(pg, cells, 10)
	require.NoError(t, err)

	// Corrupt cell ptr 3 to point to bad data.
	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff+6:], uint16(len(pg.data)-1))
	pg.data[len(pg.data)-1] = 0

	_, _, serr := searchInteriorPage(pg, []byte("eee"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// =============================================================================
// searchInteriorWithOverflow corruption tests (L729-731, L749-767)
// =============================================================================

// TestSearchCorrupt_SearchInteriorWithOverflow_CpBaseBeyondData covers L729-731:
// cpBase+2 > dataLen during binary search.
func TestSearchCorrupt_SearchInteriorWithOverflow_CpBaseBeyondData(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	usableSize := p.usableSize()

	pg.header.pageType = pageTypeIntIdx
	pg.header.cellCount = 10000
	pg.header.cellContentOff = uint16(usableSize)
	pg.header.rightChild = 99
	pg.header.serialize(pg.data[pgHdrOff(pg):])

	_, _, serr := searchInteriorWithOverflow(pg, []byte("any"), usableSize, p, 0, nil)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorWithOverflow_Lo0CpOff: cellCount=0 on a non-page-1 interior now trips the 0-cell gate
// (moveToRoot's page-1-only virtual root, btree.c:5610) before any cell-pointer
// read; the crafted layout below is kept as extra hostile input.
func TestSearchCorrupt_SearchInteriorWithOverflow_Lo0CpOff(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	usableSize := p.usableSize()

	makeInteriorPage0(pg, usableSize)
	origData := pg.data
	pg.data = origData[:pg.cellPointerOffset()]

	_, _, serr := searchInteriorWithOverflow(pg, []byte("aaa"), usableSize, p, 0, nil)
	assert.ErrorIs(t, serr, ErrCorrupt)
	pg.data = origData
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorWithOverflow_Lo0OffPlus4: cellCount=0 on a non-page-1 interior now trips the 0-cell gate
// (moveToRoot's page-1-only virtual root, btree.c:5610) before any cell-pointer
// read; the crafted layout below is kept as extra hostile input.
func TestSearchCorrupt_SearchInteriorWithOverflow_Lo0OffPlus4(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	usableSize := p.usableSize()

	makeInteriorPage0(pg, usableSize)
	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(len(pg.data)-2))

	_, _, serr := searchInteriorWithOverflow(pg, []byte("aaa"), usableSize, p, 0, nil)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorWithOverflow_LoLtN_CpBase covers L761-763:
// 0 < lo < n, cpBase+2 > dataLen. 8 cells, exact match on cell 4 => lo=5.
// Cell 4's data is relocated into the cell pointer area (overwriting ptrs 0-3).
func TestSearchCorrupt_SearchInteriorWithOverflow_LoLtN_CpBase(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()

	// Build 8-cell interior page.
	cells := []cellData{
		{key: []byte("aa"), leftChild: 2},
		{key: []byte("bb"), leftChild: 3},
		{key: []byte("cc"), leftChild: 4},
		{key: []byte("dd"), leftChild: 5},
		{key: []byte("ee"), leftChild: 6},
		{key: []byte("ff"), leftChild: 7},
		{key: []byte("gg"), leftChild: 8},
		{key: []byte("hh"), leftChild: 9},
	}
	err = bt.rebuildInteriorPage(pg, cells, 10)
	require.NoError(t, err)

	cpOff := pg.cellPointerOffset()

	// n=8. First mid = 4. Searching for "ee" => exact match.
	// lo=5, break. Post-loop: lo=5 < 8. cpBase = cpOff + 10.

	// Move cell 4 data into cpOff (overwriting cell ptrs 0-3, never accessed).
	cellOff := cpOff
	binary.BigEndian.PutUint16(pg.data[cpOff+8:], uint16(cellOff)) // cell ptr 4 -> cellOff
	binary.BigEndian.PutUint32(pg.data[cellOff:], 6)               // leftChild
	pg.data[cellOff+4] = 2                                         // keyLen varint
	copy(pg.data[cellOff+5:cellOff+7], "ee")                       // key

	// Truncate: cell ptr 5 at cpOff+10 partially cut.
	origData := pg.data
	pg.data = origData[:cpOff+11]

	_, _, serr := searchInteriorWithOverflow(pg, []byte("ee"), usableSize, p, 0, nil)
	assert.ErrorIs(t, serr, ErrCorrupt)
	pg.data = origData
	p.releasePage(pg)
}

// TestSearchCorrupt_SearchInteriorWithOverflow_LoLtN_OffPlus4 covers L765-767:
// 0 < lo < n, off+4 > dataLen. 4 cells, exact match on cell 2, corrupt cell 3 pointer.
func TestSearchCorrupt_SearchInteriorWithOverflow_LoLtN_OffPlus4(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()

	cells := []cellData{
		{key: []byte("aaa"), leftChild: 2},
		{key: []byte("ccc"), leftChild: 3},
		{key: []byte("eee"), leftChild: 4},
		{key: []byte("ggg"), leftChild: 5},
	}
	err = bt.rebuildInteriorPage(pg, cells, 10)
	require.NoError(t, err)

	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff+6:], uint16(len(pg.data)-2))

	_, _, serr := searchInteriorWithOverflow(pg, []byte("eee"), usableSize, p, 0, nil)
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// =============================================================================
// bt.searchInterior (btree method) corruption tests (L930-949)
// =============================================================================

// TestSearchCorrupt_BtSearchInterior_Lo0CpOff: cellCount=0 on a non-page-1 interior now trips the 0-cell gate
// (moveToRoot's page-1-only virtual root, btree.c:5610) before any cell-pointer
// read; the crafted layout below is kept as extra hostile input.
func TestSearchCorrupt_BtSearchInterior_Lo0CpOff(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	makeInteriorPage0(pg, bt.usablePageSize())
	origData := pg.data
	pg.data = origData[:pg.cellPointerOffset()]

	_, _, serr := bt.searchInterior(pg, []byte("aaa"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	pg.data = origData
	p.releasePage(pg)
}

// TestSearchCorrupt_BtSearchInterior_Lo0InteriorCellFullKeyError: cellCount=0 on a non-page-1 interior now trips the 0-cell gate
// (moveToRoot's page-1-only virtual root, btree.c:5610) before any cell-pointer
// read; the crafted layout below is kept as extra hostile input.
func TestSearchCorrupt_BtSearchInterior_Lo0InteriorCellFullKeyError(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	makeInteriorPage0(pg, bt.usablePageSize())
	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff:], uint16(len(pg.data)-1))
	pg.data[len(pg.data)-1] = 0

	_, _, serr := bt.searchInterior(pg, []byte("aaa"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}

// TestSearchCorrupt_BtSearchInterior_LoLtN_CpBase covers L942-944:
// 0 < lo < n, cpBase+2 > dataLen. 8 cells, exact match on cell 4 => lo=5.
// Cell 4's data is relocated into the cell pointer area (overwriting ptrs 0-3).
func TestSearchCorrupt_BtSearchInterior_LoLtN_CpBase(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	// Build 8-cell interior page.
	cells := []cellData{
		{key: []byte("aa"), leftChild: 2},
		{key: []byte("bb"), leftChild: 3},
		{key: []byte("cc"), leftChild: 4},
		{key: []byte("dd"), leftChild: 5},
		{key: []byte("ee"), leftChild: 6},
		{key: []byte("ff"), leftChild: 7},
		{key: []byte("gg"), leftChild: 8},
		{key: []byte("hh"), leftChild: 9},
	}
	err = bt.rebuildInteriorPage(pg, cells, 10)
	require.NoError(t, err)

	cpOff := pg.cellPointerOffset()

	// n=8. First mid = 0+(8-0)/2 = 4. Searching for "ee" => exact match.
	// lo=5, break. Post-loop: lo=5 < 8. cpBase = cpOff + 10.
	// Need cpBase+2 = cpOff+12 > dataLen.

	// Move cell 4 data into cpOff (overwriting cell ptrs 0-3, never accessed).
	// Cell format: [4-byte leftChild][varint keyLen=2][key "ee"] = 7 bytes.
	cellOff := cpOff
	binary.BigEndian.PutUint16(pg.data[cpOff+8:], uint16(cellOff)) // cell ptr 4 -> cellOff
	binary.BigEndian.PutUint32(pg.data[cellOff:], 6)               // leftChild
	pg.data[cellOff+4] = 2                                         // keyLen varint
	copy(pg.data[cellOff+5:cellOff+7], "ee")                       // key

	// Truncate: cell ptr 4 at cpOff+8 readable, cell ptr 5 at cpOff+10 partially cut.
	origData := pg.data
	pg.data = origData[:cpOff+11]

	_, _, serr := bt.searchInterior(pg, []byte("ee"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	pg.data = origData
	p.releasePage(pg)
}

// TestSearchCorrupt_BtSearchInterior_LoLtN_InteriorCellFullKeyError covers L947-949:
// 0 < lo < n, interiorCellFullKey fails for the lo-th cell.
// 4 cells, exact match on cell 2, corrupt cell 3.
func TestSearchCorrupt_BtSearchInterior_LoLtN_InteriorCellFullKeyError(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}

	cells := []cellData{
		{key: []byte("aaa"), leftChild: 2},
		{key: []byte("ccc"), leftChild: 3},
		{key: []byte("eee"), leftChild: 4},
		{key: []byte("ggg"), leftChild: 5},
	}
	err = bt.rebuildInteriorPage(pg, cells, 10)
	require.NoError(t, err)

	cpOff := pg.cellPointerOffset()
	binary.BigEndian.PutUint16(pg.data[cpOff+6:], uint16(len(pg.data)-1))
	pg.data[len(pg.data)-1] = 0

	_, _, serr := bt.searchInterior(pg, []byte("eee"))
	assert.ErrorIs(t, serr, ErrCorrupt)
	p.releasePage(pg)
}
