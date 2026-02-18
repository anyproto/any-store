package btree

// B-tree implementation modeled after SQLite's btree.c.
// Uses index-style B-tree where both keys and values are stored as byte slices.
// Keys are sorted in byte order. Each cell stores: key-length | key | value-length | value.

import (
	"bytes"
	"encoding/binary"
)

// btree represents a single B-tree (one namespace).
type btree struct {
	pager       *pager
	rootPage    uint32
	walMaxFrame uint32 // WAL snapshot for this operation (0 = use pager default)
	writable    bool   // true if this btree is used by a write transaction
}

// getPage returns a page using this btree's walMaxFrame for snapshot isolation.
func (bt *btree) getPage(pgno uint32) (*page, error) {
	if bt.writable {
		// Writer fast path: return own dirty pages directly.
		// writePages is only accessed by the single writer goroutine.
		if pg := bt.pager.writePages[pgno]; pg != nil {
			pg.pinCount++
			return pg, nil
		}
	} else if bt.walMaxFrame > 0 {
		// Reader MVCC path: bypass dirty pages from uncommitted writer.
		return bt.pager.readPageMVCC(pgno, bt.walMaxFrame)
	}
	if bt.walMaxFrame > 0 {
		return bt.pager.getPageAt(pgno, bt.walMaxFrame)
	}
	return bt.pager.getPage(pgno)
}

// usablePageSize returns the usable page size, accounting for reserved space.
func (bt *btree) usablePageSize() int {
	return int(bt.pager.pageSize) - int(bt.pager.header.ReservedSpace)
}

// cellData represents a parsed cell from a B-tree page.
type cellData struct {
	key        []byte
	value      []byte
	leftChild  uint32 // only for interior pages
	overflowPg uint32 // overflow page number (0 = no overflow)
}

// parseLeafCell parses a leaf cell at the given offset in page data.
// Leaf cell format: varint(keyLen) | key | varint(valLen) | value_local | [4-byte overflow pgno]
// When payload (keyLen + valLen) exceeds maxLocal, only a local portion of
// the value is stored in-page and the rest is on overflow pages.
func parseLeafCell(data []byte, offset int) (cellData, int) {
	return parseLeafCellWithSize(data, offset, 0)
}

// parseLeafCellWithSize is like parseLeafCell but uses usableSize to detect overflow.
// If usableSize is 0, overflow detection is skipped (backward compat).
func parseLeafCellWithSize(data []byte, offset int, usableSize int) (cellData, int) {
	var c cellData
	pos := offset

	keyLen, n := getVarint(data[pos:])
	pos += n

	c.key = data[pos : pos+int(keyLen)]
	pos += int(keyLen)

	valLen, n := getVarint(data[pos:])
	pos += n

	totalPayload := int(keyLen) + int(valLen)
	maxLocal := 0
	if usableSize > 0 {
		maxLocal = maxLocalPayload(usableSize)
	}

	if usableSize > 0 && totalPayload > maxLocal {
		// Overflow cell: only localSize bytes of value stored in-page
		localSize := localPayloadSize(totalPayload, usableSize)
		localValSize := localSize - int(keyLen)
		if localValSize < 0 {
			localValSize = 0
		}
		c.value = data[pos : pos+localValSize]
		pos += localValSize
		c.overflowPg = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
	} else {
		c.value = data[pos : pos+int(valLen)]
		pos += int(valLen)
	}

	return c, pos - offset
}

// parseInteriorCell parses an interior cell at the given offset.
// Interior cell format: 4-byte left child | varint(keyLen) | key
func parseInteriorCell(data []byte, offset int) (cellData, int) {
	var c cellData
	pos := offset

	c.leftChild = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	keyLen, n := getVarint(data[pos:])
	pos += n

	c.key = data[pos : pos+int(keyLen)]
	pos += int(keyLen)

	return c, pos - offset
}

// leafCellSize returns the serialized size of a leaf cell (in-page portion).
// For cells that overflow, this includes the 4-byte overflow pointer.
func leafCellSize(key, value []byte) int {
	return varintSize(uint64(len(key))) + len(key) + varintSize(uint64(len(value))) + len(value)
}

// leafSplitPoint finds the optimal split index for leaf cells, targeting ~2/3 fill
// on the left page (SQLite-style). Returns the index of the first cell that should
// go to the right page. The returned value is always in [1, len(cells)-1], so
// both left and right sides have at least one cell.
//
// SQLite's balance_nonroot (btree.c ~line 8600) redistributes cells among siblings
// so that each page is approximately equally full. For a simple 2-way split, this
// translates to keeping the left page as full as possible (up to ~2/3 of usable
// space) so that future insertions into either side are less likely to trigger
// another split.
func leafSplitPoint(cells []cellData, usableSize int) int {
	if len(cells) <= 2 {
		return 1
	}

	// Target: fill the left page to ~2/3 of usable space.
	// Available space = usableSize - leafHeaderSize (8 bytes).
	// Each cell also needs a 2-byte cell pointer.
	target := (usableSize - 8) * 2 / 3
	cumSize := 0
	bestIdx := len(cells) / 2 // fallback to 50/50

	for i, c := range cells {
		if i == 0 {
			// always put at least 1 cell on left
			cumSize += leafCellSizeWithOverflow(c.key, c.value, usableSize) + 2
			continue
		}
		cellSz := leafCellSizeWithOverflow(c.key, c.value, usableSize) + 2
		if cumSize+cellSz > target {
			bestIdx = i
			break
		}
		cumSize += cellSz
		if i == len(cells)-1 {
			// Don't put all cells on the left; right needs at least 1
			bestIdx = i
		}
	}

	// Ensure at least 1 cell on each side
	if bestIdx < 1 {
		bestIdx = 1
	}
	if bestIdx >= len(cells) {
		bestIdx = len(cells) - 1
	}
	return bestIdx
}

// interiorSplitPoint finds the optimal split index for interior cells, targeting
// ~2/3 fill on the left page. Returns the index of the middle cell that will be
// promoted as the separator. The left page gets cells[:mid], the right page
// gets cells[mid+1:], and cells[mid] is promoted to the parent.
// The returned value is always in [1, len(cells)-2] when len(cells) >= 3,
// or len(cells)/2 for smaller arrays.
func interiorSplitPoint(cells []cellData, usableSize int) int {
	if len(cells) <= 2 {
		return len(cells) / 2
	}

	// Target: fill the left page to ~2/3 of usable space.
	// Available space = usableSize - interiorHeaderSize (12 bytes).
	// Each cell also needs a 2-byte cell pointer.
	target := (usableSize - 12) * 2 / 3
	cumSize := 0
	bestIdx := len(cells) / 2 // fallback to 50/50

	for i, c := range cells {
		cellSz := interiorCellSize(c.key) + 2
		if i > 0 && cumSize+cellSz > target {
			bestIdx = i
			break
		}
		cumSize += cellSz
		if i == len(cells)-1 {
			bestIdx = i
		}
	}

	// For interior splits, the cell at bestIdx is promoted.
	// Ensure at least 1 cell on left (bestIdx >= 1) and
	// at least 1 cell on right (bestIdx <= len(cells)-2).
	if bestIdx < 1 {
		bestIdx = 1
	}
	if bestIdx > len(cells)-2 {
		bestIdx = len(cells) - 2
	}
	return bestIdx
}

// leafCellSizeWithOverflow returns the in-page size of a leaf cell, accounting for overflow.
func leafCellSizeWithOverflow(key, value []byte, usableSize int) int {
	totalPayload := len(key) + len(value)
	maxLocal := maxLocalPayload(usableSize)
	if totalPayload > maxLocal {
		localSize := localPayloadSize(totalPayload, usableSize)
		return varintSize(uint64(len(key))) + varintSize(uint64(len(value))) + localSize + overflowPtrSize
	}
	return leafCellSize(key, value)
}

// interiorCellSize returns the serialized size of an interior cell.
func interiorCellSize(key []byte) int {
	return 4 + varintSize(uint64(len(key))) + len(key)
}

// writeLeafCell writes a leaf cell to buf and returns bytes written.
func writeLeafCell(buf []byte, key, value []byte) int {
	pos := 0
	pos += putVarint(buf[pos:], uint64(len(key)))
	copy(buf[pos:], key)
	pos += len(key)
	pos += putVarint(buf[pos:], uint64(len(value)))
	copy(buf[pos:], value)
	pos += len(value)
	return pos
}

// writeLeafCellOverflow writes a leaf cell with overflow pointer.
// localVal is the portion of value stored in-page, overflowPgno is the first overflow page.
func writeLeafCellOverflow(buf []byte, key []byte, fullValLen int, localVal []byte, overflowPgno uint32) int {
	pos := 0
	pos += putVarint(buf[pos:], uint64(len(key)))
	copy(buf[pos:], key)
	pos += len(key)
	pos += putVarint(buf[pos:], uint64(fullValLen))
	copy(buf[pos:], localVal)
	pos += len(localVal)
	binary.BigEndian.PutUint32(buf[pos:], overflowPgno)
	pos += 4
	return pos
}

// writeInteriorCell writes an interior cell to buf and returns bytes written.
func writeInteriorCell(buf []byte, leftChild uint32, key []byte) int {
	binary.BigEndian.PutUint32(buf[0:4], leftChild)
	pos := 4
	pos += putVarint(buf[pos:], uint64(len(key)))
	copy(buf[pos:], key)
	pos += len(key)
	return pos
}

// searchLeafPage does binary search on a leaf page, returns the cell index
// where key should be inserted. If found, returns (index, true).
func searchLeafPage(pg *page, key []byte) (int, bool) {
	n := int(pg.header.cellCount)
	data := pg.data
	cpOff := pg.cellPointerOffset()
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		off := int(binary.BigEndian.Uint16(data[cpOff+mid*2:]))
		// Fast path: 1-byte varint for key lengths < 128
		var cellKey []byte
		b := data[off]
		if b < 0x80 {
			cellKey = data[off+1 : off+1+int(b)]
		} else {
			keyLen, vn := getVarint(data[off:])
			cellKey = data[off+vn : off+vn+int(keyLen)]
		}
		cmp := bytes.Compare(cellKey, key)
		if cmp == 0 {
			return mid, true
		}
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false
}

// interiorCellKey extracts the key from an interior cell at the given offset
// without allocating a cellData struct. Returns the key slice (pointing into
// the page buffer) and the left child page number.
func interiorCellKey(data []byte, offset int) (key []byte, leftChild uint32) {
	leftChild = binary.BigEndian.Uint32(data[offset : offset+4])
	// Fast path: 1-byte varint for key lengths < 128 (common case)
	b := data[offset+4]
	if b < 0x80 {
		keyStart := offset + 5
		return data[keyStart : keyStart+int(b)], leftChild
	}
	keyLen, n := getVarint(data[offset+4:])
	keyStart := offset + 4 + n
	return data[keyStart : keyStart+int(keyLen)], leftChild
}

// searchInteriorPage does binary search on an interior page.
// Returns the child page to descend into and the cell index.
func searchInteriorPage(pg *page, key []byte) (childPgno uint32, cellIdx int) {
	n := int(pg.header.cellCount)
	data := pg.data
	cpOff := pg.cellPointerOffset()
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		off := int(binary.BigEndian.Uint16(data[cpOff+mid*2:]))
		cellKey, _ := interiorCellKey(data, off)
		cmp := bytes.Compare(cellKey, key)
		if cmp == 0 {
			lo = mid + 1
			break
		}
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		off := int(binary.BigEndian.Uint16(data[cpOff:]))
		_, lc := interiorCellKey(data, off)
		return lc, 0
	}
	if lo < n {
		off := int(binary.BigEndian.Uint16(data[cpOff+lo*2:]))
		_, lc := interiorCellKey(data, off)
		return lc, lo
	}
	return pg.header.rightChild, n
}

// Get looks up a key in the B-tree and returns its value.
// The returned slice points directly into the page buffer and is only valid
// until the read transaction ends or any write operation occurs.
func (bt *btree) Get(key []byte) ([]byte, error) {
	maxFrame, slot, err := bt.pager.beginRead()
	if err != nil {
		return nil, err
	}
	defer bt.pager.endRead(slot)

	pg, err := bt.pager.getPageAt(bt.rootPage, maxFrame)
	if err != nil {
		return nil, err
	}
	defer bt.pager.releasePage(pg)

	usableSize := bt.usablePageSize()
	for {
		if pg.header.isLeaf() {
			idx, found := searchLeafPage(pg, key)
			if !found {
				return nil, ErrKeyNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _ := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cell.overflowPg != 0 {
				// Read full value from overflow chain
				pos := int(off)
				keyLen, kn := getVarint(pg.data[pos:])
				pos += kn + int(keyLen)
				valLen, _ := getVarint(pg.data[pos:])
				fullVal := make([]byte, int(valLen))
				copy(fullVal, cell.value)
				if err := bt.pager.readOverflowChainAt(cell.overflowPg, fullVal[len(cell.value):], bt.walMaxFrame); err != nil {
					return nil, err
				}
				return fullVal, nil
			}
			return cell.value, nil
		}

		// Interior page - descend
		childPgno, _ := searchInteriorPage(pg, key)
		bt.pager.releasePage(pg)
		pg, err = bt.pager.getPageAt(childPgno, maxFrame)
		if err != nil {
			return nil, err
		}
	}
}

// Has checks if a key exists in the B-tree.
func (bt *btree) Has(key []byte) (bool, error) {
	_, err := bt.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// maxInteriorKeySize returns the maximum key size that can fit in an interior
// cell without overflow. Our implementation does not support overflow for
// interior cells or keys, so keys must be small enough to fit entirely on-page.
// The limit is based on the usable page size: we need the interior cell
// (4-byte child ptr + varint keyLen + key) plus a 2-byte cell pointer to fit
// within the page, leaving room for the 12-byte interior header.
func maxInteriorKeySize(usableSize int) int {
	// An interior page has a 12-byte header. Each cell needs a 2-byte pointer.
	// Cell content: 4 (leftChild) + varint(keyLen) + keyLen.
	// For safety, limit key size to maxLocalPayload which is the overflow
	// threshold for index btrees. This ensures the key never needs overflow
	// in either leaf or interior cells.
	return maxLocalPayload(usableSize)
}

// Put inserts or updates a key-value pair in the B-tree.
func (bt *btree) Put(key, value []byte) error {
	// Validate key size: keys must fit in interior cells without overflow.
	// Our implementation does not support key overflow or interior cell overflow.
	pageUsable := bt.usablePageSize()
	if len(key) > maxInteriorKeySize(pageUsable) {
		return ErrKeyTooLarge
	}

	// Descend through interior pages with read-only access to find the leaf.
	// Only the leaf page (and parents on split) are dirtied, avoiding
	// unnecessary page copies for the common non-split case.
	pg, err := bt.getPage(bt.rootPage)
	if err != nil {
		return err
	}

	// Build path from root to leaf for potential split propagation.
	// Use stack-allocated array for common case (tree depth ≤ 8).
	var pathBuf [8]uint32
	path := pathBuf[:0]
	for pg.header.isInterior() {
		path = append(path, pg.pgno)
		childPgno, _ := searchInteriorPage(pg, key)
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}

	// pg is now the target leaf page — get it writable
	leafPgno := pg.pgno
	bt.pager.releasePage(pg)

	wpg, err := bt.pager.getWritablePage(leafPgno)
	if err != nil {
		return err
	}
	return bt.insertIntoLeafWithPath(wpg, key, value, path)
}

// insertIntoPage recursively inserts a key-value pair, splitting pages as needed.
// Used for splits that propagate upward (pages are already writable).
func (bt *btree) insertIntoPage(pg *page, key, value []byte) error {
	if pg.header.isLeaf() {
		return bt.insertIntoLeaf(pg, key, value)
	}
	return bt.insertIntoInterior(pg, key, value)
}

// insertIntoLeafWithPath inserts into a leaf page, using path for split propagation.
func (bt *btree) insertIntoLeafWithPath(pg *page, key, value []byte, path []uint32) error {
	idx, found := searchLeafPage(pg, key)

	if found {
		return bt.updateLeafCell(pg, idx, key, value)
	}

	pageUsable := bt.usablePageSize()
	cellSize := leafCellSizeWithOverflow(key, value, pageUsable)

	hdrSize := pg.cellPointerOffset() + int(pg.header.cellCount+1)*2
	contentStart := int(pg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	gapSpace := contentStart - hdrSize

	if cellSize+2 <= gapSpace {
		return bt.insertLeafCellAt(pg, idx, key, value)
	}

	// Check if defragmentation would free enough space.
	// Total free space = contiguous gap + fragmented bytes.
	// SQLite's allocateSpace() (btree.c ~line 1882) calls defragmentPage()
	// when the gap is insufficient but total free space is enough.
	totalFree := gapSpace + int(pg.header.fragBytes)
	if cellSize+2 <= totalFree {
		cells := bt.collectLeafCells(pg)
		if err := bt.rebuildLeafPage(pg, cells); err != nil {
			return err
		}
		return bt.insertLeafCellAt(pg, idx, key, value)
	}

	// Need to split — this will propagate up through path
	return bt.splitLeafAndInsertWithPath(pg, idx, key, value, path)
}

// insertIntoLeaf inserts into a leaf page, splitting if necessary.
func (bt *btree) insertIntoLeaf(pg *page, key, value []byte) error {
	idx, found := searchLeafPage(pg, key)

	pageUsable := bt.usablePageSize()
	cellSize := leafCellSizeWithOverflow(key, value, pageUsable)

	if found {
		// Update existing cell
		return bt.updateLeafCell(pg, idx, key, value)
	}

	// Check if there's enough contiguous space
	hdrSize := pg.cellPointerOffset() + int(pg.header.cellCount+1)*2
	contentStart := int(pg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	gapSpace := contentStart - hdrSize

	if cellSize+2 <= gapSpace { // +2 for cell pointer
		return bt.insertLeafCellAt(pg, idx, key, value)
	}

	// Check if defragmentation would free enough space
	totalFree := gapSpace + int(pg.header.fragBytes)
	if cellSize+2 <= totalFree {
		cells := bt.collectLeafCells(pg)
		if err := bt.rebuildLeafPage(pg, cells); err != nil {
			return err
		}
		return bt.insertLeafCellAt(pg, idx, key, value)
	}

	// Need to split
	return bt.splitLeafAndInsert(pg, idx, key, value)
}

// insertLeafCellAt inserts a cell at position idx in a leaf page.
// Returns an error if overflow pages need to be allocated and allocation fails.
func (bt *btree) insertLeafCellAt(pg *page, idx int, key, value []byte) error {
	pageUsable := bt.usablePageSize()
	totalPayload := len(key) + len(value)
	maxLocal := maxLocalPayload(pageUsable)

	var cellSize int
	var overflowPgno uint32

	if totalPayload > maxLocal {
		// Need overflow pages
		localSize := localPayloadSize(totalPayload, pageUsable)
		localValSize := localSize - len(key)
		if localValSize < 0 {
			localValSize = 0
		}
		overflowData := value[localValSize:]
		var err error
		overflowPgno, err = bt.pager.writeOverflowChain(overflowData)
		if err != nil {
			return err
		}
		cellSize = varintSize(uint64(len(key))) + varintSize(uint64(len(value))) + localSize + overflowPtrSize
	} else {
		cellSize = leafCellSize(key, value)
	}

	// Compute new content offset
	contentStart := int(pg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	newContentStart := contentStart - cellSize

	// Write cell data
	if overflowPgno != 0 {
		localSize := localPayloadSize(totalPayload, pageUsable)
		localValSize := localSize - len(key)
		if localValSize < 0 {
			localValSize = 0
		}
		writeLeafCellOverflow(pg.data[newContentStart:], key, len(value), value[:localValSize], overflowPgno)
	} else {
		writeLeafCell(pg.data[newContentStart:], key, value)
	}

	// Shift cell pointers to make room
	n := int(pg.header.cellCount)
	for i := n; i > idx; i-- {
		pg.setCellOffset(i, pg.getCellOffset(i-1))
	}
	pg.setCellOffset(idx, uint16(newContentStart))

	// Update header
	pg.header.cellCount++
	pg.header.cellContentOff = uint16(newContentStart)

	// Write header back to page
	off := 0
	if pg.pgno == 1 {
		off = dbHeaderSize
	}
	pg.header.serialize(pg.data[off:])
	return nil
}

// updateLeafCell updates an existing leaf cell. When the new cell fits in the
// same space as the old cell, the update is done in-place without rebuilding
// the entire page. This avoids the O(n) cost of collectLeafCells + rebuildLeafPage
// for the common case where the value size doesn't change significantly.
//
// SQLite's approach is similar: btree.c's sqlite3BtreeInsert checks if the new
// payload fits in the existing cell space before falling back to dropCell+insertCell.
func (bt *btree) updateLeafCell(pg *page, idx int, key, value []byte) error {
	usableSize := bt.usablePageSize()

	// Parse old cell to get its size and overflow info
	cellOff := int(pg.getCellOffset(idx))
	oldCell, oldCellSize := parseLeafCellWithSize(pg.data, cellOff, usableSize)

	// Free old overflow pages if the existing cell has them
	if oldCell.overflowPg != 0 {
		if err := bt.pager.freeOverflowChain(oldCell.overflowPg); err != nil {
			return err
		}
	}

	// Compute new cell size
	newCellSize := leafCellSizeWithOverflow(key, value, usableSize)

	// Fast path: if the new cell fits exactly in the old cell's space,
	// overwrite in place. This avoids the full page rebuild.
	if newCellSize <= oldCellSize {
		totalPayload := len(key) + len(value)
		maxLocal := maxLocalPayload(usableSize)

		if totalPayload > maxLocal {
			// New cell needs overflow
			localSize := localPayloadSize(totalPayload, usableSize)
			localValSize := localSize - len(key)
			if localValSize < 0 {
				localValSize = 0
			}
			overflowData := value[localValSize:]
			overflowPgno, err := bt.pager.writeOverflowChain(overflowData)
			if err != nil {
				return err
			}
			writeLeafCellOverflow(pg.data[cellOff:], key, len(value), value[:localValSize], overflowPgno)
		} else {
			writeLeafCell(pg.data[cellOff:], key, value)
		}

		// Account for wasted space as fragmentation.
		// SQLite tracks fragmentation in the page header's fragBytes field.
		waste := oldCellSize - newCellSize
		if waste > 0 {
			newFrag := int(pg.header.fragBytes) + waste
			if newFrag <= 255 {
				pg.header.fragBytes = uint8(newFrag)
				hdrOff := 0
				if pg.pgno == 1 {
					hdrOff = dbHeaderSize
				}
				pg.header.serialize(pg.data[hdrOff:])
				return nil
			}
			// Too much fragmentation — fall through to full rebuild
		} else {
			// Exact fit, no wasted space
			return nil
		}
	}

	// Slow path: new cell doesn't fit or too much fragmentation.
	// Collect all cells, replace the target, and rebuild the page.
	cells := bt.collectLeafCells(pg)
	cells[idx] = cellData{key: key, value: value}
	return bt.rebuildLeafPage(pg, cells)
}

// collectLeafCells reads all cells from a leaf page.
// Cell data is copied into a single contiguous buffer to avoid per-cell allocations.
// Overflow values are fully read from overflow pages.
func (bt *btree) collectLeafCells(pg *page) []cellData {
	n := int(pg.header.cellCount)
	cells := make([]cellData, n)
	usableSize := bt.usablePageSize()
	// Estimate actual content size from page header to avoid over-allocation.
	contentOff := int(pg.header.cellContentOff)
	if contentOff == 0 {
		contentOff = usableSize
	}
	contentSize := usableSize - contentOff
	buf := make([]byte, 0, contentSize)
	for i := range n {
		off := pg.getCellOffset(i)
		cells[i], _ = parseLeafCellWithSize(pg.data, int(off), usableSize)
		kStart := len(buf)
		buf = append(buf, cells[i].key...)

		if cells[i].overflowPg != 0 {
			// Read original valLen from the cell to compute full value size
			pos := int(off)
			keyLen, kn := getVarint(pg.data[pos:])
			pos += kn + int(keyLen)
			valLen, _ := getVarint(pg.data[pos:])

			// Reconstruct full value: local portion + overflow
			fullVal := make([]byte, int(valLen))
			copy(fullVal, cells[i].value) // local portion
			overflowSize := int(valLen) - len(cells[i].value)
			if overflowSize > 0 {
				_ = bt.pager.readOverflowChainAt(cells[i].overflowPg, fullVal[len(cells[i].value):], bt.walMaxFrame)
			}
			cells[i].key = buf[kStart:len(buf)]
			cells[i].value = fullVal
			cells[i].overflowPg = 0 // full value now in memory
		} else {
			vStart := len(buf)
			buf = append(buf, cells[i].value...)
			cells[i].key = buf[kStart:vStart]
			cells[i].value = buf[vStart:len(buf)]
		}
	}
	return cells
}

// collectInteriorCells reads all cells from an interior page.
// Cell keys are copied into a single contiguous buffer to avoid per-cell allocations.
func (bt *btree) collectInteriorCells(pg *page) []cellData {
	n := int(pg.header.cellCount)
	cells := make([]cellData, n)
	usable := bt.usablePageSize()
	contentOff := int(pg.header.cellContentOff)
	if contentOff == 0 {
		contentOff = usable
	}
	contentSize := usable - contentOff
	buf := make([]byte, 0, contentSize)
	for i := range n {
		off := pg.getCellOffset(i)
		cells[i], _ = parseInteriorCell(pg.data, int(off))
		kStart := len(buf)
		buf = append(buf, cells[i].key...)
		cells[i].key = buf[kStart:len(buf)]
	}
	return cells
}

// rebuildLeafPage rewrites a leaf page from a list of cells.
// Cells with large values will have overflow chains written automatically.
func (bt *btree) rebuildLeafPage(pg *page, cells []cellData) error {
	pageUsable := bt.usablePageSize()
	hdrOff := 0
	if pg.pgno == 1 {
		hdrOff = dbHeaderSize
	}

	// Clear the page (preserve DB header if page 1)
	if pg.pgno == 1 {
		clear(pg.data[dbHeaderSize:])
	} else {
		clear(pg.data)
	}

	pg.header.pageType = pageTypeLeafIdx
	pg.header.cellCount = uint16(len(cells))
	pg.header.firstFreeBlk = 0
	pg.header.fragBytes = 0

	maxLocal := maxLocalPayload(pageUsable)
	contentOff := pageUsable
	for i, c := range cells {
		totalPayload := len(c.key) + len(c.value)

		if totalPayload > maxLocal {
			// Need overflow
			localSize := localPayloadSize(totalPayload, pageUsable)
			localValSize := localSize - len(c.key)
			if localValSize < 0 {
				localValSize = 0
			}
			overflowData := c.value[localValSize:]
			overflowPgno, err := bt.pager.writeOverflowChain(overflowData)
			if err != nil {
				return err
			}
			size := varintSize(uint64(len(c.key))) + varintSize(uint64(len(c.value))) + localSize + overflowPtrSize
			contentOff -= size
			writeLeafCellOverflow(pg.data[contentOff:], c.key, len(c.value), c.value[:localValSize], overflowPgno)
		} else {
			size := leafCellSize(c.key, c.value)
			contentOff -= size
			writeLeafCell(pg.data[contentOff:], c.key, c.value)
		}
		// Write cell pointer
		ptrOff := hdrOff + pg.header.headerSize() + i*2
		binary.BigEndian.PutUint16(pg.data[ptrOff:], uint16(contentOff))
	}

	if contentOff < pageUsable {
		pg.header.cellContentOff = uint16(contentOff)
	} else {
		pg.header.cellContentOff = uint16(pageUsable)
	}

	pg.header.serialize(pg.data[hdrOff:])
	return nil
}

// rebuildInteriorPage rewrites an interior page from cells and a right child.
func (bt *btree) rebuildInteriorPage(pg *page, cells []cellData, rightChild uint32) {
	pageUsable := bt.usablePageSize()
	hdrOff := 0
	if pg.pgno == 1 {
		hdrOff = dbHeaderSize
	}

	if pg.pgno == 1 {
		clear(pg.data[dbHeaderSize:])
	} else {
		clear(pg.data)
	}

	pg.header.pageType = pageTypeIntIdx
	pg.header.cellCount = uint16(len(cells))
	pg.header.firstFreeBlk = 0
	pg.header.fragBytes = 0
	pg.header.rightChild = rightChild

	contentOff := pageUsable
	for i, c := range cells {
		size := interiorCellSize(c.key)
		contentOff -= size
		writeInteriorCell(pg.data[contentOff:], c.leftChild, c.key)
		ptrOff := hdrOff + pg.header.headerSize() + i*2
		binary.BigEndian.PutUint16(pg.data[ptrOff:], uint16(contentOff))
	}

	if contentOff < pageUsable {
		pg.header.cellContentOff = uint16(contentOff)
	} else {
		pg.header.cellContentOff = uint16(pageUsable)
	}

	pg.header.serialize(pg.data[hdrOff:])
}

// splitLeafAndInsertWithPath splits a leaf page using the path for parent propagation.
func (bt *btree) splitLeafAndInsertWithPath(pg *page, idx int, key, value []byte, path []uint32) error {
	cells := bt.collectLeafCells(pg)

	newCell := cellData{key: bytes.Clone(key), value: bytes.Clone(value)}
	cells = append(cells[:idx], append([]cellData{newCell}, cells[idx:]...)...)

	// Find split point targeting ~2/3 fill on the left page (SQLite-style).
	mid := leafSplitPoint(cells, bt.usablePageSize())
	leftCells := cells[:mid]
	rightCells := cells[mid:]

	sepKey := bytes.Clone(rightCells[0].key)

	rightPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	if err := bt.rebuildLeafPage(pg, leftCells); err != nil {
		return err
	}
	if err := bt.rebuildLeafPage(rightPg, rightCells); err != nil {
		return err
	}
	bt.pager.releasePage(rightPg)

	return bt.insertIntoParentWithPath(pg, sepKey, rightPg.pgno, path)
}

// insertIntoParentWithPath inserts a separator key into the parent using the tracked path.
func (bt *btree) insertIntoParentWithPath(leftPg *page, key []byte, rightPgno uint32, path []uint32) error {
	if leftPg.pgno == bt.rootPage || len(path) == 0 {
		return bt.splitRoot(leftPg, key, rightPgno)
	}

	// Get the parent page (last element of path) as writable
	parentPgno := path[len(path)-1]
	parentPath := path[:len(path)-1]

	parentPg, err := bt.pager.getWritablePage(parentPgno)
	if err != nil {
		return err
	}

	return bt.insertSepIntoInterior(parentPg, leftPg.pgno, key, rightPgno, parentPath)
}

// insertSepIntoAncestor inserts a separator key into an ancestor page identified
// by leftPgno. Unlike insertIntoParentWithPath, this takes a page number instead
// of a page pointer, avoiding use-after-release issues during recursive splits.
func (bt *btree) insertSepIntoAncestor(leftPgno uint32, key []byte, rightPgno uint32, path []uint32) error {
	if leftPgno == bt.rootPage || len(path) == 0 {
		// Need to split the root. Re-acquire as writable.
		wpg, err := bt.pager.getWritablePage(leftPgno)
		if err != nil {
			return err
		}
		err = bt.splitRoot(wpg, key, rightPgno)
		bt.pager.releasePage(wpg)
		return err
	}

	// Get the parent page (last element of path) as writable
	parentPgno := path[len(path)-1]
	parentPath := path[:len(path)-1]

	parentPg, err := bt.pager.getWritablePage(parentPgno)
	if err != nil {
		return err
	}

	return bt.insertSepIntoInterior(parentPg, leftPgno, key, rightPgno, parentPath)
}

// insertSepIntoInterior inserts a separator key into an interior page.
// This is the core logic shared by insertIntoParentWithPath and insertSepIntoAncestor.
func (bt *btree) insertSepIntoInterior(parentPg *page, leftPgno uint32, key []byte, rightPgno uint32, parentPath []uint32) error {
	// Insert separator into parent interior page
	n := int(parentPg.header.cellCount)
	cpOff := parentPg.cellPointerOffset()
	data := parentPg.data

	// Find insertion point in parent
	insertIdx := n
	for i := range n {
		off := int(binary.BigEndian.Uint16(data[cpOff+i*2:]))
		cellKey, _ := interiorCellKey(data, off)
		if bytes.Compare(cellKey, key) >= 0 {
			insertIdx = i
			break
		}
	}

	cellSize := interiorCellSize(key)
	pageUsable := bt.usablePageSize()
	hdrSize := cpOff + (n+1)*2
	contentStart := int(parentPg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	freeSpace := contentStart - hdrSize

	if cellSize+2 <= freeSpace {
		// Insert cell into parent in-place
		newContentStart := contentStart - cellSize
		writeInteriorCell(parentPg.data[newContentStart:], leftPgno, key)

		// Shift cell pointers
		for i := n; i > insertIdx; i-- {
			parentPg.setCellOffset(i, parentPg.getCellOffset(i-1))
		}
		parentPg.setCellOffset(insertIdx, uint16(newContentStart))

		if insertIdx < n {
			// The cell now at insertIdx+1 should have its leftChild set to rightPgno
			off := int(parentPg.getCellOffset(insertIdx + 1))
			binary.BigEndian.PutUint32(parentPg.data[off:off+4], rightPgno)
		} else {
			// Separator goes at the end, rightChild becomes rightPgno
			parentPg.header.rightChild = rightPgno
		}

		parentPg.header.cellCount = uint16(n + 1)
		parentPg.header.cellContentOff = uint16(newContentStart)
		hdrOff := 0
		if parentPg.pgno == 1 {
			hdrOff = dbHeaderSize
		}
		parentPg.header.serialize(parentPg.data[hdrOff:])
		bt.pager.releasePage(parentPg)
		return nil
	}

	// Parent is full — split it
	cells := bt.collectInteriorCells(parentPg)
	origRightChild := parentPg.header.rightChild
	origCellCount := len(cells)

	newCell := cellData{leftChild: leftPgno, key: bytes.Clone(key)}

	expanded := make([]cellData, 0, len(cells)+1)
	expanded = append(expanded, cells[:insertIdx]...)
	expanded = append(expanded, newCell)
	expanded = append(expanded, cells[insertIdx:]...)
	cells = expanded

	if insertIdx < len(cells)-1 {
		cells[insertIdx+1].leftChild = rightPgno
	}

	// Find split point targeting ~2/3 fill on the left page (SQLite-style).
	midIdx := interiorSplitPoint(cells, bt.usablePageSize())
	leftCells := cells[:midIdx]
	sepCellKey := bytes.Clone(cells[midIdx].key)
	rightCells := cells[midIdx+1:]

	var newRightChildForRightPg uint32
	if insertIdx == origCellCount {
		newRightChildForRightPg = rightPgno
	} else {
		newRightChildForRightPg = origRightChild
	}

	newRightPg, err := bt.pager.allocatePage()
	if err != nil {
		bt.pager.releasePage(parentPg)
		return err
	}

	bt.rebuildInteriorPage(parentPg, leftCells, cells[midIdx].leftChild)
	bt.rebuildInteriorPage(newRightPg, rightCells, newRightChildForRightPg)

	newRightPgno := newRightPg.pgno
	bt.pager.releasePage(newRightPg)

	parentPgno := parentPg.pgno
	bt.pager.releasePage(parentPg)

	return bt.insertSepIntoAncestor(parentPgno, sepCellKey, newRightPgno, parentPath)
}

// splitLeafAndInsert splits a leaf page and inserts the new key-value pair.
func (bt *btree) splitLeafAndInsert(pg *page, idx int, key, value []byte) error {
	cells := bt.collectLeafCells(pg)

	// Insert the new cell at the correct position
	newCell := cellData{key: bytes.Clone(key), value: bytes.Clone(value)}
	cells = append(cells[:idx], append([]cellData{newCell}, cells[idx:]...)...)

	// Find split point targeting ~2/3 fill on the left page (SQLite-style).
	mid := leafSplitPoint(cells, bt.usablePageSize())
	leftCells := cells[:mid]
	rightCells := cells[mid:]

	// The separator key is the first key of the right page
	sepKey := bytes.Clone(rightCells[0].key)

	// Allocate new right page
	rightPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	// Rebuild left page (reuse current page)
	if err := bt.rebuildLeafPage(pg, leftCells); err != nil {
		return err
	}

	// Build right page
	if err := bt.rebuildLeafPage(rightPg, rightCells); err != nil {
		return err
	}

	bt.pager.releasePage(rightPg)

	// Insert separator into parent
	return bt.insertIntoParent(pg, sepKey, rightPg.pgno)
}

// insertIntoParent inserts a separator key into the parent of leftPg.
// If leftPg is the root, a new root is created.
// For non-root pages, we find the parent by traversing from the root.
func (bt *btree) insertIntoParent(leftPg *page, key []byte, rightPgno uint32) error {
	if leftPg.pgno == bt.rootPage {
		return bt.splitRoot(leftPg, key, rightPgno)
	}

	// Build path from root to leftPg's parent by traversing the tree.
	// We need the path so that if the parent itself needs to split,
	// we can propagate upward.
	var pathBuf [8]uint32
	path := pathBuf[:0]
	pg, err := bt.getPage(bt.rootPage)
	if err != nil {
		return err
	}
	// Use the separator key to navigate to the parent of leftPg.
	for pg.header.isInterior() {
		childPgno, _ := searchInteriorPage(pg, key)
		if childPgno == leftPg.pgno {
			// Found: pg is the parent of leftPg
			path = append(path, pg.pgno)
			bt.pager.releasePage(pg)
			return bt.insertIntoParentWithPath(leftPg, key, rightPgno, path)
		}
		// Check if leftPg is the rightChild
		if pg.header.rightChild == leftPg.pgno {
			path = append(path, pg.pgno)
			bt.pager.releasePage(pg)
			return bt.insertIntoParentWithPath(leftPg, key, rightPgno, path)
		}
		path = append(path, pg.pgno)
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}
	bt.pager.releasePage(pg)

	// Should not reach here if the tree is consistent; fall back to splitRoot
	// as a safety net.
	return bt.splitRoot(leftPg, key, rightPgno)
}

// splitRoot creates a new root page when the current root splits.
// Modeled after SQLite's balance_deeper(): copies root content to a new child
// page, then converts the root into an interior page pointing to the new child
// and the right sibling. Handles both leaf and interior roots correctly.
func (bt *btree) splitRoot(oldRoot *page, sepKey []byte, rightChildPgno uint32) error {
	// Allocate a new page for the old root's content
	newLeftPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	// Copy old root content to new left page.
	// Detect whether the root is a leaf or interior page and use the
	// appropriate collect/rebuild path so we don't misinterpret cell formats.
	if oldRoot.header.isLeaf() {
		// Leaf root: collect leaf cells and rebuild as leaf in the new page.
		cells := bt.collectLeafCells(oldRoot)
		newLeftPg.header = oldRoot.header
		if err := bt.rebuildLeafPage(newLeftPg, cells); err != nil {
			return err
		}
	} else {
		// Interior root: collect interior cells and rebuild as interior in the
		// new page, preserving the rightChild pointer.
		cells := bt.collectInteriorCells(oldRoot)
		newLeftPg.header = oldRoot.header
		bt.rebuildInteriorPage(newLeftPg, cells, oldRoot.header.rightChild)
	}

	bt.pager.releasePage(newLeftPg)

	// Convert old root into an interior page with one cell
	cells := []cellData{
		{leftChild: newLeftPg.pgno, key: bytes.Clone(sepKey)},
	}
	bt.rebuildInteriorPage(oldRoot, cells, rightChildPgno)

	return nil
}

// insertIntoInterior handles insertion through an interior page.
func (bt *btree) insertIntoInterior(pg *page, key, value []byte) error {
	childPgno, _ := searchInteriorPage(pg, key)

	childPg, err := bt.pager.getWritablePage(childPgno)
	if err != nil {
		return err
	}

	// Save old cell count to detect splits
	oldRootPage := bt.rootPage

	err = bt.insertIntoPage(childPg, key, value)
	if err != nil {
		bt.pager.releasePage(childPg)
		return err
	}

	_ = oldRootPage
	bt.pager.releasePage(childPg)
	return nil
}

// Delete removes a key from the B-tree.
// Uses path tracking so empty leaf pages can be freed and removed from parents.
//
// Optimized to avoid full page rebuilds: instead of collectLeafCells + rebuildLeafPage,
// the cell is dropped in-place by removing the cell pointer and tracking the freed
// space as fragmentation. This is modeled after SQLite's dropCell() (btree.c line 7252)
// which calls freeSpace() to add the cell's space to the freeblock chain. Our
// simplified approach tracks freed space as fragBytes, which is reset to 0 on the
// next full rebuild (split, compaction, etc.).
//
// When fragmentation exceeds the threshold (60 bytes, matching SQLite's limit),
// a full rebuild is triggered to defragment the page.
func (bt *btree) Delete(key []byte) error {
	// Phase 1: Read-only descent to find the leaf
	pg, err := bt.getPage(bt.rootPage)
	if err != nil {
		return err
	}

	var pathBuf [8]uint32
	path := pathBuf[:0]
	for pg.header.isInterior() {
		path = append(path, pg.pgno)
		childPgno, _ := searchInteriorPage(pg, key)
		bt.pager.releasePage(pg)
		pg, err = bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}

	// Phase 2: Get leaf as writable and delete
	leafPgno := pg.pgno
	bt.pager.releasePage(pg)

	wpg, err := bt.pager.getWritablePage(leafPgno)
	if err != nil {
		return err
	}

	idx, found := searchLeafPage(wpg, key)
	if !found {
		bt.pager.releasePage(wpg)
		return ErrKeyNotFound
	}

	usableSize := bt.usablePageSize()
	cellOff := int(wpg.getCellOffset(idx))
	oldCell, oldCellSize := parseLeafCellWithSize(wpg.data, cellOff, usableSize)

	// Free overflow pages first (cell data is still in page buffer)
	if oldCell.overflowPg != 0 {
		if err := bt.pager.freeOverflowChain(oldCell.overflowPg); err != nil {
			bt.pager.releasePage(wpg)
			return err
		}
	}

	n := int(wpg.header.cellCount)

	// Check if we can do a fast in-place delete
	newFrag := int(wpg.header.fragBytes) + oldCellSize
	needsRebuild := false

	// If the cell is at the content area boundary, reclaim space directly
	contentStart := int(wpg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = usableSize
	}
	if cellOff == contentStart {
		// Cell is at the start of content area — reclaim by advancing contentStart
		newFrag -= oldCellSize
		wpg.header.cellContentOff = uint16(contentStart + oldCellSize)
	} else if newFrag > 60 {
		// SQLite's fragmentation limit is 60 bytes (btree.c pageFindSlot).
		// Too much fragmentation — need a full rebuild to defragment.
		needsRebuild = true
	}

	if needsRebuild {
		// Fall back to full rebuild (reads all cells, rewrites compacted)
		cells := bt.collectLeafCells(wpg)
		cells = append(cells[:idx], cells[idx+1:]...)
		if err := bt.rebuildLeafPage(wpg, cells); err != nil {
			bt.pager.releasePage(wpg)
			return err
		}
	} else {
		// Fast path: remove cell pointer and track freed space as fragmentation
		wpg.header.fragBytes = uint8(newFrag)
		wpg.header.cellCount = uint16(n - 1)

		// Shift cell pointers to remove the deleted entry
		for i := idx; i < n-1; i++ {
			wpg.setCellOffset(i, wpg.getCellOffset(i+1))
		}

		// Serialize updated header
		hdrOff := 0
		if wpg.pgno == 1 {
			hdrOff = dbHeaderSize
		}
		wpg.header.serialize(wpg.data[hdrOff:])
	}

	// If page is empty and not the root, free it and remove from parent
	if wpg.header.cellCount == 0 && wpg.pgno != bt.rootPage {
		bt.pager.releasePage(wpg)
		if err := bt.pager.freePage(leafPgno); err != nil {
			return err
		}
		return bt.removeChildFromParent(leafPgno, path)
	}

	bt.pager.releasePage(wpg)
	return nil
}

// leafUsedSpace returns the approximate used space on a leaf page.
func (bt *btree) leafUsedSpace(pg *page) int {
	usable := bt.usablePageSize()
	contentOff := int(pg.header.cellContentOff)
	if contentOff == 0 {
		contentOff = usable
	}
	cellPtrEnd := pg.cellPointerOffset() + int(pg.header.cellCount)*2
	return cellPtrEnd + (usable - contentOff)
}

// tryMergeLeaf attempts to merge a leaf page with a sibling.
// Only merges if both pages' content fits in a single page.
func (bt *btree) tryMergeLeaf(leafPgno uint32, path []uint32) error {
	if len(path) == 0 {
		return nil
	}

	parentPgno := path[len(path)-1]
	parentPg, err := bt.getPage(parentPgno)
	if err != nil {
		return err
	}

	// Find which child slot this leaf is in
	n := int(parentPg.header.cellCount)
	if n < 1 {
		bt.pager.releasePage(parentPg)
		return nil // need at least 2 children to merge
	}
	cpOff := parentPg.cellPointerOffset()
	childIdx := -1
	for i := range n {
		off := int(binary.BigEndian.Uint16(parentPg.data[cpOff+i*2:]))
		lc := binary.BigEndian.Uint32(parentPg.data[off : off+4])
		if lc == leafPgno {
			childIdx = i
			break
		}
	}
	if childIdx == -1 && parentPg.header.rightChild == leafPgno {
		childIdx = n // rightChild position
	}
	if childIdx == -1 {
		bt.pager.releasePage(parentPg)
		return nil
	}

	// Pick a sibling. Try right first, then left.
	var siblingPgno uint32
	mergeRight := false
	if childIdx < n {
		// This page is a leftChild entry; its right sibling is the next child
		if childIdx+1 < n {
			off := int(binary.BigEndian.Uint16(parentPg.data[cpOff+(childIdx+1)*2:]))
			siblingPgno = binary.BigEndian.Uint32(parentPg.data[off : off+4])
		} else {
			siblingPgno = parentPg.header.rightChild
		}
		mergeRight = true
	} else if n > 0 {
		// This is the rightChild; left sibling is the last cell's leftChild
		off := int(binary.BigEndian.Uint16(parentPg.data[cpOff+(n-1)*2:]))
		siblingPgno = binary.BigEndian.Uint32(parentPg.data[off : off+4])
	}
	bt.pager.releasePage(parentPg)

	if siblingPgno == 0 {
		return nil
	}

	// Read both pages
	leafPg, err := bt.getPage(leafPgno)
	if err != nil {
		return err
	}
	sibPg, err := bt.getPage(siblingPgno)
	if err != nil {
		bt.pager.releasePage(leafPg)
		return err
	}

	leafCells := bt.collectLeafCells(leafPg)
	sibCells := bt.collectLeafCells(sibPg)
	bt.pager.releasePage(leafPg)
	bt.pager.releasePage(sibPg)

	allCells := make([]cellData, 0, len(leafCells)+len(sibCells))
	if mergeRight {
		allCells = append(allCells, leafCells...)
		allCells = append(allCells, sibCells...)
	} else {
		allCells = append(allCells, sibCells...)
		allCells = append(allCells, leafCells...)
	}

	// Check if merged content fits in one page
	usableSize := bt.usablePageSize()
	totalSize := 8 // leaf header
	for _, c := range allCells {
		totalSize += leafCellSizeWithOverflow(c.key, c.value, usableSize) + 2
	}
	if totalSize > usableSize {
		return nil // doesn't fit
	}

	// Merge: keep the left page, free the right page
	var keepPgno, freePgno uint32
	if mergeRight {
		keepPgno = leafPgno
		freePgno = siblingPgno
	} else {
		keepPgno = siblingPgno
		freePgno = leafPgno
	}

	keepPg, err := bt.pager.getWritablePage(keepPgno)
	if err != nil {
		return err
	}
	if err := bt.rebuildLeafPage(keepPg, allCells); err != nil {
		bt.pager.releasePage(keepPg)
		return err
	}
	bt.pager.releasePage(keepPg)

	if err := bt.pager.freePage(freePgno); err != nil {
		return err
	}
	return bt.removeChildFromParent(freePgno, path)
}

// removeChildFromParent removes a child page reference from its parent interior page.
func (bt *btree) removeChildFromParent(childPgno uint32, path []uint32) error {
	if len(path) == 0 {
		return nil
	}

	parentPgno := path[len(path)-1]
	parentPath := path[:len(path)-1]

	parentPg, err := bt.pager.getWritablePage(parentPgno)
	if err != nil {
		return err
	}

	cells := bt.collectInteriorCells(parentPg)
	rightChild := parentPg.header.rightChild

	// Find which cell or rightChild references this child
	found := false
	for i, c := range cells {
		if c.leftChild == childPgno {
			// Remove this cell; the previous cell (or parent structure) absorbs
			cells = append(cells[:i], cells[i+1:]...)
			found = true
			break
		}
	}

	if !found && rightChild == childPgno {
		// rightChild is being removed
		if len(cells) > 0 {
			// Last cell's leftChild becomes new rightChild
			rightChild = cells[len(cells)-1].leftChild
			cells = cells[:len(cells)-1]
		}
		found = true
	}

	if !found {
		bt.pager.releasePage(parentPg)
		return nil
	}

	// If parent is now empty interior page (0 cells) and not root,
	// collapse: make the rightChild the replacement and free this page.
	if len(cells) == 0 && parentPg.pgno == bt.rootPage {
		// Root with 0 cells: collapse tree height by copying rightChild to root
		childPg, err := bt.getPage(rightChild)
		if err != nil {
			bt.pager.releasePage(parentPg)
			return err
		}
		if parentPg.pgno == 1 {
			// Preserve DB header on page 1
			copy(parentPg.data[dbHeaderSize:], childPg.data[0:])
		} else {
			copy(parentPg.data, childPg.data)
		}
		parentPg.header = childPg.header
		hdrOff := 0
		if parentPg.pgno == 1 {
			hdrOff = dbHeaderSize
		}
		parentPg.header.serialize(parentPg.data[hdrOff:])
		bt.pager.releasePage(childPg)
		bt.pager.releasePage(parentPg)
		return bt.pager.freePage(rightChild)
	}

	bt.rebuildInteriorPage(parentPg, cells, rightChild)

	if len(cells) == 0 && parentPg.pgno != bt.rootPage {
		// Non-root empty interior page: free it and remove from grandparent
		bt.pager.releasePage(parentPg)
		if err := bt.pager.freePage(parentPgno); err != nil {
			return err
		}
		return bt.removeChildFromParent(parentPgno, parentPath)
	}

	bt.pager.releasePage(parentPg)
	return nil
}

// Count returns the total number of key-value pairs in the B-tree.
// It traverses all pages but only reads page headers (cellCount),
// avoiding any key/value parsing — similar to SQLite's COUNT(*) optimization.
func (bt *btree) Count() (int, error) {
	return bt.countPage(bt.rootPage)
}

func (bt *btree) countPage(pgno uint32) (int, error) {
	pg, err := bt.getPage(pgno)
	if err != nil {
		return 0, err
	}

	if pg.header.isLeaf() {
		count := int(pg.header.cellCount)
		bt.pager.releasePage(pg)
		return count, nil
	}

	// Interior page: count all children
	n := int(pg.header.cellCount)
	children := make([]uint32, 0, n+1)
	cpOff := pg.cellPointerOffset()
	for i := range n {
		off := int(binary.BigEndian.Uint16(pg.data[cpOff+i*2:]))
		childPgno := binary.BigEndian.Uint32(pg.data[off : off+4])
		children = append(children, childPgno)
	}
	children = append(children, pg.header.rightChild)
	bt.pager.releasePage(pg)

	total := 0
	for _, child := range children {
		c, err := bt.countPage(child)
		if err != nil {
			return 0, err
		}
		total += c
	}
	return total, nil
}

// Cursor provides ordered iteration over a B-tree.
type Cursor struct {
	bt    *btree
	stack []cursorFrame
	valid bool
}

type cursorFrame struct {
	pgno    uint32
	cellIdx int
}

// First positions the cursor at the first (smallest) key.
func (c *Cursor) First() error {
	c.stack = c.stack[:0]
	c.valid = false

	pg, err := c.bt.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	// Descend to leftmost leaf
	for pg.header.isInterior() {
		if pg.header.cellCount == 0 {
			c.bt.pager.releasePage(pg)
			return nil
		}
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: 0})
		off := pg.getCellOffset(0)
		cell, _ := parseInteriorCell(pg.data, int(off))
		childPgno := cell.leftChild
		c.bt.pager.releasePage(pg)

		pg, err = c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}

	if pg.header.cellCount > 0 {
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: 0})
		c.valid = true
	}
	c.bt.pager.releasePage(pg)
	return nil
}

// Last positions the cursor at the last (largest) key.
func (c *Cursor) Last() error {
	c.stack = c.stack[:0]
	c.valid = false

	pg, err := c.bt.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	for pg.header.isInterior() {
		n := int(pg.header.cellCount)
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: n})
		childPgno := pg.header.rightChild
		c.bt.pager.releasePage(pg)

		pg, err = c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}

	n := int(pg.header.cellCount)
	if n > 0 {
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: n - 1})
		c.valid = true
	}
	c.bt.pager.releasePage(pg)
	return nil
}

// Seek positions the cursor at the first key >= the given key.
func (c *Cursor) Seek(key []byte) error {
	c.stack = c.stack[:0]
	c.valid = false

	pg, err := c.bt.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	for pg.header.isInterior() {
		childPgno, cellIdx := searchInteriorPage(pg, key)
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: cellIdx})
		c.bt.pager.releasePage(pg)

		pg, err = c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}

	idx, _ := searchLeafPage(pg, key)
	if idx < int(pg.header.cellCount) {
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: idx})
		c.valid = true
	} else {
		// Need to go to next leaf via parent
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: idx})
		c.bt.pager.releasePage(pg)
		return c.Next()
	}

	c.bt.pager.releasePage(pg)
	return nil
}

// Key returns the current key.
// The returned slice points directly into the page buffer and is only valid
// until the next cursor movement, transaction end, or any write operation.
func (c *Cursor) Key() ([]byte, error) {
	if !c.valid {
		return nil, ErrKeyNotFound
	}

	frame := c.stack[len(c.stack)-1]
	pg, err := c.bt.getPage(frame.pgno)
	if err != nil {
		return nil, err
	}
	defer c.bt.pager.releasePage(pg)

	off := pg.getCellOffset(frame.cellIdx)
	cell, _ := parseLeafCellWithSize(pg.data, int(off), c.bt.usablePageSize())
	return cell.key, nil
}

// Value returns the current value.
// For non-overflow values, the returned slice points directly into the page buffer
// and is only valid until the next cursor movement or transaction end.
// For overflow values, a new slice is allocated and returned.
func (c *Cursor) Value() ([]byte, error) {
	if !c.valid {
		return nil, ErrKeyNotFound
	}

	frame := c.stack[len(c.stack)-1]
	pg, err := c.bt.getPage(frame.pgno)
	if err != nil {
		return nil, err
	}
	defer c.bt.pager.releasePage(pg)

	usableSize := c.bt.usablePageSize()
	off := pg.getCellOffset(frame.cellIdx)
	cell, _ := parseLeafCellWithSize(pg.data, int(off), usableSize)

	if cell.overflowPg != 0 {
		// Read full valLen to compute overflow size
		pos := int(off)
		keyLen, kn := getVarint(pg.data[pos:])
		pos += kn + int(keyLen)
		valLen, _ := getVarint(pg.data[pos:])

		fullVal := make([]byte, int(valLen))
		copy(fullVal, cell.value) // local portion
		overflowSize := int(valLen) - len(cell.value)
		if overflowSize > 0 {
			if err := c.bt.pager.readOverflowChainAt(cell.overflowPg, fullVal[len(cell.value):], c.bt.walMaxFrame); err != nil {
				return nil, err
			}
		}
		return fullVal, nil
	}

	return cell.value, nil
}

// Next advances the cursor to the next key in order.
func (c *Cursor) Next() error {
	if !c.valid && len(c.stack) == 0 {
		return nil
	}

	for len(c.stack) > 0 {
		frame := &c.stack[len(c.stack)-1]

		pg, err := c.bt.getPage(frame.pgno)
		if err != nil {
			return err
		}

		if pg.header.isLeaf() {
			frame.cellIdx++
			if frame.cellIdx < int(pg.header.cellCount) {
				c.valid = true
				c.bt.pager.releasePage(pg)
				return nil
			}
			// Pop leaf and go up
			c.bt.pager.releasePage(pg)
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		// Interior page: descend to the next child
		frame.cellIdx++
		var childPgno uint32
		if frame.cellIdx < int(pg.header.cellCount) {
			off := pg.getCellOffset(frame.cellIdx)
			cell, _ := parseInteriorCell(pg.data, int(off))
			childPgno = cell.leftChild
		} else if frame.cellIdx == int(pg.header.cellCount) {
			childPgno = pg.header.rightChild
		} else {
			c.bt.pager.releasePage(pg)
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		c.bt.pager.releasePage(pg)

		// Descend to leftmost leaf of child
		childPg, err := c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
		for childPg.header.isInterior() {
			if childPg.header.cellCount == 0 {
				break
			}
			c.stack = append(c.stack, cursorFrame{pgno: childPg.pgno, cellIdx: 0})
			off := childPg.getCellOffset(0)
			cell, _ := parseInteriorCell(childPg.data, int(off))
			nextPgno := cell.leftChild
			c.bt.pager.releasePage(childPg)
			childPg, err = c.bt.getPage(nextPgno)
			if err != nil {
				return err
			}
		}

		if childPg.header.cellCount > 0 {
			c.stack = append(c.stack, cursorFrame{pgno: childPg.pgno, cellIdx: 0})
			c.valid = true
			c.bt.pager.releasePage(childPg)
			return nil
		}
		c.bt.pager.releasePage(childPg)
	}

	c.valid = false
	return nil
}

// Previous moves the cursor to the previous key in order.
// Modeled after sqlite3BtreePrevious / btreePrevious in btree.c.
// The logic mirrors Next() but in reverse: on a leaf we decrement the cell
// index; on an interior page we descend into the previous child's rightmost
// leaf.
func (c *Cursor) Previous() error {
	if !c.valid && len(c.stack) == 0 {
		return nil
	}

	for len(c.stack) > 0 {
		frame := &c.stack[len(c.stack)-1]

		pg, err := c.bt.getPage(frame.pgno)
		if err != nil {
			return err
		}

		if pg.header.isLeaf() {
			frame.cellIdx--
			if frame.cellIdx >= 0 {
				c.valid = true
				c.bt.pager.releasePage(pg)
				return nil
			}
			// Past the beginning of this leaf — pop and go up
			c.bt.pager.releasePage(pg)
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		// Interior page: descend to the previous child's rightmost leaf.
		// frame.cellIdx tracks which child subtree we just exhausted.
		// Decrement to find the previous child.
		frame.cellIdx--
		if frame.cellIdx < 0 {
			// No more children to the left at this interior level — pop up
			c.bt.pager.releasePage(pg)
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		// Get the child page at the (decremented) cellIdx position.
		var childPgno uint32
		if frame.cellIdx < int(pg.header.cellCount) {
			off := pg.getCellOffset(frame.cellIdx)
			cell, _ := parseInteriorCell(pg.data, int(off))
			childPgno = cell.leftChild
		} else if frame.cellIdx == int(pg.header.cellCount) {
			childPgno = pg.header.rightChild
		} else {
			c.bt.pager.releasePage(pg)
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		c.bt.pager.releasePage(pg)

		// Descend to the rightmost leaf of this child subtree
		childPg, err := c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
		for childPg.header.isInterior() {
			n := int(childPg.header.cellCount)
			if n == 0 {
				break
			}
			// Push frame pointing to the rightChild position (cellIdx = n)
			c.stack = append(c.stack, cursorFrame{pgno: childPg.pgno, cellIdx: n})
			nextPgno := childPg.header.rightChild
			c.bt.pager.releasePage(childPg)
			childPg, err = c.bt.getPage(nextPgno)
			if err != nil {
				return err
			}
		}

		n := int(childPg.header.cellCount)
		if n > 0 {
			// Position at the last cell of this leaf
			c.stack = append(c.stack, cursorFrame{pgno: childPg.pgno, cellIdx: n - 1})
			c.valid = true
			c.bt.pager.releasePage(childPg)
			return nil
		}
		c.bt.pager.releasePage(childPg)
	}

	c.valid = false
	return nil
}

// Valid returns true if the cursor is positioned at a valid entry.
func (c *Cursor) Valid() bool {
	return c.valid
}

// NewCursor creates a new cursor for the B-tree.
func (bt *btree) NewCursor() *Cursor {
	return &Cursor{bt: bt}
}
