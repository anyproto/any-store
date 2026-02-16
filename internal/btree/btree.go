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
	pager    *pager
	rootPage uint32
}

// cellData represents a parsed cell from a B-tree page.
type cellData struct {
	key       []byte
	value     []byte
	leftChild uint32 // only for interior pages
}

// parseLeafCell parses a leaf cell at the given offset in page data.
// Leaf cell format: varint(keyLen) | key | varint(valLen) | value
func parseLeafCell(data []byte, offset int) (cellData, int) {
	var c cellData
	pos := offset

	keyLen, n := getVarint(data[pos:])
	pos += n

	c.key = data[pos : pos+int(keyLen)]
	pos += int(keyLen)

	valLen, n := getVarint(data[pos:])
	pos += n

	c.value = data[pos : pos+int(valLen)]
	pos += int(valLen)

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

// leafCellSize returns the serialized size of a leaf cell.
func leafCellSize(key, value []byte) int {
	return varintSize(uint64(len(key))) + len(key) + varintSize(uint64(len(value))) + len(value)
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
	if err := bt.pager.beginRead(); err != nil {
		return nil, err
	}
	defer bt.pager.endRead()

	pg, err := bt.pager.getPage(bt.rootPage)
	if err != nil {
		return nil, err
	}
	defer bt.pager.releasePage(pg)

	for {
		if pg.header.isLeaf() {
			idx, found := searchLeafPage(pg, key)
			if !found {
				return nil, ErrKeyNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _ := parseLeafCell(pg.data, int(off))
			return cell.value, nil
		}

		// Interior page - descend
		childPgno, _ := searchInteriorPage(pg, key)
		bt.pager.releasePage(pg)
		pg, err = bt.pager.getPage(childPgno)
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

// Put inserts or updates a key-value pair in the B-tree.
func (bt *btree) Put(key, value []byte) error {
	// Descend through interior pages with read-only access to find the leaf.
	// Only the leaf page (and parents on split) are dirtied, avoiding
	// unnecessary page copies for the common non-split case.
	pg, err := bt.pager.getPage(bt.rootPage)
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
		pg, err = bt.pager.getPage(childPgno)
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

	cellSize := leafCellSize(key, value)
	pageUsable := int(bt.pager.pageSize)

	hdrSize := pg.cellPointerOffset() + int(pg.header.cellCount+1)*2
	contentStart := int(pg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	freeSpace := contentStart - hdrSize

	if cellSize+2 <= freeSpace {
		bt.insertLeafCellAt(pg, idx, key, value)
		return nil
	}

	// Need to split — this will propagate up through path
	return bt.splitLeafAndInsertWithPath(pg, idx, key, value, path)
}

// insertIntoLeaf inserts into a leaf page, splitting if necessary.
func (bt *btree) insertIntoLeaf(pg *page, key, value []byte) error {
	idx, found := searchLeafPage(pg, key)

	cellSize := leafCellSize(key, value)
	pageUsable := int(bt.pager.pageSize)

	if found {
		// Update existing cell
		return bt.updateLeafCell(pg, idx, key, value)
	}

	// Check if there's enough space
	hdrSize := pg.cellPointerOffset() + int(pg.header.cellCount+1)*2
	contentStart := int(pg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	freeSpace := contentStart - hdrSize

	if cellSize+2 <= freeSpace { // +2 for cell pointer
		bt.insertLeafCellAt(pg, idx, key, value)
		return nil
	}

	// Need to split
	return bt.splitLeafAndInsert(pg, idx, key, value)
}

// insertLeafCellAt inserts a cell at position idx in a leaf page.
func (bt *btree) insertLeafCellAt(pg *page, idx int, key, value []byte) {
	cellSize := leafCellSize(key, value)
	pageUsable := int(bt.pager.pageSize)

	// Compute new content offset
	contentStart := int(pg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	newContentStart := contentStart - cellSize

	// Write cell data
	writeLeafCell(pg.data[newContentStart:], key, value)

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
}

// updateLeafCell updates an existing leaf cell in place or by delete+insert.
func (bt *btree) updateLeafCell(pg *page, idx int, key, value []byte) error {
	// Simple approach: defragment and rewrite
	// For now, just rebuild the page
	cells := bt.collectLeafCells(pg)
	cells[idx] = cellData{key: key, value: value}
	bt.rebuildLeafPage(pg, cells)
	return nil
}

// collectLeafCells reads all cells from a leaf page.
// Cell data is copied into a single contiguous buffer to avoid per-cell allocations.
func (bt *btree) collectLeafCells(pg *page) []cellData {
	n := int(pg.header.cellCount)
	cells := make([]cellData, n)
	// Estimate actual content size from page header to avoid over-allocation.
	contentOff := int(pg.header.cellContentOff)
	if contentOff == 0 {
		contentOff = int(bt.pager.pageSize)
	}
	contentSize := int(bt.pager.pageSize) - contentOff
	buf := make([]byte, 0, contentSize)
	for i := range n {
		off := pg.getCellOffset(i)
		cells[i], _ = parseLeafCell(pg.data, int(off))
		kStart := len(buf)
		buf = append(buf, cells[i].key...)
		vStart := len(buf)
		buf = append(buf, cells[i].value...)
		cells[i].key = buf[kStart:vStart]
		cells[i].value = buf[vStart:len(buf)]
	}
	return cells
}

// collectInteriorCells reads all cells from an interior page.
// Cell keys are copied into a single contiguous buffer to avoid per-cell allocations.
func (bt *btree) collectInteriorCells(pg *page) []cellData {
	n := int(pg.header.cellCount)
	cells := make([]cellData, n)
	contentOff := int(pg.header.cellContentOff)
	if contentOff == 0 {
		contentOff = int(bt.pager.pageSize)
	}
	contentSize := int(bt.pager.pageSize) - contentOff
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
func (bt *btree) rebuildLeafPage(pg *page, cells []cellData) {
	pageUsable := int(bt.pager.pageSize)
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

	contentOff := pageUsable
	for i, c := range cells {
		size := leafCellSize(c.key, c.value)
		contentOff -= size
		writeLeafCell(pg.data[contentOff:], c.key, c.value)
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
}

// rebuildInteriorPage rewrites an interior page from cells and a right child.
func (bt *btree) rebuildInteriorPage(pg *page, cells []cellData, rightChild uint32) {
	pageUsable := int(bt.pager.pageSize)
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

	mid := len(cells) / 2
	leftCells := cells[:mid]
	rightCells := cells[mid:]

	sepKey := bytes.Clone(rightCells[0].key)

	rightPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	bt.rebuildLeafPage(pg, leftCells)
	bt.rebuildLeafPage(rightPg, rightCells)
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
	pageUsable := int(bt.pager.pageSize)
	hdrSize := cpOff + (n+1)*2
	contentStart := int(parentPg.header.cellContentOff)
	if contentStart == 0 {
		contentStart = pageUsable
	}
	freeSpace := contentStart - hdrSize

	if cellSize+2 <= freeSpace {
		// Insert cell into parent in-place
		newContentStart := contentStart - cellSize
		writeInteriorCell(parentPg.data[newContentStart:], leftPg.pgno, key)

		// Update the cell that previously pointed to leftPg to now point
		// to rightPgno via the rightChild update
		// Shift cell pointers
		for i := n; i > insertIdx; i-- {
			parentPg.setCellOffset(i, parentPg.getCellOffset(i-1))
		}
		parentPg.setCellOffset(insertIdx, uint16(newContentStart))

		// The rightChild of the new cell's right side:
		// If insertIdx < n, the old cell at insertIdx had leftChild pointing somewhere.
		// The new cell's leftChild = leftPg.pgno (the left split page).
		// The right side of the separator goes to rightPgno.
		// We need to update: if insertIdx == n, rightChild stays; otherwise
		// the cell at insertIdx+1's leftChild becomes rightPgno.
		if insertIdx < n {
			// The cell now at insertIdx+1 should have its leftChild set to rightPgno
			off := int(parentPg.getCellOffset(insertIdx + 1))
			binary.BigEndian.PutUint32(parentPg.data[off:off+4], rightPgno)
		} else {
			// Separator goes at the end, rightChild becomes rightPgno
			// and old rightChild was what the leftPg's child was
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

	// Parent is full — need to split it too
	cells := bt.collectInteriorCells(parentPg)
	newCell := cellData{leftChild: leftPg.pgno, key: bytes.Clone(key)}

	// Insert the new cell and fix child pointers
	cells = append(cells[:insertIdx], append([]cellData{newCell}, cells[insertIdx:]...)...)
	if insertIdx < len(cells)-1 {
		cells[insertIdx+1].leftChild = rightPgno
	}

	midIdx := len(cells) / 2
	leftCells := cells[:midIdx]
	sepCellKey := bytes.Clone(cells[midIdx].key)
	rightChildOfSep := cells[midIdx].leftChild
	_ = rightChildOfSep
	rightCells := cells[midIdx+1:]

	var rc uint32
	if insertIdx >= len(cells)-1 {
		rc = rightPgno
	} else {
		rc = parentPg.header.rightChild
	}

	newRightPg, err := bt.pager.allocatePage()
	if err != nil {
		bt.pager.releasePage(parentPg)
		return err
	}

	bt.rebuildInteriorPage(parentPg, leftCells, cells[midIdx].leftChild)
	bt.rebuildInteriorPage(newRightPg, rightCells, rc)
	bt.pager.releasePage(newRightPg)
	bt.pager.releasePage(parentPg)

	return bt.insertIntoParentWithPath(parentPg, sepCellKey, newRightPg.pgno, parentPath)
}

// splitLeafAndInsert splits a leaf page and inserts the new key-value pair.
func (bt *btree) splitLeafAndInsert(pg *page, idx int, key, value []byte) error {
	cells := bt.collectLeafCells(pg)

	// Insert the new cell at the correct position
	newCell := cellData{key: bytes.Clone(key), value: bytes.Clone(value)}
	cells = append(cells[:idx], append([]cellData{newCell}, cells[idx:]...)...)

	// Split roughly in half
	mid := len(cells) / 2
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
	bt.rebuildLeafPage(pg, leftCells)

	// Build right page
	bt.rebuildLeafPage(rightPg, rightCells)

	bt.pager.releasePage(rightPg)

	// Insert separator into parent
	return bt.insertIntoParent(pg, sepKey, rightPg.pgno)
}

// insertIntoParent inserts a separator key into the parent of leftPg.
// If leftPg is the root, a new root is created.
func (bt *btree) insertIntoParent(leftPg *page, key []byte, rightPgno uint32) error {
	if leftPg.pgno == bt.rootPage {
		return bt.splitRoot(leftPg, key, rightPgno)
	}

	// For simplicity, we track parent via a path. In this implementation,
	// we create a new root if the current page is the root.
	// For non-root splits, we need to find the parent.
	// This is handled by the recursive insert approach.
	return bt.splitRoot(leftPg, key, rightPgno)
}

// splitRoot creates a new root page when the current root splits.
func (bt *btree) splitRoot(oldRoot *page, sepKey []byte, rightChildPgno uint32) error {
	// Allocate a new page for the old root's content
	newLeftPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	// Copy old root content to new left page
	copy(newLeftPg.data, oldRoot.data)
	if oldRoot.pgno == 1 {
		// Clear DB header area in the new page (it's not page 1)
		newLeftPg.header = oldRoot.header
		bt.rebuildLeafPage(newLeftPg, bt.collectLeafCells(oldRoot))
	} else {
		newLeftPg.header = oldRoot.header
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
func (bt *btree) Delete(key []byte) error {
	pg, err := bt.pager.getWritablePage(bt.rootPage)
	if err != nil {
		return err
	}
	defer bt.pager.releasePage(pg)

	return bt.deleteFromPage(pg, key)
}

// deleteFromPage recursively deletes a key from the B-tree.
func (bt *btree) deleteFromPage(pg *page, key []byte) error {
	if pg.header.isLeaf() {
		return bt.deleteFromLeaf(pg, key)
	}

	childPgno, _ := searchInteriorPage(pg, key)
	childPg, err := bt.pager.getWritablePage(childPgno)
	if err != nil {
		return err
	}
	defer bt.pager.releasePage(childPg)

	return bt.deleteFromPage(childPg, key)
}

// deleteFromLeaf removes a key from a leaf page.
func (bt *btree) deleteFromLeaf(pg *page, key []byte) error {
	idx, found := searchLeafPage(pg, key)
	if !found {
		return ErrKeyNotFound
	}

	cells := bt.collectLeafCells(pg)
	cells = append(cells[:idx], cells[idx+1:]...)
	bt.rebuildLeafPage(pg, cells)
	return nil
}

// Count returns the total number of key-value pairs in the B-tree.
// It traverses all pages but only reads page headers (cellCount),
// avoiding any key/value parsing — similar to SQLite's COUNT(*) optimization.
func (bt *btree) Count() (int, error) {
	return bt.countPage(bt.rootPage)
}

func (bt *btree) countPage(pgno uint32) (int, error) {
	pg, err := bt.pager.getPage(pgno)
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

	pg, err := c.bt.pager.getPage(c.bt.rootPage)
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

		pg, err = c.bt.pager.getPage(childPgno)
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

	pg, err := c.bt.pager.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	for pg.header.isInterior() {
		n := int(pg.header.cellCount)
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: n})
		childPgno := pg.header.rightChild
		c.bt.pager.releasePage(pg)

		pg, err = c.bt.pager.getPage(childPgno)
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

	pg, err := c.bt.pager.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	for pg.header.isInterior() {
		childPgno, cellIdx := searchInteriorPage(pg, key)
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: cellIdx})
		c.bt.pager.releasePage(pg)

		pg, err = c.bt.pager.getPage(childPgno)
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
	pg, err := c.bt.pager.getPage(frame.pgno)
	if err != nil {
		return nil, err
	}
	defer c.bt.pager.releasePage(pg)

	off := pg.getCellOffset(frame.cellIdx)
	cell, _ := parseLeafCell(pg.data, int(off))
	return cell.key, nil
}

// Value returns the current value.
// The returned slice points directly into the page buffer and is only valid
// until the next cursor movement, transaction end, or any write operation.
func (c *Cursor) Value() ([]byte, error) {
	if !c.valid {
		return nil, ErrKeyNotFound
	}

	frame := c.stack[len(c.stack)-1]
	pg, err := c.bt.pager.getPage(frame.pgno)
	if err != nil {
		return nil, err
	}
	defer c.bt.pager.releasePage(pg)

	off := pg.getCellOffset(frame.cellIdx)
	cell, _ := parseLeafCell(pg.data, int(off))
	return cell.value, nil
}

// Next advances the cursor to the next key in order.
func (c *Cursor) Next() error {
	if !c.valid && len(c.stack) == 0 {
		return nil
	}

	for len(c.stack) > 0 {
		frame := &c.stack[len(c.stack)-1]

		pg, err := c.bt.pager.getPage(frame.pgno)
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
		childPg, err := c.bt.pager.getPage(childPgno)
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
			childPg, err = c.bt.pager.getPage(nextPgno)
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

// Valid returns true if the cursor is positioned at a valid entry.
func (c *Cursor) Valid() bool {
	return c.valid
}

// NewCursor creates a new cursor for the B-tree.
func (bt *btree) NewCursor() *Cursor {
	return &Cursor{bt: bt}
}
