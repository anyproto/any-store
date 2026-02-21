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
		if bt.walMaxFrame > 0 {
			return bt.pager.getPageAt(pgno, bt.walMaxFrame)
		}
		return bt.pager.getPage(pgno)
	}
	// Reader: always use MVCC to avoid racing with writer's writePages
	// and dirty pages in the shared cache. Safe with walMaxFrame==0 too —
	// readPageMVCC reads directly from the database file when WAL is empty.
	return bt.pager.readPageMVCC(pgno, bt.walMaxFrame)
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
func parseLeafCell(data []byte, offset int) (cellData, int, error) {
	return parseLeafCellWithSize(data, offset, 0)
}

// parseLeafCellWithSize is like parseLeafCell but uses usableSize to detect overflow.
// If usableSize is 0, overflow detection is skipped (backward compat).
func parseLeafCellWithSize(data []byte, offset int, usableSize int) (cellData, int, error) {
	var c cellData
	pos := offset
	dataLen := len(data)

	if pos >= dataLen {
		return c, 0, ErrCorrupt
	}

	keyLen, n, err := getVarintSafe(data[pos:])
	if err != nil {
		return c, 0, ErrCorrupt
	}
	pos += n

	if int(keyLen) < 0 || pos+int(keyLen) > dataLen {
		return c, 0, ErrCorrupt
	}
	c.key = data[pos : pos+int(keyLen)]
	pos += int(keyLen)

	if pos >= dataLen {
		return c, 0, ErrCorrupt
	}
	valLen, n, err := getVarintSafe(data[pos:])
	if err != nil {
		return c, 0, ErrCorrupt
	}
	pos += n

	totalPayload := int(keyLen) + int(valLen)
	maxLocal := 0
	if usableSize > 0 {
		maxLocal = maxLocalPayload(usableSize)
	}

	if usableSize > 0 && totalPayload > maxLocal {
		// Overflow cell: only localValSize bytes of value stored in-page.
		// The key is always stored fully on-page for binary search.
		localValSize := localValueSize(int(keyLen), int(valLen), usableSize)
		if pos+localValSize+4 > dataLen {
			return c, 0, ErrCorrupt
		}
		c.value = data[pos : pos+localValSize]
		pos += localValSize
		c.overflowPg = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
	} else {
		if int(valLen) < 0 || pos+int(valLen) > dataLen {
			return c, 0, ErrCorrupt
		}
		c.value = data[pos : pos+int(valLen)]
		pos += int(valLen)
	}

	return c, pos - offset, nil
}

// parseInteriorCell parses an interior cell at the given offset.
// Interior cell format: 4-byte left child | varint(keyLen) | key_local | [4-byte overflow pgno]
// When keyLen exceeds maxLocal, only localPayloadSize bytes of key are stored
// in-page and the rest is on overflow pages (matching SQLite's index btree interior cells).
// If usableSize is 0, overflow detection is skipped.
func parseInteriorCell(data []byte, offset int, usableSize ...int) (cellData, int, error) {
	var c cellData
	pos := offset
	dataLen := len(data)

	if pos+4 > dataLen {
		return c, 0, ErrCorrupt
	}
	c.leftChild = binary.BigEndian.Uint32(data[pos : pos+4])
	pos += 4

	if pos >= dataLen {
		return c, 0, ErrCorrupt
	}
	keyLen, n, err := getVarintSafe(data[pos:])
	if err != nil {
		return c, 0, ErrCorrupt
	}
	pos += n

	us := 0
	if len(usableSize) > 0 {
		us = usableSize[0]
	}

	if us > 0 && int(keyLen) > maxLocalPayload(us) {
		localSize := localPayloadSize(int(keyLen), us)
		if pos+localSize+4 > dataLen {
			return c, 0, ErrCorrupt
		}
		c.key = data[pos : pos+localSize]
		pos += localSize
		c.overflowPg = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
	} else {
		if int(keyLen) < 0 || pos+int(keyLen) > dataLen {
			return c, 0, ErrCorrupt
		}
		c.key = data[pos : pos+int(keyLen)]
		pos += int(keyLen)
	}

	return c, pos - offset, nil
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
		cellSz := interiorCellSizeWithOverflow(c.key, usableSize) + 2
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
// The key is always stored fully on-page; only the value can overflow.
func leafCellSizeWithOverflow(key, value []byte, usableSize int) int {
	totalPayload := len(key) + len(value)
	maxLocal := maxLocalPayload(usableSize)
	if totalPayload > maxLocal {
		localVal := localValueSize(len(key), len(value), usableSize)
		return varintSize(uint64(len(key))) + len(key) + varintSize(uint64(len(value))) + localVal + overflowPtrSize
	}
	return leafCellSize(key, value)
}

// interiorCellSize returns the serialized size of an interior cell (no overflow).
func interiorCellSize(key []byte) int {
	return 4 + varintSize(uint64(len(key))) + len(key)
}

// interiorCellSizeWithOverflow returns the in-page size of an interior cell,
// accounting for overflow. Matches SQLite's cellSizePtr() for index btrees.
func interiorCellSizeWithOverflow(key []byte, usableSize int) int {
	keyLen := len(key)
	maxLocal := maxLocalPayload(usableSize)
	if keyLen > maxLocal {
		localSize := localPayloadSize(keyLen, usableSize)
		return 4 + varintSize(uint64(keyLen)) + localSize + overflowPtrSize
	}
	return interiorCellSize(key)
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

// writeInteriorCellOverflow writes an interior cell with overflow pointer.
// localKey is the portion of key stored in-page, fullKeyLen is the total key length.
func writeInteriorCellOverflow(buf []byte, leftChild uint32, fullKeyLen int, localKey []byte, overflowPgno uint32) int {
	binary.BigEndian.PutUint32(buf[0:4], leftChild)
	pos := 4
	pos += putVarint(buf[pos:], uint64(fullKeyLen))
	copy(buf[pos:], localKey)
	pos += len(localKey)
	binary.BigEndian.PutUint32(buf[pos:], overflowPgno)
	pos += 4
	return pos
}

// searchLeafPage does binary search on a leaf page, returns the cell index
// where key should be inserted. If found, returns (index, true, nil).
// Returns ErrCorrupt if the page data is malformed.
func searchLeafPage(pg *page, key []byte) (int, bool, error) {
	n := int(pg.header.cellCount)
	data := pg.data
	dataLen := len(data)
	cpOff := pg.cellPointerOffset()
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		cpBase := cpOff + mid*2
		if cpBase+2 > dataLen {
			return 0, false, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpBase:]))
		if off >= dataLen {
			return 0, false, ErrCorrupt
		}
		// Fast path: 1-byte varint for key lengths < 128
		var cellKey []byte
		b := data[off]
		if b < 0x80 {
			end := off + 1 + int(b)
			if end > dataLen {
				return 0, false, ErrCorrupt
			}
			cellKey = data[off+1 : end]
		} else {
			keyLen, vn, err := getVarintSafe(data[off:])
			if err != nil {
				return 0, false, ErrCorrupt
			}
			end := off + vn + int(keyLen)
			if int(keyLen) < 0 || end > dataLen {
				return 0, false, ErrCorrupt
			}
			cellKey = data[off+vn : end]
		}
		cmp := bytes.Compare(cellKey, key)
		if cmp == 0 {
			return mid, true, nil
		}
		if cmp < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, false, nil
}

// interiorCellKey extracts the key from an interior cell at the given offset
// without allocating a cellData struct. Returns the key slice (pointing into
// the page buffer for non-overflow keys) and the left child page number.
// For keys that fit locally, the returned slice points into the page buffer.
// For overflow keys, this function cannot read the full key — use
// interiorCellFullKey instead.
func interiorCellKey(data []byte, offset int) (key []byte, leftChild uint32, err error) {
	dataLen := len(data)
	if offset+5 > dataLen {
		return nil, 0, ErrCorrupt
	}
	leftChild = binary.BigEndian.Uint32(data[offset : offset+4])
	// Fast path: 1-byte varint for key lengths < 128 (common case)
	b := data[offset+4]
	if b < 0x80 {
		keyStart := offset + 5
		keyEnd := keyStart + int(b)
		if keyEnd > dataLen {
			return nil, 0, ErrCorrupt
		}
		return data[keyStart:keyEnd], leftChild, nil
	}
	keyLen, n, verr := getVarintSafe(data[offset+4:])
	if verr != nil {
		return nil, 0, ErrCorrupt
	}
	keyStart := offset + 4 + n
	keyEnd := keyStart + int(keyLen)
	if int(keyLen) < 0 || keyEnd > dataLen {
		return nil, 0, ErrCorrupt
	}
	return data[keyStart:keyEnd], leftChild, nil
}

// interiorCellFullKey extracts the full key from an interior cell, reading
// overflow pages if necessary. Returns an allocated copy for overflow keys.
func (bt *btree) interiorCellFullKey(data []byte, offset int, usableSize int) (key []byte, leftChild uint32, err error) {
	dataLen := len(data)
	if offset+4 > dataLen {
		return nil, 0, ErrCorrupt
	}
	leftChild = binary.BigEndian.Uint32(data[offset : offset+4])
	key, err = interiorFullKey(data, offset, usableSize, bt.pager, bt.walMaxFrame)
	return key, leftChild, err
}

// searchInteriorPage is a package-level wrapper for backward compatibility
// with tests. It does not support overflow keys. Use bt.searchInterior instead.
func searchInteriorPage(pg *page, key []byte) (childPgno uint32, cellIdx int, err error) {
	n := int(pg.header.cellCount)
	data := pg.data
	dataLen := len(data)
	cpOff := pg.cellPointerOffset()
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		cpBase := cpOff + mid*2
		if cpBase+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpBase:]))
		cellKey, _, kerr := interiorCellKey(data, off)
		if kerr != nil {
			return 0, 0, kerr
		}
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
		if cpOff+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpOff:]))
		_, lc, kerr := interiorCellKey(data, off)
		if kerr != nil {
			return 0, 0, kerr
		}
		return lc, 0, nil
	}
	if lo < n {
		cpBase := cpOff + lo*2
		if cpBase+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpBase:]))
		_, lc, kerr := interiorCellKey(data, off)
		if kerr != nil {
			return 0, 0, kerr
		}
		return lc, lo, nil
	}
	return pg.header.rightChild, n, nil
}

// searchInteriorWithOverflow is a standalone function for searching interior pages
// with overflow key support. Used by ReadTx which doesn't have a btree struct.
func searchInteriorWithOverflow(pg *page, key []byte, usableSize int, p *pager, walMaxFrame uint32) (childPgno uint32, cellIdx int, err error) {
	n := int(pg.header.cellCount)
	data := pg.data
	dataLen := len(data)
	cpOff := pg.cellPointerOffset()
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		cpBase := cpOff + mid*2
		if cpBase+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpBase:]))
		cellKey, kerr := interiorFullKey(data, off, usableSize, p, walMaxFrame)
		if kerr != nil {
			return 0, 0, kerr
		}
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
		if cpOff+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpOff:]))
		if off+4 > dataLen {
			return 0, 0, ErrCorrupt
		}
		lc := binary.BigEndian.Uint32(data[off : off+4])
		return lc, 0, nil
	}
	if lo < n {
		cpBase := cpOff + lo*2
		if cpBase+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpBase:]))
		if off+4 > dataLen {
			return 0, 0, ErrCorrupt
		}
		lc := binary.BigEndian.Uint32(data[off : off+4])
		return lc, lo, nil
	}
	return pg.header.rightChild, n, nil
}

// interiorFullKey reads the full key from an interior cell, handling overflow.
func interiorFullKey(data []byte, offset int, usableSize int, p *pager, walMaxFrame uint32) ([]byte, error) {
	dataLen := len(data)
	if offset+4 >= dataLen {
		return nil, ErrCorrupt
	}
	keyLen, n, err := getVarintSafe(data[offset+4:])
	if err != nil {
		return nil, ErrCorrupt
	}
	keyStart := offset + 4 + n
	maxLocal := maxLocalPayload(usableSize)

	if int(keyLen) < 0 {
		return nil, ErrCorrupt
	}

	if int(keyLen) <= maxLocal {
		if keyStart+int(keyLen) > dataLen {
			return nil, ErrCorrupt
		}
		return data[keyStart : keyStart+int(keyLen)], nil
	}

	// Overflow: read local portion + overflow chain
	localSize := localPayloadSize(int(keyLen), usableSize)
	if keyStart+localSize+4 > dataLen {
		return nil, ErrCorrupt
	}
	fullKey := make([]byte, int(keyLen))
	copy(fullKey, data[keyStart:keyStart+localSize])
	overflowPg := binary.BigEndian.Uint32(data[keyStart+localSize : keyStart+localSize+4])
	_ = p.readOverflowChainAt(overflowPg, fullKey[localSize:], walMaxFrame)
	return fullKey, nil
}

// searchInterior does binary search on an interior page.
// Returns the child page to descend into and the cell index.
// Handles overflow keys by reading the full key from overflow pages when needed.
func (bt *btree) searchInterior(pg *page, key []byte) (childPgno uint32, cellIdx int, err error) {
	n := int(pg.header.cellCount)
	data := pg.data
	dataLen := len(data)
	cpOff := pg.cellPointerOffset()
	usableSize := bt.usablePageSize()
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		cpBase := cpOff + mid*2
		if cpBase+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpBase:]))
		cellKey, _, kerr := bt.interiorCellFullKey(data, off, usableSize)
		if kerr != nil {
			return 0, 0, kerr
		}
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
		if cpOff+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpOff:]))
		_, lc, kerr := bt.interiorCellFullKey(data, off, usableSize)
		if kerr != nil {
			return 0, 0, kerr
		}
		return lc, 0, nil
	}
	if lo < n {
		cpBase := cpOff + lo*2
		if cpBase+2 > dataLen {
			return 0, 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(data[cpBase:]))
		_, lc, kerr := bt.interiorCellFullKey(data, off, usableSize)
		if kerr != nil {
			return 0, 0, kerr
		}
		return lc, lo, nil
	}
	return pg.header.rightChild, n, nil
}

// AppendValue looks up a key in the B-tree and appends its value to buf.
// The returned slice is safe to retain after the page is released.
// Pass nil for buf to allocate a new slice (equivalent to Get).
func (bt *btree) AppendValue(key []byte, buf []byte) ([]byte, error) {
	maxFrame, slot, err := bt.pager.beginRead()
	if err != nil {
		return buf, err
	}
	defer bt.pager.endRead(slot)

	pg, err := bt.pager.getPageAt(bt.rootPage, maxFrame)
	if err != nil {
		return buf, err
	}
	defer bt.pager.releasePage(pg)

	usableSize := bt.usablePageSize()
	for {
		if pg.header.isLeaf() {
			idx, found, serr := searchLeafPage(pg, key)
			if serr != nil {
				return buf, serr
			}
			if !found {
				return buf, ErrKeyNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				return buf, cerr
			}
			if cell.overflowPg != 0 {
				// Read full value from overflow chain
				pos := int(off)
				keyLen, kn, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					return buf, ErrCorrupt
				}
				pos += kn + int(keyLen)
				valLen, _, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					return buf, ErrCorrupt
				}
				start := len(buf)
				buf = append(buf, make([]byte, int(valLen))...)
				fullVal := buf[start:]
				copy(fullVal, cell.value)
				if err := bt.pager.readOverflowChainAt(cell.overflowPg, fullVal[len(cell.value):], bt.walMaxFrame); err != nil {
					return buf[:start], err
				}
				return buf, nil
			}
			return append(buf, cell.value...), nil
		}

		// Interior page - descend
		childPgno, _, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return buf, serr
		}
		bt.pager.releasePage(pg)
		pg, err = bt.pager.getPageAt(childPgno, maxFrame)
		if err != nil {
			return buf, err
		}
	}
}

// Get looks up a key in the B-tree and returns its value.
// The returned slice is a copy and is safe to retain after the page is released.
func (bt *btree) Get(key []byte) ([]byte, error) {
	return bt.AppendValue(key, nil)
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

// maxKeySize returns the maximum key size. Since both leaf and interior cells
// support overflow (matching SQLite's index btrees), the key can be very large.
// We limit it to prevent absurd allocations. SQLite limits index keys to about
// 1/4 of SQLITE_MAX_LENGTH. We use a generous 1GB limit.
const maxKeySize = 1 << 30 // 1GB

// Put inserts or updates a key-value pair in the B-tree.
func (bt *btree) Put(key, value []byte) error {
	// Validate key size. Both leaf and interior cells support overflow,
	// so this is just a sanity limit to prevent absurd allocations.
	if len(key) > maxKeySize {
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
		childPgno, _, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return serr
		}
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
	idx, found, serr := searchLeafPage(pg, key)
	if serr != nil {
		return serr
	}

	if found {
		return bt.updateLeafCell(pg, idx, key, value, path)
	}

	pageUsable := bt.usablePageSize()
	cellSize := leafCellSizeWithOverflow(key, value, pageUsable)

	hdrSize := pg.cellPointerOffset() + int(pg.header.cellCount+1)*2
	// Validate cellContentOff before using as slice index.
	// Matches SQLite's allocateSpace() validation (btree.c lines 1843-1853).
	contentStart, err := pg.contentAreaOffset(pageUsable)
	if err != nil {
		return err
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
	idx, found, serr := searchLeafPage(pg, key)
	if serr != nil {
		return serr
	}

	pageUsable := bt.usablePageSize()
	cellSize := leafCellSizeWithOverflow(key, value, pageUsable)

	if found {
		// Update existing cell
		return bt.updateLeafCell(pg, idx, key, value, nil)
	}

	// Check if there's enough contiguous space
	hdrSize := pg.cellPointerOffset() + int(pg.header.cellCount+1)*2
	// Validate cellContentOff before using as slice index.
	// Matches SQLite's allocateSpace() validation (btree.c lines 1843-1853).
	contentStart, err := pg.contentAreaOffset(pageUsable)
	if err != nil {
		return err
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
		// Need overflow pages. Key is always fully on-page; only value overflows.
		localValSize := localValueSize(len(key), len(value), pageUsable)
		overflowData := value[localValSize:]
		var err error
		overflowPgno, err = bt.pager.writeOverflowChain(overflowData)
		if err != nil {
			return err
		}
		cellSize = varintSize(uint64(len(key))) + len(key) + varintSize(uint64(len(value))) + localValSize + overflowPtrSize
	} else {
		cellSize = leafCellSize(key, value)
	}

	// Compute new content offset.
	// Validate cellContentOff before using as slice index.
	// Matches SQLite's allocateSpace() validation (btree.c lines 1843-1853).
	contentStart, cerr := pg.contentAreaOffset(pageUsable)
	if cerr != nil {
		return cerr
	}
	newContentStart := contentStart - cellSize
	if newContentStart < 0 || newContentStart >= len(pg.data) {
		return ErrCorrupt
	}

	// Write cell data
	if overflowPgno != 0 {
		localValSize := localValueSize(len(key), len(value), pageUsable)
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
// When the new cell is larger and causes the page to overflow, the page is split
// using the path for parent propagation. This mirrors SQLite's approach in
// sqlite3BtreeInsert (btree.c): dropCell + insertCell, then balance() if the
// page overflows.
func (bt *btree) updateLeafCell(pg *page, idx int, key, value []byte, path []uint32) error {
	usableSize := bt.usablePageSize()

	// Parse old cell to get its size and overflow info
	cellOff := int(pg.getCellOffset(idx))
	oldCell, oldCellSize, cerr := parseLeafCellWithSize(pg.data, cellOff, usableSize)
	if cerr != nil {
		return cerr
	}

	// Compute new cell size
	newCellSize := leafCellSizeWithOverflow(key, value, usableSize)

	// Fast path: if the new cell fits in the old cell's space,
	// overwrite in place. This avoids the full page rebuild.
	if newCellSize <= oldCellSize {
		// Free old overflow pages — safe here because we overwrite in place
		// and won't call collectLeafCells (unless frag limit is hit below).
		if oldCell.overflowPg != 0 {
			if err := bt.pager.freeOverflowChain(oldCell.overflowPg); err != nil {
				return err
			}
		}

		totalPayload := len(key) + len(value)
		maxLocal := maxLocalPayload(usableSize)

		if totalPayload > maxLocal {
			// New cell needs overflow. Key is always fully on-page.
			localValSize := localValueSize(len(key), len(value), usableSize)
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
			// Too much fragmentation — fall through to full rebuild.
			// Note: at this point the on-page cell already has the NEW data
			// (written above), so collectLeafCells will see the new overflow
			// pgno (if any), not the old one. No double-free.
		} else {
			// Exact fit, no wasted space
			return nil
		}
	}

	// Slow path: new cell doesn't fit or too much fragmentation.
	// collectLeafCells frees all overflow chains (including old ones),
	// so we must NOT free old overflow above when taking this path.
	cells := bt.collectLeafCells(pg)
	cells[idx] = cellData{key: key, value: value}

	// Check if all cells still fit on one page after replacement.
	// This mirrors SQLite's insertCellFast (btree.c ~line 7433): if the new
	// cell doesn't fit in nFree, it sets nOverflow and defers to balance().
	pageUsable := bt.usablePageSize()
	hdrOff := 0
	if pg.pgno == 1 {
		hdrOff = dbHeaderSize
	}
	hdrSize := hdrOff + 8 + len(cells)*2 // page header + cell pointers
	totalContent := 0
	for _, c := range cells {
		totalContent += leafCellSizeWithOverflow(c.key, c.value, pageUsable)
	}

	if hdrSize+totalContent <= pageUsable {
		return bt.rebuildLeafPage(pg, cells)
	}

	// Page overflow — split and propagate to parent.
	// Mirrors SQLite's balance() called after insertCellFast detects overflow.
	mid := leafSplitPoint(cells, pageUsable)
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

	if path != nil {
		return bt.insertIntoParentWithPath(pg, sepKey, rightPg.pgno, path)
	}
	return bt.insertIntoParent(pg, sepKey, rightPg.pgno)
}

// collectLeafCells reads all cells from a leaf page.
// Cell data is copied into a single contiguous buffer to avoid per-cell allocations.
// Overflow values are fully read from overflow pages, and the old overflow chains
// are freed (since the caller will rebuild the page with new overflow chains).
func (bt *btree) collectLeafCells(pg *page) []cellData {
	n := int(pg.header.cellCount)
	cells := make([]cellData, n)
	usableSize := bt.usablePageSize()
	// Estimate actual content size from page header to avoid over-allocation.
	// Validate contentOff to avoid negative capacity from corrupted headers.
	// Matches SQLite's allocateSpace() validation (btree.c lines 1843-1853).
	contentOff, coErr := pg.contentAreaOffset(usableSize)
	if coErr != nil {
		contentOff = usableSize
	}
	contentSize := usableSize - contentOff
	if contentSize < 0 {
		contentSize = 0
	}
	buf := make([]byte, 0, contentSize)
	for i := range n {
		off := pg.getCellOffset(i)
		cells[i], _, _ = parseLeafCellWithSize(pg.data, int(off), usableSize)
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
			// Free the old overflow chain — rebuildLeafPage will create new ones.
			_ = bt.pager.freeOverflowChain(cells[i].overflowPg)
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
// For cells with overflow keys, the full key is read from overflow pages
// and the overflow chain is freed (since the caller will rebuild the page).
func (bt *btree) collectInteriorCells(pg *page) []cellData {
	n := int(pg.header.cellCount)
	cells := make([]cellData, n)
	usable := bt.usablePageSize()
	maxLocal := maxLocalPayload(usable)
	for i := range n {
		off := pg.getCellOffset(i)
		cells[i], _, _ = parseInteriorCell(pg.data, int(off), usable)
		if cells[i].overflowPg != 0 {
			// Read full key from overflow pages
			pos := int(off) + 4
			keyLen, kn := getVarint(pg.data[pos:])
			pos += kn
			localSize := localPayloadSize(int(keyLen), usable)
			fullKey := make([]byte, int(keyLen))
			copy(fullKey, pg.data[pos:pos+localSize])
			overflowPg := binary.BigEndian.Uint32(pg.data[pos+localSize : pos+localSize+4])
			_ = bt.pager.readOverflowChainAt(overflowPg, fullKey[localSize:], bt.walMaxFrame)
			_ = bt.pager.freeOverflowChain(overflowPg)
			cells[i].key = fullKey
			cells[i].overflowPg = 0
		} else {
			cells[i].key = bytes.Clone(cells[i].key)
		}
		_ = maxLocal
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
			// Need overflow. Key is always fully on-page.
			localValSize := localValueSize(len(c.key), len(c.value), pageUsable)
			overflowData := c.value[localValSize:]
			overflowPgno, err := bt.pager.writeOverflowChain(overflowData)
			if err != nil {
				return err
			}
			size := varintSize(uint64(len(c.key))) + len(c.key) + varintSize(uint64(len(c.value))) + localValSize + overflowPtrSize
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
// Keys that exceed maxLocal are written with overflow chains.
func (bt *btree) rebuildInteriorPage(pg *page, cells []cellData, rightChild uint32) error {
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

	maxLocal := maxLocalPayload(pageUsable)
	contentOff := pageUsable
	for i, c := range cells {
		keyLen := len(c.key)
		if keyLen > maxLocal {
			localSize := localPayloadSize(keyLen, pageUsable)
			overflowData := c.key[localSize:]
			overflowPgno, err := bt.pager.writeOverflowChain(overflowData)
			if err != nil {
				return err
			}
			size := 4 + varintSize(uint64(keyLen)) + localSize + overflowPtrSize
			contentOff -= size
			writeInteriorCellOverflow(pg.data[contentOff:], c.leftChild, keyLen, c.key[:localSize], overflowPgno)
		} else {
			size := interiorCellSize(c.key)
			contentOff -= size
			writeInteriorCell(pg.data[contentOff:], c.leftChild, c.key)
		}
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
	pageUsable := bt.usablePageSize()
	insertIdx := n
	for i := range n {
		off := int(binary.BigEndian.Uint16(data[cpOff+i*2:]))
		cellKey, _, _ := bt.interiorCellFullKey(data, off, pageUsable)
		if bytes.Compare(cellKey, key) >= 0 {
			insertIdx = i
			break
		}
	}

	cellSize := interiorCellSizeWithOverflow(key, pageUsable)
	hdrSize := cpOff + (n+1)*2
	// Validate cellContentOff before using as slice index.
	// Matches SQLite's allocateSpace() validation (btree.c lines 1843-1853).
	contentStart, cerr := parentPg.contentAreaOffset(pageUsable)
	if cerr != nil {
		bt.pager.releasePage(parentPg)
		return cerr
	}
	freeSpace := contentStart - hdrSize

	if cellSize+2 <= freeSpace {
		// Insert cell into parent in-place
		maxLocal := maxLocalPayload(pageUsable)
		newContentStart := contentStart - cellSize
		if newContentStart < 0 || newContentStart >= len(parentPg.data) {
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
		if len(key) > maxLocal {
			localSize := localPayloadSize(len(key), pageUsable)
			overflowData := key[localSize:]
			overflowPgno, err := bt.pager.writeOverflowChain(overflowData)
			if err != nil {
				bt.pager.releasePage(parentPg)
				return err
			}
			writeInteriorCellOverflow(parentPg.data[newContentStart:], leftPgno, len(key), key[:localSize], overflowPgno)
		} else {
			writeInteriorCell(parentPg.data[newContentStart:], leftPgno, key)
		}

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

	if err := bt.rebuildInteriorPage(parentPg, leftCells, cells[midIdx].leftChild); err != nil {
		bt.pager.releasePage(newRightPg)
		bt.pager.releasePage(parentPg)
		return err
	}
	if err := bt.rebuildInteriorPage(newRightPg, rightCells, newRightChildForRightPg); err != nil {
		bt.pager.releasePage(newRightPg)
		bt.pager.releasePage(parentPg)
		return err
	}

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
		childPgno, _, _ := bt.searchInterior(pg, key)
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
		if err := bt.rebuildInteriorPage(newLeftPg, cells, oldRoot.header.rightChild); err != nil {
			return err
		}
	}

	bt.pager.releasePage(newLeftPg)

	// Convert old root into an interior page with one cell
	cells := []cellData{
		{leftChild: newLeftPg.pgno, key: bytes.Clone(sepKey)},
	}
	return bt.rebuildInteriorPage(oldRoot, cells, rightChildPgno)
}

// insertIntoInterior handles insertion through an interior page.
func (bt *btree) insertIntoInterior(pg *page, key, value []byte) error {
	childPgno, _, serr := bt.searchInterior(pg, key)
	if serr != nil {
		return serr
	}

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
		childPgno, _, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return serr
		}
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

	idx, found, serr := searchLeafPage(wpg, key)
	if serr != nil {
		bt.pager.releasePage(wpg)
		return serr
	}
	if !found {
		bt.pager.releasePage(wpg)
		return ErrKeyNotFound
	}

	usableSize := bt.usablePageSize()
	cellOff := int(wpg.getCellOffset(idx))
	oldCell, oldCellSize, cerr := parseLeafCellWithSize(wpg.data, cellOff, usableSize)
	if cerr != nil {
		bt.pager.releasePage(wpg)
		return cerr
	}

	n := int(wpg.header.cellCount)

	// Check if we can do a fast in-place delete
	newFrag := int(wpg.header.fragBytes) + oldCellSize
	needsRebuild := false

	// If the cell is at the content area boundary, reclaim space directly.
	// Validate cellContentOff before using.
	// Matches SQLite's allocateSpace() validation (btree.c lines 1843-1853).
	contentStart, coErr := wpg.contentAreaOffset(usableSize)
	if coErr != nil {
		bt.pager.releasePage(wpg)
		return coErr
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
		// Fall back to full rebuild — collectLeafCells frees all overflow chains
		// (including the deleted cell's), so we must NOT free them above.
		cells := bt.collectLeafCells(wpg)
		cells = append(cells[:idx], cells[idx+1:]...)
		if err := bt.rebuildLeafPage(wpg, cells); err != nil {
			bt.pager.releasePage(wpg)
			return err
		}
	} else {
		// Fast path: free deleted cell's overflow before removing
		if oldCell.overflowPg != 0 {
			if err := bt.pager.freeOverflowChain(oldCell.overflowPg); err != nil {
				bt.pager.releasePage(wpg)
				return err
			}
		}
		// Remove cell pointer and track freed space as fragmentation
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
	// Validate contentOff to avoid computing with corrupted headers.
	contentOff, err := pg.contentAreaOffset(usable)
	if err != nil {
		return usable // treat corrupted page as fully used
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

	if err := bt.rebuildInteriorPage(parentPg, cells, rightChild); err != nil {
		bt.pager.releasePage(parentPg)
		return err
	}

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
	dataLen := len(pg.data)
	for i := range n {
		cpBase := cpOff + i*2
		if cpBase+2 > dataLen {
			bt.pager.releasePage(pg)
			return 0, ErrCorrupt
		}
		off := int(binary.BigEndian.Uint16(pg.data[cpBase:]))
		if off+4 > dataLen {
			bt.pager.releasePage(pg)
			return 0, ErrCorrupt
		}
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

// btCursorMaxDepth is the maximum depth of the cursor stack.
// Matches SQLite's BTCURSOR_MAX_DEPTH (btreeInt.h). Any B-tree deeper than
// this is considered corrupt. This limit also catches circular page references
// that would otherwise cause infinite loops during cursor traversal.
const btCursorMaxDepth = 20

// Cursor provides ordered iteration over a B-tree.
// The leaf frame keeps its page pinned (pg != nil) so that Key/Value can
// read directly from the page buffer without re-acquiring it. Interior
// frames release their pages after extracting child pointers. Close()
// must be called when the cursor is no longer needed.
type Cursor struct {
	bt    *btree
	stack []cursorFrame
	valid bool
}

type cursorFrame struct {
	pgno    uint32
	cellIdx int
	pg      *page // pinned page (non-nil only for the leaf frame)
}

// Close releases all pinned pages and invalidates the cursor.
func (c *Cursor) Close() {
	c.releasePages()
	c.valid = false
}

// releasePages releases all pinned pages in the cursor stack.
func (c *Cursor) releasePages() {
	for i := range c.stack {
		if c.stack[i].pg != nil {
			c.bt.pager.releasePage(c.stack[i].pg)
			c.stack[i].pg = nil
		}
	}
}

// First positions the cursor at the first (smallest) key.
func (c *Cursor) First() error {
	c.releasePages()
	c.stack = c.stack[:0]
	c.valid = false

	pg, err := c.bt.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	// Descend to leftmost leaf (mirrors SQLite's moveToLeftmost via moveToChild).
	for pg.header.isInterior() {
		if pg.header.cellCount == 0 {
			c.bt.pager.releasePage(pg)
			return nil
		}
		if len(c.stack) >= btCursorMaxDepth-1 {
			c.bt.pager.releasePage(pg)
			return ErrCorrupt
		}
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: 0})
		off := int(pg.getCellOffset(0))
		if off+4 > len(pg.data) {
			c.bt.pager.releasePage(pg)
			return ErrCorrupt
		}
		childPgno := binary.BigEndian.Uint32(pg.data[off : off+4])
		c.bt.pager.releasePage(pg)

		pg, err = c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}

	if pg.header.cellCount > 0 {
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: 0, pg: pg})
		c.valid = true
	} else {
		c.bt.pager.releasePage(pg)
	}
	return nil
}

// Last positions the cursor at the last (largest) key.
func (c *Cursor) Last() error {
	c.releasePages()
	c.stack = c.stack[:0]
	c.valid = false

	pg, err := c.bt.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	// Descend to rightmost leaf (mirrors SQLite's moveToRightmost via moveToChild).
	for pg.header.isInterior() {
		n := int(pg.header.cellCount)
		if len(c.stack) >= btCursorMaxDepth-1 {
			c.bt.pager.releasePage(pg)
			return ErrCorrupt
		}
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
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: n - 1, pg: pg})
		c.valid = true
	} else {
		c.bt.pager.releasePage(pg)
	}
	return nil
}

// Seek positions the cursor at the first key >= the given key.
func (c *Cursor) Seek(key []byte) error {
	c.releasePages()
	c.stack = c.stack[:0]
	c.valid = false

	pg, err := c.bt.getPage(c.bt.rootPage)
	if err != nil {
		return err
	}

	for pg.header.isInterior() {
		childPgno, cellIdx, serr := c.bt.searchInterior(pg, key)
		if serr != nil {
			c.bt.pager.releasePage(pg)
			return serr
		}
		if len(c.stack) >= btCursorMaxDepth-1 {
			c.bt.pager.releasePage(pg)
			return ErrCorrupt
		}
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: cellIdx})
		c.bt.pager.releasePage(pg)

		pg, err = c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
	}

	idx, _, serr := searchLeafPage(pg, key)
	if serr != nil {
		c.bt.pager.releasePage(pg)
		return serr
	}
	if idx < int(pg.header.cellCount) {
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: idx, pg: pg})
		c.valid = true
	} else {
		// Need to go to next leaf via parent
		c.stack = append(c.stack, cursorFrame{pgno: pg.pgno, cellIdx: idx})
		c.bt.pager.releasePage(pg)
		return c.Next()
	}

	return nil
}

// Key returns the current key.
// The returned slice points directly into the pinned page buffer and is valid
// until the next cursor movement or Close(). Equivalent to sqlite3BtreePayloadFetch.
func (c *Cursor) Key() ([]byte, error) {
	if !c.valid {
		return nil, ErrKeyNotFound
	}

	frame := &c.stack[len(c.stack)-1]
	if frame.pg == nil {
		return nil, ErrCorrupt
	}

	off, oerr := frame.pg.getCellOffsetSafe(frame.cellIdx)
	if oerr != nil {
		return nil, oerr
	}
	cell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(off), c.bt.usablePageSize())
	if cerr != nil {
		return nil, cerr
	}
	return cell.key, nil
}

// Value returns the current value.
// For non-overflow values, the returned slice points directly into the pinned
// page buffer and is valid until the next cursor movement or Close().
// For overflow values, a new slice is allocated and returned.
func (c *Cursor) Value() ([]byte, error) {
	if !c.valid {
		return nil, ErrKeyNotFound
	}

	frame := &c.stack[len(c.stack)-1]
	if frame.pg == nil {
		return nil, ErrCorrupt
	}

	usableSize := c.bt.usablePageSize()
	off, oerr := frame.pg.getCellOffsetSafe(frame.cellIdx)
	if oerr != nil {
		return nil, oerr
	}
	cell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(off), usableSize)
	if cerr != nil {
		return nil, cerr
	}

	if cell.overflowPg != 0 {
		// Read full valLen to compute overflow size
		pos := int(off)
		keyLen, kn, verr := getVarintSafe(frame.pg.data[pos:])
		if verr != nil {
			return nil, ErrCorrupt
		}
		pos += kn + int(keyLen)
		valLen, _, verr := getVarintSafe(frame.pg.data[pos:])
		if verr != nil {
			return nil, ErrCorrupt
		}

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

		// Leaf frame: the page is pinned in frame.pg.
		if frame.pg != nil {
			frame.cellIdx++
			if frame.cellIdx < int(frame.pg.header.cellCount) {
				c.valid = true
				return nil
			}
			// Past end of this leaf — release pinned page, pop frame, go up.
			c.bt.pager.releasePage(frame.pg)
			frame.pg = nil
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		// Interior frame: re-acquire page to read child pointers.
		pg, err := c.bt.getPage(frame.pgno)
		if err != nil {
			return err
		}

		frame.cellIdx++
		var childPgno uint32
		if frame.cellIdx < int(pg.header.cellCount) {
			off := int(pg.getCellOffset(frame.cellIdx))
			if off+4 > len(pg.data) {
				c.bt.pager.releasePage(pg)
				return ErrCorrupt
			}
			childPgno = binary.BigEndian.Uint32(pg.data[off : off+4])
		} else if frame.cellIdx == int(pg.header.cellCount) {
			childPgno = pg.header.rightChild
		} else {
			c.bt.pager.releasePage(pg)
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		c.bt.pager.releasePage(pg)

		// Descend to leftmost leaf of child (mirrors SQLite's moveToLeftmost).
		childPg, err := c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
		for childPg.header.isInterior() {
			if childPg.header.cellCount == 0 {
				break
			}
			if len(c.stack) >= btCursorMaxDepth-1 {
				c.bt.pager.releasePage(childPg)
				return ErrCorrupt
			}
			c.stack = append(c.stack, cursorFrame{pgno: childPg.pgno, cellIdx: 0})
			off := int(childPg.getCellOffset(0))
			if off+4 > len(childPg.data) {
				c.bt.pager.releasePage(childPg)
				return ErrCorrupt
			}
			nextPgno := binary.BigEndian.Uint32(childPg.data[off : off+4])
			c.bt.pager.releasePage(childPg)
			childPg, err = c.bt.getPage(nextPgno)
			if err != nil {
				return err
			}
		}

		if childPg.header.cellCount > 0 {
			c.stack = append(c.stack, cursorFrame{pgno: childPg.pgno, cellIdx: 0, pg: childPg})
			c.valid = true
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

		// Leaf frame: the page is pinned in frame.pg.
		if frame.pg != nil {
			frame.cellIdx--
			if frame.cellIdx >= 0 {
				c.valid = true
				return nil
			}
			// Past the beginning of this leaf — release pinned page, pop, go up.
			c.bt.pager.releasePage(frame.pg)
			frame.pg = nil
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		// Interior frame: re-acquire page to read child pointers.
		pg, err := c.bt.getPage(frame.pgno)
		if err != nil {
			return err
		}

		// Descend to the previous child's rightmost leaf.
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
			off := int(pg.getCellOffset(frame.cellIdx))
			if off+4 > len(pg.data) {
				c.bt.pager.releasePage(pg)
				return ErrCorrupt
			}
			childPgno = binary.BigEndian.Uint32(pg.data[off : off+4])
		} else if frame.cellIdx == int(pg.header.cellCount) {
			childPgno = pg.header.rightChild
		} else {
			c.bt.pager.releasePage(pg)
			c.stack = c.stack[:len(c.stack)-1]
			continue
		}

		c.bt.pager.releasePage(pg)

		// Descend to the rightmost leaf of this child subtree (mirrors SQLite's moveToRightmost).
		childPg, err := c.bt.getPage(childPgno)
		if err != nil {
			return err
		}
		for childPg.header.isInterior() {
			n := int(childPg.header.cellCount)
			if n == 0 {
				break
			}
			if len(c.stack) >= btCursorMaxDepth-1 {
				c.bt.pager.releasePage(childPg)
				return ErrCorrupt
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
			c.stack = append(c.stack, cursorFrame{pgno: childPg.pgno, cellIdx: n - 1, pg: childPg})
			c.valid = true
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
