package btree

// B-tree implementation modeled after SQLite's btree.c.
// Uses index-style B-tree where both keys and values are stored as byte slices.
// Keys are sorted in byte order. Each cell stores: key-length | key | value-length | value.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"sync/atomic"
)

// debugOverflowReadErrors, when non-zero, causes collectLeafCells and
// collectInteriorCells to panic when readOverflowChainAt returns an error,
// rather than silently ignoring it. Set via atomic for test use only.
var debugOverflowReadErrors atomic.Int32

// SetDebugOverflowReadErrors enables or disables the debug mode that panics
// on overflow read errors in collectLeafCells/collectInteriorCells.
func SetDebugOverflowReadErrors(enabled bool) {
	if enabled {
		debugOverflowReadErrors.Store(1)
	} else {
		debugOverflowReadErrors.Store(0)
	}
}

// btree represents a single B-tree (one namespace).
type btree struct {
	pager       *pager
	cache       *pcache // per-reader private page cache (nil for writers)
	rootPage    uint32
	walMaxFrame uint32 // WAL snapshot for this operation (0 = use pager default)
	writable    bool   // true if this btree is used by a write transaction
}

// getPage returns a page using this btree's walMaxFrame for snapshot isolation.
func (bt *btree) getPage(pgno uint32) (*page, error) {
	if bt.writable {
		// Use pager.getPage which uses wal.nFrame (not the frozen
		// bt.walMaxFrame) so the writer sees its own spilled pages.
		return bt.pager.getPage(pgno)
	}
	// Reader: use private cache for snapshot isolation. Falls back to
	// uncached reads when bt.cache is nil.
	return bt.pager.getPageReader(pgno, bt.walMaxFrame, bt.cache)
}

// usablePageSize returns the usable page size, accounting for reserved space.
func (bt *btree) usablePageSize() int {
	return bt.pager.usableSize()
}

// cellData represents a parsed cell from a B-tree page.
type cellData struct {
	key        []byte
	value      []byte
	leftChild  uint32 // only for interior pages
	overflowPg uint32 // overflow page number (0 = no overflow)
	rawCell    []byte // raw on-page cell bytes for passthrough during splits
}

// parseLeafCell parses a leaf cell at the given offset in page data.
// Leaf cell format (v5): [varint(keyLen)] [varint(valLen)] [key||value] [4-byte overflow?]
// When usableSize is 0 (via this wrapper), overflow detection is skipped.
func parseLeafCell(data []byte, offset int) (cellData, int, error) {
	return parseLeafCellWithSize(data, offset, 0)
}

// parseLeafCellWithSize is like parseLeafCell but uses usableSize to detect overflow.
// If usableSize is 0, overflow detection is skipped.
//
// IMPORTANT: For overflow cells, c.key and c.value are the LOCAL portions only.
// c.key may be a prefix (not the full key). Callers needing the full key must
// check c.overflowPg != 0 and use leafFullKey.
func parseLeafCellWithSize(data []byte, offset int, usableSize int) (cellData, int, error) {
	var c cellData
	pos := offset
	dataLen := len(data)

	if pos >= dataLen {
		return c, 0, ErrCorrupt
	}

	// Read keyLen varint
	keyLen, n, err := getVarintSafe(data[pos:])
	if err != nil {
		return c, 0, ErrCorrupt
	}
	pos += n

	if pos >= dataLen {
		return c, 0, ErrCorrupt
	}

	// Read valLen varint (immediately after keyLen in new format)
	valLen, n, err := getVarintSafe(data[pos:])
	if err != nil {
		return c, 0, ErrCorrupt
	}
	pos += n

	if int(keyLen) < 0 || int(keyLen) > maxPayloadAlloc || int(valLen) < 0 || int(valLen) > maxPayloadAlloc {
		return c, 0, ErrCorrupt
	}
	totalPayload := int(keyLen) + int(valLen)
	if totalPayload < 0 || totalPayload > maxPayloadAlloc {
		return c, 0, ErrCorrupt
	}
	maxLocal := 0
	if usableSize > 0 {
		maxLocal = maxLocalPayload(usableSize)
	}

	if usableSize > 0 && totalPayload > maxLocal {
		// Overflow cell: payload is (key||value) contiguous blob,
		// only nLocal bytes stored on-page.
		nLocal := localPayloadSize(totalPayload, usableSize)
		if pos+nLocal+4 > dataLen {
			return c, 0, ErrCorrupt
		}
		// Distribute local bytes between key and value
		localKeyBytes := min(nLocal, int(keyLen))
		localValBytes := nLocal - localKeyBytes
		c.key = data[pos : pos+localKeyBytes]
		pos += localKeyBytes
		if localValBytes > 0 {
			c.value = data[pos : pos+localValBytes]
			pos += localValBytes
		}
		c.overflowPg = binary.BigEndian.Uint32(data[pos : pos+4])
		pos += 4
	} else {
		if int(keyLen) < 0 || pos+int(keyLen) > dataLen {
			return c, 0, ErrCorrupt
		}
		c.key = data[pos : pos+int(keyLen)]
		pos += int(keyLen)
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

// leafCellSize returns the serialized size of a leaf cell (no overflow).
// Format: [varint(keyLen)] [varint(valLen)] [key] [value]
func leafCellSize(key, value []byte) int {
	return varintSize(uint64(len(key))) + varintSize(uint64(len(value))) + len(key) + len(value)
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
		var cellSz int
		if c.rawCell != nil {
			cellSz = len(c.rawCell) + 2 // +2 for cell pointer
		} else {
			cellSz = leafCellSizeWithOverflow(c.key, c.value, usableSize) + 2
		}
		if i == 0 {
			// always put at least 1 cell on left
			cumSize += cellSz
			continue
		}
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
// The payload (key||value) is treated as a single blob for overflow purposes.
func leafCellSizeWithOverflow(key, value []byte, usableSize int) int {
	totalPayload := len(key) + len(value)
	hdr := varintSize(uint64(len(key))) + varintSize(uint64(len(value)))
	maxLocal := maxLocalPayload(usableSize)
	if totalPayload > maxLocal {
		nLocal := localPayloadSize(totalPayload, usableSize)
		return hdr + nLocal + overflowPtrSize
	}
	return hdr + totalPayload
}

// leafCellSizeFromLengths returns the in-page size of a leaf cell from key/value lengths.
// Same as leafCellSizeWithOverflow but takes int lengths instead of byte slices.
func leafCellSizeFromLengths(keyLen, valLen, usableSize int) int {
	totalPayload := keyLen + valLen
	hdr := varintSize(uint64(keyLen)) + varintSize(uint64(valLen))
	maxLocal := maxLocalPayload(usableSize)
	if totalPayload > maxLocal {
		nLocal := localPayloadSize(totalPayload, usableSize)
		return hdr + nLocal + overflowPtrSize
	}
	return hdr + totalPayload
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
// Format: [varint(keyLen)] [varint(valLen)] [key] [value]
func writeLeafCell(buf []byte, key, value []byte) int {
	pos := 0
	pos += putVarint(buf[pos:], uint64(len(key)))
	pos += putVarint(buf[pos:], uint64(len(value)))
	copy(buf[pos:], key)
	pos += len(key)
	copy(buf[pos:], value)
	pos += len(value)
	return pos
}

// writeLeafCellOverflow writes a leaf cell with overflow.
// nLocal is the number of bytes of (key||value) stored on-page.
// Matches SQLite's fillInCell() for index btrees, adapted for
// separate key/value varints (see format documentation in page.go).
func writeLeafCellOverflow(buf []byte, key []byte, value []byte, nLocal int, overflowPgno uint32) int {
	pos := 0
	pos += putVarint(buf[pos:], uint64(len(key)))
	pos += putVarint(buf[pos:], uint64(len(value)))
	// Write first nLocal bytes of (key || value)
	localKeyBytes := min(nLocal, len(key))
	copy(buf[pos:], key[:localKeyBytes])
	pos += localKeyBytes
	localValBytes := nLocal - localKeyBytes
	if localValBytes > 0 {
		copy(buf[pos:], value[:localValBytes])
		pos += localValBytes
	}
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
//
// This function does NOT support overflow keys (it reads the local key prefix).
// For pages that may have overflow keys, use bt.searchLeaf or
// searchLeafWithOverflow instead. For non-overflow cells (the common case),
// this is the fastest path.
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
		// New format: [varint(keyLen)] [varint(valLen)] [key||value...]
		// Fast path: 1-byte keyLen varint (< 128)
		var cellKey []byte
		b := data[off]
		if b < 0x80 {
			kl := int(b)
			// Skip valLen varint (1-byte fast path)
			valOff := off + 1
			if valOff >= dataLen {
				return 0, false, ErrCorrupt
			}
			if data[valOff] < 0x80 {
				// Both varints are 1-byte — common fast path
				keyStart := valOff + 1
				end := keyStart + kl
				if end > dataLen {
					return 0, false, ErrCorrupt
				}
				cellKey = data[keyStart:end]
			} else {
				// Multi-byte valLen varint
				_, vn, verr := getVarintSafe(data[valOff:])
				if verr != nil {
					return 0, false, ErrCorrupt
				}
				keyStart := valOff + vn
				end := keyStart + kl
				if end > dataLen {
					return 0, false, ErrCorrupt
				}
				cellKey = data[keyStart:end]
			}
		} else {
			keyLen, kn, err := getVarintSafe(data[off:])
			if err != nil {
				return 0, false, ErrCorrupt
			}
			pos := off + kn
			if pos >= dataLen {
				return 0, false, ErrCorrupt
			}
			_, vn, verr := getVarintSafe(data[pos:])
			if verr != nil {
				return 0, false, ErrCorrupt
			}
			keyStart := pos + vn
			end := keyStart + int(keyLen)
			if int(keyLen) < 0 || end > dataLen {
				return 0, false, ErrCorrupt
			}
			cellKey = data[keyStart:end]
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

// searchLeaf does binary search on a leaf page with overflow key support.
// Used by btree methods that need to search leaves where keys may overflow.
func (bt *btree) searchLeaf(pg *page, key []byte) (int, bool, error) {
	usableSize := bt.usablePageSize()
	return searchLeafWithOverflow(pg, key, usableSize, bt.pager, bt.walMaxFrame, bt.cache)
}

// searchLeafWithOverflow is a standalone function for searching leaf pages
// with overflow key support. Used by ReadTx which doesn't have a btree struct.
func searchLeafWithOverflow(pg *page, key []byte, usableSize int, p *pager, walMaxFrame uint32, cache *pcache) (int, bool, error) {
	n := int(pg.header.cellCount)
	data := pg.data
	dataLen := len(data)
	cpOff := pg.cellPointerOffset()
	maxLocal := maxLocalPayload(usableSize)
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

		// Read keyLen and valLen varints
		keyLen, kn, kerr := getVarintSafe(data[off:])
		if kerr != nil {
			return 0, false, ErrCorrupt
		}
		pos := off + kn
		if pos >= dataLen {
			return 0, false, ErrCorrupt
		}
		valLen, vn, verr := getVarintSafe(data[pos:])
		if verr != nil {
			return 0, false, ErrCorrupt
		}
		payloadStart := pos + vn

		totalPayload := int(keyLen) + int(valLen)
		var cellKey []byte
		if totalPayload <= maxLocal {
			// Fast path: no overflow, key fully on-page
			end := payloadStart + int(keyLen)
			if int(keyLen) < 0 || end > dataLen {
				return 0, false, ErrCorrupt
			}
			cellKey = data[payloadStart:end]
		} else {
			// Overflow cell — may need to read full key
			nLocal := localPayloadSize(totalPayload, usableSize)
			localKeyBytes := min(nLocal, int(keyLen))
			if localKeyBytes == int(keyLen) {
				// Key fits fully in local portion
				end := payloadStart + localKeyBytes
				if end > dataLen {
					return 0, false, ErrCorrupt
				}
				cellKey = data[payloadStart:end]
			} else {
				// Key overflows — compare prefix first for early exit
				if payloadStart+localKeyBytes > dataLen {
					return 0, false, ErrCorrupt
				}
				prefix := data[payloadStart : payloadStart+localKeyBytes]
				cmpLen := min(localKeyBytes, len(key))
				prefixCmp := bytes.Compare(prefix[:cmpLen], key[:cmpLen])
				if prefixCmp != 0 {
					// Prefix alone determines ordering
					if prefixCmp < 0 {
						lo = mid + 1
					} else {
						hi = mid
					}
					continue
				}
				// Need full key — read from overflow
				var fkerr error
				cellKey, fkerr = leafFullKey(data, off, usableSize, p, walMaxFrame, cache)
				if fkerr != nil {
					return 0, false, fkerr
				}
			}
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
	key, err = interiorFullKey(data, offset, usableSize, bt.pager, bt.walMaxFrame, bt.cache)
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
func searchInteriorWithOverflow(pg *page, key []byte, usableSize int, p *pager, walMaxFrame uint32, cache *pcache) (childPgno uint32, cellIdx int, err error) {
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
		cellKey, kerr := interiorFullKey(data, off, usableSize, p, walMaxFrame, cache)
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

// leafFullKey reads the full key from a leaf cell, reading overflow pages
// if the key spills. Returns a slice into page buffer for non-overflow keys,
// or an allocated copy for overflow keys.
// Matches SQLite's accessPayload() slow path in sqlite3BtreeIndexMoveto().
func leafFullKey(data []byte, offset int, usableSize int, p *pager, walMaxFrame uint32, cache *pcache) ([]byte, error) {
	dataLen := len(data)
	if offset >= dataLen {
		return nil, ErrCorrupt
	}
	keyLen, n, err := getVarintSafe(data[offset:])
	if err != nil {
		return nil, ErrCorrupt
	}
	pos := offset + n
	if pos >= dataLen {
		return nil, ErrCorrupt
	}
	valLen, n, err := getVarintSafe(data[pos:])
	if err != nil {
		return nil, ErrCorrupt
	}
	pos += n

	if int(keyLen) < 0 || int(keyLen) > maxPayloadAlloc {
		return nil, ErrCorrupt
	}
	if int(valLen) < 0 || int(valLen) > maxPayloadAlloc {
		return nil, ErrCorrupt
	}

	totalPayload := int(keyLen) + int(valLen)
	if totalPayload < 0 || totalPayload > maxPayloadAlloc {
		return nil, ErrCorrupt
	}
	maxLocal := maxLocalPayload(usableSize)

	if totalPayload <= maxLocal {
		// No overflow: key is fully on-page
		if pos+int(keyLen) > dataLen {
			return nil, ErrCorrupt
		}
		return data[pos : pos+int(keyLen)], nil
	}

	// Overflow cell
	nLocal := localPayloadSize(totalPayload, usableSize)
	localKeyBytes := min(nLocal, int(keyLen))

	if localKeyBytes == int(keyLen) {
		// Key fits fully in local portion (only value overflows)
		if pos+localKeyBytes > dataLen {
			return nil, ErrCorrupt
		}
		return data[pos : pos+localKeyBytes], nil
	}

	// Key overflows: allocate and reconstruct
	if pos+nLocal+4 > dataLen {
		return nil, ErrCorrupt
	}
	fullKey := make([]byte, int(keyLen))
	copy(fullKey, data[pos:pos+localKeyBytes])
	overflowPg := binary.BigEndian.Uint32(data[pos+nLocal : pos+nLocal+4])

	// Read key remainder from overflow chain.
	// The overflow chain contains (keyRemainder || valueRemainder).
	// We only need keyLen - localKeyBytes bytes.
	keyOverflow := int(keyLen) - localKeyBytes
	overflowBuf := make([]byte, keyOverflow)
	if cache != nil {
		err = p.readOverflowChainReader(overflowPg, overflowBuf, walMaxFrame, cache)
	} else {
		err = p.readOverflowChainAt(overflowPg, overflowBuf, walMaxFrame)
	}
	if err != nil {
		return nil, err
	}
	copy(fullKey[localKeyBytes:], overflowBuf)
	return fullKey, nil
}

// interiorFullKey reads the full key from an interior cell, handling overflow.
// cache controls overflow page reads: non-nil uses the reader's private cache,
// nil uses readOverflowChainAt (for writers who need to see their own dirty pages).
func interiorFullKey(data []byte, offset int, usableSize int, p *pager, walMaxFrame uint32, cache *pcache) ([]byte, error) {
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

	if int(keyLen) < 0 || int(keyLen) > maxPayloadAlloc {
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
	var readErr error
	if cache != nil {
		readErr = p.readOverflowChainReader(overflowPg, fullKey[localSize:], walMaxFrame, cache)
	} else {
		readErr = p.readOverflowChainAt(overflowPg, fullKey[localSize:], walMaxFrame)
	}
	if readErr != nil {
		return nil, readErr
	}
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

	// For writable btrees, use maxFrame that includes spill frames.
	// Spilled pages are written to WAL mid-transaction by pagerStress but
	// are invisible to readers (mxCommitFrame). The writer must see them
	// to traverse the tree correctly after spills + evictions.
	if bt.writable {
		if mf := bt.pager.wal.index.maxFrame.Load(); mf > maxFrame {
			maxFrame = mf
		}
	}

	// For writable btrees, use bt.getPage which uses wal.nFrame to find
	// spilled pages. For readers, use getPageReader with private cache.
	var pg *page
	if bt.writable {
		pg, err = bt.getPage(bt.rootPage)
	} else {
		pg, err = bt.pager.getPageReader(bt.rootPage, maxFrame, bt.cache)
	}
	if err != nil {
		return buf, err
	}

	usableSize := bt.usablePageSize()
	for {
		if pg.header.isLeaf() {
			idx, found, serr := searchLeafWithOverflow(pg, key, usableSize, bt.pager, maxFrame, bt.cache)
			if serr != nil {
				bt.pager.releasePage(pg)
				return buf, serr
			}
			if !found {
				bt.pager.releasePage(pg)
				return buf, ErrKeyNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				bt.pager.releasePage(pg)
				return buf, cerr
			}
			if cell.overflowPg != 0 {
				// Unified payload format: read keyLen, valLen, compute nLocal
				pos := int(off)
				keyLen, kn, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					bt.pager.releasePage(pg)
					return buf, ErrCorrupt
				}
				pos += kn
				valLen, _, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					bt.pager.releasePage(pg)
					return buf, ErrCorrupt
				}

				totalPayload := int(keyLen) + int(valLen)
				nLocal := localPayloadSize(totalPayload, usableSize)
				localKeyBytes := min(nLocal, int(keyLen))
				localValBytes := nLocal - localKeyBytes
				keyOverflow := int(keyLen) - localKeyBytes
				overflowSize := totalPayload - nLocal

				start := len(buf)
				buf = append(buf, make([]byte, int(valLen))...)
				fullVal := buf[start:]
				// Copy local value portion
				if localValBytes > 0 {
					copy(fullVal, cell.value)
				}
				// Read overflow and extract value remainder
				if overflowSize > 0 {
					overflowBuf := make([]byte, overflowSize)
					if bt.cache != nil {
						err = bt.pager.readOverflowChainReader(cell.overflowPg, overflowBuf, maxFrame, bt.cache)
					} else {
						err = bt.pager.readOverflowChainAt(cell.overflowPg, overflowBuf, maxFrame)
					}
					if err != nil {
						bt.pager.releasePage(pg)
						return buf[:start], err
					}
					valOverflow := int(valLen) - localValBytes
					if valOverflow > 0 {
						copy(fullVal[localValBytes:], overflowBuf[keyOverflow:])
					}
				}
				bt.pager.releasePage(pg)
				return buf, nil
			}
			bt.pager.releasePage(pg)
			return append(buf, cell.value...), nil
		}

		// Interior page - descend
		childPgno, _, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return buf, serr
		}
		bt.pager.releasePage(pg)
		if bt.writable {
			pg, err = bt.getPage(childPgno)
		} else {
			pg, err = bt.pager.getPageReader(childPgno, maxFrame, bt.cache)
		}
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
	err = bt.insertIntoLeafWithPath(wpg, key, value, path)
	bt.pager.releasePage(wpg)
	return err
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
	idx, found, serr := bt.searchLeaf(pg, key)
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
		cells, cellBuf := bt.collectLeafCells(pg)
		err := bt.rebuildLeafPage(pg, cells)
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		if err != nil {
			return err
		}
		return bt.insertLeafCellAt(pg, idx, key, value)
	}

	// Need to split — this will propagate up through path
	return bt.splitLeafAndInsertWithPath(pg, idx, key, value, path)
}

// insertIntoLeaf inserts into a leaf page, splitting if necessary.
func (bt *btree) insertIntoLeaf(pg *page, key, value []byte) error {
	idx, found, serr := bt.searchLeaf(pg, key)
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
		cells, cellBuf := bt.collectLeafCells(pg)
		err := bt.rebuildLeafPage(pg, cells)
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		if err != nil {
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
		// Need overflow pages. Unified payload: (key||value) can overflow at any point.
		nLocal := localPayloadSize(totalPayload, pageUsable)
		localKeyBytes := min(nLocal, len(key))
		localValBytes := nLocal - localKeyBytes
		// Stream key remainder + value remainder directly to overflow pages
		// without intermediate buffer (matches SQLite fillInCell pattern).
		var err error
		overflowPgno, err = bt.pager.writeOverflowChainMulti(
			key[localKeyBytes:], value[localValBytes:],
		)
		if err != nil {
			return err
		}
		hdr := varintSize(uint64(len(key))) + varintSize(uint64(len(value)))
		cellSize = hdr + nLocal + overflowPtrSize
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
		nLocal := localPayloadSize(totalPayload, pageUsable)
		writeLeafCellOverflow(pg.data[newContentStart:], key, value, nLocal, overflowPgno)
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
			// New cell needs overflow. Unified payload: (key||value).
			nLocal := localPayloadSize(totalPayload, usableSize)
			localKeyBytes := min(nLocal, len(key))
			localValBytes := nLocal - localKeyBytes
			overflowPgno, err := bt.pager.writeOverflowChainMulti(
				key[localKeyBytes:], value[localValBytes:],
			)
			if err != nil {
				return err
			}
			writeLeafCellOverflow(pg.data[cellOff:], key, value, nLocal, overflowPgno)
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
	// collectLeafCells preserves overflow chains (raw passthrough).
	// Free overflow only for the cell being replaced.
	cells, cellBuf := bt.collectLeafCells(pg)
	defer bt.pager.recycleCellSlice(cells)
	defer bt.pager.recycleCellBuf(cellBuf)
	if cells[idx].overflowPg != 0 {
		_ = bt.pager.freeOverflowChain(cells[idx].overflowPg)
	}
	cells[idx] = cellData{key: key, value: value} // rawCell=nil → will be encoded fresh

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
		if c.rawCell != nil {
			totalContent += len(c.rawCell)
		} else {
			totalContent += leafCellSizeWithOverflow(c.key, c.value, pageUsable)
		}
	}

	if hdrSize+totalContent <= pageUsable {
		return bt.rebuildLeafPage(pg, cells)
	}

	// Page overflow — split and propagate to parent.
	// Mirrors SQLite's balance() called after insertCellFast detects overflow.
	mid := leafSplitPoint(cells, pageUsable)
	leftCells := cells[:mid]
	rightCells := cells[mid:]

	// Get full separator key (handles rare case where key itself overflows).
	sepKey, serr := bt.cellFullKey(&rightCells[0])
	if serr != nil {
		return serr
	}
	sepKey = bytes.Clone(sepKey)

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
// Overflow chains are preserved: raw cell bytes (including the 4-byte overflow pointer)
// are copied into the buffer and stored in rawCell for passthrough during rebuildLeafPage.
// This matches SQLite's balance_nonroot which never reads or frees overflow data during
// rebalancing — overflow chains are only created/destroyed when cell content actually changes.
//
// Uses pager.cellBuf as reusable scratch space, modeled after SQLite's
// Pager.pTmpSpace (pager.c). The buffer is "taken" from the pager on each call
// and "returned" by the caller via pager.recycleCellBuf after the cells are
// consumed. The take-and-nil pattern handles the merge path (two overlapping
// calls) — the second call finds nil and allocates fresh.
func (bt *btree) collectLeafCells(pg *page) ([]cellData, []byte) {
	n := int(pg.header.cellCount)

	// Reuse pager's pooled cellData slice to avoid per-call allocation.
	// +1 cap so split callers can append one cell without re-allocating.
	cells := bt.pager.takeCellSlice(n + 1)
	if cells != nil {
		cells = cells[:n]
	} else {
		cells = make([]cellData, n, n+1)
	}
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

	// Take pager's reusable buffer if available (set to nil so overlapping
	// calls like the merge path each get their own buffer). Callers return
	// the buffer via bt.pager.recycleCellBuf after consuming the cells.
	buf := bt.pager.takeCellBuf(contentSize)
	if buf == nil {
		buf = make([]byte, 0, contentSize)
	}

	for i := range n {
		off := pg.getCellOffset(i)
		var bytesRead int
		cells[i], bytesRead, _ = parseLeafCellWithSize(pg.data, int(off), usableSize)

		// Copy raw cell bytes into contiguous buffer for passthrough.
		rcStart := len(buf)
		buf = append(buf, pg.data[int(off):int(off)+bytesRead]...)
		cells[i].rawCell = buf[rcStart : rcStart+bytesRead]

		if cells[i].overflowPg != 0 {
			// For overflow cells, parse the full key for separator extraction.
			// The local key portion is already in cells[i].key (from parseLeafCellWithSize),
			// but it points into pg.data. Copy it into buf so it survives page rebuild.
			kStart := len(buf)
			buf = append(buf, cells[i].key...)
			cells[i].key = buf[kStart:len(buf)]
			// value is only the local portion; not needed for separator extraction
			cells[i].value = nil
		} else {
			// Non-overflow: key and value are fully on-page.
			// Copy into buf so they survive page rebuild.
			kStart := len(buf)
			buf = append(buf, cells[i].key...)
			vStart := len(buf)
			buf = append(buf, cells[i].value...)
			cells[i].key = buf[kStart:vStart]
			cells[i].value = buf[vStart:len(buf)]
		}
	}
	return cells, buf
}

// cellFullKey returns the full key for a cell collected by collectLeafCells.
// For non-overflow cells, c.key is already the full key.
// For overflow cells where the key fits locally (only value overflows), c.key is full.
// For the rare case where the key itself overflows, read the remainder from overflow pages.
func (bt *btree) cellFullKey(c *cellData) ([]byte, error) {
	if c.rawCell == nil || c.overflowPg == 0 {
		return c.key, nil // already full
	}
	// Parse keyLen from rawCell varints
	keyLen, kn := getVarint(c.rawCell)
	if int(keyLen) == len(c.key) {
		return c.key, nil // key fits locally
	}
	// Key overflows — read the full key from overflow pages.
	// The overflow chain contains (keyRemainder || valueRemainder).
	usableSize := bt.usablePageSize()
	valLen, _ := getVarint(c.rawCell[kn:])
	totalPayload := int(keyLen) + int(valLen)
	nLocal := localPayloadSize(totalPayload, usableSize)
	localKeyBytes := min(nLocal, int(keyLen))
	keyOverflow := int(keyLen) - localKeyBytes

	fullKey := make([]byte, int(keyLen))
	copy(fullKey, c.key[:localKeyBytes])
	overflowBuf := make([]byte, keyOverflow)
	if err := bt.pager.readOverflowChainAt(c.overflowPg, overflowBuf, bt.walMaxFrame); err != nil {
		return nil, err
	}
	copy(fullKey[localKeyBytes:], overflowBuf)
	return fullKey, nil
}

// collectInteriorCells reads all cells from an interior page.
// For cells with overflow keys, the full key is read from overflow pages
// and the overflow chain is freed (since the caller will rebuild the page).
//
// Uses pager.cellBuf as reusable scratch space (same buffer as collectLeafCells;
// callers never interleave leaf and interior collection). Self-recycles the
// buffer back to the pager at the end since interior collection never overlaps.
func (bt *btree) collectInteriorCells(pg *page) []cellData {
	n := int(pg.header.cellCount)
	cells := make([]cellData, n)
	usable := bt.usablePageSize()
	maxLocal := maxLocalPayload(usable)

	// Estimate content size for non-overflow keys to use a single contiguous buffer.
	contentOff, coErr := pg.contentAreaOffset(usable)
	if coErr != nil {
		contentOff = usable
	}
	contentSize := usable - contentOff
	if contentSize < 0 {
		contentSize = 0
	}

	// Take pager's reusable buffer.
	buf := bt.pager.takeCellBuf(contentSize)
	if buf == nil {
		buf = make([]byte, 0, contentSize)
	}

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
			if err := bt.pager.readOverflowChainAt(overflowPg, fullKey[localSize:], bt.walMaxFrame); err != nil {
				if debugOverflowReadErrors.Load() != 0 {
					panic(fmt.Sprintf("collectInteriorCells: readOverflowChainAt(pg=%d, walMaxFrame=%d) failed: %v",
						overflowPg, bt.walMaxFrame, err))
				}
			}
			_ = bt.pager.freeOverflowChain(overflowPg)
			cells[i].key = fullKey
			cells[i].overflowPg = 0
		} else {
			// Copy key into contiguous buffer instead of individual bytes.Clone per key
			kStart := len(buf)
			buf = append(buf, cells[i].key...)
			cells[i].key = buf[kStart:len(buf)]
		}
		_ = maxLocal
	}

	// Return buffer to pager for reuse.
	bt.pager.recycleCellBuf(buf)
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
		if c.rawCell != nil {
			// Raw passthrough: copy cell bytes directly, preserving overflow pointer.
			size := len(c.rawCell)
			contentOff -= size
			copy(pg.data[contentOff:], c.rawCell)
		} else {
			totalPayload := len(c.key) + len(c.value)

			if totalPayload > maxLocal {
				// Need overflow. Unified payload: (key||value).
				nLocal := localPayloadSize(totalPayload, pageUsable)
				localKeyBytes := min(nLocal, len(c.key))
				localValBytes := nLocal - localKeyBytes
				overflowPgno, err := bt.pager.writeOverflowChainMulti(
					c.key[localKeyBytes:], c.value[localValBytes:],
				)
				if err != nil {
					return err
				}
				hdr := varintSize(uint64(len(c.key))) + varintSize(uint64(len(c.value)))
				size := hdr + nLocal + overflowPtrSize
				contentOff -= size
				writeLeafCellOverflow(pg.data[contentOff:], c.key, c.value, nLocal, overflowPgno)
			} else {
				size := leafCellSize(c.key, c.value)
				contentOff -= size
				writeLeafCell(pg.data[contentOff:], c.key, c.value)
			}
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
	cells, cellBuf := bt.collectLeafCells(pg)

	// Clone key+value into a single allocation.
	combined := make([]byte, len(key)+len(value))
	copy(combined, key)
	copy(combined[len(key):], value)
	newCell := cellData{key: combined[:len(key)], value: combined[len(key):]}
	// Insert newCell at idx with a single grow + shift instead of two intermediate slices.
	cells = append(cells, cellData{})
	copy(cells[idx+1:], cells[idx:len(cells)-1])
	cells[idx] = newCell

	// Find split point targeting ~2/3 fill on the left page (SQLite-style).
	mid := leafSplitPoint(cells, bt.usablePageSize())
	leftCells := cells[:mid]
	rightCells := cells[mid:]

	// Get full separator key (handles rare case where key itself overflows).
	sepKey, serr := bt.cellFullKey(&rightCells[0])
	if serr != nil {
		return serr
	}
	sepKey = bytes.Clone(sepKey)

	rightPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	if err := bt.rebuildLeafPage(pg, leftCells); err != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		return err
	}
	if err := bt.rebuildLeafPage(rightPg, rightCells); err != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		return err
	}
	bt.pager.recycleCellSlice(cells)
	bt.pager.recycleCellBuf(cellBuf)
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
	cells, cellBuf := bt.collectLeafCells(pg)

	// Insert the new cell at the correct position.
	// Clone key+value into a single allocation.
	combined := make([]byte, len(key)+len(value))
	copy(combined, key)
	copy(combined[len(key):], value)
	newCell := cellData{key: combined[:len(key)], value: combined[len(key):]}
	// Single grow + shift instead of two intermediate slices.
	cells = append(cells, cellData{})
	copy(cells[idx+1:], cells[idx:len(cells)-1])
	cells[idx] = newCell

	// Find split point targeting ~2/3 fill on the left page (SQLite-style).
	mid := leafSplitPoint(cells, bt.usablePageSize())
	leftCells := cells[:mid]
	rightCells := cells[mid:]

	// Get full separator key (handles rare case where key itself overflows).
	sepKey, serr := bt.cellFullKey(&rightCells[0])
	if serr != nil {
		return serr
	}
	sepKey = bytes.Clone(sepKey)

	// Allocate new right page
	rightPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	// Rebuild left page (reuse current page)
	if err := bt.rebuildLeafPage(pg, leftCells); err != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		return err
	}

	// Build right page
	if err := bt.rebuildLeafPage(rightPg, rightCells); err != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		return err
	}
	bt.pager.recycleCellSlice(cells)
	bt.pager.recycleCellBuf(cellBuf)

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
		childPgno, _, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return serr
		}
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
		cells, cellBuf := bt.collectLeafCells(oldRoot)
		newLeftPg.header = oldRoot.header
		err := bt.rebuildLeafPage(newLeftPg, cells)
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		if err != nil {
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

	idx, found, serr := bt.searchLeaf(wpg, key)
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
		// Fall back to full rebuild. collectLeafCells preserves overflow chains,
		// so we must explicitly free the deleted cell's overflow chain.
		cells, cellBuf := bt.collectLeafCells(wpg)
		if cells[idx].overflowPg != 0 {
			if err := bt.pager.freeOverflowChain(cells[idx].overflowPg); err != nil {
				bt.pager.recycleCellSlice(cells)
				bt.pager.recycleCellBuf(cellBuf)
				bt.pager.releasePage(wpg)
				return err
			}
		}
		cells = append(cells[:idx], cells[idx+1:]...)
		err := bt.rebuildLeafPage(wpg, cells)
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		if err != nil {
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

	// Check if merged content fits before collecting cells.
	// collectLeafCells now preserves overflow chains (no side effects),
	// but this check is still a useful optimization to avoid unnecessary work.
	usableSize := bt.usablePageSize()
	totalSize := 8 // leaf header
	for _, pg := range []*page{leafPg, sibPg} {
		n := int(pg.header.cellCount)
		for i := range n {
			off := int(pg.getCellOffset(i))
			keyLen, kn := getVarint(pg.data[off:])
			valLen, _ := getVarint(pg.data[off+kn:])
			totalSize += leafCellSizeFromLengths(int(keyLen), int(valLen), usableSize) + 2
		}
	}
	if totalSize > usableSize {
		bt.pager.releasePage(leafPg)
		bt.pager.releasePage(sibPg)
		return nil // doesn't fit
	}

	// Merge will proceed. Collect cells (overflow chains are preserved via raw passthrough).
	leafCells, leafCellBuf := bt.collectLeafCells(leafPg)
	sibCells, sibCellBuf := bt.collectLeafCells(sibPg)
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
		bt.pager.recycleCellSlice(leafCells)
		bt.pager.recycleCellSlice(sibCells)
		bt.pager.recycleCellBuf(leafCellBuf)
		bt.pager.recycleCellBuf(sibCellBuf)
		return err
	}
	rebuildErr := bt.rebuildLeafPage(keepPg, allCells)
	bt.pager.recycleCellSlice(leafCells)
	bt.pager.recycleCellSlice(sibCells)
	bt.pager.recycleCellBuf(leafCellBuf)
	bt.pager.recycleCellBuf(sibCellBuf)
	if rebuildErr != nil {
		bt.pager.releasePage(keepPg)
		return rebuildErr
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
		// Root with 0 cells: collapse tree height by copying rightChild to root.
		// Matches SQLite's copyNodeContent() (btree.c:8148).
		childPg, err := bt.getPage(rightChild)
		if err != nil {
			bt.pager.releasePage(parentPg)
			return err
		}
		if parentPg.pgno == 1 {
			// Page 1 has a 100-byte DB header. Cell content uses absolute offsets,
			// so content must stay at the same position. Only the page header and
			// cell pointers are shifted from offset 0 (child) to offset 100 (page 1).
			pageSize := bt.usablePageSize()
			iData := int(binary.BigEndian.Uint16(childPg.data[5:7]))
			if iData == 0 {
				iData = pageSize
			}

			// Clear page 1 content area (preserve DB header).
			clear(parentPg.data[dbHeaderSize:pageSize])

			// Step 1: Copy cell content at the SAME absolute offset.
			copy(parentPg.data[iData:pageSize], childPg.data[iData:pageSize])

			// Step 2: Copy header + cell pointers with offset adjustment.
			cpSize := childPg.header.headerSize() + int(childPg.header.cellCount)*2
			copy(parentPg.data[dbHeaderSize:dbHeaderSize+cpSize], childPg.data[0:cpSize])
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
		// Non-root interior with only one child (rightChild). Copy the child's
		// content into this page and free the child — same collapse operation as
		// root collapse but for non-root pages. This prevents orphaning the
		// rightChild subtree. Matches SQLite's balance_nonroot shallowing logic.
		childPg, err := bt.getPage(rightChild)
		if err != nil {
			bt.pager.releasePage(parentPg)
			return err
		}
		copy(parentPg.data, childPg.data)
		parentPg.header = childPg.header
		parentPg.header.serialize(parentPg.data)
		bt.pager.releasePage(childPg)
		bt.pager.releasePage(parentPg)
		return bt.pager.freePage(rightChild)
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

	// Interior page: recurse into children directly from page data
	// to avoid allocating a slice of child page numbers.
	n := int(pg.header.cellCount)
	total := 0
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
		c, cerr := bt.countPage(childPgno)
		if cerr != nil {
			bt.pager.releasePage(pg)
			return 0, cerr
		}
		total += c
	}
	rightChild := pg.header.rightChild
	bt.pager.releasePage(pg)

	c, err := bt.countPage(rightChild)
	if err != nil {
		return 0, err
	}
	total += c
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
	bt       *btree
	btData   btree // embedded btree data to avoid separate heap allocation
	stack    []cursorFrame
	stackBuf [8]cursorFrame // pre-allocated stack to avoid growth allocs for typical tree depths
	valid    bool
}

type cursorFrame struct {
	pg      *page // pinned page (non-nil only for the leaf frame)
	cellIdx int
	pgno    uint32
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

	idx, _, serr := c.bt.searchLeaf(pg, key)
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

// leafKeyAt extracts the key at cell index idx from a leaf page.
// The returned slice points into the page buffer. This is a lightweight
// alternative to parseLeafCellWithSize when only the key is needed.
// For overflow cells where the key spills off-page, returns ErrCorrupt
// so that callers (SeekNear) fall back to full Seek. This avoids needing
// pager access in this lightweight function.
func leafKeyAt(pg *page, idx int) ([]byte, error) {
	data := pg.data
	dataLen := len(data)
	off, err := pg.getCellOffsetSafe(idx)
	if err != nil {
		return nil, err
	}
	pos := int(off)
	if pos >= dataLen {
		return nil, ErrCorrupt
	}
	// New format: [varint(keyLen)] [varint(valLen)] [key||value...]
	b := data[pos]
	if b < 0x80 {
		kl := int(b)
		// Skip valLen varint
		valOff := pos + 1
		if valOff >= dataLen {
			return nil, ErrCorrupt
		}
		if data[valOff] < 0x80 {
			keyStart := valOff + 1
			end := keyStart + kl
			if end > dataLen {
				return nil, ErrCorrupt
			}
			return data[keyStart:end], nil
		}
		_, vn, verr := getVarintSafe(data[valOff:])
		if verr != nil {
			return nil, ErrCorrupt
		}
		keyStart := valOff + vn
		end := keyStart + kl
		if end > dataLen {
			return nil, ErrCorrupt
		}
		return data[keyStart:end], nil
	}
	keyLen, kn, kerr := getVarintSafe(data[pos:])
	if kerr != nil {
		return nil, ErrCorrupt
	}
	pos += kn
	if pos >= dataLen {
		return nil, ErrCorrupt
	}
	_, vn, verr := getVarintSafe(data[pos:])
	if verr != nil {
		return nil, ErrCorrupt
	}
	keyStart := pos + vn
	end := keyStart + int(keyLen)
	if int(keyLen) < 0 || end > dataLen {
		return nil, ErrCorrupt
	}
	return data[keyStart:end], nil
}

// SeekNear positions the cursor at the first key >= the given key.
// It optimises for the case where the target key falls within the currently
// pinned leaf page, avoiding a full root-to-leaf traversal.
func (c *Cursor) SeekNear(key []byte) error {
	// Fast path: check if key falls within the pinned leaf page.
	if c.valid && len(c.stack) > 0 {
		leaf := &c.stack[len(c.stack)-1]
		if leaf.pg != nil {
			n := int(leaf.pg.header.cellCount)
			if n > 0 {
				firstKey, err := leafKeyAt(leaf.pg, 0)
				if err != nil {
					// leafKeyAt cannot reconstruct overflow keys. Fall back to full seek.
					return c.Seek(key)
				}
				lastKey, err := leafKeyAt(leaf.pg, n-1)
				if err != nil {
					// leafKeyAt cannot reconstruct overflow keys. Fall back to full seek.
					return c.Seek(key)
				}
				if bytes.Compare(key, firstKey) >= 0 && bytes.Compare(key, lastKey) <= 0 {
					idx, _, serr := c.bt.searchLeaf(leaf.pg, key)
					if serr != nil {
						return serr
					}
					leaf.cellIdx = idx
					if idx < n {
						c.valid = true
					} else {
						return c.Next()
					}
					return nil
				}
			}
		}
	}
	// Slow path: full traversal from root.
	return c.Seek(key)
}

// SeekExact positions the cursor at the entry matching key exactly.
// Returns ErrKeyNotFound if the key does not exist.
func (c *Cursor) SeekExact(key []byte) error {
	if err := c.SeekNear(key); err != nil {
		return err
	}
	if !c.valid {
		return ErrKeyNotFound
	}
	eq, err := c.currentKeyEqual(key)
	if err != nil {
		return err
	}
	if !eq {
		return ErrKeyNotFound
	}
	return nil
}

// AppendValueByKey seeks an exact key and appends its value bytes into buf.
// It combines seek, exact-match check, and value extraction in one path,
// allowing cursor-based callers to avoid extra key/value parsing work.
func (c *Cursor) AppendValueByKey(key []byte, buf []byte) ([]byte, error) {
	if err := c.SeekNear(key); err != nil {
		return buf, err
	}
	if !c.valid {
		return buf, ErrKeyNotFound
	}
	eq, err := c.currentKeyEqual(key)
	if err != nil {
		return buf, err
	}
	if !eq {
		return buf, ErrKeyNotFound
	}

	frame := &c.stack[len(c.stack)-1]
	if frame.pg == nil {
		return buf, ErrCorrupt
	}
	usableSize := c.bt.usablePageSize()
	off, oerr := frame.pg.getCellOffsetSafe(frame.cellIdx)
	if oerr != nil {
		return buf, oerr
	}
	cell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(off), usableSize)
	if cerr != nil {
		return buf, cerr
	}
	if cell.overflowPg != 0 {
		// Fall back to full value reconstruction for overflow payloads.
		v, verr := c.Value()
		if verr != nil {
			return buf, verr
		}
		return append(buf, v...), nil
	}
	return append(buf, cell.value...), nil
}

// currentKeyEqual checks whether the cursor's current key is equal to key.
// Fast path uses in-page key extraction; overflow keys fall back to Key().
func (c *Cursor) currentKeyEqual(key []byte) (bool, error) {
	frame := &c.stack[len(c.stack)-1]
	if frame.pg == nil {
		return false, ErrCorrupt
	}
	if k, err := leafKeyAt(frame.pg, frame.cellIdx); err == nil {
		return bytes.Equal(k, key), nil
	}
	k, err := c.Key()
	if err != nil {
		return false, err
	}
	return bytes.Equal(k, key), nil
}

// Key returns the current key.
// For non-overflow cells, the returned slice points directly into the pinned
// page buffer and is valid until the next cursor movement or Close().
// For overflow cells where the key spills, a new slice is allocated.
func (c *Cursor) Key() ([]byte, error) {
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
		// Key may be partial — use leafFullKey to reconstruct
		return leafFullKey(frame.pg.data, int(off), usableSize, c.bt.pager, c.bt.walMaxFrame, c.bt.cache)
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
		// Unified payload format: read keyLen, valLen from header varints,
		// compute nLocal, then reconstruct the full value.
		pos := int(off)
		keyLen, kn, verr := getVarintSafe(frame.pg.data[pos:])
		if verr != nil {
			return nil, ErrCorrupt
		}
		pos += kn
		valLen, vn, verr := getVarintSafe(frame.pg.data[pos:])
		if verr != nil {
			return nil, ErrCorrupt
		}
		pos += vn

		totalPayload := int(keyLen) + int(valLen)
		nLocal := localPayloadSize(totalPayload, usableSize)
		localKeyBytes := min(nLocal, int(keyLen))
		localValBytes := nLocal - localKeyBytes
		keyOverflow := int(keyLen) - localKeyBytes

		// Read entire overflow chain
		overflowSize := totalPayload - nLocal
		overflowBuf := make([]byte, overflowSize)
		var err error
		if c.bt.cache != nil {
			err = c.bt.pager.readOverflowChainReader(cell.overflowPg, overflowBuf, c.bt.walMaxFrame, c.bt.cache)
		} else {
			err = c.bt.pager.readOverflowChainAt(cell.overflowPg, overflowBuf, c.bt.walMaxFrame)
		}
		if err != nil {
			return nil, err
		}

		// Reconstruct full value: local portion + overflow portion (after key overflow)
		fullVal := make([]byte, int(valLen))
		if localValBytes > 0 {
			copy(fullVal, cell.value) // cell.value holds local value bytes
		}
		valOverflow := int(valLen) - localValBytes
		if valOverflow > 0 {
			copy(fullVal[localValBytes:], overflowBuf[keyOverflow:])
		}
		return fullVal, nil
	}

	return cell.value, nil
}

// AppendValue appends the current value to buf and returns the extended buffer.
// For non-overflow values, appends the on-page bytes (one copy into buf).
// For overflow values, reads directly into buf with no intermediate allocation,
// matching SQLite's accessPayload() offset approach.
func (c *Cursor) AppendValue(buf []byte) ([]byte, error) {
	if !c.valid {
		return buf, ErrKeyNotFound
	}

	frame := &c.stack[len(c.stack)-1]
	if frame.pg == nil {
		return buf, ErrCorrupt
	}

	usableSize := c.bt.usablePageSize()
	off, oerr := frame.pg.getCellOffsetSafe(frame.cellIdx)
	if oerr != nil {
		return buf, oerr
	}
	cell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(off), usableSize)
	if cerr != nil {
		return buf, cerr
	}

	if cell.overflowPg != 0 {
		pos := int(off)
		keyLen, kn, verr := getVarintSafe(frame.pg.data[pos:])
		if verr != nil {
			return buf, ErrCorrupt
		}
		pos += kn
		valLen, vn, verr := getVarintSafe(frame.pg.data[pos:])
		if verr != nil {
			return buf, ErrCorrupt
		}
		_ = vn

		totalPayload := int(keyLen) + int(valLen)
		nLocal := localPayloadSize(totalPayload, usableSize)
		localKeyBytes := min(nLocal, int(keyLen))
		localValBytes := nLocal - localKeyBytes
		keyOverflow := int(keyLen) - localKeyBytes
		valOverflow := int(valLen) - localValBytes

		// Grow buf to hold the full value
		start := len(buf)
		buf = slices.Grow(buf, int(valLen))[:start+int(valLen)]
		fullVal := buf[start:]

		// Copy local value portion from the page
		if localValBytes > 0 {
			copy(fullVal, cell.value)
		}

		// Read overflow value bytes directly into destination, skipping key overflow
		if valOverflow > 0 {
			err := c.bt.pager.readOverflowAt(
				cell.overflowPg, keyOverflow, valOverflow,
				fullVal[localValBytes:], c.bt.walMaxFrame, c.bt.cache,
			)
			if err != nil {
				return buf[:start], err
			}
		}
		return buf, nil
	}

	return append(buf, cell.value...), nil
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

// CountUntil counts entries from the current position until the end key,
// using page-level batch counting to avoid per-entry key extraction.
// The cursor must be positioned at or after the start of the range.
// After this call, the cursor is positioned past the end key (or invalid).
func (c *Cursor) CountUntil(endKey []byte, endInclusive bool) (int, error) {
	count := 0
	usableSize := c.bt.usablePageSize()

	for c.valid {
		frame := &c.stack[len(c.stack)-1]
		if frame.pg == nil {
			return count, ErrCorrupt
		}

		cellCount := int(frame.pg.header.cellCount)
		if frame.cellIdx >= cellCount {
			// Shouldn't happen for valid cursor, advance
			if err := c.Next(); err != nil {
				return count, err
			}
			continue
		}

		remaining := cellCount - frame.cellIdx

		if len(endKey) > 0 && remaining > 1 {
			// Batch optimization: check last entry on page against end bound.
			// If it's within bounds, count all remaining entries at once.
			lastIdx := cellCount - 1
			lastOff, oerr := frame.pg.getCellOffsetSafe(lastIdx)
			if oerr != nil {
				return count, oerr
			}
			lastCell, _, cerr := parseLeafCellWithSize(frame.pg.data, int(lastOff), usableSize)
			if cerr != nil {
				return count, cerr
			}

			if lastCell.overflowPg == 0 {
				cmp := bytes.Compare(lastCell.key, endKey)
				if cmp < 0 || (cmp == 0 && endInclusive) {
					// All remaining entries on this page are within bounds
					count += remaining
					// Advance past this page
					frame.cellIdx = cellCount - 1
					if err := c.Next(); err != nil {
						return count, err
					}
					continue
				}
			}
		}

		// End bound is on this page (or single entry left, or overflow key)
		// Fall back to per-entry counting
		k, kerr := c.Key()
		if kerr != nil {
			return count, kerr
		}
		if len(endKey) > 0 {
			cmp := bytes.Compare(k, endKey)
			if cmp > 0 || (cmp == 0 && !endInclusive) {
				c.valid = false
				return count, nil
			}
		}
		count++
		if err := c.Next(); err != nil {
			return count, err
		}
	}
	return count, nil
}

// NewCursor creates a new cursor for the B-tree.
// The btree data is copied into the Cursor to avoid a separate heap allocation.
func (bt *btree) NewCursor() *Cursor {
	c := &Cursor{btData: *bt}
	c.bt = &c.btData
	c.stack = c.stackBuf[:0]
	return c
}
