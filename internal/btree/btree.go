package btree

// B-tree implementation modeled after SQLite's btree.c.
// Uses index-style B-tree where both keys and values are stored as byte slices.
// Keys are sorted in byte order. Each cell stores: key-length | key | value-length | value.

import (
	"bytes"
	"encoding/binary"
	"slices"
	"sync/atomic"
)

// debugOverflowReadErrors is a test-only toggle retained as a stricter-debug
// aid. collectLeafCells and collectInteriorCells now propagate overflow
// read/parse errors directly (instead of silently ignoring them), so this flag
// no longer gates a panic in the collectors; real error propagation supersedes
// it. Set via atomic for test use only.
var debugOverflowReadErrors atomic.Int32

// SetDebugOverflowReadErrors enables or disables the test-only debug toggle.
// The collectors now surface overflow read errors directly, so this is a no-op
// for production behavior; kept for test compatibility.
func SetDebugOverflowReadErrors(enabled bool) {
	if enabled {
		debugOverflowReadErrors.Store(1)
	} else {
		debugOverflowReadErrors.Store(0)
	}
}

// Events reported by searchLeafOverflowProbe (test-only).
const (
	// searchProbePrefixShortCircuit is reported when the on-page local key
	// prefix alone decides the ordering for an overflow cell, so the full
	// overflow key is NOT read (the intended optimization win).
	searchProbePrefixShortCircuit = iota
	// searchProbeFullKeyRead is reported when the prefix compares equal on
	// cmpLen and the search therefore falls through to a full leafFullKey
	// read instead of making a truncated decision.
	searchProbeFullKeyRead
)

// searchLeafOverflowProbe is a test-only hook for searchLeafWithOverflow's
// overflow-key prefix shortcut. It is nil in production (the only cost is a
// nil-check, and only on the overflow-key branch). Tests set it to pin the
// by-design invariant documented at
// docs/btree/NOTES.md#old-drift-binsearch-rawbytes-prefix-no-overflow-cache:
// a truncated prefix decision (prefixCmp != 0) may short-circuit, but whenever
// the prefix compares EQUAL on cmpLen the code MUST fall through to a full key
// read so no decision is made on truncated bytes.
var searchLeafOverflowProbe func(event int)

// pathEntry records one level of the root-to-leaf descent performed by
// Put/Delete/insertIntoParent. It mirrors SQLite's cursor stack pair
// (apPage[i], aiIdx[i]) at btreeInt.h:553-556.
//
//	pgno    — page number of the interior node at this level.
//	cellIdx — index within that page that we descended through:
//	            0..nCell-1  -> the i-th cell's leftChild was followed;
//	            == nCell    -> descended via the page's rightChild.
//	          This is exactly searchInterior's second return value
//	          (see below), which was discarded at the call sites prior
//	          to this refactor.
//	nCell   — pg.header.cellCount at descent time. Carried so that the
//	          rightmost-child check (cellIdx == nCell) is O(1) without
//	          re-reading the parent. Required by the balance_quick
//	          dispatch guard in splitLeafAndInsertWithPath.
//
// Invariant: each cellIdx is consumed at most once, during upward
// propagation at its own level. After a divider is inserted into
// `pgno`, the cellIdx for `pgno` is never consulted again; higher-level
// cellIdx values remain valid because a lower split changes a parent's
// *contents* but not which pgno the grandparent points to.
type pathEntry struct {
	pgno    uint32
	cellIdx uint16
	nCell   uint16
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

// descendChild fetches a child page reached from an interior cell during
// cursor descent, after rejecting an out-of-range pgno as corruption.
// ~ C's getAndInitPage upfront guard `if( pgno>btreePagecount(pBt) ) return
// SQLITE_CORRUPT_BKPT` (btree.c:2396-2399), the getter used for every
// interior->child step in moveToChild / the inlined seek (btree.c:5475,6252).
// The pager getters keep zero-filling above-bound pages (drift-4); this guard
// lives at the descent call site so a wild or freed child pointer fails fast
// with ErrCorrupt instead of descending into a fabricated zero page. The bound
// is the snapshot page count: the writer's live p.dbSize (cache==nil) or the
// reader cache's frozen snapshot bound, exactly as readerDbSizeBound resolves.
//
// It additionally rejects a descended INTERIOR child with cellCount<1, mirroring
// moveToChild's `pPage->nCell<1 -> SQLITE_CORRUPT_PGNO` guard (btree.c:5477-5482,
// inlined at btree.c:6253-6258). On a valid any-store tree this is unreachable —
// the delete-rebalance cascade (completeMergeUpward) never persists a zero-cell
// non-root interior, and a 0-cell interior root is the empty-tree sentinel that
// callers handle before descending — so this is pure corruption-hardening (see
// the drift-11 invariant test, docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-).
// Without it, searchInterior on a corrupt n==0 interior would dereference the
// cleared first cell-pointer slot as a bogus child pgno instead of routing to
// rightChild; failing fast here yields ErrCorrupt instead of a wild descent.
func (bt *btree) descendChild(childPgno uint32) (*page, error) {
	if childPgno == 0 || childPgno > bt.pager.readerDbSizeBound(bt.cache) {
		return nil, ErrCorrupt
	}
	pg, err := bt.getPage(childPgno)
	if err != nil {
		return nil, err
	}
	if pg.header.isInterior() && pg.header.cellCount < 1 {
		bt.pager.releasePage(pg)
		return nil, ErrCorrupt
	}
	return pg, nil
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
// DRIFT: leaf cell-size computation omits SQLite's `if nSize<4 nSize=4` clamp See docs/btree/NOTES.md#drift-26-leaf-cell-size-missing-four-byte-minimum-clamp
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
// keyLen is validated against maxPayloadAlloc (mirroring parseLeafCellWithSize and
// C's btreeParseCellPtrIndex u32 nPayload truncation) to reject oversized/negative lengths.
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

	if int(keyLen) < 0 || int(keyLen) > maxPayloadAlloc {
		return c, 0, ErrCorrupt
	}

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
// DRIFT: leaf cell-size computation omits SQLite's `if nSize<4 nSize=4` clamp See docs/btree/NOTES.md#drift-26-leaf-cell-size-missing-four-byte-minimum-clamp
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
// DRIFT: leaf cell-size computation omits SQLite's `if nSize<4 nSize=4` clamp See docs/btree/NOTES.md#drift-26-leaf-cell-size-missing-four-byte-minimum-clamp
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
// DRIFT: leaf cell-size computation omits SQLite's `if nSize<4 nSize=4` clamp See docs/btree/NOTES.md#drift-26-leaf-cell-size-missing-four-byte-minimum-clamp
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
// DRIFT: Leaf cell uses 2 varints (keyLen,valLen) vs SQLite's 1 varint(nPayload) for key/value sema See docs/btree/NOTES.md#old-drift-leaf-cell-two-varint-keyvalue-format
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
// DRIFT: Leaf cell uses 2 varints (keyLen,valLen) vs SQLite's 1 varint(nPayload) for key/value sema See docs/btree/NOTES.md#old-drift-leaf-cell-two-varint-keyvalue-format
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
// DRIFT: searchLeafPage lacks indexCellCompare's overflow-cell classification guard See docs/btree/NOTES.md#drift-28-searchleafpage-missing-overflow-cell-compare-guard
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
// DRIFT: Search uses raw bytes.Compare + prefix-before-overflow-read and has no aOverflow page cach See docs/btree/NOTES.md#old-drift-binsearch-rawbytes-prefix-no-overflow-cache
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
					if searchLeafOverflowProbe != nil {
						searchLeafOverflowProbe(searchProbePrefixShortCircuit)
					}
					if prefixCmp < 0 {
						lo = mid + 1
					} else {
						hi = mid
					}
					continue
				}
				// Need full key — read from overflow
				if searchLeafOverflowProbe != nil {
					searchLeafOverflowProbe(searchProbeFullKeyRead)
				}
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
// DRIFT: descent omits moveToChild's per-child nCell>=1 corruption guard See docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-
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
		// Reject an out-of-range child pointer as corruption before fetching
		// (~ getAndInitPage's pgno>btreePagecount guard, btree.c:2396-2399);
		// see descendChild. Inlined here because this path reads with the
		// spill-inclusive maxFrame, not bt.walMaxFrame.
		if childPgno == 0 || childPgno > bt.pager.readerDbSizeBound(bt.cache) {
			return buf, ErrCorrupt
		}
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
	// Each entry records (pgno, cellIdx, nCell) — see pathEntry type
	// and btreeInt.h:553-556 for the SQLite analogue.
	var pathBuf [8]pathEntry
	path := pathBuf[:0]
	for pg.header.isInterior() {
		nCell := pg.header.cellCount
		childPgno, cellIdx, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return serr
		}
		path = append(path, pathEntry{pgno: pg.pgno, cellIdx: uint16(cellIdx), nCell: nCell})
		bt.pager.releasePage(pg)
		pg, err = bt.descendChild(childPgno)
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
func (bt *btree) insertIntoLeafWithPath(pg *page, key, value []byte, path []pathEntry) error {
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
		cells, cellBuf, cerr := bt.collectLeafCells(pg)
		if cerr != nil {
			return cerr
		}
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
		cells, cellBuf, cerr := bt.collectLeafCells(pg)
		if cerr != nil {
			return cerr
		}
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
func (bt *btree) updateLeafCell(pg *page, idx int, key, value []byte, path []pathEntry) error {
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
		// SQLite's fragmentation budget is 60 bytes: pageFindSlot refuses to
		// reuse a sub-4-byte slot once that would push fragBytes past 60
		// (btree.c:1755 "may not exceed 60", btree.c:1780 aData[hdr+7]>57).
		// Cap the in-place overwrite at the same limit so it matches both
		// SQLite and the Delete path's threshold (btree.go:2508).
		waste := oldCellSize - newCellSize
		if waste > 0 {
			newFrag := int(pg.header.fragBytes) + waste
			if newFrag <= 60 {
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
	cells, cellBuf, cerr := bt.collectLeafCells(pg)
	if cerr != nil {
		return cerr
	}
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
func (bt *btree) collectLeafCells(pg *page) ([]cellData, []byte, error) {
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
		var perr error
		// A malformed cell surfaces as corruption, mirroring SQLite's cell-pointer
		// load loop where a bad cell yields SQLITE_CORRUPT (btree.c:8443-8446,
		// 8467-8470). Recycle the taken slice/buf before returning.
		cells[i], bytesRead, perr = parseLeafCellWithSize(pg.data, int(off), usableSize)
		if perr != nil {
			bt.pager.recycleCellSlice(cells)
			bt.pager.recycleCellBuf(buf)
			return nil, nil, perr
		}

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
	return cells, buf, nil
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
func (bt *btree) collectInteriorCells(pg *page) ([]cellData, error) {
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
				// Surface the read error instead of swallowing it. Do NOT free the
				// overflow chain or overwrite cells[i].key: the cell's overflow
				// pointer must stay intact, matching C balance_nonroot which copies
				// the raw cell bytes verbatim and never re-reads/frees overflow
				// chains during balance (btree.c:8473-8489); errors propagate up the
				// balance() do-loop as SQLITE_CORRUPT/IO (btree.c:9131-9242).
				bt.pager.recycleCellBuf(buf)
				return nil, err
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
	return cells, nil
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
		// pCellptr after writing this cell's pointer (C: pCellptr += 2).
		ptrOff := hdrOff + pg.header.headerSize() + i*2
		if c.rawCell != nil {
			// Raw passthrough: copy cell bytes directly, preserving overflow pointer.
			size := len(c.rawCell)
			// Mirror C btree.c:7693: reject if content area would collide
			// with the cell-pointer array (pData < pCellptr). Check before
			// writing so we never emit content we cannot fit.
			if contentOff-size < ptrOff+2 {
				return ErrCorrupt
			}
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
				if contentOff-size < ptrOff+2 {
					return ErrCorrupt
				}
				contentOff -= size
				writeLeafCellOverflow(pg.data[contentOff:], c.key, c.value, nLocal, overflowPgno)
			} else {
				size := leafCellSize(c.key, c.value)
				if contentOff-size < ptrOff+2 {
					return ErrCorrupt
				}
				contentOff -= size
				writeLeafCell(pg.data[contentOff:], c.key, c.value)
			}
		}
		// Write cell pointer
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
// DRIFT: rebuildInteriorPage writes 0-cell pages; C rebuildPage asserts nCell>0 See docs/btree/NOTES.md#drift-31-rebuildinteriorpage-accepts-zero-cell-pages
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
		// pCellptr after writing this cell's pointer (C: pCellptr += 2).
		ptrOff := hdrOff + pg.header.headerSize() + i*2
		keyLen := len(c.key)
		if keyLen > maxLocal {
			localSize := localPayloadSize(keyLen, pageUsable)
			overflowData := c.key[localSize:]
			overflowPgno, err := bt.pager.writeOverflowChain(overflowData)
			if err != nil {
				return err
			}
			size := 4 + varintSize(uint64(keyLen)) + localSize + overflowPtrSize
			// Mirror C btree.c:7693: reject if content area would collide
			// with the cell-pointer array (pData < pCellptr). Check before
			// writing so we never emit content we cannot fit.
			if contentOff-size < ptrOff+2 {
				return ErrCorrupt
			}
			contentOff -= size
			writeInteriorCellOverflow(pg.data[contentOff:], c.leftChild, keyLen, c.key[:localSize], overflowPgno)
		} else {
			size := interiorCellSize(c.key)
			if contentOff-size < ptrOff+2 {
				return ErrCorrupt
			}
			contentOff -= size
			writeInteriorCell(pg.data[contentOff:], c.leftChild, c.key)
		}
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

// splitLeafRightmostAppend implements the rightmost-append fast path,
// porting SQLite's balance_quick (btree.c:7992-8086).
//
// Pre-conditions, checked by the caller at the top of
// splitLeafAndInsertWithPath — mirror btree.c:9170-9174:
//   - idx == pg.header.cellCount      — new cell is rightmost on pg
//   - len(path) > 0                   — pg is not the btree root
//   - path[len-1].cellIdx == path[len-1].nCell
//     — pg was reached via parent's rightChild
//   - path[len-1].pgno != bt.rootPage — parent is not the btree root
//     (SQLite: pParent->pgno != 1)
//
// Semantic adaptation from SQLite (btree.c:8066-8070): SQLite's intkey
// tables use the largest key of pPage as divider because their separator
// invariant is "left child keys <= separator"; any-store's interior
// search (btree.go searchInterior) uses "left child keys < separator,
// right child keys >= separator", so the divider is the *first* key of
// the new right sibling — the new key itself.
//
// Post-condition: pg retains all its pre-insert cells unchanged (no
// write to pg happens on this path — the whole point of the
// optimization). rightPg contains exactly (key, value). The parent has
// gained a new rightmost cell {leftChild=pg.pgno, key=divider} and its
// rightChild now points to rightPg. Parent overflow cascades through
// the standard path (insertIntoParentWithPath → insertSepIntoInterior),
// matching SQLite's balance() do-loop (btree.c:9123).
// Ports balance_quick's `nCell==0 -> CORRUPT` over-full-empty-page guard
// (btree.c:8020).
func (bt *btree) splitLeafRightmostAppend(pg *page, key, value []byte, path []pathEntry) error {
	// Corruption guard. Mirror balance_quick's first statement (btree.c:8020,
	// added for dbfuzz001.test): an over-full page reporting zero cells is
	// corrupt and is rejected before any allocation or parent mutation.
	if pg.header.cellCount == 0 {
		return ErrCorrupt
	}

	// Allocate new right sibling. Equiv. btree.c:8010 (allocateBtreePage).
	rightPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}

	// Initialize rightPg as a leaf holding just the new cell.
	// rebuildLeafPage does zeroPage (btree.c:8022), writes the cell, and
	// handles overflow allocation for oversized payloads — equivalent
	// to btree.c:8030's rebuildPage with nCell=1. any-store has no
	// ptrmap so btree.c:8046-8050 has no counterpart.
	newCell := cellData{key: key, value: value}
	if err := bt.rebuildLeafPage(rightPg, []cellData{newCell}); err != nil {
		bt.pager.releasePage(rightPg)
		return err
	}
	rightPgno := rightPg.pgno
	bt.pager.releasePage(rightPg)

	// Divider = the new key itself. See function doc for the separator-
	// invariant divergence from SQLite's btree.c:8066-8070.
	divider := bytes.Clone(key)

	// Delegate to the standard parent-insert path. Because cellIdx ==
	// nCell (fast-path precondition), insertSepIntoInterior's existing
	// branch at btree.go:1889-1896 takes effect: divider is written at
	// the end slot with leftChild=pg.pgno, and parent.rightChild is
	// repointed to rightPgno. Equivalent to SQLite's btree.c:8074
	// (insertCell) + btree.c:8079 (put4byte rightChild).
	bt.pager.balanceQuickDispatchCount.Add(1)
	return bt.insertIntoParentWithPath(pg, divider, rightPgno, path)
}

// splitLeafAndInsertWithPath rebalances an over-full leaf using the path for
// parent propagation. Dispatches like SQLite's balance() (btree.c:9133-9256):
// the balance_quick rightmost-append fast path first, then the general
// balance_nonroot (3-sibling redistribution), then balance_deeper for a root
// leaf with no parent.
// DRIFT: missing balance() refcount>1 'page is its own ancestor' corruption guard See docs/btree/NOTES.md#drift-33-missing-balance-self-ancestor-refcount-corruption-guard
func (bt *btree) splitLeafAndInsertWithPath(pg *page, idx int, key, value []byte, path []pathEntry) error {
	// Rightmost-append fast path. Port of SQLite balance_quick dispatch
	// at btree.c:9187-9210. The intKeyLeaf precondition (btree.c:9188)
	// is intkey-specific and has no any-store equivalent; we compensate
	// with the divider adaptation inside splitLeafRightmostAppend. The
	// remaining four preconditions map directly. KEEP — see plan "non-goals".
	if idx == int(pg.header.cellCount) && len(path) > 0 {
		parent := path[len(path)-1]
		if parent.pgno != bt.rootPage && parent.cellIdx == parent.nCell {
			return bt.splitLeafRightmostAppend(pg, key, value, path)
		}
	}

	// Root leaf with no parent: there is nowhere to gather siblings from. Push
	// the leaf down one level with balance_deeper (splitRoot), exactly as SQLite
	// routes a root overflow through balance_deeper first (btree.c:9152-9169);
	// the next insert balances the new child via balance_nonroot. We materialise
	// the over-full leaf into two pages here (the existing 2-way split is the
	// any-store form of "the child created by balance_deeper is then balanced").
	if len(path) == 0 || pg.pgno == bt.rootPage {
		return bt.splitRootLeafAndInsert(pg, idx, key, value)
	}

	// General path: balance the over-full leaf against up to two siblings via
	// the faithful balance_nonroot port (btree.c:8248-9030). The new cell is
	// folded into the redistribution pool through injectedCell (any-store's
	// analogue of the over-full page's overflow cell, btree.c:8466-8482) instead
	// of writing an over-full page to disk. Replaces the former 2-way
	// leafSplitPoint + 2×rebuildLeafPage + insertIntoParentWithPath block.
	parentEntry := path[len(path)-1]
	parentPg, err := bt.pager.getWritablePage(parentEntry.pgno)
	if err != nil {
		return err
	}
	// pg (the over-full target leaf) stays pinned by the caller. balanceNonroot
	// re-acquires it via getWritablePage as a gathered sibling — the writer cache
	// returns the same *page with pin 2, which it releases back to 1 before
	// returning. This matches SQLite, where the over-full page legitimately has
	// refcount 2 during balance_nonroot (cursor + gather; btree.c:8676).
	balanceErr := bt.balanceNonroot(parentPg, int(parentEntry.cellIdx),
		len(path) == 1, path[:len(path)-1],
		injectedCell{
			active:    true,
			childSlot: int(parentEntry.cellIdx),
			pos:       idx,
			key:       key,
			value:     value,
		})
	bt.pager.releasePage(parentPg)
	return balanceErr
}

// splitRootLeafAndInsert handles an over-full LEAF that is the btree root (no
// parent to gather siblings from). It is the any-store composition of SQLite's
// balance_deeper (btree.c:9052-9097) followed by a balance of the new child:
// the root's cells (plus the new cell) are split into two leaves and the root
// is converted to an interior node pointing at them. Mirrors the previous
// splitLeafAndInsertWithPath 2-way body, retained only for the root-leaf case.
func (bt *btree) splitRootLeafAndInsert(pg *page, idx int, key, value []byte) error {
	cells, cellBuf, cerr := bt.collectLeafCells(pg)
	if cerr != nil {
		return cerr
	}

	combined := make([]byte, len(key)+len(value))
	copy(combined, key)
	copy(combined[len(key):], value)
	newCell := cellData{key: combined[:len(key)], value: combined[len(key):]}
	cells = append(cells, cellData{})
	copy(cells[idx+1:], cells[idx:len(cells)-1])
	cells[idx] = newCell

	mid := leafSplitPoint(cells, bt.usablePageSize())
	leftCells := cells[:mid]
	rightCells := cells[mid:]

	sepKey, serr := bt.cellFullKey(&rightCells[0])
	if serr != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		return serr
	}
	sepKey = bytes.Clone(sepKey)

	rightPg, err := bt.pager.allocatePage()
	if err != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		return err
	}

	if err := bt.rebuildLeafPage(pg, leftCells); err != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		bt.pager.releasePage(rightPg)
		return err
	}
	if err := bt.rebuildLeafPage(rightPg, rightCells); err != nil {
		bt.pager.recycleCellSlice(cells)
		bt.pager.recycleCellBuf(cellBuf)
		bt.pager.releasePage(rightPg)
		return err
	}
	bt.pager.recycleCellSlice(cells)
	bt.pager.recycleCellBuf(cellBuf)
	rightPgno := rightPg.pgno
	bt.pager.releasePage(rightPg)

	// pg is the root; splitRoot (balance_deeper) creates a new left child,
	// copies pg's content there, and converts pg into an interior root.
	return bt.splitRoot(pg, sepKey, rightPgno)
}

// insertIntoParentWithPath inserts a separator key into the parent using the tracked path.
func (bt *btree) insertIntoParentWithPath(leftPg *page, key []byte, rightPgno uint32, path []pathEntry) error {
	if leftPg.pgno == bt.rootPage || len(path) == 0 {
		return bt.splitRoot(leftPg, key, rightPgno)
	}

	// Get the parent page (last element of path) as writable
	parentEntry := path[len(path)-1]
	parentPgno := parentEntry.pgno
	parentPath := path[:len(path)-1]

	parentPg, err := bt.pager.getWritablePage(parentPgno)
	if err != nil {
		return err
	}

	// path[len-1].cellIdx is the slot in parentPg where the new separator
	// goes — the same position the descent went through. Mirrors SQLite's
	// balance_nonroot(pParent, iIdx, ...) dispatch at btree.c:9213.
	return bt.insertSepIntoInterior(parentPg, leftPg.pgno, key, rightPgno, int(parentEntry.cellIdx), parentPath)
}

// insertSepIntoAncestor inserts a separator key into an ancestor page identified
// by leftPgno. Unlike insertIntoParentWithPath, this takes a page number instead
// of a page pointer, avoiding use-after-release issues during recursive splits.
// DRIFT: root-interior overflow uses 2-way split vs balance_deeper+balance_nonroot See docs/btree/NOTES.md#drift-29-root-interior-overflow-uses-2-way-split-not-balance-deeper-p
func (bt *btree) insertSepIntoAncestor(leftPgno uint32, key []byte, rightPgno uint32, path []pathEntry) error {
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
	parentEntry := path[len(path)-1]
	parentPgno := parentEntry.pgno
	parentPath := path[:len(path)-1]

	parentPg, err := bt.pager.getWritablePage(parentPgno)
	if err != nil {
		return err
	}

	return bt.insertSepIntoInterior(parentPg, leftPgno, key, rightPgno, int(parentEntry.cellIdx), parentPath)
}

// insertSepIntoInterior inserts a separator key into an interior page.
// This is the core logic shared by insertIntoParentWithPath and insertSepIntoAncestor.
//
// insertIdx is the position in parentPg where the new separator is
// inserted. It is path[len-1].cellIdx from the caller — the same slot
// in the parent the descent went through to reach the child that just
// split. Mirrors SQLite's balance_nonroot signature (btree.c:8230),
// which takes iIdx as a parameter populated from
// pCur->aiIdx[iPage-1] (btree.c:9162).
//
// Invariant: 0 <= insertIdx <= parentPg.header.cellCount. When
// insertIdx == cellCount we descended via rightChild; when less, we
// descended via the cell at that index.
func (bt *btree) insertSepIntoInterior(parentPg *page, leftPgno uint32, key []byte, rightPgno uint32, insertIdx int, parentPath []pathEntry) error {
	// Insert separator into parent interior page
	n := int(parentPg.header.cellCount)
	cpOff := parentPg.cellPointerOffset()

	// Defensive: guard against path staleness (should never fire in
	// correct code but surfaces drift during development).
	if insertIdx < 0 || insertIdx > n {
		bt.pager.releasePage(parentPg)
		return ErrCorrupt
	}

	pageUsable := bt.usablePageSize()
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

	// Parent interior page is full. The new divider splits the child at
	// insertIdx into leftPgno/rightPgno. If parentPg has a grandparent, balance
	// parentPg against its siblings via the faithful balance_nonroot port
	// (3-sibling interior redistribution), injecting the new divider into the
	// pool — this is the plan's step-13 interior cascade ("balance the interior
	// page against its siblings instead of the interiorSplitPoint 2-way split").
	// If parentPg IS the btree root (no grandparent), there is nowhere to gather
	// siblings, so split it 2-way and grow a new root (balance_deeper) via
	// splitRoot, exactly as SQLite routes a root overflow through balance_deeper
	// first (btree.c:9152-9169).
	if len(parentPath) > 0 && parentPg.pgno != bt.rootPage {
		grandEntry := parentPath[len(parentPath)-1]
		grandPg, gerr := bt.pager.getWritablePage(grandEntry.pgno)
		if gerr != nil {
			bt.pager.releasePage(parentPg)
			return gerr
		}
		// parentPg is gathered as a sibling under grandPg; release our own pin so
		// balanceNonroot's getWritablePage re-acquisition is the standard cursor
		// + gather double-pin (SQLite btree.c:8676), released within.
		bt.pager.releasePage(parentPg)
		balanceErr := bt.balanceNonroot(grandPg, int(grandEntry.cellIdx),
			len(parentPath) == 1, parentPath[:len(parentPath)-1],
			injectedCell{
				active:     true,
				interior:   true,
				childSlot:  int(grandEntry.cellIdx),
				pos:        insertIdx,
				key:        key,
				key2child:  leftPgno,
				rightChild: rightPgno,
			})
		bt.pager.releasePage(grandPg)
		return balanceErr
	}

	// Root interior page overflow: 2-way split + balance_deeper (splitRoot).
	cells, cerr := bt.collectInteriorCells(parentPg)
	if cerr != nil {
		bt.pager.releasePage(parentPg)
		return cerr
	}
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
// DRIFT: legacy unreachable insert-path functions undocumented as superseded See docs/btree/NOTES.md#drift-35-legacy-superseded-insert-path-functions-undocumented
func (bt *btree) splitLeafAndInsert(pg *page, idx int, key, value []byte) error {
	cells, cellBuf, cerr := bt.collectLeafCells(pg)
	if cerr != nil {
		return cerr
	}

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
// DRIFT: root-interior overflow uses 2-way split vs balance_deeper+balance_nonroot See docs/btree/NOTES.md#drift-29-root-interior-overflow-uses-2-way-split-not-balance-deeper-p
func (bt *btree) insertIntoParent(leftPg *page, key []byte, rightPgno uint32) error {
	if leftPg.pgno == bt.rootPage {
		return bt.splitRoot(leftPg, key, rightPgno)
	}

	// Build path from root to leftPg's parent by traversing the tree.
	// We need the path so that if the parent itself needs to split,
	// we can propagate upward.
	var pathBuf [8]pathEntry
	path := pathBuf[:0]
	pg, err := bt.getPage(bt.rootPage)
	if err != nil {
		return err
	}
	// Use the separator key to navigate to the parent of leftPg.
	for pg.header.isInterior() {
		nCell := pg.header.cellCount
		childPgno, cellIdx, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return serr
		}
		if childPgno == leftPg.pgno {
			// Found: pg is the parent of leftPg, reached via cellIdx.
			path = append(path, pathEntry{pgno: pg.pgno, cellIdx: uint16(cellIdx), nCell: nCell})
			bt.pager.releasePage(pg)
			return bt.insertIntoParentWithPath(leftPg, key, rightPgno, path)
		}
		// Check if leftPg is the rightChild. In that case cellIdx == nCell.
		if pg.header.rightChild == leftPg.pgno {
			path = append(path, pathEntry{pgno: pg.pgno, cellIdx: nCell, nCell: nCell})
			bt.pager.releasePage(pg)
			return bt.insertIntoParentWithPath(leftPg, key, rightPgno, path)
		}
		path = append(path, pathEntry{pgno: pg.pgno, cellIdx: uint16(cellIdx), nCell: nCell})
		bt.pager.releasePage(pg)
		pg, err = bt.descendChild(childPgno)
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
// DRIFT: splitRoot omits SQLite's anotherValidCursor() guard before deepening root See docs/btree/NOTES.md#drift-34-splitroot-missing-anothervalidcursor-corruption-guard
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
		cells, cellBuf, cerr := bt.collectLeafCells(oldRoot)
		if cerr != nil {
			bt.pager.releasePage(newLeftPg)
			return cerr
		}
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
		cells, cerr := bt.collectInteriorCells(oldRoot)
		if cerr != nil {
			bt.pager.releasePage(newLeftPg)
			return cerr
		}
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

	// Reject an out-of-range child pointer as corruption before fetching
	// (~ getAndInitPage's pgno>btreePagecount guard, btree.c:2396-2399);
	// see descendChild. Writer path: bound is the live p.dbSize.
	if childPgno == 0 || childPgno > bt.pager.readerDbSizeBound(bt.cache) {
		return ErrCorrupt
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

	var pathBuf [8]pathEntry
	path := pathBuf[:0]
	for pg.header.isInterior() {
		nCell := pg.header.cellCount
		childPgno, cellIdx, serr := bt.searchInterior(pg, key)
		if serr != nil {
			bt.pager.releasePage(pg)
			return serr
		}
		path = append(path, pathEntry{pgno: pg.pgno, cellIdx: uint16(cellIdx), nCell: nCell})
		bt.pager.releasePage(pg)
		pg, err = bt.descendChild(childPgno)
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

	// Reject a cell whose content runs into the reserved/codec tail
	// [usableSize, pageSize) before freeSpace/fragmentation accounting runs.
	// Mirrors SQLite's dropCell (btree.c:7291-7294):
	//   if( pc+sz > pPage->pBt->usableSize ){ *pRC = SQLITE_CORRUPT_BKPT; return; }
	if cellOff+oldCellSize > usableSize {
		bt.pager.releasePage(wpg)
		return ErrCorrupt
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
		cells, cellBuf, cerr := bt.collectLeafCells(wpg)
		if cerr != nil {
			bt.pager.releasePage(wpg)
			return cerr
		}
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

	// Delete-time rebalancing (spec §1, byte-exact mirror of SQLite's underfull
	// gate at btree.c:10005). After dropping the cell, decide whether the leaf
	// has fallen far enough below capacity to warrant pooling it with siblings.
	//
	//   nFree = usable - used; underfull iff nFree*3 > usable*2  (>2/3 free).
	//
	// any-store's leafUsedSpace counts fragBytes as used (it is reclaimed on the
	// next rebuild), so nFree here is <= SQLite's nFree; the trigger therefore
	// fires slightly LESS eagerly than SQLite, which is safe (an over-fragmented
	// page that SQLite would rebalance, any-store leaves until the next mutation).
	usable := bt.usablePageSize()
	nFree := usable - bt.leafUsedSpace(wpg)
	underfull := nFree*3 > usable*2

	// Empty leaf (0 cells) on a non-root page: keep the existing free-empty-leaf
	// fast path. This is the any-store equivalent of balance_nonroot freeing a
	// 0-cell page as surplus (btree.c:9000-9004), reached without a gather.
	if wpg.header.cellCount == 0 && wpg.pgno != bt.rootPage {
		bt.pager.releasePage(wpg)
		if err := bt.pager.freePage(leafPgno); err != nil {
			return err
		}
		if err := bt.removeChildFromParent(leafPgno, path); err != nil {
			return err
		}
		// Cascade the parent's own under-fullness/0-cell state upward, exactly
		// as deleteRebalanceLeaf does after balanceNonroot (above), mirroring
		// SQLite's balance() do-loop walking iPage up (btree.c:9250-9255).
		// removeChildFromParent -> finishParentRemoval always releases the parent
		// pin, so completeMergeUpward can re-acquire it fresh via getWritablePage.
		parentEntry := path[len(path)-1]
		return bt.completeMergeUpward(parentEntry.pgno, path[:len(path)-1])
	}

	// Underfull (but non-empty) non-root leaf: funnel it through the general
	// balancer in its merge direction (inject.active=false). A root leaf is never
	// balanced — SQLite's iPage==0 short-circuits balance() with no overflow
	// (btree.c:9152). len(path)>0 means the leaf has a parent to gather from.
	if underfull && len(path) > 0 {
		bt.pager.releasePage(wpg)
		return bt.deleteRebalanceLeaf(leafPgno, path)
	}

	// Not underfull (or the leaf is the root): SQLite's no-op branch
	// (btree.c:10008). The fast in-place drop above is the whole operation.
	//
	// If the drop emptied the page, reset it to a pristine empty state, mirroring
	// SQLite's dropCell when pPage->nCell reaches 0 (btree.c:7301-7306): clear the
	// freeblock pointer and fragmentation byte and reset cellContentOff to the
	// usable size. At this program point cellCount==0 can only be the ROOT leaf —
	// the non-root empty case already returned at the cellCount==0 && pgno !=
	// rootPage branch above. These values match what rebuildLeafPage writes for a
	// 0-cell page (btree.go:1743-1747).
	if wpg.header.cellCount == 0 {
		wpg.header.firstFreeBlk = 0
		wpg.header.fragBytes = 0
		// page size <= 65536 here, so the uint16 cast is exact (C's
		// usableSize==65536 / cellContentOff==0 special case cannot occur).
		wpg.header.cellContentOff = uint16(bt.usablePageSize())
		hdrOff := 0
		if wpg.pgno == 1 {
			hdrOff = dbHeaderSize
		}
		wpg.header.serialize(wpg.data[hdrOff:])
	}
	bt.pager.releasePage(wpg)
	return nil
}

// deleteRebalanceLeaf drives an underfull leaf through the general balancer in
// its merge direction. It is the delete-side dual of splitLeafAndInsertWithPath
// (btree.go:1856-1883): both acquire the parent writable and call balanceNonroot
// with the target child slot, differing only in the injectedCell — insert passes
// the new over-full cell (inject.active=true), delete passes none
// (inject.active=false), so the pooled cell count only shrinks and the balancer
// yields nNew ∈ {nOld-1, nOld}. SQLite uses the identical path: balance() →
// balance_nonroot() (btree.c:10010 → 9133 → 8248), with the merge being just
// k=nOld-1 emerging from the unchanged pack loop and the surplus page freed
// (btree.c:8563-8605, 9000-9004). There is NO separate merge routine.
//
// The caller (Delete) has already released its pin on leafPgno, so balanceNonroot
// re-acquires it as a gathered sibling via getWritablePage with the standard
// single pin — mirroring the insert path, where the over-full leaf is likewise
// released before balanceNonroot re-pins it (btree.go:1867-1871).
//
// After the leaf balance, the parent may itself be empty (single child) or
// underfull — the merge removed a divider. completeMergeUpward handles that
// cascade (spec §4); it runs AFTER parentPg is released so it can re-read the
// parent fresh and, if needed, release it before pooling it under its
// grandparent (the SQLite do-loop releases pPage before balancing the parent,
// btree.c:9251).
func (bt *btree) deleteRebalanceLeaf(leafPgno uint32, path []pathEntry) error {
	bt.pager.deleteRebalanceDispatchCount.Add(1)

	parentEntry := path[len(path)-1]
	parentPg, err := bt.pager.getWritablePage(parentEntry.pgno)
	if err != nil {
		return err
	}
	balanceErr := bt.balanceNonroot(parentPg, int(parentEntry.cellIdx),
		len(path) == 1, path[:len(path)-1],
		injectedCell{active: false})
	bt.pager.releasePage(parentPg)
	if balanceErr != nil {
		return balanceErr
	}
	// Cascade the parent's own under-fullness/emptiness upward (spec §4).
	return bt.completeMergeUpward(parentEntry.pgno, path[:len(path)-1])
}

// completeMergeUpward finishes a delete-side merge by handling the parent's own
// state after a divider was removed from it (spec §4). pgno is the just-balanced
// parent (rebuilt in place by balanceNonroot, so its page number is stable);
// parentPath is the ancestor chain strictly above it.
//
//	(b) Balance-shallower (btree.c:8960-8985): if the merge emptied the parent's
//	    divider array it now has a single child. Collapse it — absorb the child
//	    into this page and free the child, removing one tree level. This reuses
//	    collapseSingleChild (the same operation finishParentRemoval performs for
//	    the empty-leaf path), satisfying spec §4b. Checked FIRST: a 0-cell page is
//	    "empty" (collapse), not merely "underfull" (cascade), mirroring SQLite
//	    handling balance_shallower before the do-loop re-balances the parent.
//	(a) Underfull-parent cascade (btree.c:10012-10020, 9250-9255): the parent is
//	    non-empty but below 2/3 fill and has a grandparent. Pool it with its own
//	    siblings under the grandparent via balanceNonroot(inject.active=false),
//	    then recurse to complete THAT balance. SQLite re-enters balance() on the
//	    parent in its do-loop; any-store recurses through the path — the same
//	    "deviation 6" already accepted on the insert path (balance.go:65-68).
//
// A merge that neither empties nor under-fills the parent (the parent had ample
// dividers) terminates here. The empty/underfull checks are evaluated against a
// fresh read of the parent, so this never holds a pin across the recursive
// balance.
func (bt *btree) completeMergeUpward(pgno uint32, parentPath []pathEntry) error {
	pg, err := bt.pager.getWritablePage(pgno)
	if err != nil {
		return err
	}

	nCell := int(pg.header.cellCount)

	// (b) Balance-shallower — ROOT ONLY (SQLite gates on isRoot, btree.c:8960). A
	// 0-cell root has a single child: absorb it, dropping a tree level (uniformly,
	// so all leaves stay equidistant from the new root). A NON-root 0-cell interior
	// must NOT be collapsed in place — collapseSingleChild copies the child's header
	// (incl. type) into it, making its subtree one level shallower than its siblings
	// (unequal leaf depth = corruption). It is underfull, so it falls through to the
	// cascade below to be pooled with its siblings under the grandparent — exactly
	// how SQLite carries a single-child parent up the balance() do-loop (btree.c:9133).
	if nCell == 0 && len(parentPath) == 0 {
		rightChild := pg.header.rightChild
		return bt.collapseSingleChild(pg, rightChild)
	}

	// (a) Underfull non-root parent (0-cell included) → cascade under the grandparent.
	if len(parentPath) > 0 && (nCell == 0 || bt.interiorUnderfull(pg)) {
		bt.pager.releasePage(pg)
		return bt.deleteRebalanceInterior(pgno, parentPath)
	}

	// Neither: the merge is complete at this level.
	bt.pager.releasePage(pg)
	return nil
}

// deleteRebalanceInterior is the interior analogue of deleteRebalanceLeaf, used
// to cascade an underfull non-root interior parent upward after a merge removed a
// divider from it (spec §4a). It reads the grandparent writable and drives
// balanceNonroot on it with the underfull interior page as the target child and
// inject.active=false. Interior pools are already fully handled by balanceNonroot
// (balance.go:367-401, 614-628), so no balancer change is needed. After the
// balance it recurses through completeMergeUpward to finish the grandparent's own
// state, walking up to the root exactly as SQLite's balance() do-loop does
// (btree.c:9250-9255).
//
// interiorPgno is the underfull child; path[len-1] is the grandparent slot the
// child sits in. The caller has already released its pin on interiorPgno, so
// balanceNonroot re-acquires it as a gathered sibling under the grandparent with
// a single pin.
func (bt *btree) deleteRebalanceInterior(interiorPgno uint32, path []pathEntry) error {
	bt.pager.deleteRebalanceDispatchCount.Add(1)

	parentEntry := path[len(path)-1]
	grandPg, err := bt.pager.getWritablePage(parentEntry.pgno)
	if err != nil {
		return err
	}
	balanceErr := bt.balanceNonroot(grandPg, int(parentEntry.cellIdx),
		len(path) == 1, path[:len(path)-1],
		injectedCell{active: false})
	bt.pager.releasePage(grandPg)
	if balanceErr != nil {
		return balanceErr
	}
	// Cascade the grandparent's own state upward.
	return bt.completeMergeUpward(parentEntry.pgno, path[:len(path)-1])
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

// interiorUnderfull reports whether an interior page has fallen below SQLite's
// rebalance threshold — more than 2/3 of the usable space free, i.e.
// nFree*3 > usable*2 (the same byte-exact gate as the leaf trigger in Delete,
// negated from btree.c:10005). Used to decide the underfull-parent cascade after
// a delete-side merge removed a divider (spec §4a).
//
// The used-space formula (cellPtrEnd + (usable - contentOff)) is shared with
// leafUsedSpace; it is page-type-agnostic because cellPointerOffset() already
// accounts for the 12-byte interior header (vs the 8-byte leaf header) and the
// page-1 DB-header offset (page.go:354-361). A corrupt header yields used==usable
// (nFree==0, not underfull), which is safe (no spurious cascade).
func (bt *btree) interiorUnderfull(pg *page) bool {
	usable := bt.usablePageSize()
	contentOff, err := pg.contentAreaOffset(usable)
	if err != nil {
		return false // corrupt header: treat as full, do not cascade
	}
	cellPtrEnd := pg.cellPointerOffset() + int(pg.header.cellCount)*2
	used := cellPtrEnd + (usable - contentOff)
	nFree := usable - used
	return nFree*3 > usable*2
}

// tryMergeLeaf attempts to merge a leaf page with a sibling.
// Only merges if both pages' content fits in a single page.
func (bt *btree) tryMergeLeaf(leafPgno uint32, path []pathEntry) error {
	if len(path) == 0 {
		return nil
	}

	parentPgno := path[len(path)-1].pgno
	parentPg, err := bt.getPage(parentPgno)
	if err != nil {
		return err
	}

	// Find which child slot this leaf is in. path[len-1].cellIdx is the
	// slot we descended through to reach leafPgno, so the linear scan is
	// unnecessary. SQLite's delete-rebalance path uses pCur->aiIdx[iPage-1]
	// analogously.
	n := int(parentPg.header.cellCount)
	if n < 1 {
		bt.pager.releasePage(parentPg)
		return nil // need at least 2 children to merge
	}
	cpOff := parentPg.cellPointerOffset()
	childIdx := int(path[len(path)-1].cellIdx)

	// Defensive: confirm the path points at the expected leaf. Drift here
	// means the path was built incorrectly — caller bug.
	if childIdx < 0 || childIdx > n {
		bt.pager.releasePage(parentPg)
		return ErrCorrupt
	}
	if childIdx < n {
		off := int(binary.BigEndian.Uint16(parentPg.data[cpOff+childIdx*2:]))
		lc := binary.BigEndian.Uint32(parentPg.data[off : off+4])
		if lc != leafPgno {
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
	} else {
		// childIdx == n: path says we reached leafPgno via rightChild.
		if parentPg.header.rightChild != leafPgno {
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
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
	leafCells, leafCellBuf, lerr := bt.collectLeafCells(leafPg)
	if lerr != nil {
		bt.pager.releasePage(leafPg)
		bt.pager.releasePage(sibPg)
		return lerr
	}
	sibCells, sibCellBuf, serr := bt.collectLeafCells(sibPg)
	if serr != nil {
		bt.pager.recycleCellSlice(leafCells)
		bt.pager.recycleCellBuf(leafCellBuf)
		bt.pager.releasePage(leafPg)
		bt.pager.releasePage(sibPg)
		return serr
	}
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

	// Update the parent to reflect the merge. The two directions need
	// different parent edits because the divider that must be removed is the
	// SEPARATOR between the kept and freed children — divider D_childIdx —
	// in both cases, but its position relative to the freed page differs.
	//
	// Divider semantics (searchInterior, btree.go:910; splitLeaf doc,
	// btree.go:1780-1816): divider D_i is the smallest key of the subtree at
	// child i+1; keys < D_i route to child i, keys >= D_i route to child i+1.
	// Child i therefore covers [D_{i-1}, D_i).
	//
	//   mergeRight: keep = leafPgno at slot childIdx, freed = sibling at slot
	//     childIdx+1 (cells[childIdx+1].leftChild, or rightChild if
	//     childIdx+1 == n). The kept page absorbs child childIdx+1's keys, so
	//     it now spans [D_{childIdx-1}, D_{childIdx+1}). The separator
	//     D_childIdx (= cells[childIdx].key) no longer bounds anything and
	//     must be removed; the surviving entry to its right (which held the
	//     freed page) must be repointed to keep. removeChildFromParent cannot
	//     express this — it removes the cell whose leftChild == childPgno
	//     (i.e. D_{childIdx+1}), leaving the stale D_childIdx in front of the
	//     merged page and misrouting keys in [D_childIdx, D_{childIdx+1}).
	//
	//   mergeLeft: keep = sibling at slot n-1 (cells[n-1].leftChild), freed =
	//     leafPgno == rightChild (childIdx == n). The separator is D_{n-1} =
	//     cells[n-1].key. removeChildFromParent's rightChild branch removes
	//     exactly cells[n-1] and promotes its leftChild (= keep) to rightChild
	//     — already correct — so reuse it unchanged.
	if mergeRight {
		return bt.removeMergedRightSeparator(parentPgno, childIdx, keepPgno, freePgno)
	}
	return bt.removeChildFromParent(freePgno, path)
}

// removeMergedRightSeparator updates the parent interior page after a
// right-merge collapses child slots childIdx and childIdx+1 into the single
// kept page (keepPgno). It removes the separator divider D_childIdx
// (cells[childIdx]) and repoints the entry that previously referenced the
// freed page (freePgno) — which sits immediately to the right of the removed
// separator — to keepPgno.
//
// Two right-neighbor positions are possible:
//   - childIdx+1 < len(cells): the freed page is cells[childIdx+1].leftChild.
//     Set cells[childIdx+1].leftChild = keepPgno, then drop cells[childIdx].
//   - childIdx+1 == len(cells): the freed page is parentPg.rightChild.
//     Set rightChild = keepPgno, then drop cells[childIdx] (= the last cell).
//
// After the splice the page may have zero cells (it had exactly one divider),
// in which case finishParentRemoval performs the same root/non-root collapse
// as removeChildFromParent.
func (bt *btree) removeMergedRightSeparator(parentPgno uint32, childIdx int, keepPgno, freePgno uint32) error {
	parentPg, err := bt.pager.getWritablePage(parentPgno)
	if err != nil {
		return err
	}

	cells, cerr := bt.collectInteriorCells(parentPg)
	if cerr != nil {
		bt.pager.releasePage(parentPg)
		return cerr
	}
	rightChild := parentPg.header.rightChild

	// childIdx is the kept page's slot; it must address a real divider cell
	// (the separator) — never the rightChild pseudo-slot, since mergeRight is
	// only chosen when the kept page is a leftChild entry (childIdx < n).
	if childIdx < 0 || childIdx >= len(cells) {
		bt.pager.releasePage(parentPg)
		return ErrCorrupt
	}
	// Defensive: the separator cell's leftChild must be the kept page.
	if cells[childIdx].leftChild != keepPgno {
		bt.pager.releasePage(parentPg)
		return ErrCorrupt
	}

	if childIdx+1 < len(cells) {
		// Freed page was the next cell's leftChild. Repoint it to keep, then
		// drop the separator cell.
		if cells[childIdx+1].leftChild != freePgno {
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
		cells[childIdx+1].leftChild = keepPgno
	} else {
		// Freed page was the rightChild. Promote keep to rightChild, then drop
		// the (last) separator cell.
		if rightChild != freePgno {
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
		rightChild = keepPgno
	}
	cells = append(cells[:childIdx], cells[childIdx+1:]...)

	return bt.finishParentRemoval(parentPg, cells, rightChild)
}

// removeChildFromParent removes a child page reference from its parent interior page.
// DRIFT (hardened): the rightChild branch's 0-cell case is now a defensive
// corruption guard (return ErrCorrupt) rather than a silent no-op that would leave
// a dangling rightChild into a freed page and double-free it downstream. It is
// defensively unreachable post delete-rebalance refactor — descendChild's
// 0-cell-interior guard and the completeMergeUpward/deleteRebalanceInterior cascade
// prevent descend-reachable 0-cell parents.
func (bt *btree) removeChildFromParent(childPgno uint32, path []pathEntry) error {
	if len(path) == 0 {
		return nil
	}

	parentPgno := path[len(path)-1].pgno

	parentPg, err := bt.pager.getWritablePage(parentPgno)
	if err != nil {
		return err
	}

	cells, cerr := bt.collectInteriorCells(parentPg)
	if cerr != nil {
		bt.pager.releasePage(parentPg)
		return cerr
	}
	rightChild := parentPg.header.rightChild

	// Use path[len-1].cellIdx to locate the child slot directly.
	// SQLite's delete-rebalance uses pCur->aiIdx[iPage-1] analogously.
	childIdx := int(path[len(path)-1].cellIdx)

	// Defensive: confirm the path points at the expected child.
	if childIdx < 0 || childIdx > len(cells) {
		bt.pager.releasePage(parentPg)
		return ErrCorrupt
	}
	if childIdx < len(cells) {
		if cells[childIdx].leftChild != childPgno {
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
		// Remove this cell.
		cells = append(cells[:childIdx], cells[childIdx+1:]...)
	} else {
		// childIdx == len(cells): path says we reached childPgno via rightChild.
		if rightChild != childPgno {
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
		// rightChild is being removed. If there are cells, last cell's
		// leftChild becomes the new rightChild.
		if len(cells) > 0 {
			rightChild = cells[len(cells)-1].leftChild
			cells = cells[:len(cells)-1]
		} else {
			// 0-cell parent whose only child (rightChild) is the just-freed
			// childPgno. SQLite's balance() treats this configuration as an
			// asserted impossibility (a non-root single-child interior pointing
			// at a freed page never persists: btree.c:9000-9004 frees surplus
			// apOld pages only after editPage re-homes their cells, and the
			// shallower collapse is isRoot-gated, btree.c:8960). Persisting the
			// no-op would leave rightChild dangling at the freed page and
			// double-free it downstream (collapseSingleChild getPage(freed) +
			// freePage(freed)). Reject as corruption instead. Defensively
			// unreachable: descendChild's 0-cell-interior guard and the
			// completeMergeUpward/deleteRebalanceInterior cascade eliminate
			// 0-cell parents in the same Delete before this branch can run.
			bt.pager.releasePage(parentPg)
			return ErrCorrupt
		}
	}

	return bt.finishParentRemoval(parentPg, cells, rightChild)
}

// finishParentRemoval writes the post-removal cell set back to the parent
// interior page (parentPg, already pinned writable) and handles the
// height-collapse cases when the removal left the page with zero cells. It is
// the shared tail of removeChildFromParent (delete-rebalance, mergeLeft) and
// removeMergedRightSeparator (mergeRight): both compute the surviving (cells,
// rightChild) and hand off here. It always releases parentPg.
//
//   - 0 cells on the root: copy rightChild's content into the root and free
//     rightChild, shortening the tree by one level. Page 1 needs special
//     handling to preserve its 100-byte DB header (SQLite copyNodeContent,
//     btree.c:8148).
//   - 0 cells on a non-root interior: the page has a single child (rightChild);
//     copy that child's content up and free it to avoid orphaning the subtree
//     (SQLite balance_nonroot shallowing).
//   - otherwise: rebuild the interior page in place.
func (bt *btree) finishParentRemoval(parentPg *page, cells []cellData, rightChild uint32) error {
	// If parent is now an empty interior page (0 cells = single child), collapse —
	// but ONLY at the root. Balance-shallower (SQLite btree.c:8960-8985) shortens
	// all leaves uniformly only when applied at the root. Collapsing a NON-root
	// single-child interior in place (collapseSingleChild copies the child's
	// header, incl. type) makes this subtree one level shallower than its siblings
	// — unequal leaf depth, which IntegrityCheck flags as "child page depth
	// differs". For a non-root 0-cell parent, leave it as a valid degenerate
	// interior (rightChild only, via rebuildInteriorPage below): it stays
	// equidistant from the root and a later balance can absorb it (SQLite carries
	// a single-child parent up the balance() do-loop). Mirrors the
	// completeMergeUpward root gate.
	if len(cells) == 0 && parentPg.pgno == bt.rootPage {
		return bt.collapseSingleChild(parentPg, rightChild)
	}

	if err := bt.rebuildInteriorPage(parentPg, cells, rightChild); err != nil {
		bt.pager.releasePage(parentPg)
		return err
	}

	bt.pager.releasePage(parentPg)
	return nil
}

// collapseSingleChild performs the "balance-shallower" collapse: parentPg is an
// interior page that now has zero divider cells and therefore a single child
// (childPgno == its rightChild). Copy the child's content into parentPg and free
// the child, removing one level from the tree. parentPg keeps its page number, so
// the grandparent's pointer to parentPg and the divider keys bounding it stay
// valid (the collapsed node covers exactly the same key range, one level
// shallower) — no ancestor edit is needed and the collapse is terminal.
//
// This is the shared body of finishParentRemoval's old 0-cell branches (the
// empty-leaf-removal path) AND the delete-rebalance merge path
// (rewriteParentAfterBalance), satisfying spec §4b ("reuse finishParentRemoval
// for balance-shallower"). It is the any-store analogue of SQLite copyNodeContent
// + freePage (btree.c:8984-8985 for the root, the do-loop grandparent gather for
// a non-root). It always releases parentPg.
//
// Page 1 needs special handling: it carries the 100-byte DB header, so cell
// content (which uses absolute offsets) must stay at the same position while the
// page header + cell pointers shift from offset 0 (child) to offset 100 (page 1).
// The child was just rebuilt by rebuildInteriorPage / rebuildLeafPage and is
// therefore already defragmented (content packed at the top), so SQLite's
// explicit defragmentPage(apNew[0]) before the copy (btree.c:8977) is
// unnecessary — spec §4b adaptation.
func (bt *btree) collapseSingleChild(parentPg *page, childPgno uint32) error {
	childPg, err := bt.getPage(childPgno)
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
	return bt.pager.freePage(childPgno)
}

// Count returns the total number of key-value pairs in the B-tree.
// It traverses all pages but only reads page headers (cellCount),
// avoiding any key/value parsing — similar to SQLite's COUNT(*) optimization.
// DRIFT: count traversal has no per-iteration interrupt/cancellation check See docs/btree/NOTES.md#drift-16-count-traversal-missing-interrupt-cancellation-check
func (bt *btree) Count() (int, error) {
	return bt.countPage(bt.rootPage, 0)
}

// DRIFT: B+tree (leaf-only keys) drops interior-cell positions that SQLite B-tree visits See docs/btree/NOTES.md#drift-14-b-plus-tree-traversal-drops-interior-cell-keys-versus-sqlite
// DRIFT: count traversal has no per-iteration interrupt/cancellation check See docs/btree/NOTES.md#drift-16-count-traversal-missing-interrupt-cancellation-check
// DRIFT: Count uses Go int vs C i64 *pnEntry; entry total can truncate See docs/btree/NOTES.md#drift-17-count-return-type-truncates-i64-entry-total-to-go-int
func (bt *btree) countPage(pgno uint32, depth int) (int, error) {
	if depth > btCursorMaxDepth {
		return 0, ErrCorrupt
	}
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
		c, cerr := bt.countPage(childPgno, depth+1)
		if cerr != nil {
			bt.pager.releasePage(pg)
			return 0, cerr
		}
		total += c
	}
	rightChild := pg.header.rightChild
	bt.pager.releasePage(pg)

	c, err := bt.countPage(rightChild, depth+1)
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
	// Logically empty the stack so the cursor no longer claims a tree position.
	// Mirrors SQLite's pCur->iPage = -1 in btreeReleaseAllCursorPages (btree.c:707):
	// after releasing pinned pages the cursor must report no position, otherwise
	// post-close Next/Previous (whose guard is !c.valid && len(c.stack) == 0) would
	// re-pin released, possibly-repurposed pages. Capacity-preserving truncate keeps
	// the pre-allocated stackBuf backing array, so no allocation churn is introduced.
	c.stack = c.stack[:0]
}

// First positions the cursor at the first (smallest) key.
// DRIFT: descent omits moveToChild's per-child nCell>=1 corruption guard See docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-
// DRIFT: First/Last treat 0-cell interior (incl. non-page-1 root) as empty, not corrupt See docs/btree/NOTES.md#drift-13-empty-interior-root-treated-as-empty-btree-not-corruption
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

		pg, err = c.bt.descendChild(childPgno)
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
// DRIFT: descent omits moveToChild's per-child nCell>=1 corruption guard See docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-
// DRIFT: First/Last treat 0-cell interior (incl. non-page-1 root) as empty, not corrupt See docs/btree/NOTES.md#drift-13-empty-interior-root-treated-as-empty-btree-not-corruption
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

		pg, err = c.bt.descendChild(childPgno)
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
// DRIFT: descent omits moveToChild's per-child nCell>=1 corruption guard See docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-
// DRIFT: seek descent omits SQLite's per-page intKey/page-type consistency check See docs/btree/NOTES.md#drift-12-b-tree-kind-consistency-check-omitted-on-descent
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

		pg, err = c.bt.descendChild(childPgno)
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
		valOverflow := int(valLen) - localValBytes

		// Allocate full value buffer and copy local portion
		fullVal := make([]byte, int(valLen))
		if localValBytes > 0 {
			copy(fullVal, cell.value)
		}

		// Read overflow value bytes directly into destination,
		// skipping key overflow bytes. No intermediate buffer needed.
		if valOverflow > 0 {
			err := c.bt.pager.readOverflowAt(
				cell.overflowPg, keyOverflow, valOverflow,
				fullVal[localValBytes:], c.bt.walMaxFrame, c.bt.cache,
			)
			if err != nil {
				return nil, err
			}
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
// DRIFT: B+tree (leaf-only keys) drops interior-cell positions that SQLite B-tree visits See docs/btree/NOTES.md#drift-14-b-plus-tree-traversal-drops-interior-cell-keys-versus-sqlite
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

// Skip advances the cursor forward by n positions.
// Within a leaf page, this is an O(1) cellIdx bump. Page transitions
// only happen at leaf boundaries, so skipping N entries costs
// O(N / entries_per_page) page transitions instead of O(N) Next() calls.
func (c *Cursor) Skip(n int) error {
	for n > 0 && c.valid {
		frame := &c.stack[len(c.stack)-1]
		if frame.pg == nil {
			return ErrCorrupt
		}

		cellCount := int(frame.pg.header.cellCount)
		remaining := cellCount - frame.cellIdx - 1 // entries after current on this page

		if remaining >= n {
			frame.cellIdx += n
			return nil
		}

		// Skip all remaining on this page, then cross to next leaf via Next().
		n -= remaining + 1 // +1 for the current entry
		frame.cellIdx = cellCount - 1
		if err := c.Next(); err != nil {
			return err
		}
	}
	return nil
}

// SkipBackward moves the cursor backward by n positions.
// Mirror of Skip for reverse traversal.
func (c *Cursor) SkipBackward(n int) error {
	for n > 0 && c.valid {
		frame := &c.stack[len(c.stack)-1]
		if frame.pg == nil {
			return ErrCorrupt
		}

		canRewind := frame.cellIdx // entries before current on this page

		if canRewind >= n {
			frame.cellIdx -= n
			return nil
		}

		// Skip all remaining on this page, then cross to prev leaf via Previous().
		n -= canRewind + 1 // +1 for the current entry
		frame.cellIdx = 0
		if err := c.Previous(); err != nil {
			return err
		}
	}
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
