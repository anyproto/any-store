package btree

// IntegrityCheck validates the internal consistency of the database, modeled
// after SQLite's sqlite3BtreeIntegrityCheck (btree.c:10705-11274).
//
// It verifies:
//   - Every page is referenced exactly once (no orphans, no double-refs)
//   - Freelist structure is consistent with header counts
//   - B-tree page types are valid
//   - Cell pointers are within bounds and cells don't overlap
//   - Keys are in sorted order on leaf pages
//   - Interior page children all have equal depth
//   - Overflow chains have the expected number of pages
//   - Fragmentation byte count matches actual gaps

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
)

// IntegrityError is returned by IntegrityCheck when corruption is detected.
type IntegrityError struct {
	Errors []string
}

func (e *IntegrityError) Error() string {
	return strings.Join(e.Errors, "\n")
}

// integrityChecker holds state for a single integrity check run.
type integrityChecker struct {
	pager       *pager
	walMaxFrame uint32
	pageRef     []byte   // bit-packed: 1 bit per page
	nPages      uint32   // total database pages
	usableSize  int      // pageSize - reservedSpace
	maxErrors   int      // stop after this many errors
	errors      []string // accumulated error messages

	// Context for error messages
	treeName string // current tree name (e.g., "master" or namespace name)
}

func (ic *integrityChecker) tooManyErrors() bool {
	return ic.maxErrors > 0 && len(ic.errors) >= ic.maxErrors
}

func (ic *integrityChecker) report(format string, args ...any) {
	if ic.tooManyErrors() {
		return
	}
	ic.errors = append(ic.errors, fmt.Sprintf(format, args...))
}

// isReferenced returns true if page pgno has been seen.
func (ic *integrityChecker) isReferenced(pgno uint32) bool {
	return ic.pageRef[pgno/8]&(1<<(pgno&7)) != 0
}

// setReferenced marks page pgno as referenced.
func (ic *integrityChecker) setReferenced(pgno uint32) {
	ic.pageRef[pgno/8] |= 1 << (pgno & 7)
}

// checkRef validates a page number, checks for double-reference, and marks it.
// Returns false if the page is invalid or already referenced.
func (ic *integrityChecker) checkRef(pgno uint32, context string) bool {
	if pgno < 1 || pgno > ic.nPages {
		ic.report("%s: invalid page number %d", context, pgno)
		return false
	}
	if ic.isReferenced(pgno) {
		ic.report("page %d: 2nd reference (%s)", pgno, context)
		return false
	}
	ic.setReferenced(pgno)
	return true
}

// checkList validates a freelist (isFreeList=true) or overflow chain (isFreeList=false).
// Port of SQLite's checkList() (btree.c:10705-10769).
func (ic *integrityChecker) checkList(isFreeList bool, firstPgno uint32, expected uint32) {
	if ic.tooManyErrors() {
		return
	}

	pgno := firstPgno
	count := uint32(0)

	for pgno != 0 {
		if ic.tooManyErrors() {
			return
		}

		var context string
		if isFreeList {
			context = "freelist"
		} else {
			context = fmt.Sprintf("tree %s overflow", ic.treeName)
		}

		if !ic.checkRef(pgno, context) {
			return
		}
		count++

		pg, err := ic.pager.getPageAt(pgno, ic.walMaxFrame)
		if err != nil {
			ic.report("%s: unable to get page %d: %v", context, pgno, err)
			return
		}

		if isFreeList {
			// Trunk page: next-trunk at offset 0, leaf count at offset 4
			leafCount := binary.BigEndian.Uint32(pg.data[4:8])
			maxLeaves := uint32(ic.pager.freelistMaxLeaves())

			if leafCount > maxLeaves {
				ic.report("freelist: leaf count %d too big on trunk page %d (max %d)", leafCount, pgno, maxLeaves)
				ic.pager.releasePage(pg)
				return
			}

			for i := uint32(0); i < leafCount; i++ {
				if ic.tooManyErrors() {
					ic.pager.releasePage(pg)
					return
				}
				leafPgno := binary.BigEndian.Uint32(pg.data[8+i*4:])
				ic.checkRef(leafPgno, "freelist leaf")
				count++
			}

			pgno = binary.BigEndian.Uint32(pg.data[0:4])
		} else {
			// Overflow page: next pointer at offset 0
			pgno = binary.BigEndian.Uint32(pg.data[0:4])
		}

		ic.pager.releasePage(pg)
	}

	if isFreeList {
		if count != expected {
			ic.report("freelist: expected %d pages but found %d", expected, count)
		}
	} else {
		if count != expected {
			ic.report("tree %s: overflow chain length is %d but should be %d", ic.treeName, count, expected)
		}
	}
}

// checkPageCoverage validates cell/freeblock coverage and fragmentation for a page.
// This implements SQLite's min-heap overlap/fragmentation check (btree.c:10793-11090).
//
// The heap entries use SQLite's encoding: (startByte << 16) | lastByte (inclusive).
// An implied first entry covers everything up to contentOffset-1 (header + cell
// pointer array + unallocated gap). Gaps between entries within the content area
// [contentOffset, usableSize) are counted as fragmentation.
func (ic *integrityChecker) checkPageCoverage(pg *page, context string, h []uint32) {
	// contentOffset is cellContentOff from the page header.
	// This is the start of the cell content area, NOT the end of the cell pointer array.
	contentOffset := int(pg.header.cellContentOff)
	if contentOffset == 0 {
		contentOffset = ic.usableSize
	}

	// Walk freeblock chain and add to heap
	fb := int(pg.header.firstFreeBlk)
	for fb != 0 {
		if fb < contentOffset || fb > ic.usableSize-4 {
			ic.report("%s: freeblock offset %d out of range", context, fb)
			break
		}
		nextFb := int(binary.BigEndian.Uint16(pg.data[fb : fb+2]))
		fbSize := int(binary.BigEndian.Uint16(pg.data[fb+2 : fb+4]))
		if fbSize < 4 {
			ic.report("%s: freeblock at %d has invalid size %d", context, fb, fbSize)
			break
		}
		if fb+fbSize > ic.usableSize {
			ic.report("%s: freeblock at %d size %d extends off end of page", context, fb, fbSize)
			break
		}
		heapInsert(&h, (uint32(fb)<<16)|uint32(fb+fbSize-1))
		if nextFb != 0 && nextFb <= fb {
			ic.report("%s: freeblock chain is not ordered (offset %d followed by %d)", context, fb, nextFb)
			break
		}
		fb = nextFb
	}

	// Analyze the min-heap: detect overlaps and count fragmentation.
	// prev tracks the "implied first entry" covering [0, contentOffset-1].
	nFrag := 0
	prev := uint32(contentOffset - 1) // implied entry: everything before content area

	for len(h) > 0 {
		x := heapPull(&h)
		if (prev & 0xFFFF) >= (x >> 16) {
			ic.report("%s: multiple uses for byte %d", context, x>>16)
			return
		}
		nFrag += int((x >> 16) - (prev & 0xFFFF) - 1)
		prev = x
	}
	nFrag += ic.usableSize - int(prev&0xFFFF) - 1

	if nFrag != int(pg.header.fragBytes) {
		ic.report("%s: fragmentation of %d bytes reported as %d", context, nFrag, pg.header.fragBytes)
	}
}

// checkTreePage recursively validates a B-tree page.
// Returns the depth of the tree rooted at this page (1 for leaf).
// Port of SQLite's checkTreePage() (btree.c:10840-11100).
func (ic *integrityChecker) checkTreePage(pgno uint32) int {
	if ic.tooManyErrors() {
		return 0
	}

	context := fmt.Sprintf("tree %s page %d", ic.treeName, pgno)

	if !ic.checkRef(pgno, context) {
		return 0
	}

	pg, err := ic.pager.getPageAt(pgno, ic.walMaxFrame)
	if err != nil {
		ic.report("%s: unable to get page: %v", context, err)
		return 0
	}
	defer ic.pager.releasePage(pg)

	// Validate page type
	pt := pg.header.pageType
	if pt != pageTypeLeafIdx && pt != pageTypeIntIdx {
		ic.report("%s: invalid page type %d", context, pt)
		return 0
	}

	nCells := int(pg.header.cellCount)
	isLeaf := pg.header.isLeaf()

	// contentOffset from the page header (start of cell content area)
	contentOffset := int(pg.header.cellContentOff)
	if contentOffset == 0 {
		contentOffset = ic.usableSize
	}

	doCoverageCheck := true
	var prevKey []byte
	depth := 0

	// Heap for coverage checking — using a simple slice-based min-heap
	// matching SQLite's btreeHeapInsert/btreeHeapPull (btree.c:10794-10823).
	// Each entry: (startByte << 16) | lastByte (inclusive).
	var h []uint32

	for i := 0; i < nCells; i++ {
		if ic.tooManyErrors() {
			return 0
		}

		cellOff := int(pg.getCellOffset(i))

		// Validate cell pointer is within the cell content area
		if cellOff < contentOffset || cellOff > ic.usableSize-4 {
			ic.report("%s cell %d: offset %d out of range %d..%d", context, i, cellOff, contentOffset, ic.usableSize-4)
			doCoverageCheck = false
			continue
		}

		// Parse cell and compute its size
		var cellSize int
		if isLeaf {
			cell, sz, cerr := parseLeafCellWithSize(pg.data, cellOff, ic.usableSize)
			if cerr != nil {
				ic.report("%s cell %d: corrupt cell data", context, i)
				doCoverageCheck = false
				continue
			}
			cellSize = sz

			// Bounds check: cell must not extend past the page
			if cellOff+cellSize > ic.usableSize {
				ic.report("%s cell %d: extends off end of page", context, i)
				doCoverageCheck = false
				continue
			}

			// Key ordering check
			if prevKey != nil && bytes.Compare(prevKey, cell.key) >= 0 {
				ic.report("%s cell %d: key out of order", context, i)
			}
			prevKey = bytes.Clone(cell.key)

			// Overflow validation
			if cell.overflowPg != 0 {
				pos := cellOff
				keyLen, kn, verr := getVarintSafe(pg.data[pos:])
				if verr == nil {
					pos += kn + int(keyLen)
					valLen, _, verr2 := getVarintSafe(pg.data[pos:])
					if verr2 == nil {
						localValSz := localValueSize(int(keyLen), int(valLen), ic.usableSize)
						overflowBytes := int(valLen) - localValSz
						ovflUsable := overflowPageUsable(ic.usableSize)
						nOverflow := (overflowBytes + ovflUsable - 1) / ovflUsable
						ic.checkList(false, cell.overflowPg, uint32(nOverflow))
					}
				}
			}

			// Add to heap for coverage check
			heapInsert(&h, (uint32(cellOff)<<16)|uint32(cellOff+cellSize-1))
		} else {
			// Interior cell (with overflow support)
			cell, sz, cerr := parseInteriorCell(pg.data, cellOff, ic.usableSize)
			if cerr != nil {
				ic.report("%s cell %d: corrupt cell data", context, i)
				doCoverageCheck = false
				continue
			}
			cellSize = sz

			if cellOff+cellSize > ic.usableSize {
				ic.report("%s cell %d: extends off end of page", context, i)
				doCoverageCheck = false
				continue
			}

			// For key ordering, we need the full key (may require overflow read)
			fullKey, fkerr := interiorFullKey(pg.data, cellOff, ic.usableSize, ic.pager, ic.walMaxFrame)
			if fkerr != nil {
				ic.report("%s cell %d: corrupt interior key", context, i)
			} else {
				if prevKey != nil && bytes.Compare(prevKey, fullKey) >= 0 {
					ic.report("%s cell %d: key out of order", context, i)
				}
				prevKey = bytes.Clone(fullKey)
			}

			// Overflow validation for interior cells
			if cell.overflowPg != 0 {
				pos := cellOff + 4
				keyLen, _, verr := getVarintSafe(pg.data[pos:])
				if verr == nil {
					localSz := localPayloadSize(int(keyLen), ic.usableSize)
					overflowBytes := int(keyLen) - localSz
					ovflUsable := overflowPageUsable(ic.usableSize)
					nOverflow := (overflowBytes + ovflUsable - 1) / ovflUsable
					ic.checkList(false, cell.overflowPg, uint32(nOverflow))
				}
			}

			// Recursively check child page
			childDepth := ic.checkTreePage(cell.leftChild)
			if i == 0 {
				depth = childDepth
			} else if childDepth != depth {
				ic.report("%s: child page depth differs (child %d depth %d vs expected %d)", context, cell.leftChild, childDepth, depth)
			}
		}
	}

	// For interior pages, check rightChild
	if !isLeaf {
		childDepth := ic.checkTreePage(pg.header.rightChild)
		if nCells > 0 && childDepth != depth {
			ic.report("%s: child page depth differs (rightChild %d depth %d vs expected %d)", context, pg.header.rightChild, childDepth, depth)
		}
		if nCells == 0 {
			depth = childDepth
		}

		// For interior pages, add cells to heap now (SQLite does this after recursion)
		if doCoverageCheck {
			cpOff := pg.cellPointerOffset()
			for i := 0; i < nCells; i++ {
				cellOff := int(binary.BigEndian.Uint16(pg.data[cpOff+i*2:]))
				_, sz, _ := parseInteriorCell(pg.data, cellOff, ic.usableSize)
				heapInsert(&h, (uint32(cellOff)<<16)|uint32(cellOff+sz-1))
			}
		}
	}

	// Coverage check: detect overlaps and verify fragmentation
	if doCoverageCheck {
		ic.checkPageCoverage(pg, context, h)
	}

	if isLeaf {
		return 1
	}
	return depth + 1
}

// IntegrityCheck validates the internal consistency of the entire database.
// Returns nil if the database is consistent, or an *IntegrityError with
// all issues found.
func (db *DB) IntegrityCheck() error {
	return db.IntegrityCheckN(100)
}

// IntegrityCheckN is like IntegrityCheck but stops after maxErrors issues.
// Pass 0 for unlimited.
func (db *DB) IntegrityCheckN(maxErrors int) error {
	// Start a read transaction
	maxFrame, slot, err := db.pager.beginRead()
	if err != nil {
		return err
	}
	defer db.pager.endRead(slot)

	// Read page 1 to get the current header
	pg1, err := db.pager.getPageAt(1, maxFrame)
	if err != nil {
		return err
	}
	var hdr dbHeader
	if err := hdr.deserialize(pg1.data[:dbHeaderSize]); err != nil {
		db.pager.releasePage(pg1)
		return err
	}
	db.pager.releasePage(pg1)

	nPages := hdr.DatabaseSize
	if nPages == 0 {
		return nil
	}

	ic := &integrityChecker{
		pager:       db.pager,
		walMaxFrame: maxFrame,
		pageRef:     make([]byte, nPages/8+1),
		nPages:      nPages,
		usableSize:  int(db.pager.pageSize) - int(hdr.ReservedSpace),
		maxErrors:   maxErrors,
	}

	// Mark page 1 as referenced (it is the master B-tree root)
	ic.setReferenced(1)

	// 1. Check freelist
	if hdr.FirstFreelistPg != 0 {
		ic.checkList(true, hdr.FirstFreelistPg, hdr.TotalFreelistPgs)
	} else if hdr.TotalFreelistPgs != 0 {
		ic.report("freelist: header says %d free pages but first trunk is 0", hdr.TotalFreelistPgs)
	}

	// 2. Check master B-tree (page 1) structure
	ic.treeName = "master"
	pg1, err = db.pager.getPageAt(1, maxFrame)
	if err != nil {
		return err
	}

	pt := pg1.header.pageType
	if pt != pageTypeLeafIdx && pt != pageTypeIntIdx {
		ic.report("tree master page 1: invalid page type %d", pt)
		db.pager.releasePage(pg1)
		goto checkOrphans
	}

	{
		nCells := int(pg1.header.cellCount)
		contentOffset := int(pg1.header.cellContentOff)
		if contentOffset == 0 {
			contentOffset = ic.usableSize
		}

		var h []uint32
		var prevKey []byte
		doCoverageCheck := true

		for i := 0; i < nCells; i++ {
			if ic.tooManyErrors() {
				break
			}

			cellOff := int(pg1.getCellOffset(i))
			if cellOff < contentOffset || cellOff > ic.usableSize-4 {
				ic.report("tree master page 1 cell %d: offset %d out of range %d..%d", i, cellOff, contentOffset, ic.usableSize-4)
				doCoverageCheck = false
				continue
			}

			cell, cellSize, cerr := parseLeafCellWithSize(pg1.data, cellOff, ic.usableSize)
			if cerr != nil {
				ic.report("tree master page 1 cell %d: corrupt cell data", i)
				doCoverageCheck = false
				continue
			}
			if cellOff+cellSize > ic.usableSize {
				ic.report("tree master page 1 cell %d: extends off end of page", i)
				doCoverageCheck = false
				continue
			}

			// Key ordering
			if prevKey != nil && bytes.Compare(prevKey, cell.key) >= 0 {
				ic.report("tree master page 1 cell %d: key out of order", i)
			}
			prevKey = bytes.Clone(cell.key)

			// Check the namespace's root page
			if len(cell.value) >= 4 {
				rootPage := binary.BigEndian.Uint32(cell.value)
				if rootPage >= 2 && rootPage <= nPages {
					ic.treeName = string(cell.key)
					ic.checkTreePage(rootPage)
				} else if rootPage != 0 {
					ic.report("tree master page 1 cell %d: namespace %q root page %d out of range", i, string(cell.key), rootPage)
				}
			}

			heapInsert(&h, (uint32(cellOff)<<16)|uint32(cellOff+cellSize-1))
		}

		if doCoverageCheck {
			ic.treeName = "master"
			ic.checkPageCoverage(pg1, "tree master page 1", h)
		}
	}
	db.pager.releasePage(pg1)

checkOrphans:
	// 3. Check for orphan pages
	for pgno := uint32(2); pgno <= nPages; pgno++ {
		if !ic.isReferenced(pgno) {
			ic.report("page %d: never used", pgno)
		}
	}

	if len(ic.errors) > 0 {
		return &IntegrityError{Errors: ic.errors}
	}
	return nil
}

// Simple min-heap operations matching SQLite's btreeHeapInsert/btreeHeapPull.
// Operates on a []uint32 slice directly.

func heapInsert(h *[]uint32, x uint32) {
	*h = append(*h, x)
	i := len(*h) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if (*h)[parent] <= (*h)[i] {
			break
		}
		(*h)[parent], (*h)[i] = (*h)[i], (*h)[parent]
		i = parent
	}
}

func heapPull(h *[]uint32) uint32 {
	old := *h
	n := len(old)
	x := old[0]
	old[0] = old[n-1]
	*h = old[:n-1]
	// sift down
	i := 0
	for {
		left := 2*i + 1
		if left >= len(*h) {
			break
		}
		j := left
		if right := left + 1; right < len(*h) && (*h)[right] < (*h)[left] {
			j = right
		}
		if (*h)[i] <= (*h)[j] {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		i = j
	}
	return x
}
