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
	cache       *pcache // private reader cache (avoids racing with writerCache)
	walMaxFrame uint32
	pageRef     []byte   // bit-packed: 1 bit per page
	nPages      uint32   // total database pages
	usableSize  int      // pageSize - reservedSpace
	maxErrors   int      // stop after this many errors
	errors      []string // accumulated error messages

	// Context for error messages
	treeName string // current tree name (e.g., "master" or namespace name)
}

// getPage fetches a page using the checker's private reader cache.
func (ic *integrityChecker) getPage(pgno uint32) (*page, error) {
	return ic.pager.getPageReader(pgno, ic.walMaxFrame, ic.cache)
}

func (ic *integrityChecker) tooManyErrors() bool {
	return ic.maxErrors > 0 && len(ic.errors) >= ic.maxErrors
}

// DRIFT: integrity report() inverts mxErr==0 budget and omits per-message progress/interrupt hook See docs/btree/NOTES.md#drift-114-integrity-report-inverts-zero-error-budget-and-omits-progres
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
// On an over-large trunk leaf-count, checkList now reports and continues to the
// next trunk (matching SQLite btree.c:10749-10778), and suppresses the trailing
// size/length mismatch line when an error was already reported during the walk
// (SQLite's nErrAtStart==pCheck->nErr guard, btree.c:10781). See historical
// docs/btree/NOTES.md#old-drift-checklist-unconditional-size-mismatch-report
func (ic *integrityChecker) checkList(isFreeList bool, firstPgno uint32, expected uint32) {
	if ic.tooManyErrors() {
		return
	}

	pgno := firstPgno
	count := uint32(0)
	// SQLite captures the error count before the walk (nErrAtStart) and only
	// emits the size/length mismatch if NO new errors were reported during the
	// walk (btree.c:10781 `N && nErrAtStart==pCheck->nErr`). This suppresses a
	// redundant wrong-count line when the chain was already flagged as corrupt
	// (e.g. an over-large trunk leaf count above).
	nErrAtStart := len(ic.errors)

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

		pg, err := ic.getPage(pgno)
		if err != nil {
			ic.report("%s: unable to get page %d: %v", context, pgno, err)
			return
		}

		if isFreeList {
			// Trunk page: next-trunk at offset 0, leaf count at offset 4
			leafCount := binary.BigEndian.Uint32(pg.data[4:8])
			maxLeaves := uint32(ic.pager.freelistMaxLeaves())

			if leafCount > maxLeaves {
				// SQLite reports and continues to the next trunk rather than
				// aborting the freelist walk (btree.c:10749-10764). The bogus
				// leaf entries are NOT iterated and NOT counted toward `count`
				// (C does an extra N-- here, i.e. one phantom page, but skips
				// N-=n). Control falls through to the next-trunk pointer below.
				ic.report("freelist: leaf count %d too big on trunk page %d (max %d)", leafCount, pgno, maxLeaves)
				count++
			} else {
				for i := uint32(0); i < leafCount; i++ {
					if ic.tooManyErrors() {
						ic.pager.releasePage(pg)
						return
					}
					leafPgno := binary.BigEndian.Uint32(pg.data[8+i*4:])
					ic.checkRef(leafPgno, "freelist leaf")
					count++
				}
			}

			pgno = binary.BigEndian.Uint32(pg.data[0:4])
		} else {
			// Overflow page: next pointer at offset 0
			pgno = binary.BigEndian.Uint32(pg.data[0:4])
		}

		ic.pager.releasePage(pg)
	}

	if isFreeList {
		if count != expected && nErrAtStart == len(ic.errors) {
			ic.report("freelist: expected %d pages but found %d", expected, count)
		}
	} else {
		if count != expected && nErrAtStart == len(ic.errors) {
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
// DRIFT: integrity freeblock walk: break-then-analyze, weaker ordering, extra runtime checks See docs/btree/NOTES.md#drift-113-integrity-freeblock-walk-and-coverage-diagnostics-diverge
func (ic *integrityChecker) checkPageCoverage(pg *page, context string, h []uint32) {
	// contentOffset is cellContentOff from the page header.
	// This is the start of the cell content area, NOT the end of the cell pointer array.
	// Validate against usableSize, matching SQLite's allocateSpace()
	// validation (btree.c lines 1843-1853).
	contentOffset, coErr := pg.contentAreaOffset(ic.usableSize)
	if coErr != nil {
		ic.report("%s: invalid cell content offset", context)
		return
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

// keyInBounds verifies that key lies within the half-open divider range
// [lower, upper) implied by the ancestor interior cells, and reports a
// diagnostic naming the page, cell index, key, and violated bound otherwise.
//
// any-store's interior search (btree.go searchInterior / splitLeafRightmostAppend
// doc, btree.go:1780-1816) uses "left child keys < divider, right child keys
// >= divider", i.e. the divider equals the FIRST (smallest) key of its right
// subtree. Therefore a child subtree reachable through a sequence of dividers
// covers exactly the half-open interval [lower, upper):
//
//   - lower (inclusive, nil = -inf): the divider immediately to the child's
//     left — every key in the subtree is >= lower because that divider routed
//     keys >= it to the right (this child).
//   - upper (exclusive, nil = +inf): the divider immediately to the child's
//     right — every key in the subtree is strictly < upper because that
//     divider routed keys >= it away to a later sibling.
//
// This is the any-store analogue of SQLite checkTreePage's keyCanBeEqual /
// maxKey machinery (btree.c:10953-11029). SQLite's intkey divider equals the
// LARGEST key of its left subtree, so SQLite tightens an inclusive upper bound
// (maxKey) while walking cells right-to-left and only permits equality at the
// boundary cell. any-store's divider equals the SMALLEST key of its right
// subtree, so the symmetric statement is an inclusive lower / exclusive upper
// bound — encoded directly here as [lower, upper).
func (ic *integrityChecker) keyInBounds(key, lower, upper []byte, context string, cellIdx int) {
	if lower != nil && bytes.Compare(key, lower) < 0 {
		ic.report("%s cell %d: key %x below divider lower bound %x (key must be >= divider that routed it right)",
			context, cellIdx, key, lower)
	}
	if upper != nil && bytes.Compare(key, upper) >= 0 {
		ic.report("%s cell %d: key %x at/above divider upper bound %x (key must be < divider that routes later keys away)",
			context, cellIdx, key, upper)
	}
}

// checkTreePage recursively validates a B-tree page.
// Returns the depth of the tree rooted at this page (1 for leaf).
// Port of SQLite's checkTreePage() (btree.c:10840-11100).
//
// lower (inclusive, nil = -inf) and upper (exclusive, nil = +inf) are the
// divider-derived key-range bounds threaded DOWN the recursion, mirroring
// SQLite's piMinKey/maxKey arguments (btree.c:10858-10863). Every cell key on
// this page — and, transitively, in every subtree below it — must lie in
// [lower, upper). At each interior node the bounds are tightened per child:
// child i gets [D_{i-1}, D_i), and the right child gets [D_{n-1}, upper),
// where D_j is the j-th divider key on this page. Combined with the existing
// strict cell ordering check this enforces
//
//	max(subtree[child i]) < D_i <= min(subtree[child i+1]).
func (ic *integrityChecker) checkTreePage(pgno uint32, lower, upper []byte, onLeafCell func(key, value []byte)) int {
	if ic.tooManyErrors() {
		return 0
	}

	context := fmt.Sprintf("tree %s page %d", ic.treeName, pgno)

	if !ic.checkRef(pgno, context) {
		return 0
	}

	pg, err := ic.getPage(pgno)
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

	// prevDivider tracks the divider key to the LEFT of the next child to be
	// recursed into (the inclusive lower bound for that child). It starts at
	// this page's own lower bound and advances to each divider D_i as the loop
	// progresses. The slices it holds point into pg.data (for non-overflow
	// dividers) or are freshly allocated by interiorFullKey (for overflow
	// dividers); pg stays pinned for the whole frame (deferred release above),
	// so they remain valid across the child recursion and through the
	// rightChild recursion below. No extra allocation beyond what overflow
	// dividers already require.
	prevDivider := lower

	// contentOffset from the page header (start of cell content area).
	// Validate against usableSize, matching SQLite's allocateSpace()
	// validation (btree.c lines 1843-1853).
	contentOffset, coErr := pg.contentAreaOffset(ic.usableSize)
	if coErr != nil {
		ic.report("%s: invalid cell content offset", context)
		return 0
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

			// Key ordering check — need full key for overflow cells
			var fullKey []byte
			if cell.overflowPg != 0 {
				fk, fkerr := leafFullKey(pg.data, cellOff, ic.usableSize, ic.pager, ic.walMaxFrame, ic.cache)
				if fkerr != nil {
					ic.report("%s cell %d: corrupt leaf key", context, i)
				} else {
					fullKey = fk
				}
			} else {
				fullKey = cell.key
			}
			if fullKey != nil {
				if prevKey != nil && bytes.Compare(prevKey, fullKey) >= 0 {
					ic.report("%s cell %d: key out of order", context, i)
				}
				// Divider-range bound check: every leaf key must lie within
				// the [lower, upper) range implied by the ancestor dividers.
				ic.keyInBounds(fullKey, lower, upper, context, i)
				prevKey = bytes.Clone(fullKey)
			}

			// Overflow validation (unified payload format v5)
			if cell.overflowPg != 0 {
				pos := cellOff
				keyLen, kn, verr := getVarintSafe(pg.data[pos:])
				if verr == nil {
					pos += kn
					valLen, _, verr2 := getVarintSafe(pg.data[pos:])
					if verr2 == nil {
						totalPayload := int(keyLen) + int(valLen)
						nLocal := localPayloadSize(totalPayload, ic.usableSize)
						overflowBytes := totalPayload - nLocal
						ovflUsable := overflowPageUsable(ic.usableSize)
						nOverflow := (overflowBytes + ovflUsable - 1) / ovflUsable
						ic.checkList(false, cell.overflowPg, uint32(nOverflow))
					}
				}
			}

			// Add to heap for coverage check
			heapInsert(&h, (uint32(cellOff)<<16)|uint32(cellOff+cellSize-1))

			// Per-leaf-cell hook. SQLite has no equivalent here because it
			// pre-builds aRoot[] from the in-memory schema (pragma.c:1761-1774);
			// any-store has no such list, so the master root discovers each
			// namespace subtree from its own leaf cells via this callback. The
			// hook receives the reassembled key and value — cell.key/cell.value
			// hold only the local portions, and a long master key pushes the
			// 4-byte root value into the overflow chain. The hook only reads
			// them; it does not touch the coverage heap or fragmentation
			// accounting, so coverage is unchanged.
			if onLeafCell != nil {
				hookKey := fullKey
				if hookKey == nil {
					hookKey = cell.key
				}
				hookVal := cell.value
				if cell.overflowPg != 0 {
					fv, fverr := leafFullValue(pg.data, cellOff, ic.usableSize, ic.pager, ic.walMaxFrame, ic.cache)
					if fverr != nil {
						hookVal = nil // hook reports the corrupt value
					} else {
						hookVal = fv
					}
				}
				onLeafCell(hookKey, hookVal)
			}
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

			// For key ordering, we need the full key (may require overflow read).
			// This is the divider key D_i for this interior cell.
			fullKey, fkerr := interiorFullKey(pg.data, cellOff, ic.usableSize, ic.pager, ic.walMaxFrame, ic.cache)
			if fkerr != nil {
				ic.report("%s cell %d: corrupt interior key", context, i)
			} else {
				if prevKey != nil && bytes.Compare(prevKey, fullKey) >= 0 {
					ic.report("%s cell %d: key out of order", context, i)
				}
				// Divider-range bound check: D_i itself must lie within this
				// page's [lower, upper) range inherited from ancestors.
				ic.keyInBounds(fullKey, lower, upper, context, i)
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

			// Recursively check child page (cell.leftChild = child i), threading
			// the tightened bound [prevDivider, D_i): keys in this child must be
			// >= the divider to its left (prevDivider, inclusive) and strictly <
			// D_i (this cell's divider, exclusive). When D_i failed to decode
			// (fkerr != nil) we leave childUpper as prevDivider's successor would
			// be unknown — pass the page's own upper bound so we don't emit
			// false range violations against a key we couldn't read.
			childUpper := upper
			if fkerr == nil {
				childUpper = fullKey
			}
			childDepth := ic.checkTreePage(cell.leftChild, prevDivider, childUpper, onLeafCell)
			if i == 0 {
				depth = childDepth
			} else if childDepth != depth {
				ic.report("%s: child page depth differs (child %d depth %d vs expected %d)", context, cell.leftChild, childDepth, depth)
			}
			// Advance the lower bound for the NEXT child to this cell's divider.
			// fullKey points into pg.data (non-overflow) or is a fresh allocation
			// (overflow); pg is pinned for the whole frame, so this slice stays
			// valid through the remaining children and the rightChild recursion.
			if fkerr == nil {
				prevDivider = fullKey
			}
		}
	}

	// For interior pages, check rightChild. The right child holds keys
	// >= D_{n-1} (the last divider, now in prevDivider) and < this page's
	// upper bound — bound [prevDivider, upper).
	if !isLeaf {
		childDepth := ic.checkTreePage(pg.header.rightChild, prevDivider, upper, onLeafCell)
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

	// Use a private reader cache to avoid racing with the writer's cache.
	cache := newPcache(int(db.pager.pageSize), 200, true)
	cache.useSlab = db.pager.useSlab
	defer cache.destroy()

	// Read page 1 to get the current header
	pg1, err := db.pager.getPageReader(1, maxFrame, cache)
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

	// Cap nPages against the actual database size to protect against corrupted
	// headers claiming billions of pages. This matches SQLite's approach where
	// btreePagecount() uses the actual file/WAL size, not the header field.
	//
	// Use max(filePages, walIndex.maxPage) because pages allocated in the WAL
	// may have page numbers beyond the current physical DB file size (they get
	// written to the DB file during checkpoint).
	pageSize := db.pager.pageSize
	if pageSize > 0 {
		var maxPossible uint32
		if db.pager.file != nil {
			fi, statErr := db.pager.file.Stat()
			if statErr == nil {
				maxPossible = uint32(fi.Size() / int64(pageSize))
			}
		}
		// WAL's maxPage tracks the committed database size including
		// pages that only exist in the WAL (not yet checkpointed).
		walMaxPage := db.pager.wal.index.maxPage.Load()
		if walMaxPage > maxPossible {
			maxPossible = walMaxPage
		}
		if maxPossible > 0 && nPages > maxPossible {
			nPages = maxPossible
		}
	}

	// Bound reader page/overflow reads against this snapshot's (capped) page
	// count rather than the writer-only pager.dbSize. See pcache.dbSize.
	cache.dbSize = nPages

	ic := &integrityChecker{
		pager:       db.pager,
		cache:       cache,
		walMaxFrame: maxFrame,
		pageRef:     make([]byte, nPages/8+1),
		nPages:      nPages,
		usableSize:  int(db.pager.pageSize) - int(hdr.ReservedSpace),
		maxErrors:   maxErrors,
	}

	// Page 1 (the master B-tree root) is NOT pre-referenced here. It is routed
	// through checkTreePage(1, ...) below, which calls checkRef(1, ...) itself
	// (mirroring btree.c:10897). Pre-marking it would make that checkRef emit a
	// spurious "page 1: 2nd reference" and abort the walk. SQLite likewise
	// pre-references only PENDING_BYTE_PAGE (btree.c:11200-11201), never tree roots.

	// 1. Check freelist
	if hdr.FirstFreelistPg != 0 {
		ic.checkList(true, hdr.FirstFreelistPg, hdr.TotalFreelistPgs)
	} else if hdr.TotalFreelistPgs != 0 {
		ic.report("freelist: header says %d free pages but first trunk is 0", hdr.TotalFreelistPgs)
	}

	// 2. Check master B-tree (page 1) structure.
	//
	// Route page 1 through the SAME uniform checkTreePage path SQLite uses for
	// every root (btree.c:11236-11247). checkTreePage handles both leaf and
	// interior roots — recursing into children, validating coverage/depth/key
	// order at every level — so an interior master page (after a split) is no
	// longer misparsed as a flat leaf.
	//
	// masterLeaf is the per-leaf-cell hook that performs the one any-store-
	// specific step SQLite does not need: SQLite pre-builds aRoot[] from the
	// in-memory schema (pragma.c:1761-1774); any-store has no such list, so each
	// namespace subtree is discovered here from the master's own LEAF cells, at
	// ANY master depth. The hook only reads cell.value (the namespace root page),
	// never the coverage heap.
	ic.treeName = "master"
	masterLeaf := func(key, value []byte) {
		// A master leaf value < 4 bytes is corrupt (matches resolveNamespace).
		// The interior/leaf parse + bounds checks in checkTreePage already
		// validated cell structure; the value arrives reassembled (local +
		// overflow), so here we only validate its content.
		if len(value) < 4 {
			ic.report("tree master cell key %q: corrupt namespace root value", string(key))
			return
		}
		rootPage := binary.BigEndian.Uint32(value[:4])
		switch {
		case rootPage >= 2 && rootPage <= nPages:
			// Each namespace tree is an independent B-tree; its root has no
			// divider bounds (unbounded both ways) and carries user data in
			// its own leaf values, so it needs no leaf-cell hook (nil).
			savedName := ic.treeName
			ic.treeName = string(key)
			ic.checkTreePage(rootPage, nil, nil, nil)
			ic.treeName = savedName
		case rootPage != 0:
			// rootPage == 0 is a valid empty namespace (intentionally skipped;
			// exercised by TestDeleteNamespace_RootPageZero).
			ic.report("tree master cell key %q: namespace root page %d out of range", string(key), rootPage)
		}
	}
	ic.checkTreePage(1, nil, nil, masterLeaf)

	// 3. Check for orphan pages
	// The tooManyErrors check matches SQLite's `sCheck.mxErr` guard in the
	// orphan loop (btree.c:11237), preventing iteration over billions of
	// pages when nPages comes from a corrupted (but capped) header.
	for pgno := uint32(2); pgno <= nPages; pgno++ {
		if ic.tooManyErrors() {
			break
		}
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
