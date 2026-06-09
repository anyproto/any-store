package btree

import (
	"bytes"
	"encoding/binary"
)

// balance.go — faithful port of SQLite's general balancer balance_nonroot()
// (../sqlitec/src/btree.c:8248-9030), specialised to any-store's index btree.
//
// SQLite's balance_nonroot gathers the over-full child plus up to two adjacent
// siblings (NB=3), pools all their cells together with the parent divider
// cells, recomputes the minimum number of output pages k ∈ {nOld-1, nOld,
// nOld+1}, packs each output page as full as it will go (then backs off the
// last one for balance), reassigns dividers into the parent, and frees/reuses
// pages. This is what produces SQLite's high, even page fill. any-store's
// previous splitter used a 2-way split (leafSplitPoint / interiorSplitPoint),
// which never gathered siblings and left a low-fill tail.
//
// The port mirrors SQLite section-for-section; every block cites the btree.c
// line it is derived from. The ONLY deviations from btree.c are the documented
// ones from docs/btree/plans/2026-05-23-balance-nonroot-3sibling.md:
//
//  1. Index-btree only. any-store has no rowid / table-leaf path: page types are
//     only pageTypeLeafIdx / pageTypeIntIdx (page.go:77-80). SQLite's tree-wide
//     leafData (== intKeyLeaf) is therefore always 0. BUT any-store leaf cells
//     carry a value (key+value) whereas SQLite index-leaf cells carry only the
//     key, so the two are NOT interchangeable. The faithful mapping is:
//       - interior siblings  ->  SQLite "index-interior" (leafData==0, !leaf):
//         divider cells are pooled BETWEEN siblings ({leftChild, key}, with the
//         left child's rightChild folded into the divider's child slot, exactly
//         btree.c:8506-8511), redistributed as whole cells, and the boundary
//         cell at cntNew[i] is PROMOTED to the parent (the +1 skip on rebuild).
//       - leaf siblings      ->  SQLite "table-leaf" (leafData==1) for the SIZE
//         accounting: no divider cell is pooled between leaves, no +1 skip, and
//         the boundary cell at cntNew[i] STAYS as the first cell of the right
//         page. The divider promoted to the parent is a key-only COPY of that
//         boundary cell (any-store's existing sepKey = rightCells[0].key). This
//         is the only mechanically-correct choice given any-store's <,>= search
//         invariant (searchInterior, btree.go:907): a key equal to the divider
//         routes to the RIGHT child and must be findable in a leaf there, so a
//         divider is routing-only and the entry stays in the leaf. This is the
//         plan's deviation (2). Concretely the per-balance leafData flag is the
//         negation of "siblings are interior": leafDataLike := b.leaf.
//  2. rebuild*Page instead of editPage. any-store already has the wholesale
//     rebuildLeafPage / rebuildInteriorPage (btree.go:1648 / 1716) == SQLite's
//     rebuildPage (btree.c:7647), which is editPage's own fallback. We use it
//     for every output page rather than porting editPage's incremental
//     pageInsertArray/pageFreeArray two-pass (btree.c:7740-7983). This is the
//     plan's deviation (4). Because every source cell is fully materialised into
//     []cellData (with overflow bytes cloned) BEFORE any output page is written,
//     the two-pass abDone ordering (btree.c:8915-8952) is unnecessary — no
//     output write can clobber a not-yet-read source page. SQLite's apEnd[] /
//     ixNx[] machinery (btree.c:7594-7595), which only serves editPage's
//     in-place edits and rebuildPage's source-page corruption checks, is omitted
//     for the same reason.
//  3. No ptrmap / auto-vacuum (any-store has none): the ISAUTOVACUUM blocks
//     (btree.c:8692-8697, 8790-8830, 8986-8993) are dropped.
//  4. No PagerRekey ascending-pgno sort (btree.c:8713-8741): any-store has no
//     pager rekey primitive; this is a pure locality optimisation. Omitted
//     (accepted micro-drift; pages still come from the freelist/grow so order is
//     already roughly ascending).
//  5. No BTS_FAST_SECURE divider-copy-into-ovflspace (btree.c:8375-8385): no
//     secure-delete mode.
//  6. Cascade via insertSepIntoAncestor recursion instead of SQLite's balance()
//     do-loop unwinding the cursor stack (btree.c:9133-9256): any-store carries a
//     path []pathEntry, not a persistent write cursor. Semantically identical —
//     balance the parent next, up to the root.

const (
	// nbSiblings is SQLite NB (btree.c:7523): the maximum number of pages
	// involved in a single balance (NN=1 neighbour on each side + the page
	// itself). nbMaxOut is NB+2 (btree.c:8270 apNew[NB+2]): the pack loop may
	// briefly need up to two extra output pages before the back-off pass.
	nbSiblings = 3
	nbMaxOut   = nbSiblings + 2
)

// cellArray is the any-store analogue of SQLite's struct CellArray
// (btree.c:7588-7596), specialised to the wholesale-rebuild port. apCell/szCell
// (here: cells/sz) pool every cell to be redistributed, in order:
//
//   - interior siblings: all cells of old page 0, the first divider cell, all
//     cells of old page 1, the second divider, all cells of old page 2. The
//     divider cells carry {leftChild: oldPage.rightChild, key: parentDividerKey}
//     (btree.c:8506-8511).
//   - leaf siblings: all cells of old page 0, all cells of old page 1, all cells
//     of old page 2 — with NO divider cells interleaved (the leafData==1 form;
//     see deviation 1 above).
//
// SQLite's apEnd[]/ixNx[] source-end tracking (btree.c:7594-7595) is omitted:
// see deviation 2. cellData (btree.go:84) already carries {key, value,
// leftChild, overflowPg, rawCell}; collectLeafCells / collectInteriorCells
// (btree.go:1484 / 1589) produce these with leaf overflow cells preserved as
// rawCell passthrough — that IS SQLite's "include overflow cells in apCell[] in
// place" (btree.c:8466-8482).
type cellArray struct {
	cells []cellData // pooled cells (SQLite apCell[])
	sz    []int      // in-page byte size of cells[i], EXCLUDING the 2-byte cell
	//                  pointer (SQLite szCell[]); -1 means "not yet computed"
	//                  (SQLite uses 0 as the sentinel — we use -1 because a
	//                  legitimate cell can have a tiny but never-negative size).
	leaf       bool // sibling pages are leaves (leafCorrection = 4 if true)
	usableSize int  // pager usable page size (pageSize - reserved)
}

// cachedCellSize returns the in-page byte size of cells[n] (excluding the
// 2-byte cell pointer), computing and caching it lazily. Port of SQLite
// cachedCellSize / computeCellSize (btree.c:7619-7632). For a leaf pool the
// size is the leaf cell size (raw passthrough byte length when present, else
// the recomputed size); for an interior pool it is the interior cell size. Both
// account for overflow exactly as the on-page encoders do.
func (b *cellArray) cachedCellSize(n int) int {
	if b.sz[n] >= 0 {
		return b.sz[n]
	}
	c := &b.cells[n]
	var s int
	if b.leaf {
		if c.rawCell != nil {
			s = len(c.rawCell)
		} else {
			s = leafCellSizeWithOverflow(c.key, c.value, b.usableSize)
		}
	} else {
		s = interiorCellSizeWithOverflow(c.key, b.usableSize)
	}
	b.sz[n] = s
	return s
}

// injectedCell is the new cell that just over-filled the target child and must
// be redistributed along with the gathered siblings. It is any-store's analogue
// of SQLite's overflow cell on the over-full page (MemPage.apOvfl[0] at index
// aiOvfl[0]), which balance_nonroot pools at the right spot (btree.c:8466-8482).
// any-store has no on-page overflow-cell mechanism, so the driver passes the new
// cell here instead of writing an over-full page to disk; balanceNonroot injects
// it into the pool while reading the target sibling.
//
//	childSlot — parent child slot of the over-full target sibling (the slot the
//	            descent went through). The cell is injected while pooling the
//	            gathered sibling at this slot.
//	pos       — sorted insert position of the new cell within that sibling.
//	key       — the new cell key. For a LEAF injection it pairs with value as a
//	            leaf cell; for an INTERIOR injection it is the divider key.
//	value     — leaf injection only: the new leaf cell value (rawCell stays nil
//	            so it is encoded fresh, allocating an overflow chain if oversized).
//	interior  — true when injecting an interior divider (a child of the target
//	            interior sibling split); false for a leaf cell injection.
//	key2child — interior injection only: the divider's left child (leftPgno, the
//	            left half of the split child).
//	rightChild— interior injection only: the right half of the split child
//	            (rightPgno), which repoints the child formerly at pos.
type injectedCell struct {
	active     bool
	interior   bool
	childSlot  int
	pos        int
	key        []byte
	value      []byte
	key2child  uint32
	rightChild uint32
}

// balanceNonroot redistributes the cells of the over-full child at
// parentPg[parentIdx] across that child and up to two adjacent siblings, then
// rewrites the parent dividers. Faithful port of SQLite balance_nonroot
// (btree.c:8248-9030), specialised to any-store's index btree (see file header).
//
//	parentPg   — writable parent page (SQLite pParent). Caller retains its pin;
//	             balanceNonroot does NOT release it.
//	parentIdx  — slot in parentPg that the descent went through (SQLite
//	             iParentIdx == path[len-1].cellIdx). 0..cellCount; cellCount
//	             means the descent followed the parent's rightChild.
//	isRoot     — parentPg is the btree root (enables balance-shallower /
//	             balance-deeper of the parent on cascade).
//	parentPath — ancestors strictly above parentPg, for cascading the parent's
//	             own over-full state upward (SQLite's balance() do-loop;
//	             any-store recurses through insertSepIntoAncestor — deviation 6).
//	inject     — the new over-full cell to fold into the pool (see injectedCell);
//	             inject.active==false for a re-balance with no new cell.
//
// On the INSERT path the pooled cell count only ever grows (one cell was just
// added to the over-full child), so nNew ∈ {nOld, nOld+1} and the parent gains
// at most one net cell — it never shrinks, so the parent cannot become under-full
// from an insert. On the DELETE/merge path the new cell is absent
// (inject.active==false) so the pooled count only shrinks: nNew ∈ {nOld-1, nOld},
// k=nOld-1 is a genuine merge that frees the surplus page (Step 11,
// btree.c:9000-9004), and the parent loses a divider. The resulting parent
// under-fullness/emptiness is completed by completeMergeUpward after this returns
// (spec §4; see the rewriteParentAfterBalance OWNERSHIP note). This is exactly
// SQLite's design: ONE balancer for insert-split and delete-merge (btree.c:10010
// drives delete through the same balance() → balance_nonroot()).
func (bt *btree) balanceNonroot(parentPg *page, parentIdx int, isRoot bool, parentPath []pathEntry, inject injectedCell) error {
	bt.pager.balanceNonrootDispatchCount.Add(1)
	usableSize := bt.usablePageSize()

	// ---- Step 1: sibling selection + nxDiv (btree.c:8314-8334) --------------
	// any-store never holds on-page overflow cells on an interior page (the
	// parent is always rebuilt-to-fit or split before balanceNonroot is
	// re-entered), so SQLite's pParent->nOverflow is always 0 here and bBulk is
	// always 0. The arithmetic below is btree.c:8314-8334 with those constants
	// folded in.
	nCellParent := int(parentPg.header.cellCount)
	i := nCellParent // SQLite: i = pParent->nOverflow + pParent->nCell
	var nxDiv int
	if i < 2 {
		nxDiv = 0
	} else {
		if parentIdx == 0 {
			nxDiv = 0
		} else if parentIdx == i {
			nxDiv = i - 2
		} else {
			nxDiv = parentIdx - 1
		}
		i = 2
	}
	nOld := i + 1 // btree.c:8328

	// pRight (btree.c:8329-8334): the parent location holding the pointer to the
	// rightmost gathered sibling. In any-store terms it is either a parent
	// cell's leftChild (childSlot < nCellParent) or parentPg.rightChild
	// (childSlot == nCellParent). rightmostChildSlot is SQLite's
	// (i+nxDiv-pParent->nOverflow) with i==nOld-1.
	rightmostChildSlot := nxDiv + nOld - 1

	// ---- Step 2: read the gathered siblings + their dividers (btree.c:8335-8388)
	// SQLite walks i from nOld-1 down to 0, reading each old page and (for the
	// gaps) dropping the divider cell from the parent. any-store reads the
	// divider keys here (full keys, overflow resolved) but defers the parent
	// rewrite to one wholesale splice at the end (deviation 2): dropping cells
	// in place is unnecessary when the parent is rebuilt from scratch.
	apOld := make([]*page, nOld)
	// dividerKey[g] is the parent divider key separating gathered child g and
	// g+1 (g in 0..nOld-2). For interior siblings it is folded into a pooled
	// divider cell; for leaf siblings it is unused (the divider is re-derived
	// from the boundary cell — deviation 1).
	dividerKey := make([][]byte, nOld-1)

	// getChild resolves a parent child slot (0..nCellParent) to its page
	// number: a cell's leftChild, or the page rightChild. Mirrors SQLite's
	// findCell(pParent, slot)/get4byte and pParent->aData[hdr+8].
	getChild := func(slot int) (uint32, error) {
		if slot < 0 || slot > nCellParent {
			return 0, ErrCorrupt
		}
		if slot == nCellParent {
			return parentPg.header.rightChild, nil
		}
		off, oerr := parentPg.getCellOffsetSafe(slot)
		if oerr != nil {
			return 0, oerr
		}
		if int(off)+4 > len(parentPg.data) {
			return 0, ErrCorrupt
		}
		return binary.BigEndian.Uint32(parentPg.data[int(off) : int(off)+4]), nil
	}

	for g := 0; g < nOld; g++ {
		childSlot := nxDiv + g
		childPgno, err := getChild(childSlot)
		if err != nil {
			return err
		}
		apOld[g], err = bt.pager.getWritablePage(childPgno)
		if err != nil {
			// Release siblings already acquired.
			for j := 0; j < g; j++ {
				bt.pager.releasePage(apOld[j])
			}
			return err
		}
		if g < nOld-1 {
			// The divider separating gathered child g and g+1 is parent cell
			// at slot (nxDiv + g) (see balance.go derivation: divider between
			// child c and c+1 is parent cell P_c). Read its full key.
			dk, err := bt.parentDividerFullKey(parentPg, nxDiv+g, usableSize)
			if err != nil {
				for j := 0; j <= g; j++ {
					bt.pager.releasePage(apOld[j])
				}
				return err
			}
			dividerKey[g] = dk
		}
	}

	// Verify all gathered siblings are the same page type (btree.c:8443-8446).
	leaf := apOld[0].header.isLeaf()
	for g := 1; g < nOld; g++ {
		if apOld[g].header.isLeaf() != leaf {
			for j := 0; j < nOld; j++ {
				bt.pager.releasePage(apOld[j])
			}
			return ErrCorrupt
		}
	}

	// ---- Step 3: pool cells (btree.c:8428-8525) -----------------------------
	// leafCorrection: 4 if siblings are leaves, else 0 (btree.c:8429). In the
	// size loop below it is the per-page header allowance baked into
	// usableSpace; here it also marks whether divider cells are pooled.
	b := &cellArray{leaf: leaf, usableSize: usableSize}

	// tailChildCapture is the rightmost gathered sibling's (possibly
	// injection-repointed) rightChild, captured during pooling for the interior
	// tail-child carry (btree.c:8764-8772). 0 for a leaf balance.
	var tailChildCapture uint32

	// cellBufs keeps the backing buffers from collectLeafCells alive (and to be
	// recycled) for the lifetime of the balance — the pooled leaf cells alias
	// into them via rawCell/key/value.
	var cellBufs [][]byte
	// cntOld[g] = index in b.cells just past the last cell of old page g
	// (pointing AT the divider cell for interior pools). Port of btree.c cntOld.
	cntOld := make([]int, nbMaxOut)
	// szOld[g] = bytes used by old page g's OWN cells incl. their 2-byte
	// pointers, EXCLUDING any divider — equals SQLite's seed
	// szNew[i] = usableSpace - p->nFree (btree.c:8557), which also excludes the
	// divider. Computed directly here from the pooled cell sizes.
	szOld := make([]int, nbMaxOut)

	releaseAll := func() {
		for j := 0; j < nOld; j++ {
			bt.pager.releasePage(apOld[j])
		}
		for _, buf := range cellBufs {
			bt.pager.recycleCellBuf(buf)
		}
	}

	poolFail := func(upTo int) {
		for j := 0; j <= upTo && j < nOld; j++ {
			bt.pager.releasePage(apOld[j])
		}
		for _, bf := range cellBufs {
			bt.pager.recycleCellBuf(bf)
		}
	}

	for g := 0; g < nOld; g++ {
		pOld := apOld[g]
		runStart := len(b.cells)
		// rc is this sibling's effective rightChild for the interleaved divider
		// (and, for the rightmost sibling, the carried tail child). It may be
		// repointed below by an interior cell injection.
		rc := pOld.header.rightChild
		injectHere := inject.active && nxDiv+g == inject.childSlot

		if leaf {
			cells, buf, cerr := bt.collectLeafCells(pOld)
			if cerr != nil {
				// collectLeafCells already recycled its own scratch buffers on
				// failure; release the gathered pages and previously-pooled buffers
				// and propagate up the balance() do-loop (btree.c:9131-9242).
				poolFail(g)
				return cerr
			}
			cellBufs = append(cellBufs, buf)
			// Inject the new over-full LEAF cell at its sorted position in the
			// target sibling (btree.c:8466-8482 pools MemPage.apOvfl[] at
			// aiOvfl[0]).
			if injectHere {
				if inject.pos < 0 || inject.pos > len(cells) {
					bt.pager.recycleCellSlice(cells)
					poolFail(g)
					return ErrCorrupt
				}
				b.cells = append(b.cells, cells[:inject.pos]...)
				b.cells = append(b.cells, cellData{key: inject.key, value: inject.value})
				b.cells = append(b.cells, cells[inject.pos:]...)
			} else {
				b.cells = append(b.cells, cells...)
			}
			// collectLeafCells took a pooled []cellData; we appended copies, so
			// return the source slice to the pager immediately. The injected cell
			// payload is owned by the caller (driver), not this slice.
			bt.pager.recycleCellSlice(cells)
		} else {
			cells, cerr := bt.collectInteriorCells(pOld)
			if cerr != nil {
				// Release the gathered pages and previously-pooled buffers and
				// propagate up the balance() do-loop (btree.c:9131-9242).
				poolFail(g)
				return cerr
			}
			// The collectInteriorCells wrapper self-recycles its scratch buffer to
			// the pager free-list, so non-overflow cell keys alias a buffer that the
			// NEXT collectInteriorCells call (for the next gathered sibling) may
			// reuse. Clone every key into balance-owned memory before pooling so each
			// sibling's keys survive the whole redistribution. (rewriteParentAfter
			// Balance avoids this clone by using collectInteriorCellsKeepBuf and
			// holding the buffer across the rebuild instead.)
			for j := range cells {
				cells[j].key = bytes.Clone(cells[j].key)
			}
			// Inject the new over-full INTERIOR divider {leftChild: leftPgno,
			// key} at inject.pos, repointing the child that was at inject.pos to
			// inject.rightPgno — exactly insertSepIntoInterior's expanded-cell
			// construction (btree.go:2027-2050). For an interior balance the
			// injected cell is the divider promoted when a child of pOld split.
			if injectHere {
				if inject.pos < 0 || inject.pos > len(cells) {
					poolFail(g)
					return ErrCorrupt
				}
				newDiv := cellData{leftChild: inject.key2child, key: inject.key}
				expanded := make([]cellData, 0, len(cells)+1)
				expanded = append(expanded, cells[:inject.pos]...)
				expanded = append(expanded, newDiv)
				expanded = append(expanded, cells[inject.pos:]...)
				if inject.pos < len(cells) {
					// The child formerly at inject.pos now sits at inject.pos+1.
					expanded[inject.pos+1].leftChild = inject.rightChild
				} else {
					// Injected at the end: repoint this sibling's rightChild.
					rc = inject.rightChild
				}
				cells = expanded
			}
			b.cells = append(b.cells, cells...)
		}
		used := 0
		for j := runStart; j < len(b.cells); j++ {
			b.sz = append(b.sz, -1)
			used += 2 + b.cachedCellSize(j)
		}
		szOld[g] = used
		cntOld[g] = len(b.cells)
		// Record the rightmost sibling's (possibly repointed) rightChild for the
		// interior tail-child carry (btree.c:8764-8772).
		if !leaf && g == nOld-1 {
			tailChildCapture = rc
		}

		// Interior: pool the divider cell after this old page (btree.c:8493-8511).
		// {leftChild: rc, key: dividerKey[g]} — folds the left child's
		// (possibly repointed) rightChild into the divider's child slot
		// (btree.c:8506-8511).
		if g < nOld-1 && !leaf {
			div := cellData{leftChild: rc, key: dividerKey[g]}
			b.cells = append(b.cells, div)
			b.sz = append(b.sz, -1)
			cntOld[g] = len(b.cells) - 1 // cntOld points AT the divider
			_ = b.cachedCellSize(len(b.cells) - 1)
		}
	}
	nCellPool := len(b.cells)

	// ---- Step 4 + 5: usableSpace, seed szNew, and the k / split-point pack
	// loop (btree.c:8543-8605). leafDataLike negates "interior siblings": it is
	// true for leaf pools (no interleaved divider, no divider rotation, no +1
	// skip) and false for interior pools — see deviation 1. With it the loops
	// below are btree.c:8543-8605 transcribed structure-for-structure.
	leafCorrection := 0
	if leaf {
		leafCorrection = 4
	}
	leafDataLike := leaf                            // see file header deviation (1)
	usableSpace := usableSize - 12 + leafCorrection // btree.c:8543

	cntNew := make([]int, nbMaxOut)
	szNew := make([]int, nbMaxOut)
	for g := 0; g < nOld; g++ {
		szNew[g] = szOld[g]   // btree.c:8557 (seed = used bytes of old page g)
		cntNew[g] = cntOld[g] // btree.c:8561
	}

	k := nOld
	for g := 0; g < k; g++ {
		// Push cells right while page g is over its budget (btree.c:8564-8584).
		for szNew[g] > usableSpace {
			if g+1 >= k {
				k = g + 2
				if k > nbMaxOut { // btree.c:8569
					releaseAll()
					return ErrCorrupt
				}
				szNew[k-1] = 0
				cntNew[k-1] = nCellPool
			}
			sz := 2 + b.cachedCellSize(cntNew[g]-1)
			szNew[g] -= sz
			if !leafDataLike { // btree.c:8575-8581 (divider rotation)
				if cntNew[g] < nCellPool {
					sz = 2 + b.cachedCellSize(cntNew[g])
				} else {
					sz = 0
				}
			}
			szNew[g+1] += sz
			cntNew[g]--
		}
		// Pull cells right while they fit (btree.c:8585-8598).
		for cntNew[g] < nCellPool {
			sz := 2 + b.cachedCellSize(cntNew[g])
			if szNew[g]+sz > usableSpace {
				break
			}
			szNew[g] += sz
			cntNew[g]++
			if !leafDataLike {
				if cntNew[g] < nCellPool {
					sz = 2 + b.cachedCellSize(cntNew[g])
				} else {
					sz = 0
				}
			}
			szNew[g+1] -= sz
		}
		if cntNew[g] >= nCellPool { // btree.c:8599-8600
			k = g + 1
		} else if cntNew[g] <= boundaryLeft(cntNew, g) { // btree.c:8601-8604
			releaseAll()
			return ErrCorrupt
		}
	}

	// ---- Step 6: back off the last page (btree.c:8618-8649) -----------------
	// This is "not optional": the pack above is left-biased and could leave the
	// rightmost page empty/illegal. bBulk is always 0 for any-store.
	for g := k - 1; g > 0; g-- {
		szRight := szNew[g]
		szLeft := szNew[g-1]
		r := cntNew[g-1] - 1
		var d int
		if leafDataLike {
			d = r // btree.c:8625 d = r + 1 - leafData; leafData==1 -> d=r
		} else {
			d = r + 1 // leafData==0 -> d=r+1 (the divider cell)
		}
		_ = b.cachedCellSize(d)
		for {
			if r < 0 {
				break
			}
			szR := b.cachedCellSize(r)
			szD := b.sz[d]
			extra := 2
			if g == k-1 {
				extra = 0
			}
			if szRight != 0 && szRight+szD+2 > szLeft-(szR+extra) { // btree.c:8633-8635
				break
			}
			szRight += szD + 2
			szLeft -= szR + 2
			cntNew[g-1] = r
			r--
			d--
		}
		szNew[g] = szRight
		szNew[g-1] = szLeft
		if cntNew[g-1] <= boundaryLeft(cntNew, g-1) { // btree.c:8645-8648
			releaseAll()
			return ErrCorrupt
		}
	}

	nNew := k
	bt.pager.lastBalanceNOld.Store(int64(nOld))
	bt.pager.lastBalanceNNew.Store(int64(nNew))

	// tailChild (btree.c:8764-8772): the rightmost output page's rightChild for
	// interior pools, taken from the rightmost gathered sibling's rightChild
	// (captured during pooling, after any injection repoint). Always carried in
	// any-store regardless of nOld==nNew (the pooled cells fully determine the
	// child sequence either way).
	tailChild := tailChildCapture

	// ---- Step 7: allocate k pages, reuse old (btree.c:8668-8699) ------------
	// Reuse apOld[0..min(nOld,nNew)-1] in place; allocate the rest. The pgno
	// ascending sort (btree.c:8713-8741) is omitted — deviation 4.
	apNew := make([]*page, nNew)
	for g := 0; g < nNew; g++ {
		if g < nOld {
			apNew[g] = apOld[g]
			apOld[g] = nil // ownership moves to apNew (btree.c:8673)
		} else {
			np, err := bt.pager.allocatePage()
			if err != nil {
				// Release everything acquired so far.
				for j := 0; j < nNew; j++ {
					bt.pager.releasePage(apNew[j])
				}
				for j := 0; j < nOld; j++ {
					bt.pager.releasePage(apOld[j])
				}
				for _, buf := range cellBufs {
					bt.pager.recycleCellBuf(buf)
				}
				return err
			}
			apNew[g] = np
		}
	}

	// ---- Step 8 (tail child) + Step 9 (rebuild output pages) ----------------
	// Compute the new divider keys and rebuild every output page from the pool.
	// For interior pools the rightmost old sibling's rightChild is carried to
	// the rightmost output page (btree.c:8764-8772); the intermediate output
	// pages take their rightChild from the promoted boundary cell's leftChild
	// (btree.c:8846-8847). For leaf pools the boundary cell stays on the right
	// page and the divider is a key-copy (deviation 1).
	//
	// Because all source cells were materialised into b.cells before this loop,
	// rebuilding apNew[g] (which may alias apOld[g]) can never clobber an
	// unread source page — so SQLite's two-pass abDone ordering
	// (btree.c:8915-8952) is unnecessary (deviation 2).
	newDivKey := make([][]byte, nNew-1) // divider key between apNew[g], apNew[g+1]

	start := 0
	for g := 0; g < nNew; g++ {
		boundary := cntNew[g] // index of the boundary/divider cell after page g
		var err error
		if g == nNew-1 {
			boundary = nCellPool // last page takes everything remaining
		}
		if leaf {
			// Page g cells: [start, boundary). Boundary cell (first cell of the
			// next page) stays — no +1 skip.
			err = bt.rebuildLeafPage(apNew[g], b.cells[start:boundary])
			if err == nil && g < nNew-1 {
				// Divider = key-copy of the first cell of the next page
				// (b.cells[boundary]) — any-store's <,>= invariant.
				dk, kerr := bt.cellFullKey(&b.cells[boundary])
				if kerr != nil {
					err = kerr
				} else {
					newDivKey[g] = bytes.Clone(dk)
				}
			}
			start = boundary
		} else {
			// Interior: page g cells: [start, boundary). The cell at boundary is
			// the divider, PROMOTED to the parent (the +1 skip). Its leftChild
			// becomes page g's rightChild (btree.c:8846-8847).
			var rc uint32
			if g == nNew-1 {
				rc = tailChild
			} else {
				rc = b.cells[boundary].leftChild
			}
			err = bt.rebuildInteriorPage(apNew[g], b.cells[start:boundary], rc)
			if err == nil && g < nNew-1 {
				newDivKey[g] = bytes.Clone(b.cells[boundary].key)
			}
			start = boundary + 1 // +1 skip: boundary divider promoted
		}
		if err != nil {
			for j := 0; j < nNew; j++ {
				bt.pager.releasePage(apNew[j])
			}
			for j := 0; j < nOld; j++ {
				bt.pager.releasePage(apOld[j])
			}
			for _, buf := range cellBufs {
				bt.pager.recycleCellBuf(buf)
			}
			return err
		}
	}

	// Capture the new pgnos before releasing the output pages.
	newPgno := make([]uint32, nNew)
	for g := 0; g < nNew; g++ {
		newPgno[g] = apNew[g].pgno
	}

	// ---- Free surplus old pages (btree.c:9000-9004) -------------------------
	// Pages apOld[nNew..nOld) were not reused. Release our pin first (freePage
	// re-acquires the trunk via getWritablePage and marks the freed page
	// dontWrite), matching the merge path (btree.go:2552-2556).
	surplus := make([]uint32, 0, nOld)
	for g := nNew; g < nOld; g++ {
		if apOld[g] != nil {
			surplus = append(surplus, apOld[g].pgno)
			bt.pager.releasePage(apOld[g])
			apOld[g] = nil
		}
	}

	// Release output page pins now that their content is written; the writer
	// cache keeps them dirty for commit.
	for g := 0; g < nNew; g++ {
		bt.pager.releasePage(apNew[g])
	}
	// Release any remaining old pins (defensive; normally all moved/freed).
	for g := 0; g < nOld; g++ {
		if apOld[g] != nil {
			bt.pager.releasePage(apOld[g])
			apOld[g] = nil
		}
	}
	for _, buf := range cellBufs {
		bt.pager.recycleCellBuf(buf)
	}

	for _, pgno := range surplus {
		if err := bt.freePageDeferred(pgno); err != nil {
			return err
		}
	}

	// ---- Step 12 (balance-shallower, btree.c:8960-8985) is NOT done in-line
	// here. When dispatched from the INSERT path the pooled cell count only grows
	// (nNew>=nOld), so the parent never empties and Step 12 cannot apply. When
	// dispatched from the DELETE/merge path (deleteRebalanceLeaf /
	// deleteRebalanceInterior, inject.active=false) the parent CAN empty or
	// under-fill, but that completion is driven AFTER this function returns by
	// completeMergeUpward (btree.go) — which re-reads the parent fresh and reuses
	// collapseSingleChild for the balance-shallower collapse (spec §4b). Keeping
	// it out of balanceNonroot preserves the single pParent-pin borrow contract
	// for both directions (see rewriteParentAfterBalance OWNERSHIP note).
	//
	// ---- Step 10: rewrite the parent dividers (btree.c:8759-8891) -----------
	// Build the parent's new cell list by splicing the gathered child run with
	// the nNew output pages and nNew-1 new dividers. Then write it (rebuild if
	// it fits; otherwise 2-way split + cascade through insertSepIntoAncestor —
	// deviation 6). put4byte(pRight,...) (btree.c:8759) and the divider inserts
	// at slot nxDiv+i (btree.c:8888) are subsumed by the wholesale splice.
	return bt.rewriteParentAfterBalance(parentPg, nxDiv, nOld, rightmostChildSlot,
		newPgno, newDivKey, isRoot, parentPath)
}

// boundaryLeft returns cntNew[g-1] for g>0, else 0 — SQLite's
// "(i>0 ? cntNew[i-1] : 0)" guard expression (btree.c:8601, 8645).
func boundaryLeft(cntNew []int, g int) int {
	if g > 0 {
		return cntNew[g-1]
	}
	return 0
}

// parentDividerFullKey reads the full divider key at parent cell slot,
// resolving an overflow key chain if present. Mirrors how SQLite reads
// apDiv[i] from the parent (btree.c:8359-8361) — but returns the key only,
// since any-store re-derives the child pointers during the splice. The
// returned key is a fresh copy (interiorFullKey allocates), safe to retain
// after the parent is rewritten.
func (bt *btree) parentDividerFullKey(parentPg *page, slot int, usableSize int) ([]byte, error) {
	off, oerr := parentPg.getCellOffsetSafe(slot)
	if oerr != nil {
		return nil, oerr
	}
	key, _, err := bt.interiorCellFullKey(parentPg.data, int(off), usableSize)
	if err != nil {
		return nil, err
	}
	// interiorFullKey may return a sub-slice of pg.data for non-overflow keys;
	// clone so the key survives the parent rebuild.
	return bytes.Clone(key), nil
}

// freePageDeferred frees a page that is no longer reachable, used for the
// surplus old siblings after a balance (btree.c:9000-9004). The page's pin must
// already have been released by the caller (freePage re-acquires the freelist
// trunk via getWritablePage and marks the freed page dontWrite), matching the
// merge path's freePage usage (btree.go:2554).
func (bt *btree) freePageDeferred(pgno uint32) error {
	return bt.pager.freePage(pgno)
}

// rewriteParentAfterBalance rewrites parentPg's cells to reflect the balance:
// it splices out the gathered child run (children [nxDiv, nxDiv+nOld), with
// their nOld-1 internal dividers) and splices in the nNew output pages with
// their nNew-1 new dividers, then writes the result. This subsumes SQLite's
// put4byte(pRight,...) (btree.c:8759) and the per-divider insertCell at slot
// nxDiv+i (btree.c:8888) into one wholesale rebuild — the editPage replacement
// (deviation 2). If the rewritten parent does not fit, it is split 2-way and
// the middle divider cascades up via insertSepIntoAncestor (deviation 6), which
// also handles the root-overflow case (splitRoot / balance_deeper).
//
// OWNERSHIP: the caller (balanceNonroot) retains parentPg's pin; this function
// does NOT release it. The DELETE/merge-direction completion (empty-collapse and
// underfull-parent cascade, spec §4) is therefore NOT done here — it is driven
// after balanceNonroot returns and the caller has released parentPg, by
// completeMergeUpward (btree.go), which re-reads the parent fresh. That keeps the
// pin contract identical for the insert and delete paths and lets the cascade
// release the parent's pin before re-acquiring it as a gathered sibling under its
// grandparent (as SQLite does, btree.c:9251).
//
// On the INSERT direction nNew ∈ {nOld, nOld+1}, so the rewritten parent only
// ever fits (nNew==nOld) or over-fills (nNew==nOld+1, the 2-way split below); it
// never under-fills. On the DELETE direction nNew ∈ {nOld-1, nOld}; the merge
// (nNew==nOld-1) removes a divider, so the parent always FITS here (fewer cells
// than before) — the over-full split below is unreachable from delete — and any
// resulting under-fullness/emptiness is handled by completeMergeUpward.
//
// Parent child/divider geometry (any-store <,>= invariant, searchInterior
// btree.go:907): for an interior page with cells P_0..P_{nc-1} and rightChild,
// the children are P_0.leftChild..P_{nc-1}.leftChild, rightChild (nc+1 of them),
// and the divider key separating child c and child c+1 is P_c.key (0<=c<nc).
func (bt *btree) rewriteParentAfterBalance(
	parentPg *page,
	nxDiv, nOld, rightmostChildSlot int,
	newPgno []uint32,
	newDivKey [][]byte,
	isRoot bool,
	parentPath []pathEntry,
) error {
	usableSize := bt.usablePageSize()
	nNew := len(newPgno)

	// Collect the parent's cells (full keys; frees parent overflow chains, which
	// rebuildInteriorPage re-creates as needed — same contract as the existing
	// interior split, btree.go:2023). After this the parent's old divider chains
	// are gone; we rebuild from childPgnos + divKeys below.
	parentCells, parentBuf, cerr := bt.collectInteriorCellsKeepBuf(parentPg)
	if cerr != nil {
		// Caller (balanceNonroot) retains parentPg's pin; do not release it here,
		// matching the ErrCorrupt path below. Propagate up the balance() do-loop
		// (btree.c:9131-9242).
		return cerr
	}
	// Hold the collected-cell scratch buffer for the whole rebuild so the parent's
	// divider keys can be spliced by aliasing (oldDivs below) instead of cloning
	// each one — the former per-divider bytes.Clone was the largest object
	// producer of the index-build profile. parentBuf is read-only here (never
	// appended to), so the aliases stay stable; the defer returns it to the
	// free-list at every exit, after the parent rebuild(s) have consumed newCells.
	// The promoted sepKey on the split path is independently cloned below, so it
	// survives this recycle and the cascade up the tree.
	defer bt.pager.recycleCellBuf(parentBuf)
	nc := len(parentCells)
	parentRightChild := parentPg.header.rightChild

	// Old children (nc+1) and old divider keys (nc). oldDivs aliases parentBuf
	// (non-overflow keys) / the freshly-read overflow keys — both valid until the
	// deferred recycle above, which fires only after newCells is rebuilt.
	oldChildren := make([]uint32, nc+1)
	oldDivs := make([][]byte, nc)
	for j := 0; j < nc; j++ {
		oldChildren[j] = parentCells[j].leftChild
		oldDivs[j] = parentCells[j].key
	}
	oldChildren[nc] = parentRightChild

	// parentCells (the descriptor slice) is fully consumed: every leftChild is
	// copied into oldChildren and every key header aliased into oldDivs (the key
	// BYTES live in parentBuf, recycled separately by the defer above). Return the
	// slice to the pool now — before the divider splice and the cascade — so the
	// next balanceNonroot up the tree can reuse it. recycleCellSlice clears the
	// entries, which does not disturb oldDivs (it holds its own header copies).
	bt.pager.recycleCellSlice(parentCells)

	// Bounds: the gathered run children are [nxDiv, nxDiv+nOld); valid child
	// slots are 0..nc. rightmostChildSlot == nxDiv+nOld-1.
	if nxDiv < 0 || nxDiv+nOld > nc+1 || rightmostChildSlot != nxDiv+nOld-1 {
		return ErrCorrupt
	}

	// Splice children: keep [0, nxDiv); insert newPgno; keep [nxDiv+nOld, nc].
	newChildren := make([]uint32, 0, nc+1-nOld+nNew)
	newChildren = append(newChildren, oldChildren[:nxDiv]...)
	newChildren = append(newChildren, newPgno...)
	newChildren = append(newChildren, oldChildren[nxDiv+nOld:]...)

	// Splice dividers. Old internal dividers of the run are oldDivs[nxDiv ..
	// nxDiv+nOld-2]. Kept dividers: [0, nxDiv) (left, incl. div_{nxDiv-1}
	// connecting kept child to newPgno[0]) and [nxDiv+nOld-1, nc) (right, incl.
	// div_{nxDiv+nOld-1} connecting newPgno[last] to the kept child after the
	// run). The new internal dividers (nNew-1) go between the spliced children.
	newDivs := make([][]byte, 0, nc-(nOld-1)+(nNew-1))
	newDivs = append(newDivs, oldDivs[:nxDiv]...)
	newDivs = append(newDivs, newDivKey...)
	if nxDiv+nOld-1 < nc {
		newDivs = append(newDivs, oldDivs[nxDiv+nOld-1:]...)
	}

	// Sanity: a valid interior page has len(children) == len(dividers)+1.
	if len(newChildren) != len(newDivs)+1 {
		return ErrCorrupt
	}

	// Build the new parent cell list: P'_j = {leftChild: newChildren[j],
	// key: newDivs[j]} for j in 0..len(newDivs)-1; rightChild = last child.
	newCells := make([]cellData, len(newDivs))
	for j := range newDivs {
		newCells[j] = cellData{leftChild: newChildren[j], key: newDivs[j]}
	}
	newRightChild := newChildren[len(newChildren)-1]

	// Does the rewritten parent fit? Compute content + pointers vs usable space
	// (interior header is 12 bytes; +dbHeader on page 1). Mirrors the fit check
	// in insertSepIntoInterior (btree.go:1963-1975) generalised to a cell list.
	hdrOff := 0
	if parentPg.pgno == 1 {
		hdrOff = dbHeaderSize
	}
	used := hdrOff + 12 + len(newCells)*2
	for j := range newCells {
		used += interiorCellSizeWithOverflow(newCells[j].key, usableSize)
	}
	if used <= usableSize {
		// Fits: rebuild the parent wholesale. The merge-direction completion
		// (empty-collapse / underfull-cascade, spec §4) is handled by the caller
		// via completeMergeUpward after parentPg is released — see the OWNERSHIP
		// note above.
		return bt.rebuildInteriorPage(parentPg, newCells, newRightChild)
	}

	// Over-full parent: split 2-way and cascade up (deviation 6). On the insert
	// path nNew<=nOld+1 so the parent gained at most one net cell; a single
	// 2-way split always suffices to relieve the overflow. The middle cell is
	// promoted; its leftChild becomes the left page's rightChild; the right
	// page keeps newRightChild. This is the same shape as insertSepIntoInterior's
	// slow path (btree.go:2039-2075).
	mid := interiorSplitPoint(newCells, usableSize)
	leftCells := newCells[:mid]
	sepKey := bytes.Clone(newCells[mid].key)
	rightCells := newCells[mid+1:]
	leftRightChild := newCells[mid].leftChild

	newRightPg, err := bt.pager.allocatePage()
	if err != nil {
		return err
	}
	if err := bt.rebuildInteriorPage(parentPg, leftCells, leftRightChild); err != nil {
		bt.pager.releasePage(newRightPg)
		return err
	}
	if err := bt.rebuildInteriorPage(newRightPg, rightCells, newRightChild); err != nil {
		bt.pager.releasePage(newRightPg)
		return err
	}
	newRightPgno := newRightPg.pgno
	bt.pager.releasePage(newRightPg)

	parentPgno := parentPg.pgno
	// Cascade: insert the promoted divider into the grandparent. When parentPg is
	// the root, insertSepIntoAncestor detects it (leftPgno == bt.rootPage) and
	// routes to splitRoot (balance_deeper), matching SQLite btree.c:9152-9169
	// where a root overflow goes through balance_deeper first — so the isRoot
	// flag (== (len(parentPath)==0) here) needs no separate branch; it is
	// threaded only for the balance-shallower note in balanceNonroot.
	_ = isRoot
	return bt.insertSepIntoAncestor(parentPgno, sepKey, newRightPgno, parentPath)
}
