# balance_nonroot 3-sibling redistribution — faithful port of SQLite's general balance

## Overview

- any-store v2's btree splits an over-full / overflowing page with a **2-way
  split** (`leafSplitPoint` / `interiorSplitPoint` + `rebuild*Page`), then
  propagates one divider upward. It never gathers sibling pages.
- SQLite's `balance_nonroot()` (`btree.c:8248`) is the **general** balancer:
  it gathers the target page **plus up to two adjacent siblings** (NB=3),
  pools ALL their cells **and** the parent divider cells into one `apCell[]`
  array, recomputes the minimum number of output pages `k ∈ {nOld−1, nOld,
  nOld+1}`, packs each output page as full as it will go (then backs off the
  last one for balance), reassigns dividers into the parent, and frees/reuses
  pages. This is what produces SQLite's high, even page-fill.
- This plan replaces any-store's 2-way split with a **faithful port of
  `balance_nonroot`** — same gather→size→redistribute→reassign→alloc/free→
  parent-rewrite structure, same NB=3 / pack-then-back-off / k-selection
  invariants — while **keeping `balance_quick`** (`splitLeafRightmostAppend`)
  as the rightmost-append fast path. Closes the **"No Full 3-Sibling
  Redistribution on Split"** drift in [NOTES.md:1440](../NOTES.md).
- Scope is the **insert/over-full balance path** only. The delete-side
  underfull merge (`tryMergeLeaf` / `removeChildFromParent`) and root growth
  (`splitRoot` = `balance_deeper`) are reused as-is; this plan funnels them
  through, and is compatible with, the new general `balance`.
- **No on-disk format change.** Page layout, cell encoding, freelist,
  codec/checksum, and WAL framing are untouched. Rollback = revert the commit.

## Motivation (measured)

Existing measurement test `TestBalanceQuick_AppendFillFactor`
(`btree_balance_quick_test.go:32`), pageSize=1024 (usable=1024 after the
8-byte reserve → 1032 total), valSize=80, 4-byte keys, 5000 rows, single tx:

| workload          | leaves | ideal | overhead | avg fill | median | min   |
|-------------------|-------:|------:|---------:|---------:|-------:|------:|
| monotonic_append  |    488 |   455 |    +7.3% |    88.7% |  95.3% | 60.6% |
| **random**        |    631 |   455 |   +38.7% |    68.6% |  69.3% |  8.7% |

Monotonic append is already near-ideal because `balance_quick` keeps the full
left leaf intact (resolved 2026-04-23). The **random-insert** case is where the
missing 3-sibling redistribution costs us: **+38.7% extra leaves** and a long
low-fill tail (min 8.7%). This translates directly to a larger file, more
pages touched per scan, and more WAL frames per checkpoint.

Why 68.6% and not "50%": any-store's `leafSplitPoint`/`interiorSplitPoint`
already bias the **left** page to ~2/3 fill (`btree.go:252`, `btree.go:303`),
and the fresh ~1/3-full right page refills under subsequent random inserts to a
~68% steady state. So the realistic win is **~68% → ~75%** average leaf fill
and the elimination of the low-fill tail (the partially-emptied right pages),
**not** a 50→70 jump. The structural win is the leaf-count reduction toward the
~455 ideal: SQLite achieves it because every split that touches a low-fill
neighbour re-packs that neighbour too, and a split only adds a page (k=nOld+1)
when the pooled cells genuinely do not fit in nOld pages.

## Context (from discovery)

### SQLite reference — `../sqlitec/src/btree.c`

| Symbol | Line | Role |
|--------|------|------|
| `#define NN 1` / `#define NB 3` | `btree.c:7522-7523` | 1 neighbour each side; 3 pages total. |
| `struct CellArray` | `btree.c:7588-7596` | `apCell[]`/`szCell[]` pool + `apEnd[NB*2]`/`ixNx[NB*2]` source-end tracking. |
| `populateCellCache` / `computeCellSize` / `cachedCellSize` | `btree.c:7602-7632` | Lazy per-cell size; `szCell[i]==0` means "not computed". |
| `rebuildPage` | `btree.c:7647-7714` | Rewrite a page wholesale from `apCell[iFirst..iFirst+nCell)`. |
| `pageInsertArray` / `pageFreeArray` | `btree.c:7740-7862` | Incremental insert/free used by `editPage`. |
| `editPage` | `btree.c:7876-7983` | In-place re-edit of a reused sibling (free head/tail, shift, insert), falling back to `rebuildPage`. |
| `balance_quick` | `btree.c:8010-8104` | Rightmost-append fast path (already ported). |
| `copyNodeContent` | `btree.c:8166` | Copy a node's body to another page (root collapse / deepen). |
| **`balance_nonroot`** | **`btree.c:8248-9030`** | **The general balancer (this port).** |
| `balance_deeper` | `btree.c:9052-9097` | Root grows by one level. |
| `balance` (driver) | `btree.c:9133-9262` | do-loop; picks quick vs nonroot vs deeper; sibling/`iIdx` selection. |

Sub-structure of `balance_nonroot`, with the lines this port mirrors:

1. **Sibling selection + divider drop** `btree.c:8314-8388`. `nxDiv` =
   first divider slot; `nOld = i+1` (≤3); copy `apDiv[]`, record `szNew[i]`,
   `dropCell()` each divider from the parent so the parent has no overflow for
   the rest of the routine.
2. **Pool cells** `btree.c:8428-8525`. For each old page: append its cells
   (incl. overflow cells in place) to `b.apCell[]`; record `cntOld[i]`; for
   index btrees append a **copy** of the divider (into `aSpace1`), with the
   left child's rightChild folded into the divider's child-pointer slot
   (`btree.c:8506-8511`). `leafCorrection = leaf*4`; `leafData = intKeyLeaf`.
3. **Initial sizing** `btree.c:8543-8562`. `usableSpace = usableSize − 12 +
   leafCorrection`; seed `szNew[i]` = used bytes of old page i; set
   `apEnd[]`/`ixNx[]`.
4. **k + split points** `btree.c:8563-8605`. The pack loop: while a page
   exceeds `usableSpace`, push a cell right (growing `k` to `i+2`, capped at
   `NB+2=5`); then pull cells right while they fit. Sets `k`, `cntNew[i]`,
   `szNew[i]`.
5. **Back-off the last page** `btree.c:8618-8649`. Right-to-left pass moves
   cells from left sibling to right sibling until the balance condition
   `szRight+szD+2 > szLeft−(szR+...)` trips — "not optional", prevents an
   empty/illegal rightmost page.
6. **Allocate k pages, reuse old** `btree.c:8668-8699`; **sort by pgno**
   `btree.c:8713-8741` (locality; uses `PagerRekey`).
7. **Parent right-pointer + divider reassignment** `btree.c:8759-8891`:
   `put4byte(pRight, apNew[nNew-1]->pgno)`; for non-leaf, carry the old
   rightmost child pointer (`btree.c:8764-8772`); insert `nNew−1` new dividers
   into the parent at `nxDiv+i` (`insertCell`, `btree.c:8888`).
8. **Edit/rebuild siblings in a crash-safe order** `btree.c:8915-8952`:
   two-pass `iPg` loop (down `nNew-1→0`, back up) honoring move-left/move-right
   safety; each page via `editPage`, `nFree = usableSpace − szNew[iPg]`.
9. **Root collapse / free unused** `btree.c:8960-9004`: balance-shallower if
   root emptied; `freePage()` old pages `nNew..nOld`.

### any-store side — `internal/btree/`

Current over-full / split path (no sibling gather anywhere):

| Function | Line | Role |
|----------|------|------|
| `Put` | `btree.go:1111` | Descends building `path []pathEntry`; calls `insertIntoLeafWithPath`. |
| `insertIntoLeafWithPath` | `btree.go:1170` | In-place insert, else defrag-rebuild, else `splitLeafAndInsertWithPath` (`btree.go:1213`). |
| `splitLeafAndInsertWithPath` | `btree.go:1829` | `balance_quick` dispatch (`btree.go:1835`), else 2-way: `leafSplitPoint` → 2× `rebuildLeafPage` → `insertIntoParentWithPath`. |
| `splitLeafRightmostAppend` | `btree.go:1794` | `balance_quick` port (KEEP). |
| `leafSplitPoint` / `interiorSplitPoint` | `btree.go:244` / `btree.go:295` | 2/3-on-left split index. |
| `rebuildLeafPage` / `rebuildInteriorPage` | `btree.go:1648` / `btree.go:1716` | Wholesale page rewrite from `[]cellData` (≈ `rebuildPage`). |
| `collectLeafCells` / `collectInteriorCells` | `btree.go:1484` / `btree.go:1589` | Parse a page into `[]cellData`; overflow cells kept as `rawCell` passthrough. |
| `insertIntoParentWithPath` / `insertSepIntoInterior` / `insertSepIntoAncestor` | `btree.go:1889` / `1951` / `1913` | Divider insert into parent; recursive parent split. |
| `splitRoot` | `btree.go:2188` | `balance_deeper` port (KEEP). |
| `tryMergeLeaf` / `removeChildFromParent` | `btree.go:2412` / `2575` | Delete-side underfull merge (out of scope; KEEP). |

Documented drift: **[NOTES.md:1440](../NOTES.md)** — *"No Full 3-Sibling
Redistribution on Split — Severity: Important (partially addressed). SQLite's
`balance_nonroot()` collects cells from up to 3 siblings and redistributes them
targeting ~67% fill across all siblings. Our implementation uses a 2-way split
targeting 2/3 fill on the left page. This captures most of the benefit without
the complexity of multi-sibling redistribution."* This plan removes the
"partially addressed" qualifier by porting the full algorithm.

Mapping rows currently pointing the 2-way helpers at `balance_nonroot`
(`docs/btree/mappings/go_to_sqlite.json`): `splitLeafAndInsert` (`:334`),
`splitLeafAndInsertWithPath` (`:340`), `insertSepIntoInterior` (`:288`),
`leafSplitPoint` (`:438`), `interiorSplitPoint` (`:404`). These will be
re-pointed at the new `balanceNonroot` and its helpers.

### Critical "cannot be 1:1" facts (drive the whole design)

1. **any-store is index-btree only.** Page types are only `pageTypeLeafIdx`
   (10) / `pageTypeIntIdx` (2) (`page.go:77-80`). In SQLite terms `leafData`
   (= `intKeyLeaf`) is **always 0** and there is **no table/rowid leaf-data
   path**. ⇒ The port omits SQLite's `leafData==1` branches
   (`btree.c:8848-8859`, the `!leafData` guards in the size loops, the
   `cntOld[i]+1` divider accounting). `leafCorrection` is still 4 for leaf
   siblings / 0 for interior siblings, exactly as SQLite for index btrees.
2. **Divider semantics differ** ([NOTES.md:1464-1472](../NOTES.md)). any-store
   `searchInterior` (`btree.go:911`) uses "left child keys `<` separator,
   right child keys `>=` separator"; SQLite intkey uses `<=`. For an index
   btree the divider promoted between sibling i and i+1 is **the full first key
   of the right sibling** (any-store's existing `sepKey = rightCells[0].key`),
   which equals the cell at `cntNew[i]` in the pool — semantically the same
   cell SQLite promotes. The mechanical difference (SQLite strips the 4-byte
   child pointer off a non-leaf divider and folds in the left child's
   rightChild; the leaf case promotes a copy with `leafCorrection`) is handled
   by any-store's `cellData{leftChild, key}` model directly.
3. **No `MemPage` mutation in place / no rekey / no ptrmap.** any-store has no
   auto-vacuum (no `ptrmap*` calls — drop all `ISAUTOVACUUM` blocks
   `btree.c:8064-8069, 8692-8697, 8790-8830, 8986-8993`). The `PagerRekey`
   ascending-pgno sort (`btree.c:8713-8741`) has **no pager primitive** in
   any-store and is a pure locality optimisation — **omit it** (call out as a
   deliberate, accepted micro-drift; pages still come from the freelist/grow
   so order is already roughly ascending). Page identity is the pgno; we
   reuse old page structs by pgno without rekey.
4. **`editPage` vs `rebuildLeafPage`.** any-store already has the wholesale
   `rebuild*Page` (= SQLite `rebuildPage`, the `editPage` fallback). The port
   **uses `rebuild*Page` for every output page** rather than porting
   `editPage`'s incremental free-head/free-tail/insert optimisation. This is
   correctness-equivalent (SQLite itself falls back to `rebuildPage` whenever
   `editPage` can't fit) and avoids porting `pageInsertArray`/`pageFreeArray`
   /freeblock-slot reuse, which any-store does not have (NOTES "No Full
   Freeblock Chain"). The crash-safe **ordering** concern that motivates
   `editPage`'s two-pass loop **disappears** when every page is rebuilt from
   the already-materialised `[]cellData` pool: we read all source bytes into
   `cellData` (cloning overflow `rawCell` bytes) **before** writing any output
   page, so no output write can clobber a not-yet-read source page. This is the
   single most important simplification and is called out again under
   Crash-safety.

## Target design (balance_nonroot-faithful)

New file `internal/btree/balance.go` (keeps `btree.go` from growing; same
package). One entry point plus pooling/sizing helpers that mirror SQLite
section-for-section.

### Data structures (mirror `CellArray` + the szNew/cntNew/cntOld arrays)

```go
const nbSiblings = 3 // SQLite NB (btree.c:7523); NN=1 neighbour each side.

// cellArray is the any-store analogue of SQLite's struct CellArray
// (btree.c:7588). apCell/szCell pool every cell to be redistributed:
// all cells of the nOld old pages, in order, with the parent divider
// cells interleaved between adjacent old pages (index-btree: always
// present — leafData is always 0 for any-store).
type cellArray struct {
	cells []cellData // pooled cells (apCell[]); divider cells carry key (+leftChild for interior)
	sz    []int      // szCell[]: in-page byte size of cells[i] (incl. interior 4B child ptr); lazily 0
	leaf  bool       // sibling pages are leaves (leafCorrection = 4 if true, else 0)
}
```

`cellData` (`btree.go:84`) already carries `{key,value,leftChild,overflowPg,
rawCell}`; `collectLeafCells`/`collectInteriorCells` already produce these with
overflow cells preserved as `rawCell` — that **is** SQLite's "include overflow
cells in `apCell[]` in place" (`btree.c:8466-8482`). The per-balance scratch
arrays (sized `[nbSiblings+2]`, i.e. 5): `cntOld`, `cntNew`, `szNew`, plus
`apOld []*page` (≤3) and `apNew []*page` (≤5).

### `balanceNonroot` — the function

Signature mirrors `balance_nonroot(pParent, iParentIdx, …)` (`btree.c:8248`):

```go
// balanceNonroot redistributes cells across pPage (the over-full child at
// parentIdx) and up to two adjacent siblings, then rewrites the parent
// dividers. Faithful port of SQLite balance_nonroot (btree.c:8248-9030),
// specialised to any-store's index-btree (leafData==0 always).
//
//   parentPg   — writable parent (SQLite pParent)
//   parentIdx  — slot in parentPg the descent went through (SQLite iParentIdx,
//                = path[len-1].cellIdx)
//   isRoot     — parentPg is the btree root (enables balance-shallower)
//   parentPath — ancestors above parentPg, for cascading the parent's own
//                over-full state upward (SQLite handles this via the balance()
//                do-loop; any-store recurses through insertSepIntoAncestor)
func (bt *btree) balanceNonroot(parentPg *page, parentIdx int, isRoot bool, parentPath []pathEntry) error
```

Step-by-step, each mapped to SQLite:

| # | any-store step | SQLite |
|---|----------------|--------|
| 0 | Pre: `parentPg` writable, at most one over-full child = the one at `parentIdx`. | `btree.c:8287-8296` |
| 1 | **Sibling selection.** `nCellParent = parentPg.cellCount`. If `<2`: `nxDiv=0`. Else `nxDiv = parentIdx==0 ? 0 : parentIdx==nCellParent ? nCellParent-2 : parentIdx-1`; window width `i=2`. `nOld = i+1` (≤3, clamped to children available). `pRight` = slot in parent holding the rightmost sibling's pgno (a cell's `leftChild` or `parentPg.rightChild`). | `btree.c:8314-8334` (with `bBulk=0`) |
| 2 | **Load + drop dividers.** For each gap between chosen siblings, read divider key from `parentPg` (its cell at `i+nxDiv`), remember it, and drop that cell from the parent. Read each old sibling page (`getWritablePage`) into `apOld[]`. | `btree.c:8335-8388` |
| 3 | **Pool cells.** `b.leaf = apOld[0].isLeaf()`. For i in 0..nOld-1: append `collect*Cells(apOld[i])` to `b.cells`, set `cntOld[i]=len(b.cells)`; if `i<nOld-1` append a divider cell: for **leaf** siblings it is `cellData{key: dividerKey_i}` (the separator copy; `leafCorrection=4` ⇒ no child ptr); for **interior** siblings it is `cellData{leftChild: apOld[i].rightChild, key: dividerKey_i}` — folding the left child's rightChild into the divider, exactly SQLite `btree.c:8506-8511`. | `btree.c:8428-8525` (leafData branch omitted) |
| 4 | **Initial sizing.** `usableSpace = usableSize - 12 + leafCorrection` (12 = interior header; leaf adds back 4 ⇒ effectively `usableSize-8`). `szNew[i]` = used bytes of `apOld[i]` (`usableSpace - freeBytes(apOld[i])`, computed from header like `walkForFill` does: `cellCount*2 + (usable-cellContentOff) - fragBytes`). `cntNew[i]=cntOld[i]`. | `btree.c:8543-8562` |
| 5 | **k + split points.** Port `btree.c:8563-8605` **verbatim in structure** (it has no leafData dependence once the `!leafData` divider-size adjustments are inlined — for any-store every inter-page boundary consumes one divider cell, so the `sz` of the boundary divider is always counted, i.e. the SQLite `!leafData` branch is always taken). Pack loop grows `k` to at most `nbSiblings+2`; `ErrCorrupt` if exceeded. Use a `cachedCellSize(&b, idx)` helper (`btree.c:7628`). | `btree.c:8563-8605` |
| 6 | **Back-off last page.** Port `btree.c:8618-8649` verbatim (the right-to-left rebalance; `bBulk=0`). Guards `cntNew[i-1] <= cntNew[i-2]` ⇒ `ErrCorrupt`. | `btree.c:8618-8649` |
| 7 | **Allocate k pages, reuse old.** For i in 0..k-1: if `i<nOld`, reuse `apOld[i]` as `apNew[i]` (already writable); else `allocatePage()` → `apNew[i]`. `nNew=k`. **Omit** the pgno-sort (no `PagerRekey`; documented micro-drift). | `btree.c:8668-8699` (ptrmap + rekey omitted) |
| 8 | **Parent right-pointer + interior tail child.** Repoint `pRight` (a parent cell's leftChild, or `parentPg.rightChild`) to `apNew[nNew-1].pgno`. If interior and `nOld!=nNew`, carry the rightmost child pointer from the old/new rightmost page into `apNew[nNew-1].rightChild`. | `btree.c:8759-8772` |
| 9 | **Rebuild output pages from the pool.** For i in 0..nNew-1: the cells for page i are `b.cells[start_i : cntNew[i]]` where `start_i = i==0 ? 0 : cntNew[i-1]+1` (the `+1` skips the divider cell promoted to the parent — SQLite `iNew = cntNew[iPg-1] + !leafData`, and `!leafData==1` always for any-store). For **leaf** pages call `rebuildLeafPage(apNew[i], slice)`; for **interior** pages call `rebuildInteriorPage(apNew[i], slice, rightChild_i)` where `rightChild_i` is the `leftChild` of the divider cell at `cntNew[i]` (the cell that becomes the parent divider), or the carried tail child for the last page. No two-pass ordering needed — see Crash-safety. | `btree.c:8915-8952` via `editPage`→here `rebuild*Page` |
| 10 | **Insert new dividers into parent.** For i in 0..nNew-2: the divider is `b.cells[cntNew[i]]`. Its key is the separator; insert into `parentPg` at slot `nxDiv+i` with `leftChild = apNew[i].pgno`, via the existing in-place interior-insert logic (the body of `insertSepIntoInterior`, `btree.go:1975-2019`). For leaf siblings the divider key is `b.cells[cntNew[i]].key` (the first key of the right page); for interior siblings the promoted cell's own `leftChild` becomes `apNew[i+1]`'s... no: the promoted divider's `leftChild` is set to `apNew[i].pgno` and its right neighbour pointer is `apNew[i+1].pgno` (next iteration / rightChild). | `btree.c:8832-8891` (insertCell) |
| 11 | **Free unused old pages.** For i in nNew..nOld-1: `freePage(apOld[i].pgno)`. | `btree.c:9000-9004` |
| 12 | **Balance-shallower (root collapse).** If `isRoot && parentPg.cellCount==0`: copy `apNew[0]`'s body into `parentPg` (reuse `splitRoot`'s inverse, or `collect*`+`rebuild*` into parentPg) and `freePage(apNew[0])`, reducing height by one. | `btree.c:8960-8985` |
| 13 | **Cascade.** If `parentPg` is now itself over-full (its in-place divider inserts overflowed), recurse: balance `parentPg` against **its** siblings using `parentPath` (mirrors SQLite's do-loop re-entering `balance_nonroot` on the parent, `btree.c:9213-9255`). When `parentPath` is empty and `parentPg` is the root and over-full, call `splitRoot`/`balance_deeper` first (SQLite `btree.c:9152-9169`). | `btree.c:9133-9256` |

### Driver integration (`balance` equivalent) — keep balance_quick

`balance_nonroot` is dispatched by SQLite's `balance()` (`btree.c:9133`). In
any-store the dispatch is inline in the split helpers. Re-shape **only the
over-full leaf path**:

`splitLeafAndInsertWithPath` (`btree.go:1829`) becomes:

1. Insert the new cell in-place if it fits (already handled upstream in
   `insertIntoLeafWithPath`, `btree.go:1192-1210`).
2. **balance_quick fast path** — unchanged dispatch (`btree.go:1835-1840` →
   `splitLeafRightmostAppend`). This is SQLite `btree.c:9187-9210`. **KEEP.**
3. Otherwise, if the leaf has a parent (`len(path)>0`): materialise the
   over-full leaf by inserting the new cell into a `[]cellData` (as today,
   `btree.go:1842-1853`) and writing it back so the leaf is genuinely
   over-full, then call `bt.balanceNonroot(parentPg, path[len-1].cellIdx,
   isRoot=(len(path)==1), path[:len-1])`. This replaces the
   `leafSplitPoint`+2×`rebuild`+`insertIntoParentWithPath` block
   (`btree.go:1854-1885`).
4. If the leaf **is** the root (`len(path)==0`): there is no parent to gather
   siblings from. Use `splitRoot` (`balance_deeper`, `btree.go:2188`) to push
   the leaf down one level, then the next insert balances normally — exactly
   SQLite, where a root overflow goes through `balance_deeper` first
   (`btree.c:9152-9169`) and `balance_nonroot` runs on the child next iteration.

Interior over-full during a divider insert (`insertSepIntoInterior`,
`btree.go:2022`) is handled by step 13's cascade (balance the interior page
against its siblings) instead of the current `interiorSplitPoint` 2-way split
(`btree.go:2040-2075`). The existing `insertSepIntoAncestor` recursion
(`btree.go:1913`) is the any-store stand-in for SQLite's `balance()` do-loop
walking up `pCur`.

`leafSplitPoint` / `interiorSplitPoint` (`btree.go:244` / `295`) become
**dead code** for the insert path and are deleted (or retained only if the
non-path `splitLeafAndInsert`/`insertIntoInterior` recursive variants, used by
upward propagation, are also migrated — see Phasing).

### What is a 1:1 port vs adapted (for reviewer verification)

**1:1 (logic preserved):** NB=3 sibling window + `nxDiv` selection (step 1);
divider drop-from-parent (step 2); pooling order cells→divider→cells
(step 3); `usableSpace` formula and initial `szNew` (step 4); the k/split-point
pack loop (step 5) and the mandatory back-off pass (step 6) — **ported
line-structure-for-line-structure**; reuse-old-then-allocate page order
(step 7); parent right-pointer + interior tail-child carry (step 8); divider
reassignment slots `nxDiv+i` (step 10); free `nNew..nOld` (step 11);
balance-shallower (step 12).

**Adapted (with reason, all in the table above):**
- `leafData==1` branches **removed** — any-store has no rowid/table btree
  (fact 1); equivalently `!leafData` is hard-coded true.
- `editPage` incremental edit → `rebuild*Page` wholesale (fact 4) — SQLite's
  own fallback; removes the need to port `pageInsertArray`/`pageFreeArray`/
  freeblock slots and the two-pass safety loop.
- `ptrmap*` / auto-vacuum blocks **removed** — no auto-vacuum (fact 3).
- `PagerRekey` ascending-pgno sort **omitted** — no pager rekey primitive;
  pure locality optimisation, accepted micro-drift (fact 3).
- `BTS_FAST_SECURE` divider-copy-into-ovflspace (`btree.c:8375-8385`)
  **removed** — no secure-delete mode.
- Cascade via `insertSepIntoAncestor` recursion instead of the `balance()`
  do-loop unwinding `pCur` (any-store has no persistent cursor stack on the
  write path; it carries `path []pathEntry`). Semantically identical: balance
  the parent next, up to the root.

## Crash-safety

Balance is a write-path structural mutation spanning multiple pages. Invariants:

1. **All page mutations occur inside the active write tx.** Every old sibling
   is acquired via `getWritablePage` (`pager.go:1043`) and every new page via
   `allocatePage` (`pager.go:1117`, freelist-then-grow), all of which require
   `pagerWriter` state and route through `writerCache.makeDirty`. Dirty pages
   are flushed to the WAL atomically at `Commit`; a crash before commit
   discards them via WAL recovery. No bytes hit the main DB file mid-balance.
2. **Read-before-write ordering (the editPage-ordering replacement).** Before
   any output page is written, **all** source cells are materialised into the
   `cellData` pool, and overflow-bearing cells are copied as `rawCell`
   (`collectLeafCells` already clones into a scratch buffer, `btree.go:1484`).
   Therefore rewriting `apNew[i]` (which may alias `apOld[i]`) can never
   clobber a not-yet-read sibling. This is why the two-pass `abDone` loop
   (`btree.c:8915-8952`) is unnecessary here and its omission is **safe, not a
   drift in correctness** — it is a consequence of full materialisation, which
   SQLite avoids only for performance.
3. **Freelist consistency.** Reused pages are removed from `apOld` accounting
   before `freePage`; only genuinely surplus pages (`nNew..nOld`) are freed,
   each via `pager.freePage` (`pager.go:1181`), which validates pgno bounds and
   updates the freelist trunk under the writer. Overflow chains attached to
   pooled cells are **never freed during balance** (they pass through as
   `rawCell`), matching SQLite (`btree.c` balance never touches overflow data)
   and any-store's existing `collectLeafCells` contract (`btree.go:1476`).
4. **WAL framing / codec unchanged.** Output pages are ordinary dirty pages;
   they are encoded (encryption/checksum, `codec.go`) and framed
   (`wal.go`) by the existing commit path with no new ordering. Page-1 header
   offset handling (`hdrOff = dbHeaderSize`) is preserved by `rebuild*Page`
   (`btree.go:1651`, `1719`).
5. **Savepoints / `dontWritePages` / `hasContent`.** New pages from the
   freelist may carry stale content; `allocatePageNear` `clear()`s grown pages
   and `rebuild*Page` `clear()`s before writing, so no stale bytes leak.
   Savepoint rollback restores prior page images from the sub-journal; because
   balance only dirties pages already in the writer cache (or freshly
   allocated, which rollback frees), a mid-tx savepoint rollback after a
   balance is covered by the existing sub-journal machinery — gated by the
   savepoint tests below.

**Gating tests (must pass, `-tags vfs`):**

- `internal/btree`: `TestRebalance*` (`rebalance_test.go`), `TestIntSplit_*`
  (`btree_intsplit_test.go`, 13 cases incl. deep/random-order/overflow-key
  interior splits), `TestBalanceQuick_*` (`btree_balance_quick_test.go`, the
  full matrix — proves the fast path is untouched), `TestSavepointCrash*` +
  `TestSavepointPartialCheckpointBackfill` (`savepoint_crash_test.go`),
  `TestCrash1..8` (`crash_test.go`), `TestSavepoint*` (`savepoint_sqlite_test.go`,
  `savepoint2_sqlite_test.go`), `insert_delete_sqlite_test.go`,
  `btree_deep_test.go`, `btree_edge_test.go`, `overflow_savepoint_test.go`,
  and `IntegrityCheck` (`integrity.go`) after large insert workloads.
- storetest (cwd repo `any-store-tests/storetest`):
  `TestCrashFuzzShort`, `TestRepeatedCrashSameDB`, `TestCommitSyncCrash`,
  `TestWALTruncationRecovery`, `TestCrashOnCheckpoint`,
  `TestCrashDuringWALRecovery`, `TestCrashDuringIndexBuild`
  (`crashtest_test.go`), plus the multiprocess stress
  (`multiprocess_stress_test.go`) and `optverify_test.go` cross-engine checks.
  Run e.g. `go test -tags vfs -timeout 12m ./storetest/ -run
  'CrashFuzzShort|CommitSyncCrash|RepeatedCrashSameDB|WALTruncationRecovery'
  -crash.iterations=8`.

## Measurable success criteria

### a. New test — `TestBalanceNonroot_FillFactor` (`internal/btree/balance_test.go`)

Reuse the existing harness verbatim: `walkLeavesForFill` /
`reportFillStats` / `leafFillStats` (`btree_balance_quick_test.go:134-278`).
The fill metric is **already defined there** and is the one to assert:

> per-leaf `used = cellCount*2 + (usable - cellContentOff) - fragBytes`
> (cell pointers + live cell content, minus fragmentation);
> `leafCapacity = usable - 8` (8-byte leaf header);
> `avgFill = Σ used / (leafCount * leafCapacity)`.

Test shape (deterministic; pageSize=1024, valSize=80, 4-byte keys, 5000 rows,
single write tx; matches the measurement above so before/after are comparable):

```
for each workload in {random, sequential_reverse, interleaved}:
    open db (PageSize:1024); create ns; insert 5000 rows in one tx; commit
    rtx := BeginRead; bt := &btree{pager, rootPage, walMaxFrame}
    stats := walkLeavesForFill(t, bt); reportFillStats(...)
    require IntegrityCheck() == nil
    assert avgFill >= 0.72        // random: 0.686 (today) -> >=0.72 target
    assert leafCount <= 560       // random: 631 (today) -> ideal 455; allow margin
    assert minFill  >= 0.30       // kills the low-fill tail (today min 8.7%)
```

- **random** is the primary case (3-sibling redistribution's home turf).
  Thresholds: `avgFill ≥ 0.72`, `leafCount ≤ 560` (vs 631 today, 455 ideal),
  `minFill ≥ 0.30` (vs 0.087 today). These are conservative — SQLite-class
  balancing reaches ~0.75 avg; 0.72 leaves headroom for the omitted
  `editPage`/rekey micro-optimisations.
- **sequential_reverse** (descending keys = leftmost inserts) exercises the
  `nxDiv` left-edge selection and must not regress vs random.
- **interleaved** (alternating low/high keys) forces repeated 3-page gathers.
- Keep `monotonic_append` asserting `avgFill ≥ 0.85` (proves balance_quick
  still fires and nonroot didn't break the append path).
- A counter analogous to `balanceQuickDispatchCount`
  (`pager.balanceNonrootDispatchCount`, test-only `atomic.Int64`) asserts the
  nonroot path actually fired (>0) on the random workload — guards against the
  fast path silently swallowing everything.

Also add `TestBalanceNonroot_SiblingGather` (a focused unit test, not a fill
metric): build a depth-2 tree with three adjacent leaves at known fills
(e.g. ~95%, ~95%, then overflow the middle), insert one cell that overflows the
middle leaf, and assert (a) `nOld==3` siblings were gathered (via a debug hook
or by asserting the parent divider count/child fills change on **all three**
siblings, not just two), and (b) `k==nNew` stayed 3 if the cells fit, or became
4 only when they didn't — pinning the `k ∈ {nOld-1, nOld, nOld+1}` invariant.

### b. On-disk fill-rate analysis (before vs after vs SQLite)

Three independent measurements on the **same** dataset:

1. **Engine fill metric** — the new test's `reportFillStats` output (avg /
   median / min / histogram / `leaf-count overhead vs ideal`). Capture
   BEFORE (current `main`) and AFTER. Expected delta on random/5000:
   leaves `631 → ≤560`, avg `68.6% → ≥72%`, the `0-25%`/`25-50%` histogram
   buckets drained.

2. **`Collection.Stats()` on a representative dataset** (`stats.go:95` →
   `tx.NamespaceSize`, `namespace_size.go:23`). `NamespaceSize` reports
   `{Pages, OverflowPages, Entries, PayloadBytes}`. Compute on-disk leaf-fill
   as `PayloadBytes / ((Pages - interiorPages) * usable)`; or more simply track
   `TotalPages()` and DB file size. Procedure:
   ```
   # BEFORE on current main, AFTER on the branch — same workload generator.
   # Insert e.g. 100k docs random-key into one collection, then:
   st, _ := coll.Stats(ctx)   // st.Total{Pages,Size}, per-namespace sizes
   # record st (Pages, Size) and os.Stat(dbfile).Size()
   ```
   Expected: fewer `Pages`, smaller file, higher `PayloadBytes/Pages`. Overflow
   page count is unchanged (balance never re-chains overflow).

3. **vs SQLite on the identical dataset** — the repo already benches against
   real SQLite (v0.4.6 modernc) via `/tmp/bench_main` and the storetest
   cross-engine harness (`optverify_test.go`, `crossengine_test.go`). Load the
   same random-key dataset into a SQLite table (single-column PK = key, or
   key+blob) and compare `PRAGMA page_count` × `page_size` (file size) and
   SQLite's own avg payload/page. Target: any-store AFTER within ~5–10% of
   SQLite's page count on the random workload (BEFORE is ~+38% on leaves).
   Commands: `sqlite3 file 'PRAGMA page_count; PRAGMA page_size;'` and
   `dbstat` virtual table (`SELECT name,SUM(pgsize),SUM(payload) FROM dbstat
   GROUP BY name;`) for per-table fill.

Report the before/after/SQLite triple as a table in NOTES when the drift is
moved to "resolved".

## Risks / phasing / non-goals

**Phasing (each phase independently testable, ordered):**

1. **Leaf nonroot.** Implement `balanceNonroot` + `cellArray` for the **leaf**
   case only; dispatch from `splitLeafAndInsertWithPath` (keep balance_quick;
   keep `interiorSplitPoint` for interior over-full). Gate: `TestRebalance*`,
   `TestBalanceQuick_*`, new `TestBalanceNonroot_FillFactor` (random), full
   `internal/btree` suite + `IntegrityCheck`.
2. **Interior nonroot.** Extend gather/redistribute to interior siblings
   (divider carries `leftChild`; `rightChild` carry); dispatch from the
   over-full branch of `insertSepIntoInterior`. Gate: `TestIntSplit_*`
   (all 13), deep-tree tests.
3. **Cascade + balance-shallower.** Wire step 13 (parent re-balance) and
   step 12 (root collapse) through `insertSepIntoAncestor`/`splitRoot`. Gate:
   `TestRebalanceRootCollapse`, savepoint + crash suite, storetest crash subset.
4. **Cleanup.** Delete dead `leafSplitPoint`/`interiorSplitPoint` and the
   non-path `splitLeafAndInsert`/2-way interior split if fully superseded;
   update `go_to_sqlite.json` rows (point the helpers at `balanceNonroot`/
   `cellArray`/`cachedCellSize`); move the NOTES drift to "resolved" with the
   before/after/SQLite fill table.

**Risks:**
- *Divider-key correctness.* The promoted divider must be the right sibling's
  first key (any-store `<`/`>=` invariant, fact 2). A wrong promote corrupts
  search. Mitigation: `IntegrityCheck` after every gating workload + the
  `TestBalanceNonroot_SiblingGather` boundary test + the existing
  `btree_cursor_test.go` ordered-scan assertions.
- *Off-by-one in `cntNew`/divider skip.* The `+1` divider skip (step 9,
  `start_i = cntNew[i-1]+1`) is the any-store form of SQLite's `!leafData`
  arithmetic; an error here drops or duplicates a cell. Mitigation: assert
  `Σ output cells + (nNew-1) dividers == len(b.cells)` at the end of balance;
  fuzz via `storetest` fault/crash fuzz.
- *Recursion depth on cascade.* `insertSepIntoAncestor` recursion is bounded
  by tree depth (`btCursorMaxDepth`); guard as the existing helpers do.
- *Performance.* Full materialisation + `rebuild*Page` per balance is O(pooled
  cells) — same order as today's 2-way (which also `collect`+`rebuild`s), and
  balances are rarer than the in-place fast path. Watch the insert benchmarks
  (`bench_test.go`, `BenchmarkBalanceQuick_MonotonicAppend`) for no regression
  on the append path (balance_quick still owns it).

**Non-goals:**
- Do **not** modify `balance_quick`/`splitLeafRightmostAppend` (it is the
  rightmost-append fast path and stays the first dispatch).
- Do **not** change the on-disk format (page layout, cell encoding, freelist,
  codec, WAL) — purely an in-memory redistribution of cells across pages.
- Do **not** port `editPage`/`pageInsertArray`/`pageFreeArray`/freeblock-slot
  reuse, the `PagerRekey` pgno-sort, `ptrmap`/auto-vacuum, or `BTS_FAST_SECURE`
  (all justified above; the first is replaced by `rebuild*Page`, the rest have
  no any-store counterpart).
- Do **not** touch the delete-side underfull merge (`tryMergeLeaf`/
  `removeChildFromParent`); it is a separate balance path.
- **Rollback:** revert the commit — no migration, no format change.
