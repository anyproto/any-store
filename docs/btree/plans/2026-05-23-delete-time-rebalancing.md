# Delete-time rebalancing — wiring SQLite's underfull merge into `Delete`

## Overview

- any-store v2's `Delete` (`internal/btree/btree.go:2356`) does **no
  delete-time rebalancing**. It removes the cell from the leaf (fast in-place
  drop or defrag-rebuild) and frees a leaf **only when it becomes fully empty**
  (`btree.go:2477-2483` → `removeChildFromParent`). An *under-full but non-empty*
  leaf is left in place forever. Delete-heavy workloads therefore degrade page
  fill monotonically and never reclaim or merge half-empty pages.
- SQLite, on delete, runs the **same general balancer** it uses for inserts
  (`balance()` → `balance_nonroot()`), triggered when the freed page falls below
  a fill threshold (`nFree*3 > usableSize*2`, i.e. more than 2/3 free). The
  balancer pools the under-full page with up to two siblings, recomputes the
  minimum number of output pages `k`, and when the pooled cells fit in **fewer**
  pages (`k = nOld−1`) it frees the surplus. Delete-merge is just
  `balance_nonroot` producing fewer output pages — there is no separate
  merge routine and no merge "direction" flag.
- any-store **already has** a faithful `balance_nonroot` port
  (`balance.go:187`, `balanceNonroot`, landed `4834f89`) whose `k`-computation
  is the SAME as SQLite's and **mechanically supports `k = nOld−1`**
  (`balance.go:13` documents the general invariant `k ∈ {nOld−1, nOld,
  nOld+1}`; surplus-page freeing already exists at `balance.go:655-661`). It is
  driven **only from the insert/over-full path** today, where the pooled count
  only grows, so it only ever yields `nNew ∈ {nOld, nOld+1}` (`balance.go:184`).
- any-store also has a `tryMergeLeaf` (`btree.go:2503`, just bug-fixed in
  `c1b7247`) that does a **2-page-only** merge, but it is **not wired into
  `Delete`** — it is dead code exercised only by tests
  (`btree_merge_cursor_test.go`).
- This plan **wires SQLite-style delete-time rebalancing into `Delete`** by
  adding an underfull trigger (mirroring SQLite's exact threshold), the
  interior-delete separator replacement (predecessor borrow), and a delete-side
  driver that funnels the underfull leaf through the existing `balanceNonroot`
  in its **merge (`k = nOld−1`) direction**, with the two faithful extensions
  the merge direction requires (underfull-parent cascade and balance-shallower
  root collapse). `tryMergeLeaf` is **retired** in favour of the general
  balancer (it is a strict subset: 2-page-only, no redistribution, no
  three-sibling consolidation).
- **No on-disk format change.** Page layout, cell encoding, freelist,
  codec/checksum, WAL framing are untouched. Rollback = revert the commit.

## Motivation (measured)

A throwaway measurement (random 4-byte keys, valSize 80, pageSize 1024,
usable 1016; insert 20000 rows in one tx, then delete 80% — every key with
`i%5 != 0` — in a second tx; reverted before commit) gives the current `Delete`
behaviour:

| stage                | cells | leaves | interior | avg fill | median | min fill | dbSize (pages) |
|----------------------|------:|-------:|---------:|---------:|-------:|---------:|---------------:|
| after insert         | 20000 |   2067 |       36 |   83.8%  | 86.6%  |   8.7%   | 2104 |
| **after delete 80%** |  4000 |   1828 |       36 | **18.9%**| 17.3%  |   8.7%   | **2104** |

The 4000 survivors would occupy ~480 leaves if freshly inserted (at the
post-insert ~84% fill); instead they are smeared across **1828** near-empty
leaves. Deleting 80% of the data freed **11.6%** of the leaves
(2067 → 1828) and **zero** pages overall (dbSize unchanged — leaves only
empty fully when *every* cell on them is deleted, which random deletes almost
never achieve). Average leaf fill **collapsed 83.8% → 18.9%**.

The win from this plan: the survivors consolidate back toward ~480–560 leaves
at ~70–80% fill, and the freed pages return to the **freelist** (reused by
future inserts; the on-disk file does not shrink without a vacuum, but leaf
count, average fill, and freelist headroom are restored). This is exactly the
SQLite delete-rebalance behaviour.

## Context (from discovery)

### SQLite reference — `../sqlitec/src/btree.c`

`sqlite3BtreeDelete` (`btree.c:9844-10043`) is the orientir. Cited blocks:

| Block | Line | Role |
|-------|------|------|
| **Underfull-rebalance gate** | `btree.c:10005-10011` | `if( pCur->pPage->nFree*3 <= (int)pCur->pBt->usableSize*2 ) rc=SQLITE_OK; else rc=balance(pCur);` — **the exact underfull trigger.** When >2/3 of the page is free, balance; else no-op. |
| `bPreserve` cursor-save gate | `btree.c:9903-9917` | If the delete *will* rebalance — `!leaf` **or** `nFree+cellSize+2 > usableSize*2/3` **or** `nCell==1` — save the cursor key (→ `CURSOR_REQUIRESEEK`); else keep position (`CURSOR_SKIPNEXT`). Same 2/3 threshold. |
| **Interior-delete: borrow predecessor** | `btree.c:9919-9930` | If the cell is on an **interior** page, move the cursor to `sqlite3BtreePrevious` — the largest key in the left subtree (always a leaf under the deleted cell's left child). |
| Drop the interior cell | `btree.c:9948-9952` | `sqlite3PagerWrite`; `BTREE_CLEAR_CELL` (free overflow); `dropCell`. |
| **Promote leaf cell to interior** | `btree.c:9959-9986` | Take the leaf's largest cell (`findCell(pLeaf, nCell-1)`), `insertCell` it into the interior at `iCellIdx` (with the left-subtree child pointer `n`), then `dropCell` it from the leaf. The separator is **replaced**, not just removed. |
| **Balance the leaf, then walk up** | `btree.c:9988-10020` | `balance(pCur)` on the (now under-full) leaf first; then if balance climbed past `iCellDepth`, release down to `iCellDepth` and `balance` the interior node too. |
| **Cursor state after delete** | `btree.c:10022-10042` | `bPreserve>1`: `CURSOR_SKIPNEXT` (skipNext ±1). Else `moveToRoot`; if `bPreserve`, release pages and set `CURSOR_REQUIRESEEK`. |

The balancer itself (shared with insert):

| Symbol | Line | Role |
|--------|------|------|
| `balance` (driver) | `btree.c:9133-9262` | do-loop. The **same** no-op gate `pPage->nFree*3<=usableSize*2` (`btree.c:9146-9151`). `balance_deeper` for an over-full root (`btree.c:9152-9169`); `balance_nonroot` otherwise (`btree.c:9213-9244`); unwinds the cursor stack upward (`btree.c:9250-9255`). |
| `balance_nonroot` | `btree.c:8248-9030` | The general balancer. **`k` is computed purely from the pooled cell sizes** (`btree.c:8543-8605`): `k` shrinks (the pack loop never grows it) when the cells fit, so `k = nOld−1` is the natural merge outcome. |
| **k can be nOld−1** | `btree.c:8563-8605` | The pack loop starts `k=nOld` and only sets `k=i+2` *inside* `while(szNew[i]>usableSpace)` (an over-full page); it sets `k=i+1` at `btree.c:8599-8600` when the last needed page is reached. If two siblings' cells fit on one page, `cntNew[0]` reaches `b.nCell` at `i=0` and `k` becomes 1 = `nOld−1`. |
| **Free surplus pages** | `btree.c:9000-9004` | `for(i=nNew; i<nOld; i++) freePage(apOld[i]);` — the merge frees the pages `balance_nonroot` no longer needs. **This is the entire "merge" mechanism.** |
| **Balance-shallower (root collapse)** | `btree.c:8960-8985` | After redistribution, if `isRoot && pParent->nCell==0`, defragment `apNew[0]`, `copyNodeContent` it into the root, `freePage(apNew[0])` — the tree loses a level. The delete-only analogue of `balance_deeper`. |
| `balance_deeper` | `btree.c:9052-9097` | Root grows a level (insert side only; not used on delete). |

**Confirmed: SQLite uses ONE balancer for both insert-split and delete-merge.**
The only delete-specific code in `sqlite3BtreeDelete` is the underfull gate
(`btree.c:10005`), the interior-cell predecessor borrow
(`btree.c:9919-9986`), and the cursor-state handling
(`btree.c:9903-9917, 10022-10042`). Everything structural is `balance()`.

### any-store side — `internal/btree/`

Current delete path (free-empty-leaf only; **no** rebalance, **no** interior
separator replacement, **no** merge driver):

| Function | Line | Role |
|----------|------|------|
| `Delete` | `btree.go:2356` | Phase 1 read-only descent building `path []pathEntry` (`btree.go:2363-2378`); Phase 2 in-place cell drop or defrag-rebuild on the leaf (`btree.go:2399-2474`). |
| free-empty-leaf | `btree.go:2477-2483` | `if cellCount==0 && pgno!=rootPage { freePage; removeChildFromParent }`. **The only structural mutation on delete.** No underfull (>0 cells) handling. |
| `tryMergeLeaf` | `btree.go:2503` | 2-page-only merge (collect both, rebuild left, free right, fix parent divider). **DEAD CODE** — not called from `Delete`; only tests call it (`btree_merge_cursor_test.go`). Fixed in `c1b7247` (mergeRight removed the wrong divider). |
| `removeMergedRightSeparator` | `btree.go:2697` | `tryMergeLeaf`'s mergeRight parent fixup (`c1b7247`). |
| `removeChildFromParent` | `btree.go:2742` | Removes one child slot from the parent after an empty-leaf free; delegates to `finishParentRemoval`. **Does not cascade an underfull parent upward.** |
| `finishParentRemoval` | `btree.go:2805` | Writes the post-removal parent; handles **0-cell** root collapse (`copyNodeContent`-equivalent, page-1-header-aware, `btree.go:2808-2847`) and **0-cell** non-root interior collapse (`btree.go:2854-2870`). Only fires when the parent reaches **exactly 0 cells**. |
| `leafUsedSpace` | `btree.go:2490` | Already computes a leaf's used bytes (`cellPtrEnd + (usable - contentOff)`) — the basis for the underfull check. |

The balancer (reusable for the merge direction):

| Function | Line | Role |
|----------|------|------|
| `balanceNonroot` | `balance.go:187` | Faithful `balance_nonroot`. `k`-pack loop `balance.go:449-497`; back-off `balance.go:502-538`; reuse/alloc `balance.go:554-576`; **surplus free `balance.go:655-661`** (`for g:=nNew; g<nOld` → `freePageDeferred`); parent rewrite `balance.go:698`. Takes an `injectedCell`; **`inject.active==false` is already supported** (every `inject.*` use is guarded by `injectHere := inject.active && …`, `balance.go:343`). |
| `rewriteParentAfterBalance` | `balance.go:754` | Splices the gathered child run out of the parent and the `nNew` outputs + `nNew−1` dividers in (`balance.go:791-819`); rebuilds the parent if it **fits** (`balance.go:833-835`), else **over-full** 2-way split + `insertSepIntoAncestor` cascade (`balance.go:844-873`). **Does NOT handle an *under-full* parent** (the merge case) — see Gap. |
| Step 12 note | `balance.go:685-690` | Explicitly: "balance-shallower is NOT reachable here … Root collapse is a delete-side phenomenon handled by the reused tryMergeLeaf / removeChildFromParent." |
| `freePageDeferred` | `balance.go:736` | `freePage` wrapper for surplus pages (pin already released). |

Strengthened `IntegrityCheck` (`97e9236`): `integrity.go:514` →
`checkTreePage` (`integrity.go:275`) threads divider-derived `[lower, upper)`
bounds down the recursion (`keyInBounds`, `integrity.go:226-262`), enforcing
`max(subtree[child i]) < D_i <= min(subtree[child i+1])` **across page
boundaries** (`integrity.go:272-274`). This is the validator for rebalanced
trees. Tests: `integrity_divider_test.go` (`TestIntegrityCheck_DividerRange_*`).

### The exact gap (why fill degrades)

1. **No underfull trigger.** `Delete` (`btree.go:2477`) only acts when a leaf
   reaches **0 cells**. SQLite acts when a leaf exceeds **2/3 free**
   (`btree.c:10005`). Random deletes almost never empty a leaf, so any-store
   never rebalances and fill decays to ~19% (measured).
2. **No merge driver.** `balanceNonroot`'s `k`-math already yields `k=nOld−1`
   and `balance.go:655-661` already frees the surplus, but **nothing calls it
   from `Delete`** with `inject.active=false`. The capability is present and
   unreachable.
3. **No interior-cell replacement.** When the deleted key lives on an interior
   page (it is a divider), any-store's `Delete` descends to the **leaf** and
   deletes there (`searchInterior` routes `>= divider` to the right child;
   `btree.go:910`), so the divider key may remain as a *routing* key with no
   matching entry. SQLite instead **replaces** the divider with the
   predecessor leaf cell (`btree.c:9919-9986`). any-store's invariant differs
   (divider = smallest key of the right subtree, `<`/`>=`), so the equivalent
   is to **delete the leaf entry and, if it was the leaf's first key / a live
   divider, let the subsequent balance re-derive the divider** — see Design §3.
4. **No underfull-parent cascade / balance-shallower in the merge direction.**
   `finishParentRemoval` collapses only a **0-cell** parent;
   `rewriteParentAfterBalance` handles only an **over-full** parent. A merge
   that leaves the parent under-full (but non-empty) is not cascaded — the
   under-fullness simply stops at the parent. SQLite cascades via the
   `balance()` do-loop unwinding the cursor (`btree.c:10012-10020`).

`go_to_sqlite.json` rows to update: `btree.go:*btree.Delete` (`:186`),
`btree.go:*btree.tryMergeLeaf` (`:359`),
`btree.go:*btree.removeChildFromParent` (`:317`), `db.go:*WriteTx.Delete`
(`:888`).

## SQLite delete-rebalance algorithm (the essence, cited)

1. **Find + drop the cell.** Descend to the cell; `dropCell` it
   (`btree.c:9948-9952`).
2. **Interior delete → borrow predecessor** (`btree.c:9919-9986`). If the cell
   is on an interior node, the divider must be *replaced* (a divider with no
   subtree boundary corrupts search). SQLite moves to the predecessor
   (`sqlite3BtreePrevious` → largest key of the left subtree, always a leaf),
   `insertCell`s that leaf cell into the interior at the vacated slot (carrying
   the left-subtree child pointer), and `dropCell`s it from the leaf. The
   *predecessor* (not successor) is chosen because it is in the subtree headed
   by the deleted cell's left child, which makes the subsequent balance local.
3. **Underfull trigger** (`btree.c:10005`). `if (nFree*3 <= usableSize*2)` →
   no-op; else `balance(pCur)`. I.e. rebalance iff **free bytes > 2/3 of
   usable** (page is <1/3 full of live content). This is byte-exact identical
   to the insert-side `balance()` no-op gate (`btree.c:9146`).
4. **Rebalance via the general balancer** (`btree.c:10010`, `9133`, `8248`).
   `balance_nonroot` pools the underfull page + up to 2 siblings, recomputes
   `k`. When the pooled cells fit in `nOld−1` pages, `k=nOld−1` and the
   surplus pages are `freePage`d (`btree.c:9000-9004`). **No merge-specific
   code** — `k=nOld−1` is just what the size loop returns. The redistribution
   case (`k=nOld`, cells reshuffled to even out fill) and the merge case
   (`k=nOld−1`) are the same code.
5. **Cascade up** (`btree.c:10012-10020`). After balancing the leaf, if the
   parent is now under/over-full, `balance` it too, walking up to the root.
6. **Balance-shallower** (`btree.c:8960-8985`). If a balance empties the root's
   cell array, copy the single child into the root and free it — the tree
   shrinks a level (the delete-side dual of `balance_deeper`).
7. **Cursor state** (`btree.c:9903-9917, 10022-10042`). Preserve the cursor by
   key (`CURSOR_REQUIRESEEK`) when a rebalance moved pages, or by offset
   (`CURSOR_SKIPNEXT`) when it didn't.

## Faithful port design

Each step maps 1:1 to its SQLite counterpart; every adaptation is flagged.
The design **reuses `balanceNonroot` for the merge direction** and adds the
minimum to make that direction reachable and complete. **`tryMergeLeaf` is
retired** (see §6).

### §1. Underfull trigger in `Delete` — mirror SQLite's threshold

After the cell is removed and the leaf header rewritten (`btree.go:2474`),
**before** the existing free-empty-leaf block (`btree.go:2477`), compute the
leaf's free bytes and apply SQLite's exact gate.

SQLite: `nFree*3 <= usableSize*2` → skip; else balance (`btree.c:10005`).
`nFree` = bytes **not** holding live content = `usableSize − used`, where
`used` is what `leafUsedSpace` (`btree.go:2490`) already computes
(`cellPtrEnd + (usable − contentOff)`; note this includes `fragBytes` as used,
which is conservative — SQLite's `nFree` excludes fragmentation, so any-store's
trigger fires slightly *less* eagerly, which is safe). Then:

```
nFree := usable - bt.leafUsedSpace(wpg)
underfull := nFree*3 > usable*2          // SQLite btree.c:10005 negated
```

Dispatch:
- `wpg.header.cellCount == 0 && pgno != rootPage`: **keep the existing
  free-empty-leaf fast path** (`btree.go:2477-2483`). It is a legitimate
  SQLite-equivalent shortcut: a 0-cell page pooled by `balance_nonroot` is
  freed as surplus anyway (`btree.c:9000-9004`); freeing it directly avoids a
  gather. KEEP, but route through the new path's parent handling if the parent
  then becomes underfull (see §4) — i.e. after `removeChildFromParent` returns,
  the *parent* may need a balance. (Today it doesn't get one; folding it in is
  part of §4.)
- `underfull && len(path) > 0`: invoke the **delete-side merge driver**
  (`deleteRebalanceLeaf`, §2). This replaces the dead `tryMergeLeaf`.
- otherwise (not underfull, or the leaf is the root): nothing — exactly
  SQLite's no-op branch (`btree.c:10008`). A root leaf is never balanced
  (SQLite's `iPage==0` short-circuits `balance`, `btree.c:9152` with no
  overflow).

**Adaptation flagged:** any-store deletes by **key** (`bt.Delete(key)`,
called from `WriteTx.Delete`, `db.go:1658-1663`), not via a persistent write
cursor. There is **no cursor to save/restore** on the write path, so SQLite's
entire `bPreserve` / `CURSOR_SKIPNEXT` / `CURSOR_REQUIRESEEK` machinery
(`btree.c:9903-9917, 10022-10042`) has **no analogue and is omitted** — see §5
for the read-cursor interaction (which is a *separate* concern).

### §2. Delete-side merge driver — funnel through `balanceNonroot`

New `deleteRebalanceLeaf(leafPgno uint32, path []pathEntry) error`, modelled on
the *structure* of `splitLeafAndInsertWithPath` (`btree.go:1833`) but with
`inject.active=false` and **no balance_quick** (quick is an append-only split
fast path; irrelevant to merge). It:

1. Reads the parent (`path[len-1].pgno`) writable.
2. Calls `bt.balanceNonroot(parentPg, path[len-1].cellIdx, isRoot=(len(path)==1),
   path[:len-1], injectedCell{active:false})`.
3. Releases the parent.

`balanceNonroot` then, **unchanged for the merge math**:
- gathers the underfull leaf + up to 2 siblings (NB=3 window, `balance.go:191`);
- pools their cells (no injection; `balance.go:336-427`);
- runs the `k`-pack loop (`balance.go:449-497`) which, for under-full input,
  **returns `k < nOld`** when the cells fit in fewer pages;
- runs the back-off pass (`balance.go:502-538`; with `k=1` the loop body is
  skipped since `g>0` is false — already correct);
- reuses `apOld[0..nNew-1]`, **frees `apOld[nNew..nOld)`** as surplus
  (`balance.go:655-661`);
- rewrites the parent (`rewriteParentAfterBalance`, `balance.go:754`).

**Whether `balanceNonroot` already supports merge: YES for the redistribution
core, NO for the parent/root completion.** The pooling, `k`-computation,
back-off, page reuse, and **surplus free** all already handle `nNew < nOld`
(documented general invariant `k ∈ {nOld−1, nOld, nOld+1}`, `balance.go:13`).
The two things that are insert-path-only and need a **faithful extension** are
in §4 (underfull-parent cascade) and §4 (balance-shallower). No change to the
gather/size/redistribute/free logic is required.

Add a **dispatch counter** `pager.deleteRebalanceDispatchCount atomic.Int64`
(mirroring `balanceNonrootDispatchCount`, `pager.go:97-102`), incremented at
the top of `deleteRebalanceLeaf`, for the success test's `> 0` guard.

### §3. Interior-delete separator handling

SQLite replaces an interior divider with the left-subtree predecessor
(`btree.c:9919-9986`). any-store's `Delete` already **never deletes on an
interior page**: it descends to the leaf (`btree.go:2365-2378`) and deletes the
key there. The interior divider that *routes* to that key is the **smallest key
of the right subtree** (any-store `<`/`>=` invariant, `searchInterior`
`btree.go:910`; `integrity.go:226-262`). Two sub-cases:

- **The deleted key is NOT a divider** (the common case): nothing to do beyond
  the leaf delete + §1 trigger. The divider still bounds a non-empty subtree.
- **The deleted key WAS a live divider** (it was the smallest key of some
  subtree, hence equal to an ancestor divider): the divider now points at a
  subtree whose minimum has changed. any-store does **not** eagerly fix the
  stale divider; instead the **balance** triggered in §1 re-derives it. When
  `balanceNonroot` rebuilds the parent (`rewriteParentAfterBalance`,
  `balance.go:602-611` for leaf pools), each new divider is computed as a
  **fresh key-copy of the first cell of the right output page**
  (`bt.cellFullKey(&b.cells[boundary])`) — which is exactly the new subtree
  minimum. So a divider made stale by a leaf delete is corrected the next time
  that leaf participates in a balance.

**Correctness subtlety (must be in the design, gated by `IntegrityCheck`):** a
stale divider that is **larger** than the new subtree minimum is *safe for
search* under `<`/`>=` (a probe for the old-min key descends right and finds
nothing-smaller; a probe for any surviving key still routes correctly because
all surviving keys are `> old min ≥ new min`). It is, however, flagged by the
strengthened `IntegrityCheck` (`keyInBounds`: a leaf key `< divider lower
bound`, `integrity.go:252`). **Therefore the underfull trigger alone is not
sufficient for divider correctness** when a leaf does *not* drop below 2/3 fill
after deleting its first key. Two faithful options:

- **Option A (SQLite-faithful, chosen): borrow on first-key delete.** Mirror
  SQLite's "replace the separator" by detecting `idx==0` on the leaf delete
  (the deleted key was this leaf's minimum, i.e. a potential divider) and, when
  the leaf is **not** about to be merged (still `>= 2/3` full), updating the
  ancestor divider that equals the old min to the new min (`newMin =`
  leaf's new first key). This is a **targeted parent divider update**, the dual
  of SQLite promoting the predecessor. Implement by walking `path` to the
  nearest ancestor whose divider for this child equals the old first key and
  rewriting that one divider cell in place (reusing the in-place interior cell
  rewrite logic; `insertSepIntoInterior` body, `btree.go:2030-2074`, but as an
  overwrite-key not an insert). Bounded by tree depth.
- **Option B (simpler, weaker): always balance on first-key delete.** Force the
  §1 trigger whenever `idx==0` regardless of fill. Simpler but does extra
  merges; rejected because it changes fill behaviour vs SQLite and does more
  page churn than SQLite.

**Decision: Option A.** It is the faithful dual of `btree.c:9919-9986`
(SQLite replaces the divider exactly when the deleted cell *is* the boundary;
any-store's boundary is the *successor* min rather than the *predecessor* max,
but the act — keep the divider in sync with the subtree boundary — is the
same). Flag this as the one **unavoidable adaptation** of the interior path:
SQLite borrows the **predecessor (left subtree max)** into the interior;
any-store keeps the divider equal to the **right subtree min**, so on a
first-key delete it advances the divider to the new min instead of pulling a
cell up. No cell moves between pages in this sub-case (the divider is a pure
routing key under `<`/`>=`), which is *simpler* than SQLite (SQLite's intkey
divider carries data, any-store's index divider does not).

### §4. Underfull-parent cascade + balance-shallower (the merge completion)

These are the two `balanceNonroot` extensions the merge direction needs. Both
are faithful ports of SQLite blocks currently marked "not reachable here"
(`balance.go:685`).

**(a) Underfull-parent cascade.** After `rewriteParentAfterBalance` writes the
parent, the parent may itself be under-full (a merge removed a divider). SQLite
re-enters `balance` on the parent (`btree.c:10012-10020`, do-loop
`btree.c:9250-9255`). any-store's analogue: after the parent rewrite, apply the
**same 2/3-free underfull test** to the parent; if under-full and it has a
grandparent (`len(parentPath) > 0`), recurse
`bt.balanceNonroot(grandPg, parentPath[len-1].cellIdx, …, parentPath[:len-1],
inject{active:false})` — exactly the interior cascade already used on the
*insert* over-full path (`insertSepIntoInterior`, `btree.go:2087-2110`), but
triggered by under-fullness instead of over-fullness. This is **deviation 6**
in `balance.go` (cascade via path recursion, not cursor unwind) applied to the
delete direction.

Where: extend `rewriteParentAfterBalance` (`balance.go:833-836`, the "fits"
branch). After the in-place rebuild, add:
```
if isRoot { return bt.maybeBalanceShallowerRoot(parentPg) }   // (b) below
if parentUnderfull(parentPg) && len(parentPath) > 0 {
    return bt.deleteRebalanceInterior(parentPg.pgno, parentPath)
}
return nil
```
`deleteRebalanceInterior` is the interior analogue of `deleteRebalanceLeaf`
(§2): read the grandparent, call `balanceNonroot` on it with the interior
parent as the target child, `inject.active=false`. **Interior pools are already
fully handled by `balanceNonroot`** (`balance.go:367-401, 614-628`) — no new
code in the balancer.

**Adaptation flagged:** the parent-underfull check must use the **interior**
free-byte formula (12-byte header, `balance.go:829-832` already computes
`used`), not the leaf one. Add a small `interiorUnderfull(pg)` helper mirroring
`nFree*3 > usable*2` with `usable - usedInterior`.

**(b) Balance-shallower (root collapse on merge).** SQLite, after a balance
that empties the root, copies the lone child into the root and frees it
(`btree.c:8960-8985`). any-store **already has this exact operation**:
`finishParentRemoval` (`btree.go:2808-2870`) does the page-1-header-aware root
copy + child free for a 0-cell root, and the 0-cell non-root interior collapse.
**Reuse it.** After the parent rewrite leaves a **0-cell root**, call the same
collapse logic (factor `finishParentRemoval`'s 0-cell branches into a
`collapseSingleChild(parentPg)` helper that both `removeChildFromParent` and
`rewriteParentAfterBalance` call). This satisfies the `balance.go:685-690` note
("root collapse … handled by the reused … removeChildFromParent") by *actually
reusing* that code from the balance path.

**Adaptation flagged:** SQLite defragments `apNew[0]` before the copy
(`btree.c:8977`, critical for page 1 because its header shrinks the usable
area). any-store's `finishParentRemoval` page-1 branch already copies at the
**same absolute offsets** and shifts only header+pointers
(`btree.go:2816-2834`), which is the any-store equivalent of "free space up
front" — no separate defragment needed because the child was just rebuilt by
`rebuildInteriorPage` (content is already packed). Confirm with the page-1 root
collapse test (`TestMergeCursor_RootCollapseOnPage1`).

### §5. Cursor repositioning after a structural delete

SQLite deletes via a write cursor and must reposition it
(`btree.c:10022-10042`). **any-store deletes by key with no write cursor**, so
there is nothing to reposition on the write side. The relevant interaction is
the **read** `Cursor` (`btree.go:2957+`) used by `Find().Delete()` and reverse
scans:

- any-store's read `Cursor` holds a `stack []cursorFrame` of pinned leaf /
  pgno+cellIdx frames (`Next` `btree.go:3456`, `Previous` `btree.go:3547`). A
  structural delete that **frees or rebuilds a page the cursor has on its
  stack** would invalidate the cursor's `cellIdx` / pinned page.
- **Today's `Delete` already mutates pages structurally** (free-empty-leaf +
  parent rewrite) and the existing model handles concurrent
  iterate-and-delete: deletes happen in a `WriteTx`; read cursors run in a
  separate `ReadTx` against a **WAL snapshot** (`walMaxFrame`), so a read
  cursor never observes the writer's structural changes within its snapshot.
  Cross-tx isolation (the WAL `mxFrame` boundary) is what makes this safe, not
  cursor repositioning. **This plan does not change that** — it only frees/
  rebuilds *more* pages per delete, all within the write tx, all invisible to
  concurrent readers' snapshots.
- The one place to **audit** (not change): `Find(...).Delete(ctx)` in the
  query layer — confirm it materialises the matching keys **before** deleting
  (snapshot-then-delete), not delete-during-live-cursor on the same tx. The
  storetest `TestCursorMutationDuringIteration`
  (`crashtest_test.go:2696`) and `TestIndex_Concurrent_ConcurrentDeleteAndQuery`
  gate this. **Subtlety to call out:** if any code path holds a *write-tx* read
  cursor open across a `Delete` on the same tree, the increased page churn
  could surface a pre-existing latent bug; the audit must confirm no such path
  exists (the query layer collects ids first).

**Net:** cursor repositioning is a **non-issue for the write path** (no write
cursor) and a **pre-existing, unchanged invariant for the read path** (WAL
snapshot isolation). This is the cursor-stability subtlety flagged in
§Non-goals.

### §6. Retire `tryMergeLeaf`; keep the free-empty-leaf fast path

- `tryMergeLeaf` (`btree.go:2503`) + `removeMergedRightSeparator`
  (`btree.go:2697`) become **dead** once `deleteRebalanceLeaf` (§2) is the
  underfull handler. The general balancer is a strict superset: `tryMergeLeaf`
  merges exactly 2 pages and only when they *fully* fit, with no redistribution
  and no 3-sibling consolidation; `balanceNonroot` does all of that and more.
  **Delete `tryMergeLeaf` / `removeMergedRightSeparator`** (and their
  test-only callers) in the cleanup phase, or keep them one release as
  cross-checks (Phasing). `c1b7247`'s mergeRight fix is thereby subsumed
  (the balancer rewrites the parent wholesale via `rewriteParentAfterBalance`,
  which cannot mis-target a divider — it rebuilds from the spliced child/divider
  lists).
- **Keep** the free-empty-leaf fast path (`btree.go:2477-2483`): it is the
  any-store equivalent of `balance_nonroot` freeing a 0-cell page as surplus
  (`btree.c:9000-9004`), reached without a gather. Its `removeChildFromParent`
  tail must, after this plan, **also apply the §4(a) parent-underfull cascade**
  (today it stops at the parent) so an empty-leaf removal that under-fills the
  parent is completed — fold the cascade into `finishParentRemoval`'s non-
  collapse exit (`btree.go:2872`).

### Mapping table (each step → SQLite)

| any-store step | SQLite |
|----------------|--------|
| §1 underfull gate `nFree*3 > usable*2` | `btree.c:10005` |
| §1 keep free-empty-leaf | `btree.c:9000-9004` (0-cell page is surplus) |
| §2 `deleteRebalanceLeaf` → `balanceNonroot(inject.active=false)` | `btree.c:10010` → `9133` → `8248` |
| §2 merge = `k=nOld−1`, surplus freed | `btree.c:8563-8605`, `9000-9004` |
| §3 first-key divider advance (Option A) | `btree.c:9919-9986` (separator replacement; min vs max adaptation) |
| §4a underfull-parent cascade | `btree.c:10012-10020`, `9250-9255` |
| §4b balance-shallower (reuse `finishParentRemoval`) | `btree.c:8960-8985` |
| §5 no write-cursor reposition | `btree.c:10022-10042` (N/A — key-based delete) |
| §6 retire `tryMergeLeaf` | (no SQLite analogue — it was a non-faithful shortcut) |

### What is a 1:1 port vs adapted (for reviewer verification)

**1:1 (logic preserved):** the underfull threshold (`nFree*3 > usable*2`,
byte-exact); the use of the single general balancer for the merge; `k=nOld−1`
emerging from the unchanged pack loop; surplus-page freeing; the
underfull-parent cascade; balance-shallower root collapse.

**Adapted (with reason):**
- **No write cursor** ⇒ all `bPreserve`/`CURSOR_*` handling omitted (§1, §5).
  any-store deletes by key (`db.go:1658`).
- **Interior separator: advance-to-successor-min instead of borrow-
  predecessor-max** (§3). any-store's `<`/`>=` divider = right-subtree min;
  SQLite intkey divider = left-subtree max + carries data. any-store's divider
  is routing-only, so the fix moves no cell between pages (simpler than SQLite).
- **Cascade via `path` recursion, not cursor-stack unwind** (§4a) — the same
  deviation 6 already accepted for the insert path (`balance.go:65-68`).
- **Reuse `finishParentRemoval` for balance-shallower** instead of porting
  `copyNodeContent`+`defragmentPage` (§4b) — any-store already has the page-1-
  aware collapse; the rebuilt child is pre-packed so no defragment is needed.
- **`tryMergeLeaf` retired**, not ported (§6) — it predates the general
  balancer and is a non-faithful 2-page subset.

## Crash-safety

Delete-time rebalancing is a write-path structural mutation spanning multiple
pages. The invariants are the **same** as the insert-side `balanceNonroot`
(already gated by the crash suite) plus the surplus-free direction:

1. **All mutations inside the active write tx.** Gathered siblings via
   `getWritablePage` (`pager.go:1057`), new pages (only when `k=nOld+1`, which
   the *delete* path never hits — merge is `k<=nOld`) via `allocatePage`. Dirty
   pages flush to the WAL atomically at `Commit`; a crash before commit is
   discarded by WAL recovery. No bytes hit the main DB file mid-balance.
2. **Read-before-write ordering.** `balanceNonroot` materialises **all** source
   cells into `b.cells` (overflow cells cloned as `rawCell`) **before** writing
   any output page (`collectLeafCells`/`collectInteriorCells`,
   `btree.go:1484/1589`), so rewriting `apNew[g]` (which may alias `apOld[g]`)
   never clobbers an unread source — SQLite's two-pass `abDone` ordering is
   unnecessary (`balance.go:585-589`). Unchanged for the merge direction.
3. **Freelist consistency (the merge-specific concern).** Surplus pages
   `apOld[nNew..nOld)` are freed via `freePageDeferred` → `freePage`
   (`pager.go:1195`) **after** their pin is released (`balance.go:655-661`),
   matching the existing empty-leaf free (`btree.go:2479`). `freePage`
   re-acquires the freelist trunk under the writer, validates pgno bounds, and
   marks the freed page `dontWrite` if dirty (`pager.go:1234-1244`).
   **Savepoint interaction:** a page freed by a merge that is then rolled back
   to a savepoint must restore its prior content — `freePage` uses
   `getWritablePage` (saving a savepoint copy) when savepoints are active
   (`pager.go:1253-1267`), and the `hasContent`/`dontWritePages` maps
   (`pager.go:154-178`) track freelist-leaf content for rollback. This is the
   **same machinery** the empty-leaf free already relies on; the only delta is
   *more* pages freed per delete. Gated by `TestSavepointFreelistConsistency`
   (`storetest/crashtest_test.go:1882`) and `savepoint_crash_test.go`.
4. **WAL framing / codec unchanged.** Output pages are ordinary dirty pages,
   encoded + framed by the existing commit path (`codec.go`, `wal.go`). Page-1
   header offset handling preserved by `rebuild*Page` and the
   `finishParentRemoval` page-1 branch.
5. **Atomicity within the tx.** A merge is several page writes + several
   `freePage`s, all dirtied in the writer cache and committed as one WAL
   transaction (one commit frame). Partial application is impossible: either
   the whole WAL tx is recovered or none of it. A crash *mid-balance* (before
   commit) leaves the pre-delete tree intact on disk.

**Gating tests (must pass, `-tags vfs`):**

- `internal/btree`: `TestRebalance*` (`rebalance_test.go` — esp.
  `TestRebalanceRootCollapse:49`, `TestRebalanceDeleteAndReinsert:162`,
  `TestRebalanceStress:95`); `TestMergeCursor_*` (`btree_merge_cursor_test.go`
  — `RootCollapseOnPage1:168`, `NonRootEmptyInteriorFree:260`,
  `DeleteAllFromThreeLevelTree:295`, and the Previous/Next reverse-scan cases
  that exercise cursor traversal over a merged tree); `insert_delete_sqlite_test.go`;
  `TestSavepointCrash*` + `savepoint_crash_test.go`; `TestCrash1..8`
  (`crash_test.go`); `IntegrityCheck` after every delete workload
  (`integrity_divider_test.go` divider-range checks are the make-or-break
  validator for §3/§4 divider correctness).
- storetest (cwd `any-store-tests/storetest`): `TestCrashFuzzShort:408`,
  `TestRepeatedCrashSameDB:460`, `TestCommitSyncCrash:2255`,
  `TestWALTruncationRecovery:2844`, `TestCrashOnCheckpoint:247`,
  `TestSavepointFreelistConsistency:1882`,
  `TestCursorMutationDuringIteration:2696`, plus `optverify_test.go` /
  `crossengine_test.go` cross-engine checks. Run e.g.
  `go test -tags vfs -timeout 12m ./storetest/ -run
  'CrashFuzzShort|CommitSyncCrash|RepeatedCrashSameDB|SavepointFreelistConsistency'
  -crash.iterations=8`.

## Measurable success criteria

### a. New test — `TestDeleteRebalance_FillFactor` (`internal/btree/balance_test.go`)

Reuse the existing harness verbatim: `walkLeavesForFill` / `reportFillStats` /
`leafFillStats` (`btree_balance_quick_test.go:151-278`). The fill metric is
already defined there: per-leaf `used = cellCount*2 + (usable −
cellContentOff) − fragBytes`; `leafCapacity = usable − 8`;
`avgFill = Σused / (leafCount·leafCapacity)`.

Test shape (deterministic; pageSize=1024, valSize=80, 4-byte keys, single write
tx per phase; mirrors `TestBalanceNonroot_FillFactor:27`):

```
for each pattern in {random, range_low, range_mid}:
    open db (PageSize:1024); create ns; insert N=20000 rows; commit
    record before := walkLeavesForFill(bt)          // ~2067 leaves, ~84% fill
    db.pager.deleteRebalanceDispatchCount.Store(0)
    delete a large fraction (random: 80% by i%5!=0; range_*: a contiguous
        key band) in one tx; commit
    require IntegrityCheck() == nil                  // divider-range validator
    full forward scan: assert surviving keys == expected set, in order
    full reverse scan (Cursor.Previous): same set
    stats := walkLeavesForFill(bt)
    reportFillStats(...)
    assert deleteRebalanceDispatchCount > 0          // the dispatch guard
    assert stats.leafCount < before.leafCount * 0.45 // leaves SHRINK (today: ~0.88)
    assert avgFill >= 0.55                           // fill stays high (today: 0.19)
    assert stats.totalCells == survivingCount        // no lost/dup cells
```

Thresholds (conservative, derived from the measurement above and SQLite-class
balancing):
- **random / delete 80%**: `leafCount ≤ 0.45·before` (today 0.88; survivors'
  ideal is ~0.23·before; 0.45 leaves headroom for the omitted editPage/rekey
  micro-opts and incomplete consolidation), `avgFill ≥ 0.55` (today 0.19;
  SQLite reaches ~0.65–0.70 on this workload). The **primary** case.
- **range_low** (delete the lowest 80% of keys): exercises left-edge `nxDiv`
  selection and repeated leftmost merges + balance-shallower; same thresholds.
- **range_mid** (delete a 60% contiguous middle band): forces 3-sibling
  gathers straddling the band edges; `avgFill ≥ 0.55`.

Also add `TestDeleteRebalance_Merge` (focused unit test, not a fill metric):
build a depth-2 tree with three adjacent leaves at ~35% fill each, delete a few
keys to push the middle one below 1/3, and assert via `lastBalanceNOld` /
`lastBalanceNNew` (`pager.go:108-109`) that `nOld==3`, `nNew==2` (a genuine
`k=nOld−1` merge), a page was freed (freelist count grew by 1), and
`IntegrityCheck` passes. Add `TestDeleteRebalance_RootCollapse`: delete down to
a single child under the root and assert the tree height drops by one
(reuse the `finishParentRemoval` path; cross-check
`TestMergeCursor_RootCollapseOnPage1`).

### b. On-disk fill-rate under delete-heavy load (before/after/vs SQLite)

Three measurements on the **same** insert-then-delete dataset:

1. **Engine fill metric** — the new test's `reportFillStats` output (avg /
   median / min / histogram / leaf count). Capture **before** (current `main`,
   the measured `[after-delete-80pct]` row: 1828 leaves, avg 18.9%, min 8.7%)
   and **after** (the branch). Expected delta on random/20000/delete-80%:
   leaves `1828 → ≤ ~520`, avg `18.9% → ≥ 55%`, the `0-25%`/`25-50%` histogram
   buckets drained.

2. **`Collection.Stats()` / `NamespaceSize`** on a representative dataset
   (`stats.go` → `NamespaceSize`, `{Pages, OverflowPages, Entries,
   PayloadBytes}`). Procedure — same workload generator before/after:
   ```
   # insert 100k random-key docs into one collection; record:
   st0,_ := coll.Stats(ctx); sz0 := os.Stat(dbfile).Size()
   # delete 80% (random); checkpoint; record:
   st1,_ := coll.Stats(ctx); sz1 := os.Stat(dbfile).Size()
   # fill = PayloadBytes / ((Pages - interiorPages) * usable)
   ```
   Expected after the change: `st1.Pages` (live leaf pages) far fewer than on
   `main`; freelist grows (freed pages reused by later inserts); `PayloadBytes
   / Pages` (fill) up from ~0.19 to ≥0.55. File size `sz1` unchanged without a
   vacuum (no on-disk truncation) — the win is **freelist reuse + fill**, not
   file shrink. Overflow page count unchanged (balance never re-chains
   overflow).

3. **vs SQLite on the identical dataset** — the repo benches against real
   SQLite (modernc) via the storetest cross-engine harness
   (`optverify_test.go`, `crossengine_test.go`). Load the same random-key
   dataset (key = blob PK) into a SQLite `WITHOUT ROWID` table, delete the same
   80%, and compare:
   - `sqlite3 file 'PRAGMA page_count; PRAGMA page_size; PRAGMA freelist_count;'`
     — SQLite also retains freed pages on the freelist (no auto-vacuum), so the
     **page_count** stays ~constant and **freelist_count** rises, matching
     any-store's post-fix behaviour.
   - `SELECT name, SUM(pgsize), SUM(payload) FROM dbstat GROUP BY name;` — the
     `dbstat` virtual table gives SQLite's per-table live fill. Target:
     any-store **after** within ~10% of SQLite's live leaf count and fill on
     the random workload. **Before** any-store is ~3.5× SQLite's live leaf
     count (1828 vs ~520).

Report the before/after/SQLite triple as a table in NOTES when the
delete-degradation drift is moved to "resolved".

## Risks / phasing / non-goals

**Phasing (each phase independently testable, ordered):**

1. **Underfull leaf merge.** Add the §1 trigger + `deleteRebalanceLeaf` (§2)
   calling `balanceNonroot(inject.active=false)`; add the §4a parent cascade
   and §4b root collapse (reuse `finishParentRemoval`). Keep `tryMergeLeaf`
   alive but unused (for A/B cross-check). Gate: `TestRebalance*`,
   `TestMergeCursor_*`, new `TestDeleteRebalance_FillFactor` (random) +
   `_Merge` + `_RootCollapse`, full `internal/btree` suite + `IntegrityCheck`.
2. **Interior first-key divider advance (§3, Option A).** Detect `idx==0` leaf
   delete; advance the matching ancestor divider. Gate: divider-range
   `IntegrityCheck` (`integrity_divider_test.go`), random+range delete fuzz,
   `insert_delete_sqlite_test.go`.
3. **Fold empty-leaf path into the cascade (§6).** Route the existing
   free-empty-leaf `removeChildFromParent` tail through the §4a parent-
   underfull cascade. Gate: `TestRebalanceEmptyPageRemoval`, savepoint + crash
   suite, storetest crash subset.
4. **Cleanup.** Delete `tryMergeLeaf` / `removeMergedRightSeparator` + their
   test-only callers; update `go_to_sqlite.json` rows
   (`btree.go:*btree.Delete` → add `balance_nonroot` / `balance`;
   drop the `tryMergeLeaf` row); move the delete-degradation drift to
   "resolved" in NOTES with the before/after/SQLite table.

**Risks:**
- *Divider correctness on first-key delete (§3).* A missed divider advance
  leaves a stale (too-large) lower bound → `IntegrityCheck` failure (search is
  still correct under `<`/`>=`, but the invariant is violated). Mitigation:
  `IntegrityCheck` after every delete workload + the planted-violation test
  (`TestIntegrityCheck_DividerRange_CatchesPlantedViolation:138`).
- *Cascade depth.* The §4a parent cascade recurses up the tree; bounded by
  `btCursorMaxDepth` exactly as the insert cascade
  (`insertSepIntoAncestor`). Guard identically.
- *Merge thrash.* Deleting and re-inserting around the 2/3 boundary could
  merge then immediately split. SQLite tolerates this (same threshold both
  ways); the back-off pass (`balance.go:502-538`) biases outputs away from the
  boundary, damping it. Watch `TestRebalanceDeleteAndReinsert:162`.
- *Performance.* Each underfull delete now does an O(pooled cells) gather +
  rebuild instead of an O(1) cell drop. This is the same order as the insert-
  side balance and rarer than the fast in-place delete (only fires below 2/3
  fill). Watch the delete benchmarks for no regression on the common
  not-underfull delete (which stays O(1) — the §1 gate skips balance).
- *Cursor stability (§5).* Pre-existing WAL-snapshot isolation makes this a
  non-issue for the write path and unchanged for the read path; the audit of
  `Find().Delete()` (collect-ids-then-delete) is the one residual check.

**Non-goals:**
- **No on-disk format change** (page layout, cell encoding, freelist, codec,
  WAL) — purely an in-memory redistribution + freelist return.
- **No file shrink / vacuum** — freed pages go to the freelist for reuse, like
  SQLite without auto-vacuum. (A compacting vacuum is a separate feature.)
- **Reuse `balance_nonroot`** — do not write a second balancer; the merge
  direction is the existing one with `inject.active=false` plus the §4
  completion (cascade + shallower).
- **Do not port** SQLite's write-cursor `bPreserve`/`CURSOR_*` machinery (no
  write cursor in any-store).
- **Rollback:** revert the commit — no migration, no format change.
