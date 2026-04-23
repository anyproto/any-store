# Design: Port SQLite's `balance_quick` to any-store

**Date:** 2026-04-23
**Branch:** `btree`
**Scope:** `internal/btree/` only
**Status:** Approved — proceed to implementation plan

---

## 1. Goal

Eliminate the measured ~57% leaf-count overhead on monotonic-append workloads
(ObjectID primary-key inserts — any-store's dominant write pattern) by porting
SQLite's `balance_quick` fast path to any-store, and close the
`internal/btree/NOTES.md:1425-1429` "Path Tracking Stores Only Page Numbers"
item by migrating `path []uint32` to a `[]pathEntry` that carries the parent
cell index — the direct equivalent of SQLite's cursor stack
(`btreeInt.h:553-556`).

Reference point for the measured cost: `internal/btree/btree_balance_quick_test.go`
(reproducer landed separately) reports, for 5000 monotonic appends at
pageSize=1024, valSize=80:

| Scenario | Leaves | Avg fill | Leaf-count overhead vs balance_quick ideal |
|----------|--------|----------|---------|
| monotonic append (current) | 714 | 60.7% | +56.9% |
| random (current) | 631 | 68.6% | +38.7% |
| balance_quick ideal | 455 | ~99% | — |

The 60.6% monotonic fill is analytically derivable from `leafSplitPoint`
(`btree.go:217`) targeting 2/3 on the left page; the port recovers the
other ~40% of usable space for the dominant workload.

## 2. Scope

**In scope.**
- Rightmost-append fast path matching SQLite's `balance_quick` preconditions
  at `btree.c:9170-9174` (adapted for any-store's index-btree semantics — see §6).
- `path` structure change from `[]uint32` to `[]pathEntry{pgno, cellIdx, nCell}`.
- Consumption of `cellIdx` in every consumer that today does a linear scan
  on the parent to locate the child slot — insert side AND delete side.
- NOTES.md updates and a regression-guard test derived from the existing
  reproducer.

**Out of scope.**
- Leftmost-prepend symmetry (no current workload motivates it).
- Updates-at-rightmost that cascade to split (niche; falls through to slow path).
- `balance_nonroot` port (3-sibling redistribution) — separately documented
  in `internal/btree/NOTES.md:1408-1415` as "Important (partially addressed)";
  its own much larger change.
- Propagating a `BTREE_APPEND`-style hint from the collection layer
  (`btree.h:271`). The btree detects the condition locally from `(idx, path)`
  and needs no upstream help.
- Changes to `Cursor` (iteration). Cursor has its own frame structure and
  is orthogonal.

## 3. SQLite reference points

All references are to `/home/dev/work/sqlitec/src/`.

| SQLite symbol | Location | Any-store equivalent |
|---|---|---|
| `balance_quick()` function | `btree.c:7992-8086` | new `splitLeafRightmostAppend` (commit 4) |
| `balance_quick` dispatch | `btree.c:9169-9192` | dispatch branch in `splitLeafAndInsertWithPath` (commit 4) |
| `balance()` outer loop | `btree.c:9115-9250` | `insertIntoLeafWithPath` → `splitLeafAndInsertWithPath` chain |
| `balance_nonroot(pParent, iIdx, ...)` | `btree.c:8230, 9213` | `insertSepIntoInterior` (commit 2: takes `cellIdx` param) |
| `iIdx = pCur->aiIdx[iPage-1]` | `btree.c:9162` | `path[len-1].cellIdx` (commits 1+2) |
| cursor `apPage[]`/`aiIdx[]` | `btreeInt.h:553-556` | `pathEntry{pgno, cellIdx, nCell}` (commit 1) |
| push cursor frame | `btree.c:5446-5456` | `path = append(path, pathEntry{...})` in descent loops |
| `BTREE_APPEND` hint | `btree.h:271`, usage `btree.c:9397,9499` | not adopted — detection is local |
| `aBalanceQuickSpace[13]` | `btree.c:9117` | not applicable — any-store separators are variable-length, see §6 |
| intkey-only precondition `pPage->intKeyLeaf` | `btree.c:9170` | adapted; see §6 |

## 4. Data structure

```go
// pathEntry records one level of the root-to-leaf descent performed by
// Put/Delete/insertIntoParent. It mirrors SQLite's cursor stack pair
// (apPage[i], aiIdx[i]) at btreeInt.h:553-556.
//
//   pgno    — page number of the interior node at this level.
//   cellIdx — index within that page that we descended through:
//               0..nCell-1  -> the i-th cell's leftChild was followed;
//               == nCell    -> descended via the page's rightChild.
//             This is exactly searchInterior's second return value
//             (btree.go:883), which today is discarded at the call sites
//             (btree.go:1105, btree.go:2039, btree.go:2162).
//   nCell   — pg.header.cellCount at descent time; enables O(1) check
//             "descended via rightChild?" without re-reading the parent.
//             Required for the balance_quick dispatch guard (commit 4).
//
// Invariant: each cellIdx is used at most once, during the upward
// propagation step at that level. After a divider is inserted into
// `pgno`, the cellIdx for `pgno` is never consulted again; higher-level
// cellIdx values remain valid because a lower split does not change
// which pgno the higher level points to — only that pgno's *contents*.
type pathEntry struct {
    pgno    uint32
    cellIdx uint16
    nCell   uint16
}
```

Stack-buf shape preserved: `var pathBuf [8]pathEntry; path := pathBuf[:0]`
replaces the existing `var pathBuf [8]uint32` at `btree.go:1101`, `btree.go:2031`,
`btree.go:2158`. Per-entry size 4B → 8B; depth-8 buffer grows from 32B to 64B
on the stack. Negligible.

## 5. Implementation — four commits on branch `btree`

Each commit is self-contained, passes the full test suite (`go test -race -count=1 ./internal/btree/...`)
independently, and updates `internal/btree/NOTES.md` with the relevant
SQLite cross-references.

### Commit 1 — `btree: introduce pathEntry with cellIdx tracking`

Pure mechanical refactor. No behavior change.

**Add:** `pathEntry` struct (as in §4) in `btree.go`, with the full block comment.

**Change 7 function signatures** (replace `path []uint32` → `path []pathEntry`):

| Function | Location |
|---|---|
| `insertIntoLeafWithPath` | `btree.go:1140` |
| `updateLeafCell` | `btree.go:1311` |
| `splitLeafAndInsertWithPath` | `btree.go:1739` |
| `insertIntoParentWithPath` | `btree.go:1787` |
| `insertSepIntoAncestor` | `btree.go:1807` |
| `tryMergeLeaf` | `btree.go:2297` |
| `removeChildFromParent` | `btree.go:2437` |

**Change 3 path builders** (capture `cellIdx` and `nCell` from the descent):

- `Put` loop `btree.go:1101-1115` — the discarded second return of
  `searchInterior` at `btree.go:1105` is stashed as `cellIdx`; loop-local
  computation of `pg.header.cellCount` before descent becomes `nCell`.
- `Delete` loop `btree.go:2158-2172` — same treatment.
- `insertIntoParent` navigation loop `btree.go:2031-2061` — including the
  two special-case branches at `btree.go:2044-2054` (found-as-leftChild
  and found-as-rightChild); in the rightChild branch, record
  `cellIdx = nCell`.

**Do NOT change:**
- Any consumer's internal logic. `insertSepIntoInterior`'s linear re-search
  at `btree.go:1841-1849` is left in place. `tryMergeLeaf`'s childIdx scan
  at `btree.go:2316-2326` is left in place. `removeChildFromParent`'s
  scan is left in place. These become no-ops on the new field in this commit.

**Tests added:**
- `TestPath_CellIdxRightmost` — build a tree of depth ≥ 3 via monotonic
  inserts, then instrument `Put` (or a new test hook) to capture the
  path at the moment the leaf is hit, assert every entry has
  `cellIdx == nCell` (rightmost descent).

**NOTES.md:** partially close the "Path Tracking Stores Only Page Numbers"
item at `NOTES.md:1425-1429` — note that the index is now carried and
list the consumption work as Commits 2 & 3.

**Success criteria:**
- Full existing suite green: `go test -race -count=1 -timeout=300s ./internal/btree/...`
- `TestPath_CellIdxRightmost` passes.
- No perf regression (sanity check a couple of existing benchmarks).

### Commit 2 — `btree: skip parent re-search in insertSepIntoInterior using path cellIdx`

Harvests the insert-side win. Matches SQLite's `balance_nonroot(pParent, iIdx, ...)`
signature pattern at `btree.c:8230, 9213`.

**Change:**
- `insertSepIntoInterior` (`btree.go:1833`) gains a new `insertIdx int`
  parameter (range-checked `0 <= insertIdx <= n`).
- Callers pass `int(path[len-1].cellIdx)`:
  - `insertIntoParentWithPath` at `btree.go:1801`.
  - `insertSepIntoAncestor` at `btree.go:1828`.
- Delete the linear scan at `btree.go:1841-1849`.
- Preserve the existing branching at `btree.go:1889-1896` (insertIdx < n vs
  insertIdx == n); the semantics are unchanged.

**Correctness note in commit message and code comment:** when we descended
to child C via parent cell at position `cellIdx`, after splitting C the new
separator belongs at parent position `cellIdx` (the existing shift logic
at `btree.go:1883-1896` places the new cell at `insertIdx` with
`leftChild = leftPgno`, and shifts what was at `insertIdx` to `insertIdx+1`
with its `leftChild` updated to `rightPgno`). This is precisely what SQLite
does — we are just reaching it without a binary search.

**Assertions:** in test/debug builds, guard against drift:
`if insertIdx < 0 || insertIdx > int(parentPg.header.cellCount) { return ErrCorrupt }`.

**Benchmark added:** `BenchmarkInsertSepIntoInterior_DeepTree` — insert
into a tree with wide parents (pageSize=4096, 1M rows, depth ≥ 4).
Expected improvement is proportional to parent cellCount; report the number.

**Success criteria:**
- Full suite green.
- Benchmark shows ≥ 3% improvement over the `path []uint32` baseline on
  a tree with parent `cellCount` ≥ 32 (the linear scan cost dominates
  there). Report raw numbers; regression would indicate the new
  parameter threading introduced overhead greater than the scan savings.

### Commit 3 — `btree: skip parent linear scan in tryMergeLeaf/removeChildFromParent using path cellIdx`

Harvests the delete-side win.

**Change:**
- `tryMergeLeaf` (`btree.go:2297`): replace the scan at `btree.go:2316-2326`
  with `childIdx := int(path[len-1].cellIdx)`. The `rightChild == leafPgno`
  special case at `btree.go:2324-2326` becomes `cellIdx == n`, branch
  preserved.
- `removeChildFromParent` (`btree.go:2437`): analogous treatment.

**Correctness note:** `tryMergeLeaf` is called on an underfull leaf we
just descended to. The parent-pointer index is exactly what the path
recorded. No drift as long as commit 1 built the path correctly.

**Success criteria:**
- Full suite green. Specifically:
  - `stressRebalance` flow in `helpers_test.go:99`
  - All `TestSavepoint*`, `TestCheckpoint*` tests
  - `TestCacheStress` with `-count=3`
- `IntegrityCheck()` passes on any test that invokes deletes.

### Commit 4 — `btree: add balance_quick fast path for rightmost-append splits`

The payoff. Port of SQLite `balance_quick` at `btree.c:7992-8086`, with
dispatch mirroring `btree.c:9169-9192`.

**Add new function `splitLeafRightmostAppend`:**

```go
// splitLeafRightmostAppend implements the fast path for appends at the
// rightmost edge of the btree. Port of SQLite's balance_quick
// (btree.c:7992-8086), adapted for any-store's index-btree semantics.
//
// Pre-conditions (checked by caller — mirror btree.c:9170-9174):
//   - idx == pg.header.cellCount        — new cell is rightmost on pg
//   - len(path) > 0                     — pg is not the btree root
//   - path[len-1].cellIdx == path[len-1].nCell
//                                        — pg was reached via parent's rightChild
//   - path[len-1].pgno != bt.rootPage   — parent is not the btree root
//                                        (SQLite: pParent->pgno != 1)
//
// Semantic adaptation from SQLite (btree.c:8066-8070): SQLite's intkey
// tables use the largest key of pPage as divider — for intkey semantics
// that IS the right value, because the new rowid is placed on pNew and
// the separator invariant is "left child keys <= separator". any-store's
// interior search (btree.go:883) uses the invariant "left child keys <
// separator, right child keys >= separator", so the divider is the
// *first* key of the new right sibling — i.e. the new key itself.
//
// After this returns, pg retains all its pre-insert cells unchanged
// (100% fill), rightPg contains exactly the new (key, value), and the
// parent has gained a new rightmost cell with leftChild=pg.pgno and
// rightChild repointed to rightPg. Parent overflow cascades through the
// standard path (insertIntoParentWithPath → insertSepIntoInterior),
// matching SQLite's balance() do-loop behavior at btree.c:9123.
func (bt *btree) splitLeafRightmostAppend(pg *page, key, value []byte, path []pathEntry) error {
    // 1. Allocate new right sibling (equiv. btree.c:8010 allocateBtreePage).
    //    Initialize as empty leaf (equiv. btree.c:8022 zeroPage).
    // 2. Insert (key, value) as the sole cell of rightPg via
    //    insertLeafCellAt — this handles overflow for oversized payloads
    //    (equivalent to SQLite's ptrmap handling at btree.c:8046-8050;
    //    any-store has no ptrmap).
    // 3. Build divider = bytes.Clone(key) — see semantic-adaptation note
    //    above.
    // 4. Delegate to insertIntoParentWithPath(pg, divider, rightPgno, path).
    //    This (a) inserts the new cell at parent position cellIdx using
    //    commit 2's direct-index path, with leftChild=pg.pgno, and
    //    (b) sets parent.rightChild = rightPgno when cellIdx == nCell
    //    (which this fast path guarantees). Equivalent to SQLite's
    //    insertCell at btree.c:8074 plus put4byte at btree.c:8079.
    // 5. pg itself is NOT modified — this is the whole point. The
    //    writable reservation from Put at btree.go:1121 is benignly
    //    unused on the fast path.
}
```

**Add dispatch guard at the top of `splitLeafAndInsertWithPath` (`btree.go:1739`):**

```go
// Port of SQLite's balance_quick dispatch at btree.c:9169-9192.
// The intKeyLeaf precondition (btree.c:9170) is intkey-specific and
// has no any-store equivalent; we compensate with the divider
// adaptation in splitLeafRightmostAppend. All other 4 preconditions
// map directly.
if idx == int(pg.header.cellCount) && len(path) > 0 {
    parent := path[len(path)-1]
    if parent.pgno != bt.rootPage && parent.cellIdx == parent.nCell {
        return bt.splitLeafRightmostAppend(pg, key, value, path)
    }
}
// ... existing slow-path code
```

**Tests (the balance_quick matrix):**

1. **Happy path.** 1000 monotonic appends, pageSize=1024, valSize≈80.
   Assert: leaf count ≈ `nRows / cellsPerFullLeaf ± 2`; every non-rightmost
   leaf fill ≥ 95%; `IntegrityCheck()` passes.
2. **Root-is-parent edge.** Small tree where the leaf's parent is the
   btree root. Fast path must NOT fire (SQLite `pParent->pgno != 1` at
   `btree.c:9173`). Verify by instrumenting `splitLeafRightmostAppend`
   with a test-only dispatch counter (e.g. a `sync/atomic` counter on
   the `btree` struct exposed by a test-only accessor) and asserting
   the counter stays at 0 for this fixture while > 0 for Matrix test 1.
3. **Cascade to parent split.** Monotonic inserts until the leaf fast
   path fires AND the divider insertion overflows the parent. Assert
   the parent split goes through the commit-2 direct-index path with
   `insertIdx == parent.nCell`; tree remains consistent via
   `IntegrityCheck()`.
4. **Interleaved rightmost + middle.** 500 monotonic appends → 500
   random inserts → 500 more monotonic appends. Asserts (a) random
   inserts use the slow path correctly, (b) later appends still hit
   the fast path when applicable, (c) final tree is valid.
5. **Overflow-bearing new cell.** Monotonic append where
   `len(key)+len(value) > maxLocalPayload`. Fast path allocates the
   overflow chain on the new right sibling; verify readback equals
   the written bytes.
6. **Savepoint rollback.** Begin savepoint → monotonic inserts that
   fire the fast path multiple times → rollback → verify tree matches
   pre-savepoint snapshot. Reuses existing savepoint test infra.
7. **Concurrent reader snapshot.** Begin reader → monotonic inserts
   via fast path → commit → reader's snapshot unchanged.
8. **IO-error injection at allocatePage.** Using the
   `btree_io_error_test.go` infrastructure, fail `allocatePage` inside
   the fast path. Assert clean rollback; no partial pages.

**Benchmark added:** `BenchmarkBalanceQuick_MonotonicAppend` in
`bench_test.go`. Reports rows/sec, final leaf count, and bytes-written
per insert (derive from file-size delta if no pager instrumentation).

**Ship criteria for commit 4:**
- Leaf-count reduction ≥ 40% on the monotonic reproducer (expected ~55%).
- Throughput improvement ≥ 10% on the monotonic benchmark.
- No regression on random-insert or mixed benchmarks. If any, dispatch
  guard is mis-firing.
- Stress suite green with `-count=3`:
  `TestCacheStress|TestCheckpoint|TestConcurrent|TestSavepoint|TestOverflow`.
- `IntegrityCheck()` passes across every new test.

**NOTES.md:** new subsection "B-tree Operations → Rightmost-Append Fast Path
(balance_quick port)" under the existing B-tree Operations heading
(`NOTES.md:1408`). Cross-references `btree.c:7992-8086` and `btree.c:9169-9192`;
documents the divider-key adaptation.

## 6. Semantic adaptation: why the divider differs from SQLite

SQLite's `balance_quick` (`btree.c:7992`) fires only for intkey leaves
(`pPage->intKeyLeaf`, `btree.c:9170`). In intkey-table btrees:
- Interior cells are `[child_pgno, varint(rowid)]`.
- The separator rowid bounds children: subtree below cell[i] has keys
  `<= separator[i]`; `rightChild` has keys `> separator[n-1]`.
- SQLite's divider = largest key currently on `pPage` (= the old max before
  the new overflow cell is accounted for) — see `btree.c:8066-8070`.
  That key ends up as the separator, and the new cell sits on pNew with
  a rowid strictly greater than the separator. Correct.

any-store is an index-btree:
- Interior cells are `[leftChild, len, key [, overflow]]`.
- Separator semantics at `btree.go:883` (`searchInterior`): binary search
  moves right on both `cellKey < target` and `cellKey == target`, so
  the invariant is "left child keys **<** separator, right child keys
  **>=** separator" (equal keys descend right of the separator).
- Therefore the divider must be the first key of the right sibling.
  In the fast path the right sibling contains exactly `(key, value)`,
  so divider = `bytes.Clone(key)`.

This divergence is a necessary consequence of separator-invariant
differences, not drift. It is documented both in
`splitLeafRightmostAppend`'s docstring and in the `NOTES.md` entry.

## 7. Risks & edge cases

| Risk | Mitigation |
|------|------------|
| `cellIdx` staleness after mid-operation tree modification | Each cellIdx is consumed at most once, at its own level, during upward propagation. Higher-level cellIdx values remain valid: lower-level splits modify their parent's contents but not which pgno the grandparent points to. Documented in `pathEntry` doc comment. |
| Root-as-parent mis-fire | Explicit guard `parent.pgno != bt.rootPage` mirroring SQLite's `pParent->pgno != 1` at `btree.c:9173`. Matrix test 2. |
| Single-leaf btree | `len(path) == 0` guard. |
| New cell > maxLocalPayload | `splitLeafRightmostAppend` reuses `insertLeafCellAt` which already handles overflow. Matrix test 5. |
| Divider key > interior maxLocal | Goes through existing `insertSepIntoInterior` overflow branch at `btree.go:1870-1881`. No new code. |
| `pg` is writable but not modified on fast path | Harmless. `putWritablePage`'s work was COW — if we don't mutate, there is no dirty write. One-line comment in code. |
| Benchmarks regress on random workloads | Dispatch guard is a 3-comparison tuple; wrong firing would appear immediately. Explicit regression check via `BenchmarkInsert_Random` before/after commit 4. |
| `cellIdx == nCell` encoding | `nCell` is carried in `pathEntry` (§4). Unambiguous, no sentinel needed. |
| Fast-path interaction with delete/rebalance | Balance_quick produces valid btree structure. Subsequent deletes use the standard rebalance path unchanged. Matrix test 6 covers rollback; existing delete tests cover merge semantics. |
| Encryption / WAL behavior | Fast path only decides which cells end up on which pgno. The WAL/codec layer sees ordinary page writes. No special handling. |
| IntegrityCheck | Must continue to pass. A failure indicates a porting bug. Run as an assertion in every new test. |

## 8. Non-changes

Explicitly confirmed unchanged by this design:

- Public API: `btree.Put`, `btree.Delete`, `ReadTx`, `WriteTx`, `Cursor`.
- Page formats, cell formats, db header, WAL format, checkpoint flow.
- Codec/encryption layer (`codec.go`, `codec_aes.go`, `codec_kdf.go`).
- Savepoint semantics (savepoints track writer cache dirty pages, unchanged).
- Existing NOTES.md content — only additions and the "partially addressed"
  marker flips on the Path Tracking item.

## 9. Follow-ups (explicitly deferred)

- Leftmost-prepend symmetric fast path (if descending-key workloads emerge).
- `balance_nonroot` port (NOTES.md "Important (partially addressed)").
- `BTREE_APPEND`-style upstream hint from collection layer (probably
  never needed given local detection works).

## 10. Ship sequence

Four commits on branch `btree`, linear, no PRs. After commit 4 the fill-factor
reproducer (`btree_balance_quick_test.go`) is promoted from a passive
diagnostic to a regression guard with explicit `require.Greater(avgFill, 0.90)`
on the monotonic case.
