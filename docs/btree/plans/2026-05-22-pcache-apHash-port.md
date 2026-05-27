# pcache apHash port — replace `map[uint32]*page` with SQLite's chained hash

## Overview

- Replace the page cache's Go `map[uint32]*page` with a SQLite-`pcache1`-style
  chained hash table (`apHash []*page` + an embedded per-page chain link), so a
  page's cache membership is carried *on the page* rather than in a side map.
- This eliminates the **two** hot-path costs the cache pays per page touched:
  1. `fetch()` does a Go-map hash lookup, and
  2. `release()` does a **second** Go-map hash lookup (the "ghost page" guard,
     `pc.pages[p.pgno] == p`) on every unpin.
- **Closes documented drift #2** ("Go `map[uint32]*page` instead of `apHash[]`")
  in [NOTES.md §9](../NOTES.md). This *reduces* btree drift from upstream — it
  does not add any.
- Scope is deliberately narrow: **only the page→struct hash lookup** changes.
  LRU ordering, dirty-list management, admission control, slab/bulk allocation,
  spill/stress, and the persistent-reader-cache machinery are untouched.

## Motivation (measured)

`Fullscan/Count` (`coll.Find(nil).Count(ctx)` → `btree.countPage`, a COUNT(*)-style
page-header walk) on the 10k-row `noIdxColl` is **+144% slower than v0.4.6 (real
SQLite via modernc)** on the same machine, same uncompressed data
(`v2 ≈ 75µs` vs `main ≈ 30µs`, n=5, p=0.008).

Root cause, established by **SIGPROF-immune** measurement (wall-clock phase timing
+ per-operation counters — *not* CPU-profile syscall attribution, which is
unreliable on macOS):

- Wall-clock split of v2's Count: **scan 95.8%**, read-tx setup 3.5%, `fcntl` 1.2%.
  (`fcntl` and the WAL read-lock are a faithful SQLite port and are **not** the
  problem — a prior profile blaming them was a macOS SIGPROF artifact.)
- Within the scan, **per page visited** the cache does **2 Go-map lookups + 2 LRU
  splices** (`fetch`: `pc.pages[pgno]` + `lruRemove`; `release`: `pc.pages[p.pgno]==p`
  + `lruPrepend`). Over `noIdxColl`'s ~2304 pages that is ~4600 `mapaccess1_fast32`
  calls ⇒ ~77µs. A SIGPROF-clean CPU profile of the pure scan attributes
  **`pcache.release` 45.8% / `pcache.fetch` 29.4%**, of which `runtime.mapaccess1_fast32`
  is ~38%.
- Cost is **linear in scanned page count** (~25–33 ns/page): 218 pages → 5.4µs,
  2304 pages → 77µs; flat across `-docs` (so it is *not* shared-cache pressure,
  WAL state, or page reads — 100% cache hit, zero `pread`).
- SQLite walks the same ~2300 pages at **~13 ns/page**: `pcache1FetchNoMutex`
  is one chained-bucket probe, and `pcache1Unpin` adds to the LRU via embedded
  pointers with **no hash op** on release. v2 pays two Go-map lookups for the
  same work → the ~2.4× gap.

Because every page fetch/release on every cursor goes through this path, the win
generalises beyond Count to all scans, index walks, and (via `getPageWriter`)
writes.

## Context (from discovery)

Files involved:
- `internal/btree/pcache.go` — the cache (this is the file that changes).
- `internal/btree/page.go` — the `page` struct (add one pointer field).
- `internal/btree/pager.go`, `btree.go` — callers of `fetch`/`create`/`release`/
  `discard`/`truncate` (no signature changes; verify no direct `pc.pages` access).

SQLite reference (`../sqlitec/src/pcache1.c`):
- `struct PgHdr1` (`pNext` hash-chain link; `pLruNext/pLruPrev` LRU links; `iKey`).
- `struct PCache1` (`apHash`, `nHash`, `nPage`).
- `pcache1FetchNoMutex` — bucket probe `apHash[iKey % nHash]`, walk `pNext` by `iKey`.
- `pcache1ResizeHash` — grow + rehash when `nPage >= nHash`.
- `pcache1RemoveFromHash` — unlink from bucket chain.
- `pcache1PinPage` / `pcache1Unpin` — LRU add/remove via embedded links, no hash op.

Existing accepted drifts that this plan **keeps** (do not touch): #1 no PGroup,
#3 head/tail LRU instead of circular anchor, #5 single `page` struct, #6 no
pcache2 vtable, #7–#10 slab/bulk, #11 dual-version staleness, #14 per-cache max.

## Current design and why it costs

`pcache.pages` is `map[uint32]*page`. The `page` struct already carries `next`/`prev`
(shared by the LRU list and the dirty list — a page is in at most one of them) and
`pgno`. There is **no** hash-chain link on the page, so membership lookups go
through the Go map:

- `fetch(pgno)` → `pc.pages[pgno]` (lookup #1).
- `release(p)` → `pc.pages[p.pgno] == p` (lookup #2) — the **ghost-page guard**.
  This exists because `discard`/`truncate`/savepoint-rollback can remove a page
  from the map *while a caller still holds it pinned*; on the eventual `release`
  the page is no longer in the map, and adding such a "ghost" to the LRU would
  make `evictOne` loop without shrinking `len(pages)`. The guard detects this by
  re-probing the map.

## Target design (pcache1-faithful)

A page's membership becomes a property *of the page* (its presence in a hash
bucket chain), exactly as in `pcache1`:

### Data structure changes

`page` (page.go) — add one field:
```go
hashNext *page // next page in its pcache hash bucket chain (apHash). Independent
               // of next/prev, which serve the LRU OR dirty list. Matches
               // SQLite PgHdr1.pNext (pcache1.c).
```
Note: `hashNext` is **independent** of `next`/`prev` — a cached page is always in
the hash chain, *and simultaneously* in either the LRU or dirty list. (`next`/`prev`
remain shared between LRU and dirty, which are mutually exclusive, as today.)

`pcache` (pcache.go) — replace the map:
```go
apHash []*page // hash buckets: pgno%len(apHash) -> head of chain via page.hashNext
nPage  int     // number of pages currently in apHash (was len(pc.pages))
// (drop: pages map[uint32]*page)
```

### Membership / ghost-page invariant

A page is "in this cache" **iff it is reachable from `apHash[p.pgno % nHash]` via
`hashNext`**. To keep `release` O(1) and hash-op-free (the whole point), make
membership directly testable on the page:

- `hashInsert(p)` links `p` at the head of its bucket and increments `nPage`.
- `hashRemove(p)` unlinks `p` from its bucket and decrements `nPage`; it sets
  `p.hashNext = nil`. After removal, `p` is "not in cache."
- `release(p)` adds `p` to the LRU **only if `p` is still in the cache**. Test
  this with a cheap, hash-op-free signal. **Recommended:** a `page.inCache bool`
  set `true` in `hashInsert` and `false` in `hashRemove`. (Alternative considered:
  reuse the existing `uncached` flag — rejected, it has a distinct MVCC meaning.)
  This replaces the `pc.pages[p.pgno]==p` re-probe with a field read.

This is correct because the only ways a page leaves the cache (`evictOne`,
`discard`, `truncate`, `clear`, `destroy`) all route through `hashRemove`, which
clears `inCache`. SQLite needs no such flag because its invariants guarantee a
pinned page is never removed from the hash; v2's `discard`/`truncate` can remove
pinned pages, so the flag makes that explicit and cheap. (A follow-up could
tighten v2 to SQLite's invariant and drop the flag, but that is out of scope.)

### Hash sizing

- Initial `nHash`: a small power of two (e.g. 256), or seed from `maxPages` rounded
  up to a power of two for the writer/large caches to avoid early resizes.
- Grow + rehash in `hashInsert` when `nPage >= nHash` (load factor 1.0, matching
  `pcache1ResizeHash`'s `nPage >= nHash` trigger). Double `nHash`, reinsert all
  pages. Use power-of-two sizes so `pgno % nHash` is a mask (`pgno & (nHash-1)`).
- `clear()`/`destroy()` reset buckets; `truncate()` does not shrink `nHash`.

## Method-by-method changes (pcache.go)

| Method | Change |
|--------|--------|
| `fetch` | `pc.pages[pgno]` → bucket walk `for p := apHash[pgno&mask]; p != nil; p = p.hashNext { if p.pgno==pgno {…} }`. Same pin + `lruRemove`. |
| `create` | The initial "already cached?" probe uses the bucket walk. On allocate, `hashInsert(p)` (sets `inCache`, bumps `nPage`, may resize) instead of `pc.pages[pgno]=p`. All `len(pc.pages)` reads → `pc.nPage`. |
| `release` | Drop the `pc.pages[p.pgno]==p` re-probe; gate LRU insert / overfull-discard on `p.inCache`. Overfull discard path calls `hashRemove` + `returnPageBuffer`. |
| `evictOne` | `delete(pc.pages, p.pgno)` → `hashRemove(p)`. |
| `discard` | `delete(...)` → `hashRemove(p)`. |
| `truncate` | iterate pages (see note) and `hashRemove` those with `pgno > maxPage`. |
| `clear` | reset all buckets (`apHash` to empty), `nPage = 0`, clear `inCache` as buffers are routed to `pFree`/freed. |
| `destroy` | same as clear + release `apHash`. |
| `makeDirty`/`makeClean`/`dirtyMoveToFront`/`findSpillVictim`/`lruPrepend`/`lruRemove`/`appendDirtyPages` | **unchanged** — they operate on `next`/`prev` (LRU/dirty), not the hash. |

Note on iteration: `clear`/`truncate`/`destroy` currently `range pc.pages`. With
`apHash`, iterate by walking each bucket chain (collect-then-remove, or walk with
a saved `hashNext` since `hashRemove` nils it). Keep these O(nPage).

## Crash-safety & correctness analysis

- **No on-disk, WAL, header, or freelist format change.** This is a purely
  in-memory restructuring of how cached `*page` structs are found. There is no
  migration and no change to what bytes are written or their order.
- **Dirty-list ordering is untouched** (it lives on `next`/`prev`/`dirtyHead`/
  `dirtyTail`), so the order in which dirty pages are handed to `appendDirtyPages`
  → WAL frame writes is byte-for-byte identical → no checkpoint/recovery impact.
- **Spill/stress, savepoint rollback, eviction, overfull-discard** semantics are
  preserved; only the membership test changes (`map` probe → `inCache` field).
  The ghost-page case is handled identically (LRU insert suppressed for removed
  pages), just without the second hash op.
- **Concurrency unchanged**: each pcache is single-goroutine-owned (writer cache,
  or a reader cache pinned to one read tx); no locking is added or removed.
- Risk surface = the eviction / spill / `discard` / `truncate` / ghost paths. These
  are exactly what the existing `internal/btree` stress and crash tests exercise.

## Tasks (ordered)

1. `page.go`: add `hashNext *page` and `inCache bool` to `page`; reset both in
   `pcache.resetPage`.
2. `pcache.go`: add `apHash []*page`, `nPage int`; remove `pages map`. Add
   `hashFind(pgno) *page`, `hashInsert(p)`, `hashRemove(p)`, `resizeHash()`.
   Initialise `apHash` in `newPcache` (power-of-two seed).
3. Rewrite `fetch` and the `create` lookup/insert to use the hash; replace all
   `len(pc.pages)` with `pc.nPage`.
4. Rewrite `release` to gate on `p.inCache` (drop the map re-probe).
5. Update `evictOne`, `discard`, `truncate`, `clear`, `destroy` to maintain
   `apHash`/`nPage`/`inCache`.
6. `grep -rn "\.pages\b" internal/btree` — confirm no other reader of the old map
   (tests included); migrate any found.
7. Docs: in `NOTES.md §9`, move drift #2 from the drift table to "resolved"
   (apHash chained hash now ported); refresh the prose "No hash table" bullet.
   Update `docs/btree/mappings/go_to_sqlite.json` rows for `fetch`/`create`/
   `release`/new hash helpers to cite `pcache1FetchNoMutex`/`pcache1ResizeHash`/
   `pcache1RemoveFromHash`/`pcache1PinPage`/`pcache1Unpin`.

## Test plan (correctness gate — must pass before commit)

- `go test -tags vfs -timeout 20m ./internal/btree/` — full engine suite. Pay
  attention to: cache spill/stress, eviction, savepoint rollback (truncate of
  pinned pages — the ghost path), checkpoint, recovery, multiprocess, and the
  cache-spill/per-connection-pcache tests. Treat only the known pre-existing
  failures (`TestMinFrameFilter_*`, `TestCheckpointBackfill_ShortWrite_InlineRead`,
  `TestRegression_Bug11_Simulation`, flaky `TestDb_Close/race`) as baseline —
  verify each still fails identically on the pre-change commit.
- `go test -tags vfs -race ./internal/btree/ -run 'Cache|Pcache|Spill|Evict|Cursor|Savepoint'`.
- storetest crash subset:
  `go test -tags vfs -timeout 12m ./storetest/ -run 'CrashFuzzShort|CommitSyncCrash|RepeatedCrashSameDB|WALTruncationRecovery|MultiProcess' -crash.iterations=8`.
- Add a focused unit test for the ghost path: pin a page, force its removal via
  `truncate`/`discard`, then `release` it, and assert it is **not** added to the
  LRU and `evictOne`/`nRecyclable` accounting stays consistent.

## Validation (performance — SIGPROF-immune)

Do **not** read `fcntl`/syscall share from a CPU profile that includes
`BeginRead`/`Rollback` (macOS over-attributes to the blocking syscall). Instead:

1. Bench A/B (build before = pre-change HEAD, after = fix; verify distinct md5):
   `cd /tmp/bench-run && /tmp/bench_after -no-compress -name Count -docs 500000 -duration 12s`
   vs before. Expect per-page ~33 ns → ~15 ns, Count ~75µs → ~30–35µs (≈ parity
   with main's ~30µs). Also check `Fullscan/EqFilter|RangeFilter|NeFilter` and a
   couple of index scans improve or hold; full suite shows **no B/op or allocs/op
   regression** (allocations are deterministic).
2. `-docs` sweep (10k/100k/500k): Count on the fixed-size `noIdxColl` stays flat
   vs golden size (confirms no new size-coupling).
3. A btree-package microbench looping `tx.Count` under one long-lived read tx
   with `-cpuprofile` (zero syscalls in the loop ⇒ SIGPROF-clean): confirm
   `runtime.mapaccess1_fast32` disappears and per-page ns drops.

## Risks & rollback

- **Risk:** the cache core is shared by reads, writes, checkpoint spill, and
  savepoint rollback; a hash-maintenance bug could surface as a lost/duplicated
  cached page (wrong data) or a leaked buffer. **Mitigation:** the change is
  semantically equivalent (same eviction/dirty/LRU behaviour, only the lookup
  structure changes), guarded by the stress/crash suite, plus the new ghost-path
  unit test.
- **Rollback:** revert the commit — no on-disk format or migration is involved.

## Non-goals

- No PGroup / cross-cache page stealing (drift #1 stays).
- No `PgHdr`/`PgHdr1` split or pcache2 vtable (drifts #5/#6 stay).
- No change to LRU shape (head/tail vs circular anchor — drift #3 stays), slab/bulk
  allocation, admission control, or the persistent-reader-cache / max-readers logic.
- No change to read-transaction locking (`fcntl`/WAL read-mark) — confirmed a
  faithful SQLite port and not the bottleneck.
