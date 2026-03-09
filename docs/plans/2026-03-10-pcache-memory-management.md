# pcache Memory Management — Slab Allocator, Persistent Cache, LRU Fix

## Overview
- Bring pcache memory management in line with SQLite's pcache1.c model: global slab allocator, per-cache bulk allocation, proper LRU ordering, buffer recycling, admission control
- Cap total page cache memory across all open databases (hundreds of DBs scenario)
- Make reader cache persistent across transactions (nuke only when WAL changes) for dramatically better hit rates
- Limit max concurrent readers per DB to bound memory growth

## Context (from discovery)
- Files/components involved:
  - `internal/btree/pcache.go` — page cache (LRU + dirty list, fetch/create/release)
  - `internal/btree/pager.go` — pager (writerCache, getPageWriter/Reader, releasePage, pagerStress)
  - `internal/btree/db.go` — DB struct, BeginRead/Rollback, readerCachePool, Options
  - `internal/btree/page.go` — page struct definition
  - `internal/btree/btree.go` — btree.getPage routing (writer vs reader)
  - `internal/btree/page_slab.go` — NEW file for global slab allocator
- SQLite reference files (in `../sqlitec/src/`):
  - `pcache1.c` — slab allocator, bulk alloc, LRU, fetch/unpin, admission control
  - `pcache.c` — dirty list management, release, stress, clear
  - `pager.c` — pagerBeginReadTransaction, pager_reset, cache hit detection
  - `wal.c` — walIndexTryHdr changed flag
- Allowed drifts from SQLite documented below in detail

## Allowed Drifts from SQLite

1. **No PGroup**: No cross-cache page stealing. Each cache (writer/reader) is isolated. SQLite itself defaults to this in multithreaded mode (`pcache1.c:718-719`, `separateCache=1`).
2. **No hash table**: Continue using Go `map[uint32]*page` instead of SQLite's `apHash[]` with chaining (`pcache1.c:199-200`). Go maps are hash tables internally.
3. **No circular LRU**: Keep doubly-linked list with head/tail pointers instead of SQLite's circular list with anchor node (`pcache1.c:112-115`). Same semantics, simpler code.
4. **No nRef**: Keep `pinCount` (same thing, different name). SQLite uses `nRef` in PgHdr (`pcache.h:37`).
5. **No PgHdr/PgHdr1 split**: Keep single `page` struct. SQLite splits into public PgHdr (`pcache.h:25-48`) + private PgHdr1 (`pcache1.c:117-127`) for the pluggable pcache2 interface we don't need.
6. **No pcache2 plugin interface**: Direct implementation, no vtable indirection (`pcache1.c:1196-1207`).
7. **Slab uses `[][]byte` slice, not contiguous buffer**: SQLite carves a contiguous `void*` buffer into fixed slots by pointer arithmetic (`pcache1.c:283-288`). Go slices carry length and capacity — pointer arithmetic isn't idiomatic. Same semantics: O(1) get/put from a free list.
8. **Slab accepts all buffers back (no range check)**: SQLite checks `SQLITE_WITHIN(p, pStart, pEnd)` (`pcache1.c:381`) to distinguish slab vs heap allocations in `pcache1Free`. We accept all buffers since they're all `[]byte` — overflow buffers get recycled into the slab, which is fine.
9. **Slab lazy init, not library init**: SQLite initializes in `pcache1Init` (`pcache1.c:695-741`) at library init time. We do lazy init on first `Open()` or explicit `ConfigPageCache()` call.
10. **Per-cache bulk alloc uses individual page structs, not contiguous pBulk**: SQLite allocates one contiguous `pBulk` buffer and carves it (`pcache1.c:312-327`). We allocate individual page structs with slab-allocated data buffers. The page struct itself is small (~96 bytes) — the buffer is what matters for memory, and that comes from the slab.
11. **Reader cache cleared on walMaxFrame change, not change-counter file read**: SQLite reads 15 bytes from the DB file at offset 24 (`pager.c:5410`) and compares via `memcmp(pPager->dbFileVers, dbFileVers)` (`pager.c:5418`). We compare `walMaxFrame` only — sufficient because any commit increments `mxFrame` in the WAL header. This avoids reading the WAL header bytes; we already have `walMaxFrame` from `pager.beginRead()`.
12. **`sync.Pool` for pcache struct recycling**: Go-specific, replaces `malloc`/`free` of the PCache1 struct (`pcache1.c:771`, `pcache1.c:1184`).
13. **Drop `createFlag=0`**: SQLite has `createFlag` values 0 (lookup only), 1 (soft), 2 (unlimited). We drop 0 since our `fetch()` method already handles lookup-only.
14. **Per-cache max page check, not PGroup-level**: SQLite checks `nPurgeable > nMaxPage` on the PGroup level (`pcache1.c:1094`, cross-cache). We check per-cache `len(pages) > maxPages` plus global slab pressure. Same effect without PGroup.

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- Run tests after each change
- Maintain backward compatibility

## Testing Strategy
- **Unit tests**: required for every task
- Run `go test ./internal/btree/ -count=1 -short` after each task
- Use `testing.AllocsPerRun` for allocation-sensitive phases (slab, recycling)
- Benchmark cache hit rates before/after LRU fix

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: Fix LRU insertion order (insert at HEAD, evict from TAIL)
**Bug fix — currently FIFO instead of LRU.**
SQLite ref: `pcache1.c:1098-1101` (unpin inserts at `pGroup->lru.pLruNext` — HEAD), `pcache1.c:623-624` (evicts from `pGroup->lru.pLruPrev` — TAIL).
- [x] rename `lruAppend` → `lruPrepend` in `pcache.go:243-253`, insert page at HEAD (`pc.lruHead`) instead of TAIL
- [x] change `evictOne` in `pcache.go:275-290` to pop from TAIL (`pc.lruTail`) instead of HEAD
- [x] change `evictOne` to return `*page` (evicted page) instead of void — needed by Task 5 for buffer recycling
- [x] write test: pin pages 1-5, release in order 5,4,3,2,1, verify eviction order is 1,2,3,4,5 (least recently released first)
- [x] write test: release page A, fetch page A again, release page A — verify A is at MRU position (not evicted first)
- [x] run tests — must pass before next task

### Task 2: Dirty page move-to-front on release
**Bug fix — SQLite moves dirty pages to front of dirty list when unpinned.**
SQLite ref: `pcache.c:558` (`pcacheManageDirtyList(p, PCACHE_DIRTYLIST_FRONT)` on nRef==0 + dirty). Constants: `pcache.c:185-187`.
- [x] add `dirtyMoveToFront(p *page)` method to pcache — unlinks from current position, inserts at `pc.dirtyHead`
- [x] modify `release()` in `pcache.go:119-132`: when `pinCount` reaches 0 and page is dirty, call `dirtyMoveToFront(p)`
- [x] write test: dirty pages A, B, C; release C then A; verify dirty list order is A, C, B (most recently released at front)
- [x] write test: `findSpillVictim` returns B (oldest, at back) not A (most recently released)
- [x] run tests — must pass before next task

### Task 3: Global slab allocator
**New file `page_slab.go`. Process-global pre-allocated pool for `[]byte` page buffers.**
SQLite ref: `pcache1.c:222-236` (pcache1_g struct), `pcache1.c:271-291` (sqlite3PCacheBufferSetup), `pcache1.c:341-374` (pcache1Alloc), `pcache1.c:379-406` (pcache1Free), `pcache1.c:518-524` (pcache1UnderMemoryPressure), `pcache1.c:350,389` (bUnderPressure AtomicStore).
- [x] create `internal/btree/page_slab.go` with `pageSlab` struct: `mu sync.Mutex`, `freeList [][]byte`, `nTotal int`, `nSlab int`, `nOverflow int`, `nReserve int`, `underPressure atomic.Bool`, `pageSize int`
- [x] implement `Init(pageSize, nPages int)` — pre-allocate nPages buffers, set `nReserve = nPages/10 + 1` (matches `pcache1.c:279`)
- [x] implement `Get() []byte` — pop from freeList under lock; if empty, `make([]byte, pageSize)` overflow; update `underPressure` (matches `pcache1.c:344-356`)
- [x] implement `Put(buf []byte)` — append to freeList under lock; update `underPressure` (matches `pcache1.c:379-391`)
- [x] implement `UnderPressure() bool` — atomic load (matches `pcache1.c:520`)
- [x] add `var globalPageSlab pageSlab` package-level singleton
- [x] add `ConfigPageCache(pageSize, nPages int)` public API to init the slab (mirrors `sqlite3_config(SQLITE_CONFIG_PAGECACHE)`)
- [x] add lazy init in `Open()` — if slab not initialized, init with default size based on first DB's page size
- [x] write test: Init, Get N pages, verify all returned, freeList empty
- [x] write test: Get beyond slab capacity, verify overflow works, `UnderPressure()` returns true
- [x] write test: Put pages back, verify `UnderPressure()` clears when freeList refills
- [x] write test: concurrent Get/Put from multiple goroutines (race detector)
- [x] run tests — must pass before next task

### Task 4: Per-cache bulk allocation (pFree)
**Depends on: Task 3 (global slab). Pre-allocate ~100 page objects per cache on first use, drawing buffers from global slab.**
SQLite ref: `pcache1.c:201` (pFree field), `pcache1.c:297-330` (pcache1InitBulk), `pcache1.c:434-438` (pcache1AllocPage tries pFree first).
- [x] add `pFree []*page` and `bulkInit bool` fields to pcache struct in `pcache.go:13-40`
- [x] implement `initBulk()` method: `nBulk = min(maxPages, 100)`, allocate page structs with `data` from `globalPageSlab.Get()`, set `page.cache = pc`, push onto `pFree` (matches `pcache1.c:304-327`)
- [x] modify `create()` in `pcache.go:108-115`: try `pFree` first → if empty and `!bulkInit` call `initBulk()` → else allocate with `globalPageSlab.Get()`
- [x] write test: first create() triggers initBulk, subsequent creates use pFree without slab calls
- [x] write test: after pFree exhausted, pages allocated from slab directly
- [x] run tests — must pass before next task

### Task 5: Buffer recycling on eviction
**Depends on: Task 1 (evictOne returns `*page`), Task 3 (slab). Reuse evicted page's `[]byte` buffer for the new page instead of GC + alloc.**
SQLite ref: `pcache1.c:897-914` (step 4 — reuses LRU victim's buffer by re-keying at `pcache1.c:928`: `pPage->iKey = iKey`), `pcache1.c:903` (victim = `pGroup->lru.pLruPrev`). Return buffers to slab on cache clear/discard/truncate.
- [ ] modify `create()` eviction loop: capture returned `*page` from `evictOne()`, reuse its `.data` buffer for the new page (clear contents, reset fields, assign new pgno)
- [ ] modify `clear()` in `pcache.go:190-197`: iterate `pc.pages`, call `globalPageSlab.Put(p.data)` for each; also return `pFree` buffers to slab
- [ ] modify `discard()` in `pcache.go:200-219`: return evicted page's buffer to slab via `globalPageSlab.Put(p.data)`
- [ ] modify `truncate()` in `pcache.go:222-241`: return evicted page buffers to slab
- [ ] write test: fill cache to maxPages, create one more page — verify zero new allocations (buffer reused from evicted page, use `testing.AllocsPerRun`)
- [ ] write test: `clear()` returns all buffers to slab — verify slab freeList grows by expected count
- [ ] run tests — must pass before next task

### Task 6: Wire pcache.create() and pager through slab
**Depends on: Task 3 (slab). Replace all `make([]byte, pageSize)` in page allocation paths with slab-backed allocation.**
- [ ] update `acquireTempPage()` in `pager.go:994-1001` to use `globalPageSlab.Get()` for `page.data`
- [ ] update `recycleTempPage()` in `pager.go:1004-1014` to use `globalPageSlab.Put(pg.data)` (return buffer to slab, keep page struct in pagePool)
- [ ] verify `getPageWriter` and `getPageReader` allocate pages through `pcache.create()` which now uses slab — no direct `make([]byte)` in hot paths
- [ ] audit for remaining `make([]byte, pageSize)` in pager.go that should use slab (header reads, savepoint copies are OK to keep as-is — they're transient)
- [ ] write test: open DB, run read+write transactions, verify slab tracks all allocations (nTotal matches expected)
- [ ] run tests — must pass before next task

### Task 7: Admission control (createFlag)
**Depends on: Task 3 (slab pressure). Refuse cache growth when thrashing or under memory pressure.**
SQLite ref: `pcache1.c:881-892` (step 3 guards), `pcache1.c:197` (nRecyclable), `pcache1.c:589` (decrement), `pcache1.c:1102` (increment), `pcache1.c:886-891` (90% pinned, pressure + recyclable checks).
- [ ] add `nRecyclable int` field to pcache struct — tracks unpinned clean pages in LRU
- [ ] increment `nRecyclable` in `lruPrepend`, decrement in `lruRemove` and `evictOne` (replaces `nClean` or coexists — verify semantics match)
- [ ] change `create()` signature to `create(pgno uint32, createFlag int) *page` — createFlag 1=soft, 2=hard
- [ ] add step-3 guard in `create()` when `createFlag == 1`: return nil if `nPinned >= maxPages*9/10` OR `(globalPageSlab.UnderPressure() && nRecyclable < nPinned)` (matches `pcache1.c:886-891`)
- [ ] update callers: `getPageReader` → `createFlag=1`; `getPageWriter` → `createFlag=2`; stress retry → `createFlag=2` (matches `pcache.c:486`)
- [ ] handle nil return from `create()` in `getPageReader` — fall back to uncached `readTempPage()`
- [ ] write test: pin 95% of maxPages, soft create returns nil; hard create succeeds
- [ ] write test: slab under pressure + low recyclable ratio → soft create returns nil
- [ ] run tests — must pass before next task

### Task 8: Enforce max page on unpin
**Depends on: Task 3 (slab pressure), Task 7 (nRecyclable). Immediately evict instead of adding to LRU when cache is overfull and slab is under pressure.**
SQLite ref: `pcache1.c:1094-1095` (pcache1Unpin — `reuseUnlikely || nPurgeable > nMaxPage` → remove from hash + free).
- [ ] modify `release()` in `pcache.go:119-132`: after `pinCount` reaches 0, if `pc.purgeable && globalPageSlab.UnderPressure() && len(pc.pages) > pc.maxPages` → delete from `pc.pages`, return buffer to slab (don't add to LRU)
- [ ] write test: overfull cache + slab pressure → released page is immediately evicted, slab gets buffer back
- [ ] write test: overfull cache without pressure → page goes to LRU normally
- [ ] run tests — must pass before next task

### Task 9: Persistent reader cache across transactions
**Depends on: Task 5 (clear returns buffers to slab). Keep reader cache pages between transactions; nuke only when walMaxFrame changes.**
SQLite ref: `pager.c:3246-3267` (pagerBeginReadTransaction — `pager_reset` only if changed), `pager.c:3261` (`if( rc!=SQLITE_OK || changed ){ pager_reset(pPager); }`), `pager.c:1772-1776` (pager_reset clears cache + increments iDataVersion), `wal.c:2610-2611` (walIndexTryHdr sets `*pChanged = 1` via memcmp of WalIndexHdr).
- [ ] add `walMaxFrame uint32` field to pcache struct in `pcache.go`
- [ ] modify `BeginRead()` in `db.go:255-299`: after getting cache from pool, compare `cache.walMaxFrame` with new `maxFrame`; if different → `cache.clear()` (returns buffers to slab); if same → keep pages; set `cache.walMaxFrame = maxFrame`
- [ ] modify `ReadTx.Rollback()` in `db.go:905-921`: remove `tx.cache.clear()` call; just `db.readerCachePool.Put(tx.cache)` (cache may be reused with pages intact)
- [ ] note: no cross-goroutine notification needed — when a write tx commits, walMaxFrame advances; reader caches taken from the pool after this point will see the new walMaxFrame and clear themselves automatically
- [ ] write test: two sequential read transactions with no writes between — second tx gets cache hits on pages read by first tx
- [ ] write test: read tx, then write tx commits, then read tx — second reader's cache is cleared (walMaxFrame changed)
- [ ] write test: verify cleared cache returns buffers to slab (no memory leak)
- [ ] run tests — must pass before next task

### Task 10: Max concurrent readers limiter
**Depends on: Task 9 (persistent cache). Configurable semaphore to bound the number of concurrent read transactions per DB.**
No SQLite equivalent — our addition for memory management with many open DBs.
- [ ] add `MaxReaders int` to `Options` struct in `db.go` (default 4)
- [ ] add `readerSem chan struct{}` field to `DB` struct in `db.go:70-99`
- [ ] initialize `readerSem` as buffered channel with capacity `MaxReaders` in `Open()`
- [ ] acquire semaphore (`db.readerSem <- struct{}{}`) at start of `BeginRead()` before `mu.RLock()`
- [ ] release semaphore (`<-db.readerSem`) at end of `ReadTx.Rollback()` after pool.Put
- [ ] handle `db.closing` — don't block forever on semaphore if DB is closing (use select with closing check)
- [ ] write test: `MaxReaders` concurrent BeginRead succeed; `MaxReaders+1` blocks until one Rollback
- [ ] write test: DB.Close unblocks waiting readers
- [ ] run tests — must pass before next task

### Task 11: Verify acceptance criteria
- [ ] verify all 7 goals from Overview are implemented (slab cap, persistent cache, reader limit, slab allocator, bulk alloc, LRU fix, buffer recycling)
- [ ] verify edge cases: InMemory databases (`purgeable=false`) still work correctly with slab
- [ ] verify writer cache draws from slab (writerCache.create uses slab via pcache.create)
- [ ] run full test suite: `go test ./internal/btree/ -count=1 -race`
- [ ] run full test suite: `go test ./... -count=1 -short`
- [ ] verify test coverage for new code (pcache.go, page_slab.go, db.go changes)

### Task 12: [Final] Update documentation
- [ ] update `docs/NOTES.md` or relevant docs with pcache memory management design
- [ ] document `ConfigPageCache()` API and `MaxReaders` option
- [ ] document allowed drifts from SQLite in code comments

## Technical Details

### Data Structures

**pageSlab** (new, `page_slab.go`):
- Process-global singleton for `[]byte` page buffer allocation
- Soft cap: pre-allocated slab + `make()` overflow fallback
- `underPressure` atomic bool: true when `len(freeList) < nReserve`
- Thread-safe via `sync.Mutex` (matches `pcache1.c` MUTEX_STATIC_PMEM)

**pcache** (modified, `pcache.go`):
- `pFree []*page` — per-cache bulk pre-allocated pages (matches `pcache1.c:201`)
- `nRecyclable int` — unpinned clean pages in LRU (matches `pcache1.c:197`)
- `walMaxFrame uint32` — WAL snapshot for staleness detection
- `lruPrepend` replaces `lruAppend` — insert at HEAD not TAIL
- `evictOne` pops from TAIL, returns `*page` for buffer reuse
- `create(pgno, createFlag)` — admission control on createFlag==1

### Memory Bound Formula

Per-DB reader cache memory is bounded by: `MaxReaders × readerCacheSize × pageSize`.
Example: 4 readers × 50 pages × 4096 bytes = 800KB per DB.
With 200 open DBs: 200 × 800KB = ~156MB reader cache total.
Writer caches (1 per DB): 200 × 5000 pages × 4096 = ~3.8GB — this is why writer caches MUST draw from the global slab and respect the global cap.

### Processing Flow

**Page buffer lifecycle:**
```
ConfigPageCache() → pageSlab.Init() pre-allocates N buffers
                          ↓
pcache.initBulk() ← pageSlab.Get() (bulk of ~100 pages)
                          ↓
pcache.create() ← pFree pop (or slab.Get if exhausted)
                          ↓
page in use (pinned) → release → lruPrepend (MRU at head)
                          ↓
evictOne (from tail) → buffer reused for new page (or slab.Put)
                          ↓
cache.clear() → all buffers returned to pageSlab.Put()
```

**Reader cache lifecycle (persistent):**
```
BeginRead() → readerCachePool.Get()
                ↓
         walMaxFrame changed? → cache.clear() (buffers→slab)
         walMaxFrame same?    → keep pages, cache hits!
                ↓
         tx.Rollback() → readerCachePool.Put(cache) — no clear
```

## Post-Completion

**Manual verification:**
- Memory profiling with 100+ open databases — verify total page cache memory stays within slab cap
- Benchmark read-heavy workload before/after persistent cache — measure cache hit rate improvement
- Profile GC pressure before/after slab allocator — verify reduced allocation churn

**Future considerations:**
- Shrink API (`sqlite3PcacheShrink` equivalent) for external memory pressure signals
- `pSynced` optimization pointer in dirty list for faster spill victim search (matches `pcache.c:463-467`)
- Telemetry: expose slab stats (nTotal, nOverflow, underPressure) via metrics
