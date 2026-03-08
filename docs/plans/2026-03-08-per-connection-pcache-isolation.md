# Per-Connection Page Cache Isolation

## Overview

Rewrite any-store's shared page cache (`pcache`) to use per-connection private caches, matching SQLite's model. The shared cache forced complex workarounds (`pcache.mu`, `fetchPinned`, `createNoStress`, `reinsertDirty`) while readers never benefited from caching. This rewrite:
- Removes pcache mutex — each cache is single-goroutine, no contention
- Removes all shared-cache workarounds
- Gives readers their own caches — pages stay cached across lookups within a transaction
- Resolves 4 DRIFT comments at their root cause
- Introduces `masterStore` for InMemory mode to replace shared pcache as "disk"

## Context (from discovery)

**Files involved:**
- `internal/btree/pcache.go` — page cache with mutex and shared-cache workarounds
- `internal/btree/pager.go` — pager with `readPageMVCC`/`readPageUncached`, `pagerStress`, `getPageAtImpl`
- `internal/btree/page.go` — page struct (needs `cache *pcache` backpointer)
- `internal/btree/db.go` — DB/ReadTx/WriteTx, transaction lifecycle
- `internal/btree/btree.go` — btree struct with `getPage`, search functions with `mvcc bool` param
- `internal/btree/wal.go` — WAL checkpoint backfill writes to shared pcache for InMemory
- `internal/btree/NOTES.md` — drift documentation

**Key patterns:**
- Reader path: `btree.getPage` → `readPageMVCC` → `readPageUncached` (always bypasses cache, allocates throwaway page)
- Writer path: `btree.getPage` → `writePages` fast path → `getPageWriter` → `getPageAtImpl` (uses shared cache)
- Overflow reading: `leafFullKey`/`interiorFullKey` → `readOverflowChainMVCC`/`readOverflowChainAt` based on `mvcc bool`
- InMemory checkpoint: `wal.go:1831-1843` writes frames into shared pcache via `cache.create()`

**DRIFT comments to resolve:**
- `pcache.go:141-144` — re-check after stress (concurrent readers)
- `pager.go:1081-1082` — "shared cache means readers could trigger xStress"
- `pager.go:1093-1097` — page 1 unpinned guard
- `wal.go:663-668` — "SQLite has per-connection page caches"

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - write unit tests for new functions/methods
  - write unit tests for modified functions/methods
  - add new test cases for new code paths
  - update existing test cases if behavior changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- Run tests after each change
- Maintain backward compatibility of public API (DB, ReadTx, WriteTx, Cursor)

## Testing Strategy
- **Unit tests**: required for every task
- Run with `-race` flag for all test runs
- Stress tests (`TestCacheStress*`, `TestCheckpoint*`, `TestConcurrent*`, `TestSavepoint*`) run with `-count=3`
- Benchmark comparison: capture baseline before changes, compare after

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates from original scope
- Keep plan in sync with actual work done

## What Goes Where
- **Implementation Steps** (`[ ]` checkboxes): tasks achievable within this codebase
- **Post-Completion** (no checkboxes): items requiring external action

## Implementation Steps

### Task 1: Capture baseline benchmarks
- [x] write read/write benchmarks in `internal/btree/bench_test.go` (single-key Get, cursor scan, concurrent readers, writer+readers)
- [x] run benchmarks on current code: `go test -bench=. -benchmem -count=5 ./internal/btree/ > bench-before.txt`
- [x] save `bench-before.txt` for later comparison
- [x] run tests `cd internal/btree && go test -race -count=1 -timeout=300s ./...` — must pass

### Task 2: Add `cache *pcache` backpointer to page struct
- [x] add `cache *pcache` field to `page` struct in `internal/btree/page.go`
- [x] set `pg.cache` when pcache creates pages in `createInternal` (`internal/btree/pcache.go`)
- [x] update `releasePage` in `internal/btree/pager.go` to route via `pg.cache` when set (keep existing `uncached` and shared-cache paths as fallbacks)
- [x] update `recycleTempPage` to clear `pg.cache = nil`
- [x] write test verifying page.cache is set correctly after create and cleared after recycle
- [x] run tests — must pass

### Task 3: Add masterStore for InMemory mode
- [x] add `masterStore` struct to `internal/btree/pager.go` with `readPageInto(pgno, dst)` and `writePage(pgno, src)` methods using `sync.RWMutex`
- [x] add `masterStore *masterStore` field to pager struct
- [x] create masterStore in `newPager` when InMemory is true (non-purgeable map)
- [x] update `checkpointWithMode` in `internal/btree/wal.go` — InMemory backfill writes to masterStore instead of pcache (change signature to accept `*masterStore`)
- [x] update pager's `checkpointWithMode` wrapper to pass masterStore
- [x] update `readPageUncached` InMemory fallback: read from masterStore instead of shared pcache fetch+copy
- [x] write tests for masterStore read/write operations
- [x] write test for InMemory checkpoint backfill using masterStore
- [x] run tests — must pass

### Task 4: Add reader cache pool and getPageReader
- [ ] add `readerCachePool sync.Pool` and computed `readerCacheSize` (max(CacheSize/10, 50)) to DB struct in `internal/btree/db.go`
- [ ] add `cache *pcache` field to ReadTx struct
- [ ] add `cache *pcache` field to btree struct in `internal/btree/btree.go`
- [ ] add `getPageReader(pgno, walMaxFrame uint32, cache *pcache)` method to pager in `internal/btree/pager.go` — checks reader cache, validates against WAL index, reads from WAL/disk/masterStore on miss, populates reader cache
- [ ] add `readOverflowChainReader(firstPgno uint32, buf []byte, walMaxFrame uint32, cache *pcache)` to pager — overflow chain reading using reader cache
- [ ] write tests for getPageReader (cache hit, cache miss, stale cache eviction, InMemory fallback to masterStore)
- [ ] write tests for readOverflowChainReader
- [ ] run tests — must pass

### Task 5: Wire readers to private caches
- [ ] update `BeginRead` in `internal/btree/db.go` to allocate reader cache from pool, assign to `tx.cache`
- [ ] update `ReadTx.Rollback` to clear and recycle reader cache back to pool
- [ ] update `btree.getPage` reader path to call `getPageReader(pgno, walMaxFrame, bt.cache)` instead of `readPageMVCC`
- [ ] update `ReadTx.txGetPage` reader path to use `getPageReader` with `tx.cache`
- [ ] update `ReadTx.readOverflow` reader path to use `readOverflowChainReader` with `tx.cache`
- [ ] update `ReadTx.NewCursor` / `ReadTx.AppendValue` / `ReadTx.GetNamespace` — ensure btree structs get `cache` from ReadTx
- [ ] update `searchLeafWithOverflow`, `searchInteriorWithOverflow`, `leafFullKey`, `interiorFullKey` — replace `mvcc bool` param with `cache *pcache` (nil = writer path, non-nil = reader path using cache for overflow reads)
- [ ] update all callers of above functions to pass cache instead of mvcc bool
- [ ] write test: concurrent readers each get independent caches, verify no cross-contamination
- [ ] write test: reader cache correctly handles page staleness (writer commits between reader cache accesses)
- [ ] run full test suite with -race — must pass
- [ ] run stress tests `go test -race -run 'TestCacheStress|TestCheckpoint|TestConcurrent|TestSavepoint' -count=3 -timeout=300s` — must pass

### Task 6: Remove shared-cache workarounds from pcache
- [ ] remove `sync.Mutex` from pcache struct and all `mu.Lock()`/`mu.Unlock()` calls in `internal/btree/pcache.go`
- [ ] remove `fetchPinned()` method — replace its call in `getPageAtImpl` with plain `fetch()` + separate dirty check
- [ ] remove `createNoStress()` method and `noStress` parameter from `createInternal()`
- [ ] remove `reinsertDirty()` method — replace its call in `getWritablePage` with `makeDirty()`
- [ ] remove re-check-after-stress block (lines 141-152 in current pcache.go)
- [ ] remove `fetchAndMakeDirty()` (uses mutex, writer has single-goroutine access — inline or simplify)
- [ ] rename `pager.cache` → `pager.writerCache` throughout `internal/btree/pager.go`
- [ ] update all tests that reference `pager.cache` or `db.pager.cache` to use `pager.writerCache`
- [ ] run full test suite with -race — must pass

### Task 7: Simplify pager writer path
- [ ] simplify `getPageAtImpl` — remove `noStress` parameter, always allow stress (writer-only)
- [ ] remove `getPageAt` method (was reader-specific, now unused — readers use getPageReader)
- [ ] remove `readPageMVCC` and `readPageUncached` methods (dead code after Task 5)
- [ ] remove `readOverflowChainMVCC` method (replaced by readOverflowChainReader)
- [ ] simplify `pagerStress` — remove pagerState check (line 1081-1082), remove pgno==1 guard (lines 1093-1097)
- [ ] simplify `getWritablePage` — use `makeDirty` directly instead of `reinsertDirty`
- [ ] verify `pager.close()` cleans up writerCache properly
- [ ] run full test suite with -race — must pass

### Task 8: Update DRIFT comments and documentation
- [ ] remove/update DRIFT at `pcache.go:141-144` (re-check after stress — removed)
- [ ] remove/update DRIFT at `pager.go:1081-1082` ("shared cache means readers could trigger xStress" — removed)
- [ ] remove/update DRIFT at `pager.go:1093-1097` ("page 1 may become unpinned" — removed)
- [ ] update DRIFT at `wal.go:663-668` ("SQLite has per-connection page caches" — resolved, update comment)
- [ ] update `internal/btree/NOTES.md` — section on cache spill drifts, MVCC/snapshot isolation
- [ ] update code comments throughout pager.go, pcache.go referencing "shared cache" or "concurrent readers"
- [ ] run tests — must pass

### Task 9: Verify acceptance criteria and benchmark comparison
- [ ] run full test suite: `cd internal/btree && go test -race -count=1 -timeout=300s ./...`
- [ ] run stress tests: `go test -race -run 'TestCacheStress|TestCheckpoint|TestConcurrent|TestSavepoint|TestOverflow' -count=3 -timeout=300s`
- [ ] run InMemory tests: `go test -race -run TestInMemory -count=3`
- [ ] run from repo root: `cd /home/dev/work/any-store && go test -race ./...`
- [ ] run benchmarks: `go test -bench=. -benchmem -count=5 ./internal/btree/ > bench-after.txt`
- [ ] compare benchmarks: `benchstat bench-before.txt bench-after.txt`
- [ ] verify reader performance improved (cache hits vs allocate-per-page)
- [ ] verify no DRIFT comments remain that reference shared cache

### Task 10: [Final] Update documentation
- [ ] update README.md if any public API semantics changed
- [ ] update memory notes if new patterns discovered during implementation

*Note: ralphex automatically moves completed plans to `docs/plans/completed/`*

## Technical Details

### masterStore (InMemory "disk" replacement)
```go
type masterStore struct {
    mu    sync.RWMutex
    pages map[uint32][]byte // pgno -> page data copy
}
func (ms *masterStore) readPageInto(pgno uint32, dst []byte) bool  // RLock, copy, returns found
func (ms *masterStore) writePage(pgno uint32, src []byte)           // Lock, copy into map
```

### Reader cache lifecycle
```
BeginRead → cache = pool.Get() or newPcache(pageSize, readerCacheSize, purgeable=true)
Transaction → getPageReader checks cache → validates against WAL → reads on miss → caches
Rollback   → cache.clear() → pool.Put(cache)
```

### mvcc bool → cache *pcache migration
Functions changing signature (nil cache = writer, non-nil = reader with cache):
- `searchLeafWithOverflow(pg, key, usableSize, pager, walMaxFrame, cache)`
- `searchInteriorWithOverflow(pg, key, usableSize, pager, walMaxFrame, cache)`
- `leafFullKey(data, offset, usableSize, pager, walMaxFrame, cache)`
- `interiorFullKey(data, offset, usableSize, pager, walMaxFrame, cache)`

### DRIFT resolution table
| Location | DRIFT | Status |
|----------|-------|--------|
| pcache.go:141-144 | re-check after stress (concurrent readers) | REMOVE |
| pager.go:1081-1082 | shared cache + readers could trigger xStress | REMOVE |
| pager.go:1093-1097 | page 1 unpinned between operations | REMOVE |
| wal.go:663-668 | SQLite per-connection caches vs our shared | RESOLVE (we match SQLite now) |
| pcache.go:136-139 | xStress error ignored | KEEP (structural) |
| pager.go:1102-1105 | DONT_WRITE skip optimization | KEEP |
| pager.go:1340-1343 | pagerError eager cleanup | KEEP |
| wal.go:500-502,565-568 | pendingShmFrames deferred writes | KEEP |

## Post-Completion

**Manual verification:**
- Run full benchmark suite under load to confirm reader throughput improvement
- Test with any-store consumers (anytype-heart) to verify no regressions

**External system updates:**
- any-store version bump if public behavior changes
- anytype-heart integration test after merge
