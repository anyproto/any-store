# Cache Spill (pagerStress) Implementation

## Overview

any-store's page cache grows unbounded during large write transactions. A transaction modifying 100K 4KB pages consumes ~400MB regardless of `CacheSize`. SQLite solves this with `pagerStress()`: when the cache is full and needs to evict a page, dirty pages are "spilled" to the WAL mid-transaction (`isCommit=0`), marked clean, and become evictable.

**NOTES.md (line 991) documents this as "Severity: Critical (skipped)".**

The current `writeFrames()` has bugs that must be fixed first. Calling `writeFrames(pages, false, 0)` today would break correctness because `setBatch()` unconditionally updates `maxFrame` and writes to SHM hash tables, making uncommitted frames visible to readers.

## Context

- **Files involved:**
  - `internal/btree/wal.go` — walIndex struct, setBatch/set/getLatest/get, writeFrames/writeFramesMem, SHM hash, recover, reset
  - `internal/btree/pcache.go` — page cache, create/evict/makeClean, dirty/clean page tracking
  - `internal/btree/pager.go` — pager struct, beginWrite/commit/rollback/rollbackToSavepoint, getWritablePage, savepoints
  - `internal/btree/NOTES.md` — documents this as critical missing feature (line 991)
- **Related patterns:**
  - `walIndex.maxFrame` is a single atomic used by both writer (frame counting) and readers (snapshot bounds)
  - `setBatch()` always calls `shmHashWrite()` — no commit/non-commit distinction
  - `pcache` only evicts clean pages; dirty pages cause unbounded growth when `nClean == 0`
  - Savepoints save page copies lazily in `getWritablePage()`
- **Dependencies:**
  - WAL frame format: `dbSize=0` in frame header = non-commit frame (already used by recovery)
  - Cross-process readers use SHM header's `mxFrame` for snapshot isolation
  - In-process readers use `walIndex.maxFrame` via `getLatest()` and `readHeaderCounters()`

## SQLite Reference

**SQLite C source code is located at `../sqlitec/src`** — use this for cross-referencing the original implementation.

| Component | File | Lines | Purpose |
|-----------|------|-------|---------|
| `pagerStress()` | pager.c | 4609-4681 | Stress callback: spills dirty page to WAL |
| `SPILLFLAG_*` | pager.c | 447-449 | `OFF`, `ROLLBACK`, `NOSYNC` control flags |
| `pagerWalFrames()` | pager.c | 3179-3236 | Wraps `sqlite3WalFrames()`, sets `nList=1` for spill |
| `walFrames()` | wal.c | 4015-4243 | Core: writes frames, `walIndexAppend` always, `walIndexWriteHdr` only on commit |
| `walIndexAppend()` | wal.c | 1295-1360 | Writes aPgno + hash slot, calls `walCleanupHash` on collision |
| `walCleanupHash()` | wal.c | 1233-1282 | Removes hash entries for frames > `mxFrame` (recovery from abandoned spill) |
| `sqlite3PcacheFetchStress()` | pcache.c | 445-490 | Invokes xStress when page count > szSpill |
| `subjournalPageIfRequired()` | pager.c | 4582-4588 | Records page in sub-journal before spill (savepoint support) |

**Key SQLite design decisions:**
- `walFrames(isCommit=0)`: calls `walIndexAppend()` (writes SHM hash), updates private `pWal->hdr.mxFrame`, does NOT call `walIndexWriteHdr()` (SHM header stays at committed mxFrame), does NOT fsync
- Cross-process readers are protected because they read `mxFrame` from the SHM header (not updated until commit)
- Same-process readers use `pWal->hdr.mxFrame` snapshot from `beginRead`, never the live value
- `SPILLFLAG_ROLLBACK` prevents re-entrant spills during savepoint rollback

## Development Approach

- **Testing approach**: Regular (code first, then tests for each task)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- Run tests after each change
- Maintain backward compatibility
- **CRITICAL: Follow the SQLite C implementation as closely as possible.** The goal is to replicate the full flow and logic from the SQLite C source (`../sqlitec/src`). Read the relevant SQLite C functions before implementing each task. Do not invent alternative approaches when SQLite already has a proven solution. Adapt only where Go language specifics require it (e.g., goroutines vs threads, sync.Mutex vs pthread, atomics API differences).
- **CRITICAL: Document all drifts from SQLite.** When the implementation must diverge from SQLite's C code — whether due to Go idioms, existing any-store architecture, or deliberate design choices — add a `// DRIFT from SQLite: <reason>` comment at the point of divergence. Also maintain a running list of drifts in the "Drifts from SQLite" section below.

## Testing Strategy

- **Unit tests**: Required for every task — test both success and error/edge cases
- Tests in `internal/btree/*_test.go` using `testify/assert` and `testify/require`
- Use existing helpers: `tempDBWithPageSize()`, `putN()`, `updateAll()`, `countKeys()`
- Pattern: Create DB -> BeginWrite/BeginRead -> operations -> Commit/Rollback -> verify

## Progress Tracking

- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with + prefix
- Document issues/blockers with ! prefix
- Update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: Separate committed vs uncommitted maxFrame in walIndex

**Problem:** `setBatch()` (`wal.go:536`) unconditionally calls `wi.maxFrame.Store(f)` for spilled frames. `getLatest()` (`wal.go:590`) reads `wi.maxFrame.Load()` and passes it to `shmHashGet()`, making uncommitted frames visible to readers. `readHeaderCounters()` (`pager.go:932`) also reads the inflated `maxFrame`.

**Fix:** Add `mxCommitFrame` atomic to `walIndex` that only advances on commit. Keep `maxFrame` as the total (committed + spilled) frame count for writer-internal use.

- [x] Add `mxCommitFrame atomic.Uint32` field to `walIndex` struct (`wal.go:465`)
- [x] Update `getLatest()` (`wal.go:590`): change `wi.maxFrame.Load()` -> `wi.mxCommitFrame.Load()` for the SHM hash fallback bound
- [x] Update `readHeaderCounters()` (`pager.go:932`): change `maxFrame.Load()` -> `mxCommitFrame.Load()`
- [x] Update `writeFrames()` (`wal.go:1226`): on `commit=true`, call `wi.mxCommitFrame.Store(wi.maxFrame.Load())`
- [x] Update `writeFramesMem()` (`wal.go:1331`): same treatment — only update `mxCommitFrame` on commit
- [x] Update `writeHeader()` (`wal.go:631`): use `mxCommitFrame` for the SHM header, not `maxFrame`
- [x] Update `reset()` (`wal.go:608`): also reset `mxCommitFrame` to 0
- [x] Update `recover()`: set `mxCommitFrame` to recovered maxFrame (only committed frames survive recovery)
- [x] Write tests: `TestWriteFramesCommitFalseDoesNotAdvanceMxCommitFrame` — call `writeFrames(pages, false, 0)`, verify `mxCommitFrame` unchanged while `maxFrame` advanced
- [x] Write tests: `TestGetLatestIgnoresSpilledFrames` — spill frames, verify `getLatest()` uses `mxCommitFrame`
- [x] Write tests: `TestReadHeaderCountersIgnoresSpilledFrames` — spill frames, verify `readHeaderCounters()` uses committed maxFrame
- [x] Run tests — must pass before next task

### Task 2: Defer SHM hash writes until commit

**Problem:** `setBatch()` always calls `shmHashWrite()` for every frame. Cross-process readers scanning hash tables could find uncommitted spilled frames.

**Fix:** Defer all `shmHashWrite()` calls until commit. Accumulate frame->pgno pairs in a slice, flush them in one batch at commit time. The in-process `pageMap` still gets entries immediately (needed for writer to see its own spilled pages).

- [x] Add `pendingShmFrames []struct{ pgno, frame uint32 }` field to `walIndex` struct
- [x] Modify `setBatch()` (`wal.go:536`): always update `pageMap`, only call `shmHashWrite` when not deferring (add `commit bool` parameter or separate deferred path)
- [x] Add `flushPendingShmFrames()` method to `walIndex`: writes all pending entries to SHM hash tables, clears the pending slice
- [x] Update `writeFrames()`: call `flushPendingShmFrames()` on commit after `setBatch()`
- [x] Update `writeFramesMem()`: same treatment (though InMemory is always `inProcess=true`, keep consistent)
- [x] Ensure `set()` (`wal.go:525`) used during recovery still writes SHM hash immediately
- [x] Write tests: `TestWriteFramesCommitFalseDoesNotWriteShmHash` — spill frames, verify `shmHashGet()` returns 0 for spilled pages before commit
- [x] Write tests: `TestWriteFramesCommitFlushesToShm` — spill + commit, verify `shmHashGet()` finds all frames after commit
- [x] Run tests — must pass before next task

### Task 3: Handle rollback of spilled frames

**Problem:** After spill, frames are in WAL and `pageMap`. On rollback, they must be cleaned up. Spilled frames in WAL are harmless (no commit marker), but `pageMap` entries and `maxFrame` must be restored.

- [x] Add `savedWalFrame uint32` field to `pager` struct (`pager.go:32`)
- [x] Update `beginWrite()` (`pager.go:314`): save `p.savedWalFrame = p.wal.nFrame.Load()`
- [x] Add `rollbackToFrame(frame uint32)` method to `walIndex`: remove `pageMap` entries with frame > target, restore `maxFrame`, clear `pendingShmFrames`
- [x] Update `pager.rollback()` (`pager.go:1146`): call `walIndex.rollbackToFrame(p.savedWalFrame)` after discarding dirty pages
- [x] Update `pager.rollbackToSavepoint()` (`pager.go:1234`): roll back WAL to savepoint's `walFrame` count via `rollbackToFrame()`
- [x] Write tests: `TestRollbackCleansUpSpilledFrames` — spill, rollback, verify `pageMap` rolled back and `maxFrame` restored
- [x] Write tests: `TestRollbackToSavepointWithSpilledFrames` — savepoint, spill, rollback to savepoint, verify correct state
- [x] Run tests — must pass before next task

### Task 4: Cross-process reader isolation verification

**Problem:** Cross-process readers must not see spilled frames before commit. Verify end-to-end with SHM.

- [x] Write tests: `TestCrossProcessReaderDoesNotSeeSpilledFrames` — multi-process mode: spill (no commit), verify SHM header `mxFrame` unchanged, `shmHashGet` doesn't find spilled frames
- [x] Write tests: `TestRecoveryIgnoresSpilledFrames` — write spill frames to WAL (no commit marker), recover, verify only committed frames are visible
- [x] Run tests — must pass before next task

### Task 5: Add pcache stress callback

**File:** `pcache.go`

Add a stress callback that the pager registers, invoked when cache is full and all clean pages are exhausted.

- [x] Add `xStress func(p *page) error` field to `pcache` struct (`pcache.go:12`)
- [x] Add `szSpill int` field to `pcache` struct (spill threshold, 0 = use maxPages)
- [x] Add `findSpillVictim() *page` method: walk dirty list, return first page with `pinCount == 0`
- [x] Modify `create()` (`pcache.go:81`): after clean eviction loop exhausts clean pages, if cache still full and `xStress != nil`, find unreferenced dirty victim, unlock mutex, call `xStress(victim)`, re-lock (victim now clean and evictable)
- [x] Write tests: `TestPcacheStressCallbackInvoked` — set small maxPages, fill cache with dirty pages, verify stress callback fires
- [x] Write tests: `TestPcacheStressOnlyUnreferenced` — pinned dirty pages not offered to stress callback
- [x] Write tests: `TestPcacheNoStressWhenCleanPagesAvailable` — stress callback not invoked when clean eviction succeeds
- [x] Write tests: `TestPcacheStressDisabledForInMemory` — non-purgeable caches don't invoke stress
- [x] Run tests — must pass before next task

### Task 6: Implement pagerStress callback

**File:** `pager.go`

Register a stress callback during pager initialization. When invoked, spill the given dirty page to the WAL.

- [x] Add `doNotSpill uint8` field to `pager` struct (bitmask for SPILLFLAG constants)
- [x] Add spillFlag constants: `spillFlagOff = 0x01`, `spillFlagRollback = 0x02`
- [x] Implement `pagerStress(pg *page) error` method:
  1. Check `doNotSpill` flags — return nil if set
  2. If savepoints active: save page data in savepoint if not already saved (`subjournalPageIfRequired` equivalent)
  3. Call `writeFrames([pg], false, 0)` — spill single page to WAL without commit
  4. Call `cache.makeClean(pg)` — page becomes evictable
- [x] Register callback: in `newPager()` or `open()`, set `p.cache.xStress = p.pagerStress`
- [x] Update `rollback()` (`pager.go:1146`): set `doNotSpill |= spillFlagRollback` before restoring pages, clear after
- [x] Update `rollbackToSavepoint()` (`pager.go:1234`): set `doNotSpill |= spillFlagRollback` during restore, clear after
- [x] Write tests: `TestPagerStressSpillsDirtyPage` — trigger stress, verify page written to WAL without commit marker, page marked clean
- [x] Write tests: `TestPagerStressSpillFlagOff` — set spillFlagOff, verify no spill occurs
- [x] Write tests: `TestPagerStressSpillFlagRollback` — during rollback, verify no spill occurs
- [x] Write tests: `TestPagerStressWithSavepoint` — spill with active savepoint, verify page data saved for rollback
- [x] Run tests — must pass before next task

### Task 7: End-to-end commit flow with spill

Verify the commit flow works correctly when pages have been spilled mid-transaction. Spilled pages are `makeClean()`'d, so `appendDirtyPages()` won't include them. Remaining dirty pages get written with `commit=true`. The commit frames follow spill frames contiguously since `nFrame` was already incremented during spill.

- [x] Verify `commit()` (`pager.go:1013`) needs no structural changes (spilled pages already clean, not re-collected)
- [x] Ensure `commit()` calls `flushPendingShmFrames()` after `writeFrames(true)`
- [x] Ensure `commit()` updates `mxCommitFrame` to current `maxFrame` (via writeFrames commit path)
- [x] Write tests: `TestPagerStressThenCommit` — spill some pages, then commit, verify all data correct and readable
- [x] Write tests: `TestPagerStressThenRollback` — spill some pages, rollback, verify spilled frames cleaned up
- [x] Write tests: `TestPagerStressThenSavepointRollback` — savepoint, modify, spill, rollback to savepoint, verify data restored
- [x] Run tests — must pass before next task

### Task 8: Integration and stress tests

End-to-end tests verifying bounded memory, concurrent readers, checkpoint, and InMemory mode.

- [x] Write tests: `TestLargeTransactionBoundedMemory` — insert enough data to exceed CacheSize, verify cache stays near limit (pages are spilled), all data readable after commit
- [x] Write tests: `TestSpillThenCheckpoint` — spill, commit, checkpoint, verify database file correct
- [x] Write tests: `TestSpillMultipleRounds` — trigger spill multiple times in one transaction, commit, verify
- [x] Write tests: `TestConcurrentReaderDuringSpill` — reader holds snapshot while writer spills, verify reader sees consistent pre-spill data
- [x] Write tests: `TestSpillInMemoryMode` — verify InMemory databases handle spill correctly via `writeFramesMem`
- [x] Run tests — must pass before next task

### Task 9: Verify acceptance criteria

- [ ] Verify all requirements from Overview are implemented
- [ ] Verify edge cases are handled (rollback, savepoints, cross-process, recovery)
- [ ] Run full test suite (`go test ./...`)
- [ ] Run linter (`golangci-lint run` or project linter) — all issues must be fixed
- [ ] Verify cache memory stays bounded during large transactions

### Task 10: [Final] Update documentation

- [ ] Update `internal/btree/NOTES.md` line ~991: change "Severity: Critical (skipped)" to document implementation
- [ ] Update README.md if needed

## Technical Details

### Data structures

**walIndex additions:**
```go
type walIndex struct {
    // ... existing fields ...
    mxCommitFrame    atomic.Uint32                          // highest COMMITTED frame — visible to readers
    pendingShmFrames []struct{ pgno, frame uint32 }         // deferred SHM hash writes
}
```

**pager additions:**
```go
type pager struct {
    // ... existing fields ...
    doNotSpill    uint8   // spillFlagOff | spillFlagRollback bitmask
    savedWalFrame uint32  // WAL frame count at beginWrite() time
}
```

**pcache additions:**
```go
type pcache struct {
    // ... existing fields ...
    xStress func(p *page) error  // callback to spill a dirty page
    szSpill int                   // spill threshold (0 = use maxPages)
}
```

### Processing flow

1. Writer modifies pages via `getWritablePage()` -> pages marked dirty
2. Cache reaches `maxPages`, `nClean == 0` in `create()`
3. `create()` calls `xStress(victim)` on unreferenced dirty page
4. `pagerStress()` writes victim to WAL via `writeFrames(false)`, calls `makeClean(victim)`
5. Victim now evictable via LRU; `create()` can evict and allocate
6. On commit: remaining dirty pages written with `commit=true`, `flushPendingShmFrames()`, `mxCommitFrame` updated
7. On rollback: `rollbackToFrame(savedWalFrame)` cleans up `pageMap`, restores `maxFrame`

### Frame visibility rules

| Component | Sees spilled frames? | Mechanism |
|-----------|---------------------|-----------|
| Writer (same tx) | Yes | `pageMap` updated immediately |
| In-process reader | No | `getLatest()` uses `mxCommitFrame` |
| Cross-process reader | No | SHM header not updated until commit; `shmHashWrite` deferred |
| Recovery | No | Scans for commit markers (`dbSize != 0`), ignores trailing non-commit frames |

## Drifts from SQLite

Document every intentional divergence from the SQLite C implementation here. Each drift must also have a `// DRIFT from SQLite: <reason>` comment in the Go source code.

| Location | SQLite Behavior | Our Behavior | Reason |
|----------|----------------|--------------|--------|
| *(to be filled during implementation)* | | | |

## Post-Completion

**Manual verification:**
- Memory profiling: run large transaction benchmark, verify RSS stays bounded at ~CacheSize
- Cross-process test: open DB from two processes, verify spill doesn't corrupt reader snapshots

**Performance testing:**
- Benchmark spill overhead vs unbounded cache (tradeoff: more WAL I/O vs less memory)
- Tune `szSpill` default (SQLite uses `cache_spill` pragma, default = `cache_size`)
