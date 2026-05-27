# Read/write hot-path optimizations (post-pcache) — SQLite-aligned, drift-free

## Scope & selection criteria

Follow-up to the four landed fixes (#1 sorter LIMIT, #2 indexed OFFSET, #5 per-commit
page-1 read removal, pcache `apHash` port). Three deep-investigation agents profiled the
remaining >20% regressions vs main (v0.4.6 real SQLite) on the same Mac, S2 off, using
**SIGPROF-immune** methods (wall-clock phase timing, per-op counters, `-memprofile`,
`-docs` sweeps — never CPU-profile syscall attribution, which mis-blames `fcntl` on macOS).

This plan includes **only** fixes that are (a) **confirmed** by measurement, (b) **aligned
with `../sqlitec/src`** (mirror SQLite's algorithm/shape), and (c) **introduce no btree-logic
drift** (they remove drift, or live in the anyenc/qplanner/codegen layers, never changing
btree/WAL/pager *semantics*). Items failing these bars are listed under "Excluded" with reasons.

Each task is independently committable and gated. SQLite C reference (orientir):
`/Users/roma/anytype/sqlitec/src/`.

---

## Task 1 — Remove `runtime.Gosched()` from `walShmBarrier` (write path)

**Confirmed cause.** `walShmBarrier()` (`internal/btree/wal.go`) is:
```go
var dummy uint32
atomic.StoreUint32(&dummy, 0)   // memory fence (MFENCE/DMB) — correct, keep
runtime.Gosched()               // scheduler yield — Go-specific heuristic, remove
```
It is invoked **~9×/commit** (readHeader ×~3, per-frame `shmHashWrite` ×~3, writeHeader ×2,
tryBeginRead ×1). Wall-clock A/B (env-gated skip of the yield, 20k iters mirroring bench
config) measured the 9 yields at **~8–11µs/commit idle, ~21µs under scheduler pressure**.
End-to-end recovery (rebuilt v2, per-op): `Crud/UpdateId` −11.3µs (48% of its gap),
`Crud/Insert` −8µs (33%), `Crud/DeleteReinsert` −34µs (40%), `Overflow/UpdateId` −10.5µs,
`Overflow/DeleteReinsert` −23µs (51%). Single-op CRUD/Overflow write regressions are
**per-commit-fixed-cost dominated**, and the yield is the single largest *removable* component.
(`Overflow/DeleteReinsert` +128% is explained by it being **two** commits/op = 18 yields —
not chain alloc/free, re-confirming the earlier overflow-not-the-hot-path finding.)

**SQLite alignment.** SQLite's barrier is `unixShmBarrier` (`os_unix.c:5488`) →
`sqlite3MemoryBarrier` (`mutex_unix.c:98-104`) = a pure `__sync_synchronize()` CPU fence,
**no scheduler yield**. Removing the `Gosched()` makes `walShmBarrier` exactly match SQLite.
NOTES §7 already claims "walShmBarrier … matches SQLite" — this makes that true.

**Drift impact: REMOVES drift.** Pure deletion of a Go-specific heuristic. The
`atomic.StoreUint32` fence (the actual MFENCE/DMB providing cross-process publish ordering)
stays, so memory-ordering semantics are unchanged — **no btree/WAL logic change**.

**Crash-safety.** The WAL publish-ordering contract (aPgno/hash-slot before mxFrame;
copy-2 before copy-1 in header writes; cross-process frame visibility) is provided by the
**fence**, not the yield (Go's memory model: atomics establish happens-before; `Gosched`
only reschedules). Agent ran the WAL/SHM/multiprocess/checkpoint/recover btree suite +
storetest `CrashFuzzShort|WriteReorder|CommitSyncCrash` with the yield disabled — all passed.
**Full gate before commit:** `go test -tags vfs ./internal/btree/` (esp. `multiprocess_test.go`,
`shm_hash_test.go`, `walcrash_sqlite_test.go`, checkpoint/recovery) + storetest
`-run 'CrashFuzz|WriteReorder|CommitSyncCrash|CrashDuringWALRecovery|MultiProcess' -crash.iterations=8`.

**Change.** Delete the `runtime.Gosched()` line (and the `runtime` import if now unused).

**Expected gain.** ~8–11µs per single-op commit (×2 for the 2-commit Delete/Reinsert),
33–51% of each single-op write gap. Negligible on batch/bulk (correctly — yield amortizes there).

**Validation (SIGPROF-immune).** Per-scenario MIN over the 8s bench, `/tmp/bench_main` vs
rebuilt v2, on `Crud/{Insert,UpdateId,DeleteReinsert}`, `Overflow/{Insert,UpdateId,DeleteReinsert}`;
`-memprofile` to confirm zero alloc change.

---

## Task 2 — Projected / lazy field decode in the scan + filter path (anyenc + qplanner)

**Confirmed cause.** Every scanned row is fully materialized into an owned `anyenc.Value`
tree before the filter looks at one field. Decode micro-bench on the exact `buildDoc` bytes:
`ParseOwned` full doc (920 B incl. an 80-element `nums` int array) = **611 ns/row**; the same
doc *without* `nums` (193 B) = **213 ns/row** → the unused `nums` array costs **~398 ns/row
(~65% of decode)**, pure waste for a filter like `{a:50}`. Per-row EqFilter split: cursor
`Next` ~8 ns, off-page value copy ~38 ns, **`ParseOwned` 611 ns (dominant)**, filter eval ~5–10 ns.
**Allocations are flat** (the parser's `cache.vs` is reused) — this is a **CPU (decode)**
problem, not allocation. The measured filter gap to SQLite (~387 ns/row) ≈ the wasted `nums`
decode. This same scan-decode cost is **~90% of the unindexed-sort regressions** too
(`Sort/*NoIdx` sort by `val`: a third agent isolated the sorter itself at only ~9.5% / ~23 ns/row;
the rest is this scan+decode), so Task 2 also addresses `Sort/NoIdx|LimitNoIdx|DescNoIdx`.

**SQLite alignment.** This is the v2 analogue of `OP_Column` (`vdbe.c:3010`), which fetches
the raw payload pointer (no copy) and incrementally parses the record header only up to the
requested column (`vdbe.c:3083-3088`, `aOffset[]`), extracting one column into a reused
register — it never touches `nums`. Lazy/projected decode moves v2's execution toward SQLite's.

**Drift impact: none to the btree port.** The change lives in `anyenc/` (doc codec) and
`internal/qplanner/` (query execution) — **not** `internal/btree/`. No btree NOTES § is touched.
It is a *new* divergence from anyenc's current eager parse, but anyenc has **no SQLite
counterpart** (it is any-store's own format), so "alignment" here means matching SQLite's
*behavior* (decode only what's referenced), which it does.

**Mechanism.** anyenc is a sequential stream (no offset table), so SQLite-style random column
access is impossible — but **structural stream-skip is** (the existing `parseValue(b, nil)` /
`c==nil` path in `anyenc/parser.go` walks a value without allocating).
1. Add `Parser.ParseProjected(b []byte, want fieldSet) (*Value, error)`: parse the top-level
   object; for each key, decode its value into the cache only if the key ∈ `want`, else skip
   via the no-alloc structural path. For `{a:50}` it decodes `id`,`a` and skips
   `b,c,val,email,…,tags,nums` → decode ~611 ns → ~120–150 ns/row.
2. Derive the referenced field set statically from the `query.Filter` (the `Key.Path` roots
   are known — `query/filter.go:189`) and the `Sort` key fields; pass it into
   `FullScanIter.checkFilter` (`internal/qplanner/fullscan_iter.go:231`) and
   `FilterIter` / the sort key-extraction path.
3. **Fall back to full `ParseOwned`** whenever the whole doc is needed: `Iter().Doc()`
   materialization, nested-path/array-membership filters that scan all elements
   (`query/filter.go:69-100`), or any filter whose field set can't be determined statically.
   Keep the `DocParsed` handoff correct (a matched row that will be emitted must end up fully
   parsed) — re-parse fully on match, or parse-full-on-first-emit.

**Crash-safety.** Read path only — no WAL/durability surface. Confirm.

**Expected gain.** ~400–480 ns/row → ~3.5–4.5 ms off the 12.7 ms `EqFilter` (plausibly
reaching/beating SQLite's 8.8 ms); proportional wins on `RangeFilter`/`NeFilter`/`ComplexAndOr`
and the unindexed sorts. **Memory strictly improves** (fewer Values built).

**Validation.** Unit-test `ParseProjected` == `ParseOwned` on the projected fields across all
`buildDoc` shapes incl. arrays/overflow; benchmark `-check` (EqFilter=100, RangeFilter=2100,
NeFilter=9900, ComplexAndOr>0) must still pass; re-run the four `Fullscan/*Filter` + `Sort/*NoIdx`;
confirm `B/op`/`allocs/op` do not rise. Guard array/`$ne`-over-array filters to fall back.

---

## Task 3 — Make the warm page-cache hit path inlinable (Count / scan traversal) — SECONDARY

**Confirmed cause.** `Fullscan/Count` walks ~2304 pages; the walk is ~94% of the op. The
pcache `apHash` port already removed the hash-lookup cost (`hashFind` now ~4.6%). What remains
is **per-page call-frame + branch + LRU-splice overhead that does not inline**: each page does
`countPage`→`getPage`→`getPageReader`→`fetch`→`hashFind`, then `releasePage`→`release`→`lruPrepend`.
`-gcflags=-m=2`: `getPageReader: cost 1225 exceeds budget 80`, `release: cost 261`, `fetch: cost 116`
— none inline. ~13–16 ns/page in v2 vs SQLite's ~2 ns/page. `countPage` already pins each parent
while iterating children, so it fetches each page exactly once (same as SQLite) — **not** extra
fetches.

**SQLite alignment.** Mirror `sqlite3PagerGet`/`getPageNormal` (`pager.c:5535`), whose
cached-hit branch (`pager.c:5568-5573`) returns immediately, and `pcache1FetchNoMutex`/
`pcache1PinPage` (`pcache1.c`) which are branch-light/inlined.

**Drift impact: none.** A **codegen-shape** refactor (Go inlining), not a logic change — same
fetch/pin/release behavior. Split `getPageReader` into a tiny inlinable fast path
(`if pg := cache.fetch(pgno); pg != nil { return pg, nil }`) + a `//go:noinline`
`getPageReaderMiss` for the WAL/disk/codec body; factor the cold LRU-overfull/dirty branches
out of `fetch`/`release` so the clean-hit core fits the inline budget.

**Confidence: MEDIUM (hence secondary).** (a) The main-side Count baseline is disputed — the
agent measured SQLite Count at **4.49µs** isolated vs the ~30µs the full-suite benchstat reports,
so the true gap magnitude is uncertain; (b) the fix is an inline-budget tweak (Go-version
sensitive) with an honest target of **2–3× on the walk (~15–20µs)**, not parity. Land Tasks 1–2
first; pursue this only if Count remains a priority after.

**Explicitly rejected for Count:** the pinned-ancestor cursor stack (NOTES §15 `apPage[]` port)
— re-evaluated, does **not** help, because `countPage` already holds parents pinned and fetches
each page once. (It may help cursor-`Next`-heavy filter scans, but that overlaps Task 2's
bigger decode win; defer.)

**Crash-safety.** Read path only. Validation: `-gcflags=-m` shows the hit path now inlines;
re-run `Fullscan/Count`; `go test -tags vfs ./internal/btree/` (pcache/cursor/MVCC/crash) —
pin/unpin semantics unchanged.

---

## Benchmark-correctness item (any-store-tests, not an engine change)

`CompoundRev/SortAscDesc` (+56%) and `CompoundRev/FilterSort` are **not** a v2 bug — they are a
benchmark API-misuse. `Sort` is variadic (`.Sort("a","-b")`, see `query/sort_test.go`), and
`parseSortString` splits each field only on `.`. The benchmark calls `.Sort("a,-b")` (one
comma-joined string) → `parseSortString` yields a single bogus field named `a,-b` → no index
matches → `FullScan → TopK(100)` over 500k rows. Verified via `Explain` and a patch:
`.Sort("a","-b")` → `IndexScan(a,-b) → Fetch → Limit`, and the real 500k bench drops
**801ms → ~115µs (~7000×)**, results still correct — and it benefits **main equally** (it's a
measurement fix). v2's `IndexSortMatch` (`internal/qplanner/planner.go:1438`) already implements
SQLite's `wherePathSatisfiesOrderBy` (`where.c:5148`) semantics (equality-prefix skip + composite
asc/desc-direction rule), so **no engine change is needed**.

- **Fix:** in `any-store-tests/benchmark/scenarios.go`, express multi-field sorts as variadic
  fields (the two `compound_rev` scenarios → `.Sort("a","-b")`).
- **Do NOT** make `parseSortString` split on `,` — that would break legitimate comma-bearing
  field names, diverge from the documented variadic API, and has **no SQLite analog** (it would
  *introduce* drift). Excluded for that reason.
- **Also (docs):** add a NOTES.md drift entry recording that the qplanner index-for-order
  matching mirrors SQLite `wherePathSatisfiesOrderBy` (currently UNDOCUMENTED).

---

## Excluded (failed a criterion)

- **Per-frame `walShmBarrier` → release-atomic hash-slot store** (Agent 1 "Fix B"): would change
  the WAL hash-slot **publish logic** and needs arm64 weak-memory cross-process validation →
  risks a subtle ordering **drift**. Defer until Task 1 lands and a weak-memory verification
  exists; not in this plan.
- **Batch/bulk per-row btree insert/balance + 2-`WriteAt`-per-frame** (~6µs/row, the residual
  +24–51% on `Batch*`/`Bulk*`): a real but **uninvestigated** axis; coalescing frame-header+page
  into one `WriteAt` would alter WAL write logic (potential drift) and needs its own SIGPROF-immune
  study. Out of scope here.
- **Sorter micro-tweak** (retain winning docs to skip re-fetch on emit): ~µs, lost in noise.
- **Comma-splitting `parseSortString`**: see above — would introduce drift.

## Suggested sequencing

1. **Task 1** (Gosched removal) — smallest, highest-confidence, drift-*reducing*, big single-op
   write win; gate hard on the crash/multiprocess suite.
2. **Task 2** (lazy/projected decode) — largest aggregate win (filters + unindexed sorts),
   no btree drift; the most code but contained to anyenc/qplanner.
3. **Benchmark sort-spec fix + NOTES drift entry** — trivial, unblocks accurate `CompoundRev`
   measurement.
4. **Task 3** (Count inlining) — only if Count stays a priority; medium confidence.
