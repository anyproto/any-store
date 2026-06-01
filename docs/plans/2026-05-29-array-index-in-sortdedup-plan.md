# Array-index multi-bound `$in` Count via pooled sort-dedup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: `superpowers:executing-plans`. TDD per step: failing test FIRST, confirm it fails for the right reason, implement minimal, confirm PASS, run audit suite, commit.

**Branch:** `feat/array-index-in-sortdedup` (off `btree` @ `eb667a0`, alpha.6). Matching `feat/array-index-in-sortdedup` in `/Users/roma/anytype/any-store-tests` (off `main`).

**Goal.** Make `Find({arrayField:{$in:[v1,...,vk]}}).Count(ctx)` over a multi-key (array) single-field index correct and low-alloc, using a **pooled sort-dedup** instead of the k-way heap merge. Fix three pre-existing wrong-answer Count bugs (I-04, I-05, I-06) in the same branch. No heap, no two-buffer page-aliasing, no cursor-ownership transfer, no per-doc map allocation.

## Why sort-dedup, not the k-way merge

The k-way merge (parked on `feat/array-index-multi-bound-in-merge`) delivers the alloc win but carries the densest cluster of correctness invariants on that branch. A benchmark (worktree `agent-aecdbf9b1a8e4d58c`) measured three strategies head-to-head on identical real-btree data:

| Strategy (LargeN ~75K distinct) | ns/op | B/op | allocs/op |
|---|---|---|---|
| seen-set (`map[string]struct{}`) | 6.0 ms | 4.1 MB | 75,265 |
| sort-dedup (fresh arena) | 3.4 ms | 10.3 MB | 70 |
| **sort-dedup (pooled arena)** | **2.9 ms** | **25 KB** | **8** |
| k-way heap merge | 2.8 ms | 2 KB | 21 |

Pooled sort-dedup **beats the merge on allocs/op** (8 vs 21; and 8 vs 89 at k=20 — the merge's allocs scale with k, sort-dedup's don't) and ties on wall-clock, with ~40 lines of straight-line code. The merge's only edge is steady-state **live footprint** (O(k) ≈ 2 KB vs sort-dedup's O(n) ≈ 10 MB held during the sort) — which matters only under many concurrent large-N `$in` counts. For bounded Count concurrency (UI counts, pagination) the pooled arena is the better complexity/alloc trade. The merge stays parked for the narrow high-concurrency-large-N case.

## The three bugs being fixed (all pre-existing on btree, all Count-path, Count wrong / Iter right)

- **I-04** — `And.IndexBounds` (`query/filter.go:220-227`) returns the first conjunct that grows the bounds and silently discards the rest; combined with `indexCoversFilter` (`planner.go`) checking only field membership, the CountOnly fast path bypasses FilterIter. `Find({a:{$in:[1,2]},$and:[{a:{$gte:5}}]}).Count()` → 2, should be 0.
- **I-05** — `countEntriesWithDedup` peek-then-batch shortcut double-counts when a bound's first entry is scalar but later entries are multi-key. Scalar field `x`, docs `{x:5}`, `{x:10}`, `{x:[5,10]}`, `Find({x:{$in:[5,10]}}).Count()` → 4, should be 3. **Fixed by construction**: sort-dedup has no peek-then-batch shortcut.
- **I-06** — `CoverIter.Next` (`cover_iter.go:40`) hardcodes `multiKey=false`; a unique single-field multi-key `$in` Count over-counts. Unique `x`, `{x:["a","b"]}`, `Find({x:{$in:["a","b"]}}).Count()` → 2, should be 1.

Full design rationale (probe correctness, BLOCKER analysis from 3 review rounds) lives in the parked spec on `feat/array-index-multi-bound-in-merge`: `docs/specs/2026-05-28-i04-i05-fix-option-d-canonical-key-probe.md`. This plan re-derives the fixes on the simpler sort-dedup dispatch.

## Dispatch (simpler than Option D — no merge, no min-N gate, no kMax)

`IndexIter.CountEntries` becomes a 4-branch tree:

1. `len(Bounds) <= 1` → `countEntriesBatch` (unchanged; page-batch CountUntil).
2. `!PointLookup || len(IdxInfo.FieldNames) != 1` (compound / range) → `countEntriesViaSortDedup` (safe per-doc dedup; the probe doesn't apply to compound — its array tag is mid-key, see I-04/BLOCKER-1 reasoning in the parked spec).
3. single-field PointLookup, probe says no multi-key → `countEntriesBatch` (no cross-bound docId overlap possible when no doc is array-valued).
4. single-field PointLookup, probe says multi-key → `countEntriesViaSortDedup`.

No `kWayMergeMin/Max`, no merge-eligibility cut. The probe (`indexProbeAnyMultiKey`, `Seek([0x06])` for the anyenc array type tag) is the snapshot-consistent multi-key detector; it gates only the single-field PointLookup fast-path-vs-dedup choice.

The probe is gated on `len(Bounds) > 1` at the CoverIter site too (I-06).

---

## Tasks

### Task 1 — Carry-over the independent pieces + bench scenarios

**Purpose.** Bring the three workload-independent commits from the parked branch (and the bench scenarios) onto the fresh branch so later tasks have measurement + the latent-leak fix in place.

- [ ] **Step 1: Cherry-pick the two independent any-store commits.**
```bash
cd /Users/roma/anytype/any-store
git cherry-pick 721171e   # query: Count's CountableIterator branch closes plan
git cherry-pick cc5d2dc   # btree(test): reset page buffer pool in integrity sweep setup
```
If `cc5d2dc` conflicts (it touches `internal/btree/integrity_sweep_test.go` which is unchanged on btree, so it should apply clean), resolve by taking the parked version.

- [ ] **Step 2: Verify they apply + build.**
```bash
go build ./...
go test -run '^TestAudit' . -count=1
```
Expected: PASS.

- [ ] **Step 3: Bench scenarios in any-store-tests.**
```bash
cd /Users/roma/anytype/any-store-tests
git checkout -b feat/array-index-in-sortdedup main
git cherry-pick ca42d66   # bench: add array_index/InLarge, InEmpty, InAllMatch scenarios
cd benchmark && go build -o /tmp/bench_run .
```
Expected: builds; `/tmp/bench_run -docs 100000 -check -group 'array_index'` → all PASS.

- [ ] **Step 4: Capture the btree baseline** (the "before" column).
```bash
/tmp/bench_run -duration 3s -docs 500000 -no-compress -group 'array_index|simple_index|unique_index' \
  2>&1 | grep '^Benchmark' | tee /tmp/sortdedup-baseline.txt
```
Record `array_index/In`, `InLarge`, `InEmpty`, `InAllMatch`, `simple_index/In`, `unique_index/In3`. (Expect alpha.6 numbers: array_index/In ~8 ms / 75584 allocs.)

No commit for Step 4 (measurement only).

---

### Task 2 — `Bounds.Intersect` (I-04 layer 1, helper)

**Files:** `query/bound.go`, `query/bound_test.go`.

- [ ] **Step 1: Failing tests first.** Append `Bounds.Intersect` unit tests to `query/bound_test.go`: point∩point (equal→point, disjoint→empty), point∩range (inside→point, outside→empty), range∩range (overlap→intersection, disjoint→empty), multi-bound cross-product, open-ended (-inf/+inf) handling, exclusivity at equal endpoints, empty input either side. Run → fail to compile (`Intersect` undefined).

- [ ] **Step 2: Implement** `Bounds.Intersect(other Bounds) Bounds` + helpers `intersectPair`, `maxStartKey`, `minEndKey` in `query/bound.go` (the inverses of the existing `minStartKey`/`maxEndKey`). Empty result when start>end or equal-with-an-exclusive-endpoint. SortAndMerge the result. (See the parked spec Component 1 for the exact code sketch.)

- [ ] **Step 3:** `go test ./query/ -count=1` → PASS.

- [ ] **Step 4: Commit.** `query: add Bounds.Intersect for I-04 conjunction support`.

---

### Task 3 — `And.IndexBounds` intersects conjuncts (I-04 layer 1)

**Files:** `query/filter.go:220-227`, `query/filter_test.go`.

- [ ] **Step 1: Failing tests.** `TestAndIndexBounds_TwoConjunctsSameField_Range` (`{$and:[{a:{$gte:5}},{a:{$lte:10}}]}` → `[{5,10}]`), `_InAndRange` (`{a:{$in:[1,2,5,10]},$and:[{a:{$gte:5}}]}` → `[{5},{10}]`), `_DisjointConjuncts` (`{a:{$in:[1,2]},$and:[{a:{$gte:5}}]}` → `[]`). Run → fail (current returns the first conjunct's bounds).

- [ ] **Step 2: Rewrite** `And.IndexBounds` to intersect each contributing conjunct's bounds into the running result; empty result short-circuits. (Parked spec Component 2.)

- [ ] **Step 3:** `go test ./query/ . -run '^TestAnd|^TestAudit' -count=1` and `go test ./query/ -count=1` → PASS. Update any existing And-bounds test that relied on the buggy first-conjunct behavior.

- [ ] **Step 4: Commit.** `query: And.IndexBounds intersects across conjuncts (I-04)`.

---

### Task 4 — I-04 reproducer + indexCoversFilter doc-comment

**Files:** `query_test.go`, `internal/qplanner/planner.go` (doc-comment only), `internal/qplanner/planner_test.go`.

- [ ] **Step 1: Add the I-04 end-to-end reproducer** `TestQueryCount_AndConjunctionLostInCount` in `query_test.go`: `{a:{$in:[1,2]},$and:[{a:{$gte:5}}]}.Count()` expect 0, the inline `{a:{$in:[1,2],$gte:5}}` form expect 0, and Iter expect 0. The fail-first gate for I-04 was already exercised by Task 3's `TestAndIndexBounds_DisjointConjuncts` unit test (fails on btree → passes after the And.IndexBounds fix); this end-to-end test is the regression pin at the Count API. It passes now because Task 3 fixed the root cause. To confirm it genuinely pins I-04, optionally `git stash` the Task 3 change and observe Count=2, then restore.

- [ ] **Step 2:** Confirm `indexCoversFilter` needs no code change (after Task 3, disjoint conjuncts yield empty `idx.Bounds`, hitting the existing early-return). Add a defensive regression test `TestIndexCoversFilter_RejectsUncoveredField` and a doc-comment recording the post-Task-3 invariant. (Parked spec Component 3.)

- [ ] **Step 3:** `go test -run '^TestQueryCount_AndConjunction|^TestIndexCoversFilter|^TestAudit' . ./internal/qplanner/ -count=1` → PASS.

- [ ] **Step 4: Commit.** `tests: pin I-04 fix (And conjunction in CountOnly fast path)`.

---

### Task 5 — `IndexIter.PointLookup` field + the canonical-key probe

**Files:** `internal/qplanner/index_iter.go`, `internal/qplanner/planner.go` (two IndexIter literals), `internal/qplanner/index_iter_test.go`.

- [ ] **Step 1: Add `PointLookup bool` to `IndexIter`** (doc: true iff every original bound was equality before AdjustBoundsForNonUnique). Wire `PointLookup: idx.PointLookup` into both IndexIter construction sites in planner.go (`buildIndexSeekChain` ~line 879, `buildIndexScanChain` ~line 981). (We do NOT add `Sketch` — sort-dedup needs no capacity hint.)

- [ ] **Step 2: Failing tests for the probe.** Add `indexProbeAnyMultiKey` unit tests (`internal/qplanner/index_iter_test.go`): pure-scalar (type-tag 0x02/0x03 entries) → false; array entries (canonical-key 0x06 prefix) → true; mixed → true; empty namespace → false; object-only (0x07) → false (cursor lands past 0x06, `k[0] != 0x06`); legacy nil-value entries → works (probe reads key prefix, not value byte). Use the existing `indexEntryBtree` helper; add a `rawKeyBtree` helper for hand-shaped 0x06/0x07 keys (build keys via `anyenc.AppendAnyValue` where possible to avoid hand-coded byte drift). Run → fail to compile.

- [ ] **Step 3: Implement** `arrayPrefix = []byte{byte(anyenc.TypeArray)}` and `func indexProbeAnyMultiKey(cs *CursorSource) (bool, error)` — single `Seek(arrayPrefix)`, return `c.Valid() && k[0] == byte(anyenc.TypeArray)`. (Parked spec Component 4.)

- [ ] **Step 4:** `go test -run '^TestIndexProbeAnyMultiKey' ./internal/qplanner/ -count=1 -v` → PASS. `go test ./internal/qplanner/ -count=1` → PASS (no dispatch change yet).

- [ ] **Step 5: Commit.** `qplanner: add IndexIter.PointLookup + indexProbeAnyMultiKey (probe)`.

---

### Task 6 — Pooled sort-dedup count path

**Files:** `internal/qplanner/index_iter.go` (or a new `internal/qplanner/sort_dedup.go`), `internal/qplanner/index_iter_test.go`.

- [ ] **Step 1: Failing tests.** `TestCountEntriesViaSortDedup_Disjoint`, `_Overlapping`, `_HeavyOverlap200`, `_LegacyNilValue`, `_AllocsBudget` (assert `testing.AllocsPerRun` ≤ ~30 with a warmed pool). Build the fixtures with `indexEntryBtree` (multi-key entries) and `boundForValue`. Run → fail to compile.

- [ ] **Step 2: Implement.** A package-level `sync.Pool` of `*sortDedupArena{ buf []byte; spans []sortDedupSpan }` (mirroring the `syncpool.DocBuffer` pattern). `countEntriesViaSortDedup()`:
  - `arena := sortDedupPool.Get().(*sortDedupArena); arena.reset(); defer sortDedupPool.Put(arena)`.
  - Walk each bound via `seekBoundStart`; for each in-bound entry, copy `extractDocId(k, fieldCount)` into `arena.buf`, append a `span{off,len}` (copy is required — docId aliases the cursor page).
  - `sort.Slice(arena.spans, ...)` by `bytes.Compare` of the arena-backed docId bytes.
  - Count distinct by adjacent comparison.
  Lift the prototype from worktree `agent-aecdbf9b1a8e4d58c` (`countEntriesViaSortDedupPooled`); generalize the pool ownership (Get/Put inside the method, not caller-owned, so call sites stay simple).

- [ ] **Step 3:** `go test -run '^TestCountEntriesViaSortDedup' ./internal/qplanner/ -count=1 -v` → PASS.

- [ ] **Step 4: Commit.** `qplanner: pooled sort-dedup distinct-docId count`.

---

### Task 7 — Rewire `CountEntries` dispatch + delete `countEntriesWithDedup` (fixes I-05)

**Files:** `internal/qplanner/index_iter.go`, `internal/qplanner/index_iter_test.go`, `query_test.go`.

- [ ] **Step 1: Add the I-05 reproducer FIRST** in `query_test.go` (`TestQueryCount_ScalarFirstCrossBoundDedup`): scalar `x` index, docs `{x:5}`,`{x:10}`,`{x:[5,10]}`, `Find({x:{$in:[5,10]}}).Count()` expect 3 (+ Iter expect 3). Run on the current branch → it FAILS (Count=4) because the btree `countEntriesWithDedup` peek-then-batch bug is still present. This is the fail-before-fix gate for I-05.

- [ ] **Step 2: Rewrite `CountEntries`** to the 4-branch dispatch (above). Add `probeMultiKey()` cached helper. Branch 2 (compound/non-PointLookup) and Branch 4 (multi-key) → `countEntriesViaSortDedup`; Branch 3 (scalar) → `countEntriesBatch`.

- [ ] **Step 3: Delete `countEntriesWithDedup`** (the peek-then-batch + seen-set) — no caller remains. `grep -rn countEntriesWithDedup internal/qplanner/` to confirm. This removes the I-05 bug surface entirely.

- [ ] **Step 4: Dispatch tests** (`internal/qplanner/index_iter_test.go`): pure-scalar multi-bound → batch path (verify count correct); mixed multi-key → sort-dedup (count correct); compound → sort-dedup (not probe). Add a perf-counter or code-path marker if needed to assert the route (reuse the existing perf-counter infra if present on btree; if not, assert via counts + a lightweight instrumentation).

- [ ] **Step 5:** I-05 reproducer now PASSES. Run `go test -run '^TestQueryCount_ScalarFirstCrossBoundDedup|^TestAudit|^TestQuery' . ./internal/qplanner/ -count=1` → PASS.

- [ ] **Step 6: Commit.** `qplanner: 4-branch CountEntries via probe + sort-dedup (fixes I-05)`.

---

### Task 8 — I-06: CoverIter multi-key detection

**Files:** `internal/qplanner/cover_iter.go`, `internal/qplanner/cover_iter_test.go`, `query_test.go`.

- [ ] **Step 1: Add the I-06 reproducer FIRST** in `query_test.go` (`TestQueryCount_UniqueIndex_MultiKeyData_DedupsCorrectly`): unique `x` index, `{x:["a","b"]}`, `Find({x:{$in:["a","b"]}}).Count()` expect 1 (+ Iter expect 1). Run → FAILS (Count=2) because CoverIter hardcodes `multiKey=false`. Fail-before-fix gate for I-06.

- [ ] **Step 2: Add probe-gated multi-key detection to `CoverIter`.** Fields `multiKeyProbed bool`, `hasMultiKey bool`. In `Next`, when `len(it.Bounds) > 1 && !multiKeyProbed`, run `indexProbeAnyMultiKey(it.Source)` once. Yield `it.hasMultiKey` instead of hardcoded `false`. Single-bound (`len(Bounds) == 1`) keeps the zero-cost `false`. Index-level `hasMultiKey` is correct because `DocDedup.Accept(docId, true)` keys on docId (only collapses same-doc repeats). (Parked spec Component 8.)

- [ ] **Step 3: Unit test** `TestCoverIter_MultiBound_MultiKey_SetsMultiKeyFlag` — multi-bound + array data yields true; single-bound and pure-scalar yield false.

- [ ] **Step 4:** I-06 reproducer PASSES. `go test -run '^TestQueryCount_UniqueIndex|^TestCoverIter|^TestAudit' . ./internal/qplanner/ -count=1` → PASS.

- [ ] **Step 5: Commit.** `qplanner: CoverIter detects multi-key entries (fixes I-06)`.

---

### Task 9 — Compound BLOCKER-1 guard + final validation + bench

**Files:** `query_test.go`, `docs/known-issues.md`.

- [ ] **Step 1: Compound guard test** `TestQueryCount_Compound_ArrayNotFirst_NoDoubleCount` (compound `(priority, tags)`, priority scalar, tags array; `{priority:5, tags:{$in:["a","b"]}}.Count()` expect 1). Forward-looking guard for the Branch-2 gate (routes compound to sort-dedup, never the probe). Honest docstring: passes because Branch 2 routes compound to sort-dedup.

- [ ] **Step 2: Full suites.**
```bash
go test -run '^TestAudit' . -count=3
go test ./internal/qplanner/ ./query/ -count=1
go test ./... -count=1   # modulo the pre-existing btree -p1 flake fixed by cc5d2dc
```

- [ ] **Step 3: 6× bench** vs `/tmp/sortdedup-baseline.txt`. Build the result file; benchstat. Targets:
  - `array_index/In` (k=3): allocs/op drops ~75584 → ~tens; wall-clock ~8 ms → ~3 ms; pooled arena B/op low.
  - `simple_index/In`, `unique_index/In3`: within ±5% (scalar → batch; probe adds one Seek).
  - `array_index/InEmpty`: within ±5%.

- [ ] **Step 4: Update `docs/known-issues.md`** — mark I-04, I-05, I-06 FIXED with pointers to this branch.

- [ ] **Step 5: Commit** (any-store) + result-file commit (any-store-tests).

---

## Hard-stop checkpoints

1. After Task 4 — I-04 reproducer green; report.
2. After Task 7 — I-05 reproducer green; report dispatch + a quick array_index/In bench (the headline alloc number).
3. After Task 9 — final benchstat + all three reproducers green.

## Non-goals

- No k-way merge (parked on the sibling branch).
- No `kWayMergeMax`/`kWayMergeMinEntries`, no merge-eligibility gate, no `mergeIterAdapter`.
- No `IndexSketch.HasMultiKey` (Task 8 of the merge plan — ruled out, I-03 unsafe).
- No Iter/Update/Delete merge wiring (those paths already dedup correctly via `CanonicalKeyDedupIter`/`DocDedup`).
- No projected-decode work (sibling `btree-projected-decode` branch; complementary, separate).

## Definition of done

- 3 reproducers (`TestQueryCount_AndConjunctionLostInCount`, `_ScalarFirstCrossBoundDedup`, `_UniqueIndex_MultiKeyData_DedupsCorrectly`) pass; each demonstrably failed on btree before its fix.
- `countEntriesWithDedup` deleted; no heap, no merge code on this branch.
- `go test ./... -count=1` passes in both repos.
- `array_index/In` allocs/op down ~3 orders of magnitude vs btree baseline; `simple_index/In` + `unique_index/In3` within ±5%.
- `docs/known-issues.md` I-04/I-05/I-06 marked FIXED.
