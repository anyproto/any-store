# Known Issues

Latent bugs discovered during code review but not yet fixed. Each entry includes a brief description, the affected code, the failure mode, the impact level (correctness vs performance vs predictability), and a fix sketch.

Entries should be moved to a per-issue plan in `docs/plans/` when work starts on them.

---

## I-01: `IndexSketch` is not rolled back on `tx.Rollback()`

**Discovered:** 2026-05-28, during the array-index multi-bound `$in` merge plan review.

**Affected code:**
- `/Users/roma/anytype/any-store/index.go:70` — the `sketchModified bool` field.
- `/Users/roma/anytype/any-store/index.go:179, 184, 204, 209` — call sites for `sketch.Increment`, `sketch.Decrement`, `sketch.IncrementDocCount`, `sketch.DecrementDocCount` (all inline in `insertKeys` / `deleteKeys`, executed *before* the write tx commits).
- `/Users/roma/anytype/any-store/collection.go:604, 796, 802` — `sketchModified` only controls *whether to persist on commit*; it does **not** revert the in-memory sketch state.

**Failure mode:**
Write tx calls `insertKeys` or `deleteKeys`, which atomically updates the process-shared `*IndexSketch` in memory **before** commit. If the tx is then rolled back (busy-snapshot retry, validation error, explicit rollback), the in-memory sketch keeps the increment/decrement and now permanently disagrees with the actual on-disk btree state. The disagreement accumulates over the lifetime of the process; the next `Open` reloads the sketch from disk and corrects it.

**Impact: PERFORMANCE / PREDICTABILITY only — not correctness.**
Every existing sketch read in `internal/qplanner/planner.go` (lines 305, 643, 688, 713) is a *cost-estimation hint* feeding the planner's plan choice. A drifted sketch causes the planner to pick suboptimal plans (FullScan when IndexSeek would be better, or vice versa). The plan still runs against the snapshot's MVCC-consistent view, so query answers remain correct.

This becomes a correctness issue *only* if any new feature uses sketch reads to **determine an answer** instead of to **decide a plan**. See I-03 for the rule.

**Symptom in production:** slow-query reports that don't reproduce after restart, because the in-memory sketch is reloaded clean from disk at every `Open`.

**Fix sketch:**
- Track per-tx sketch deltas (`Increment`/`Decrement` ops batched into a slice owned by the `WriteTx`).
- On `Commit`, apply the deltas to the live sketch and persist.
- On `Rollback`, discard the deltas. The live sketch never sees them.

Cost: one slice allocation per write tx that mutates an indexed field; per-op O(1) append. Negligible compared to the btree work.

---

## I-02: Cross-process sketch staleness causes plan instability

**Discovered:** 2026-05-28, same review as I-01.

**Affected code:**
- `/Users/roma/anytype/any-store/internal/qplanner/sketch.go:11-60` — `IndexSketch` is process-local in-memory state.
- `/Users/roma/anytype/any-store/collection.go` — sketches are loaded from disk at `Open` and persisted on commit (via `sketchModified`), but **never refreshed** during a process's lifetime to pick up commits from peer processes.

**Failure mode:**
Process A commits a write at T=1 that increments the sketch and persists it. Process B is already open at T=0 with a sketch snapshot from disk at that time; B's in-memory sketch never picks up A's commit. B's planner uses the stale sketch for cost estimation.

Same query in A and B can pick different plans at the same logical instant. Latency variance across processes — for example, in the Anytype client-server model where a desktop and a sync server both have the DB open, identical queries can hit IndexSeek on one and FullScan on the other.

**Impact: PERFORMANCE / PREDICTABILITY only — not correctness.**
Same reason as I-01: all current sketch reads are cost-estimation hints. Plan differs, latency differs, **answer is the same** because each process's plan still runs against its own MVCC-consistent snapshot.

This becomes a correctness issue *only* if any new feature uses sketch reads to determine an answer. See I-03.

**Symptom in production:** "Why is this query slow on the sync server but fast on the desktop?" support tickets with the same DB and the same query at the same time.

**Fix sketch (graduated cost):**
1. **Cheap:** check the sketch file's mtime/size at the start of each transaction; reload from disk if changed. ~1 stat call per tx.
2. **Better:** version-bump the sketch file on commit; readers keep a version counter and reload when the on-disk version exceeds their cached version.
3. **Most correct:** make sketches snapshot-isolated by storing them in the btree under a versioned key (just like the freelist) and consulting the snapshot's version. Heavyweight.

(1) closes most of the variance window with negligible cost; (3) is the proper fix.

---

## I-03 (rule, not a bug): sketch reads must remain cost-estimation hints, never answer-determining

**Established:** 2026-05-28, during the array-index `$in` merge plan review.

**The rule.** Any code reading `IndexSketch.Estimate` or `IndexSketch.GetDocCount` may use the result to **decide a plan** (which index to pick, what cost to assign, what initial allocation size to request). It may **not** use the result to **decide a query answer** (e.g. "if Σ Estimate == 0, return 0 to the caller without scanning"). 

**Why.** I-01 and I-02 together mean the in-memory sketch can be inconsistent with the snapshot in either direction:
- Mid-write (pre-commit `Decrement` visible to readers on the same process) — sketch can be *below* the true count for an active reader's snapshot.
- Cross-process (peer process committed a write the reader hasn't refreshed) — sketch can be *below* the true count for the reader's snapshot.
- Failed-rollback drift (I-01) — sketch can be persistently off in either direction.

A plan-time read tolerates this — wrong cost → wrong plan choice → slower query, still-correct answer because execution runs against the snapshot.

An answer-time read does not tolerate this — wrong estimate of zero → false short-circuit → wrong answer.

**Current compliance.** All four existing sketch reads (`planner.go:305, 643, 688, 713`) feed cost estimation. ✓.

**Trip-wire.** During the array-index `$in` merge review, an "S1" sketch-zero short-circuit was proposed in the first draft of `docs/plans/2026-05-28-array-index-multi-bound-in-merge.md`. It was dropped before implementation. The "S2" pre-sized seen-set in the same plan reads the sketch only to size a map (a plan-time concern); it stays within the rule.

If a future feature is tempted to use the sketch to determine an answer, fix I-01 and I-02 first (so the sketch is snapshot-isolated and rollback-safe), then proceed. Don't paper over the race with retries or freshness checks — the resulting code is hard to reason about and only mostly safe.

---

## I-04: `And.IndexBounds` silently discards conjuncts; CountOnly fast path returns wrong answer

**Status: FIXED** on `feat/array-index-in-sortdedup` (2026-05-29). `And.IndexBounds`
(`query/filter.go`) now intersects the bounds of every contributing same-field
conjunct via a new `Bounds.Intersect` helper (`query/bound.go`); an empty
intersection short-circuits, so `indexCoversFilter`'s `len(idx.Bounds)==0`
early-return rejects the CountOnly fast path and FilterIter is applied.
Reproducer: `TestQueryCount_AndConjunctionLostInCount` in `query_test.go`
(verified Count=2 before the fix); unit gate
`query/filter_test.go:TestAndIndexBounds_DisjointConjuncts`.

**Discovered:** 2026-05-28, during the array-index multi-bound `$in` merge code review (4-agent review of `feat/array-index-multi-bound-in-merge`).

**Affected code:**
- `/Users/roma/anytype/any-store/query/filter.go:220-227` — `And.IndexBounds` iterates conjuncts and returns the first one whose `IndexBounds` call grew the `bs` slice. The rest are silently discarded.
- `/Users/roma/anytype/any-store/internal/qplanner/planner.go:1234` — `indexCoversFilter` checks only that every filter *field* is in the index, NOT that the bounds capture every filter *predicate*.
- `/Users/roma/anytype/any-store/internal/qplanner/planner.go:945` — CountOnly fast path: when `PointLookup && indexCoversFilter`, returns the raw `IndexIter`, bypassing `FilterIter`. Misapplied here because the bounds don't represent the conjunction.

**Failure mode:**

Query:
```js
Find({a:{$in:[1,2]}, $and:[{a:{$gte:5}}]}).Count(ctx)
```

True semantics: `a ∈ {1,2} AND a >= 5` = ∅, so Count = 0.

Computed:
1. `And.IndexBounds("a", [])` walks children:
   - Child 0 (`In{1,2}`): returns `[{1},{2}]`. `len([{1},{2}]) != len([])` → returns immediately. **Child 1 (`Comp{$gte,5}`) never visited.**
2. `AllBoundsFixed([{1},{2}])` → true. `CBOIndex.PointLookup = true`.
3. `indexCoversFilter` returns true: filter only references field `a`, which is in the index. (The function does not verify bound completeness — only field membership.)
4. The fast path at `planner.go:945` returns the raw `IndexIter` (no `FilterIter` wrap).
5. `CountEntries` counts entries for `a ∈ {1, 2}` → returns 2. The `$gte:5` is never applied.

**Equivalent same-field form (also broken):**
```js
Find({a:{$in:[1,2], $gte:5}}).Count(ctx)  // returns 2, true answer is 0
```

**Impact: CORRECTNESS (wrong answer).**

- Count returns wrong values for any conjunction of `$in` (or any other equality-set operator) with a range operator on the same field.
- Iter is **correct** (the non-CountOnly path always wraps with `FilterIter` regardless of bound coverage). This causes `coll.Find(q).Count()` and `len(iter results)` to disagree.
- The merge feature inherits this bug — `countEntriesViaMerge` happily counts the wrong bounds. The branch does NOT introduce the bug; it predates the merge.
- The error magnitude can be arbitrary: returns `count(a ∈ {1,2})` when truth is `count(a ∈ {1,2} ∧ a ≥ 5)`.

**Reproducer:** `TestQuery_KnownIssueI04_AndConjunctionLostInCount` in `query_test.go` (marked `t.Skip`).

**Fix sketch (graduated):**

1. **Cheapest, narrow:** in `And.IndexBounds`, after a child returns bounds that grew, continue walking subsequent children that also return larger bounds AND intersect them with the running bounds. Requires a `Bounds.Intersect` helper (the inverse of `SortAndMerge`).
2. **Tighter `indexCoversFilter`:** make the function verify that every filter predicate has been absorbed into the bounds, not just that the field is in the index. Probably easier to do in conjunction with (1).
3. **Conservative:** drop the CountOnly fast path at `planner.go:945` entirely; always wrap `FilterIter` when the filter has any conjunctive predicate not provably absorbed by `And.IndexBounds`. Loses the fast path for cases where it IS sound (single-predicate $in), but removes the wrong-answer surface entirely until (1)/(2) land.

This issue is OUT OF SCOPE for `docs/plans/2026-05-28-array-index-multi-bound-in-merge.md`. It must be addressed in a dedicated follow-up plan.

---

## I-05: `countEntriesWithDedup` peek-then-batch double-counts cross-bound docs when bound-firsts are scalar

**Status: FIXED** on `feat/array-index-in-sortdedup` (2026-05-29). `countEntriesWithDedup`
(the peek-then-batch + `stickyMulti` shortcut) is **deleted**. `IndexIter.CountEntries`
is now a 4-branch dispatch: single-bound and probe-says-pure-scalar use the
page-batch fast path; compound/non-PointLookup and probe-says-multi-key use a
pooled sort-dedup that counts distinct docIds with no peek-then-batch shortcut to
misclassify. Reproducer: `TestQueryCount_ScalarFirstCrossBoundDedup` in
`query_test.go` (verified Count=4 before the fix).

**Trade-off (accepted 2026-05-29):** the canonical-key probe cannot detect a
compound index's multi-key entries (the array type tag is mid-key, not at byte 0),
so compound / non-PointLookup multi-bound counts always route to sort-dedup. For a
high-selectivity *scalar* compound index this is materially slower than the old
value-byte peek-batch: `simple_index/In` (`{a:{$in:[...]}}`), which the planner
routes to a compound `(a,b)` index on a cost tie, regresses ~24 µs → ~3.5 ms
(~144×) at 500k docs. This is a performance-only regression — the answer is
correct. The single-field array path it was built for is *faster* than baseline:
`array_index/In` 8.56 ms → 7.75 ms (0.91×) with allocs 75 584 → 63 (~1200×
fewer); `unique_index/In3` stays within ±5%; `array_index/InEmpty` is +9%
(~0.5 µs, the one extra probe Seek). Two further characteristics: the canonical-
key probe adds one Seek to every single-field multi-bound count (visible only on
near-empty results like InEmpty); and `sortDedupPool` retains each arena at the
high-water mark of the largest count it served (the pooling is what buys the
low-alloc win — the parked k-way merge is the O(k)-memory alternative if bounded
footprint under concurrent large counts ever matters). See the bench table in
`docs/plans/2026-05-29-array-index-in-sortdedup-plan.md`.

**Discovered:** 2026-05-28, during the 4-agent review of `feat/array-index-multi-bound-in-merge`. Pre-existing on the `btree` baseline — not introduced by the merge feature.

**Affected code:**
- `/Users/roma/anytype/any-store/internal/qplanner/index_iter.go:656` — `countEntriesWithDedupUsingSeen`. The peek-then-batch shortcut at lines 673–687 reads the first entry's value byte and, if scalar, calls `cursor.CountUntil(b.End, ...)` to count ALL entries in the bound (including any multi-key entries that share docIds with other bounds). `stickyMulti` never latches because the peek saw scalar.

**Trigger query:**
```js
// Single-field index on scalar field "x"
Find({x:{$in:[5,10]}}).Count(ctx)
```

with docs:
- `d1: x=5` (scalar — 1 entry, value byte = SCALAR)
- `d2: x=10` (scalar — 1 entry, value byte = SCALAR)
- `d3: x=[5,10]` (array — 2 entries on bound's values + 1 canonical-key, all MULTI-KEY)

Bound "5": first entry `(5, d1)` SCALAR. Second entry `(5, d3)` MULTI-KEY.
Bound "10": first entry `(10, d2)` SCALAR. Second entry `(10, d3)` MULTI-KEY.

**True answer:** distinct docs matching `x ∈ {5, 10}` = `{d1, d2, d3}` = 3.

**Computed:**
- Bound 5: peek `(5, d1)` SCALAR → `stickyMulti=false` → `CountUntil("5", incl=true)` returns 2 (covers `(5, d1)` AND `(5, d3)`). `total = 2`. Continue.
- Bound 10: peek `(10, d2)` SCALAR → `stickyMulti=false` → `CountUntil("10", incl=true)` returns 2. `total = 4`. Continue.

Returned: 4. **Wrong answer** (d3 double-counted, once on bound 5 and once on bound 10).

**Impact: CORRECTNESS (wrong answer).**

- Count overcounts by N where N = number of docs that match multiple bounds via multi-key array values AND whose bound-firsts are scalar (i.e., a scalar-only doc happens to have a smaller docId than any multi-key match for the same value).
- Iter is CORRECT for the same query (the chain `IndexIter → CanonicalKeyDedupIter → FetchIter` dedups via canonical-key). So `Find(q).Count()` and `len(Iter results)` disagree on this data shape — a discoverable artifact.
- This branch (`feat/array-index-multi-bound-in-merge`) does NOT introduce the bug; it predates the merge. The merge branch makes the divergence newly observable on the merge route (which dedups correctly), but the Count code path that hosts the bug is the alpha.6 `countEntriesWithDedup`.

**Why this is reachable only on specific data shapes:**

For an array-only field (e.g., a `tags` field that is always an array), every doc has ≥2 entries in the index (one per array element + one canonical-key entry). So `len(idx.keysBuf) > 1` is always true at insert and every entry's value byte is `IndexValueMultiKey`. `boundsAllScalar` peek returns false, `stickyMulti` latches on the very first peek, and per-entry dedup correctly engages.

For a scalar field with NO arrays, every doc has exactly 1 entry in the index. SCALAR is the only value byte; the peek-then-batch path is exact (each bound's CountUntil counts exactly one doc per value; cross-bound overlap is impossible because no doc has multiple values).

The bug appears only when the SAME index field is sometimes scalar (1 entry, SCALAR byte) and sometimes an array (≥2 entries, MULTI-KEY byte) — a "mixed-array workload" (e.g., legacy single-value docs + new array-value docs).

**Why the merge feature doesn't fix it:**

The dispatch routes to `countEntriesWithDedup` (not the merge) when the sketch sum is below `kWayMergeMinEntries` — true at small N. At larger N where the merge engages, the bug doesn't trigger because the merge correctly dedups by docId. So a mixed-array workload at small N is the window of exposure. At larger N the bug becomes correct again — a confusing data-size-dependent artifact.

**Reproducer:** `TestQuery_KnownIssueI05_ScalarFirstCrossBoundDedup` in `query_test.go` (marked `t.Skip` pending the fix plan). Sets up the trigger conditions, confirms Count=4 and Iter=3.

**Fix sketch (graduated):**

1. **Conservative:** in `countEntriesWithDedupUsingSeen`, when the peek is scalar AND `len(it.Bounds) > 1`, do not use `CountUntil`; instead per-entry walk the bound and lazily allocate the seen-set on the first multi-key entry. Eliminates the bug entirely at the cost of the peek-then-batch fast path for the scalar-only case. Quantify impact on SimpleIndex/In before landing.
2. **Sketch-flag-gated:** when an `IndexSketch.HasMultiKey` flag (Task 8, deferred) is true, skip the peek-then-batch shortcut entirely and always per-entry walk multi-bound queries. The peek-then-batch path is preserved when the flag is false (index proven scalar-only).
3. **Aggressive:** remove the peek-then-batch shortcut from `countEntriesWithDedup` entirely; rely on the merge for fast multi-bound counts. Bigger SimpleIndex/In regression but eliminates the bug surface.

This issue is OUT OF SCOPE for `docs/plans/2026-05-28-array-index-multi-bound-in-merge.md` (it predates the merge feature). It must be addressed in a dedicated follow-up plan, likely co-scheduled with Task 8 (the `HasMultiKey` flag) — option 2 above is the lowest-cost fix that doesn't reintroduce a regression on pure-scalar indexes.

---

## I-06: `CoverIter` hardcodes multiKey=false; unique-index multi-key `$in` Count over-counts

**Status: FIXED** on `feat/array-index-in-sortdedup` (2026-05-29). `CoverIter.Next`
(`internal/qplanner/cover_iter.go`) runs the canonical-key probe once, gated on
`len(Bounds) > 1`, and yields the index-level multi-key flag instead of a hardcoded
`false`. `DocDedup` then collapses the cross-bound repeats of an array doc; single-
bound point lookups skip the probe and keep the zero-cost `false`. Reproducer:
`TestQueryCount_UniqueIndex_MultiKeyData_DedupsCorrectly` in `query_test.go`
(verified Count=2 before the fix); unit test
`internal/qplanner/cover_iter_test.go:TestCoverIter_MultiBound_MultiKey_SetsMultiKeyFlag`.

**Discovered:** 2026-05-28, during round-3 review of the I-04/I-05 fix design (`docs/specs/2026-05-28-i04-i05-fix-option-d-canonical-key-probe.md`).

**Affected code:**
- `/Users/roma/anytype/any-store/internal/qplanner/cover_iter.go:40` — `CoverIter.Next` returns `multiKey=false` unconditionally, with the comment "Unique-index point lookup: at most one entry per doc — never a duplicate."
- `/Users/roma/anytype/any-store/internal/qplanner/planner.go:837` — the planner routes `idx.Info.Unique && idx.PointLookup && idx.BoundFields == len(idx.Info.FieldNames)` to a `CoverIter`-rooted chain, BEFORE the `IndexIter` construction. This intercepts unique single-field PointLookup queries (including multi-bound `$in`).
- `/Users/roma/anytype/any-store/query.go:485` — the Count loop uses `DocDedup.Accept(docId, multiKey)`; with `multiKey=false`, it never dedups.

**Failure mode:**

A unique index permits a doc with an array value as long as each element is unique across docs. `{x:["a","b"]}` on a unique `x` index writes 3 entries (`(a,d1)`, `(b,d1)`, `([a,b],d1)`), all tagged `IndexValueMultiKey` (per `index.go:155-158`, `len(idx.keysBuf) > 1`). Confirmed by `index_unique_sparse_test.go:370-403`.

Query `Find({x:{$in:["a","b"]}}).Count(ctx)`:
- The planner picks the CoverIter shortcut (unique + PointLookup + full coverage).
- CoverIter yields `(a,d1)` and `(b,d1)`, both with `multiKey=false` (hardcoded).
- The Count loop's `DocDedup.Accept(d1, false)` returns true both times → Count=2.

True answer: 1 distinct doc. **Over-counts by the number of bounds the doc's array values straddle.**

**Impact: CORRECTNESS (wrong answer).** Same class as I-04 and I-05 — Count wrong, Iter correct. Confirmed empirically: Count=2, Iter=1 for the trigger above.

**Why Iter is correct but Count is wrong:** the asymmetry is in how the two consumers handle the `multiKey=false` signal from CoverIter. Iter yields 1 (verified empirically); Count returns 2. The exact Iter dedup mechanism that compensates was not fully traced — worth confirming when this issue is worked.

**Reachability vs I-05:** I-05 is the `IndexIter.CountEntries` peek-then-batch path (non-unique or non-covering indexes). I-06 is the `CoverIter` path (unique single-field PointLookup full-coverage). They are disjoint code paths; a fix for one does not touch the other. The Option D fix for I-04/I-05 (`docs/specs/2026-05-28-i04-i05-fix-option-d-canonical-key-probe.md`) does NOT fix I-06.

**Fix sketch (graduated):**

1. **CoverIter reads the value byte.** After the seek confirms the key prefix (`cover_iter.go:35`), do an `AppendValue` (or switch to a cursor) to read the entry's value byte, and return `EntryValueIsMultiKey(val)` instead of hardcoded `false`. `DocDedup` then dedups correctly for both Count and Iter. Mirrors what `IndexIter.extractResult` already does. Cost: one extra single-shot value lookup per yielded entry on the CoverIter path. Minimal, correct for all consumers.
2. **Planner routes unique multi-bound away from CoverIter.** When the index is unique BUT may contain multi-key entries (probe / future `HasMultiKey` flag), skip the CoverIter shortcut and use the `IndexIter` chain (which reads the value byte). Keeps CoverIter as the fast path for scalar-only unique indexes. Bigger planner change.
3. **CoverIter detects multi-key once.** Run the canonical-key probe (the same `Seek([0x06])` Option D uses for I-05) when constructing the CoverIter; if the index has any multi-key entry, set a flag and read value bytes per entry. Pure-scalar unique indexes keep the zero-cost hardcoded-false.

Option 1 is the smallest correct fix. Tracked as a follow-up; may be folded into the Option D plan if scope is expanded.

---

## I-07: `Count()` with `Limit`/`Offset` over a multi-key index disagrees with `Iter()`

**Discovered:** 2026-05-29, adversarial review of `feat/array-index-in-sortdedup`. **Pre-existing on the `btree` baseline (alpha.6)** — confirmed by detaching to `eb667a0` and reproducing identical numbers. NOT introduced by the sort-dedup branch.

**Affected code:** `query.go` Count path. When the query carries a `Limit`/`Offset`, the plan root is a `LimitIter`, which is not a `CountableIterator`, so Count falls to the generic dedup loop. Over a multi-key index two dedup layers (`CanonicalKeyDedupIter` upstream + the consumer `DocDedup`) interact with the limit cutoff and produce a count that is neither `min(limit, distinct)` nor the true distinct count.

**Trigger:** index `{x}`, docs `d0..d9` each `{x:[i,i+1]}`; `Find({x:{$in:[0..10]}}).Limit(3).Count()` → **2**, but `.Limit(3).Iter()` yields **3**. `.Offset(4).Count()` → **8** vs `Iter` **6**. Scalar indexes are unaffected (Count respects the limit). 

**Impact: CORRECTNESS (Count ≠ len(Iter)).** Most likely to surface as wrong paginated counts on array fields. Out of scope for the sort-dedup branch (predates it; lives in the Count/Limit wiring, not the dedup count path).

**Fix sketch:** route limited/offset counts through a count-aware limit (count distinct, then clamp), or make the limited multi-key Count path reuse the same dedup as Iter.

---

## I-08: `$in` containing an empty array (`$in:[[]]`) over-counts vs the filter

**Discovered:** 2026-05-29, adversarial review of `feat/array-index-in-sortdedup`. **Pre-existing on the `btree` baseline** — confirmed identical at `eb667a0`. NOT introduced by the sort-dedup branch.

**Affected code:** `query/filter.go` `In.IndexBounds` vs `In.Ok`. `writeValues` indexes an empty-array field value under its canonical key (`0x06 0x00`); `In.IndexBounds` builds a point bound for the `[]` member that matches that entry, so the index/Count path counts the doc — but `In.Ok` iterates the (empty) array and matches nothing, so the FullScan/Iter path correctly excludes it.

**Trigger:** index `{x}`, docs `{x:[]}`,`{x:1}`; `Find({x:{$in:[[],1]}}).Count()` → **2**, `Iter()` → **1**.

**Impact: CORRECTNESS (Count ≠ len(Iter)), extreme corner** (an `$in` list literally containing an empty array). Fix would live in `In.IndexBounds` / unsatisfiability handling, not the count dispatch.
