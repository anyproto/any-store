# Known Issues

Latent bugs discovered during code review but not yet fixed. Each entry includes a brief description, the affected code, the failure mode, the impact level (correctness vs performance vs predictability), and a fix sketch.

Entries should be moved to a per-issue plan in `any-store-tests:docs/any-store/plans/` when work starts on them.

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

**Status: FIXED** (verified 2026-07-11; the fix landed earlier and this entry was never closed). `db.resetUncommittedSketches` runs at every write-tx begin: a still-set `sketchModified` means a prior tx mutated the live sketch and rolled back, so the live sketch is rebased to the last committed on-disk bytes before the new tx applies its own deltas — phantom deltas are discarded instead of accumulating and are never persisted. Regression test: `TestSketchNoAccumulatingDriftAcrossRolledBackTxs` (sketch_isolation_repro_test.go). Residual (accepted): the drift of the single most recent rolled-back tx survives until the NEXT write tx begins — bounded, advisory-only, and read at most by plan choice in that window. Note the interplay recorded in I-13: any future sketch-persistence throttle must not confuse this rollback-detection flag with a committed-but-unpersisted state.

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

**Status: FIXED** on `feat/array-index-in-sortdedup` (2026-05-29). Two
collaborating parts (and see the 2026-07-10 follow-up note at the end of this
entry: the perf cost of the over-approximation — dropped range Ends — is now
recovered by a separate TIGHT bounds channel without touching this contract):

1. `And.IndexBounds` (`query/filter.go`) returns a SOUND OVER-APPROXIMATION — the
   first contributing same-field conjunct's bounds, a superset of the matches. It
   does NOT intersect conjunct bounds.
2. The CountOnly fast path (which skips `FilterIter`) is gated in
   `indexCoversFilter` (`internal/qplanner/planner.go`): a covered field carrying
   more than one predicate (same-field `$and`, inline `{$in,$gte}`, or a
   two-sided range) is rejected, so the fast path runs only when the bounds equal
   the matches exactly. Every other case falls through to `FilterIter`, which
   re-checks the full conjunction.

Reproducers in `query_test.go`: `TestQueryCount_AndConjunctionLostInCount` (the
original disjoint-conjuncts case, Count must be 0) and
`TestQueryCount_ArrayTwoSidedRange` (the array-range regression below, Count/Iter
must be 3). Unit gate: `internal/qplanner/planner_test.go:TestIndexCoversFilter_GatesMultiPredicateField`;
over-approx contract: `query/filter_test.go:TestAndIndexBounds_SameFieldOverApprox`.

**Regression note (array two-sided range).** The initial fix here intersected
same-field conjunct bounds via a `Bounds.Intersect` helper (fix-sketch option 1
below). That fixed the scalar over-count but was UNSOUND for array/multi-key
fields: a doc matches `{tags:{$gte:2,$lte:3}}` when one element is >=2 and a
*different* element is <=3, so it need not have any element in the intersection
`[2,3]`. Intersecting narrowed the index seek to `[2,3]` and dropped such docs
from BOTH Count and Iter (e.g. `{tags:[5,1,4]}` — 5>=2, 1<=3 — was missed),
under-counting silently. Found by the 2026-05-29 differential review.
`Bounds.Intersect` was removed; the over-approximation + fast-path gate above
(fix-sketch options 2+3) replace it — sound for scalar and array fields alike,
at the cost of routing scalar two-sided ranges through `FilterIter` instead of a
tight seek.

**Follow-up (2026-07-10, `bug02-two-sided-bounds`):** the over-approximation's
perf cost (two-sided ranges scanned as half-open; CBO fed one-sided ranges) is
fixed WITHOUT weakening this contract, via a second channel:
`query.TightIndexBounds` intersects same-field conjuncts; estimation always
uses it; actual seeks use it only for the primary key (array pks now rejected
on write) and for indexes whose persisted `idx_mk:` record proves no fan-out
entry was ever written (see index.markMultiKey / isScalarProven). Multikey and
unknown indexes keep the wide bounds this entry mandates. Plan and tests:
any-store-tests:docs/any-store/plans/2026-07-10-bug02-two-sided-bounds-plan.md,
multikey_flag_test.go, query/tight_bounds_test.go.

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
pooled **seen-set** (`internal/qplanner/seenset.go`) that counts distinct docIds
with no peek-then-batch shortcut to misclassify. Reproducer:
`TestQueryCount_ScalarFirstCrossBoundDedup` in `query_test.go` (verified Count=4
before the fix).

**Trade-off (accepted 2026-05-29):** the canonical-key probe cannot detect a
compound index's multi-key entries (the array type tag is mid-key, not at byte 0),
so compound / non-PointLookup multi-bound counts always route to the seen-set. For
a high-selectivity *scalar* compound index this is slower than a page-level batch:
`simple_index/In` (`{a:{$in:[...]}}`), which the planner routes to a compound
`(a,b)` index on a cost tie, walks every entry instead of batching. The seen-set's
**skip-scalar** mode (read the per-entry value byte and count scalar entries
without touching the map) keeps this as cheap as a walk can be — **−78%** vs the
original typed-sort sort-dedup — but it is still a per-entry walk (~0.7 ms for a
25k-doc count vs `btree`'s ~24 µs batch). This is a performance-only regression —
the answer is correct. The single-field array path it was built for is *faster*
than the `btree` baseline: the seen-set beats the sort-dedup that was already
0.91× `btree` on `array_index/In`, with allocs ~1200× fewer; `unique_index/In3`
within ±5%; `array_index/InEmpty` +9% (~0.5 µs, the one extra probe Seek). Two
further characteristics: the canonical-key probe adds one Seek to every
single-field multi-bound count (visible only on near-empty results like InEmpty);
and `seenSetPool` retains its map + chunks at the high-water mark of the largest
count it served (the pooling is the low-alloc win, and the map buckets dominate —
~3.5× the chunk bytes). The 4-agent review confirmed this retention is bounded and
GC-reclaimable — no leak/corruption; a Put-time cap is a known follow-up. See the
seen-set A/B in the `129ffc0` commit message and
`any-store-tests:docs/any-store/2026-05-29-array-index-sortdedup-summary.md`.

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

**Reproducer:** `TestQueryCount_ScalarFirstCrossBoundDedup` in `query_test.go` (now active). Sets up the trigger conditions, confirms Count=4 before the fix and 3 after, with Iter=3.

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

## I-09: files written before the array-primary-key ban may contain array pks — recreate them

**Status: BY DESIGN** (pre-beta, alpha back-compat out of scope). Array pk
values are rejected on write since the two-sided-bounds work (ErrArrayPrimaryKey,
`collection.newItem`): filter semantics on arrays are element-wise while the
data-namespace key is the whole-array encoding, so such docs were already
semantically broken (match and key position decoupled even under the old
half-open bounds). Files written by earlier builds can still contain them; on
current builds pk-range queries seek tight two-sided bounds and will SILENTLY
exclude those docs (pure read paths do not type-check the pk — the excluded
keys are outside the scanned range by definition, so a scan cannot cheaply
detect them). Any update, upsert, or index backfill touching such a doc fails
loudly with ErrArrayPrimaryKey. Remediation: recreate the collection (or
re-insert the docs with scalar pks) before upgrading.

---

## I-07: `Count()` with `Limit`/`Offset` over a multi-key index disagrees with `Iter()`

**Status: FIXED** on `bug02-two-sided-bounds` (2026-07-10, two-sided-bounds plan commit 4; see any-store-tests:docs/any-store/plans/2026-07-10-bug02-two-sided-bounds-plan.md).
`LimitIter.CountDistinct` (internal/qplanner/limit_iter.go) deduplicates BEFORE
the cutoff: offset and limit apply to distinct-doc counts (early exit at
Offset+Limit distinct), and the Count dispatch (query.go) routes any
LimitIter-rooted plan through it instead of the generic Next loop. The
cursor-level offset fast-skip stays sound: it skips only scalar-recorded
entries, each of which is exactly one distinct doc. Regression:
`query_test.go:TestQueryCount_LimitOffsetMultiKey` (the trigger below plus
combined limit+offset and past-the-end cutoffs, asserted against Iter).

*Update 2026-07-16:* the residual asymmetry this fix left — Count deduping
BEFORE its cutoff while Iter's doc-dedup ran at the consumer, ABOVE the
in-plan Limit/Sort — is gone for compound multikey indexes too: a
`DocDedupIter` now dedups in-plan below every cutoff (see
`internal/qplanner/dedup_iter.go`), so `Count == len(Iter)` holds at any
offset on every plan shape. `CountDistinct` remains as the count sink; on a
deduped stream its own `DocDedup` is a passthrough.

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

**Status: FIXED** (2026-07-11) — on the OTHER side than sketched above. The entry assumed the Iter path was right and the Count path wrong; empirically it is the reverse: whole arrays ARE indexed alongside their elements and `Comp.Ok` ($eq) matches a whole array before falling back to elements (`Find({x:{$eq:[1,2]}})` was already consistent on both paths), so `$in` — an OR of equalities — was the odd one out: `In.Ok` only probed per-element membership. Worse than filed: `$in:[[1,2]]` gave Count=1/Iter=0 for a doc `{x:[1,2]}` (missed matches, not just the empty-array corner). Fix: `In.Ok` probes whole-array membership before iterating elements, mirroring `Comp.Ok`; matches MongoDB semantics. Tests: `TestIn` whole/empty/different-array subtests, `TestCollQuery_InArrayMember_CountIterConsistent` (Count==Iter across the trigger cases; red pre-fix).

---

## I-10: CBO ignores index uniqueness — unique-index point lookups flip to FullScan above ~150k docs

**Discovered:** 2026-07-11, while benchmarking storage layouts for any-sync spacestorage v2 (5 layouts × 6 real-corpus-shaped workloads).

**Affected code:**
- `internal/qplanner/planner.go` — IndexSeek cost estimation: the per-index count-min sketch (`DefaultSketchSize` = 1024 buckets) estimates `$eq` selectivity; `idx.Info.Unique` is never consulted when estimating rows for a full-key equality.
- `internal/qplanner/cost.go:27` — 1024 buckets means the sketch floor for a hot bucket is ~N/1024 rows.

**Failure mode:**
Collection with a custom pk and a UNIQUE single-field index on `id` (random CID-like strings). At 50k docs, `Find({id: $eq}).Limit(1)` always plans IndexSeek: 0.01 ms/get. At 175k docs the sketch estimates 152–951 rows for a value that occurs exactly once; IndexSeek's estimated cost crosses FullScan's and the plan flips — per value, so behavior is erratic: measured 8/20 sampled values choosing FullScan, 7.4 ms/get average (CPU profile: 89% in `FullScanIter.checkFilter`). The flip threshold scales with collection size vs sketch resolution, not with any property of the data.

**Impact: PERFORMANCE (severe, ~700× per lookup), PREDICTABILITY.** Answers stay correct. Any schema that uses a unique secondary index for point lookups on a large collection hits this once past the sketch resolution.

**Reproducer:** `any-store-tests:docs/any-store/repro/i10-unique-index-cbo/main.go` (self-contained sweep over doc count / payload size / id randomness; prints ms/get and per-value plan-flip counts; `PROFILE=1` writes cpu.prof).

**Fix sketch:** for a full-key `$eq` (all index fields bound with equality) on an index with `Unique: true`, estimate ≤1 row regardless of the sketch. One condition in the estimator; no sketch change needed. (Same reasoning would also cap `$in` on a unique index at len(in).)

**Workaround (what any-sync ships meanwhile):** `IndexHint{IndexName: "id", Boost: 1<<30}` on every point lookup — boost is subtracted from seek cost (`internal/qplanner/planner.go:508`), so the hint forces IndexSeek deterministically.

**Status: FIXED** (2026-07-11). Verified: the flip is real but hinges on `Limit(1)` — with a limit and no sort, `fullScanEffective = limit/pTotal`, and since pTotal is also sketch-derived (~1/1024) FullScan cost collapses to a constant ~614 while seek cost grows as N/1024; crossover lands at ~150–180k. So the fix caps BOTH sides: `uniqueFullKeyDocs` (planner.go) prices a full-key equality on a Unique index at `len(bounds)` rows in `estimateIndexDocsWithFieldSel`/`selectivityForIndex`, and the same rule feeds `calculateSelectivity` + the per-field selectivity table so pTotal becomes ~1/N and FullScan is priced honestly again. Matches SQLite (`whereLoopAddBtreeIndex`, WHERE_ONEROW). Partial prefixes on compound unique indexes are excluded (uniqueness of `(a,b)` bounds nothing for `a=x`), as are SPARSE unique indexes on the pTotal/per-field-selectivity sites (a sparse index drops null/missing docs, so eq-null bounds can match far more documents than the index has entries; the seek-side `uniqueFullKeyDocs` needs no sparse guard because `GuaranteesPresence` already rejects eq-null bounds for sparse candidates). `calculateSelectivity` claims fields for single-field unique indexes in a first pass so the bypass is independent of index declaration order (a compound index listed first would otherwise claim the field with a sketch-bucket or DefaultRangeSelectivity estimate — not enough to re-flip the plan, since the seek side alone pins the unique candidate at ~cost 4 vs FullScan's ≥7, but it inflates pTotal by orders of magnitude, and pTotal feeds estimatedYield and every candidate's filtered-yield comparison). Regression tests: `TestBuildPlan_UniqueIndexPointLookup_NoFlipAtScale`, `TestBuildPlan_UniqueIndexPointLookup_OrderIndependent`, `TestUniqueFullKeyDocs`. Repro after fix: 0/20 flips at 175k, est_rows=1, 0.009 ms/get. (Nit: the sketch is a single-hash count sketch, not count-min — inflation-only, which is exactly why the bug existed.)

---

## I-11: DDL inside an ambient tx + outer rollback leaves stale in-memory handles → writes to freed pages

**Discovered:** 2026-07-11, adversarial design review of any-sync spacestorage v2 (create-space-collections-in-one-tx pattern).

**Affected code:**
- `db.go:512-563` — `CreateCollection` registers the new `Collection` in `db.openedCollections` (`:558-560`) inside the tx callback; with an ambient ctx-tx this runs in a savepoint of the caller's tx.
- `collection.go:873-928` — `createIndex` publishes the index (+ its namespace handle) to the collection's in-memory index set the same way.
- `db.go:397-405` (`checkStale`) reacts only to a *committed* schema-cookie advance; `db.go:412-423` (`reconcileIndexSet`) re-resolves index namespaces of open collections but never evicts phantom collections.
- `internal/btree/db.go:1881` — `WriteTx.Put` builds a btree rooted at the handle's cached `ns.rootPage` with no by-name re-resolution.

**Failure mode:**
`tx := db.WriteTx(ctx)` → `db.Collection(tx.Context(), "x")` creates the collection (savepoint commits, handle registered) → caller's OUTER `tx.Rollback()` undoes the namespace creation on disk — but the handle stays in `openedCollections` with its now-dangling root page. A later `db.Collection(ctx, "x")` returns the stale handle; the next write lands on a page that is free (or reallocated to another namespace): silent cross-namespace corruption. v1/sqlite failed loudly in the equivalent situation because tables resolve by name per statement.

**Impact: CORRECTNESS (silent data corruption)** on a plausible pattern (create + populate in one tx, roll back on validation failure, retry later on the same DB handle).

**Fix sketch:** publish DDL results to `openedCollections` / index sets only on top-level commit (tie registration to the outermost tx lifecycle), or re-validate `ns` by name at top-level tx begin, or evict handles created within a tx when that tx (or any ancestor) rolls back.

**Workaround (any-sync):** treat a failed create-space tx as poisoning the whole DB handle — close and reopen it before any retry.

**Reproducer:** `any-store-tests:docs/any-store/repro/i11-stale-handle-rollback/main.go` (post-fix it demonstrates the correct behavior: the re-acquired handle is fresh and the write lands in the right collection).

**Status: FIXED** (2026-07-11, ddl-rollback-eviction). Empirically confirmed first: create "x" via `tx.Context()`, `tx.Rollback()`, create "y" (reuses x's freed root page), insert through the stale "x" handle → the document landed inside collection "y" (`y.FindId` returned it) while `IntegrityCheck` passed before AND after reopen — logical, not structural corruption. Before page reuse the write failed loudly (`btree: database is corrupt`); the silent phase started on reuse. Two additions to the write-up: (1) the trigger is broader than ambient-tx rollback — the handle is registered before `tx.Commit()`, so a failed top-level commit poisons it the same way (also covered by the fix); (2) "re-validate at tx begin" does NOT work: `checkStale` triggers on the schema cookie, which a rollback never bumps.

Fix: a per-tx undo log on `commonTx` (`tx.go`). Every DDL that publishes in-memory schema state registers its reversal via `onRollbackUndo`: CreateCollection (evict + reset phantom fts pendings + mark closed), CreateIndexes/EnsureIndex and DropIndex (restore pre-change index-set snapshots via the shared `registerIndexSetRestore` — a rolled-back create otherwise maintains an index over freed pages, a rolled-back drop silently stops maintaining a live one), Rename (restore the name). Undos run in reverse order on top-level rollback, failed commit, and savepoint rollback (`savepointTx` records a mark, so a nested rollback unwinds only its own scope while a release keeps entries registered for the outer tx). `Drop` needs no undo: it closes and evicts at drop time, which is rollback-safe.

Locking (from the adversarial review of the fix): the unwind must run inside the btree write critical section — `writeTx.Rollback` and the flush-failure commit paths run it BEFORE the btree rollback (which releases the global write lock); savepoint rollback keeps the lock throughout; the failed-COMMIT path is the exception (btree `Commit` releases the lock before returning its error, and a failed pager commit self-recovers to `pagerOpen` — no persistent error state fences writers), so it is serialized by `db.ddlUnwindGate`, held across btree commit + unwind by DDL-bearing txs and passed through once by `newWriteTx`.

~~Known residual (pre-existing, broader than this bug): the `closed` flag is not checked by collection operations, so a caller that retains a handle across a rollback/Drop can still misuse it — eviction guarantees every NEWLY acquired handle is consistent with disk.~~ **Residual closed by the I-16 fix (2026-07-11):** every public collection operation now checks the flag via `alive()` and fails with `ErrCollectionClosed` (wraps `ErrCollectionNotFound`); write ops check INSIDE the tx scope so the check runs after the tx-begin staleness pass. See also I-16 (Rename never re-keys `openedCollections`; pre-existing, surfaced by this review — since fixed). Tests: `ddl_rollback_test.go` (5 cases, all red pre-fix), `TestClosedHandleOpsFail`; race detector clean.

---

## I-12: `docCount` full-walks index-less collections on every query

**Discovered:** 2026-07-11, same layout-benchmark round as I-10.

**Affected code:**
- `query.go:753-762` — `collQuery.docCount` returns the first index sketch's DocCount, else falls back to `tx.Count(ns)`.
- `query.go:232, 310, 458, 599, 686` — the fallback runs on every `Iter`/`Update`/`Delete`/`Count`/`Explain`.
- `internal/btree/btree.go` `Count` — page-header traversal of the entire namespace (reads every page).

**Failure mode / impact: PERFORMANCE.** A collection with no secondary indexes pays O(all pages of the namespace) *before* every query — even a fully pk-bounded range read of 3 rows. For a 300MB namespace that is ~73k page reads per query. This silently punishes exactly the schemas the custom-pk feature encourages (pk-ordered collections that need no secondary index).

**Fix sketch:** persist a per-namespace document count (namespace header or master-table record, maintained on insert/delete like the freelist) → O(1) `docCount` and O(1) `Collection.Count()`. Until then any index's sketch (I-10 caveats aside) is the only O(1) source.

**Status: FIXED** (2026-07-11), by a different route than the sketch above. Key fact the write-up missed: with no secondary indexes the planner has exactly one candidate (FullScan), so the counted value was never used — the walk was pure waste, not a needed input. Fix: `docCountForPlan` skips the `tx.Count` namespace walk on the plan paths (Iter/Update/Delete/Count) when the collection has no secondary indexes; Explain uses `docCountExact` and keeps the real count; both take the caller's index snapshot so the count and the CBO candidates cannot disagree. The persisted-count idea was rejected: it adds a per-commit write hotspot and diverges from sqlite (which never counts during planning — stat tables only); indexed collections already have an O(1) source via the sketch DocCount. Regression test: `TestCollQuery_DocCountSkipsWalkWithoutIndexes`.

---

## I-13: whole-sketch persistence on every touched-index commit — 2-3× WAL amplification for small txs

**Discovered:** 2026-07-11, perf review of the spacestorage v2 design (per-commit write patterns of sync workloads).

**Affected code:**
- `collection.go:1448` — `persistSketches` marshals and `Put`s the full sketch blob whenever `sketchModified`.
- `db.go:944` — `persistAllDirtySketches` runs in the commit hook for every write tx.
- `internal/qplanner/sketch.go` `MarshalBinary` — `DefaultSketchSize`=1024 buckets → 8,236-byte blob (~3 pages with overflow).

**Failure mode / impact: PERFORMANCE.** A typical small commit (one ~1.7KB doc insert = 2-3 data WAL frames) that touches one indexed field also rewrites the 8.2KiB sketch blob — every commit, per touched index. Workloads with an always-touched index (e.g. a last-seq index maintained on every batch) carry 2-3× WAL bytes; two such indexes carry more. Compounds with I-01 (sketch drift is reloaded-clean only on reopen anyway).

**Fix sketch (graduated):** (1) skip sketches entirely for `Unique` single-field indexes — they serve point lookups; with I-10 fixed the estimator needs no sketch for them; (2) persist dirty sketches on checkpoint/close instead of per-commit (they are advisory statistics; staleness within a session is already tolerated per I-03); (3) delta-encode sketch updates.

**Status: OPEN — deliberately not fixed 2026-07-11.** A persist-every-N-commits throttle was considered and rejected for now: the persistence policy for shared state is a contract-level decision — the DB promises sqlite-like multi-process semantics (any process may open and read/write at any time), and the persisted blob is the only channel through which other processes see sketch updates. (SQLite itself keeps stats only ANALYZE-fresh, so bounded cross-process staleness would not violate that contract — which is why this stays a deliberate open decision rather than a quick fix.) Mechanically, a naive throttle also collides with `resetUncommittedSketches` (db.go), which reads a still-set `sketchModified` at write-tx begin as rolled-back phantom deltas and rebases the live sketch to the (now stale) committed bytes — any throttle needs a separate committed-but-unpersisted dirty signal. Note (2) is also harder than it sounds — checkpoints don't originate logical writes, so it needs a new write-tx-at-close path. Corrections to the numbers: the single-field blob is 8,220 bytes (not 8,236), and compound indexes are larger (3-field ≈ 24.6KiB, `nb = Size*Levels`); the trigger is any insert/delete/update of an indexed document (docCount bump), not only a changed indexed field. Remaining viable directions: (1) skip-unique (now unblocked by the I-10 fix, but only helps unique single-field indexes) and (3) delta encoding.

---

## I-14: `MarshalCompressed` has no incompressible fallback; parser `decompBuf` pins the largest-doc high-water mark

**Discovered:** 2026-07-11, perf review of spacestorage v2 (encrypted change payloads; 5.4MB max doc in the profiled corpus).

**Affected code:**
- `anyenc/value.go:387` — `MarshalCompressed` S2-tags every object > `CompressMinSize` unconditionally; there is no "keep plain if not smaller" check.
- `anyenc/parser.go:386, 400` + `anyenc/parser.go:112-116` — `decompBuf` grows to the largest compressed doc ever parsed and is deliberately excluded from the syncpool size accounting (`syncpool/syncpool.go:47-54`).

**Failure mode / impact: PERFORMANCE / MEMORY.** High-entropy payloads (encrypted or already-compressed — the normal case for CRDT change bodies) pay `s2.Encode` + a second marshal on every insert and `s2.Decode` + a full copy on every parse (every doc of every scan), for zero size win. Pooled DocBuffers pin multi-MB `decompBuf`s per DB on blob-heavy data — invisible to `SyncPoolElementMaxSize`.

**Fix sketch:** byte-level fallback at marshal time — if the S2 output is not smaller than plain, store plain (one length compare; the type tag already distinguishes). Separately, cap or count `decompBuf` in the pool's size accounting.

**Workaround (any-sync):** `CollectionOptions{Compression: NoCompression}` on collections holding encrypted/binary payloads.

**Status: FIXED** (2026-07-11). Both halves: (1) `MarshalCompressed` keeps the plain `TypeObject` bytes (already materialized in `scratch`) when `len(compressed)+5 >= len(plain)` — NOT a format change: readers dispatch on the leading type byte, so old readers handle the output unchanged, and incompressible docs skip the decode+copy on every future parse entirely; (2) `Parser.TrimScratch(limit)`, called from `SyncPool.ReleaseDocBuf` with `SyncPoolElementMaxSize`, frees the input-copy and decompression buffers when their combined capacity exceeds the pool element limit (decompBuf sacrificed first), so retained scratch is always <= the limit. Corrections to the write-up: `decompBuf` retains the largest DECOMPRESSED doc (worse than stated, bounded only by the 1GB `maxDecompressedSize`); high-entropy S2 output was slightly LARGER than plain, not equal; the encode-side CPU cost was overstated (S2 emits uncompressed blocks) — the decode-side full copy per parse was the real cost. Tests: `TestMarshalCompressedIncompressibleKeepsPlain`, `TestParserTrimScratch`.

---

## I-15: Backup holds the entire destination in one write tx — peak RAM ≈ database size

**Discovered:** 2026-07-11, node cold-sync benchmark of any-sync spacestorage v2 (real 491MB space store).

**Affected code:**
- `internal/btree/backup.go:46` — "dstWriteTx is held from first Step until Finish": a single destination write tx spans the
  whole backup, so every copied page stays dirty in memory until the final commit.

**Failure mode / impact: MEMORY.** Measured: backing up a 491MB store adds ~700MB VmHWM to the process (~1.27GB peak during a
node cold-sync serve, vs ~200MB for the sqlite/v1 equivalent). Scales linearly with db size — a multi-GB space would take
multi-GB RAM per concurrent backup. Wall time is fine (0.9s vs sqlite 0.66s for 491MB); this is purely a dirty-page-retention
peak. On any-sync nodes Backup runs for coldsync serving and archiving — serialized today, so it's a spike not a leak, but it
caps the safe space size and concurrency.

**Fix sketch:** commit (or spill) the destination tx every N copied pages — SQLite's backup restarts on source change anyway
(the FileChangeCounter check at backup.go:207 already handles that), so intra-backup commits only need the existing
restart-on-change logic plus marking the backup file invalid until Finish (e.g. keep the header/page-1 write for the final
commit). Alternatively spill dirty pages to the destination WAL as it goes.

**Workaround (any-sync node):** none needed at current space sizes; keep backups serialized and mind RAM headroom for
multi-GB spaces until fixed.

**Status: ROOT-CAUSED, fix validated (2026-07-11) — the original mechanism was RIGHT and the first review refutation was WRONG.** The pager does implement cache spill (`pagerStress`), and backup pages are unpinned and eligible — but the spill is **permanently wedged on page 1**: backup copies page 1 first and releases it, making it the oldest unpinned dirty page; `findSpillVictim` (pcache.go) walks from `dirtyTail` and returns page 1 every time; `pagerStress` refuses `pgno == 1` by returning nil WITHOUT cleaning it, and `pcache.create` never retries another victim. Instrumented counters over one backup of a 467MB store: 108,975 spill attempts, 108,975 hit the page-1 guard, 0 frames spilled, all 113,974 dst pages accumulate dirty. Measured: VmHWM +608-645MB (heap profile: 95%+ `allocPageBuffer` via `Backup.onePage`), independent of CacheSize (the limit is unenforceable) and of MmapSize (production configs set none — the mmap theory is dead). This is the exact hazard named in `docs/btree/NOTES.md#old-drift-pagerstress-page1-exclusion`: C relies on page 1 staying pinned; backup breaks that invariant.

**Status: FIXED** (2026-07-11). `findSpillVictim` skips page 1 (`p.pinCount == 0 && p.pgno != 1`); the page1-exclusion drift note now covers both guard layers; regression `TestBackupWriterCacheBounded` bounds the destination writer cache during a >CacheSize backup and asserts frames reach the WAL before Finish (red pre-fix). The pre-existing `TestPagerStress_Page1NeverSpilled_RegressionPin` was updated: its precondition pinned the OLD wedge-prone behavior (victim search returning page 1). Validated effect: Result: VmHWM delta 612MB -> 84MB, writer cache bounded at exactly CacheSize (RSS now scales with it: 85MB @ 5000 pages, 315MB @ 20000), all spills write WAL frames, wall time 0.59s -> 0.49s. Also unwedges the same latent stall for any large write tx that dirties+releases page 1 early. Ship with a regression test bounding `writerCache.nPage` during a >CacheSize backup and an update to the page1-exclusion drift note.

**Disk doubling: VERIFIED, accept-as-designed.** The dst WAL peaks at ~= full DB size (truncated by Finish's checkpoint); on current HEAD it is written in one burst at Finish, with spill fixed it grows gradually to the same peak. Single-tx WAL backup doubles transient disk in SQLite too; no fix needed.

---

## I-16: `Rename` is half-implemented — stale map key is only the surface; renamed collections become unopenable

**Discovered:** 2026-07-11 (map-key symptom during the I-11 fix review); full scope established by a dedicated investigation the same day (empirical, incl. a two-process repro).

**Corrected failure mode (the original "second handle" claim was wrong — reality is worse):** `Rename(A→B)` rewrites the four system-namespace key families (`coll:`, `collcfg:`, `idx:`, `stat_data:`) and updates `c.name`, but (a) never re-keys `db.openedCollections` (entry stays under "A") and (b) never renames any btree namespace — the data namespace is still "A", index namespaces still `ix:A:*`, and `collection.init` resolves the data namespace BY NAME. Observed through the public API:

- `OpenCollection("B")` fails with a leaked `btree: namespace not found` (not `ErrCollectionNotFound`); after the cached handle is evicted or the DB reopens, the renamed collection is **permanently unopenable** — bricked durable state.
- `OpenCollection("A")` returns the renamed handle under the wrong name; `CreateCollection("A")` is blocked by the stale map key even though the catalog freed the name.
- `Rename` onto an existing collection name silently **overwrites its catalog entry** (blind `tx.Put(collKey(newName))`, no existence check).
- **Cross-process (two-OS-process repro):** a peer holding "A" open sees the schema-cookie bump, reconciles under its stale name, finds no `idx:A:*` metadata, and publishes an EMPTY index set — its subsequent writes commit **unindexed** (verified raw: doc in ns "A" with no entry in `ix:A:a`). Durable index corruption, invisible to all processes.
- `Drop` after rename deletes the "B"-derived namespaces (tolerating NotFound) and orphans the real "A" data + pre-rename index namespaces forever.
- Post-rename index DDL splits worlds: metadata registers `idx:B:x` while pre-rename entries live in `ix:A:*`; the vector index even derives its HNSW seed from `c.name`.

**Impact: CORRECTNESS (bricked collection, durable unindexed writes, silent catalog overwrite).** Trigger is any Rename use at all — not just the rollback corner the entry originally described.

**Fix plan (from the investigation):**
- *Phase 1 (~half a day):* re-key `openedCollections` inside `Rename` under `c.mu`→`db.mu` with a single combined rollback undo (restore key + name by value; the `cur == c` guard makes the Rename→Drop-rollback zombie heal itself); guarded identity-delete in `onCollectionClose`; reject rename onto an existing name (`ErrCollectionExists`) and same-name no-op. Fixes the map-level symptoms while any handle stays cached.
- *Phase 2 (~2-4 days):* the real fix — btree `RenameNamespace` (master-table re-key, root page unchanged; the ALTER TABLE RENAME analog, keeping sqlitec alignment), then `renameCollection` sweeps data + `ix:` + fts + vector namespaces AND `multikey:` keys; rename detection in peers' `checkStale` (collKey vanished → adopt new name or invalidate, SQLite re-prepare style); Rename input validation. Until Phase 2, Rename cannot be made safe — consider documenting it as broken/deprecated in the interim.

**Status: FIXED** (2026-07-11, both phases in one change; peers INVALIDATE stale handles, they do not adopt the new name).

- *btree:* `RenameNamespace` (DB method + WriteTx wrapper) re-keys the master cell, root page unchanged; cookie bump stays caller-side like Create/Delete. Drift note: `docs/btree/NOTES.md#drift-2026-07-11-renamenamespace-delete-put-vs-sqlite-master-row-update`.
- *renameCollection:* validates the new name (`ErrInvalidCollectionName`: empty / `_system` / `ix:`,`ftx:`,`vix:` prefixes; `":"` deliberately allowed — within-family ambiguity fails loudly with `ErrNamespaceExists`, never silently), rejects rename onto an existing catalog entry (`ErrCollectionExists` — the silent overwrite is gone), renames the data namespace (hard error if missing — legacy pre-fix victims get a pointed diagnostic) and every per-index namespace (range/fts×5/vector×7, tolerating absent ones like Drop does), and re-keys `idx_mk:` records alongside `stat_data:`. `CreateCollection` now applies the same name validation.
- *collection.Rename:* re-binds range-index handles to the renamed namespaces (`cloneWithNs`, copy-on-write publish — a stale `ns.Name()` would write the multikey flag under the wrong `idx_mk:` key and a rename cycle would clobber the real record: tight seeks over an array-holding index, dropped docs), re-keys `openedCollections` with identity guards, single combined rollback undo. `onCollectionClose` evicts by identity scan, not by (racy) name.
- *Collection identity:* the `coll:` record value is now a per-creation random token (`newCatalogID`; legacy files hold "1"), moved verbatim on rename, cached on the handle at init. The peer staleness pass (`reconcileIndexSet` → `collectionVanished`) invalidates a handle whose catalog key vanished OR whose token/root no longer matches — detecting rename-away, drop, drop+recreate (the root page alone false-negatives there: the freelist typically hands the recreate the same root) and rename-then-recreate name reuse.
- *Invalidation semantics:* `invalidateCollection` (CAS on `closed`, fts pendings reset — never orphaned, identity-guarded eviction); every public collection op now checks `alive()` → `ErrCollectionClosed` (wraps `ErrCollectionNotFound`), write ops inside the tx scope so a racing op fails after the begin-time staleness pass. `IntegrityCheck` additionally reports cataloged collections whose data namespace is missing (pre-fix rename victims).
- *Tests:* `internal/btree/namespace_rename_test.go` (10 cases incl. interior master table, rollback/savepoint, freelist), `collection_rename_lifecycle_test.go` (reopen-bricked repro, all-index-kinds reopen, drop-no-orphans, closed-handle matrix), `ddl_rollback_test.go` extensions (map-key restore, savepoint scope, rename→drop heal), `rename_mp_test.go` (peer invalidation incl. the durable-unindexed-write repro, name reuse, drop+recreate, read-path). Red-on-HEAD verified for the three headline repros (bricked reopen, silent catalog overwrite, silent unindexed peer write) against 72acf6c.

**Accepted residuals (documented, not fixed):**
- The `FindId`/`BeginReadFast` fast path never runs `checkStale`, so a process that only ever uses fast reads learns of the vanish on its first non-fast op; until then it reads a consistent (renamed but intact) snapshot — no durable damage. Once invalidation has run, `FindId` fails via `alive()` like everything else.
- A read that itself observes the cookie bump completes on its consistent snapshot (invalidation applies to subsequent ops) — SQLite re-prepare semantics.
- ~~In-process race: between Rename's map re-key and its commit, a concurrent `openCollection(oldName)` can register a second handle from the still-committed old catalog.~~ Closed by review (2026-07-12): the registry re-keys only at commit via a commit-publication queue on `commonTx` (`pubs`, the dual of the undo log, with savepoint scoping) — pre-commit, `OpenCollection(oldName)` keeps returning the renaming handle, which matches the committed catalog. The staleness pass skips a collection whose registered map key and `c.name` disagree (rename in flight), which also fixes a spurious self-invalidation window, and `collectionVanished` checks by registered name.
- Legacy DBs already broken by a pre-fix rename are detected (`IntegrityCheck`, and `Rename` on them errors with a diagnostic) but not auto-repaired; a manual `RepairRenamedCollection(old,new)` sweep is a straightforward follow-up if users hit it.
