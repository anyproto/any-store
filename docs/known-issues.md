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
