# BUG-02: restore two-sided range bounds — fix plan

**Date:** 2026-07-10
**Bug:** `../any-storev2-pre-beta-bugs/BUG-02-and-indexbounds-drops-range-end.md` (pre-beta gate, severity High)
**Also fixes:** BUG-06/I-07 (commit 4), BUG-12/NEW-A (commit 2), BUG-13/NEW-B (commit 3)
**Status:** IMPLEMENTED on `bug02-two-sided-bounds` (2026-07-10), commits 1-7 as
planned (design expert-consulted; adversarially reviewed by 4 independent
passes — all confirmed holes folded in below), plus a post-implementation
hardening pass from a second adversarial review: verify-chain
residual-predicate gate (a tight-collapsed PointLookup could skip the residual
filter and over-count); scan cost priced from the bounds the chain executes
with (rangeSel/rangeSelTight split); the multikey flag rekeyed by the
IMMUTABLE index namespace name (rename handling becomes unnecessary; the
plan-time probe no longer reads the mutable collection name); array-pk pre-ban
detection documented as known-issues I-09. Measured on the acceptance shape
(200k docs, descending two-sided pk range, Limit 10): 14.6ms → 26.9µs.
External validation: any-store-tests e2e+storetest green; the 175-bench
harness run shows no >10% regressions, CompoundIndex/PrefixRange -47%.

## Goal

`{"field":{"$gt":lo,"$lt":hi}}` currently loses its upper bound before reaching the
planner: `And.IndexBounds` (query/filter.go:372) returns the first contributing
conjunct's bounds only. Fix the three consequences — (1) descending range scans start
at the last key of the collection and forward scans overrun to namespace end,
(2) secondary-index range scans identically affected, (3) CBO selectivity computed on
the half-open range (0.50 instead of ~0.005, flipping plans) — WITHOUT reintroducing
the I-04 array regression.

## Why the obvious fix is forbidden

Intersecting same-field conjunct bounds was shipped (6fca7dd) and reverted (ba92bc7):
array filter semantics are per-conjunct-per-element, so `{tags:{$gte:2,$lte:3}}`
matches `{tags:[5,1,4]}` via different elements while NONE of its index entries
(1, 4, 5, whole-array) lies in `[2,3]`. A seek narrowed to the intersection never
yields the doc and the residual FilterIter cannot re-add it — docs vanish from both
Iter and Count. See docs/known-issues.md I-04 and the contract comment at
query/filter.go:352-371. `TestQueryCount_ArrayTwoSidedRange` pins this and must stay
green through every commit below.

The same semantics make **contradictory ranges satisfiable**: `{a:{$gt:5,$lt:3}}`
MATCHES `{a:[6,1]}` (6>5 via one element, 1<3 via another — verified empirically on
HEAD: Count=1). So an empty tight intersection is NOT proof of an empty result set
except where fan-out is provably absent. This killed the first draft's general unsat
short-circuit; see design point 5.

Narrowing is sound exactly when every matching doc has at least one index entry
inside the narrowed range. For a doc with a single index entry (scalar value, or the
single whole-value entry of an empty array), match ⟹ that entry satisfies every
conjunct ⟹ it lies in the intersection. (This argument covers `Comp`'s whole-array
branch AND `In` — note `In.Ok` has no whole-array membership branch at all,
query/filter.go:458.) Unsoundness comes only from fan-out docs (non-empty arrays:
≥2 entries — one per distinct element plus the whole array, index.go writeValues).
Nested paths do NOT fan out on either side: anyenc `Value.Get` descends arrays only
by numeric key (anyenc/value.go:85) and `Key.Ok` uses the same Get
(query/filter.go:326), so `{a:[{b:1},{b:5}]}` with index/filter on `"a.b"` is null on
both the key-generation and the match side — no Mongo-style nested multikey. Do not
"fix" Get to fan out without revisiting this plan.

So: **tight bounds may drive seeks (or any answer-determining decision) only where
fan-out docs are provably absent; estimation may use tight bounds whenever they are
non-empty** (estimates cannot drop docs; the empty-intersection case falls back to
wide in both channels, design point 5).

## Design overview (expert-reviewed, adversarially verified)

Two bound channels per field, plus a persisted per-index scalar/multikey gate:

1. **Wide channel (unchanged):** `Filter.IndexBounds` keeps today's sound
   over-approximation contract. No existing pin tests change semantics.
2. **Tight channel (new):** `query.TightIndexBounds(f, fieldName) (Bounds, empty bool)`
   — a free function (type switch), NOT a Filter interface change:
   - `Key`: path match → recurse into child.
   - `And` / `*And` (parser emits both — value `And` inside `Key` for inline
     `{$gt,$lt}` objects, `*And` for `$and` arrays): intersect the tight bounds of
     contributing children (interval-set intersection), skipping children that
     contribute nothing (they are unconstrained, not empty). Empty intersection →
     `empty=true` (see point 5 for what callers may do with it).
   - Everything else (`Or`, `Not`, `Nor`, `Comp`, `In`, …): delegate to
     `IndexBounds(fieldName, nil)`. (`Not`/`Nor`/`All` contribute nothing;
     `Or` keeps its existing all-or-nothing union. Tightening inside Or branches is
     deferred — see Deferred.)
   - Reintroduce `Bounds.Intersect` from 6fca7dd (pairwise `intersectPair` +
     `SortAndMerge`; handles multi-interval children: `$in` point sets, `$ne` rays;
     uses interface-based sort.Sort, compliant with the no-reflection-sort rule).
     Endpoints alias already-cap-clipped conjunct memory and are never mutated in
     place, preserving the docs/query-filter-contract.md aliasing rules.
3. **Estimation uses tight bounds; pricing is split.** Two distinct estimation roles:
   - *Doc selectivity / yield* (how many docs match): tight bounds whenever
     non-empty (empty falls back to wide, point 5) — sound, fixes plan flips and
     the 0.50 selectivity header.
   - *Scan cost* (entries the chosen chain will actually visit): priced from the
     bounds the chain actually seeks with (wide for multikey/unproven indexes).
     Costing tight while seeking wide undercharges the seek and picks plans that
     still scan half the index.
4. **Seeks use tight bounds only behind a proof:**
   - **Primary key:** array primary keys become a write-time error (pre-beta,
     MongoDB-compatible restriction; array pks are already semantically broken —
     element-wise Ok vs whole-array key ordering decouples match from key position
     even for today's one-sided bounds). With arrays banned, the data namespace is
     fan-out-free **for data written after the ban**; files already containing array
     pks are explicitly out of scope per the pre-beta no-alpha-back-compat decision
     (they are broken today; commit 1 adds a loud defensive error on reading one, so
     the failure mode is detection, not silent drops). Tight `idBounds`
     unconditionally — this soundness claim DEPENDS on commit 1.
   - **Secondary indexes:** a persisted, sticky, one-way per-index multikey flag in
     the system namespace (same btree ⇒ same transaction/snapshot as the entries):
     - index creation writes an explicit "scalar-so-far" marker (same tx as the
       backfill, which re-flips it via insertKeys if the backfill meets arrays —
       do not "optimize" the marker to after backfill);
     - `index.insertKeys` — the single entry-creation choke point — flips it in the
       SAME write tx that writes the first fan-out entries. The flip keys off
       `len(keysBuf) > 1` (precisely "non-empty array at an indexed field", incl.
       compound cartesian fan-out) — NOT off whether a Put happened (unique-index
       re-inserts skip Puts, index.go:466);
     - **check-and-put, no commit-time latch:** on the fan-out path, `Get` the
       record through the write tx's own view and Put only if not already multikey.
       A "set latch on commit / reset on rollback" scheme is UNSOUND: savepoint
       partial rollback inside a committing outer tx (tx.go:162-227) and
       cross-process drop+recreate with root-page reuse + live-object reconcile
       (collection.go:1367, pager freelist-first) both leave a stale latch that
       skips the Put while fan-out entries commit. One extra tx-view point Get per
       array-writing tx is the price; memoize per-tx if it shows in benchmarks;
     - `deleteKeys` never clears it (concurrent snapshots may still hold fanned-out
       entries); drop+recreate is the reset;
     - **lifecycle:** the record is keyed by collection+index name, and names are
       mutable — `renameCollection` must move `mk:` records alongside `idx:` records
       (a stale scalar record resurrected by rename A→B→A after B saw arrays is a
       permanent wrong-answer hole); `dropIndex` and `removeCollection` must delete
       them (next to the sketchKey delete, collection.go:1079);
     - plan-time: at most one point `Get` per candidate index, **on the same read
       tx the query executes with**. That is the load-bearing rule (a probe on a
       different snapshot can diverge from what the scan sees); "not in
       buildCBOIndexesInto" is merely its consequence under today's call timing —
       Count builds CBO indexes before doReadTx opens (query.go:582/584). After the
       commit-7 restructure the probe lives wherever candidate finalization runs
       inside the tx. Probe only when tight ≠ wide (define Bounds equality: interval
       count + endpoint bytes + inclusivity) for some bounded field of the candidate
       index; **absent record ⇒ assume multikey** (covers alpha-era files
       conservatively; no backfill, no probe — drop/recreate to opt in);
     - snapshot consistency is free: if our snapshot can see fan-out entries, it can
       see the flag written in the same tx. NO in-memory caching on the read side.
5. **Empty tight intersection is NOT unsat in general.** `len(bounds)==0` keeps
   meaning "unconstrained" everywhere; `TightIndexBounds` reports emptiness via the
   explicit `empty` flag, and callers may treat it as "provably zero results" ONLY
   for the primary-key field (sound post-commit-1). For any other field — indexed,
   unindexed, flagged or not — an empty intersection means "fall back to the wide
   bounds for this field, both channels" (`{a:{$gt:5,$lt:3}}` over `{a:[6,1]}` must
   keep returning 1 row on Find/Count/Update/Delete; pin this). A plan-time,
   flag-gated unsat for non-sparse scalar-proven indexed fields is Deferred.
6. **Iterator prerequisite:** IndexIter walks its interval list in ASCENDING order
   even when `Reverse=true` (index_iter.go:143-166), unlike FullScanIter which
   reverses (fullscan_iter.go:31-33). This is a live wrong-answer bug today for
   reverse ExactSort multi-interval plans (`$in` + `Sort("-f")` + Limit) and
   intersection widens exposure (`$ne` splits one interval into two). Fix the walk
   order first; do NOT suppress ExactSort (that would kill limit pushdown).

### Invariants for the execution hookup

1. **One bounds set per chain, flags included.** The chosen plan's chain must derive
   from ONE bounds set decided up front — seek bounds AND the flags computed from
   them: `PointLookup`, `equalityPrefix`, `ExactSort`/`shouldReverse`, `dedupBounds`
   (CanonicalKeyDedupIter takes the un-padded view — planner.go:1205, 1316),
   `AdjustBoundsForNonUnique`, reverse-tail padding. Swapping bounds under flags
   computed from the other channel produces verified wrong answers: stale
   `PointLookup=true` from tight `{$gte:5,$lte:5}` sends a unique multikey index
   into CoverIter (planner.go:1161), which point-Gets `Start` and ignores `End` —
   `{a:[4,6]}` vanishes; stale `equalityPrefix` from tight bounds makes
   `ExactSort=true` while executing wide → no SortIter → wrong sort order under
   Limit. Mechanically: build BOTH complete variants (bounds + derived flags) per
   candidate, probe the flag inside the read tx, select one variant before cost
   evaluation. This requires moving CBO candidate finalization inside the read tx
   on every path (Count builds CBO indexes before doReadTx today, query.go:582/584).
2. **Cross-index consumers read the channel of THEIR index.** A candidate index's
   scalar proof says nothing about a different index consulted mid-chain:
   `buildVerifyChain` (planner.go:1647) drives VerifyIter point-probes against the
   VERIFY index's namespace using per-field Fixed bounds — with tight bounds,
   `{b:{$gte:3,$lte:3}}` collapses to a point and VerifyIter drops `{b:[2,4]}`
   (matches via elements 4 and 2; has no entry enc(3)). Verify chains and any other
   consumer keyed to a non-candidate namespace read the WIDE per-field channel
   unless the index they probe is itself scalar-proven.

## Newly discovered bugs (file in the catalog alongside this work)

- **NEW-A (wrong answer, exists today, broader than first thought):** the id-only
  Count fast path (query.go:563-577) does `tx.Has(bound.Start)` per bound;
  `isIDOnlyFilter` (query.go:789) checks only that the filter references the pk
  field, never the predicate shape, and wide And.IndexBounds keeps only the first
  contributing conjunct. Verified on HEAD: `{id:{$gt:1,$lt:5}}` → Count 1 vs Iter 3;
  `{id:{$in:[1,2,3],$nin:[2]}}` → 3 vs 2; `{id:{$in:[1,2,3],$gt:1}}` → 3 vs 2;
  `{id:{$in:[1,2,3],$type:"string"}}` → 3 vs 0. Note the last shape can NEVER be
  fixed by bounds (TypeFilter contributes none) — the gate must be filter-shape,
  not bound-shape.
- **NEW-B (wrong answer, needs repro-first):** reverse multi-interval ExactSort
  order (design point 6). Repro: index on `a`, `{a:{$in:[1,5]}}`,
  `Sort("-a").Limit(1)` — expect the doc with a=5, suspect a=1 today.
- **(observed, not in scope):** `Or.IndexBounds` drops ALL bounds when a branch's
  contribution merges into an existing overlapping bound (the `len` delta protocol,
  filter.go:563); overlapping `$or` ranges degrade to full scans. Perf-only.
  Catalog as low-priority; the tight channel does not use the length-delta protocol.
- **(observed, likely existing `stat_data:` leak):** renameCollection moves only
  collKey/collConfig/`idx:` records (db.go:1314-1378); sketch records appear to leak
  on rename/drop the same way `mk:` would. Advisory-only data, so harmless today —
  catalog it, and fix it in passing in commit 7 (moving `stat_data:` alongside `mk:`
  in the same rename/drop touch is a two-line addition).

Filing: NEW-A and NEW-B are filed as BUG-12 and BUG-13 in the catalog; the Or quirk
and the `stat_data:` leak get entries with commit 7. BUG-06 already has a catalog
entry (and is I-07 in known-issues.md) — commit 4 fixes it and updates both statuses.

## Commit sequence

Each commit is separable, individually testable, and keeps
`TestQueryCount_ArrayTwoSidedRange` + the I-04 pins green.

### 1. `db: reject array-valued primary keys on write; error loudly on read`

- `collection.go` `newItem` (single validation choke point, presence check today):
  reject `TypeArray` pk values with a new exported error (follow `ErrDocWithoutId`
  conventions). Insert/Update/Upsert/UpdateId paths all construct items here —
  verify and add tests per path. Objects stay legal (whole-value semantics, total
  order holds; no element-wise decoupling — `Comp.Ok` treats objects as whole
  values, query/filter.go:131).
- Defensive detection (as implemented): `newItem` is also the choke point for
  update/upsert/index-backfill over EXISTING docs, so any write-adjacent touch
  of a pre-ban array-pk doc fails loudly. Pure read paths do not type-check the
  pk — a doc whose whole-array key lies outside the tight seek range is by
  definition never reached by the scan, so read-side detection would need an
  offline sweep, not a per-row check. Documented as known-issues I-09: files
  containing array pks predate the ban and must be recreated (per the
  no-alpha-back-compat decision).
- Check interaction with BUG-09 (non-default pk) test fixtures.
- Tests: each write path errors on array pk; object/string/number pks unaffected;
  existing suites green.

### 2. `db: gate the id-only Count fast path on exactly-representable filters`

- `query.go:563`: the `tx.Has` path may fire only when the pk field carries exactly
  ONE predicate and it is Eq or In — i.e. tighten `isIDOnlyFilterNode` to inspect
  the Key's INNER filter shape (mirror the "bounds exactly equal matches" criterion
  ba92bc7 used for indexCoversFilter, planner.go countFilterFieldPreds). A
  bound-shape gate (all fixed points) is NOT sufficient: `$in+$ne`, `$in+$gt`,
  `$in+$type` all produce pure point bounds from the first contributor and still
  over-count (verified, see NEW-A). Update the stale doc comment.
- Tests: `Count == len(Iter)` for `{id:{$gt,$lt}}`, `{id:{$gt}}`, `{id:{$ne}}`,
  `{id:{$in,$nin}}`, `{id:{$in,$gt}}`, `{id:{$in,$type}}`; plain Eq/`$in` still take
  the fast path (correctness + optionally a trace hook).

### 3. `qplanner: reverse multi-interval scans walk intervals in descending order`

- Repro test FIRST (NEW-B above), asserting correct descending order across
  intervals with and without Limit/Offset, plus CountUntil/limit-quota carry-over
  across interval boundaries.
- Fix: when `Reverse`, IndexIter iterates `bounds` from `len-1` down (mirror
  FullScanIter); audit `boundIdx` uses in Next/CountEntries/CountUntil, and check
  padBoundsForReverseTail (planner.go:1856) / transformReverseBounds
  (planner.go:1812) for ascending-iteration assumptions.
- Benchstat on existing index-scan benchmarks (expect noise-level change).

### 4. `qplanner: Count with Limit/Offset counts deduped docs (BUG-06 / I-07)`

- Pre-existing wrong-answer bug (BUG-06 in the catalog, I-07 in known-issues.md),
  folded in because commit 7's verification net (Count == len(Iter) over array
  datasets with Limit/Offset) cannot pass with it open, and the fix lives in the
  same Count dispatch commit 2 touches. Independent of the bounds work — placed
  with the other standalone wrong-answer fixes so the bounds commits land on a
  clean baseline.
- Mechanism (verified at HEAD): with Limit/Offset the plan root is a LimitIter,
  not a CountableIterator, so Count falls to the generic Next-loop + consumer
  DocDedup (query.go:609-632). LimitIter applies Offset/Limit to SOURCE ROWS
  (limit_iter.go:38-53); over a multikey index those are per-entry rows — the
  Iter chain dedups BEFORE LimitIter (dedup_iter.go:131), the CountOnly chain
  does not — so offset skips entry-rows (`.Offset(4).Count()` skips only 2
  distinct docs → 8 vs Iter's 6) and limit caps entry-rows that then collapse in
  DocDedup (`.Limit(3).Count()` → 2 vs Iter's 3).
- Fix: order dedup before the cutoff — either reuse the Iter chain shape (dedup
  stage upstream of LimitIter) for CountOnly plans carrying Limit/Offset, or make
  the generic count loop dedup-first (skip the first Offset DISTINCT docs, cap at
  Limit distinct docs). Decide at implementation after enumerating which chain
  shapes reach the generic loop; scalar chains keep their current behavior.
- Tests: the I-07 triggers as regressions (index `{x}`, docs d0..d9 `{x:[i,i+1]}`,
  `$in[0..10]`: `Limit(3).Count()==3`, `Offset(4).Count()==6`); offset+limit
  combined; scalar-index Limit/Offset counts unchanged; the CountEntries fast
  path unaffected (it never carries Limit/Offset); Count==len(Iter) property
  checks over array fixtures.
- Docs: mark I-07 fixed in known-issues.md; update the BUG-06 catalog entry.

### 5. `query: Bounds.Intersect + TightIndexBounds (tight channel, unused)`

- Reintroduce `Intersect`/`intersectPair`/`maxStartKey`/`minEndKey` (recover from
  `git show 6fca7dd`, re-review, keep out of the IndexBounds contract). Resurrect and
  extend `TestBounds_Intersect` (point∩point, point∩range, overlap, disjoint,
  multi-interval cross product, ±inf, equal-endpoint inclusivity, commutativity).
- Add `TightIndexBounds` + the `empty` flag as designed above. Handle both `And` and
  `*And`; document why `Or` delegates wide and why `empty` ≠ unsat (array
  semantics — cite the `{a:[6,1]}` counterexample in the doc comment).
- Tests: `$gt+$lt` → single closed interval; `$in`∩range → filtered point set;
  `$ne`∩range → split intervals; contradiction → `empty=true`; empty-array-doc
  intersection soundness (`{a:[]}` has a single whole-value entry, so match ⟹ entry
  in intersection — pin the query-level behavior; the flag half of this property is
  pinned in commit 7); Or-containing filters unchanged vs wide; nested `$and`
  forms; extend `TestIndexBounds_FilterOwnedBytesAreCapped` with intersected shapes
  (cap==len on every returned endpoint).
- Update the contract comment at filter.go:352-371 and
  docs/query-filter-contract.md to name the two channels. No behavior change yet.

### 6. `qplanner: estimation uses tight bounds; pk-only unsat short-circuit`

- `BoundsResult` gains per-field `Tight query.Bounds` (+ per-field empty flags),
  built once per query alongside the wide bounds (planner.go:1700-1759). A field
  whose tight intersection is empty keeps its WIDE bounds in both channels (design
  point 5). The pk path (query.go:557/711) computes both variants too.
- `interpolateRangeSel` (planner.go:928-946, the ONLY bounds→fraction path via btree
  `Cursor.RangeFraction`) consumes the tight bounds — **routed through the same
  per-index transform as the wide chain, including transformReverseBounds for
  reverse-flagged (descending) indexes** (planner.go:1918-1928): RangeFraction ranks
  stored keys, and ascending-space bounds against a bit-inverted keyspace clamp to
  ~0 and silently fall back to the 0.5 default — the fix would be inert exactly on
  descending indexes. The `<0.5` adoption ratchet now receives ~0.005 for the repro.
- `calculateSelectivity` pTotal: for a field with a finite tight range, use the
  interpolated fraction instead of hardcoded `DefaultRangeSelectivity=0.5`
  (cost.go:24). This also moves the explain `Selectivity:` header, the full-scan
  LIMIT shortcut (planner.go:323-332), and the sort-materialize term — run the full
  planner benchmark suite with benchstat and re-pin explain expectations
  deliberately. Pin the usedFields dedup (planner.go:804) behavior when both asc and
  desc indexes cover one field. Scan-cost pricing keeps using the bounds the chain
  will actually seek with (design point 3) — with seeks still wide in this commit,
  that means wide.
- Unsat: extend the `isUnsatisfiable` pre-planner hook with tight-empty **on the
  primary-key field ONLY** (depends on commit 1; hook sites: Iter 138 / Update 262 /
  Delete 410 / Count 551). Explain intentionally bypasses the hook — pin that too.
- Tests: explain-level plan-choice assertions (cbo_range_interpolation_test.go
  pattern) incl. a two-sided range on a reverse-flagged index; Selectivity header
  assertion; pk contradiction → 0 rows on Find/Count/Update/Delete; **array-field
  contradiction must-match pin: `{a:{$gt:5,$lt:3}}` over `{a:[6,1]}` returns 1 row
  on all four verbs** (also add an empty-intersection variant to
  TestQueryCount_ArrayTwoSidedRange); NO seek behavior change yet (bounds strings
  in explain unchanged this commit).

### 7. `db,qplanner: persisted multikey flag; tight seeks for pk and scalar indexes`

- New systemNS record (own key, `mk:<coll>:<index>` following the
  `idx:`/`stat_data:` naming scheme; NOT inside the sketch blob — the flag is
  answer-determining, sketches are advisory-only per I-03). Semantics, check-and-put
  write discipline, creation marker, and lifecycle (rename moves it, dropIndex and
  removeCollection delete it) exactly as in design point 4. While touching
  renameCollection, verify/fix the `stat_data:` leak (catalog entry above).
- Plan-time channel selection per the invariants: both complete variants per
  candidate index — bounds plus EVERYTHING derived from them (PointLookup,
  equalityPrefix, ExactSort/shouldReverse, SortMatchStart, dedupBounds,
  AdjustBoundsForNonUnique, reverse-tail padding — invariant 1's full list), flag
  probe on the executing read tx, one variant selected before costing;
  CBO candidate finalization moves inside the read tx on all paths (Count path
  restructure, query.go:579-584). Verify chains read the wide channel (invariant 2).
  Pk `idBounds`: tight unconditionally (commit 1 guarantees no fan-out going
  forward; pre-ban array-pk files error loudly per commit 1).
- Sparse note: sparse-index admission (sparseIndexComplete/GuaranteesPresence,
  planner.go:956) is bounds-shape-independent, so tight bounds cannot newly admit a
  sparse index; a sparse index can hold zero entries for array-bearing docs without
  flipping the flag, which is exactly why absence-of-flag ⇒ multikey and why sparse
  admission stays orthogonal.
- `indexCoversFilter`'s multi-predicate gate stays UNCHANGED (conservative, sound;
  relaxing it for scalar-proven indexes is Deferred).
- Tests:
  - array regression suite green: an index that ever saw an array serves wide
    bounds — extend `TestQueryCount_ArrayTwoSidedRange` to assert explain still
    shows the half-open bounds on the flagged index;
  - flag lifecycle: creation → scalar; first array write flips within same tx
    (visible to a snapshot that sees the entries, invisible to an older snapshot);
    creating an index on a collection that ALREADY contains array docs → backfill
    flips to multikey (the marker-then-backfill re-flip branch); `{a:[]}` docs do
    NOT flip the flag; delete-all-arrays does NOT clear; drop+recreate resets
    (single-process; the cross-process root-page-reuse hazard from design point 4
    is moot under check-and-put — no in-memory state to go stale); **nested
    savepoint rollback inside a committing outer tx** leaves the record consistent
    with the entries (the check-and-put discipline's reachable hazard), plus
    top-level rollback; rename A→B (insert arrays) →A serves wide bounds;
  - absent record (hand-deleted, simulating alpha files) ⇒ wide;
  - cross-index verify: `{a:1, b:{$gte:3,$lte:3}}` CountOnly with scalar-proven
    index on `a` and multikey index on `b`, doc `{a:1,b:[2,4]}` → count 1;
  - unique multikey index + tight point `{$gte:5,$lte:5}` + doc `{a:[4,6]}` →
    doc yielded (CoverIter/PointLookup variant-consistency);
  - compound multikey index (a,b) + `{a:{$gte:5,$lte:5}}` + Sort("b").Limit(n) →
    correct order (ExactSort variant-consistency);
  - explain regression: `bounds=Bounds{('lo','hi')}` / `idBounds=...` two-sided on
    scalar index and pk (index_query_test.go:2673 exact-equality pattern);
  - differential test: for random scalar and array datasets, Find/Count/Sort±/
    Limit/Offset/Update/Delete results identical with the feature on vs.
    wide-forced — with the pk unsat hook force-disabled on BOTH sides so it cannot
    mask divergence — plus Count == len(Iter) cross-checks on every combination
    (these depend on commit 4: BUG-06/I-07 makes them fail today for Limit/Offset
    over multikey indexes);
  - descending repro benchmark: new high-cardinality fixture (setupBenchCollection's
    `a=i%100` cannot express it), 200k docs, `Sort("-id").Limit(n)` near-bottom
    range; assert plan via explain and O(page) via benchstat (17ms → ~100µs class),
    forward no-limit overrun likewise;
  - cross-process: writer flips flag, reader process (fresh open, own read tx) sees
    it — reuse the multiprocess/debugtrace harness.
- Docs: update known-issues.md I-04 (fixed-by note), BUG-02 catalog entry, this
  plan's status.

### 8. (Deferred, explicitly out of scope)

- Plan-time unsat for non-pk fields gated on a non-sparse scalar-proven index
  covering the field (non-sparse ⇒ every doc has entries; scalar-proven ⇒ semantics
  coincide). Unindexed fields can never be proven — no unsat for them, ever.
- Tightening inside `Or` branches (union of per-branch intersections) and fixing
  Or's length-delta contribution quirk.
- Relaxing the CountOnly `indexCoversFilter` gate for scalar-proven indexes.
- Per-field multikey bitmap for compound indexes (coarse per-index bit first;
  upgrade the record format later if warranted).
- Backfill/probe for alpha-era indexes (drop+recreate instead).

## Verification strategy

- Every commit: full test suite + the I-04 pin set + the new array-contradiction
  must-match pin.
- Commits 6-7: benchstat against the planner/scan benchmark suite (no-regression
  rule for hot paths), new descending benchmark, explain-string regressions.
- Commit 7: differential (feature-on vs wide-forced, unsat disabled both sides,
  Count==Iter cross-checks) randomized suite as the main soundness net, mirroring
  the differential review that caught the ba92bc7 regression.
