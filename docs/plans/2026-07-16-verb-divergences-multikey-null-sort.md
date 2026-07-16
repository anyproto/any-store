# Verb-divergence endgame grooming: compound-multikey dedup, array sort keys, $in-with-null

**STATUS: IMPLEMENTED (2026-07-16, this branch).** Increment 1 = `c307d29`, Increment 2 = `cbdf9ae` (+ a CoverIter finding below), Increment 3 = `193bfd0`. Acceptance reached: `DF_STRICT=1` full storetest green, `dfKnownOpen` EMPTY, all 8 repros + 4-seed soak green, benchstat within noise on every group. One addition beyond this plan, found during implementation: the multi-bound `CoverIter` path (unique multikey index + full-key `$in`) had the same dedup-above-cutoffs shape and got the same `DocDedupIter` wrap. One refinement: the order-providing gate is direction-aware (a lower cut only breaks ascending/min, an upper cut only breaks descending/max), which keeps lower-bounded descending scans order-providing — two pre-existing tests pin that shape and the matching-doc argument (max ≥ bound ⇒ max in bounds) makes it sound.

**Repo:** `github.com/anyproto/any-store/v2`, branch `btree` @ `f0fc0e9`
**Worktree (read/edit code HERE):** `/home/che/projects/any-store/.claude/worktrees/verb-divergences-groom`
**Companion test repo:** `/home/che/projects/any-store-tests` (module `any-store-tests`, go.work → `../any-store`)
**Bug docs:** `BUG-24-compound-multikey-dedup-above-count-limit.md` (covers BUG-25), `BUG-27-in-null-misses-missing-field.md`, `BUG-28-array-sort-plan-dependent-order.md` — all in the test repo
**Plan context:** Phase 2 of `~/.claude/plans/structured-stirring-map.md` (the compilePlan seam from Phase 1 is merged; consumers key off the `multiKey` flag, so everything below is plan-level — **no verb changes anywhere in this document**)

**Goal (acceptance):** `DF_STRICT=1 go test ./storetest/ -run TestDifferential` green in the test repo with the `dfKnownOpen` suppression table (differential_fuzz_test.go:1067) **emptied**, plus the eight hand repros green: `TestBug24_CompoundIndexArrayFanoutCount`, `TestBug25_CompoundMultikey{Limit,Offset,SortTopK,DeleteLimit,UpdateLimit}`, `TestBug27_InNullMissingField`, `TestBug28_ArraySortPlanDependentOrder`.

**Measured baseline (this worktree, @ f0fc0e9):** all 8 repros fail; the fuzz suppresses **84** divergences in query-consistency (BUG-24/25 = 42, BUG-27 = 42) + **2** in write-consistency (BUG-24/25). BUG-28's fuzz entry fires zero times — dead by generator design (the `id` tiebreaker forces a SortIter above every index; documented at differential_fuzz_test.go:1128-1137), so its closure is gated on the hand repro, not the fuzz.

**Semantics oracle:** MongoDB, **not** sqlitec — the SQLite-alignment directive covers btree/storage decisions, not query-language semantics. No DRIFT entries needed. Convention reminders: no BUG-NN identifiers in any-store code or test names (descriptive names; commit messages may cite the catalog); benchstat before/after on every hot-path commit (Limit/dedup/Count sit on every query).

Every empirical claim below was probed on this worktree (throwaway `storetest/probe_groom_test.go`, deleted after).

---

## Decision summary

| # | question | decision |
|---|---|---|
| 1 | BUG-24/25 mechanism | new **`DocDedupIter`** (streaming, wraps the existing `DocDedup`) inserted **in-plan** for compound indexes, so dedup runs **below** SortIter/LimitIter. Emits `multiKey=false`; keep-first-occurrence. Consumers unchanged — their `DocDedup.Accept` degenerates to a free passthrough. |
| 2 | where in the chain | **between IndexFilterIter and FetchIter** — must be *above* IndexFilterIter (entry-level cover filters give per-entry verdicts; deduping first would drop docs), may be *below* FetchIter (the residual FilterIter's verdict is per-doc, identical across a doc's entries). Below-Fetch placement also skips fetching duplicate entries — a win, not a cost. |
| 3 | insertion gate | **unconditionally for compound indexes** (`len(FieldPaths) > 1`); single-field keeps `CanonicalKeyDedupIter`. Scalar streams cost one interface call + a two-instruction `Accept` per row, no map. Gating on `isScalarProven` is a follow-up **only if** benchstat shows the wrapper (decision 10). |
| 4 | Count covering path | `IndexIter.CountEntries` Branch 1 (page-batch) is **unsound for compound prefix bounds** (BUG-24: Count=4 for 1 doc — 3 element entries + the whole-array entry). Gate: page-batch only when `single-field ∨ full-key bound (BoundFields == len(FieldNames)) ∨ scalar-proven`; otherwise the existing skip-scalar seen-set walk (Branch 2). |
| 5 | BUG-28 semantics | **Mongo ≥4.4 / SERVER-19402**: an array's sort key is its **minimum element ascending / maximum element descending**, computed **per sort field independently**, and **independent of the query predicate**. |
| 6 | empty array / missing | missing field ⇒ null (already consistent on both paths — nil `*Value` marshals to TypeNull). **Empty array keeps the whole-array key** on both paths (already consistent, probed). Documented divergence from Mongo, which sorts `[]` *before* null — matching that needs an on-disk key change for no user value. |
| 7 | which key builders change | `SortField.AppendKey` (query/sort.go:167) **and** `SortField.AppendKeyRaw` (query/sort_raw.go:25) together — the raw path currently *handles* leaf arrays (appends the whole array) and `TestSortAppendKeyRawParity` pins them equal. Aggregation `$sort` (internal/aggregate/stage_sort.go:98) inherits for free — same interface. |
| 8 | order-providing gate | when the chosen index is **not scalar-proven**, `ExactSort` is kept only if the sort-matched run carries **no bounds**, and — for **compound** indexes — every matched sort field is **ascending-requested**. Single-field survives both directions (`CanonicalKeyDedupIter` picks the per-direction canonical element); compound reverse is broken by the **whole-array entry**, which a reverse scan meets first (probed, §BUG-28-D3). Mirrors Mongo's rule ("multikey index provides a sort only when the sort-field bounds are [MinKey, MaxKey]"). Uses the **existing** `isScalarProven(tx)` snapshot-exact flag (index.go:559) at the spot ExactSort is computed (query.go:1164). |
| 9 | BUG-27 policy | Mongo: **`$in` containing null matches missing fields.** `In.Ok(nil)` returns `true` iff `encodedNull ∈ Values`. Everything else in the null model is **already consistent** and stays untouched: `$eq null` matches missing (Comp encodedNull path), `$ne null` / `$nin:[null]` exclude missing (Nor-of-Comp), sparse selection auto-follows (GuaranteesPresence probes `Ok(nil)`), raw path auto-follows (`Key.OkRaw` → `Ok(nil)`). |
| 10 | increments | **3 code increments + 1 test-repo closure.** 1 = `$in`-null (S). 2 = in-plan compound dedup + CountEntries gate (M). 3 = array sort keys + order-providing gate, **one increment, not two** — shipping the key change without the gate *creates* divergences that don't exist today (§BUG-28-D3). 4 = empty `dfKnownOpen`, DF_STRICT green, doc statuses. |

---

## Root cause — three independent defects, one invariant

The Phase-1 seam made all verbs consume one plan, so each remaining divergence is a *plan-construction* defect, visible on every verb at once:

| # | defect | file:line | symptom |
|---|---|---|---|
| **D-dedup** | compound multikey fan-out is deduped at the **consumer** (iterator.go:74, dedup.go:68), *above* the in-plan LimitIter (planner.go:664) and SortIter TopK (planner.go:1420); single-field indexes got an in-plan fix (`CanonicalKeyDedupIter`, planner.go:1410/1495) that is explicitly gated `len(FieldPaths)==1` | limit/offset/TopK slots are consumed by raw index entries; `Count` (via `CountDistinct`, limit_iter.go:71, which *does* dedup before the cutoff) disagrees with every other verb; `CountEntries` Branch 1 (index_iter.go:384) page-batches entries incl. the whole-array entry | all six `TestBug24/25` repros; measured `Count=4, Iter=1` for one doc |
| **D-sortkey** | two definitions of an array's sort key: index scans emit a doc at its canonical **element** (min forward / max reverse — dedup_iter.go), SortIter/agg-$sort build the key from the **whole-array encoding** (query/sort.go:167-173), which sorts by type tag after every scalar | result order and `Limit` membership depend on plan choice | `TestBug28` (asc), plus the desc shape probed below |
| **D-innull** | `In.Ok(nil)` is unconditionally `false` (query/filter.go:509-512) while `In.IndexBounds` (query/filter.go:553) emits a point bound for the null member and index.go:665 stores a missing field under TypeNull | covering Count includes missing-field docs, Iter/Update/Delete exclude them; `{"$in":[null]}` ≠ `{"$eq":null}` | `TestBug27`; 42 fuzz suppressions |

Two structural facts discovered while grooming that the bug docs don't record — both load-bearing for the design:

1. **`writeValues` stores the whole array as an entry alongside its elements** (index.go:662-668 runs unconditionally after the element loop). This is why BUG-24's Count is 4, not 3, and why a *reverse* scan meets the whole-array entry before any element (the array type tag sorts above every scalar).
2. **A sticky, snapshot-exact index-level multikey flag already exists**: `markMultiKey` (index.go:542) is written in the same tx as the first fan-out entry, `isScalarProven(tx)` (index.go:559) reads it through the query's own read tx (correct across processes — matches the multi-process contract), and `buildCBOIndexesInto` (query.go:1100) already consults it for tight-bounds gating. The order-providing gate and the CountEntries gate get exactness for free; no Mongo-style per-field multikeyPaths needed yet.

---

## BUG-24/25 — in-plan dedup below Limit/Sort

### Semantics

For any plan, the row stream that reaches SortIter/LimitIter/consumers must be **one row per distinct matching document**. Offset skips distinct docs, Limit counts distinct docs, TopK ranks distinct docs, `modified` counts distinct docs. `Count == len(Iter)` at every offset — the fuzz's `Count >= len(Iter)` hard floor (differential_fuzz_test.go:1204-1247) becomes a theorem at **any** offset, not just offset 0.

### Mechanism

New `internal/qplanner/dedup_iter.go` sibling:

```go
// DocDedupIter collapses a compound multikey index's fan-out to one row per
// distinct document, in first-occurrence order, so downstream Sort/Limit/Offset
// stages and consumers operate on documents rather than index entries. Entries
// flagged multiKey are deduped via DocDedup (lazy map); scalar entries pass
// through untouched, so a scalar stream allocates nothing. Emits multiKey=false:
// downstream DocDedup consumers (planIterator.Next, ForEachDistinct,
// LimitIter.CountDistinct) then skip their seen-sets entirely.
type DocDedupIter struct {
    Source Iterator
    dedup  DocDedup
}
```

- `Next()`: pull, `if !dedup.Accept(docId, mk) { continue }`, emit `(key, docId, false)`.
- `skipOffset(n)`: delegate to source, exactly like `CanonicalKeyDedupIter.skipOffset` (dedup_iter.go:133) and for the same reason — the cursor-level skip only ever skips **scalar-flagged** entries (limit_iter.go:66-70), and a scalar entry is *by construction* its doc's only entry (index.go:460-467), so no dedup decision is bypassed and no seen-set recording is missed.
- `Close()`: close source. `String()`: `"%s -> Dedup(docid)"`.
- **`setPlanRef` (planner.go:1544) gets a `*DocDedupIter` arm** — recurse into Source (no Plan field needed; this iterator never touches the doc).

Insertion, in **both** `buildIndexSeekChain` and `buildIndexScanChain`, directly above the entry source:

```
IndexIter -> [IndexFilterIter] -> [DocDedupIter]  <- compound only, here
          -> FetchIter -> [FilterIter]
          -> [CanonicalKeyDedupIter]              <- single-field only, unchanged
          -> [SortIter] ... -> LimitIter (BuildPlan root)
```

Position argument (verified against the iterator contracts):
- **Above `IndexFilterIter`** is mandatory: cover filters test the *entry key tuple* (planner.go:1464-1469), and a doc's entries differ in covered fields — dedup-first would emit only the doc's first entry, which may fail the cover filter while a later entry passes → lost doc.
- **Below `FetchIter`** is safe and profitable: the residual `FilterIter` evaluates the fetched *document*, so the verdict is identical for every entry of the doc; deduping before the fetch skips N−1 fetch+parse+filter rounds per duplicated doc.
- Keep-first order is exactly what the consumer-side dedup produces today, so unsorted result order is unchanged.

### Why the consumer-side dedup stays

`planIterator.Next` / `ForEachDistinct` / `CountDistinct` keep their `DocDedup` — it is the documented twin-contract backstop (dedup.go:63-67), it still serves plans with no DocDedupIter (single-field CanonicalKeyDedupIter passes through docs it cannot dedup when `Plan.DocParsed` is unavailable, dedup_iter.go:73-75), and with `multiKey=false` flowing downstream it costs two instructions per row. Update the `SeenSetDedupIter` history note at dedup_iter.go:150-155: the 2026-04-29 decision moved dedup to the consumer *boundary*; this pass moves it back **in-plan but below the cutoffs**, which is the part the boundary design got wrong (docs/plans/2026-04-29-multikey-bit-and-dedup-pipeline.md should get a superseded-by pointer).

### Count covering path

`CountEntries` dispatch (index_iter.go:378-403) — Branch 1 today is `len(Bounds) <= 1 → page-batch`. Its stated justification ("within-doc dedup guarantees ≤1 entry per distinct value per doc") holds for a **full-key** bound but not for a compound **prefix** bound, which fans out across the array suffix *plus* the whole-array entry. New Branch 1 guard:

```go
if len(it.Bounds) <= 1 &&
    (len(it.IdxInfo.FieldNames) == 1 || it.boundsCoverFullKey() || it.ScalarProven) {
    return it.countEntriesBatch()
}
```

- `boundsCoverFullKey` = the plan's `BoundFields == len(FieldNames)` (pass it in from the CBOIndex when BuildPlan constructs the IndexIter; a full-key equality bound admits ≤1 entry per doc — per-field `isUnique` dedup in writeValues makes tuples distinct within a doc).
- `ScalarProven` comes from the chosen CBOIndex (below). Not-proven compound prefix counts route to the existing `countEntriesViaSeenSet(true)` skip-scalar walk (Branch 2) — scalar entries stream without map touches, so a *actually-scalar-but-unproven* index pays one value-byte read per entry, no allocations.
- Branches 3/4 (single-field probe) unchanged.

### Plumbing the proof

`CBOIndex` gains `ScalarProven bool`, set in `buildCBOIndex`/`buildCBOIndexesInto` (query.go:1089-1110) where `tx` and `idx.isScalarProven` already meet. Read it **lazily** — only for candidates that need it (CountOnly compound; ExactSort gate in §BUG-28) — each read is one systemNS point Get on the query's own read tx; don't add 8 unconditional Gets to every plan. A *negative* in-memory cache on the live `*index` object ("multikey seen at least once") is safe if ever needed (one-way flag; a positive scalar cache is NOT safe across processes — see the markMultiKey comment at index.go:530-541).

### What deliberately does NOT change

- No canonical-key selection for compound tuples — that is the deferred O(1)-memory design (docs/plans/2026-04-17-multikey-index-dedup.md). `DocDedupIter` is O(distinct docs *pulled*): bounded by Limit for bounded queries, and for unbounded queries it is the exact map the consumer allocates today, moved down. Net allocation is ≤ today's.
- No verb code. Update/Delete's `collectDistinctIDs` (query.go:480/628), `countPlanRoot`'s 3-way dispatch (query.go:736-753), and `planIterator.Next` are already correct against a deduped stream — that is the Phase-1 seam paying off.
- `LimitIter.CountDistinct` stays as-is: it is the twin-contract implementation for any plan without in-plan dedup, and on a deduped (or inherently scalar) stream its `DocDedup` is pure passthrough.

---

## BUG-28 — one array sort key, all plans

### Semantics (Mongo ≥4.4, SERVER-19402)

For each sort field independently: if the value is a non-empty array, the sort key is the **minimum element** for an ascending field and the **maximum element** for a descending field, chosen from **all** elements — the query predicate does not participate (Mongo changed exactly this in SERVER-19402 because predicate-dependent sort keys were incoherent; do not resurrect them via the index's in-bounds canonical element). Cross-type comparison stays anyenc tag order (that is any-store's collation, globally). Missing field ⇒ TypeNull (unchanged). Empty array ⇒ whole-array encoding (unchanged, internally consistent, **documented divergence** from Mongo's `[] < null`).

### The empirical matrix (probed @ f0fc0e9)

| shape | plain (SortIter) today | index today | agree today? | after AppendKey fix only | after fix + gate |
|---|---|---|---|---|---|
| single-field asc, no bounds (`TestBug28`) | whole-array key: `[d b c a]` | min element: `[d a c b]` | ✗ | ✓ (both min) | ✓ |
| single-field **desc**, two arrays | whole-array: `[e a b]` | **max element**: `[a e b]` | ✗ | ✓ (both max) | ✓ |
| single-field asc, **bounds on sort field** (`{"x":{"$gt":0}}`, doc `x:[5,-1]`) | `[b a]` (whole-array) | `[b a]` (in-bounds min = 5) | ✓ (coincidence) | **✗ NEW divergence** (global min −1 → `[a b]` vs in-bounds `[b a]`) | ✓ (gate forces SortIter) |
| compound `(a,x)` asc, equality prefix | whole-array: `[d3 d1 d2]` | min element: `[d1 d2 d3]` | ✗ | ✓ (keep-first = min combo; cartesian fan-out makes every `(5, x_j)` entry exist, so a bounded *prefix* — even a multikey one — preserves min-x order) | ✓ |
| compound `(a,x)` **desc** | whole-array: `[d2 d1 d3]` | whole-array-entry-first: `[d2 d1 d3]` | ✓ (coincidence) | **✗ NEW divergence** (SortIter → max element `[d1 d2 d3]`; reverse scan still meets `x:[8,2]`'s whole-array entry before `x:[1,9]`'s max element 9) | ✓ (gate) |
| empty/missing/null/object | `[m n s e o]` | `[m n s e o]` | ✓ | ✓ (empty-array fallback keeps whole-array key) | ✓ |

The two ✗-after-fix rows are why decision 10 makes the key change and the gate **one increment**: the key change alone converts two accidental agreements into live divergences on shapes the current repros don't cover.

### D1 — the key builders

`SortField.AppendKey` (query/sort.go:167) and `SortField.AppendKeyRaw` (query/sort_raw.go:25-45) route through one shared helper:

```go
// appendSortElement appends v's sort key under Mongo array semantics: a
// non-empty array contributes its minimum element ascending / maximum element
// descending; an empty array contributes its own encoding (any-store keeps the
// whole-array key here — the index stores an empty array only under that key);
// nil/missing contributes TypeNull via the nil marshal.
func (s *SortField) appendSortElement(tuple anyenc.Tuple, v *anyenc.Value) anyenc.Tuple
```

- Selection: marshal each element into a scratch and keep the byte-wise min (asc) / max (desc) — the same comparison space `CanonicalKeyDedupIter` uses (dedup_iter.go:84-104), so the two definitions cannot drift; then append plain or inverted per `s.Reverse`. Elements that are themselves arrays/objects participate by their encodings (nested-array min matches the index's element entry — probed consistent).
- **No `sort.Slice` / no per-element allocations** — a running-best scratch on the SortField or DocBuffer, mirroring `CanonicalKeyDedupIter.best`. Note `SortField.AppendKey` currently has a value receiver in an interface via `*SortField` — keep the pointer receiver so the scratch persists.
- `AppendKeyRaw`: `RawByPathChecked` already returns leaf arrays as handled (a raw array fragment parses fine); after parsing `fv`, route through the same helper. Do **not** take the lazy out of returning `handled=false` for arrays — the raw path is the unindexed-sort hot path and array sort fields would permanently fall back to full parses. `TestSortAppendKeyRawParity` (query/filter_raw_test.go:138) enforces byte equality either way; extend its corpus with array/empty-array/nested-array leaves.
- Aggregation `$sort` (stage_sort.go:98) and SortIter (sort_iter.go:148) both call `Sorter.AppendKey` — no changes there. SortIter's raw fallback counter (sort_iter.go:53-58) becomes less relevant (arrays now handled raw) — leave it.

### D2 — the order-providing gate

Where ExactSort is computed (query.go:1151-1164, feeding `IndexSortMatch`, planner.go:2457):

```
exact := IndexSortMatch(...)
if exact && !scalarProven(idx) {          // lazy systemNS Get, chosen-candidates only
    run := FieldNames[matchStart : matchStart+len(sortFields)]
    if anyFieldHasBounds(br, run)         // predicate on a sort-run field
       || (compound && anySortFieldDescending(sortFields)) {
        exact = false                      // Plan C skips it; Plan B wraps SortIter
    }
}
```

- **Bounds on the run** (any index): the scan only visits in-bounds element entries, so the first-encountered element is the *in-bounds* min — pre-4.4 Mongo semantics, now wrong. Bounds on the **equality prefix** are fine, even when the prefix field is itself an array (cartesian fan-out — see matrix row 4). This is Mongo's own published rule: a multikey index provides a sort only when the sort fields' index bounds are `[MinKey, MaxKey]`.
- **Compound + any descending-requested sort field**: the traversal meets the whole-array entry before the max element (probed). Single-field is exempt — `CanonicalKeyDedupIter` picks the canonical element per direction and skips the whole-array entry.
- Scalar-proven indexes: zero behavior change, zero extra cost beyond at most one point Get per sorted query.
- Consequences to expect in reviews: CBO cost for gated candidates gains `sortCost` (planner.go:511-513), so some plans flip from IndexScan to IndexSeek+SortIter or FullScan+SortIter — Explain goldens shift on array-heavy fixtures only. `PartiallySorted` stays a pdqsort hint (correctness-neutral).

### Out of scope, filed as such

- **Recovering compound-descending order-providing scans** by teaching `DocDedupIter` to skip whole-array entries (it would need `Plan.DocParsed` + per-field tuple slicing — the deferred canonical-compound design). File as a perf follow-up in the test repo if it ever matters.
- **Array primary keys**: not representable — `newItem` (collection.go:381-394) rejects them with `ErrArrayPrimaryKey` on every write path (insert/update/upsert/backfill; pinned by collection_primary_key_test.go), so `Sort("id")`'s FullScanIter fast path (planner.go:1200-1223) can never disagree with the new SortIter key. Only caveat, already accepted policy: read paths don't re-check, so pre-ban alpha data could theoretically hold array ids — alpha back-compat is explicitly out of scope (pre-beta catalog decision). Verified 2026-07-16; nothing to do.

---

## BUG-27 — `$in` with null matches missing fields

### Policy

Mongo's null model, adopted wholesale (it is also the only self-consistent option given `$eq null` already matches missing): **`{"$in":[…, null, …]}` matches a document whose field is missing, explicit null, or an array containing null.** `$nin`/`$ne` complements exclude those docs.

### The fix — one arm, everything else already agrees

`In.Ok` (query/filter.go:509-512):

```go
if v == nil {
    _, ok := e.Values[string(encodedNull)]   // encodedNull: filter.go:76
    return ok
}
```

Precompute `hasNull bool` in `NewInValue` (filter.go:472-507, the span check is one byte) and fall back to the map lookup for hand-built `In{Values: …}` — mirroring the `sorted`/`numBits` authoritative-only-when-built pattern. Zero cost on the non-nil path.

### The audit table (this is the "system-wide null policy" the bug doc asked for — all verified in code, none need changes)

| surface | behavior with the fix | why it's already right |
|---|---|---|
| `In.IndexBounds` | null member already emits the TypeNull point bound | that's why Count=2 today (filter.go:553-585); missing fields are indexed under TypeNull (index.go:665) — Iter now agrees with Count instead of vice versa |
| `$eq null` / `$ne null` | match / exclude missing | `Comp.Ok(nil) → e.comp(encodedNull)` (filter.go:112-114) |
| `$nin:[null]` | excludes missing | parsed as `Nor(makeEqArray)` (cond_parse.go:725-726) → Comp path above |
| `Not{In(null)}` | excludes missing | `!Ok(nil)` — derived |
| whole-array/element probes | doc `x:[null]` matches | array branch already walks elements (filter.go:516-532) |
| raw fast path | in lockstep | `Key.OkRaw` missing-field arm calls `e.Filter.Ok(nil, buf)` (filter_raw.go:37); `TestFilterOkRawParity` pins it |
| sparse index selection | `$in`-with-null keeps sparse indexes excluded | `GuaranteesPresence` probes `Ok(presenceNullProbe)` which is already true for null∈set (filter.go:994-1009; its doc comment at :991 even names "$in containing null") — the fix makes `Ok(nil)` true as well, same verdict, now for two reasons |
| unsatisfiability / tight bounds | unaffected | `$in:[]` unsat path keys on empty Values; null adds a normal member |

### Cross-check against the fuzz census

All 42 BUG-27 suppressions are `kind=count!=iter` with `$in`-null bounds on the used index (including reverse-declared `{-a}` variants, where the bound is the *inverted* null tag — no special casing needed: `Ok` never sees stored bytes). Deleting the registry entry (differential_fuzz_test.go:1089-1108) after this fix is the acceptance check.

---

## Rejected alternatives

| alternative | why rejected |
|---|---|
| Consumer-side fixes (teach each verb about entry-vs-doc cutoffs) | five verbs × three cutoffs; the Phase-1 seam exists precisely so selection semantics live in the plan. |
| Canonical-key dedup for compound tuples (extend CanonicalKeyDedupIter past `FieldPaths==1`) | the 2026-04-17 doc already found per-tuple canonical selection non-trivial (in-bounds combos across fields); `DocDedupIter`'s seen-set is O(docs pulled), bounded by Limit for every bounded query, and equals today's consumer map otherwise. Revisit only if a real workload shows unbounded-scan map pressure. |
| DocDedupIter above FetchIter/FilterIter (mirror CanonicalKeyDedupIter's slot) | correct but strictly worse: pays fetch+parse+residual-filter for every duplicate entry. Above-IndexFilterIter is the only hard constraint. |
| Gating DocDedupIter on `isScalarProven` from day one | adds a systemNS Get to every compound-index query to save a two-instruction passthrough; measure first (benchstat gate in Increment 2), gate later if the wrapper shows up. |
| Predicate-dependent array sort keys (in-bounds min — keep index behavior, change SortIter to match) | pre-4.4 Mongo semantics, abandoned upstream for incoherence (SERVER-19402); would also need per-field bound extraction inside SortIter for arbitrary filters, and makes sort order change when the filter changes. |
| Mongo-exact empty-array ordering (`[] < null`) | needs a new key encoding below TypeNull on disk plus reindex; buys agreement with Mongo on a corner nobody filed. Both plans already agree internally (probed). Document in `docs/query-filter-contract.md`. |
| Per-field multikey metadata (Mongo multikeyPaths) to keep e.g. `(scalarField)` sorts order-providing on a mixed index | the index-level flag already exists, is snapshot-exact, and covers every filed shape; per-field paths are a schema change to the system NS. File as future perf work if gated plans show up in profiles. |
| Rejecting/erroring on array sort fields under bounds | silently-working queries would start erroring; Mongo serves them with a blocking sort — so do we (SortIter). |
| Fixing `In.Ok(nil)` via parse-time rewrite (`$in:[null]` → `Or{Comp(null), In(rest)}`) | fixes only parsed filters; both production consumers also build filters programmatically (the BUG-32 lesson — detection-time and hand-built paths must agree), and `In` is public API. |

---

## Implementation plan

`WT` = this worktree. **S** ≈ under an hour, **M** ≈ a few hours. Every increment: `go test ./...` green in any-store; repros + `go test ./storetest/ -run 'TestBug|TestDifferential' -count=1` in the test repo (go.work already points at the local checkout — coordinate with whatever branch `../any-store` has checked out, or run via a scratch `GOWORK` file pointing here); separable commit; no BUG-NN in code.

### INCREMENT 1 — `$in`-with-null (S)

- **T1.1** `WT/query/filter.go`: `In.Ok` nil arm + `hasNull` in `NewInValue` (+ map fallback for hand-built `In`).
- **T1.2** in-repo regression `WT/query/filter_test.go` (descriptive: `TestInNullMatchesMissingField`): `Ok(nil)` true/false by membership; hand-built `In` parity; `$nin:[null]`/`Not` complements; and a store-level test (query_test.go) pinning Count==Iter==2 for the bug-doc fixture on an indexed field.
- **T1.3** confirm no golden drift: `GuaranteesPresence` tests, `TestFilterOkRawParity`, sparse-index planner tests.
- **T1.4** test repo: `TestBug27_InNullMissingField` green; **delete the BUG-27 `dfKnownOpen` entry**; fuzz non-strict shows 0 BUG-27 suppressions.

### INCREMENT 2 — compound multikey dedup below the cutoffs (M)

- **T2.1** `WT/internal/qplanner/dedup_iter.go`: `DocDedupIter` (Next/skipOffset/Close/String as specced); update the `SeenSetDedupIter` history note; cross-reference comments with `DocDedup`/`ForEachDistinct`.
- **T2.2** `WT/internal/qplanner/planner.go`: insert in `buildIndexSeekChain` + `buildIndexScanChain` for `len(FieldPaths) > 1`, above `IndexFilterIter`/`IndexIter`, below `FetchIter`; add the `setPlanRef` arm. (CoverIter and verify-chain paths are unique/point shapes — no insertion.)
- **T2.3** `WT/internal/qplanner/index_iter.go`: Branch-1 guard (`single-field ∨ full-key bound ∨ ScalarProven`); plumb `BoundFields`-derived flag + `ScalarProven` from the chosen `CBOIndex` into `IndexIter`; `WT/query.go` `buildCBOIndex`: set `ScalarProven` lazily (CountOnly-compound candidates only, this increment).
- **T2.4** in-repo regression tests (`WT/query_test.go` / `internal/qplanner`), descriptive names: compound-array fixture — Count==Iter with prefix bound (covering path, both proven and unproven variants — force with a scalar-only compound index vs an array one); Limit/Offset/TopK/Delete-limit/Update-limit against a FullScan oracle; offsetSkipper fast-skip variant (no residual filter, offset over mixed scalar+multikey entries); Explain string pins `-> Dedup(docid)` placement.
- **T2.5** goldens: audit plan-string tests (`qplanner_reverse_index_test.go`, `planner_test.go`, `index_query_test.go` pin `Dedup(canonical)` today — compound fixtures gain `Dedup(docid)`).
- **T2.6** test repo: all six BUG-24/25 repros green; **delete the BUG-24/25 `dfKnownOpen` entry**; update the two hard-floor comments (the `Count >= len(Iter)` theorem now holds at any offset — keep the floors, widen the `Offset == 0` gate note, differential_fuzz_test.go:1204-1247).
- **T2.7** **benchstat** (hot path): test-repo benchmark tool, groups `compound,simple_index,sort,filter_sort` + the count/fullscan group, before/after. Watch: per-row interface-call overhead on scalar compound scans (T2.2) and compound covering-count walks vs page-batch (T2.3). If the wrapper is measurable → add the `ScalarProven` insertion gate (decision 3 fallback); if unproven-count walks are measurably slow on scalar data → nothing to do, the flag already rescues real scalar indexes (they're proven unless created pre-flag).

### INCREMENT 3 — array sort keys + order-providing gate, one commit (M)

- **T3.1** `WT/query/sort.go` + `sort_raw.go`: `appendSortElement` shared helper (min/max element, empty-array whole-key fallback, nil→null); pointer-receiver scratch; no reflection sorts, no per-element allocs.
- **T3.2** `WT/query/filter_raw_test.go`: extend `TestSortAppendKeyRawParity` corpus (array leaf, empty array, nested arrays, array-of-objects, missing, both directions).
- **T3.3** `WT/query.go` (:1151-1164): the ExactSort gate — run-field bounds check via `br.Lookup`, compound-descending check, lazy `isScalarProven`; keep `SortMatchStart` semantics intact (gate after `IndexSortMatch`, don't re-derive).
- **T3.4** in-repo tests: the six-row matrix from this doc as a table test (plain vs indexed collection, order + Limit membership, asc/desc, bounded, compound, empty/missing) + Explain assertions that gated shapes now show a SortIter/TopK and scalar-proven shapes don't.
- **T3.5** goldens: Explain candidate lists shift where ExactSort was gated (array fixtures only); audit `docs/vector-search.md`-style user docs? → `docs/query-filter-contract.md` gains the array-sort-key section (min/max rule, empty-array + cross-type divergences from Mongo).
- **T3.6** test repo: `TestBug28_ArraySortPlanDependentOrder` green; add desc/bounded/compound-desc variants next to it (the shapes only this grooming probed); **delete the BUG-28 `dfKnownOpen` entry** (it never fired — deleting it is documentation, the new hand tests are the enforcement).
- **T3.7** **benchstat**: `sort,filter_sort` groups (AppendKey/AppendKeyRaw are the unindexed-sort hot path; scalar values take one extra type check).

### INCREMENT 4 — closure (test repo, S)

- `dfKnownOpen` is now `[]` — leave the (empty) table + its doc comment as the mechanism for future bugs; run `DF_STRICT=1 go test ./storetest/ -run TestDifferential -count=1` and a long-seed soak (`ANYSTORE_FUZZ_SEED` sweep) — acceptance.
- Bug-doc status lines (BUG-24/25/27/28 → FIXED, commit SHAs); `docs/known-issues.md` I-07 note (the "Count dedups before cutoff / Iter after" asymmetry paragraph is obsolete — both dedup below the cutoff now); memory/plan updates (`structured-stirring-map.md` Phase 2 → done).

Ordering: 1 → 2 → 3 (3's TopK matrix rows on compound fixtures assume 2's dedup; 1 is independent but trivial — land it first so the fuzz census isolates the remaining mechanisms).

---

## Verification

- Per increment: `go test ./...` (any-store, this worktree); repros + fuzz non-strict census in the test repo (expect the increment's suppression count → 0); after Increment 3: `DF_STRICT=1` full green.
- Fuzz invocation against this worktree without touching the test repo's go.work: `GOWORK=<scratch>/groom.work go test ./storetest/ ...` with a `use` block pointing here (pattern used for this grooming's probes).
- benchstat: increments 2 and 3 as specced; keep pprof handy for any >2% regression on `compound`/`sort` groups.
- Explain/golden audits are enumerated per increment (T2.5, T3.5) — goldens change only where the old output described a wrong plan shape.

## Sources consulted for the oracle

- MongoDB sort-key semantics + the 4.4 predicate-independence change: [cursor.sort() sort consistency](https://www.mongodb.com/docs/manual/reference/method/cursor.sort/), [SERVER-19402](https://jira.mongodb.org/browse/SERVER-19402)
- Multikey order-providing rule: [Use Indexes to Sort Query Results](https://www.mongodb.com/docs/manual/tutorial/sort-results-with-indexes/), [Multikey Indexes](https://www.mongodb.com/docs/manual/core/indexes/index-types/index-multikey/)
- Null/missing matching and `[]` comparison order: [Query for Null or Missing Fields](https://www.mongodb.com/docs/manual/tutorial/query-for-null-fields/), [BSON type comparison order](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/)
