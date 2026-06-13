# Compound-index leading-column selectivity — review findings & decision

Status: **PARKED.** Branch `qplanner-indexed-equality-distinct`. This documents a prototype
that should **not** be merged, and why.

## TL;DR

- We prototyped a heuristic to fix the planner's blind 50% selectivity for an equality on the
  **leading column of a compound index** (which made `count()` / no-sort queries pick a FullScan,
  ~300–400× slower on a real 823k-doc store). Two variants: `1/distinct` and `distinct^(-1/k)`.
- An adversarial review found the heuristic is **race-clean** but has **two real HIGH regressions**,
  reproduced end-to-end. The "never worse than 0.5" safety claim is false.
- A composite-keyed sketch **cannot** recover the leading column's cardinality; a saturation gate
  to fix the regression was **proven to re-break the real target** (`changes(t,o)` is a unique
  index, so its composite is saturated identically to the regression case).
- **Reconciliation:** the real tree-build query (`Find(filter).Sort(OrderKey)`) **has a sort the
  index covers**, so on the unpatched baseline it **already picks IndexSeek** — it was never hit by
  the FullScan trap. The heuristic only changes **non-sort** queries, which is exactly its unsafe
  surface. So it gives the real workload nothing.

## What we built

`indexedEqualitySelectivity(idx)` = `clamp( DistinctEstimate(sketch)^(-1/k), 1e-4, 0.5 )`,
k = #index columns, wired into the three selectivity gates (`calculateSelectivity`,
`selectivityForIndex`, `estimateIndexDocsWithFieldSel`) for a covered leading equality, plus
`fieldBoundsLookup` so `{a==X, b>=Y}` is handled per-field. `DistinctEstimate()` is an
occupancy/coupon-collector estimate capped at docCount.

The root exponent is the principled form: `distinct(composite) ≈ ∏ distinct(col_i)`, so
`distinct^(-1/k)` estimates one column's average selectivity. On the real DB it lands est_rows for
`{t}` at 908 (true average tree ≈ 968) vs the floored `1/distinct` value of 82.

## Adversarial review results

| | finding | status |
|---|---|---|
| Races | `DistinctEstimate` all-buckets atomic loop | clean — `go test -race ./internal/qplanner/` passes, no new race |
| **HIGH #1** | low-cardinality leading col (boolean `flag` + unique trailing): composite saturates → `distinct^(-1/k)` collapses ≪ 0.5 → flips a correct FullScan to a ~1.6× (warm; worse cold) IndexScan | reproduced |
| **HIGH #2** | array/multikey leading col (common tag on every doc): per-element Increment vs per-doc docCount saturates the sketch → same flip, ≈6× | reproduced |
| safety claim | "0.5 upper clamp ⇒ no previously-correct plan regresses" | **false** (clamping the *estimate* ≤0.5 says nothing about the *true* selectivity) |
| MED #3 | the `{a==X,b>=Y}`+Sort test passes on baseline (exercises sort-coverage, not the change — not a regression guard) | valid |
| LOW #4/#5 | leading `$in` arity under-count; `selectivityForIndex` vs `calculateSelectivity` divergence on `{a==X,b>=Y}` | valid, minor |

## The fundamental limitation

The reviewer's proposed fix — "only apply when the sketch is **not** saturated (distinct ≪ docCount)"
— does not work. `changes(t,o)` is a **unique** index, so distinct(composite) = docCount = 823,701,
i.e. fully saturated — byte-for-byte indistinguishable from the boolean-flag regression case (also a
unique trailing column → also saturated). **Proven empirically:** injecting that gate makes the real
`{t}` query revert to `Plan: FullScan, Selectivity 0.50 (411850 of 823701)` — the original trap.

The distinguishing quantity is `distinct(leading)` (t: 851 vs flag: 2), which is **not recoverable**
from a sketch keyed on the full tuple. Both `1/distinct` and `distinct^(-1/k)` silently assume
balanced per-column cardinality; when the leading column is far less selective than `√composite`
they badly under-estimate, and no composite-derived gate can detect that. The only safe fix is a
**separate leading-prefix statistic** (`1/distinct(leading)` → flag 0.5 / t 1.3%, both correct),
which requires the persisted-sketch + copy-on-write + migration lifecycle.

## The reconciliation that parks it

The real tree-build queries are `s.changesColl.Find(filter).Sort(OrderKey)`
(any-sync `commonspace/object/tree/objecttree/storage.go:239` and `:264`). FullScan cost includes
`sortCost(M) = M·log₂M·0.25`; an `ExactSort`-covering index pays **zero** sort cost. So for a
filter+sort query the index wins **regardless of the cardinality estimate**.

Verified on the real 823k-doc DB, **unpatched baseline**:

| query | baseline plan |
|---|---|
| `{t, o>=} ORDER BY o` (the real tree-build query) | **IndexSeek** (720k vs FullScan 1.4M) |
| `{t}` (pure filter, no sort) | FullScan (trap) |

The FullScan trap only appears on **non-sort / count** variants (`{t}.count()`, `{t,o>=}.count()`),
not the tree-build query. The heuristic's entire behavioral footprint is those non-sort queries —
and that is exactly where it both helps (selective leading) and regresses (low-cardinality leading).
It does not touch the real ordered workload.

(Thin caveat: the real query wins by only ~2× because cardinality is *over*-estimated, 205,925 vs
true 10,981; safe for trees ≤1.5% of the collection. An *accurate* estimate would harden that
margin — the only real value the cardinality work would add, not worth the regression risk.)

## Decision & next steps

- **Park** this branch (kept for the record; do **not** merge — known HIGH regressions).
- The genuine tree-build / space-load lever is `FetchIter`: the per-row point lookup re-descends the
  data btree from root in OrderKey order (random, ~164 µs/row vs SQLite's hot-cursor ~12 µs/row).
  Fixing it needs data-key-ordered fetch or cursor reuse — a separate, larger change.
- If a **non-sorted hot query** on a compound index ever shows up (e.g. an unread-`count()` per chat),
  fix it with a **covering count** (count index entries in range, no doc fetch — cardinality-robust)
  or the leading-prefix sketch — not the composite-distinct heuristic.

(Companion analysis lives in anytype-heart: `docs/AnyStoreV2-TreeBuild-Findings.md` — the original
profiling — and `docs/research-compound-index-cardinality.md` — a solution-free research brief.)
