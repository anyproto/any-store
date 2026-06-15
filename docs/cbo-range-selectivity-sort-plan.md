# CBO tuning: range selectivity, sparse-index cuts, and exact-sort boost

Branch: `cbo-range-selectivity-sort`

## Goal

Two user-facing problems, one shared root cause:

1. `{a:{$gt:5}}` (and other ranges) wrongly pick **FullScan** even when a usable
   index exists and the range is selective.
2. Index plans that satisfy `ORDER BY` ("exact sort") are under-preferred.
3. Sub-case: `{a:{$ne:""}}` on a **sparse** index should use the index — the
   sparse index *itself* is a cut of the collection.

**Root cause:** any range predicate falls back to `DefaultRangeSelectivity = 0.5`
measured against `totalDocs`. The hash-bucketed sketch cannot estimate ranges
(hashing destroys value order). At 0.5, IndexSeek ≈ `3.5·e = 1.75·N` vs FullScan
`0.6·N`, so the index loses; the same bogus 0.5 makes exact-sort scans look
expensive.

## Decisions (settled with the user + external expert)

- **Improve the CBO; do NOT mix in an RBO.** A rule-based fallback/override
  (v0-style structural weights) re-introduces the random-fetch cliff for
  non-selective dense indexes and gives silent "adding an unrelated index tanks
  a plan" behavior. Kill v0 weights; do not resurrect them as a fallback or
  tie-breaker.
- **Bound the estimate by the index's own population**, not `totalDocs`. The
  fix is `e = f · EntryCount(L)` where `EntryCount(L)` = live index entries at
  prefix level L (already tracked, distinct from `docCount`). This makes sparse
  indexes win natively — no override branch.
- **User risk preference:** prefer occasionally over-using an index on an
  ambiguous range over *wrongly* flipping to FullScan. So in genuinely
  ambiguous cases the cost model should lean toward the index (index-favoring
  defaults + the EntryCount bound), while path-interpolation makes the dense
  case *accurate* rather than guessed.
- **Exact-sort lever:** add a linear `CostMaterialize` to the full-scan sort
  path. Do NOT use a multiplicative discount on the index scan (causes the
  cliff) and do NOT bump `CostSortSwap` (its n·log n shape over-penalizes large
  safe full scans).

## Plan

### Phase 1 — cheap, no btree changes (delivers sparse `$ne` + exact-sort boost)

**1a. `EntryCount` population bound.** New helper `indexPopulation(idx, totalDocs)`
returning `EntryCount(BoundFields-1)` (clamped to sketch levels; falls back to
`totalDocs` when no/untrusted sketch; capped at `totalDocs` for multikey where
entries can exceed docs). Use it as the population base in the fallback paths,
replacing `totalDocs`:
- `selectivityForIndex`: range fallback → `DefaultRangeSelectivity · pop/totalDocs`.
- `estimateIndexDocsWithFieldSel`: fallback returns `pop · sel` instead of `totalDocs · sel`.
- Leave `calculateSelectivity` on `totalDocs` — it estimates the filter's true
  selectivity over the collection (drives FullScan yield), which a sparse index
  does not reduce.

Effect: `{a:{$ne:""}}` sparse (2% present) → `e ≈ 0.5·0.02N` → IndexSeek wins.
Dense `$ne` → `EntryCount ≈ N` → stays FullScan (no regression).

**1b. `CostMaterialize = 1.0`** in `cost.go`; apply in `computeFullScanCost` when
`needSort`: `cost += estimatedYield · CostMaterialize` (alongside the existing
`sortCost`). Protected failure mode check: 1% filter + indexed ORDER BY → ordered
scan still pays `e·CostDocFetch` for rows walked, so FullScan (`~0.61N`) still
correctly beats it (`~3.6N`); selective/limited queries flip to the index.

**1c. Tests:** sparse `$ne` → IndexSeek; dense `$ne` → FullScan (regression
guard); `WHERE b=x ORDER BY a LIMIT 10` → ordered index scan; `WHERE b=x ORDER
BY a` (no limit, poor selectivity) → FullScan.

### Phase 2 — B-tree path interpolation (accurate `f` for dense selective ranges)

The only thing that catches a *selective* range on a *dense* index (where
`EntryCount ≈ N`, so Phase 1 alone still guesses 0.5).

**2a. `internal/btree`:** add `Cursor.RangeFraction(lowKey, highKey []byte)
(float64, error)` (or standalone). Two Seek descents; find the highest level
where the paths diverge; interpolate `(j-i)/numCells` and recurse down both
divergent paths scaled by cumulative fanout (SQLite-style; uniform-fanout
assumption — sufficient for the binary "beat full scan" decision near the
~17%-of-N break-even). `Seek` already records `cursorFrame{pgno, cellIdx}` per
level; extend `cursorFrame` with `numCells` captured during descent so no extra
page reads. Cost ≈ tree depth (3–5 pages, already warm).
- One-sided: `low=-inf` → leftmost path (cellIdx 0); `high=+inf` → rightmost.
- Guard rails: clamp `f ∈ (tiny, 1.0]`; floor `e ≥ 1` for a non-empty range;
  `start==end` bypasses interpolation → use the hash sketch (exact for points).

**2b. `internal/qplanner`:** add `Ns *btree.Namespace` to `CBOIndex` (populated
in `query.go` from `idx.ns`); the planner holds `params.Tx`. In the range
branches, open a plan-time cursor, sum `RangeFraction` over the index's bounds
(handles the `$ne` 2-bound union), and set `e = f · EntryCount(L)` — the unifying
formula; Phase 1's bound is the `f=1` special case.
- **One-way ratchet:** adopt the interpolated `f` only when it is MORE selective
  than `DefaultRangeSelectivity` (i.e. `f < 0.5`). A selective range is refined
  down so the index wins; a broad range / dense `$ne` (`f ≈ 1`) keeps the
  conservative default, so no previously-indexed plan regresses to full scan.
  This matches the user's "prefer the index" lean and protects the cliff (the
  0.5 default already routes 99%-ranges to full scan on large collections).
- **Multikey gate:** skip interpolation when `EntryCount(0) > totalDocs` (array
  fan-out): `RangeFraction` counts entries, not documents, so its entry-fraction
  doesn't map to a doc-fraction. Those fall back to the Phase-1 EntryCount path.
- Degrade safely to Phase-1 behavior when `Ns`/Tx unavailable or sketch needs
  rebuild.

**2c. Tests:** `RangeFraction` units (one-sided, fully-contained, full-span,
empty, single-leaf, post-delete skew); planner picks IndexSeek for selective
`{a:{$gt:v}}` on a dense index and FullScan when `a>v` matches most docs.

### Phase 3 — calibration / validation

- benchstat the 50K-doc suite (constants are calibrated there); confirm no
  plan-time regression from the 2 descents.
- Revisit `CostDocFetch`/`DefaultRangeSelectivity` only if benchmarks show the
  model is still too FullScan-happy for the user's query mix.

## Out of scope (follow-ups)

- `{a:{$exists:true}}` produces no bounds today (`Exists.IndexBounds` returns
  input unchanged) so it can never use a sparse index — the purest version of
  the cut. Folding it in = give `Exists` a full-range bound / dedicated
  "scan sparse index" candidate with `e = EntryCount`.
- Background rebuild of shallow sketch levels after a legacy V1 load (already
  deferred on the prefix-sketch branch).

## Commit sequencing (separable, per scope-discipline)

1. `qplanner`: `EntryCount` population bound + tests (1a, 1c).
2. `qplanner`: `CostMaterialize` exact-sort boost + tests (1b, 1c).
3. `btree`: `RangeFraction` + `cursorFrame.numCells` + unit tests (2a).
4. `qplanner`: wire `Ns`, consume interpolation, guard rails + tests (2b, 2c).
5. benchstat pass (3).
