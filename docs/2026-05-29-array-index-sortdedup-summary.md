# Array-index `$in` Count — `feat/array-index-in-sortdedup` vs `btree`

Fixes pre-existing wrong-answer `Count()` bugs on multi-key (array) indexes and
replaces the high-alloc multi-bound Count path with a pooled **seen-set**. (The
branch name keeps the historical `sortdedup` token; the first implementation was
a sort-dedup, since replaced — see Decisions.)

## Files changed (vs `btree`)

| File | Change |
|---|---|
| `query/filter.go` | `And.IndexBounds` returns a sound **over-approximation** (first contributing conjunct), not an intersection — required for array fields |
| `internal/qplanner/planner.go` | `indexCoversFilter` **gates the CountOnly fast path**: a covered field with >1 predicate is rejected (I-04) |
| `internal/qplanner/index_iter.go` | `PointLookup` field, canonical-key probe, 4-branch `CountEntries` (branches 2 & 4 → seen-set); **deleted** `countEntriesWithDedup` |
| `internal/qplanner/seenset.go` | **new** pooled, near-zero-alloc seen-set (replaced the typed-sort `sort_dedup.go`) |
| `internal/qplanner/cover_iter.go` | probe-gated multi-key detection (I-06) |
| `docs/known-issues.md` | I-04/05/06 → FIXED; added I-07/I-08 |

(`query/bound.go` is effectively unchanged vs `btree`: a `Bounds.Intersect`
helper was added for the first I-04 fix, then removed — see the I-04 row in
Decisions.)

## Bugs fixed (each reproducer verified failing pre-fix)

| ID | Bug | Count before → after |
|---|---|---|
| I-04 | `And.IndexBounds` dropped same-field conjuncts → CountOnly fast path over-counts | 2 → 0 |
| I-04′ | the first I-04 fix (bounds intersect) under-counted **array two-sided ranges** in Count *and* Iter | 2 → 3 |
| I-05 | peek-then-batch shortcut double-counts mixed scalar/array docs | 4 → 3 |
| I-06 | `CoverIter` hardcoded `multiKey=false` → unique-index array over-counts | 2 → 1 |

## Decisions (problem → options → choice → why)

| Problem | Options | Chosen | Why |
|---|---|---|---|
| Multi-bound array Count was high-alloc (~75k) and carried the I-05 bug | seen-set / sort-dedup / k-way merge | **pooled seen-set** | lowest alloc + fastest on real interleaved `$in` docIds; a skip-scalar contract avoids the map entirely for provably-unique scalar entries |
| First impl was a typed-sort **sort-dedup**; a microbench favored it over a map | keep the sort / **switch to seen-set** | seen-set | the microbench was flawed — its synthetic docIds were pre-sorted per bound, handing pdqsort an O(n) best case; real `$in` docIds interleave, so the sort does O(n log n) work while the order-independent map wins (measured below) |
| **I-04: the bounds-intersect fix was unsound for ARRAY fields** — a doc matches `{$gte:2,$lte:3}` when one element is ≥2 and a *different* element ≤3, so it need not have any element in `[2,3]`; the narrowed seek dropped such docs from Count *and* Iter | array-aware intersect / **over-approx + fast-path gate** / revert intersect only | over-approx (first conjunct) **+** gate `indexCoversFilter` on >1-predicate fields | sound for scalar *and* array; the gate keeps Count exact without the unsound intersect, subsuming the earlier `Or`-seeded-split patch. Found by the differential review. Cost: scalar two-sided ranges route through `FilterIter` instead of a tight seek |
| Probe can't see a compound index's multi-key (`0x06` tag sits mid-key); planner routes `{a:$in}` to a compound `(a,b)` on a cost tie → `simple_index/In` slow | planner prefer narrower index / value-byte per-entry walk / **accept** | accept | keeps the clean 4-branch dispatch; the seen-set's skip-scalar already cut this path −78%; the real fix is a planner tie-break (Next steps) |
| Synthetic legacy test (`AllNil`) had array entries with **no** canonical `0x06` key → probe blind | always dedup / value-byte fallback / **fix the test** | update test to realistic shape | git history: `writeValues` has always emitted the `0x06` key, so the no-key shape can't occur in real data |
| `seenSetPool` retains O(n) bytes at the high-water mark (map buckets dominate ~3.5× the chunks) | cap the pool / **document** | document (cap is a known follow-up) | warm reuse *is* the alloc win; the 4-agent review confirmed no leak/corruption — the high-water is bounded and GC-reclaimable |

## Result

Two measured A/Bs, both 500k docs, 6× `benchstat`. (A fresh seen-set-vs-`btree`
table has not been re-run; the figures below are exact for the comparisons
stated, and the branch is strictly faster than the `btree` baseline on the array
paths because the seen-set beats the sort-dedup that was already ≤1.0× `btree`.)

**Seen-set vs the prior typed-sort sort-dedup** — the current dedup path vs what
it replaced (commit `129ffc0`, p=0.002):

| Path | wall-clock |
|---|---|
| `array_index/In` | **−42%** |
| `array_index/InLarge` / `InAllMatch` | **−39%** |
| `simple_index/In` | **−78%** (skip-scalar walk) |
| B/op (geomean) | **−25%**; allocs ~flat; non-dedup scenarios unchanged |

**Sort-dedup vs `btree`** — the prior baseline, showing the branch was already
≤1.0× `btree` on the array paths before the seen-set:

| Scenario | wall-clock | allocs |
|---|---|---|
| `array_index/In` (target) | 0.91× | 75,584 → **63** (~1200× fewer) |
| `array_index/InLarge` / `InAllMatch` | 0.90× / 0.92× | ~1150× / ~2950× fewer |
| `unique_index/In3` | 1.04× (within ±5%) | 64 → 68 |
| `array_index/InEmpty` | 1.09× (~0.5µs probe Seek) | 59 → 63 |
| `simple_index/In` | 144× slower → **~30× after the −78% above** (compound routing — accepted) | 66 → 67 |
| `Eq` / `Range` / `HighSel` / `LowSel` | ~1.0× (no collateral regression) | unchanged |

Tests: all packages green (incl. `-race`) except the pre-existing
`TestCollection_Stats/concurrent_with_writes` sketch race, which also fails on
the `btree` base.

## `simple_index/In` regression — why, and next steps

### Why it happens
`{a:{$in:[...]}}` can use either the single-field `(a)` index or the compound
`(a,b)` index — both cover `a`. The cost model scores them **equal** (a tie), and
the tie-break currently picks the **compound `(a,b)`**.

The Count dispatch then splits on field count:
- single `(a)` → canonical-key probe → `a` is scalar (no `0x06` entries) →
  **batch `CountUntil`** (page-level, ~24µs).
- compound `(a,b)` → **seen-set, always**. The probe only works for single-field
  indexes: in a compound key the array `0x06` tag sits *mid-key*, not at byte 0,
  so `Seek(0x06)` can't detect compound multi-key. So compound counts can never
  take the fast batch path.

For this scalar `(a,b)` index, batch would actually be correct and fast — but the
Count path can't cheaply *prove* a compound index is scalar-only. The probe can't
see it, and the only sound signal is reading every entry's value byte. The
seen-set's **skip-scalar** mode does read that byte and counts scalar entries
without touching the map, which is why this path is now −78% — but it still walks
every entry (no page-level batch). For a 25k-doc count that is ~0.7ms vs btree's
~24µs batch.

**It is a performance problem only — the count is correct.** Trigger: a
single-field `$in` query when a compound index whose prefix is that field also
exists. Pure single-field setups are unaffected (they hit the fast batch path).

### Next steps (ranked)
1. **Planner: break index-cost ties toward the narrower index** *(recommended)*.
   On equal cost, prefer fewer fields / the field-prefix index. Then `{a:$in}`
   picks `(a)` → batch → ~24µs, and compound indexes are used only when genuinely
   needed. Cleanest fix, a general planner win, doesn't touch Count correctness.
   Risk: affects all index-selection ties — validate against the existing planner
   tests. *(`internal/qplanner/planner.go`, candidate scoring/tie-break.)*
2. **Give compound indexes a multi-key signal** so Branch 2 can batch when the
   index is scalar-only. The proper long-term fix for genuinely-compound
   multi-key counts, but it needs a per-index `HasMultiKey` flag stored
   snapshot-consistently — entangled with I-01/I-02 (the sketch is neither
   snapshot-isolated nor rollback-safe), so those must be fixed first. Heavier;
   out of scope here.
3. **Leave as-is.** Acceptable if single-field indexes are the norm for these
   queries; the regression only triggers with a redundant compound-prefix index.

## Known issues recorded (not fixed here, pre-existing)

- **I-07** — `Count()` + `Limit`/`Offset` over a multi-key index disagrees with `Iter()`.
- **I-08** — `$in:[[]]` (empty array as an `$in` member) over-counts vs the filter.
