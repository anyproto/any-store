# Array-index `$in` Count — `feat/array-index-in-sortdedup` vs `btree`

Fixes 3 pre-existing wrong-answer `Count()` bugs on multi-key (array) indexes and
replaces the high-alloc multi-bound Count path with a pooled sort-dedup.

## Files changed (vs `btree`)

| File | Change |
|---|---|
| `query/bound.go` | new `Bounds.Intersect` + `intersectPair`/`maxStartKey`/`minEndKey` |
| `query/filter.go` | `And.IndexBounds` intersects same-field conjuncts |
| `internal/qplanner/index_iter.go` | `PointLookup` field, canonical-key probe, 4-branch `CountEntries`; **deleted** `countEntriesWithDedup` |
| `internal/qplanner/sort_dedup.go` | new pooled arena + typed `sort.Interface` count |
| `internal/qplanner/cover_iter.go` | probe-gated multi-key detection |
| `docs/known-issues.md` | I-04/05/06 → FIXED; added I-07/I-08 |

## Bugs fixed (each reproducer verified failing pre-fix)

| ID | Bug | Count before → after |
|---|---|---|
| I-04 | `And.IndexBounds` dropped same-field conjuncts → CountOnly fast path over-counts | 2 → 0 |
| I-05 | peek-then-batch shortcut double-counts mixed scalar/array docs | 4 → 3 |
| I-06 | `CoverIter` hardcoded `multiKey=false` → unique-index array over-counts | 2 → 1 |

## Decisions (problem → options → choice → why)

| Problem | Options | Chosen | Why |
|---|---|---|---|
| Multi-bound array Count was high-alloc (75k) and carried the I-05 bug | seen-set / **pooled sort-dedup** / k-way merge | pooled sort-dedup | low-alloc; far simpler than the merge's correctness invariants |
| The probe can't see a compound index's multi-key (0x06 tag is mid-key); planner routes `{a:$in}` to a compound `(a,b)` on a cost tie → `simple_index/In` 144× slower | planner prefer narrower index / value-byte per-entry walk / **accept regression** | accept | keeps the clean 4-branch dispatch; the single-field array path it targets is *faster* than baseline |
| Synthetic legacy test (`AllNil`) had array entries with **no** canonical 0x06 key → probe blind → wrong count | always sort-dedup / robust value-byte fallback / **fix the test** | update test to realistic shape | git history: `writeValues` has always emitted the 0x06 key, so the no-key shape can't occur in real data |
| sort-dedup was 1.5× slower wall-clock (`sort.Slice` reflection) | accept / **typed sort** / switch back to seen-set / revisit merge | typed `sort.Interface` | dropped `reflect.Swapper` → 0.91× (faster than baseline) with the alloc win kept |
| Review: `And.IndexBounds` under-approximated when `Or`-seeded (non-empty `bs`) → empty bounds for a satisfiable query | full `Or`/`And` refactor / **split on empty `bs`** / leave latent | intersect only at top level; `Or`-seeded keeps the safe union | minimal; restores the bounds ⊇ matches invariant; keeps the I-04 fix |
| Review: `sortDedupPool` retains O(n) bytes at the high-water mark | cap the pool / **document** | document | capping removes the alloc win (it depends on pooling the arena); the parked k-way merge is the O(k)-memory alternative |

## Result (500k docs, vs `btree`)

| Scenario | wall-clock | allocs |
|---|---|---|
| `array_index/In` (target) | 8.56ms → **7.75ms (0.91×)** | 75,584 → **63** (~1200× fewer) |
| `array_index/InLarge` / `InAllMatch` | 0.90× / 0.92× | ~1150× / ~2950× fewer |
| `unique_index/In3` | 1.04× (within ±5%) | 64 → 68 |
| `array_index/InEmpty` | 1.09× (~0.5µs probe Seek) | 59 → 63 |
| `simple_index/In` | **144× slower** (compound routing — accepted) | 66 → 67 |
| `Eq` / `Range` / `HighSel` / `LowSel` | ~1.0× (no collateral regression) | unchanged |

Tests: all packages green except two pre-existing flakes (`TestCollection_Stats`,
`TestDb_Close`) that also fail on the `btree` base.

## `simple_index/In` regression — why, and next steps

### Why it happens
`{a:{$in:[...]}}` can use either the single-field `(a)` index or the compound
`(a,b)` index — both cover `a`. The cost model scores them **equal** (a tie), and
the tie-break currently picks the **compound `(a,b)`**.

The Count dispatch then splits on field count:
- single `(a)` → canonical-key probe → `a` is scalar (no `0x06` entries) →
  **batch `CountUntil`** (page-level, ~24µs).
- compound `(a,b)` → **sort-dedup, always**. The probe only works for
  single-field indexes: in a compound key the array `0x06` tag sits *mid-key*,
  not at byte 0, so `Seek(0x06)` can't detect compound multi-key. So compound
  counts can never take the fast batch path.

For this scalar `(a,b)` index, batch would actually be correct and fast — but the
Count path can't cheaply *prove* a compound index is scalar-only. The probe can't
see it, and the only sound signal is reading every entry's value byte, which is
exactly the per-entry walk we're avoiding. So it conservatively sort-dedups:
collects + sorts ~25k docIds → ~3.5ms.

**It is a performance problem only — the count is correct.** Trigger: a
single-field `$in` query when a compound index whose prefix is that field also
exists. Pure single-field setups are unaffected (they hit the fast batch path).

### Next steps (ranked)
1. **Planner: break index-cost ties toward the narrower index** *(recommended)*.
   On equal cost, prefer fewer fields / the field-prefix index. Then `{a:$in}`
   picks `(a)` → batch → ~24µs, and compound indexes are used only when genuinely
   needed. Cleanest fix, a general planner win, and it doesn't touch Count
   correctness. Risk: affects all index-selection ties — validate against the
   existing planner tests. *(`internal/qplanner/planner.go`, candidate
   scoring/tie-break.)*
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
