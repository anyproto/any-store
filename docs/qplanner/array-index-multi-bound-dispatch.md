# Multi-bound `$in` dispatch over array indexes

How a `Find({field:{$in:[v1,...,vk]}})` query lands on a specific code path
inside `internal/qplanner/index_iter.go` and `internal/qplanner/planner.go`.

## Dispatch table

| Conditions on the query and the index | Code path | Hot file:func |
|---|---|---|
| k=0 | early return 0 | `query.go` `isUnsatisfiable` |
| k=1 | page-batch count | `index_iter.go` `countEntriesBatch` |
| k≥2, all-scalar | peek-then-batch (per-bound CountUntil under sticky-multi=false) | `index_iter.go` `countEntriesWithDedup` |
| k≥2, multi-key, PointLookup, single-field, 2 ≤ k ≤ kWayMergeMax (=64 by default), sketch nil OR sum sketch est ≥ kWayMergeMinEntries (=200) | k-way docId-merge | `kway_merge.go` + `index_iter.go` `countEntriesViaMerge` |
| k≥2, multi-key, PointLookup, single-field, 2 ≤ k ≤ kWayMergeMax, sketch present AND sum sketch est < kWayMergeMinEntries | pre-sized seen-set walk | `index_iter.go` `countEntriesViaPreSizedSeenSet` |
| k > kWayMergeMax (PointLookup single-field multi-key only) | pre-sized seen-set walk | `index_iter.go` `countEntriesViaPreSizedSeenSet` |
| Compound multi-key OR non-PointLookup (range bound) OR Sort requested OR residual Filter present | existing dedup pipeline (Count) / non-merge buildIndexSeekChain (Iter/UpdateMany/DeleteMany) | `index_iter.go` `countEntriesWithDedup` / `planner.go` `buildIndexSeekChain` non-merge path |

## Pinning the route in debug

```go
import "github.com/anyproto/any-store/v2/internal/qplanner"

qplanner.EnablePerfCounters(true)
qplanner.ResetPerfCounters()

// ... run the query ...

snap := qplanner.SnapshotPerfCounters()
fmt.Printf("merge dispatches: %d\n", snap.MergeDispatches)
fmt.Printf("index Next calls: %d (yields: %d)\n",
    snap.IndexNextCalls, snap.IndexYields)
```

If `MergeDispatches == 0` for a query you expected to use the merge,
check (in order):

1. `idx.PointLookup` — true iff every original bound was equality
   (Start==End before AdjustBoundsForNonUnique). Range bounds disqualify.
2. `len(idx.Info.FieldNames)` — must be 1.
3. `len(idx.Bounds)` — must be 2..kWayMergeMax. Use
   `qplanner.SetKWayMergeMax(...)` to raise/lower the cap at runtime.
4. The `boundsAllScalar` peek — if every bound's first entry has value
   byte 0x00 (IndexValueScalar), the existing peek-then-batch is
   preferred and the merge is skipped.
5. The min-N gate — if the sum of `Sketch.Estimate(b.Start)` across
   bounds is below `kWayMergeMinEntries`, the pre-sized seen-set is
   preferred and the merge is skipped.

## Runtime escape hatches

```go
prev := qplanner.SetKWayMergeMax(0)         // disable merge entirely
defer qplanner.SetKWayMergeMax(prev)

prev := qplanner.SetKWayMergeMinEntries(0)   // disable min-N gate
defer qplanner.SetKWayMergeMinEntries(prev)
```

## Slice-lifetime contract (merge primitive)

`kWayDocIdMergeIter.Next` returns a `[]byte` that aliases an internal
two-buffer scheme. The slice is valid ONLY until the next `Next` call.
Callers that need the docId to outlive the next `Next` MUST copy. See
the docstring on `kWayDocIdMergeIter.Next` in `kway_merge.go`.

## Sketch reads are cost hints, never answer-determining

Two sketch uses in this dispatch:

- **`seenSetCapacityHint`**: pre-allocates the seen-set's map to
  `sum(Sketch.Estimate(b.Start))`. Sketch-stale-under degrades to
  today's growth-from-default; sketch-stale-over wastes a bit of
  capacity. Neither affects the answer.
- **`passesMergeMinNGate`**: routes between merge vs pre-sized seen-set
  based on whether `sum(Sketch.Estimate(...))` clears
  `kWayMergeMinEntries`. A stale sketch produces a suboptimal plan
  choice, still-correct answer.

Both uses are documented in `docs/known-issues.md` I-03 as the only
sketch reads permitted until I-01 (rollback drift) and I-02 (cross-
process staleness) are fixed.
