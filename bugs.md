# Bugs

Tests that reveal bugs are marked with `t.Skip("FAIL: ...")` so they count as
expected-fail. Each entry below links the skipped test to the specific
line/function in production code.

## BoundsResult.AllFixed — zero-bounds fields incorrectly treated as "fixed"

- **File**: `internal/qplanner/planner.go:1297`
- **Test**: `internal/qplanner/planner_unit_test.go` → `TestBoundsResult_AllFixed_ZeroBoundsField` (skipped with `FAIL:` prefix)

`BoundsResult.Build` at planner.go:1263-1269 initializes `allFixed := true` and
only flips it to false inside the `for _, b := range br.Bounds[start:]` loop.
When a field has zero bounds (e.g. the filter does not reference the field),
the loop body never runs and the field is stored with `Fixed: true`.

`BoundsResult.AllFixed` then returns true for a `BoundsResult` whose fields
all have zero bounds, even though there are no equality constraints at all.
The godoc for `AllFixed` says "returns true if all fields have equality (fixed
point) bounds" — which is not strictly satisfied when a field has no bounds.

`AllFixed` is currently unused by production code (grep-verified), so there is
no user-visible regression, but the contract mismatch should be addressed
before any caller relies on it.

## filterFieldsCoveredBy / indexCoversFilter — missing `*query.And` case

- **File**: `internal/qplanner/planner.go:1116` (`filterFieldsCoveredBy`) and
  transitively `internal/qplanner/planner.go:1102` (`indexCoversFilter`).
- **Test**: `internal/qplanner/planner_unit_test.go` → `TestFilterFieldsCoveredBy_PointerAndBranch` (skipped with `FAIL:` prefix).

`filterFieldsCoveredBy` has `case query.And` (value) but no `case *query.And`
(pointer). The sibling function `collectUncoveredFilterFields` (planner.go:1142)
has BOTH value and pointer cases.

`query.MustParseCondition` produces `*query.And` for `{"$and":[...]}` JSON
syntax (see `query/cond_parse.go:103`), so `indexCoversFilter` returns false
for any filter built that way, even when the index fully covers the filter
fields. This disables the covering-count fast path for `$and`-spelled filters
that are structurally identical to comma-spelled `{"a":1,"b":2}` filters.

## UNREACHABLE

- `internal/qplanner/planner.go:1355` — `if !chain[i-1].fixed { break }`
  inside the compound-index loop of `ComputeIndexBounds`. The outer loop at
  planner.go:1319-1329 already breaks after appending the first non-fixed
  field, so `chain[0..len-2]` are always fixed and `chain[i-1].fixed`
  (with `i >= 1 && i < len(chain)`) is always true. Defensive dead code.

- `internal/qplanner/planner.go:669-671` — `if pTotal <= 0 { pTotal = 0.0001 }`
  outer clamp in `calculateSelectivity`. Every per-field branch either hits
  the sketch clamp at line 648-650 (which promotes 0 → 0.0001) or multiplies
  by a positive constant (`DefaultRangeSelectivity`). The only other path
  sets `pTotal = DefaultRangeSelectivity` directly when no field matched.
  There is no combination that makes the running product `<= 0` at the outer
  check. Defensive dead code.

- `internal/qplanner/planner.go:347-348` — `if nSeeks < 1 { nSeeks = 1 }` in the
  Plan-B loop. The outer `if len(idx.Bounds) == 0 { continue }` at line 323
  already skips indexes with no bounds, so `len(idx.Bounds) >= 1` and
  `nSeeks` (= float64(len(idx.Bounds))) is always ≥ 1 when this check runs.
  Defensive dead code.

- `internal/qplanner/planner.go:367-369` — `if scanSel <= 0 { scanSel = 0.0001 }`
  inside Plan-B ExactSort+Limit branch. `scanSel = pTotal / idxSel`. `pTotal`
  is clamped by calculateSelectivity (which produces > 0 values via the
  inner-sketch and default-range branches), and `idxSel` is clamped by
  `selectivityForIndex` (low clamp to 0.0001). So `scanSel` cannot reach 0.
  Defensive dead code.

- `internal/qplanner/planner.go:435-437` — `if scanSel <= 0 { scanSel = 0.0001 }`
  in Plan-C. Same reasoning as above: both numerator and denominator are
  pre-clamped positive, so `scanSel > 0`. Defensive dead code.

- `internal/qplanner/planner.go:672-674` — `if pTotal > 1.0 { pTotal = 1.0 }`
  outer clamp in `calculateSelectivity`. The inner sketch clamp at line 645-647
  caps each individual `p` at 1.0, and subsequent multiplications by values
  in (0, 1] can only shrink the product. The outer >1.0 check cannot be
  reached. Defensive dead code.

- `internal/qplanner/planner.go:1425` — `if start < 0 || start >= len(idx.FieldNames)`
  inside `matchAt` in `IndexSortMatch`. `matchAt` is called with `0`
  (always safe) and `equalityPrefix` (gated at planner.go:1456 by
  `equalityPrefix > 0 && equalityPrefix < len(idx.FieldNames)`). A negative
  `equalityPrefix` is never produced by the planner. Defensive dead code.
