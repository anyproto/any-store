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

_(None identified yet.)_
