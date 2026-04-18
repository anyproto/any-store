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

## UNREACHABLE

_(None identified yet.)_
