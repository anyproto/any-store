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

## isIDOnlyFilterNode — missing `*query.And` case

- **File**: `query.go:538`
- **Test**: `query_unit_test.go` → `TestQuery_IsIDOnlyFilterNode_PointerAnd_FAIL` (skipped with `FAIL:` prefix).

`isIDOnlyFilterNode` at query.go:538-552 has `case query.And` (value receiver)
but no `case *query.And` (pointer). `query.MustParseCondition` produces
`*query.And` for `{"$and":[...]}` JSON (same as in qplanner's
`filterFieldsCoveredBy` — same latent asymmetry).

This means `Count({"$and":[{"id":"a"},{"id":"b"}]})` misses the ID-only
fast path at query.go:382 and takes the CBO route instead. Behavior is
correct (just slower), but the fast path was not disabled intentionally
for `$and` JSON syntax — it's an oversight.

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

## ERROR-INJECTION-ONLY

- `iterator.go:48-51` — `if err != nil { pi.err = err; return false }` in
  `planIterator.Next`. Requires the underlying plan iterator's `Next` to
  surface a btree error (corrupt WAL, IO failure). No public seam to inject.

- `iterator.go:93-96` — `val, err := pi.dataCursor.Value(); if err != nil`
  in the Doc() fallback. Requires `btree.Cursor.Value` to fail after a
  successful `SeekExact`. No public seam to inject.

- `anyenc/tuple.go:100-117` — error branches in `Tuple.String` (Parse error,
  ReadValues error). Reachable only with a pre-corrupted tuple; the string
  representation of corrupted tuples is not exercised by the rest of the
  codebase (error is returned internally and a sentinel string is emitted).

- `anyenc/parser.go:266-295 misc branches` — specific error sub-branches
  inside `parseCompressedObjectS2` (bad inner value after decompression,
  s2 decode error). Reachable only with handcrafted malformed compressed
  input; requires knowledge of s2 framing which is out of scope for a
  pure-Go test suite that consumes its own encoder's output.

- `internal/qplanner/fullscan_iter.go` — cursor-error arms
  (`cursor.First/Last/Next/Previous/Seek` returning errors, `cursor.Key`
  errors). The happy paths and bound-clamping are all covered; the error
  returns require a btree cursor failure that has no public injection seam.

- `internal/qplanner/index_iter.go` — same as above for IndexIter's cursor
  error arms across seek/forward/reverse variants.

- `internal/qplanner/filter_iter.go:96-102` — `Parser.ParseOwned` error
  branch and cursor-level AppendValue failures other than ErrKeyNotFound.
  Require corrupted stored docs; no injection seam.

- `internal/qplanner/sort_iter.go:141-150` — `heapUp` else-break branch
  occurs only when heapLess is false at every step of ascent. Reachable
  in principle but would require scripted heap states that bypass the
  SortIter public API; limited coverage value.

- `internal/qplanner/dedup_iter.go:54-55` — `if len(key) < len(docId)` defensive
  arm in `CanonicalKeyDedupIter.Next`. The comment itself labels it "defensive;
  shouldn't happen" — IndexIter always emits keys of form (field..., docId)
  where len(key) ≥ len(docId) by construction.

- `internal/qplanner/sort_iter.go:141-150` — `heapUp` else-break: reached
  only when the heap property is already satisfied at every upward step.
  Can happen but requires a scripted heap sequence; partial coverage is
  acceptable since the successful swap path is already tested.

- `internal/qplanner/cover_iter.go:45` — `Close` is an empty function
  (comment at line 44 notes "no cursor to close"). go-cover registers it
  as 0% statements because there are no statements to execute. Not a real
  coverage gap.

- `internal/qplanner/cover_iter.go:32-33` — `AppendSeekKey` error branch
  (continues the loop). Requires btree read error; no injection seam.

- `iterator.go:107-109` — `if perr != nil { return nil, perr }` in the Doc()
  fallback. Requires `Parser.ParseOwned` to fail on a document stored via
  the normal write path. No public seam to inject.

- `query.go:117-120, 122-126` — error propagation in `Iter`: `q.makeQuery`
  failure and `getReadTx` failure. The first is covered via filter-parse
  error; the second requires tx-open failure (e.g. already-closed DB).

- `query.go:167-170, 171-177` — WriteTx failure and commit/rollback
  branches in `Update`. Requires concurrent tx conflict or write failure.

- `query.go:206-210, 235-238, 240-243, 246-249, 253-256, 264-266, 267-269` —
  mid-iteration error branches in `Update` (plan.Root.Next error,
  AppendValue error, ParseOwned error, newItem error, Modifier.Modify
  error, modified-newItem error, update error). All require error
  injection during the update loop.

- `query.go:285-289, 313-315, 326-328, 332-334` — corresponding
  error-injection branches in `Delete`.

- `query.go:389-390` — `else if gerr != btree.ErrKeyNotFound` in Count's
  ID-only fast path (tx.Get returning a non-ErrKeyNotFound error).

- `query.go:423-426` — CountableIterator fast path error branch.

- `query.go:432-434` — plan.Root.Next error during Count's iteration loop.

- `query.go:473-480` — selected-index mark logic in Explain's loop only
  partially exercised. Already at 95%.



These branches are reachable only by injecting errors into the `btree.WriteTx`
interface, which has no public mocking seam. Testing them would require
modifying production code to accept a swappable tx, which is out of scope
for a tests-only coverage pass.

- `index.go:161-163` — `if err := tx.Put(...); err != nil { return err }`
  in `insertKeys`. The `err != nil` arm requires `btree.WriteTx.Put` to
  fail (e.g., a corrupt WAL or IO error). Unreachable from the public API
  in normal test conditions.

- `index.go:185-187` — `if !errors.Is(err, btree.ErrKeyNotFound) { return err }`
  in `deleteKeys`. The non-swallow arm requires `btree.WriteTx.Delete` to
  return an error OTHER than `btree.ErrKeyNotFound`. Same rationale as above.

- `index.go:157` — `continue // same doc, idempotent` in `insertKeys`.
  Reached only when a unique-index entry for the *same* doc is found during
  re-insertion. The public API always deletes existing index entries before
  inserting (see `collection.go:425-428` update flow and `collection.go:285-300`
  insert flow which rejects duplicate doc IDs). This branch is defensive
  for out-of-sequence callers that don't exist in current code.

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

- `internal/qplanner/planner.go:374-376` — `if s < 1 { s = 1 }` in Plan-B
  ExactSort+Limit branch. `s = float64(params.Limit+params.Offset) / scanSel`.
  The Plan-B branch is gated on `params.Limit > 0` (line 362), so
  `params.Limit+params.Offset ≥ 1`. `scanSel` is clamped to ≤ 1.0 by the
  preceding block (line 364-366). Therefore `s = (≥1) / (≤1) ≥ 1` always.
  Defensive dead code.

- `internal/qplanner/planner.go:461-463` — `if s < 1 { s = 1 }` in Plan-C
  LIMIT branch. Same reasoning as above: `s = (params.Limit+params.Offset) / scanSel`
  with `params.Limit > 0` (outer gate at line 455) and `scanSel ≤ 1.0` (clamp
  at 432-434). Defensive dead code.

- `internal/qplanner/planner.go:442-444` — `if scanPopulation < 1 { scanPopulation = 1 }`
  in Plan-C. `scanPopulation = totalDocs * idxSel`. `totalDocs` is clamped up
  to 1 at line 224-226, and `idxSel` from `selectivityForIndex` has a low
  clamp at 0.0001. Product ≥ 0.0001; it CAN drop below 1 when totalDocs is
  small (e.g. totalDocs=1, idxSel=0.0001 → scanPopulation=0.0001). This
  branch IS reachable in principle, but requires pathological inputs that
  the planner is unlikely to receive in practice; unit tests to force the
  combination are fragile — see planner_test.go integration tests.

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

- `anyenc/tuple.go:96` — `return nil, fmt.Errorf("tuple: field %d out of range", n)`
  post-loop return in `Tuple.FieldBytes`. The only way to exit the loop is
  the `return tail[:...]` inside the `i == n` branch (success) or the
  `return nil, ...` inside the `len(tail) == 0` branch (early error at
  line 85). The `for i := 0; i <= n` condition means `i` reaches `n` before
  the loop check ever fails, so control never falls through the closing
  brace. Defensive dead code.

- `anyenc/tuple.go:73-75` — `if off > len(t) { off = len(t) }` in `OffsetAfter`.
  `off` accumulates `consumed = len(tail) - len(nextTail)` per iteration and
  `tail` always shrinks monotonically, so `off` never exceeds the initial
  tuple length. Defensive clamp.

- `anyenc/value.go:460-462` — `default: panic(...)` in `Value.GoType`.
  Reachable only by constructing a `Value` with a `Type` not in the 8
  supported values. All public constructors produce a valid Type; `Type` is
  a byte with unexported semantics and no exported conversion. Defensive
  dead code that cannot be triggered without modifying production.

- `internal/qplanner/planner.go:1425` — `if start < 0 || start >= len(idx.FieldNames)`
  inside `matchAt` in `IndexSortMatch`. `matchAt` is called with `0`
  (always safe) and `equalityPrefix` (gated at planner.go:1456 by
  `equalityPrefix > 0 && equalityPrefix < len(idx.FieldNames)`). A negative
  `equalityPrefix` is never produced by the planner. Defensive dead code.
