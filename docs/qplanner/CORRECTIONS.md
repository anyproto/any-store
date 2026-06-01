# qplanner specs — corrections (wrong assumptions found by the audit)

This file records places where `docs/qplanner/specs/*` disagree with the actual
implementation, found while auditing the specs against the code. The specs
describe an **older design**; the corrections below reflect the code as of branch
`btree-qplanner-review`. Verified against `internal/qplanner/`, `query/`,
`index.go`, `query.go`, `errors.go`.

## Planner model

| Spec claim | Reality |
|---|---|
| "weight-based planner"; `weight = queryWeight + sortWeight + hints`; weight tables (first field `10`, `×2`, `−1`, `+2`, `+1`; sort weight `11`, `×2`, `+2`, `+5`) | **Cost-Based Optimizer.** `internal/qplanner/planner.go` `BuildPlan` evaluates `FullScan`/`IndexSeek`/`IndexScan` candidates and picks the lowest **cost**. Cost constants in `cost.go` (`CostIndexSeek=0.5`, `CostDocFetch=3.0`, `CostSeqRead=0.1`, `CostFilter=0.5`, `CostSortSwap=0.25`). Selectivity via count sketches (`sketch.go`). |
| weight logic in `internal/qplanner/weight.go` | **No `weight.go` exists.** Sort/cost logic lives in `planner.go`. |
| `MaxIndexes` (default 2) caps selected indexes | The CBO chooses **one** index (the cheapest candidate); `IndexSortMatch`/cost selection, not a `MaxIndexes` cap. |

## Explain output

| Spec claim | Reality |
|---|---|
| `Explain.Sql` = `"IndexScan(a) WHERE a=5 \| Filter \| Limit(10)"` (pipe + SQL `WHERE`) | `Explain.Sql == plan.String()` = the iterator chain joined by `" -> "`, e.g. `IndexScan(a)[bounds=Bounds{['5','5']}] -> Fetch -> Filter -> Limit(10)`. Bounds render inside `[bounds=...]`; there is no `\|`/`WHERE`. |
| `Explain{ Sql; SqliteExplain []string; Indexes []IndexExplain{Name, Weight, Used} }` | Actual (`query.go:52`): `Explain{ Sql string; Plan string; Indexes []IndexExplain }` with `IndexExplain{ Name string; Cost float64; Used bool }`. No `SqliteExplain`, no `Weight`. |
| (implicit) an in-memory sort always prints `Sort` | Under a `Limit`, the in-memory sort prints **`TopK(N)`**, not `Sort`. Assertions using `NotContains "Sort"` are unreliable. |

## Index storage / iterators

| Spec claim | Reality |
|---|---|
| Unique index: `Key = Tuple(value1, …)`, `Value = docId` | Both unique and non-unique store `key = Tuple(fields…, docId)`; the **value is a 1-byte flag** (`IndexValueScalar`/`IndexValueMultiKey`, see `index.go insertKeys`). Uniqueness is enforced by a prefix `AppendSeekKey` on the field tuple, not by the value. |
| Iterator `CoverFilterIter` ("secondary index filtering") | **No `CoverFilterIter`.** Real iterators: `FullScanIter`, `IndexIter`, `CoverIter` (→ `CoverLookup` in Explain), `FilterIter`, `FetchIter`, `IndexFilterIter`, `SortIter`, `LimitIter`, `CanonicalKeyDedupIter`, `VerifyIter`. |
| Reverse fields use "inverted encoding — a forward btree scan of inverted keys yields descending order" | **Now true** (completed by this audit — was previously dead code). `index.go writeValues` bitwise-inverts reverse-flagged fields on write (via `anyenc.Tuple.AppendInverted`), so a forward index scan yields the index's declared per-field directions and a reverse scan their exact opposite. A compound `(a,-b)` index serves `Sort("a","-b")` (forward) and `Sort("-a","b")` (reverse) by a fast `IndexScan` with no in-memory sort; `shouldReverse` = `sort[0].Reverse != idx.Reverse[matchStart]`; per-field bounds are inverted+swapped for reverse fields (`transformReverseBounds`); `anyenc/parser.go` skips inverted-tag fields on the length-only path. **Migration:** reverse indexes created before this change are stored ascending and must be `DropIndex`+`EnsureIndex` rebuilt. |

## Error sentinels

| Spec claim | Reality (`errors.go`) |
|---|---|
| `ErrDocumentNotFound` | `ErrDocNotFound` |
| `ErrUniqueConstraint`, `ErrCollectionNotFound` | correct (exist) |

## "Known Issues" in `index-planner-architecture.md`

- **"Reverse scan direction inverted (`planner.go:94-101`): `!=` should be `==`"** — **STALE.** `shouldReverse` (`planner.go`) just returns `sortFields[0].Reverse`; the `!=`/`==` code is gone (those line numbers are now `ExplainString`).
- **"FilterFullyCovered bitmap inverted (FIXED)"** — no such symbol in the current code; covering-count gating is `indexCoversFilter` (and was itself fixed by this audit, bug-02).
- **"Range selectivity not considered"** — **STILL ACCURATE.** `selectivityForIndex` returns a constant `DefaultRangeSelectivity = 0.5` for any range regardless of bound width, so a wide range can be mis-costed. This is a genuine cost-model limitation (not just stale docs).

## Stale test comment

`index_maintenance_test.go` (`TestIndex_UpsertMutation_FindUpdateUniqueConstraintViaInsert`) comments that the unique constraint is *not* enforced on `Find().Update()` ("a btree Put (overwrite), not a uniqueness-checked insert"). This is **false**: `collection.update` → `insertKeys` runs the `AppendSeekKey` + prefix unique check on every insert, so `Find().Update()` onto another doc's unique value returns `ErrUniqueConstraint` and rolls back. (Locked in by a new audit test.)

## Correctness bugs found AND fixed by this audit

See `qplanner_audit_bugfix_test.go` and the fix commit. Summary:

1. **Mixed-direction compound order** — `(a,-b)` returned b ascending within each a group; `IndexSortMatch` falsely claimed `ExactSort` against all-ascending storage. **Fixed properly via invert-on-write** (see the reverse-fields row above): reverse fields are now physically inverted, so `(a,-b)` serves `Sort("a","-b")`/`Sort("-a","b")` by a fast forward/reverse `IndexScan` (no in-memory sort). (An earlier interim fix instead made the planner fall back to an in-memory sort — correct but slow — and was superseded by the invert-on-write commit.)
2. **Reverse covering filter dropped all rows** — `coveringFilterFields` bitwise-inverted the match value, which only matches once storage is inverted. Now correct because reverse fields are physically inverted on write (the inversion is the right behavior, not a bug).
3. **Skip-middle `Count` over-counted** — `indexCoversFilter` gated on all index fields instead of the contiguously-bounded prefix. Now gates on `FieldNames[:BoundFields]`.
4. **Offset-without-Limit returned zero** on the in-memory sort path — `SortIter` `TopK` was `Limit+Offset` even when `Limit==0`. Now `0` (full sort) when `Limit==0`.
