# Aggregation pipelines

any-store has a MongoDB-style aggregation framework: an ordered pipeline of
stages that filters, reshapes, unwinds, groups and sorts documents inside one
read-transaction snapshot. The leading part of the pipeline is compiled into a
regular query and executed by the access planner — secondary indexes, the
cost-based optimizer, full-text (`$text`) and vector sources all apply — and
only the stages the planner can't express run as streaming operators in Go.

```go
iter, err := coll.Aggregate(`[
    {"$match":  {"status": "published"}},
    {"$unwind": "$tags"},
    {"$group":  {"_id": "$tags", "n": {"$count": {}}, "lastEdit": {"$max": "$edited"}}},
    {"$sort":   {"n": -1}},
    {"$limit":  10}
]`).Iter(ctx)
defer iter.Close()
for iter.Next() {
    doc, _ := iter.Doc() // e.g. {"id":"go","n":42,"lastEdit":17498}
    ...
}
```

The pipeline is accepted in the same form as `Find()` filters: a JSON string,
`[]byte` (a **marshaled anyenc value**, not JSON text), `*anyenc.Value`, or
any JSON-marshalable Go value.

## 1. Stages

| Stage | Form | Notes |
|---|---|---|
| `$match` | `{"$match": <filter>}` | The full `Find()` filter language, including `$text` and `$knn` clauses. |
| `$sort` | `{"$sort": {"a": 1, "b.c": -1}}` | 1 ascending, -1 descending; anyenc value order across types; missing sorts as null. Stable. |
| `$skip` / `$limit` | `{"$skip": 10}` / `{"$limit": 5}` | |
| `$count` | `{"$count": "n"}` | Terminal: emits the single document `{"n": <count>}`. |
| `$project` | `{"$project": {"a": 1, "b": "$x.y", "c": {"p": "$q"}}}` | Strictly explicit: only listed fields appear (`id` included **only if listed** — unlike Mongo's implicit `_id`). Exclusion (`"a": 0`) is not supported. Bare numbers and booleans are include flags (Mongo): `{"a": 5}` includes the stored field `a`; a literal number needs `{"$literal": 5}`. |
| `$addFields` / `$set` | `{"$addFields": {"b": "$x.y"}}` | Overlays computed fields; an expression evaluating to missing removes the field. All expressions are evaluated against the **stage input** (Mongo): a field added by the same stage is not visible to its sibling expressions. |
| `$unwind` | `"$tags"` or `{"path": "$tags", "preserveNullAndEmptyArrays": true}` | Default drops documents whose path is missing/null/empty; preserve emits them as-is (empty array: field removed). Non-array values pass through. |
| `$group` | see below | Hash aggregation. |

Not supported in v1: `$lookup`, `$facet`, `$bucket`, exclusion projections,
nested (dotted) output field names, and compute expression operators
(`$add`, `$cond`, ...) — the expression parser rejects them explicitly so they
can be added compatibly later.

A pipeline that does not parse is rejected as a structured `*query.ParseError`
with `Source` `"pipeline"`: `Path` locates the failure inside the pipeline
document with the stage index as the leading segment (`"1.$match.a.$gt"`),
`Op` names the stage or operator at fault, and
`errors.Is(err, query.ErrUnknownOperator)` identifies vocabulary misses —
unknown stage, accumulator, or expression operator. The stage and accumulator
vocabularies are exported as data (`anystore.AggregateStages()`,
`anystore.AggregateAccumulators()`), so consumers advertising the grammar
never hand-copy the lists.

## 2. Expressions

Inside `$project`/`$addFields` values, `$group` keys and accumulator arguments:

- **Field references** — `"$a.b.c"` (dot paths into the document; FTS/$knn
  virtual fields work too: `"$_score"`, `"$_distance"`).
- **Literals** — any non-`$` value; `{"$literal": "$kept-verbatim"}` escapes a
  literal that starts with `$`.
- **Document/array expressions** — `{"x": "$a", "y": 1}` and `["$a", 1]`
  evaluate their members (Mongo expression-context rules); a missing member is
  omitted from objects and becomes null in arrays.

## 3. $group

```json
{"$group": {
    "_id":  {"cat": "$cat", "year": "$meta.year"},
    "n":    {"$count": {}},
    "total":{"$sum": "$amount"},
    "tags": {"$addToSet": "$tag"}
}}
```

- The group key is spelled `_id` (Mongo) or `id`; **the output field is always
  `id`** — any-store documents carry `id`, so group results can be inserted
  back into a collection unchanged. A missing key value groups as `null`.
- Key equality is byte equality of the canonical anyenc encoding. For object
  keys this is field-order-sensitive (a deliberate divergence from Mongo's
  order-insensitive document comparison).
- Output order is first-seen (scan) order — unspecified; add `$sort`.

Accumulators: `$sum`, `$avg` (numeric inputs only; empty: `0` / `null`),
`$min`, `$max` (anyenc value order; null/missing ignored; empty: `null`),
`$count` (`{}` argument), `$first`, `$last` (missing value omits the output
field; without a preceding `$sort` they reflect scan order), `$push` (skips
missing, keeps null), `$addToSet` (byte-equality dedup).

> **Numbers are IEEE 754 float64.** anyenc stores every number as float64, so
> `$sum`/`$avg` are float arithmetic — integer precision ends at 2^53. There is
> no int/long/decimal type tracking (divergence from Mongo, documented here
> rather than half-emulated).

## 4. Pushdown: what the planner executes

`Aggregate` splits the longest pushable prefix — `$match` chain (folded into
one `$and`), then at most one `$sort`, `$skip`, `$limit` *in that order* — and
hands it to the regular query planner. That means:

- an indexed `$match`+`$sort` prefix runs as an index seek/scan with
  index-order sorting and cursor-level offset skips, exactly like `Find()`;
- a `$match` containing `$text` makes the BM25 search drive the pipeline
  source (`_score` available downstream), a `$knn` clause makes the ANN index
  drive it (`_distance` available downstream). With `$k` in the clause, the
  prefix denotes at most `$k` documents — downstream `$group`/`$count` stages
  aggregate exactly that page, never a silently `ef`-truncated stream;
- `{"$match": {"x": {"$in": []}}}` short-circuits to an empty source with no
  I/O.

Pushdown stops at the first `$group`/`$project`/`$addFields`/`$unwind`/`$count`
or any out-of-canonical-order stage; the remainder runs in-pipeline. An
in-pipeline `$sort` directly followed by `$skip`/`$limit` keeps only the top
`skip+limit` rows (heap + packed arena, O(K) memory).

`$text` and `$knn` clauses are valid **only inside the pushdown prefix** —
they are executed by the index sources the planner builds, not by the
streaming `$match` operator. A `$match` containing them after the prefix ends
(e.g. after `$unwind`/`$group`, or preceded by `$skip`/`$limit`) fails
`Iter`/`Count`/`Explain` with a descriptive error instead of silently
matching everything ($text) or matching nothing ($knn). The legacy bare-array
ANN spelling is likewise rejected in-pipeline (`ErrLegacyVectorClause`). This
is final: `AggQuery` has no `Delete`/`Update`, so the rejection costs
expressiveness, never data.

`AggQuery.Explain` shows the split:

```
... access plan of the pushed prefix ...
Pushdown: filter={"cat":{"$eq":"c1"}} sort limit=3
Stages:
  1. $group {id:$cat,"top":{$push:$v}}
```

## 5. Limits and memory

Streaming stages retain nothing and are allocation-free in steady state.
Blocking stages (`$group`, in-pipeline `$sort`) retain data and are bounded;
exceeding a bound aborts the iteration with a sentinel error:

| Bound | Default | Override | Error |
|---|---|---|---|
| Unique `$group` keys | 50 000 | `GroupLimit(n)` | `ErrGroupLimitExceeded` |
| `$push`/`$addToSet` length | 10 000 | `AccumArrayLimit(n)` | `ErrAccumArrayLimitExceeded` |
| Retained bytes (all blocking stages) | 256 MiB | `MemoryLimit(n)` | `ErrAggMemoryLimitExceeded` |

Negative values mean unlimited. There is no spill-to-disk: a pipeline that
needs more than the budget should filter earlier or raise the limit
explicitly.

```go
iter, err := coll.Aggregate(pipeline).
    GroupLimit(200_000).
    MemoryLimit(1 << 30).
    Iter(ctx)
```

## 6. Iterator semantics

`Aggregate(...).Iter(ctx)` returns the same `Iterator` interface as `Find()`:
documents are valid **only until the next `Next()` call** (copy if you keep
them). `Score()`/`Distance()` return 0 on aggregation iterators —
for an FTS/vector prefix, read the `_score`/`_distance` fields off the
documents instead. `Count(ctx)` runs the pipeline and counts results;
`$count`-as-last-stage emits the count as a document instead.
