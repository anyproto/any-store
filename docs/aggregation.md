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
| `$match` | `{"$match": <filter>}` | The full `Find()` filter language, including `$text` and `$knn` clauses, plus `$expr` (aggregation expressions as predicates — see section 2.1). |
| `$sort` | `{"$sort": {"a": 1, "b.c": -1}}` | 1 ascending, -1 descending; anyenc value order across types; missing sorts as null. Stable. |
| `$skip` / `$limit` | `{"$skip": 10}` / `{"$limit": 5}` | |
| `$count` | `{"$count": "n"}` | Terminal: emits the single document `{"n": <count>}`. |
| `$project` | `{"$project": {"a": 1, "b": "$x.y", "c": {"p": "$q"}}}` | Strictly explicit: only listed fields appear (`id` included **only if listed** — unlike Mongo's implicit `_id`). Exclusion (`"a": 0`) is not supported. Bare numbers and booleans are include flags (Mongo): `{"a": 5}` includes the stored field `a`; a literal number needs `{"$literal": 5}`. |
| `$addFields` / `$set` | `{"$addFields": {"b": "$x.y"}}` | Overlays computed fields; an expression evaluating to missing removes the field. All expressions are evaluated against the **stage input** (Mongo): a field added by the same stage is not visible to its sibling expressions. |
| `$unwind` | `"$tags"` or `{"path": "$tags", "preserveNullAndEmptyArrays": true}` | Default drops documents whose path is missing/null/empty; preserve emits them as-is (empty array: field removed). Non-array values pass through. |
| `$group` | see below | Hash aggregation. |
| `$lookup` | `{"$lookup": {"from"?: c, "localField": f, "foreignField": "id", "as": out}}` | Self-join point lookup on the primary key — see section 3.1. |
| `$facet` | `{"$facet": {"name": [stage...], ...}}` | Named sub-pipelines over one shared scan — see section 3.2. |

Not supported in v1: cross-collection and pipeline-form `$lookup` (see
section 3.1), `$bucket`, exclusion projections,
nested (dotted) output field names, and compute expression operators beyond
the set of section 2 (`$dateToString`, `$toUpper`, ...) — the expression
parser rejects unknown operators explicitly so they can be added compatibly
later.

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
- **Compute operators** — arithmetic, string, conditional and comparison
  expressions, composable to any depth:
  `{"$cond": [{"$lt": ["$a", 10]}, "low", {"$concat": ["$a", "!"]}]}`.

| Operator | Form | Notes |
|---|---|---|
| `$add`, `$multiply` | `{"$add": [e, ...]}` | Variadic; an empty operand list yields the identity (`0` / `1`). `$add` with exactly one dateTime operand among numbers shifts the date by their sum of millis; two dateTimes → `null`. `$multiply` is numeric-only. |
| `$subtract`, `$divide` | `{"$subtract": [a, b]}` | Exactly two operands. `$subtract`: `[date, date]` → millis number, `[date, number]` → date, `[number, date]` → `null`. `$divide` by zero → `null`. |
| `$abs` | `{"$abs": e}` | |
| `$round` | `{"$round": [x, place?]}` | Half to even (banker's: `1.5`→`2`, `2.5`→`2`); `place` in `[-20, 100]`, default `0`, negative rounds left of the decimal point. |
| `$concat` | `{"$concat": [e, ...]}` | String operands; an empty operand list yields `""`. |
| `$cond` | `{"$cond": [if, then, else]}` or `{"$cond": {"if": e, "then": e, "else": e}}` | All three parts required. Lazy: only the taken branch is evaluated. Truthiness is Mongo's: `false`, `0`, `null`, missing → false; everything else — including `""`, `[]`, `{}` — true. |
| `$switch` | `{"$switch": {"branches": [{"case": e, "then": e}, ...], "default": e?}}` | At least one branch; cases evaluate lazily in order, first truthy case wins; no match falls to `default`. |
| `$ifNull` | `{"$ifNull": [e, e, ...]}` | At least two operands (Mongo 4.4 variadic form): the first non-null, non-missing value, else the last operand's value. Lazy left-to-right. |
| `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte` | `{"$eq": [a, b]}` | Exactly two operands; `true`/`false` over any value types (see the comparison note below). |
| `$cmp` | `{"$cmp": [a, b]}` | `-1`/`0`/`1`. |
| `$dateAdd` | `{"$dateAdd": {"startDate": e, "unit": u, "amount": e, "timezone"?: tz}}` | Units: `year`, `quarter`, `month`, `week`, `day`, `hour`, `minute`, `second`, `millisecond`. `year`/`quarter`/`month` are calendar-aware in the operative timezone and clamp the day of month (Jan 31 + 1 month = Feb 28); `week`/`day` add calendar days, preserving the local clock across DST; `hour` and smaller are fixed millis spans. Non-integral `amount` → `null`. |
| `$dateDiff` | `{"$dateDiff": {"startDate": e, "endDate": e, "unit": u, "timezone"?: tz, "startOfWeek"?: w}}` | Signed count of unit-boundary **crossings**, not elapsed time (day diff of 23:59 → 00:01 is `1`). `year`…`day` cross local calendar boundaries; `hour` and smaller cross absolute-millis boundaries. `startOfWeek` (default `sunday`) applies to `week` only. |
| `$dateTrunc` | `{"$dateTrunc": {"date": e, "unit": u, "binSize"?: n, "timezone"?: tz, "startOfWeek"?: w}}` | Truncates down to its `binSize`×`unit` bin (`binSize`: positive integer literal, default `1`). Bins anchor at Mongo's reference point 2000-01-01T00:00:00 in the operative timezone; `week` bins anchor at the first `startOfWeek` on or after 2000-01-01 (a Saturday). |
| `$year`, `$week` | `{"$year": e}` or `{"$year": {"date": e, "timezone"?: tz}}` | `$week` is the Sunday-based week of year `0`–`53` (days before the year's first Sunday are week `0`) — not the ISO week. |

A single non-array operand is Mongo's shorthand for a one-element list
(`{"$abs": "$x"}`); arity is checked at parse time with structured errors.

> **Comparison order** is the engine's canonical anyenc value order — the same
> order `$sort` and `$min`/`$max` use: values order by type tag first
> (`null < number < string < false < true < array < object < ... < dateTime`),
> then by value within a type (numbers numerically, strings bytewise, arrays
> elementwise, objects by their marshaled bytes). This **differs from BSON's
> canonical cross-type order** (where e.g. booleans sort after strings and
> object comparison ignores field order) — object equality here is
> field-order-sensitive, consistent with `$group` key equality. A missing
> operand compares as `null` (`{"$eq": ["$nope", null]}` is `true`), and
> `-0` equals `0`. Vector values order by their encoded bytes too (aligned
> with `$sort`) — unlike query filters, where ordering operators against a
> vector are always false (Rule V).

> **Null instead of runtime errors** (divergence from Mongo): evaluation is
> streaming with no per-document error channel, so conditions Mongo reports as
> query errors yield `null` instead — a non-numeric operand of an arithmetic
> operator, a non-string operand of `$concat`, division by zero, a non-finite
> result (overflow, NaN), an out-of-range or non-integer `$round` place, and a
> `$switch` with no matching case and no `default` (Mongo raises). Null and
> missing operands also yield `null` (as in Mongo).
>
> `$round` precision is float64: values round by their binary double value
> (`{"$round": [2.345, 2]}` is `2.35` — the stored double sits above the
> midpoint; `{"$round": [1.25, 1]}` is `1.2` — an exact tie, half to even),
> and a `place` beyond float64 resolution returns the value unchanged.

> **Date operators** work over the dateTime value type (`{"$date": ...}` in
> JSON). `unit`, `timezone`, `startOfWeek` and `binSize` are **parse-time
> literals** — an expression there is a parse error (divergence: Mongo
> accepts dynamic values; a literal resolves the `*time.Location` once at
> parse time). `timezone` is an Olson name (`"Europe/Berlin"`) or a fixed
> offset (`"+02:00"`, `"-0500"`, `"+02"`); default UTC. A date operand
> (`startDate`/`endDate`/`date`, and the date side of `$add`/`$subtract`)
> that is null or missing → `null`, and any other non-dateTime type → `null`
> too (divergence: Mongo errors — same no-error-channel rationale as above);
> an unrepresentable result (overflow past the int64-millis range) is also
> `null`. Around DST transitions: a computed wall time that a fall-back
> repeats resolves to its **earlier** occurrence, one that a spring-forward
> skips normalizes forward, and sub-day `$dateTrunc` subtracts the wall-clock
> residue on the absolute timeline, so it stays monotone and idempotent
> across the transition. `$dateDiff`'s `hour`/`minute`/`second` boundaries
> are absolute UTC millis, so `timezone` affects `day` and larger units only.
> `$dateDiff`, `$year` and `$week` return float64 numbers;
> `$dateAdd`/`$dateTrunc` return dateTimes. Millis in numeric form (date
> differences, `$add`/`$subtract` shifts) are float64: exact only up to 2^53
> ms, i.e. within roughly year ±285,000.

## 2.1 $expr: expressions as $match predicates

`{"$match": {"$expr": E}}` evaluates the aggregation expression `E` per
document and keeps the row when the result is truthy (`$cond` truthiness:
`false`, `0`, `null`, missing → drop; everything else — including `""`, `[]`,
`{}` — keep). This enables field-to-field predicates the filter language
cannot express:

```json
{"$match": {"$expr": {"$gt": ["$allocated", "$capacity"]}}}
```

- `$expr` may coexist with ordinary filter keys — `{"$match": {"cat": "a",
  "$expr": E}}` means `cat = "a" AND E` — and may appear inside a top-level
  `$and` array (a conjunction splits cleanly). Under `$or`/`$nor` it is
  rejected with a dedicated parse error, and inside a field condition
  (`{"a": {"$not": {"$expr": ...}}}`) it is an unknown operator — no silent
  misparse.
- `$expr` is **always a residual per-document predicate** (Mongo semantics):
  it never becomes index bounds. In a leading `$match`, the ordinary filter
  keys are still pushed into the access plan (indexes, CBO) with the
  expression applied as a residual on top; a pure-`$expr` `$match` at the
  pipeline head is a full scan. A following `$sort` can still push (filtering
  a sorted stream preserves order), but `$skip`/`$limit` stay in-pipeline —
  they must apply after the predicate.
- The same contract holds inside one `$match` mixing `$expr` with `$text` or
  `$knn`: the ordinary keys reach the planner with the ranked source as usual,
  while `$expr` filters **after** the ranked scan — for `$knn` that means
  after the `$k`-bounded page, so it can shrink the result below `$k`.
- After `$group`/`$project`/..., `$expr` sees that stage's output (e.g.
  accumulator fields).
- Evaluation is alloc-free in steady state, like the other streaming stages.

`Find()` filters do **not** accept `$expr` — it is rejected as
`unknown operator: $expr` (`query.ErrUnknownOperator`). Field-to-field
predicates belong in an aggregation `$match`:
`coll.Aggregate('[{"$match": {"$expr": ...}}]')`.

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

## 3.1 $lookup: self-join point lookup

```json
{"$lookup": {"localField": "refs", "foreignField": "id", "as": "linked"}}
```

`$lookup` is scoped to the case the data model makes cheap: relation values
are object ids and all objects live in one collection, so the join is a point
lookup per streamed row. For each row it reads `localField` (any field path),
resolves the value(s) as primary keys of the **same collection**, and sets
`as` (same naming rules as `$project` outputs, replacing any existing field)
to the array of matched full documents — always an array (Mongo semantics),
empty when nothing matches.

- `from` is optional; when present it must name the aggregated collection
  itself — anything else fails `Iter`/`Count`/`Explain` with an error naming
  both collections. `foreignField` is optional and must be `"id"` (the
  primary key); any other value is a parse error. The pipeline/`let` form is
  a parse error too.
- A missing or **null** local value yields `[]`. Divergence from Mongo: the
  primary key is never null, so `$lookup` never does Mongo's null-matching
  join.
- An **array** local value is set membership (Mongo semantics): elements are
  deduplicated by first occurrence, and the output keeps first-occurrence
  order (Mongo leaves the order unspecified). A null element is skipped; an
  element of a type no stored key has (or a dangling id) simply doesn't
  match — no error.
- A document may match itself (single hop, no recursion).
- Expression paths do not traverse into the `as` array: `"$linked.id"` yields
  missing, unlike Mongo's array-collecting path semantics (pre-existing
  expression behavior). `$unwind` the `as` field before referencing subfields
  of the matched documents.
- Point lookups run inside the **same snapshot** the pipeline streams from,
  at any pipeline position — after `$group`, `localField` can name a group
  key, resolving keys back to their documents:

```json
[{"$group": {"_id": "$assignee", "n": {"$count": {}}}},
 {"$lookup": {"localField": "id", "as": "assigneeDoc"}}]
```

The stage is streaming and alloc-free in steady state for single-id lookups
(fetched documents reuse per-stage buffers); an id array only allocates while
growing the stage's high-water match count.

## 3.2 $facet: sub-pipelines over one scan

```json
[{"$match": {"space": "s1"}},
 {"$facet": {
     "total":  [{"$count": "n"}],
     "byType": [{"$group": {"_id": "$type", "n": {"$count": {}}}}],
     "recent": [{"$sort": {"modified": -1}}, {"$limit": 5}]
 }}]
```

`$facet` feeds every input row to each named sub-pipeline and emits **exactly
one document** `{"total": [...], "byType": [...], "recent": [...]}` — each
field the full result array of its sub-pipeline. This is the dashboard
pattern: N widgets over one shared scan instead of N independent scans.

- At least one facet; names follow output-field naming rules; each value is a
  non-empty pipeline of any supported stage **except `$facet` itself** (no
  nesting, as in Mongo). `$lookup` inside a facet works, at the same
  snapshot.
- Empty input yields empty arrays (`$count` still emits its zero row, as it
  does standalone).
- A `$match` **before** `$facet` participates in prefix pushdown as usual —
  that shared indexed scan is the point. A `$match` at the head of a
  sub-pipeline filters the shared stream in-flight and never becomes index
  bounds; `$text`/`$knn` are therefore rejected inside facets (section 4).
- Facet result arrays are inherently buffered: their bytes count against the
  shared memory budget (section 5). Sub-pipeline `$sort`/`$group` stages keep
  their own bounds, including the `$sort`+`$limit` top-K fold.
- Stages after `$facet` see the single result document (`$unwind` a facet
  array to keep processing it).
- The fan-out itself does not allocate per row; once every facet has
  satisfied a `$limit`, the scan stops early.

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
  I/O;
- `$expr` predicates in the leading `$match` chain never enter the plan: the
  ordinary keys push down, the expressions run as a residual streaming `$match`
  (section 2.1) — visible in `Explain` as a `Stages:` entry.

Pushdown stops at the first `$group`/`$project`/`$addFields`/`$unwind`/
`$count`/`$lookup`/`$facet` or any out-of-canonical-order stage; the remainder runs in-pipeline. An
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
Blocking stages (`$group`, in-pipeline `$sort`, `$facet` result buffers)
retain data and are bounded; exceeding a bound aborts the iteration with a
sentinel error:

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
