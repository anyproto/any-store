# Query filter contract

The `query` package guarantees, for every `Filter` produced by `ParseCondition`
or the exported constructors (`NewComp`, `NewCompValue`, `NewInValue`):

1. **Build once, reuse concurrently.** A built filter is immutable. It may be
   shared by any number of queries running in parallel; `Ok` and `IndexBounds`
   take no locks and never mutate filter state. Corollary for hand-constructed
   filters: do not mutate exported fields (`Comp.EqValue`, `In.Values`) after
   the filter has been handed to a query.

2. **Alloc-free evaluation.** `Ok` performs zero heap allocations once the
   per-query `*syncpool.DocBuffer` is warm. Per-document scratch lives in that
   buffer (one per executing query), never in the filter.

3. **Read-only bound bytes.** `IndexBounds` may return bounds whose
   `Start`/`End` alias filter-owned memory (`Comp.EqValue`, `In`'s interned
   keys). These slices are clipped to `cap == len`, so planner bound-extension
   appends (`AdjustBoundsForNonUnique`, `padReverseBounds`) reallocate instead
   of writing into memory shared across queries. Code consuming bounds must
   never write `Start`/`End` in place.

4. **Two bound channels.** `Filter.IndexBounds` is the WIDE channel: a sound
   over-approximation (superset of every matching doc's index entries) —
   `And` keeps only the first contributing same-field conjunct, which is what
   keeps array/multi-key seeks correct (docs/known-issues.md I-04).
   `TightIndexBounds` (query/tight_bounds.go) is the TIGHT channel: same-field
   conjuncts interval-intersected via `Bounds.Intersect`, plus an explicit
   `empty` flag for provably-empty value sets. Tight bounds are safe for cost
   estimation always, and for seeks/emptiness decisions only where fan-out
   entries provably cannot exist (pk namespace, scalar-proven indexes) — see
   the `TightIndexBounds` doc comment. Rules 1-3 apply to both channels.

Pinned by `query/filter_contract_test.go`: `TestFilterConcurrentReuse` (run
under `-race`), `TestFilterOkAllocFree`, and
`TestIndexBounds_FilterOwnedBytesAreCapped`; the tight channel additionally by
`query/tight_bounds_test.go` (`TestTightIndexBounds_CapClipped`,
`TestTightIndexBounds_SubsetOfWide`).

Note: the contract covers filters only. `Sort`/modifier values are cheap to
build per query and carry no such guarantee.

5. **Source filters do not evaluate.** `Text` and `Knn` are SOURCE filters:
   matching is performed by an index scan the executor builds, not by `Ok`.
   Their `Ok` is a fail-direction choice, not a predicate: `Text.Ok` returns
   `true` unconditionally (fail-open), `Knn.Ok` returns `false` (fail-closed —
   a leaked `$knn` must match nothing rather than everything, because the
   query verbs include `Delete`). External consumers that post-filter by
   calling `Filter.Ok` directly (the subscription pattern) MUST reject filters
   containing either — detect them with `query.ContainsSourceFilter` (walks
   the whole tree).

   Corollary for CUSTOM `Filter` implementations: **never embed a source
   filter inside one.** The detection/rejection walks descend only the
   package's own node types (`And`/`Or`/`Nor`/`Not`/`Key`, value and pointer
   forms alike) — a foreign type is structurally opaque, so an embedded `Knn`
   silently matches nothing (fail-closed inherited through a pass-through
   wrapper) and an embedded `Text` silently matches everything, on every
   verb, `err == nil`. A custom filter that INVERTS its inner `Ok` reflects
   fail-closed into match-all, exactly like `Not` would — that is arbitrary
   user matching code, outside what any walk can guard. Pinned by
   `TestKnn_InsideCustomFilterFailsClosed`.

6. **`TypeVectorF32` is not orderable (Rule V).** In `Comp`, an ordering op
   (`$gt`/`$gte`/`$lt`/`$lte`) evaluates to `false` whenever either side is a
   packed vector — including vector-vs-vector. `$eq` is byte equality, `$ne`
   its negation. The parser additionally rejects an ordering op whose OPERAND
   is a `$vector` literal (`ErrVectorNotOrderable`), which also keeps it out
   of `$not`. Consequence (deliberate, Mongo-style type bracketing):
   `{"v":{"$not":{"$gt":1}}}` matches a vector-valued `v` — `$not` of an
   unsatisfiable comparison is true.

7. **`$vector`, `$oid` and `$binary` are forbidden as option-key names**
   inside any operator's options object. anyenc decodes a single-key
   `{"$vector":[…]}` (et al.) object into a typed VALUE before the query
   parser sees it, so an options object whose sole key were one of these
   would change type depending on which other options are present. This is
   why `$knn`'s payload key is `$query`.

8. **A filter overriding `Ok`'s truth direction must be checked against
   `GuaranteesPresence`.** It probes the inner filter's `Ok` directly
   (`!Ok(nil) && !Ok(null)` ⇒ "guarantees presence") — a fail-closed `Ok`
   reads as the AGGRESSIVE answer and feeds sparse-index selection. Source
   filters get explicit `false` arms there; any future `Ok`-overriding filter
   needs the same.

9. **Null matches missing.** A missing field evaluates as an explicit `null`
   throughout the filter surface: `Ok` receives a nil `*anyenc.Value`, the
   index stores the doc under the `TypeNull` key, and every equality-family
   operator treats the two identically — `{"$eq":null}`, `{"$in":[…,null,…]}`
   match missing fields; `{"$ne":null}`, `{"$nin":[…,null,…]}` exclude them
   (Mongo's null model). Sparse-index selection follows automatically:
   `GuaranteesPresence` probes `Ok(nil)`/`Ok(null)`, so an operator matching
   either keeps sparse indexes out of the plan.

10. **Array sort keys are the min/max element.** A sort field holding a
    non-empty array sorts by its MINIMUM element ascending / MAXIMUM element
    descending — chosen from all elements, independent of any query predicate
    (MongoDB ≥ 4.4 semantics, SERVER-19402). This is one definition shared by
    every consumer: `SortField.AppendKey`, the raw fast path
    (`AppendKeyRaw`, byte-identical — `TestSortAppendKeyRawParity`), the
    aggregation `$sort` stage, and index-order-providing scans (the planner
    demotes `ExactSort` to an in-memory sort whenever the index's intrinsic
    order could differ: any sort run on a compound index — its whole-array
    entries can precede the key element in either direction, since element
    types tagged above `TypeArray` exist — or a single-field sort field with
    a lower cut ascending / an upper cut descending — unless the index is
    scalar-proven via its sticky multikey flag).
    Documented divergences from Mongo, both deliberate: an EMPTY array sorts
    by its whole-array encoding (after scalars, where the index stores its
    only entry; Mongo sorts `[]` before null — matching that would need a key
    encoding below `TypeNull` on disk), and cross-type order is anyenc tag
    order, not the BSON type order, as everywhere else in this engine.

11. **`$regex` is RE2, case-sensitive by default; `$options` is its only
    modifier.** The pattern is compiled by Go's `regexp` (RE2 syntax — no
    backreferences or lookarounds), unanchored, case-sensitive. Mongo-style
    `{f: {"$regex": "...", "$options": "i"}}` is accepted with the flags RE2
    shares with Mongo: `i` (case-insensitive), `m` (multiline `^`/`$`), `s`
    (dot matches newline); Mongo's `x` and `u` are rejected (`ParseError`,
    `Op: "$options"`). `$options` is in the operator vocabulary but is NOT a
    predicate: it must accompany a `$regex` in the same condition object
    (standalone or top-level use is a parse rejection), and it compiles into
    the sibling `Regexp` filter — equivalent to prefixing the pattern with
    `(?flags)`. Inline flag groups in the pattern itself remain legal.
    Duplicate `$options` keys collapse last-wins in the JSON parser (standard
    JSON behavior); the surviving occurrence is validated like any other.
    Anchored-prefix index bounds (`^literal…`) are suppressed exactly when a
    flag can widen the match: `i` (case folding) and `m` (any-line anchoring)
    keep the scan wide — via `$options` or a leading `^(?i)` in the pattern —
    while `s` only changes what `.` matches and keeps the prefix bounds.
    Pinned by `TestRegexp` (query/filter_test.go) and the `$options` cases in
    `TestParseError`.

12. **Parse rejections are structured.** Everything `ParseCondition`,
    `ParseModifier`, and the aggregation pipeline parser reject — unknown
    operator, wrong operand type, malformed `$and`/`$or`/`$nor` array, bad
    `$regex`, unknown modifier, unknown stage, … — is reported as a
    `*query.ParseError` whose `Source` names the grammar (`"filter"`, the
    default when empty; `"modifier"`; `"pipeline"`), whose `Path` locates the
    offending key inside the input document (`"tags.$sizee"`,
    `"$and.1.price.$gt"`, `"$inc.count"`; pipeline paths lead with the stage
    index: `"1.$match.a.$gt"`), whose `Op` names the operator at fault, and
    whose `Reason` is a self-contained message. Finer classes stay reachable
    through `errors.Is` (`ParseError.Err`, the `Unwrap` target):
    `ErrUnknownOperator` for vocabulary misses across all three grammars,
    `ErrVectorNotOrderable` for ordering ops on vector operands. A known
    operator in a position that does not accept it (`{"$eq":1}` at top
    level, `{"$set":{"$a":1}}`) is deliberately NOT `ErrUnknownOperator`.
    There is no swallow-and-fallback anywhere in the grammars: a `$pull`
    object operand is a condition (as in Mongo), and a malformed one is a
    rejection, never a literal-equality pull — a swallowed error would make
    the same bytes mean different pulls across library versions in a
    multi-process deployment.
    Each vocabulary is data: `query.Operators()`, `query.ModifierOperators()`,
    `anystore.AggregateStages()` and `anystore.AggregateAccumulators()`
    return exactly what the parsers recognize, so callers advertising a
    grammar (docs, 400 payloads) never hand-copy the lists. Pinned by
    `query/errors_test.go` (`TestParseError`, `TestParseModifierError`,
    `TestParseConditionErrorsAreStructured`, `TestOperators`,
    `TestModifierOperators`) and `internal/aggregate/pipeline_parse_test.go`
    (`TestPipelineParseError`, `TestStages`, `TestAccumulators`).
