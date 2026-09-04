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
   keys, the static type-edge table). These slices are clipped to
   `cap == len`, so planner bound-extension appends
   (`AdjustBoundsForNonUnique`, `padForwardBounds`, `padReverseBounds`)
   reallocate instead of writing into memory shared across queries. Code
   consuming bounds must never write `Start`/`End` in place.

4. **Two bound channels.** `Filter.IndexBounds` is the WIDE channel: a sound
   over-approximation (superset of every matching doc's index entries) —
   `And` keeps only the first contributing same-field conjunct, which is what
   keeps array/multi-key seeks correct.
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
   packed vector — including vector-vs-vector, which is stricter than item 13's
   bracketing. `$eq` is byte equality, `$ne` its negation. The parser
   additionally rejects an ordering op whose OPERAND is a `$vector` literal
   (`ErrVectorNotOrderable`), which also keeps it out of `$not`. Consequence
   (item 13 again): `{"v":{"$not":{"$gt":1}}}` matches a vector-valued `v` —
   `$not` of an unsatisfiable comparison is true.

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
    scalar-proven via its sticky multikey flag. A cut that is only a type
    bracket edge (item 13) is opened back up instead of demoting: the scan
    then covers the pre-bracketing range, whose open side cannot hide the
    extremum element, and the residual filter still applies the bracket).
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

13. **Ordering predicates are type-bracketed; sort is not.** `$gt`/`$gte`/
    `$lt`/`$lte` compare only inside one type bracket, as MongoDB's query
    operators do: a value of another type is never less or greater, it is not
    matched. A bracket is the anyenc type tag, except that `false`/`true` form
    one bracket (`{"$gt":false}` matches `true`); numbers already share one
    type. A missing field is `null` and so in no other bracket:
    `{"$lt":5}` never matches an absent field, `{"$gte":null}`/`{"$lte":null}`
    match null and missing, `{"$gt":null}`/`{"$lt":null}` match nothing.
    Array fields bracket per element (a whole-array comparison needs an array
    operand). `$eq`/`$ne`/`$in`/`$nin` were already type-strict and are
    unchanged (`$ne 5` still matches a string, as in Mongo). `Comp.IndexBounds`
    emits the bracket-clamped range — `$gt X` is `(X, <next tag>)`, `$lt X` is
    `[<tag>, X)` — so bounds stay the exact value image of the predicate (the
    planner's residual elisions depend on that, together with its key-suffix
    pads) and a wrong-typed literal is an empty seek, not an index walk. The
    bracket edges are marked on the `Bound` (`StartIsTypeEdge`/`EndIsTypeEdge`)
    so the planner can tell an edge from a value cut. Shapes with nothing to
    seek contribute no bounds: an empty half-open range (`{"$lt":null}`,
    `{"$lt":false}`), an empty operand, and an ordering op against a vector
    (Rule V). `$pull` conditions are query predicates and bracket the same
    way. `Sort`, index-order scans and the aggregation
    expression operators (`$gt`, `$cmp`, `$min`/`$max`, `$sort`) keep the full
    anyenc tag order, exactly as Mongo's sort and `$expr` keep full BSON order.
    Pinned by `TestComp_TypeBracketing`, `TestCompOkScalar_MatchesMarshalReference`
    (the oracle carries the rule) and `TestGuaranteesPresence` (`$lt`/`$lte`
    with a non-null operand guarantee presence: they reject null and missing).

14. **Dotted paths traverse arrays of objects (MongoDB field-path
    semantics).** A path resolves to a LEAF SET (`anyenc.Value.AppendLeaves`):
    an object descends by key; an array met by a non-numeric segment maps the
    rest of the path over its elements — an object element continues, an
    element that cannot carry the path (scalar, null, nested array, object
    lacking the key) is a MISSING leaf, and an empty array is one missing
    leaf; a scalar met mid-path is a missing leaf. `{"a.b":1}` matches
    `{"a":[{"b":1},{"b":2}]}`; traversal repeats at every array level. A leaf
    that is itself an array keeps the element-wise rules of item 13 (whole
    array for an array operand, per element otherwise).
    Quantification is per operator, as in Mongo: a positive predicate
    (`$eq`, ranges, `$in`, `$regex`, `$exists`, `$type`, `$size`) matches when
    ANY leaf satisfies it; a negation (`$ne`, `$nin`, `$not`, `$nor`) when NO
    leaf satisfies the negated predicate; sibling operators on one path pick
    their witnesses independently — `{"a.b":{"$gt":1,"$lt":3}}` matches
    `[{"b":1},{"b":5}]`. `$elemMatch` (item 15) binds them.
    A NUMERIC segment indexes an array (`"a.0"`, engine extension over
    Mongo's positional-or-key ambiguity) and the leaf it picks is POSITIONAL:
    compared whole even when it is an array (`{"a.0":1}` does not match
    `{"a":[[1,2]]}`; `{"a.0":[1,2]}` does), stored whole by an index, and its
    whole value is its sort key. Traversal resumes on later segments.
    One definition feeds every consumer: `Key.Ok` (`LeafFilter`), index
    entries (one per `anyenc.AppendIndexValues` value, fanning out like a
    leaf array — the multikey flag, per-doc dedup and sparse handling follow;
    a sparse index drops a BRANCH whose leaf is null or missing and the doc
    only when no branch survives), sort keys (min/max over
    `anyenc.AppendElementValues`, item 10) and `CanonicalKeyDedupIter`. The
    raw fast paths (`OkRaw`, `AppendKeyRaw`) decline at an array container
    and fall back to the parsed document. `$type` matches an array whose
    ELEMENT has the type as well as the array itself (`"array"`), and
    accepts Mongo's `"bool"` alias.
    Deliberate divergences from Mongo, pinned by the MongoDB-fixture replay
    `TestMongoArraySemantics` in the `any-store-tests` repository: (a) the null-equality
    family (`$eq null`, `$in [null]`, and their negations) matches a missing
    leaf produced by an element that cannot carry the path or by an empty
    array — Mongo's matcher skips those elements, though its sort and index
    keys treat them as null; any-store keeps filter, sort and index on one
    answer; (b) numeric segments never match an object element keyed by the
    numeral; (c) an empty leaf array sorts by its whole-array key (item 10)
    and cross-type order is anyenc tag order (item 13) — Mongo sorts `[]`
    before null and orders objects type-first, then by field name.
    Pinned by `TestKey_PathThroughArrays`, `TestKey_PathThroughArrays_AllocFree`
    (item 2 holds for traversal), `anyenc.TestAppendLeaves` and
    `TestIndex_ArrayNested_NestedField_IntermediateArray_Traversed`.

15. **`$elemMatch` binds predicates to one element.** `{"a":{"$elemMatch":
    C}}` matches when `a` is an array with an element satisfying `C` as a
    whole. The operand's first key decides the form (Mongo's rule): a
    field-level operator (`$gt`, `$in`, `$not`, `$elemMatch`, …) makes the
    VALUE form — `C` is an operator set applied to each element as ONE value
    (`ElemFilter`: an element that is itself an array is compared whole, as a
    positional leaf is); anything else (a field name, `{}`, `$or`/`$and`/
    `$nor`) makes the OBJECT form — `C` is a document condition applied to
    each OBJECT element (Mongo also admits array elements, viewed as objects
    keyed by position; here they never match). `$all` accepts a list made
    only of `{"$elemMatch": …}` objects (a conjunction of element matches;
    mixing with values is a `ParseError`). `$text` and `$knn` are rejected
    inside either form. Through a dotted path it applies to every leaf array,
    positional leaves included.
    Bounds: the value form contributes `C`'s own bounds on the field (an
    element's value is one of the field's entries) unless the path is
    positional; the object form re-keys `C` under the sub-fields —
    `{"a":{"$elemMatch":{"b":{"$gt":1}}}}` bounds `a.b` as `{"a.b":{"$gt":1}}`
    would. Both are supersets, never the exact value image (a scalar or an
    object with that value sits in the bounds without matching), so a `Key`
    holding a `$elemMatch` always keeps its residual filter: the planner's
    covering-count, verify-chain and residual-elision paths treat it as
    uncovered (`keyBoundsExact`). Pinned by `TestElemMatch_Parse`,
    `TestElemMatch_Ok` and `TestElemMatch_IndexBoundsAndString`.
