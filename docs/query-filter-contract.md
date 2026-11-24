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
   `And` keeps a single same-field conjunct, never an intersection, which is
   what keeps array/multi-key seeks correct. `IndexBounds` itself keeps the
   first contributing conjunct; `ConjunctBounds` (query/tight_bounds.go)
   enumerates each conjunct's own bounds so the planner can seed the seek
   with whichever its statistics rate most selective — every one is a sound
   seek range on its own.
   `TightIndexBounds` (query/tight_bounds.go) is the TIGHT channel: same-field
   conjuncts interval-intersected via `Bounds.Intersect`, plus an explicit
   `empty` flag for provably-empty value sets. Tight bounds are safe for cost
   estimation on an index whose entries do not outnumber its documents, and
   for seeks/emptiness decisions only where fan-out entries provably cannot
   exist (pk namespace, scalar-proven indexes) or where the conjuncts are
   bound to one element by a value-form `$elemMatch` (item 15) — see the
   `TightIndexBounds` doc comment. Rules 1-3 apply to both channels.

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
   `TestKnn_InsideCustomFilterFailsClosed` in the `any-store-tests`
   repository.

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
   (`!Ok(nil)` ⇒ "guarantees presence") — a fail-closed `Ok` reads as the
   AGGRESSIVE answer and feeds sparse-index selection. Source filters get
   explicit `false` arms there; any future `Ok`-overriding filter needs the
   same.

9. **Null matches missing.** A missing field evaluates as an explicit `null`
   throughout the filter surface: `Ok` receives a nil `*anyenc.Value`, the
   index stores the doc under the `TypeNull` key, and every equality-family
   operator treats the two identically — `{"$eq":null}`, `{"$in":[…,null,…]}`
   match missing fields; `{"$ne":null}`, `{"$nin":[…,null,…]}` exclude them
   (Mongo's null model). Only a sparse index tells the two apart: it holds
   an explicit null under the `TypeNull` key and has no entry for a missing
   field. Sparse-index selection follows automatically: `GuaranteesPresence`
   probes `Ok(nil)`, so an operator matching a missing field never guarantees
   presence on its own, and keeps sparse indexes out of the plan when it also
   contributes bounds — `$ne` aside, whose bounds hold every key the index
   wrote. A sparse index complete for the filter answers `{"$exists":true}`
   by a whole-index scan. One over a dotted path never provides sort order:
   a document fanning out through an array of objects can keep a single key
   while its sort key is a missing leaf.

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
    Index bounds seek the prefix range of the pattern's anchored literal
    head, read off the parsed syntax tree so it means what the engine
    means: a quantifier binds the literal before it (`^ab*` seeks `a`,
    `^ab+` seeks `ab`), an escape class is no literal (`^\d` seeks
    nothing), a top-level alternation has no head, and `i` (case folding)
    or `m` (any-line anchoring) — via `$options` or an inline flag group —
    keeps the scan wide, while `s` only changes what `.` matches. The range
    is the exact match set only when the pattern is that literal and
    nothing more; any other pattern keeps its residual filter
    (`query.IndexBoundsExact`). Pinned by `TestRegexp` and
    `TestRegexp_LiteralPrefix` (query/filter_test.go), the `$options` cases
    in `TestParseError`, and the oracle runs `TestRegexPrefixBounds_Oracle`
    and `TestResidualElision_InexactBounds` in the `any-store-tests`
    repository.

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
    The empty shapes of the logical operators follow MongoDB. An empty
    object `{}` is the condition every document satisfies, as the whole
    filter and as an element of `$and`/`$or`/`$nor`: `{"$or":[{},{"a":1}]}`
    matches everything, `{"$nor":[{}]}` nothing. An empty ARRAY under
    `$and`, `$or` or `$nor` is a rejection at every level, alone or next to
    another key (`Reason`: `"$or must be a non-empty array"`). An empty
    disjunction is false and an empty conjunction true, so accepting the
    shape would make a filter built from an empty list match nothing or
    everything depending on the operator, and a `Delete` with it could empty
    the collection; a caller that means "every document" passes `{}` or a
    nil filter. The Go values are not parsed and keep that arithmetic:
    `query.And{}` and `query.Nor{}` match every document and `query.Or{}`
    none, on every verb, `Delete` included; build one from a list only
    where an empty list should mean that. Pinned by
    `TestParseCondition_EmptyObjectMatchesAll` and
    `TestParseCondition_EmptyLogicalArrayRejected` (query/cond_parse_test.go)
    and, through every verb, by `TestQuery_EmptyObjectInLogicalOperator` and
    `TestQuery_EmptyLogicalArrayIsParseError` (test/query_test.go).
    `{"tags":{"$all":[]}}` is accepted and matches no document, as in MongoDB: not
    one whose array is empty, not one without the field. It is the empty
    `$in`, so a conjunct of it makes the whole filter match nothing (the
    query layer answers without a scan, as for `{"$in":[]}`), and its
    index bounds are empty: it narrows no seek, and the residual filter
    rejects every row. Pinned by
    `TestParseCondition_AllWithEmptyArrayMatchesNothing` and, through every
    verb and with an index on the field, by
    `TestQuery_AllWithEmptyArrayMatchesNothing`.
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
    a sparse index holds a document only when every field exists somewhere
    in it (an explicit null exists), and then writes every key that has at
    least one present field — `{"a":[{"b":1},{"c":2}]}` under a sparse
    `(a.b, a.c)` has keys `(1, null)` and `(null, 2)`; fields of a COMPOUND
    index that run through the same array iterate it together, one entry per
    element, as Mongo generates them — never a cross product; a scalar, null
    or nested-array element is a missing leaf in every one of them
    (`{"x":[[{"y":1,"z":2}]]}` under `(x.y, x.z)` has the key
    `(null, null)`) — so the planner compounds bounds and entry-level cover
    filters across such fields only on a scalar-proven index and seeks the
    first of them otherwise), sort keys (min/max over
    `anyenc.AppendElementValues`, item 10) and `CanonicalKeyDedupIter`. The
    raw fast paths (`OkRaw`, `AppendKeyRaw`) decline at an array container
    and fall back to the parsed document. `$type` matches an array whose
    ELEMENT has the type as well as the array itself (`"array"`), and
    accepts Mongo's `"bool"` alias. `$type: "null"` needs an explicit null:
    a missing field has no type. Its index bounds cover the null key, which
    a missing field and an empty array also occupy, so it keeps its
    residual filter (`query.IndexBoundsExact`); every other `$type` is
    exactly its type range.
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
    (item 2 holds for traversal), `anyenc.TestAppendLeaves`, and
    `TestIndex_ArrayNested_NestedField_IntermediateArray_Traversed` in the
    `any-store-tests` repository.

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
    Bounds: the value form contributes the intersection of `C`'s bounds on
    the field (an element's value is one of the field's entries, and every
    operator of `C` holds for that one element, so the intersection is a
    sound seek range even over an array); it contributes nothing when the
    path is positional, when `C` nests another `$elemMatch`, or when the
    intersection is provably empty (no element can satisfy `C`; the residual
    rejects every row). The object form re-keys `C` under the sub-fields —
    `{"a":{"$elemMatch":{"b":{"$gt":1}}}}` bounds `a.b` as `{"a.b":{"$gt":1}}`
    would. Both are supersets, never the exact value image (a scalar or an
    object with that value sits in the bounds without matching), so a `Key`
    holding a `$elemMatch` always keeps its residual filter: the planner's
    covering-count, verify-chain and residual-elision paths treat it as
    uncovered (`query.IndexBoundsExact`). Pinned by `TestElemMatch_Parse`,
    `TestElemMatch_Ok`, `TestElemMatch_IndexBoundsAndString` and
    `TestCBO_ElemMatch_RangeOnArrayIndex`.

16. **Rows with equal sort keys have no specified order.** An in-memory sort
    orders a tie group by document id; an index that provides the order yields
    it as its own keys run — by the index's remaining fields, then by id, and
    in a reverse scan backwards — so the same query can order a tie group
    differently under two plans, and a `Limit` that cuts through the group
    returns different rows. A total order needs a unique last sort field:
    `Sort("field", "id")`.

17. **Update modifiers treat missing and mistyped fields as MongoDB does.**
    A missing field is created by `$set`, by `$inc` (with the increment as
    its value), and by `$push` and `$addToSet`, which make it an array
    holding the value: `{"$push":{"tags":"a"}}` on `{"id":1}` gives
    `{"id":1,"tags":["a"]}`. `$unset`, `$rename`, `$pop`, `$pull` and
    `$pullAll` leave a missing field missing and create no part of a missing
    path: `{"$pull":{"meta.old":"x"},"$set":{"seen":true}}` on `{"id":1}`
    gives `{"id":1,"seen":true}`, with no `meta`, and so does a path through
    an index past an array's end (`items.3.tags` on `{"items":[]}`). A path
    the document cannot hold, through a field of a scalar or a name in an
    array (`a.b` on `{"a":1}`, `items.bad.tags` on `{"items":[]}`), is a
    no-op for `$unset` and an error for `$rename`'s source, `$pop`, `$pull`
    and `$pullAll`, as in MongoDB (`PathNotViable`). An array operator
    (`$push`, `$addToSet`, `$pop`, `$pull`, `$pullAll`) on a field that
    holds anything else, `null` included, is an error (`$push: field 'tags'
    must be an array: …`), and `$inc` on a non-number is one too; the update
    then writes nothing. `$push` and `$addToSet` take one value: MongoDB's
    modifiers (`$each`, and for `$push` `$position`, `$slice` and `$sort`)
    are not supported, and an operand object with a key that starts with
    `$` is a parse error, not a value to store. `$set`, `$push` and
    `$addToSet` store a copy of their operand, as MongoDB copies it into
    each document (a number, a bool, null, an object id or a date, which
    nothing changes in place, is stored as itself): every document an
    update reaches gets a value of its own, and a later operation of the
    same modifier that changes it
    (`{"$push":{"p":{"n":1}},"$inc":{"p.0.n":1}}`), or a write through the
    bytes a result's `GetStringBytes`, `GetBytes` or `GetVectorF32` returns,
    changes that document's copy, never the modifier or the next document.
    A modifier given as an `*anyenc.Value` is copied when it is parsed, and
    nests at most 512 levels, the modifier object being the first.
    `UpsertId` of an id with no document inserts what the modifier makes of
    `{<primary key>: id}`, as MongoDB's upsert inserts the query's equality
    fields with the update applied: `{"id":1,"tags":["a"]}` for the `$push`
    above, and `{"id":1}` alone for a modifier that reports no change (a
    lone `$pull` or `$unset`), built from the id whatever such a modifier
    did to the document it was handed. A modifier that changes the id fails
    with `ErrPrimaryKeyModification` there as on an existing document.
    Pinned by `TestModifierPush_Modify` and its siblings,
    `TestModifier_RemovalsCreateNoPath`,
    `TestModifier_RemovalPathViability`,
    `TestParseModifier_PushModifiersRefused`,
    `TestModifier_ObjectOperandsAreCopied`,
    `TestModifier_ByteOperandsAreCopied` and
    `TestParseModifier_GoValueBoundAndCopied` (query/modifier_test.go), and
    through the collection by `TestCollection_PushMissingField`,
    `TestCollection_RemovalPathViability`,
    `TestCollection_UpsertIdNoOpInsertsId`,
    `TestCollection_UpsertIdUnchangedInsertsId` and
    `TestCollection_UpsertIdKeepsTheId` (test/collection_test.go).
    A numeric path segment sets an array element, padding the array with
    nulls up to it (`{"$set":{"arr.3":1}}` on `{"arr":[]}` gives
    `[null,null,null,1]`), but by no more than 1,500,000 nulls, MongoDB's
    limit: a segment that needs more is an error (`can't backfill more than
    1500000 elements`) and the update writes nothing. Pinned by
    `TestModifier_ArrayPaddingIsBounded` and
    `TestModifier_PaddingRefusalLeavesDocumentUnchanged`.

18. **Numbers are float64.** anyenc stores every number as a float64, so an
    integer is exact up to 2^53 (9,007,199,254,740,992) in magnitude and a
    larger one, given as a Go integer or written as a plain integer literal
    in JSON, is the nearest float64 — in a stored document, a filter
    operand, an index key and a document id alike. (The JSON parser rounds
    a number written with an exponent twice, the digits and then the
    scaling, so `9007199254740993e1` becomes `90071992547409920` where
    `90071992547409930`, the same integer, becomes `90071992547409936`.)
    Two such integers can be
    one value: `{"n":9007199254740993}` matches a document whose `n` is
    `9007199254740992`, and as ids they are one key: with a document of id
    2^53 in the collection, `Insert` of id 2^53+1 fails with `ErrDocExists`,
    `UpsertOne` with it replaces that document, and `FindId(2^53+1)` returns
    it. There is no integer type next to the float (MongoDB has int, long and
    decimal); integers that can go past 2^53 belong in strings. The
    aggregation arithmetic has the same limit (docs/aggregation.md). Pinned
    by `TestCollection_IdsPast2To53AreOneKey` (test/collection_test.go).

19. **A `$text` query enforces at most 64 required clauses.** The required
    clauses are the clauses of `$search` under `"$defaultOperator": "and"`
    and then those of the `$require` entries, in that order. Each `$require`
    entry is parsed with the clause syntax, so one entry can be several
    clauses: `"red blue"` is two, `"\"red blue\""` one phrase. A clause the
    analyzer reduces to no terms does not count. The first 64 are enforced;
    every required clause after them still matches and scores, but a
    document need not satisfy it, as if it were a `$search` clause under the
    default `or`. `$exclude` entries have no such limit. Pinned by
    `TestFtsProbe_RequiredBitExhaustion`.

20. **A stored document nests at most 511 levels.** The document is the
    first level and each object or array in it one more. A write that would
    store a deeper one — an `Insert`, `UpsertOne` or `UpdateOne` of a value
    built in an `anyenc.Arena`, a modifier whose path or operand reaches
    past it, a `$merge`/`$out` result — fails and stores nothing. The
    parser reads containers 512 levels deep and a compressed document's
    object one level below its wrapper, so 511 is what every stored
    document reads back with. MongoDB refuses documents nested past 100
    levels; JSON input stops at 300 (the parser's limit), so only a value
    built in Go or a modifier gets near the bound. A value built in Go that
    contains itself is refused the same way, before anything walks it to
    the end: the document a `query.ModifyFunc` returns, and an operand of a
    modifier given as an `*anyenc.Value` (item 17). Pinned by
    `TestCollection_DocumentDepthBounded` and
    `TestCollection_SelfContainingValuesRefused` (test/collection_test.go).
