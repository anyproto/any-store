# BUG-32 grooming: explicit ANN query operator

**Repo:** `github.com/anyproto/any-store/v2`, branch `btree` @ `18a38bc`
**Worktree (read/edit code HERE):** `/home/che/projects/any-store/.claude/worktrees/bug-32-vector-clause`
**Companion test repo:** `/home/che/projects/any-store-tests` (module `any-store-tests`, `replace … => ../any-store`)
**Bug doc:** `/home/che/projects/any-store-tests/BUG-32-vector-clause-ignored-mass-delete.md`
**Depends on:** BUG-47 (`Update`/`Delete` drop the sort, keep the limit) — must land before Increment 2. Rediscovered by this pass; filed and fixed separately.
**Baseline note:** groomed against `18a38bc`. `btree` HEAD has since advanced (`5d6ad58`, BUG-26/BUG-43 fixes); neither touches the vector or `Comp` paths, so every file:line below still holds — **re-verify before editing anyway.**

This document supersedes the first grooming pass. It absorbs four adversarial reviews; every finding was re-verified against the code with throwaway probes (all removed). **Three of the four reviews independently found the same blocker: the previously proposed Increment-1 patch does not close the data-loss hole.** That is fixed here, and the corrected patch is empirically validated (33/33 rows, and the full existing suite green except one parity test that must be updated by design).

**Two claims in this document were re-verified by hand, from scratch, before it was accepted:**
1. `Find({"nosuchfield":{"$lt":{"$vector":[0]}}}).Delete(ctx)` → `matched=5 modified=5 err=<nil>`, collection emptied, **on a plain collection with no vector index and a field that does not exist**. The filed bug (a `$gt` on a vector-indexed field) is a *special case* of this.
2. `Sort("-p").Limit(3)`: `Iter` → ids 7, 3, 1; `Delete` → removes ids 0, 1, 2. (= BUG-47.)

---

## Decision summary

| # | question | decision |
|---|---|---|
| 1 | operator name | **`$knn`**, a **field-level** operator: `{"embedding":{"$knn":{…}}}` |
| 2 | argument shape | **options object only**, no shorthand: `{"$query": <vec>, "$k": N, "$ef": E?, "$index": S?}`. `<vec>` = plain number array **or** `{"$vector":[…]}` |
| 3 | verb semantics | one denoted set, six verbs (Iter/Count/Delete/Update/Explain/Aggregate-prefix). **No verb rejects `$knn`.** |
| 4 | `k` / `ef` / threshold | **`$k` mandatory, in the clause.** `ef` = pure function of the clause. **No threshold in v1** (`$maxDistance` reserved + rejected). |
| 5 | determinism | total order `(distance, docId)` **enforced at the source (`vivf`/`vindex`), not in `VectorIter`** — sorting the output cannot fix a non-deterministic ef-cut *membership*. |
| 6 | interactions | filter → cut-to-k → sort → page. `$and` legal at any depth; `$or`/`$nor`/`$not`/multi-`$knn`/`$knn`+`$text`/no-index/**ambiguous-index** → hard errors. |
| 7 | old bare-array syntax | **hard error** (`ErrLegacyVectorClause`), permanently. Scoped to a **`TypeArray`-of-numbers of exactly the index dim** — *not* `TypeVectorF32`, which stays an ordinary byte-equality filter. |
| 8 | Rule V (ordering vs vectors) | ordering ops are **`false`** when *either* side is a vector (two-sided guard), **plus** a parse-time rejection of an ordering op with a `$vector` **operand**. |
| 9 | `$type:"vectorF32"` | shipped, **but `TypeFilter.IndexBounds` must return `bs` verbatim for `TypeVectorF32`** — otherwise a descending index silently drops every row. |
| 10 | increments | **3 here + 1 dependency.** **1** = Rule V (kills the mass delete). **1c** = source-level determinism. Both non-breaking. **2** = `$knn` + seam + legacy error, one breaking release. **1b** (sorted+limited `Update`/`Delete`) was rediscovered by this pass but is **BUG-47**, filed and fixed separately — a *dependency* of Increment 2, not scope. |

**Ship Increment 1 and 1b today, on their own branch, regardless of what happens to the rest of this document.**

---

## Root cause

Three defects wearing one costume.

| # | defect | file:line | weapon |
|---|---|---|---|
| **D1** | `Comp` resolves cross-type comparisons on the anyenc **type tag**, and `TypeVectorF32 = 10` (`anyenc/type.go:25`) sorts above **every** scalar tag. | `query/filter.go:183-188` (`okScalar` `default:` arm), the operand-side arms at `:135`, `:146`, `:155`, and `e.comp(encodedNull)` at `:78` | `Delete` empties the collection, `err=nil` |
| **D2** | Only `Iter` runs `detectVectorQuery`. | `query.go:197` vs `Count` `:545`, `Update` `:259`, `Delete` `:423`, `Explain` `:687` — all pass `q.cond` straight to the CBO | `Count`=0, `Delete` no-op, `Explain` lies |
| **D3** | ANN intent is **inferred from clause shape**, so it is undetectable syntactically. | `vector_index.go:561-566`, `filterRefsField` `:692-709`, `hasVectorClause` `aggregate.go:138` — all switch on `query.And` only, never `*query.And` | `{"$and":[{"v":[…]},{"x":1}]}` → 0 rows, no error, **on `Iter` too** |

### D1 is much bigger than the bug report says — measured on `18a38bc`

Doc `{"n":5,"s":"abc","tags":[1,2],"v":{"$vector":[1,2,3]}}`:

| clause | `Filter.Ok` today |
|---|---|
| `{"v":{"$gt":1}}`, `{"v":{"$gte":1}}` | **true** ← the filed bug |
| `{"n":{"$lt":{"$vector":[1,2,3]}}}` | **true** ← `n` is the *number* 5 |
| `{"s":{"$lt":{"$vector":[…]}}}` | **true** ← `s` is a string |
| `{"tags":{"$lt":{"$vector":[…]}}}` | **true** ← `tags` is `[1,2]` |
| `{"nosuchfield":{"$lt":{"$vector":[…]}}}` | **true** ← the field is **absent** |

**`Find({"anyfield":{"$lt":{"$vector":[0]}}}).Delete(ctx)` empties ANY collection — no vector index required anywhere in the database.** The mechanism is `bytes.Compare` on a tag-prefixed encoding; every tag 1–9 is `< 10`.

**Neither `$knn` nor the seam fixes D1 — they *legalize* it.** Under the operator design `{"v":{"$gt":1}}` becomes "an ordinary filter over the stored field", which is *exactly the code path that empties the collection*. That is why Increment 1 exists and ships alone.

### D3, with a live wrong answer

`{"$and":[…]}` with ≥2 members parses to `*query.And` (`query/cond_parse.go:105-107`). All three shape-guessers switch on the **value** type `query.And` only. So `Find({"$and":[{"v":[…]},{"bucket":"b0"}]})` **silently returns 0 rows on `Iter`**, where the flat `{"v":[…],"bucket":"b0"}` returns real ANN hits.

---

## Design

### 1. The invariant

> **Verb Coherence.** For a fixed query `Q` and a fixed snapshot `T`, the document sequence `Rows(Q)` is the same on every verb: `Count(Q) == len(Rows(Q))`; `Delete(Q)` removes exactly `set(Rows(Q))`; `Update(Q)` visits exactly `set(Rows(Q))`; `Explain(Q)` describes the plan producing `Rows(Q)`; `Aggregate([{$match:Q},…])` streams exactly `Rows(Q)`.

Scope: **one snapshot**. `Count` and `Iter` open separate read txs (`query.go:613` vs `:146`), so a concurrent write between them can change the answer — true of every filter.

**We explicitly refuse the "compositional filter" axiom.** `⟦And{Knn, R}⟧ ≠ ⟦Knn⟧ ∩ ⟦R⟧`: the residual participates in the selection. Cut-then-filter would make `{"v":{"$knn":{…,"$k":10}}, "lang":"en"}` return ~1 row instead of 10 and destroy hybrid search. **Filter, then cut.** Every comparable engine agrees.

### 2. The operator: `$knn`, field-level

**Not `$near`.** Mongo's `$near` is a geo-radius operator denoting an *unbounded* set — precisely the mental model that produces the mass delete, and precisely what this engine cannot deliver (`vindex.Index.SearchCandidates(rtx, q, ef)` at `internal/vindex/search.go:20` and `vivf.StoreIndex.SearchCandidates` at `internal/vivf/store.go:682` are the *only* ANN entry points; both are top-`ef`). `$knn` names its own mandatory argument, is the Elasticsearch/OpenSearch/Lucene/Redis spelling, and is unclaimed by Mongo.

**Field-level**, handled in `makeCompFilter` (`query/cond_parse.go:328`), not in `parseAnd`'s top-level switch — dotted paths, `Key.Path` splitting, and the residual machinery come free. (`$text` is top-level only because it *has no field*: it grabs `loadFtsIndexes()[0]` blind, `fulltext_search.go:954`. We do not copy that wart.)

### 3. Grammar — exactly one accepted form

```
knnClause   := '"$knn"' ":" knnOptions          -- sole operator on its field
knnOptions  := "{" knnField ("," knnField)+ "}"
knnField    := '"$query"' ":" vectorValue       -- REQUIRED
             | '"$k"'     ":" int               -- REQUIRED, 1 <= k <= 10000
             | '"$ef"'    ":" int               -- optional, k <= ef <= 65536
             | '"$index"' ":" string            -- optional, vector index name
vectorValue := "[" number ("," number)* "]"            -- plain JSON array
             | "{" '"$vector"' ":" "[" number+ "]" "}" -- extjson packed literal
```

```json
{"embedding": {"$knn": {"$query": [0.1, 0.2, …], "$k": 10}}}
{"embedding": {"$knn": {"$query": {"$vector": [0.1, …]}, "$k": 10, "$ef": 400, "$index": "emb"}}}
```

**No shorthand.** A bare `{"$knn":[floats]}` would take `k` from `.Limit()` — reinstating builder-owned `k`, which is the disease. `AggQuery` has no `.Limit` and no `VectorEf` (`aggregate.go:22-52`), so any design keeping `k` in the builder has already failed its sixth verb.

**Both vector encodings accepted.** `anyenc.AppendFloat32s` (`anyenc/lookup.go:194-234`) already reads `TypeVectorF32` *and* `TypeArray`-of-numbers. This retires today's absurdity where `decodeVectorValue` (`vector_index.go:726-747`) **rejects** a `{"$vector":[…]}` literal that is the *recommended storage form*.

#### The extjson landmine

`anyenc` decodes a **single-key** `{"$vector":[…]}` object into a `TypeVectorF32` **value** *before the query parser sees it* (`anyenc/extjson.go:17-23`). Probe-confirmed:

```
{"$vector":[1,2]}          -> anyenc type = vectorF32   ← eaten
{"$vector":[1,2],"k":10}   -> anyenc type = object      ← not eaten
```

An options object whose *sole* key was `$vector` would arrive as a **vector, not an object** — its type flipping with which other options happen to be present. Hence the payload key is **`$query`**, and:

> **RULE: `$vector`, `$oid` and `$binary` are forbidden as option-key names inside any operator's options object.** Write this into `docs/query-filter-contract.md`.

Friendly error for the obvious typo: `unknown $knn field: $vector (did you mean "$query"? $vector is the value-type wrapper: {"$query":{"$vector":[…]}})`.

**Reserved and rejected in v1** (named errors, so adding them later is non-breaking): `$maxDistance`, `$minScore`, `$prefilter`, `$nprobe`.

### 4. Rejection rules

**Parse-time** (`query/cond_parse.go`; error strings normative):

| condition | error |
|---|---|
| `$knn` value is not an object | `$knn must be an object, e.g. {"$knn":{"$query":[...],"$k":10}}` |
| missing `$query` | `$knn requires $query` |
| `$query` neither a number array nor a `$vector` value | `$knn: $query must be an array of numbers or {"$vector":[...]}` |
| `$query` empty | `$knn: $query must be non-empty` |
| a `$query` element is NaN or ±Inf | `$knn: $query must contain finite numbers` |
| missing `$k` | `$knn requires $k (the number of neighbours to select)` |
| `$k` not an integer in `[1, 10000]` | `$knn: $k must be an integer in [1, 10000], got %v` |
| `$ef` present and `< $k` or `> 65536` | `$knn: $ef must be an integer in [$k, 65536], got %v` |
| `$index` not a string | `$knn: $index must be a string` |
| unknown key | `unknown $knn field: %s` (+ the `$vector` hint) |
| `$knn` mixed with another operator on the same field | `$knn must be the only operator on field %q` |
| `$knn` under `$not` | `$knn is not allowed under $not` |
| **ordering op (`$gt`/`$gte`/`$lt`/`$lte`) whose operand is a `$vector` value** | **`a vector is not orderable: $lt/$gt against {"$vector":[…]} is undefined (BUG-32)`** ← Increment 1 |

`parseKnn` **never returns `(nil, nil)`**: a `query.Key` with a nil inner `Filter` is BUG-23 (SIGSEGV in `Key.OkRaw`, `query/filter_raw.go:46`), fixed by `dabefab`.

**Detection-time** (`detectKnnQuery`) — **authoritative**, because `ParseCondition` short-circuits on an already-built `Filter` (`cond_parse.go:77-79`) and *the only two production consumers build filters programmatically*. **Every parse rule above is re-checked here**, plus:

| condition | error |
|---|---|
| field has no vector index (or `$index` names none) | **`ErrNoVectorIndex`** — *"no vector index on field %q"* |
| **>1 vector index on the field and no `$index`** | **`ErrAmbiguousVectorIndex`** — *"field %q has %d vector indexes; name one with $index"* |
| `len($query) != index.dim` | `ErrInvalidVectorQuery` — *"$knn on %q: got %d dims, index has %d"* |
| ≥2 `$knn` clauses | `ErrMultipleVectorClauses` (existing, `vector_index.go:534`) |
| `$knn` under `$or`/`$nor`/`$not`, nested inside a `Key`'s inner filter, or a bare top-level `Knn` with no `Key` | **`ErrKnnBadPlacement`** |
| `$knn` and `$text` in one query | **`ErrKnnWithText`** |
| `_distance` in filter/sort with no `$knn` | `ErrDistanceWithoutVector` (existing) — guard becomes a **full** tree walk |
| legacy bare-array clause on a vector-indexed field | **`ErrLegacyVectorClause`** |

`ErrInvalidVectorQuery` **narrows** to "malformed `$knn` argument". Its old job — "this clause targets a vector field but isn't the ANN shape" — ceases to exist. **That is load-bearing** (see Migration §Downstream).

### 5. The `Filter` type

```go
// query/filter.go

// Knn is the {"<field>":{"$knn":{"$query":[…],"$k":N}}} approximate-nearest-neighbour
// clause. It is NOT a predicate: it selects the K documents whose vector at this
// field the field's vector index ranks closest to Query. "Is this document one of
// the k nearest?" is a property of the CANDIDATE SET, not of the document, so no
// per-document truth value exists.
//
// Ok therefore returns FALSE, unconditionally, and is unreachable in a correct
// plan: the executor detects the clause on EVERY verb, drives the query from the
// ANN source, strips the clause from the residual, and hard-errors on any
// placement where it could not be stripped. The false FAILS CLOSED — a Knn that
// somehow reaches a document-by-document evaluator matches nothing rather than
// everything. Contrast query.Text, whose Ok returns TRUE (filter.go:711): that is
// BUG-21. Fail-open on Query.Delete costs the collection. Never do that here.
//
// FAIL-CLOSED IS LOAD-BEARING IN TWO PLACES, both of which must be kept in sync:
//   - ContainsKnn must walk Not/Nor: Not{Knn}.Ok == !false == MATCH-ALL. A $knn
//     that reaches a Not is BUG-32 through the front door, reachable from Delete.
//   - GuaranteesPresence must special-case Knn: it calls the inner filter's Ok
//     DIRECTLY (filter.go:748) and reads !Ok(nil) && !Ok(null) as "guarantees
//     presence" — so a fail-closed Ok yields TRUE, the AGGRESSIVE answer.
type Knn struct {
    Query []float32 // non-empty, finite; len checked against the index dim at detection
    K     int       // required, 1..KnnMaxK
    Ef    int       // 0 = auto; candidate/beam depth (numCandidates); >= K when set
    Index string    // optional: vector index name (disambiguates 2 indexes on one field)
}

const (
    KnnMaxK  = 10_000
    KnnMaxEf = 65_536
)

func (k Knn) Ok(*anyenc.Value, *syncpool.DocBuffer) bool { return false }
func (k Knn) IndexBounds(_ string, bs Bounds) Bounds     { return bs } // verbatim bs
func (k Knn) String() string                                            // LOSSLESS round-trip

// NewKnn is the programmatic constructor. REQUIRED, not optional: any/any-p2p
// build their ANN filter as a Go value and never touch JSON.
func NewKnn(vec []float32, k int, opts ...KnnOpt) Knn
type KnnOpt func(*Knn)
func KnnEf(ef int) KnnOpt
func KnnIndex(name string) KnnOpt

// ContainsKnn reports whether the tree contains a Knn ANYWHERE. Mirrors
// containsText (fulltext_search.go:1050) — it MUST walk And/*And/Or/Nor/Not/Key.
func ContainsKnn(f Filter) bool

// ContainsSourceFilter reports whether the tree contains a SOURCE filter — one
// matched by an index (Text, Knn) rather than by Ok. RECURSIVE, not a shallow
// type check: the only legal shapes are Key{path, Knn} and And{Text, …}, so a
// shallow test would return false for every real query and protect nobody.
// External consumers that post-filter by calling Filter.Ok directly (the
// subscription pattern at filter.go:650-655) MUST reject these: Text.Ok matches
// everything, Knn.Ok matches nothing.
func ContainsSourceFilter(f Filter) bool
```

Three hard constraints, each with a named prior bug:

1. **`Ok` = `false`, never `true`.** `Text.Ok`'s unconditional `true` (`filter.go:711-713`) *is* BUG-21.
2. **`IndexBounds` returns `bs` verbatim.** `Or.IndexBounds` detects "this branch contributed nothing" by **comparing lengths** (`filter.go:577`); a shorter return silently discards accumulated sibling bounds. That is BUG-30.
3. **Do not implement `RawFilter`.** Leaf filters don't (`query/filter_raw.go:27-122`). `Key.OkRaw` reaches `Knn.Ok` → `false`, matching the parsed path → `TestFilterOkRawParity` passes.

Verified safe by inspection: `isUnsatisfiable` (`query.go:845`), `isIDOnlyFilter` (`:916`), `countFilterFieldPreds` (`internal/qplanner/planner.go:1651`), `TightIndexBounds` (`query/tight_bounds.go:48-50`), `MayTighten` (`:79-98`) all default conservatively for an unknown node. **`GuaranteesPresence` does NOT** — it is the one exception, and it gets an explicit arm (see above).

No `$meta:"distance"` sort spelling: `_distance` is a **real injected field** (`internal/qplanner/vector_iter.go:102-105`), so `Sort("_distance")` / `Sort("-_distance")` already work.

### 6. Semantics — one definition, six verbs

For query `Q` on snapshot `T`, `$knn` clause `N = (field f, vector q, k, ef, index)`, residual `R`, sort `σ`, limit `L`, offset `O`:

```
ef' = knnEf(ef, indexDefault, k, R≠∅)                  -- verb-independent
C   = Search(T, X, q, ef')                             -- |C| ≤ ef'
C*  = sort C by (distance ASC, docId ASC)              -- TOTAL order, ENFORCED AT THE SOURCE
F   = [c ∈ C* : R(doc(c))]                             -- residual survivors, order preserved
S   = F[0:k]                                           -- ★ THE DENOTED SET, |S| ≤ k, always
Rows(Q) = page_{O,L}( sort_σ(S) )                      -- SORT, THEN PAGE
```

**`Rows(Q)` mentions no verb.** That is the whole design.

| verb | result |
|---|---|
| **`Iter`** | streams `Rows(Q)`; `Distance()` from the sidecar; `_distance` injected into the doc |
| **`Count`** | `len(Rows(Q))`, **≤ k always**. Documented verbatim: *"the size of the result page — **not** the number of documents matching, because `$knn` does not denote a match set."* |
| **`Delete`** | removes exactly `set(Rows(Q))`. **Blast radius is `k`, a number the caller typed.** `VectorIter` materializes all candidates on first `Next` (`vector_iter.go:57-72`) and `Delete` collects every docId before mutating (`query.go:497-529`); the search runs on `tx.btreeReadTx()` **inside** the write tx (`query.go:467-468`), so ranking and deletion share one snapshot. |
| **`Update`** | visits exactly `set(Rows(Q))`; same two-phase collect (`query.go:343-372`) |
| **`Explain`** | `Plan.Name = "KnnSearch"`, `Sql = KnnSearch(k=10,ef=100) -> Filter -> Limit(...)`, vector index in `Explain.Indexes` with `Used:true`. Today it prints `FullScan(filtered) -> Limit(10)` for a valid ANN query — **that is why this bug survived.** |
| **`Aggregate` `$match`** | **pushdown prefix only** → yields `Rows(Q)`. Anywhere else → `errAggVectorNotInPrefix`. With `k` in the clause, the "prefix `$match` silently truncates `$group` to `ef=64`" hazard **disappears by construction**. |

**No verb rejects `$knn`.** Rejecting it on the mutating verbs (Mongo's approach) would strand `aggQuery.Count`, forbid the legitimate bounded "delete the 5 nearest duplicates", and leave `Explain` unable to explain a vector query at all.

**Why "delete the k nearest" is a feature.** The footgun is *"delete everything near X"*, whose size would be decided by `efSearch` (a **tuning knob**) or by the index mode — 64 docs on HNSW, the whole collection on brute-force, for the same query. With mandatory `$k` that query is **unrepresentable**. Mongo Atlas forbids vector delete/count outright; Weaviate fails without `objectLimit`/`certainty`; Qdrant requires `limit` even with `score_threshold`; pgvector makes you write `LIMIT k`. `$knn`'s `k` is that `LIMIT`, hoisted into the operator so it cannot be forgotten on any verb.

**Honest caveats (into the docs verbatim):**
1. `$knn` is a **ranked source, not a predicate**. `S` is a *page*; `k` is the page size.
2. `S` is **approximate** (recall < 1). A selective residual with only ×10 over-fetch can yield fewer than `k`. Widen `$ef`.
3. `S` is **not stable across index churn** (compaction, tombstones, a different `ef`). For determinism: brute-force mode (exact), or `Iter` → collect ids → `DeleteId`.
4. **`k` selects. `Sort` orders. `Limit` paginates.**

### 7. `ef` is a pure function of the clause

Today `chooseEf(q.vectorEf, indexDefault, q.limit+q.offset, residual != nil)` (`vector_index.go:899-917`) makes the candidate set — and therefore the answer — **a function of the verb's builder state**. `Count()` (no limit) and `Iter().Limit(10)` compute different `ef`, walk different beams, and *provably* return different sets. This breaks Verb Coherence even if D2 were fixed.

```go
// knnEf resolves the ANN candidate depth. PURE function of the clause (+ whether a
// residual filter exists). q.limit / q.offset / q.sort are deliberately NOT inputs,
// so every verb computes the same ef and walks the same beam. That is what makes
// Count == len(Iter) a theorem instead of a hope.
func knnEf(explicit, indexDefault, k int, hasResidual bool) int {
    ef := explicit
    if ef == 0 {
        ef = indexDefault // HNSW: vi.ix.EfSearch(); IVF: vi.ivf.NProbe()*8
        want := k
        if hasResidual {
            want = k * vectorOverFetch // 10
        }
        if ef < want { ef = want }
        if c := max(vectorEfCap, k); ef > c { ef = c } // the 4096 cap may NEVER starve k
    }
    if ef < k { ef = k }
    return ef
}
```

**`hasResidual` must be `residual != nil && !isAllFilter(residual)`.** The FTS residual builder returns `query.All{}` for the empty case (`fulltext_search.go:1082-1109`) while the vector one returns `nil` (`vector_index.go:924`). Copying the FTS template naively makes `hasResidual` permanently **true** → ×10 over-fetch on every bare `$knn`, and — the real damage — brute-force `topK = 0` fires, turning every `Count`/`Delete`/`Explain` into a full `O(N·dim)` ranking of the whole collection, invisibly (the plan string is unchanged, because `buildVectorPlan` guards the `FilterIter` on `!isAllFilter`).

Brute-force (`vi.ix == nil && !vi.isIVF()`) ignores `ef` and takes `topK`: **`topK = k` when `R == ∅`, `topK = 0` when `R ≠ ∅`.** It is the only backend that can *guarantee* `k` post-residual survivors.

**`Query.VectorEf` (`query.go:36-39, 87-89, 117-120`) is DELETED.** It is builder state that changes *which* documents are returned, and it is the only way `AggQuery` could ever have set `ef` — which it cannot, today, at all.

### 8. Determinism — the fix goes in the source, not `VectorIter`

The first pass prescribed sorting the materialized candidate slice in `VectorIter.Next` and claimed it *"kills the IVF non-determinism for every consumer at once."* **It cannot.** Verified in the code:

`internal/vivf/store.go:726-729`:
```go
s.cands = s.dedup.collect(s.cands)   // walks an open-addressed table in SLOT order
if len(s.cands) > ef {
    selectSmallest(s.cands, ef)      // quickselect: "the k smallest by dist occupy
    s.cands = s.cands[:ef]           //  cs[:k] (in ARBITRARY order)" — store.go:806-807
}
```

`s.dedup` is a `u32fmap` on a **pooled** searcher (`store.go:686`). `reset()` bumps a generation and **never shrinks** (`labelmap.go:32-50`); `grow()` doubles (`:92`). `collect()` walks `for i := range m.key` — **slot order** (`labelmap.go:84-88`). So the permutation handed to the quickselect is a function of *that pooled searcher's history*, not of the query. **Among equal distances straddling the cut, membership of `cs[:ef]` differs per pooled searcher.** Candidates dropped there are gone before `VectorIter` sees them; sorting the output restores *order*, never *membership*. The final `slices.SortFunc(out, cmpF32(a.Distance, b.Distance))` (`store.go:803`) is also non-stable and has no docId tiebreak.

**Failing scenario:** IVF-PQ, near-duplicate embeddings (identical PQ codes ⇒ *exact* float32 ties — the design's own "delete the 5 nearest duplicates" use case). `Count(Q)` on searcher A → 7; `Iter(Q)` on searcher B (table grown by an earlier label-heavy query) → 9. `Rows(Q)` is not even **well-defined**.

**The fix (Increment 1c), in two layers:**
1. **Cut** by `(dist, label)`: tie-break `partitionDist`/`selectSmallest` (`internal/vivf/store.go:806-823`) on `label`, and HNSW's beam `ef`-boundary admission likewise. `cs[:ef]` becomes the unique `(dist,label)`-smallest `ef`. This is the *membership* fix, and only this closes it.
2. **Output** in `(dist, docId)` order at the source, for all three backends. Brute-force already does (`vector_index.go:838-846`). Set `TotallyOrdered: true` on the spec.

`VectorIter` then **does not sort in the hot path** — it asserts the order in a test. This also resolves the perf trap: the first pass costed the sort as *"≤ ef ≤ 65536 already-materialized elements… free"*, but on the brute-force + residual path `topK = 0` returns **every document**, already `(d, docId)`-sorted — a redundant re-sort of all N on every verb.

`VectorQuerySpec.Ordered` is therefore **kept, renamed `TotallyOrdered`** (meaning `(distance, docId)`, not merely "closest-first"). Its doc comment at `vector_iter.go:34-38` is **stale and wrong today** — it says brute-force leaves it false; `vector_index.go:648` sets it `true`. All three backends set `true` (`:621`, `:648`, `:663`), so the `!vspec.Ordered` branch at `query.go:206-212` is genuinely dead and is deleted.

### 9. No threshold in v1

`$maxDistance` / `$minScore` are **rejected with a named error**. There is no radius search at any layer; a threshold can only be a post-filter over the ≤`ef` candidates — which is *exactly what `_distance:{"$lt":x}` already is*, and it silently under-returns (on HNSW at default `ef=64`: 64 rows of a true 202). IVF cannot do a radius search at **any** `ef`, because `nprobe` is fixed inside the index and is not a parameter of `SearchCandidates(rtx, q, ef)` — points in unprobed cells are never visited. A threshold that means three different things per index mode, and that authorizes a `Delete`, is BUG-32 with better branding.

`_distance:{"$lt":x}` stays fully supported as a residual, and **`docs/vector-search.md:130` must be corrected**: it is *"a threshold **within** the k nearest"*, not *"all documents within d"*.

### 10. Interaction rules

| interaction | rule |
|---|---|
| **Sort** | Sorts `S`, **after** the k-cut. No sort → the source's intrinsic `(distance, docId)` order. **The sorter is passed on the source path for EVERY verb**, so `Delete` cuts the same page `Iter` shows. |
| **Limit / Offset** | Pagination over `sort_σ(S)`. They do **not** feed `k` or `ef`. `Offset ≥ k` → empty. `Limit > k` → ≤ k rows. |
| **`$and`** | Legal at any depth. The walker handles `query.And` **and `*query.And`** — closing the live wrong-answer bug of D3. |
| **`$or` / `$nor`** | **`ErrKnnBadPlacement`.** A ranked source cannot be a disjunct. Mirrors `$text`'s `errFtsBadPlacement` (`fulltext_search.go:1045`). |
| **`$not`** | Parse error, and `ErrKnnBadPlacement` at detection. `Not{Knn}.Ok == !false == match-all` — this is load-bearing. |
| **multiple `$knn`** | `ErrMultipleVectorClauses`. |
| **`$knn` + `$text`** | **`ErrKnnWithText`.** Today this silently degrades: `detectFtsQuery` runs first (`query.go:167` before `:197`) *while* `BuildPlan` checks `Vector` **before** `Fts` (`planner.go:285-294`). The two layers disagree about who wins. |
| **no vector index on the field** | **`ErrNoVectorIndex`.** Exact scan is an index **mode** (`VectorModeBruteForce`), not a fallback. |
| **>1 vector index on the field** | **`ErrAmbiguousVectorIndex`** unless `$index` names one. Probe-confirmed: two `EnsureIndex` calls with different names, same field, different metrics **both succeed**, and `findVectorIndexByField` (`vector_index.go:681-688`) returns the **first** — so the deleted set would depend on index load order. |
| **residual filters (hybrid)** | Post-filter with ×10 over-fetch, then the k-cut. |
| **primary-key constraints** | `buildVectorPlan` **ignores** `IDBounds`/`PrimaryKey`/`Indexes` (`planner.go:704-732` reads only `Tx, DataNs, Filter, Sorter, Limit, Offset, Buf, Vector`). So `{"id":X, "v":{"$knn":…}}` means *"X, if it is among the k nearest"* — a residual, never a seek. **Document it.** |
| **`_distance`** | Legal iff a `$knn` is present; else `ErrDistanceWithoutVector`. Guard becomes a **full** tree walk (`filterRefsField`, `vector_index.go:692-709`, today misses `*And`/`Nor`/`Not`). |
| **`$exists`/`$type`/`$ne`/`$in`/`$size`/`$regex` on a vector-indexed field** | **Ordinary filters, on every verb.** Non-negotiable — `any`'s `EnsureVectorIndex` depends on it. |
| **bare array on a vector-indexed field** | **`ErrLegacyVectorClause`**. |
| **ordering ops against a vector value, anywhere** | **Always false** (Rule V). Parse-rejected when the *operand* is a `$vector` literal. |

### 11. The old bare-array syntax: hard error — **scoped to `TypeArray`**

`ErrLegacyVectorClause`, raised by the seam on **all six verbs**, whenever a `query.Key` whose field has a vector index carries a `*query.Comp{CompOp: CompOpEq}` whose `EqValue` is a **`TypeArray` of exactly `index.dim` numbers**.

```
any-store: field "v" has a vector index; a bare-array/equality clause is no longer an
ANN query. Use {"v":{"$knn":{"$query":[…],"$k":N}}} — or query.NewKnn(vec, k) — to
search it. Rejected on every verb (BUG-32).
```

**NOT `TypeVectorF32`.** The first pass also caught the packed form, which would have made **exact-vector equality unexpressible on a vector-indexed field** — contradicting Rule V, which explicitly *defines* `$eq` as byte equality and whose own test table asserts `{"v":{"$eq":{"$vector":[1,2,3]}}} → true`. And it is unnecessary: the legacy ANN trigger was *only ever* a `TypeArray` — `decodeVectorValue` (`vector_index.go:726-747`) **rejects** anything else, and `any`'s programmatic site builds `arena.NewArray()`. Scoping to `TypeArray`-of-dim-numbers catches every real legacy call site and leaves `$eq` against a `{"$vector":[…]}` literal an ordinary byte-equality filter. (The `== dim` requirement also means `{"v":{"$eq":[]}}` is never caught.)

**Why not "it silently becomes an ordinary filter"** (the direction the first pass proposed):

| storage | `{"v":[768 floats]}` today | as an "ordinary filter" |
|---|---|---|
| packed `{"$vector":[…]}` — the **recommended** encoding **and the one the live downstream consumer uses** (`any/internal/indexer/store.go:482`) | 10 real ANN hits | **0 rows, `err == nil`** (tag 6 vs tag 10) |
| plain JSON array | 10 real ANN hits | **1 row** (the byte-identical self-doc) |

Silent demotion converts a *loud* bug into a *silent* one, on the one code path that currently works, and it means two different wrong things depending on a storage decision the query author may not know about. A hard error turns ~30 migration points into a single `go test ./...` run.

Additivity is guaranteed: `ParseCondition('{"v":{"$knn":…}}')` today returns `unknow operator: $knn` (probe-confirmed), so no existing query can contain the token.

---

## Rejected alternatives

| alternative | why rejected |
|---|---|
| **`$near` / `$nearest`** | Mongo's `$near` is a geo-**radius** operator denoting an unbounded, data-defined set. That is the exact mental model that produces the mass delete, and the engine has no radius search at any layer. Borrowing the token imports the wrong intuition and burns it for a future geo index. |
| **Bare-array shorthand `{"$knn":[floats]}`** | `k` would come from `.Limit()` — reinstating builder-owned `k`. `AggQuery` has no `.Limit`, so it fails on the sixth verb. Two spellings of one operator is how you get two behaviours. Also: `makeArrComp`'s `default:` arm **panics** on an unrecognized op (`cond_parse.go:606`), so a bare-array `$knn` is one missing `case` away from a panic rather than an error. |
| **Top-level `$knn`** (P3) | Every field-scoped operator in this language spells the field as the object key. Top-level would need its own field-extraction machinery and would copy `$text`'s blind `loadFtsIndexes()[0]` wart. |
| **Cut-then-filter denotation** (P3) | `{"v":{"$knn":{…,"$k":10}}, "lang":"en"}` would return ~1 row instead of 10, destroying hybrid search and making `vectorOverFetch` dead code. No comparable engine does this. |
| **Silent demotion of the bare-array clause to an ordinary filter** | Turns a loud bug into a silent one (0 rows on packed storage, 1 row on plain), on the one verb that works today, and the two downstream repos build the clause **programmatically** — no compiler and no grep would catch them. |
| **`$maxDistance` / threshold in v1** | No radius search exists. It would mean three different things per index mode, silently under-return, and authorize an unbounded `Delete`. |
| **Rejecting `$knn` on the mutating verbs** (Mongo's approach) | Strands `aggQuery.Count`, forbids the legitimate bounded "delete the 5 nearest duplicates", and leaves `Explain` unable to explain a vector query. |
| **Erroring on `offset+limit > k`** | Makes paging fragile and blocks the legitimate "give me the k nearest, page 2". |
| **Fixing determinism by sorting in `VectorIter`** | Sorts a set whose *membership* is already non-deterministic. Fixes order, not membership. The cut must be tie-broken at the source. |
| **Deleting `VectorQuerySpec.Ordered`** | Its consumer at `query.go:206` is dead, but the flag itself is not: without it, brute-force + residual (`topK = 0` → every document, already sorted) gets re-sorted on every verb. Kept, renamed `TotallyOrdered`. |
| **A `cboSorter` planOpt that preserves today's sorterless Update/Delete** | It preserves a live data-loss bug. Fixed properly in Increment 1b. |

---

## Implementation plan

`WT` = `/home/che/projects/any-store/.claude/worktrees/bug-32-vector-clause`. **S** ≈ under an hour, **M** ≈ a few hours, **L** ≈ a day.

### INCREMENT 1 — Rule V (non-breaking, ships alone, kills the mass delete)

> **Rule V.** In `query.Comp`, if **either** side is a `TypeVectorF32` value:
> - `$gt`/`$gte`/`$lt`/`$lte` → **`false`**, always (including vector-vs-vector).
> - `$eq` → byte equality. `$ne` → its negation.
>
> Plus: an ordering op whose **operand** is a `$vector` literal is a **parse error** (decidable syntactically; kills the `{"anyfield":{"$lt":{"$vector":[0]}}}` family at the door, and prevents its reflection through `$not`).

#### T1.1 — TDD: the failing unit test · **S**
**New file** `WT/query/filter_vector_test.go`, `TestComp_VectorIsNotOrdered`, over doc `{"n":5,"s":"abc","tags":[1,2],"v":{"$vector":[1,2,3]}}`:

| clause | want `Ok` |
|---|---|
| `{"v":{"$gt":1}}`, `{"$gte":1}`, `{"$lt":1}`, `{"$lte":1}`, `{"$gt":"zzz"}`, `{"$lt":true}` | `false` |
| `{"n":{"$lt":{"$vector":[…]}}}`, `{"$lte":…}`, `{"$gt":…}` | `false` |
| `{"s":{"$lt":{"$vector":[…]}}}`, `{"tags":{"$lt":…}}`, `{"nosuchfield":{"$lt":…}}` | `false` |
| `{"v":{"$eq":{"$vector":[1,2,3]}}}` | **`true`** |
| `{"v":{"$eq":{"$vector":[9,9,9]}}}`, `{"$eq":[1,2,3]}`, `{"$eq":1}` | `false` |
| `{"v":{"$ne":1}}`, `{"$ne":{"$vector":[9,9,9]}}`, `{"nosuchfield":{"$ne":{"$vector":[…]}}}` | `true` |
| `{"v":{"$ne":{"$vector":[1,2,3]}}}`, `{"v":{"$exists":true}}` | `false`, `true` |
| **hand-built** `NewCompValue(CompOpLt, vec)` vs {array, vector, number} probes (×4 ordering ops) — `NewCompValue` leaves `notArray=false`, so this exercises the whole-array shortcut | `false` |
| **`$not`/`$nor` reflection rows** (below) | see note |

**All 33 rows verified passing against the patch in T1.2.**

#### T1.2 — `query/filter.go`: the guard · **S** (~15 LOC)

> ⚠️ **The first pass's patch did not implement Rule V.** It guarded only `okScalar`'s **probe** side (`v.Type() == TypeVectorF32`). When the *operand* is the vector and the probe is a scalar, `okScalar` falls into its per-type tag-compare arms (`:135`, `:146`, `:155`) and **still returns `true`** — measured: 5 of the design's own T1.1 rows still failed, and `Find({"n":{"$lt":{"$vector":[0]}}}).Delete(ctx)` **still emptied any collection**. Three of four reviewers found this independently.

The correct patch is **two guards**, and needs neither an `okVector` helper nor the array-shortcut change:

```go
// isOrderingOp reports whether the op places its operands on the scalar order.
func (e *Comp) isOrderingOp() bool {
	switch e.CompOp {
	case CompOpGt, CompOpGte, CompOpLt, CompOpLte:
		return true
	}
	return false
}

// eqIsVector reports whether the filter OPERAND is a packed TypeVectorF32.
func (e *Comp) eqIsVector() bool {
	return len(e.EqValue) > 0 && e.EqValue[0] == byte(anyenc.TypeVectorF32)
}

func (e *Comp) Ok(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	// Rule V (BUG-32), OPERAND side. Must precede the nil check and the array
	// branch: e.comp(encodedNull) would make $lt true for an ABSENT field, and
	// the whole-array shortcut (notArray==false on a hand-built Comp) would make
	// it true for an array-valued one.
	if e.eqIsVector() && e.isOrderingOp() {
		return false
	}
	if v == nil {
		return e.comp(encodedNull)
	}
	... // unchanged
}

func (e *Comp) okScalar(v *anyenc.Value, docBuf *syncpool.DocBuffer) bool {
	// Rule V (BUG-32), PROBE side — top-level value and array element alike.
	// The anyenc tag order (vectorF32 = 10, above every scalar tag) made
	// {"v":{"$gt":1}} — and, symmetrically, {"anyField":{"$lt":{"$vector":[…]}}} —
	// TRUE for every document, which on Query.Delete emptied the collection with
	// err=nil. Vectors are not points on the scalar order.
	if e.isOrderingOp() && (v.Type() == anyenc.TypeVectorF32 || e.eqIsVector()) {
		return false
	}
	if len(e.EqValue) > 0 {
		... // unchanged
	}
}
```

`Comp.IndexBounds` (`:229`): **unchanged**; add the soundness comment below.

**Soundness vs the index (verified two ways).** Making `Ok` *stricter* while the bounds stay a sound over-approximation is safe iff no plan can skip the residual `FilterIter`. The first pass argued from `idx.PointLookup` — that is *not* the whole story. The two `CountOnly` covering shortcuts (`planner.go:1344`, `:1359`) are gated on `PointLookup`, and an ordering op never produces one; but `buildVerifyChain` (`:1359` → `:1784`) additionally bails unless the *uncovered* field's bounds are **`fixed`** (`:1793`) — and an ordering op is never `fixed`. **That `Fixed` gate is what actually saves it.** `CoverIter` (`:1271`) and `IndexFilterIter` (`:1455`) both still wrap a `FilterIter`. `$eq` against a vector *can* be a point lookup, and there `Ok` still agrees with the bounds (byte equality). `Key.OkRaw` (`filter_raw.go:46`) calls this same `Ok`, so the raw fast path stays in parity. **No plan can diverge.**

#### T1.3 — parse-time rejection of a vector operand on an ordering op · **S** (~5 LOC)
`WT/query/cond_parse.go`, in `makeCompFilter`'s `opGt`/`opGte`/`opLt`/`opLte` arms: `if v.Type() == anyenc.TypeVectorF32 { return nil, errVectorNotOrderable }`. This makes the widest form of the data-loss hole — `{"anyfield":{"$lt":{"$vector":[0]}}}` — **unrepresentable in JSON**, and prevents its reflection through `$not`. The eval-time guard (T1.2) remains the backstop for hand-built filters, which have no parser.

#### T1.4 — **update the parity reference** · **S** ← *missing from the first pass; without it the build is red*
`WT/query/comp_fastpath_test.go:20` `TestCompOkScalar_MatchesMarshalReference` includes `a.NewVectorF32([]float32{1,2})` in `values` — on **both** sides — and asserts `okScalar` ≡ marshal + `bytes.Compare`. Rule V deliberately diverges. **Measured: this is the ONLY failing test in the entire repo under Rule V** (`./query` fails; `.` — 100 s of tests incl. every vector suite — and `./internal/qplanner` both pass). Teach `compReference` Rule V (same two guards), keeping the vector values in the matrix so the divergence stays pinned.

#### T1.5 — `$type:"vectorF32"` · **S**
Rule V removes the (accidental) way to select vector-valued documents; give the correct one.
- `WT/query/type.go:34-37`: `typeString[10] = "vectorF32"`; add `TypeVectorF32` to the const block (`:9-18`).
- `WT/query/cond_parse.go:647`: the numeric form's rejection `(tv > TypeObject && tv != TypeObjectID)` must also admit `TypeVectorF32`.
- ⚠️ **`TypeFilter.IndexBounds` (`query/filter.go:791-802`) MUST return `bs` verbatim for `TypeVectorF32`.** The first pass claimed it was *"index-sound for free"* via the BUG-29 exclusive-next-tag bound. **It is not.** Measured on a real DB (3 vector docs + 3 scalar docs):

  | index on `v` | Count | Iter | want | plan |
  |---|---|---|---|---|
  | none | 3 | 3 | 3 | `FullScan(filtered)` |
  | `"v"` (fwd) | 3 | 3 | 3 | `IndexScan` |
  | **`"-v"` (rev)** | **0** | **0** | **3** | `IndexScan(ix)[bounds=…'err:unknown type 245']` |

  Reverse keys are bitwise-inverted, so a vector key becomes `0xF5…` — and `anyenc/type.go:40-44` states `^Type(10) = 0xF5` *"must stay an unknown type"* (an invariant `index.go:661-667` already violates by marshaling vectors into keys). `transformReverseBounds` + `AdjustBoundsForNonUnique` produce a bound strictly below every real entry. **A descending index silently drops every row, `err == nil`** — a textbook BUG-29 under-approximation, newly *reachable* because `$type` opens tag 10.

  Returning `bs` (no bounds → always a filter) fixes it: **verified, all three configs → 3 rows.** A vector is not a range; contributing no bounds is exactly right and costs only a scan.
- Test **must include a `-v` index** — a forward-only test passes and hides this.

#### T1.6 — regression tests · **S**
- `WT/query/filter_contract_test.go:22-36`: add `"vf": {"$eq": {"$vector":[1,2]}}` to `filterZoo`.
- `/home/che/projects/any-store-tests/storetest/query_correctness_test.go`: **new** `TestVectorOrderingNeverMatchesEverything` — syntax-independent, permanent:
  1. Collection **with** a vector index on `v`, 20 packed-vector docs: `Find({"v":{"$gt":1}}).Delete(ctx)` → `Matched==0`, `Count==20`. Same for `$gte`/`$lt`/`$lte`.
  2. Collection with **no vector index at all**, 20 plain docs: `{"n":{"$lt":{"$vector":[0]}}}` and a **missing** field → parse error (T1.3); hand-built equivalents → `Matched==0`, `Count==20`.
  3. **`$not`/`$nor` rows** (below).

**`$not` note — a decided, documented consequence, not a defect.** After Rule V, `{"v":{"$not":{"$gt":1}}}` becomes match-all (measured: `false` today → `true` after). This is exactly MongoDB's type-bracketing semantics — `$not` of a comparison the value cannot satisfy is true, and `{"x":{"$not":{"$eq":"…"}}}` already behaves this way today. It requires the user to *deliberately type `$not`*, unlike the bug, which fires on a plain `$gt`. The operand-side family (`{"n":{"$not":{"$lt":{"$vector":[…]}}}}`) is killed outright by T1.3's parse rejection. Both get explicit test rows and a line in `docs/query-filter-contract.md`.

> **Increment 1 complete: 4 source files + 3 test files, ~110 LOC. Non-breaking.**

### INCREMENT 1b — sorted+limited `Update`/`Delete` → **NOT OURS: this is BUG-47**

This grooming pass independently rediscovered a second data-loss bug and proposed fixing it here. **It has since been filed and fixed elsewhere — see `/home/che/projects/any-store-tests/BUG-47-update-delete-drop-sort-keep-limit.md`** (filed 2026-07-13, reproduced on HEAD `5d6ad58`, fix written and A/B-verified). Do **not** re-fix it in this plan.

Reproduced here anyway, because the `$knn` design depends on it (10 docs, `p` uncorrelated with `id`):

```
Iter  (Sort("-p").Limit(3))  => id 7 (p=100), id 3 (p=99), id 1 (p=90)
Delete(Sort("-p").Limit(3))  => Matched=3, Modified=3, err=nil
remaining                    => id 3,4,5,6,7,8,9      ← it deleted ids 0, 1, 2
```

`Update` (`query.go:322-336`) and `Delete` (`:480-494`) build `PlanParams` with **no `Sorter` at all** while still forwarding `Limit`/`Offset`; `Iter` passes `q.sort` (`:238`). `Delete` kept the two highest-priority docs it had just shown and removed two of the lowest. No vector index anywhere.

**Dependency, not scope.** BUG-47 must land before Increment 2: the `$knn` seam (T2.8) passes `q.sort` on the source path for *every* verb, so `Delete` cuts the same page `Iter` shows. If BUG-47's fix is still pending when Increment 2 starts, the CBO branch of `accessPlan` will re-introduce it on the non-vector path. The design's `planOpts.mutation` flag exists **only** to carry BUG-47's fix; if BUG-47 lands first with a different mechanism, **delete the flag and adopt theirs** — do not ship two fixes for one bug.

Retained from this pass: the test `TestVerbCoherence_CBO` — `ids(Iter(Q))` == the ids `Delete(Q)` removes, over `{no sort, Sort("-p"), Sort("p","-id")} × {no limit, Limit(3), Offset(2).Limit(3)}` — which is a stronger property assertion than a single-shape regression test. Contribute it to BUG-47 if not already covered.

### INCREMENT 1 — AS SHIPPED (2026-07-13), with corrections to the plan above

Increment 1 is **implemented and green** (`go test ./...` + `-race`). Four adversarial reviewers attacked the diff; three corrections to the plan came out of it, recorded here because the plan text above is now wrong in two places.

**1. A second, independent bug was found and fixed: BUG-48 (`anyenc` cannot read an inverted vector tag).**
Not in the plan at all. `index.go` `AppendInverted`s a vector into a **descending** index key, producing the tag `^Type(10) = 0xF5` — which `anyenc/parser.go` explicitly reserved as an *unknown type*, on the stated grounds that "vectors are never index-keyed." That invariant is false. The parse failure made `extractDocId` return the whole key as the docId, the `Fetch` missed, and the row **vanished with err=nil**. Reachable with **no filter at all**: `Find(nil).Sort("-v")` silently dropped every vector-valued document while `Count` returned the right number. Fixed reader-side (`anyenc/type.go` + `parser.go`, mirroring `parseBinary`'s inverted length header) — no on-disk format change, so existing DBs are repaired, not migrated. See `any-store-tests/BUG-48-*.md`.

**2. The plan's stated root cause for the `TypeFilter.IndexBounds` carve-out (T1.5) was wrong — but the carve-out is still required.**
The plan (and a reviewer) each had half of it. Measured both ways: with BUG-48 fixed and the carve-out removed, `$type:"vectorF32"` still returns `Count=0, Iter=0` on **every** `-v` config — the `[tag, tag+1)` bound genuinely does not survive inversion. And with the carve-out but *without* the BUG-48 fix, rows are still dropped by any reverse-index scan. **Two independent defects, one masking the other.** Both are fixed; the comment in `filter.go` now says so.

**3. `Count` swallowed every parse error** (the plan listed this as a T2.8 prerequisite; it had to land now, because Rule V's parse rejection is useless if `Count` discards it). `Count` is the only verb that bypasses `makeQuery`, so its `q.err` check sat *below* the no-filter fast path: a query that failed to parse left `q.cond == nil`, which the fast path read as "count everything." On the old code `Find('{"$badop":1}').Count(ctx)` returned `5, err=nil` on a 5-doc collection. Fixed by hoisting the check.

**Scope note (the plan overclaims):** Increment 1 kills the **vector** mass delete, not *the* mass delete. `Find({"anyfield":{"$lt":1}}).Delete(ctx)` still empties any collection — missing-field-is-null under a total cross-type order is the general disease, and fixing it needs Mongo-style type bracketing (breaking, separate bug). Rule V closes the vector spelling, which is the one that fires on a *plain* `$gt` against a legitimately-typed field.

**Accepted, deliberate widening:** `{"v":{"$not":{"$gt":1}}}` on a vector field now matches (it did not before, when `$gt` itself matched everything). This is MongoDB type-bracketing semantics and it is correct — a vector is not greater than 1. It is pinned by a test so nobody "fixes" it back.

**Follow-up (not a blocker, lands with Increment 2):** a hand-built `Not{Comp{$lt, vector}}` is still match-all, because a fail-closed `Ok` is never safe under `Not`. Every JSON spelling is closed at the parser (`$not`/`$nor`/`$or`/`$and` all reject), and net reachability strictly *decreases* versus the old code, where the bare form was already a mass delete. The durable fix is a filter-tree walk at query-build time — the same walk Increment 2 needs for `ContainsKnn`.

### INCREMENT 1c — source-level determinism (non-breaking) · **M**

1. `internal/vivf/store.go:806-823` — tie-break `partitionDist`/`selectSmallest` by `label`, so the ef-cut is the unique `(dist,label)`-smallest `ef`. **This is the membership fix.** Replace the final distance-only `slices.SortFunc` (`:803`) with `(dist, docId)`.
2. `internal/vindex/` — same tie-break on the HNSW beam's `ef`-boundary admission; sort the ≤`ef` output by `(dist, docId)`.
3. Set `TotallyOrdered: true` on all three specs; `VectorIter` skips any sort.
4. Test **at the `vivf.SearchCandidates` level with a deliberately pre-grown pooled searcher** — a qplanner-level "run it twice" test passes while the bug is live. Reproducer: 300 labels, 10 unique distances + a 290-member exact-tie group straddling the cut, `ef=20`; a cold (1024-slot) and a warm (8192-slot) searcher select **10 of 20 different candidates**.

### INCREMENT 2 — `$knn` + the seam + the legacy error (breaking, one release)

Shipped as **one release**. An intermediate "seam-only, old trigger" release would turn `Delete({"v":[…]})` from a silent no-op into a real ANN delete of `ef` documents **on inferred intent** — a mass delete of a different flavour, on the way to abolishing inference.

#### T2.1 — TDD: the grammar test · **S**
**New file** `WT/query/knn_parse_test.go`. Every accept/reject row of §4, asserting exact error strings; both `$query` encodings; `String()` round-trip (`MustParseCondition(f.String())` ≡ `f`) — do not repeat `Text.String()`'s lossiness (`filter.go:719-721`).

#### T2.2 — `query` package: the `Knn` type + parser · **M** (~200 LOC)
- `WT/query/filter.go`: `Knn`, `Ok`/`IndexBounds`/`String`, `NewKnn`, `KnnOpt`/`KnnEf`/`KnnIndex`, `ContainsKnn`, `ContainsSourceFilter`, `KnnMaxK`/`KnnMaxEf` — verbatim including the doc comments in §5.
- **`GuaranteesPresence` (`filter.go:744`): add `case Knn: return false`.** It has **no default arm for a `Key`'s inner filter** — it calls `ft.Filter.Ok(nil, buf)` and `Ok(presenceNullProbe, buf)` **directly** (`:748`), so a fail-closed `Ok` yields `!false && !false == true`, the **aggressive** answer. Probe-confirmed. Its only consumer is `sparseIndexComplete` (`planner.go:1041`), on the CBO path that `$knn` bypasses — so it is not exploitable today, but it is exactly the trap that fires the moment a `Knn` leaks, converting "silent zero rows" into "silently missing rows from a sparse index" on `Iter` **and** `Delete`. (Ironically `Text` is safe here only because its `Ok` fails *open*.) Also add `case Text: return false` for the same reason.
- `WT/query/cond_parse.go`: **append `opKnn` at the END of the `Operator` const block** (`:16-38`) — it is still `> _opVal`, so it is field-level for free (`isTopLevel`, `:709-711`), and it shifts no existing iota value. Add `opBytesKnn`, the `isOperator` arm (`:663-707`), and — **critically** — the `case opKnn: return parseKnn(v)` arm in `makeCompFilter` (`:328`). Without that arm, `makeCompFilter`'s `default:` falls through to `makeArrComp`, whose own `default:` **`panic`s** (`:606`) — so a bare-array `{"v":{"$knn":[1,2]}}` would be a **panic**, not an error.
- `opNot` arm (`:366-375`): after `parseCompObjOp`, `if ContainsKnn(not.Filter) { return nil, errKnnUnderNot }`.
- `parseCompObjOp` (`:263-326`): track `hasKnn`; after the Visit, `if hasKnn && obj.Len() > 1 → "$knn must be the only operator on field"`.
- New `parseKnn(v *anyenc.Value) (Filter, error)` — model `parseText` (`:404-490`). Decode `$query` with `anyenc.AppendFloat32s` (accepts both encodings). Reject NaN/±Inf.

#### T2.3 — contract tests for `Knn` · **S**
`filterZoo` gains `"vk": {"$knn": {"$query":[1,2],"$k":3}}`. New `TestKnn_OkIsFalse` (nil/scalar/array/vector probes), with a comment naming BUG-21. `TestFilterOkRawParity` picks the zoo entry up automatically.

#### T2.4 — TDD: detection tests · **S**
**New file** `WT/vector_knn_detect_test.go`: valid clause; `{"$and":[{"v":{"$knn":…}},{"x":1}]}` (**`*query.And`**); **`{"$and":[{"$and":[{"v":{"$knn":…}},{"a":1}]},{"b":2}]}` (nested `*And` — the residual-leak shape)**; `$or` → `ErrKnnBadPlacement`; two `$knn` → `ErrMultipleVectorClauses`; unindexed field → `ErrNoVectorIndex`; **two indexes on one field, no `$index` → `ErrAmbiguousVectorIndex`**; wrong dim → `ErrInvalidVectorQuery`; `$knn`+`$text` → `ErrKnnWithText`; `_distance` without `$knn` (incl. inside `*And`/`Or`/`Not`) → `ErrDistanceWithoutVector`; legacy `{"v":[…]}` → `ErrLegacyVectorClause`; **hand-built** `query.Not{query.Knn{}}` and a bare top-level `query.Knn{}` → `ErrKnnBadPlacement`.

#### T2.5 — `vector_index.go`: rewrite detection · **M** (~250 LOC)

**Write ONE tree walker and reuse it.** The first pass specified five separate walkers with three different bugs between them.

```go
// detectKnnQuery is a SYNTACTIC operator check (template: detectFtsQuery,
// fulltext_search.go:942). Runs on every verb, via accessPlan.
func (q *collQuery) detectKnnQuery() (*qplanner.VectorQuerySpec, query.Filter, error)

// findKnnClause walks the WHOLE tree: Key{…, Knn} → found; And/*And → recurse
// (a 2nd hit ⇒ ErrMultipleVectorClauses); Or/Nor/Not, a Knn nested inside a
// Key's inner filter, or a bare unwrapped Knn ⇒ ErrKnnBadPlacement.
func findKnnClause(f query.Filter) (node query.Key, found bool, err error)

// knnResidualFilter removes the Knn NODE (by identity, never by path) and
// RECURSIVELY rebuilds And/*And. Collapses an empty result to nil (NOT All{}).
func knnResidualFilter(f query.Filter, node query.Key) query.Filter

// rejectLegacyVectorClause errors on the OLD syntax. FULL tree walk. TypeArray
// of exactly index.dim numbers only — never TypeVectorF32.
func rejectLegacyVectorClause(f query.Filter, vidxs []*vectorIndex) error

// knnEf — pure function of the clause. Replaces chooseEf (:899-917).
func knnEf(explicit, indexDefault, k int, hasResidual bool) int
```

Three traps the first pass fell into, all now fixed:

1. **`knnResidualFilter` must RECURSE.** Its stated template `ftsResidualFilter` (`fulltext_search.go:1080-1109`) drops only *direct* `query.Text` children of the top `And`/`*And`. Measured: for `{"$and":[{SRC},{"a":1}],"b":2}` the residual **still contains the source**. For `$text` that is benign (`Text.Ok → true`); for `Knn` (`Ok → false`, fail-closed) the residual `FilterIter` rejects **every** ANN candidate → **`Iter` = 0 rows, `Count` = 0, `Delete` = no-op, `err == nil`** — on a shape this design explicitly legalizes. `detectKnnQuery` must **assert `!query.ContainsKnn(residual)`** before returning. (This is the first pass's own Risk 3, live on day one; the assertion is promoted from "nice-to-have" to a hard post-condition. *`ftsResidualFilter` has the same latent leak — file separately.*)
2. **Strip by node identity, not by path.** Two `Key`s can legitimately share a path — the "`$knn` must be the only operator on field" rule is a *parse-time* rule about one JSON object, and the only two production consumers build filters **programmatically**: `And{Key{["v"], Knn{…}}, Key{["v"], Comp{Ne, x}}}` is legal. A path-keyed strip drops the `$ne` too → the residual **widens** → `Delete` removes documents the filter excluded. (The cited template `ftsResidualFromAnd` drops by **type**, not path — the first pass's signature diverged from its own model.)
3. **Empty residual → `nil`, not `All{}`** (see §7).

Also in this file:
- New errors: `ErrNoVectorIndex`, `ErrAmbiguousVectorIndex`, `ErrKnnBadPlacement`, `ErrKnnWithText`, `ErrLegacyVectorClause`. Reuse `ErrMultipleVectorClauses`, `ErrDistanceWithoutVector`. Narrow `ErrInvalidVectorQuery`'s doc comment.
- `filterRefsField` (`:692-709`): add `*query.And`, `query.Nor`, `query.Not` — fold into the one walker.
- Spec construction (`:610-676`): all three backends set `K: knn.K`, `Ef: knnEf(...)`, `IndexName: vi.info.Name`, `TotallyOrdered: true`. Brute-force `topK` = `k` when residual is empty, else `0`.
- **Delete `decodeVectorValue`** (`:726-747`).
- `$index` disambiguation: when `knn.Index != ""` match on `vi.info.Name`; else match on `vi.info.Vector.Field` and **error if >1 matches**.

#### T2.6 — `internal/qplanner`: spec, k-cut · **M** (~80 LOC)
- `vector_iter.go:30-39`: `VectorQuerySpec{Query []float32; K int; Ef int; Search VectorSearchFunc; IndexName string; NeedDistances bool; TotallyOrdered bool}`. **Fix the stale `Ordered` doc comment** (`:34-38`) — it claims brute-force leaves it false; `vector_index.go:648` sets it true.
- `VectorIter.Next` (`:57-72`): **gate ONLY `Plan.Distances.Set` on `Spec.NeedDistances`. `injectDistance` stays UNCONDITIONAL.**

  ⚠️ Today `injectDistance(...)` and `Plan.Distances.Set(...)` sit in the same `if it.Plan != nil` block. Gating the block — which the first pass's wording invites (*"saves the sidecar **and** the per-row docId copy"*) — stops injecting `_distance` on Count/Delete/Update. A residual `{"_distance":{"$lt":0.35}}` then evaluates `Comp.Ok(nil)` → **probe-confirmed `true`** (number tag 2 > null tag 1) → **match-all** → `Delete` removes all `k` instead of only those inside the threshold, silently. That is BUG-32 reinstated *through the fix*. `FtsIter` gets this right (`fts_iter.go:107-113`: gates only `Plan.Scores.Set`, always calls `injectScore`). **Mirror it exactly.** Doc comment: *"`injectDistance` is unconditional; `NeedDistances` gates the sidecar only — the residual filter reads `_distance` from `Plan.DocParsed`."*
- `String()` → `KnnSearch(k=%d,ef=%d)`.
- `planner.go:704-732` `buildVectorPlan`: new shape —
  ```
  VectorIter(ef) → [FilterIter(residual)] → LimitIter{Limit:K} → [SortIter] → [LimitIter{Offset,Limit}]
  ```
  The k-cut sits **after** the filter and **before** any user sort. `plan.Name = "KnnSearch"`, `plan.IndexName = params.Vector.IndexName`. (`setPlanRef` (`:1535-1562`) already recurses through `*LimitIter` and `*SortIter`. `LimitIter` is not an `offsetSkipper` — only `IndexIter`/`FetchIter`/`CanonicalKeyDedupIter` are — so no pushed-down skip can corrupt the k-cut. `Count`'s `LimitIter.CountDistinct()` fast path (`query.go:645`) grabs the outer `LimitIter` and computes correctly over the stacked shape; verified against `limit_iter.go:56-112` for all 8 sort×paging combinations.)
- Delete the dead `!vspec.Ordered` branch at `query.go:206-212`; fix the stale comments at `internal/vindex/search.go:16-19`.

#### T2.7 — TDD: the Verb-Coherence property test · **S**
**New file** `WT/query_verbcoherence_test.go`. Matrix over `{btree, hybrid, bruteforce, ivfpq, ivfsq} × {no residual, selective residual, **`_distance` residual**} × {no sort, Sort("-id"), Sort("_distance")} × {no limit, Limit(3), Offset(2).Limit(3)}`:
- `ids(Iter(Q))` == the ids `Delete(Q)` removes; `Count(Q) == len(ids)`; `Update(Q).Matched == len(ids)`; `Explain(Q).Sql` contains `KnnSearch`; `len(ids) <= $k`; two identical runs → identical output.
- **The `_distance` residual axis is mandatory**, on the *non-`Iter`* verbs — it is the axis that catches the T2.6 gating trap.
- **For `hybrid` mode: warm the l0 mirror and force a dirty overlay (queries + writes) before running `Iter`, then `Delete` on the same snapshot and require identical id sets.** `Delete`/`Update` pass `tx.btreeReadTx()`, for which `ReadTx.IsWriteTx()` is `true`, and `internal/vindex/search.go:58` gates the RAM l0 mirror + vector tier on `!rtx.IsWriteTx()`. So `Iter`/`Count` traverse the **RAM mirror** while `Delete`/`Update` traverse **raw btree adjacency** — an equivalence this design's central invariant assumes and which **has never been exercised**, because Delete/Update never reached the vector path at all. As specified, a naive matrix runs cold, publishes a fresh base, and never exercises the split. State the invariant in the code: *"an HNSW search over the l0 mirror is candidate-identical to one over the btree."*

#### T2.8 — `query.go`: the seam · **M** (~200 LOC, net −100)

`qplanner.BuildPlan` (`planner.go:284-294`) **already** dispatches `Vector → Fts → CBO`. The duplication is entirely in the anystore-side construction of `PlanParams` — 8 hand-written literals (`query.go:175, 213, 234, 323, 481, 624, 717` + `fulltext_search.go:979`), of which exactly one builds a `Vector` spec. **That asymmetry is BUG-32.**

```go
// planResult is what accessPlan returns. It carries cboIndexes because Explain
// needs them to build Explain.Indexes (query.go:735) and cannot rebuild them —
// accessPlan owns the PlanParams literal that consumes them.
type planResult struct {
    plan       *qplanner.Plan
    cboIndexes []qplanner.CBOIndex
}

// planOpts is the ONLY per-verb variance.
type planOpts struct {
    idBounds     query.Bounds
    countOnly    bool // Count
    exactCount   bool // Explain: docCountExact instead of docCountForPlan
    needSidecars bool // Iter only: populate Plan.Scores / Plan.Distances
    mutation     bool // Update/Delete: pass q.sort to the CBO only when limit/offset > 0 (Increment 1b)
}

// accessPlan is THE access-path seam: validate → $text → $knn → CBO. Every verb
// calls exactly this; a source not reachable from here is not reachable at all.
// It replaces ftsScanPlan and Iter's two inline BuildPlan blocks, and makes "only
// Iter runs vector detection" structurally impossible to reintroduce.
func (q *collQuery) accessPlan(btx *btree.ReadTx, buf *syncpool.DocBuffer, o planOpts) (planResult, error) {
    vidxs := q.c.loadVectorIndexes()
    hasText, hasKnn := containsText(q.cond), query.ContainsKnn(q.cond)

    // ---- UNCONDITIONAL GUARDS: they must run on EVERY branch, not just the CBO
    // fall-through. Otherwise {"$text":…, "v":[768 floats]} and
    // {"a":{"$knn":…}, "b":[floats]} both survive into a residual as a literal
    // Comp{Eq, array}, match nothing against packed storage, and silently return
    // 0 rows / delete nothing on all six verbs.
    if err := rejectLegacyVectorClause(q.cond, vidxs); err != nil {
        return planResult{}, err
    }
    if !hasKnn && (filterRefsField(q.cond, qplanner.DistanceField) ||
        sortRefsField(q.sort, qplanner.DistanceField)) {
        return planResult{}, ErrDistanceWithoutVector
    }
    if hasText && hasKnn {
        return planResult{}, ErrKnnWithText
    }
    // ---- SOURCE DISPATCH
    if hasText { … Fts spec, Sorter: ftsSorter(q.sort) on EVERY verb … }
    if hasKnn  { … Knn spec, Sorter: q.sort         on EVERY verb … }
    // ---- CBO (the existing literal, query.go:234-248)
}
```

Call sites:

| verb | before | after |
|---|---|---|
| `Iter` `:130` | `detectFtsQuery` + `detectVectorQuery` inline, **three** `&planIterator{…}` literals | one `accessPlan(…, planOpts{idBounds: qb.idBounds, needSidecars: true})` + **one** `planIterator` |
| `Update` `:259` | `ftsScanPlan` + `if !isFts { BuildPlan }` | `accessPlan(…, planOpts{idBounds: qb.idBounds, mutation: true})` |
| `Delete` `:423` | same | same |
| `Explain` `:687` | same, + `cboIndexes` built at `:712` | `accessPlan(…, planOpts{idBounds: qb.idBounds, exactCount: true})`; `explain.Indexes` from `res.cboIndexes` |
| `Count` `:545` | `findTextFilter` → **re-enters `Iter`** (`:568-580`) | drop the re-entry; `accessPlan(…, planOpts{idBounds: idBounds, countOnly: true})` inside the existing `doReadTx` (`:613`) |

Also in T2.8:
- **Delete `ftsScanPlan`** (`fulltext_search.go:970`).
- **Delete `Query.VectorEf`** (`query.go:36-39`, `:87-89`, `:117-120`) and the `vectorEf` field.
- **PREREQUISITE FIX — hoist `if q.err != nil` above `Count`'s raw fast path** (`query.go:549-553` vs `:555`). Probe-confirmed: `Find('{"a":{"$bogusOp":1}}').Count(ctx)` returns **`n=3, err=nil`** while `.Iter()` correctly errors — `Cond` leaves `q.cond == nil` on a parse error and the fast path fires. Every `$knn` validation error would be swallowed identically. **Non-optional.**
- **PREREQUISITE FIX — validate sources before `unsatisfiable()`.** `Iter` `:141`, `Update` `:277`, `Delete` `:435`, `Count` `:586` all short-circuit on `q.unsatisfiable()` **before** detection; `Explain` `:687` does not. So `{"v":{"$knn":{…wrong dim…}}, "x":{"$in":[]}}` gives `Iter`/`Count`/`Delete` → `0, nil` while `Explain` → `ErrInvalidVectorQuery`. Detection is declared *authoritative*, and it is the **only** validation the two programmatic production consumers ever see. Add `q.validateSources()` (the detection walk, no tx needed) immediately after the `q.err` check and **before** `unsatisfiable()` on all five verbs; add `unsatisfiable()` to `Explain` for parity.

#### T2.9 — `aggregate.go` · **S**
- **Delete `hasVectorClause`** (`:138-168`) — a third hand-rolled shape guess, one level deep, blind to `*And`/`$or`/`$not`.
- `validateInPipelineStages` (`:115-136`) becomes:
  ```go
  if containsText(m.Filter)                          { return errAggTextNotInPrefix }
  if query.ContainsKnn(m.Filter)                     { return errAggVectorNotInPrefix }
  if err := rejectLegacyVectorClause(m.Filter, vidxs); err != nil { return err }   // ← KEEP loadVectorIndexes()
  ```
  ⚠️ **The first pass deleted the `loadVectorIndexes()` call and the legacy check.** But `rejectLegacyVectorClause` lives in `accessPlan`, which only ever sees the **pushdown prefix** filter (`cq.cond`, built at `:95-102`) — never `rest`'s in-pipeline `$match` filters. So `[{"$skip":0},{"$match":{"v":[…768 floats…]}},{"$count":"n"}]` would go from **an error today** (asserted at `/home/che/projects/any-store-tests/e2e/aggregation_test.go:1102` and `:1108`) to a silent `n=0, err=nil` — committing the exact sin §11 spends a page forbidding. **Keep the index load; add the legacy check.**
- The `*And`/`$or`/`$not` blind spots close for free, and `$knn` on an **unindexed** field is now rejected (today `len(vidxs)==0` at `:131` means *no rejection at all*).
- Reword `errAggVectorNotInPrefix` (`:172`) to name `$knn` — but **keep the word "vector"** (`aggregation_test.go:1102/:1108` assert `ErrorContains(err, "vector")`).
- Aggregate's broken `Explain` (`:245` → `cq.Explain`) is fixed **for free** by T2.8.

#### T2.10 — Explain reports the vector index · **S**
`query.go:735-749` builds `explain.Indexes` only from `cboIndexes` (range indexes). Enumerate `loadVectorIndexes()` and `loadFtsIndexes()` too, with `Used = (name == plan.IndexName)`. `Explain` printing `FullScan(filtered) -> Limit(10)` for a working ANN query is *why nobody noticed that four verbs were ignoring the vector clause*.

#### T2.11 — migration · **M** (mechanical) · T2.12 — docs · **S** · T2.13 — downstream PRs · **S**

---

## Test plan

### Unit / integration (`WT`)

| test | file | asserts |
|---|---|---|
| `TestComp_VectorIsNotOrdered` | `query/filter_vector_test.go` **(new)** | Rule V, all 33 rows of T1.1 incl. hand-built + `$not`/`$nor` |
| `TestCompOkScalar_MatchesMarshalReference` | `query/comp_fastpath_test.go:20` | **UPDATE** the reference to Rule V — the only existing test Rule V breaks |
| `TestParse_VectorOperandNotOrderable` | `query/cond_parse_test.go` | T1.3's parse rejection |
| `TestType_VectorF32` | `query/type_test.go` | `$type:"vectorF32"` and `$type:10` — **on no index, `"v"`, AND `"-v"`** |
| `TestKnnParse` / `TestKnn_OkIsFalse` | `query/knn_parse_test.go` **(new)** | full grammar, both `$query` encodings, `String()` round-trip, fail-closed |
| `TestGuaranteesPresence_SourceFilters` | `query/filter_test.go` | `Key{v, Knn}` and `Key{v, Text}` → **false** |
| `TestFilterConcurrentReuse` / `…AllocFree` / `…BoundsCapped` / `TestFilterOkRawParity` | `query/filter_contract_test.go`, `filter_raw_test.go` | `filterZoo` gains `$knn` and `$eq:{"$vector":…}` |
| `TestDetectKnn_*` | `vector_knn_detect_test.go` **(new)** | the 12 detection rows of T2.4, incl. the **nested-`*And` residual-leak** shape and `ErrAmbiguousVectorIndex` |
| `TestKnnResidual_NoKnnSurvives` | `vector_knn_detect_test.go` | **post-condition**: `!ContainsKnn(residual)` for every legal placement |
| `TestVerbCoherence_Knn` | `query_verbcoherence_test.go` **(new)** | **the core property**: 5 modes × {none, selective, **`_distance`**} residual × 3 sorts × 3 pagings; hybrid runs **warm-mirror + dirty-overlay** |
| `TestVerbCoherence_CBO` | `query_verbcoherence_test.go` | **Increment 1b**: sorted+limited `Delete`/`Update` == `Iter` |
| `TestVivf_EfCutIsDeterministic` | `internal/vivf/store_test.go` | **Increment 1c**: pre-grown pooled searcher + exact-tie group straddling the cut → identical membership |
| `TestVectorIter_TotalOrder` | `internal/qplanner/vector_iter_test.go` | `(distance, docId)`; truncate-to-`K`; `injectDistance` fires with `NeedDistances=false` |
| `TestBuildVectorPlan_KCutBeforeSort` | `planner_test.go` | `KnnSearch(...) -> Filter -> Limit(...) -> Sort(...) -> Limit(...)` |
| `TestNoPlanContainsKnnInFilter` | `planner_test.go` | **the fourth door**: no built plan has a `FilterIter` whose tree contains a `Knn` |
| `TestPipeline_*` | `vector_pipeline_test.go` | migrate 9 sites; **rewrite the error table at `:205-210`** |
| `TestAggregation_*` | `aggregate_test.go` | `$knn` in prefix works; outside rejected incl. `$and`/`*$and`/`$or`/`$not`; **in-pipeline legacy clause still errors** |

### storetest regression (`/home/che/projects/any-store-tests`)

**`TestBug32_VectorClauseIgnoredMassDelete`** (`storetest/query_correctness_test.go:812`) — **rewrite** as three subtests:
1. `no ordering comparison against a vector ever matches everything` (Increment 1; syntax-independent, must hold forever)
2. `legacy bare-array clause is rejected on every verb` (Increment 2 → `ErrLegacyVectorClause` × 5 verbs)
3. `verb coherence for $knn` — `Count`/`Update`/`Delete`/`Explain` all agree with `Iter`; `Delete` removes exactly those and nothing else

New siblings:
- `TestVectorOrderingNeverMatchesEverything` — **including the collection with NO vector index**, where `{"n":{"$lt":{"$vector":[0]}}}` (parse error) and its hand-built equivalent (`Matched==0`) must be no-ops. This is the widest form of the data-loss hole.
- `TestBug32_KnnDeleteIsBoundedByK` — 100 docs, `$k:5` → `Delete` removes exactly 5.
- `TestBug32_KnnNoIndex` / `TestBug32_KnnAmbiguousIndex` — `ErrNoVectorIndex` / `ErrAmbiguousVectorIndex` on all five verbs.
- `TestBug32_SortedLimitedDeleteMatchesIter` — Increment 1b, **no vectors involved**.

### Fuzz / property (cheap, high value)
`Count(Q) == len(Iter(Q))` over randomized `$knn` + residual + sort + limit/offset, across all 5 index modes.

---

## Migration checklist

### any-store (worktree) — **~24 edit points / 11 files** (the first pass said 16/6)

| file | edits | note |
|---|---|---|
| `vector_index_test.go:54-62` (`vsearch` body) | 1 | **absorbs 34 call sites.** Drops `.Limit(k)`/`.VectorEf(ef)`; **`k<=0` is no longer legal** — all 34 in-repo sites pass `k > 0` (verified). |
| `vector_index_test.go:292, 376` | 2 | direct |
| `vector_pipeline_test.go:55, 89, 114, 162, 181, 229, 240, 280, 283` | 9 | `:181` is `.VectorEf(5)` → `"$ef":5` |
| `vector_pipeline_test.go:205-210` | 1 block | the error table **inverts** — but see the §10 correction below |
| **`vector_modes_test.go:23-31`** (`vectorEqFilter`) | 1 | ⚠️ **missed by the first pass.** A *programmatic* ANN helper — `query.Key{Path:["v"], Filter: query.NewCompValue(query.CompOpEq, arr)}` — the identical pattern to `any`'s downstream site. It becomes `ErrLegacyVectorClause`, and **no grep for `vqJSON`/`vsearch` finds it.** |
| **`vector_modes_test.go:142`, `vector_ivfsq_test.go:84`, `vector_ivfpq_test.go:117`** | 3 | its call sites |
| `vector_ivfpq_test.go:150`, `vector_ivfpq_bench_test.go:33` | 2 | `:150` is the residual case — keep it |
| `vector_compaction_test.go`, `vector_entry_repoint_test.go`, `vector_ivfpq_mp_test.go` | ~4 | ⚠️ also missing from the first pass's table (they use `vsearch`) |
| `collection_rename_lifecycle_test.go:222` | 1 | the only vector query outside a `vector_*` file |
| `query/comp_fastpath_test.go:20` | 1 | Increment 1's parity reference |

### any-store-tests — **~14 edit points / 9 files**

| file | edits | note |
|---|---|---|
| `e2e/helpers_test.go:162-168` (`vsearch`) | 1 | absorbs **10** call sites; also holds a `.VectorEf` at `:168` |
| `e2e/aggregation_test.go:1059, 1082, 1098, 1105, 1123` | 5 | keep and **extend** the "a non-ANN predicate on a vector field stays a legal literal filter" case at `:1110-1118` — that becomes the rule, not the exception. `:1102`/`:1108` assert `ErrorContains(err, "vector")` — keep the word. |
| `e2e/vector_dataset_test.go:134` | 1 | residual case |
| `storetest/multiprocess_vector_test.go:68-75` (`mpVecQueryJSON`) | 1 | **add a `k` parameter** |
| `storetest/multiprocess_vector_ivf_test.go:75` | 1 | inline |
| **`storetest/verifier.go:133`** | 1 | ⚠️ the crash verifier's `knnSearch` |
| `storetest/query_correctness_test.go:833` | 1 | the BUG-32 test itself |
| `benchmark/vecgen.go:52-70` (`vecQueryJSON`) | 1 | **add `k` AND `ef` parameters** (the first pass said `k` only) |
| **`benchmark/scenarios_vector.go:118`** | 1 | ⚠️ **missed by the first pass.** `q = q.VectorEf(uint(ef))` — a **hard compile break** the moment `Query.VectorEf` is removed. `runVecQuery`'s `k`/`ef` params must be plumbed into the **clause**, not the builder, changing its contract and every caller. Absorbs 5 sites (`:48,49,50,193,443`). |

### Downstream — **2 one-line edits, and they decide the API shape**

Scan of every `go.mod` under `/home/che/projects/`:

| repo | dep | status |
|---|---|---|
| `anytype-heart` | `any-store v0.4.7` (**v1** path) | 0 vector usage. Unaffected. |
| **`any`** | `any-store/v2 v2.0.0-alpha.15` | **1 ANN construction site.** |
| **`any-p2p`** | `any-store/v2 v2.0.0-alpha.16` | `internal/indexer/store.go` **byte-identical** to `any`'s. Same 1 site. |
| `any-sync-sdk`, `any-sync-sdk-p2p` | `any-store/v2 v2.0.0-alpha.16` (+ v1 `v0.4.7`) | **0 vector hits** (verified by grep). Unaffected. |

`/home/che/projects/any/internal/indexer/store.go:~708` builds the ANN query **programmatically, never from JSON**:

```go
arr := arena.NewArray()
for i, f := range vec { arr.SetArrayItem(i, arena.NewNumberFloat64(float64(f))) }
var filter query.Filter = query.Key{Path: []string{"vector"}, Filter: query.NewCompValue(query.CompOpEq, arr)}
if sk := scopeKey(scopes); sk != nil { filter = query.And{filter, sk} }
iter, err := coll.Find(filter).Limit(uint(limit)).Iter(ctx)
```

→ becomes

```go
var filter query.Filter = query.Key{Path: []string{"vector"}, Filter: query.NewKnn(vec, limit)}
if sk := scopeKey(scopes); sk != nil { filter = query.And{filter, sk} }
iter, err := coll.Find(filter).Iter(ctx)
```

**Two non-negotiable consequences:**

1. **`$knn` MUST ship an exported Go constructor + `Filter` type**, not just a JSON token. A JSON-only operator leaves `any` and `any-p2p` with **no migration path at all**.
2. **Non-`$knn` clauses on a vector-indexed field MUST stay ordinary filters on every verb.** `store.go:365` does `coll.Find(query.Key{Path:["vector"], Filter: query.Exists{}}).Count(ctx)` to decide whether to build the index — and it works **today only because `Count` skips detection**. Any fix that naively runs today's shape-guessing `detectVectorQuery` on `Count` would hit `vector_index.go:585-588` (`!isComp || CompOp != CompOpEq → ErrInvalidVectorQuery`) and **`any`'s `EnsureVectorIndex` would start erroring and never build its index.** This is why `ErrInvalidVectorQuery` must narrow. (`Exists` is not a `*Comp`, so it survives.)

`any` stores embeddings **packed** (`store.go:482`) and queries with a **number array** — the exact combination for which "bare array silently becomes an ordinary filter" returns **0 rows, `err == nil`**.

### Also affected (the first pass called these "not affected")
- **`cmd/any-store-cli2/commands.js:16`** — exports only the `$vector` **value** constructor. After this change the CLI's JS surface has **no way to express an ANN search at all**, and `Vector([...])` in a `find` on a vector-indexed field becomes `ErrLegacyVectorClause`. Add a `Knn(vec, k, opts)` helper. Small, but it is a task, not a non-event.

### Genuinely not affected (verified)
`wasm/` (0 vector hits, no JS query surface) · `cmd/vectorbench` (imports the **dead** `vector/` package; never calls `Find`) · `example/` · `docs/repro/`.

### Release context
Module `github.com/anyproto/any-store/v2`, current tag **`v2.0.0-alpha.17`**; **no `v2.0.0` final**. Vector search **does not exist on `main`** — it lives only on the `btree` line. `any-store-tests` uses `replace` and migrates in lockstep; `any`/`any-p2p` pin published tags. Breaking is acceptable; **zero-consumer is false.**
**UNVERIFIED:** whether any repo *outside* `/home/che/projects/` imports `any-store/v2` and uses the vector query API. Worth one GitHub code search before the tag.

---

## Docs checklist

| file | change |
|---|---|
| `docs/vector-search.md` (320 LOC) | 10 syntax occurrences (`:106, 113, 130, 133, 134, 137, 154, 312` + the `:89` sample). **The error table at `:168-171` inverts** and gains `ErrLegacyVectorClause`, `ErrNoVectorIndex`, `ErrAmbiguousVectorIndex`, `ErrKnnBadPlacement`, `ErrKnnWithText`. `:154` `.VectorEf(200)` → `"$ef":200`. **New section: "`$knn` is a ranked source, not a predicate"** — `k` selects, `Sort` orders, `Limit` paginates; `Count` is a page size; `Delete` removes exactly the k nearest; recall < 1. **Correct `:130`**: `_distance:{"$lt":x}` is a threshold **within** the k nearest, not a radius. |
| `docs/aggregation.md` (154 LOC) | `:33, 51-52, 98-99, 108, 113, 152` — "vector clauses" → `$knn`; prefix-only; `$k` removes the silent-`ef`-truncation hazard. |
| `docs/vector-engine.md` | `:8-9` — "a vector clause selects candidates" → `$knn`. |
| `docs/query-filter-contract.md` (41 LOC) | **New rule 5: source filters** (`Text.Ok`→true, `Knn.Ok`→false; external `Ok`-consumers must reject via `query.ContainsSourceFilter`). **New rule 6: `TypeVectorF32` is not orderable** (Rule V) — including the `$not` reflection note and the parse-time operand rejection. **New rule 7: `$vector`/`$oid`/`$binary` are forbidden as option-key names** inside an operator's options object. **New rule 8: a filter that overrides `Ok` must be checked against `GuaranteesPresence`**, which calls the inner `Ok` directly. |
| in-code | `index.go:26`; `vector_index.go:536-549`; `aggregate.go:15-21, 105-141`; `internal/qplanner/vector_iter.go:34-38` (**stale**: brute-force *does* set `Ordered`); `internal/vindex/search.go:16-19`; `anyenc/type.go:21-24` (soften "vectors never appear in index keys" — `index.go:661-667` marshals one into a key, and `$type:"vectorF32"` on a reverse index proves the invariant is violated). |
| `any-store-tests/docs/aggregation-coverage.md:54-58` | "the detection mirrors `detectVectorQuery` exactly" — becomes false; rewrite as "a syntactic `$knn` walk". |

---

## Risks / open edges

1. **Two limits will confuse people.** `Find({"v":{"$knn":{…,"$k":10}}}).Limit(20)` returns 10; `.Offset(15).Limit(10)` returns 0. Erroring on `offset+limit > k` was rejected (it blocks legitimate paging). **Mitigation: one sentence in the doc comment — "`k` selects, `Sort` orders, `Limit` paginates."**
2. **Hybrid search under-returns, by construction.** With a selective residual, `{"$k":10}` may return < 10 despite the ×10 over-fetch. Escape valve is explicit: raise `$k`/`$ef`. Document `k ≈ desired / expected_residual_selectivity`.
3. **`Ok = false` is fail-closed, and therefore a *silent-zero-rows* trap** if a `Knn` ever leaks into a residual. Right direction for `Delete`, wrong one for debuggability. **Four doors close it:** parse rejection; detection-time `ErrKnnBadPlacement`; the aggregate guard; and the `!ContainsKnn(residual)` post-condition in `detectKnnQuery` + `TestNoPlanContainsKnnInFilter`. **The fourth door is not optional — the first pass's residual builder leaked on day one.**
4. **A pre-built `query.Knn` bypasses the parser entirely** (`cond_parse.go:77-79`) — and that is *exactly* the path the only two production consumers use. **Every parse rule must be re-checked at detection**, and detection must run **before** `unsatisfiable()` (T2.8).
5. **`GuaranteesPresence` is a landmine for any future `Ok`-overriding filter.** It calls the inner `Ok` *directly* and reads fail-closed as "guarantees presence" — the aggressive answer, feeding sparse-index selection. Fixed for `Knn`/`Text`; write the rule into the contract doc.
6. **The HNSW write-tx / read-tx path split is an untested equivalence.** `Delete`/`Update` search over raw btree adjacency; `Iter`/`Count` over the RAM l0 mirror + vector tier (`internal/vindex/search.go:58`). Verb Coherence *assumes* they are candidate-identical. This has never been exercised — Delete/Update never reached the vector path. **T2.7's hybrid axis must warm the mirror and force a dirty overlay.** Also a perf cliff: an ANN `Delete` loses the vector tier and does random btree `:vec` reads.
7. **Count is now O(candidates), not O(1).** One ANN search + a fetch/parse per surviving candidate — identical to what `$text` `Count` already pays. **Except in brute-force mode**, where every `Count`/`Delete`/`Explain` now pays a full `O(N·dim)` scan (~7 µs/doc). Inherent, and always true of `Iter`.
8. **A large `$knn` `Delete` self-degrades the index.** HNSW deletes are tombstones (`internal/vindex/hnsw.go:564, 663`), degrading search roughly linearly with the deleted/live ratio. `maybeAutoCompactVectors` fires post-commit (`query.go:442-446`); compaction is O(live) and synchronous. **UNVERIFIED:** whether a mass ANN delete can chain `repointEntry`'s cursor-scan fallback (`hnsw.go:637-654`) into quadratic behaviour. **Stress-test before shipping.**
9. **`$k ≤ 10 000` / `$ef ≤ 65 536` are policy, not semantics.** Raising a limit later is non-breaking; lowering one is not — start conservative.
10. **Rule V diverges from MongoDB** (which defines cross-type ordering for `$gt`). Deliberate: Mongo has no vector type, and ours sorts **last**, turning every `$gt` into a match-all against exactly the documents users least want deleted. Compensated by `$type:"vectorF32"`.
11. **`$not` of an undefined comparison is match-all.** After Rule V, `{"v":{"$not":{"$gt":1}}}` matches every document. This is standard Mongo type-bracketing, requires a deliberate `$not`, and is pre-existing for any unsatisfiable inner predicate. The operand-side family is killed by T1.3's parse rejection. Tested and documented; **not** silently accepted.
12. **`$type:"vectorF32"` costs a scan, always** (no index bounds). Correct, and the only sound option short of forbidding reverse indexes on vector-valued fields.
13. **Aggregate still cannot express `$knn` outside the pushdown prefix, and that is final.** `AggQuery` has no `Delete`/`Update`, so the rejection costs expressiveness, never data. Mongo's `$vectorSearch` is first-stage-only for the same reason.

### Follow-up bugs found during grooming (file separately, out of scope)

1. **`ftsResidualFilter` has the same nested-`*And` leak** as the one fixed for `Knn` — benign only because `Text.Ok` returns `true`. It should recurse.
2. **`$type:"objectId"` on a reverse index** has the same under-approximation as `$type:"vectorF32"` whenever the oid's first byte is `0x00`.
3. **IVF silently degrades `VectorDot` to L2.** `vivf.StoreParams` has no `Metric` field (`internal/vivf/store.go:30`) — only `Normalize`.
4. **`SortIter` drops `_distance` from `Doc()`.** `sort_iter.go:88-92` clears `Plan.DocParsed`, so `planIterator.Doc()` re-fetches without the injected field. `Distance()` still works (sidecar). With `$knn` making sorted ANN results normal, re-inject from the sidecar after sorting. **Note it collides with T2.6's `NeedDistances` gate** — on Count/Delete/Update there is no sidecar to re-inject *from*. They coexist today only because those verbs never call `Doc()`.
5. **`Explain` skips `unsatisfiable()`** (fixed opportunistically in T2.8, but the general asymmetry deserves its own audit).
6. **`_distance:{"$lt":x}` silently truncates to `ef` on HNSW/IVF** — a real limitation (no radius search), currently mis-sold by `docs/vector-search.md:130`. Documented, not fixed.

---

## Ship order

| | what | files | LOC | breaking | closes |
|---|---|---|---|---|---|
| **1** | Rule V + parse rejection + `$type:"vectorF32"` (+`IndexBounds` fix) | `query/{filter,type,cond_parse}.go` (+3 test files, incl. the parity reference) | ~110 | **no** | **D1 — the mass delete**, on every verb and every field, immediately |
| **1b** | *(not ours)* sorted+limited `Update`/`Delete` pass the sorter — **filed and fixed as BUG-47** | `query.go` | ~15 | **no** | a **second, independent** data-loss bug: `Sort(…).Limit(n).Delete()` deletes the wrong `n`. **Dependency of Increment 2**; do not re-fix. |
| **1c** | source-level `(dist,label)` cut + `(dist,docId)` output | `internal/vivf/store.go`, `internal/vindex/` (+2 tests) | ~60 | **no** | IVF ef-cut membership non-determinism |
| **2** | `$knn` + the seam + the legacy error | `query/{filter,cond_parse}.go`, `vector_index.go`, `query.go`, `fulltext_search.go` (−`ftsScanPlan`), `aggregate.go`, `internal/qplanner/{planner,vector_iter}.go` + ~38 call sites + 6 docs | ~700 | **yes** | **D2 + D3** — Verb Coherence; the `*And` blind spot; the `$or`/`$not`/no-index/ambiguous-index/`$text` silent degradations; aggregate's third clause-walker; Explain's lie; Count's swallowed `q.err`; the `unsatisfiable` validation bypass |

---

## Appendix: rejected adversarial findings

Everything else in the four reviews was verified and **accepted** (folded into the body above). These three are rejected:

| finding | source | verdict |
|---|---|---|
| **"`any-sync-sdk` and `any-sync-sdk-p2p` pin only `any-store v0.4.7` (the v1 path); only `any`, `any-p2p`, `any-store-tests` import `/v2`. The §11.3 table is wrong."** | plan-feasibility MINOR 13 | **REJECTED — the critic is wrong.** `/home/che/projects/any-sync-sdk/go.mod:7` and `/home/che/projects/any-sync-sdk-p2p/go.mod:7` both require `github.com/anyproto/any-store/v2 v2.0.0-alpha.16` (alongside v1 `v0.4.7`). The original table was correct. Its *conclusion* is also correct and independently verified: `grep -rl 'VectorEf\|EnsureVectorIndex\|NewVectorF32\|VectorIndex'` over both repos returns **nothing** — 0 vector hits, unaffected. |
| **"§0's claim that the probe was deleted is false — `zz_canary_probe_test.go` is present and untracked at the worktree root."** | plan-feasibility MINOR 14 | **REJECTED for the worktree.** `git status --short` in `/home/che/projects/any-store/.claude/worktrees/bug-32-vector-clause` is **empty** at `18a38bc`. The stray file lives in the *main* checkout (`/home/che/projects/any-store`), which is a different working tree. It is not a defect in the design under review, and the worktree the design instructs everyone to read is clean. (Worth deleting from the main checkout anyway — but that is housekeeping, not a grooming finding.) |
| **"`ErrLegacyVectorClause` will reject `{"v":{"$eq":[]}}` forever — `anyenc.AppendFloat32s` returns `ok=true` for an empty array."** | data-loss MINOR 11 | **REJECTED as stated — subsumed, not a residual risk.** The mechanism is real, but the finding is moot under the corrected §11 scoping: the legacy check now requires a `TypeArray` of **exactly `index.dim`** numbers, and `validateVectorParams` forbids `dim == 0`. An empty array can therefore never trip it. Recorded here so the next reader does not "re-fix" it by loosening the dim check. |

**One finding accepted, but with its remedy rejected:** data-loss BLOCKER 2 correctly identifies the sorted+limited `Update`/`Delete` data-loss bug and correctly refuses the `cboSorter` flag, but proposes a **hard error** as the fix. Rejected in favour of the real fix (Increment 1b): pass `q.sort` to the CBO `BuildPlan` for mutations **when `limit`/`offset` > 0**. This is ~15 LOC, makes `Delete` *correct* rather than merely loud, and avoids the plan-selection regression the critic feared by leaving the unlimited case (where order is semantically irrelevant to a mutation) untouched.