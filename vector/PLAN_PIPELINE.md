# Plan: vector search inside the common query pipeline

Branch `feat/vector-index`. Supersedes the standalone `Collection.VectorSearch`
API with a first-class query path so vector search composes with filters, sort,
limit, and the `_distance` virtual field — driven from `Find(...)`.

Target API:

```go
// minimum — implicit sort by distance, distance exposed on the iterator
iter := coll.Find(`{"chunkVector": [1,2,3]}`).Iter(ctx)
for iter.Next() {
    _ = iter.Doc()
    _ = iter.Distance()
}

// maximum — _distance as a filterable/sortable virtual field, plus normal filters
iter := coll.Find(`{"chunkVector": [1,2,3], "_distance": {"$gt": 0.5}, "lang": "en"}`).
    Sort("_distance,name").Iter(ctx)
for iter.Next() { _ = iter.Doc() }
```

Design directives (from the brief): **(1)** drop the result heap; **(2)** add a
vector iterator to qplanner; **(3)** CBO detects a vector query and initialises
the iterator, exposing a virtual `_distance`; **(4)** sort by `_distance` (simple)
or a given sort (advanced) using the existing optimised `SortIter`; **(+)** apply
additional (non-vector) filters when present.

All file:line anchors below are from the current tree.

---

## The keystone: `_distance` as an injected virtual field

`SortIter` (`internal/qplanner/sort_iter.go:145`) and `FilterIter`
(`filter_iter.go`) only read **real document fields** via `doc.Get(path...)` on
the parsed `*anyenc.Value`. There is no virtual-field hook today.

Solution: make `_distance` a *real* field on the **in-flight parsed document**.
When a vector result's doc is materialised, set `_distance` on the
`*anyenc.Value` (via an arena) before it flows downstream. Then:

- `{"_distance": {"$gt": 0.5}}` → the existing `Comp.Ok` reads `_distance` like
  any field. **Zero changes to FilterIter.**
- `Sort("_distance,name")` → `SortField.AppendKey` reads `_distance` then `name`.
  **Zero changes to SortIter.**

Because `SortIter` re-fetches docs by id on output (it clears `Plan.DocParsed`,
`iterator.go:48`/`sort_iter.go`), the distance must survive a re-fetch. So we
also keep a per-query **sidecar**:

```
Plan.Distances  map[string]float32   // docId(string) -> distance, set by VectorIter
```

A single doc-materialisation hook injects `_distance` from the sidecar every time
a vector-query doc is parsed (initial fetch, SortIter buffering, and the
`planIterator.Doc()` re-fetch). The sidecar also backs `iter.Distance()`.

`_distance` is a reserved field name: the planner never matches it to a real
index; it only exists for vector queries.

---

## A. Detection (CBO stage, in `query.go` where vindexes are known)

Vector indexes live in `collection.vindexes` (not in qplanner). `Find().Iter()`
(`query.go:116`) already loads indexes and builds `PlanParams`. Add a pre-plan
scan there:

1. Walk the parsed top-level filter (the `And` of `Key→Comp`). For each clause
   `Key(field) → Comp(Eq, arrayValue)` where `field` has a **vector index** and
   `arrayValue` is a numeric array of the index dim → this is the vector clause.
2. Extract the query vector `[]float32` from the array value (trivial here — we
   have the parsed `*anyenc.Value`, no need to decode index bytes).
3. Build the **residual filter** = the original filter minus the vector clause
   (keeps `_distance` predicates and all other field filters intact).
4. Emit a `VectorQuerySpec` into `PlanParams`:
   ```go
   type VectorQuerySpec struct {
       Query  []float32
       Ef     int                 // numCandidates; default = index EfSearch
       Search func(tx *btree.ReadTx, q []float32, ef int) ([]VectorCandidate, error)
   }
   type VectorCandidate struct { DocId []byte; Distance float32 }
   ```
   The `Search` closure is supplied by the anystore layer and captures the
   `vindex.Index`, so qplanner stays free of any vindex dependency.
5. If no explicit `Sort` was given, the planner injects an implicit
   `Sort("_distance")` (ascending) — the "simple variant".

Rationale for detecting in `query.go` rather than deep in `BuildPlan`: vector
indexes and the parsed `anyenc` values both live in the anystore layer, so vector
extraction is a field read, not an index-byte decode. The planner just receives a
ready directive — keeping the qplanner generic.

Ambiguity note: `{"chunkVector": [..]}` is syntactically an array *equality*. On a
vector-indexed field we reinterpret it as ANN (equality on a raw embedding is
never a useful query). If a literal array-equality is ever needed on such a
field, add an explicit operator (e.g. `$eq`) escape — out of scope for v1.

---

## B. `VectorIter` (new source iterator in qplanner)

`internal/qplanner/vector_iter.go`, implementing `Iterator`
(`Next() (key, docId, multiKey, err)`, `Close`, `String`):

- **First `Next()`**: run `spec.Search(tx, spec.Query, spec.Ef)` → the candidate
  set `[]VectorCandidate`. Populate `Plan.Distances`. Hold the candidates.
- **Each `Next()`**: pop the next candidate; fetch+parse its doc (like
  `FetchIter`, `fetch_iter.go`); inject `_distance` via the shared decorator; set
  `Plan.DocParsed`; return `(nil, docId, false, nil)`.
- Emission order is **unsorted** (see C); ordering is delegated to `SortIter`.
- `Close()` releases buffers.

It reuses the plan's `CursorSource` (the read tx) for doc fetch, so the HNSW
search and the doc reads share one transaction/snapshot.

---

## C. Drop the result heap (vindex change)

`vindex` currently drains a max-heap and reverses to return sorted `Hit`s
(`search.go`). For the pipeline, add:

```go
func (ix *Index) SearchCandidates(rtx *btree.ReadTx, q []float32, ef int) ([]Candidate, error)
```

returning the **unsorted** live candidate set (`{label/docId, distance}`). The
final sort/limit is the pipeline's job (`SortIter` + `LimitIter`), so vindex stops
ranking results.

Scope of "drop heap": the *result ranking* heap is removed. The HNSW beam search
still needs its candidate/result frontier internally — the ef-th distance is the
admission/termination bound of the algorithm, so those small per-call heaps stay.
(If a fully heap-less beam is wanted, that's an algorithm change with recall
implications — flagged as a decision point, not assumed.)

---

## D. Plan shape

```
VectorIter(spec)                          ← source: ANN candidates + _distance inject
  └─ FilterIter(residual)        (if any) ← _distance threshold + additional field filters
       └─ SortIter("_distance"[,"name"…]) ← reuse optimised arena sort + TopK heap for limit
            └─ LimitIter           (if limit/offset)
```

- **Minimum**: residual empty, implicit `Sort("_distance")`.
- **Maximum**: residual = `{_distance>0.5, lang:"en"}`, explicit
  `Sort("_distance,name")`.
- `SortIter`'s existing `TopK` max-heap (`sort_iter.go:104`) handles `limit`
  efficiently over the small candidate set.

**Hard rule — a vector query ignores all other indexes.** `BuildPlan`
(`planner.go:224`) gets an early branch: if `params.Vector != nil`, build this
chain and `return` immediately, *before* any range-index cost evaluation. The
HNSW search is the sole source; there is no cost comparison, no candidate range
plan, no full-scan fallback. Range indexes (and the CBO entirely) are bypassed
for the query. The residual filter and sort run only as downstream
`FilterIter`/`SortIter` stages over the ANN candidates — never as an index
choice. (The vector clause is also removed from the residual so the planner can't
even see a bound on the vector field.)

---

## E. Public iterator: `Distance()` + additional filters

- Add `Distance() float32` to the `Iterator` interface (`iterator.go:15`).
  `planIterator.Distance()` returns `plan.Distances[string(pi.docId)]` (0 for
  non-vector queries).
- **Additional filters** (the brief's "+"): the residual filter flows through the
  normal `FilterIter`, so any non-vector predicate (`lang:"en"`, ranges, `$in`,
  …) is applied to the ANN candidates exactly as in a normal query — **no new
  code**, it falls out of the residual-filter design.

This is **post-filtering** (ANN first, then filter). It can under-fill when the
filter is selective (MongoDB's "asked for 10, got 3"). Mitigations:
- v1: over-fetch — default `Ef = max(EfSearch, k·factor)` and let the user raise
  `numCandidates`.
- v2: "iterative" vector scan — `VectorIter` re-runs/extends the search until
  enough post-filter survivors are found (mirrors Mongo's `iterative_scan`).
- v3: true pre-filtering — push a cheap predicate into the ANN walk. Advanced;
  out of scope here.

---

## F. numCandidates / Ef control

The examples don't specify ef. Options, in order of preference:
- Default `Ef = index.EfSearch` (and `Ef = max(Ef, limit)` when a limit is set).
- Optional query knob: `Find(...).VectorEf(128)` (a new `collQuery` field), or a
  reserved `_ef` clause in the filter that the detector consumes. Recommend the
  builder method — explicit and discoverable.

---

## G. Touch list

| File | Change |
|------|--------|
| `vindex/search.go` | add `SearchCandidates` (unsorted candidate set) |
| `internal/qplanner/planner.go` | `PlanParams.Vector *VectorQuerySpec`; early vector-plan branch in `BuildPlan`; implicit `_distance` sort |
| `internal/qplanner/vector_iter.go` | new `VectorIter` + `Plan.Distances` sidecar + `_distance` inject decorator |
| `internal/qplanner/sort_iter.go`, `filter_iter.go` | **no change** (read `_distance` as a field) — but route their doc-materialise through the decorator |
| `query.go` | detect vector clause vs `c.vindexes`, extract query vector, compute residual filter, build `VectorQuerySpec` + `Search` closure, inject implicit sort, `VectorEf` builder |
| `iterator.go` | `Iterator.Distance()`; `planIterator.Distance()` from sidecar; route `Doc()` re-fetch through the decorator |
| `vector_index.go` | provide the `Search` closure (wraps `vindex.SearchCandidates`); keep `VectorSearch` as a thin wrapper over the new path (or deprecate) |

---

## H. Edge cases / risks

- **`_distance` without a vector clause**: field is absent → predicate never
  matches / sort sees null. Document as undefined; optionally error in the
  detector.
- **Multiple vector clauses / multiple vector indexes**: v1 supports one vector
  clause per query; error if two are present.
- **Mutating the parsed doc**: `_distance` is set on the transient in-flight
  `*anyenc.Value` via an arena; verify `Value.Set` works on a `ParseOwned` value
  and that the injected field survives until the sort key is captured (it is
  re-injected on every materialise via the sidecar).
- **Sidecar lifetime/size**: bounded by the candidate count (ef, ~tens–hundreds);
  freed with the plan.
- **Re-fetch correctness**: the decorator must run on *every* doc-materialise path
  for vector queries (VectorIter fetch, SortIter output fetch, `planIterator.Doc`
  fallback) — single shared helper keyed off `Plan.Distances != nil`.
- **Recall under post-filter**: surface `numCandidates`/`VectorEf`; log when the
  result set is smaller than the limit (truncation signal).

---

## I. Phasing

1. **Core**: `SearchCandidates`, `VectorIter`, `_distance` inject + sidecar,
   detection in `query.go`, implicit `_distance` sort, `Distance()` on the public
   iterator. → minimum API works.
2. **Filters + sort**: residual `FilterIter` (additional filters + `_distance`
   threshold), explicit multi-key sort. → maximum API works.
3. **Controls**: `VectorEf`/numCandidates, over-fetch defaults, truncation
   signal.
4. **Iterative scan**: re-extend the ANN search until enough post-filter
   survivors (recall under selective filters).
5. (later) pre-filtering pushed into the ANN walk.

Keeps the existing `Collection.VectorSearch` working throughout (it becomes a
thin wrapper over the same `Search` closure), so nothing regresses while the
pipeline path lands.
