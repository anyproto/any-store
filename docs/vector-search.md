# Vector search

any-store has a built-in vector (embedding) index for approximate nearest-neighbour
(ANN) search, implemented as a btree-resident HNSW graph. It integrates with the
normal `Find()` query pipeline: a `$knn` clause selects the k nearest documents,
ordinary filters and sorting apply on top, and each result carries its distance
to the query.

- **Storage-resident** — the graph lives in the btree, so it's crash-safe,
  MVCC-consistent across readers, and multiprocess-safe for writes. No separate
  in-memory index to build or invalidate.
- **Queried through `Find()`** — there is no separate search call; you express the
  query as `Find({ <vectorField>: {"$knn": {"$query": [...], "$k": 10}} })` and
  combine it with any other filter, sort, limit, and offset. Every verb —
  `Iter`, `Count`, `Update`, `Delete`, `Explain`, and an `Aggregate` `$match`
  prefix — denotes the same k-bounded document set for the same query.

## 1. Create a vector index

Register an index of kind `IndexKindVector` with `VectorParams`:

```go
err := coll.EnsureIndex(ctx, anystore.IndexInfo{
    Name: "emb",                       // index name (auto-generated if empty)
    Kind: anystore.IndexKindVector,
    Vector: &anystore.VectorParams{
        Field:  "embedding",           // path to the embedding field (required)
        Dim:    768,                   // embedding dimension (required)
        Metric: anystore.VectorCosine, // VectorCosine (default) | VectorL2 | VectorDot
    },
})
```

`CreateIndex` errors if the index already exists; `EnsureIndex` is idempotent.
Creating the index on a non-empty collection builds it from existing documents
using a **parallel in-RAM build** across all cores, then writes the finished graph
in one pass — far faster than inserting into a live index one vector at a time.

> **Bulk-load pattern.** To load a large dataset, insert all documents *first*
> (a plain doc-store write), *then* create the vector index. The one-shot parallel
> build is ~20× faster per vector than building the graph incrementally. Inserting
> into an already-existing index still works and is fully supported — it's just the
> slower path for a large initial load.

Build (and compaction) parallelism is a process-wide worker budget, default
`GOMAXPROCS`. Cap it with `anystore.SetVectorBuildConcurrency(n)` (e.g. `1`–`2`) so
many indexes building at once can't oversubscribe the CPU; it never affects search
or insert/update/delete. Like `InitPageBuffer`, it's a process-global call made once
at startup — not a per-database `Config` field.

Only `Field` and `Dim` are required. Other `VectorParams`:

| Field | Default | Purpose |
|---|---|---|
| `Metric` | `VectorCosine` | distance measure: `VectorCosine`, `VectorL2`, `VectorDot`. `VectorDot` is not supported by the IVF modes (index creation and open fail with `ErrVectorMetricUnsupported`) |
| `M`, `EfConstruction`, `EfSearch` | sensible defaults (0) | HNSW graph/search tuning |
| `Quantization` | `VectorQuantNone` | `VectorQuantInt8` ≈ 4× smaller, RAM-resident vectors |
| `Mode` | `VectorModeBTree` | `VectorModeHybrid` (RAM layer-0 cache), `VectorModeBruteForce` (exact O(N) scan, no graph), `VectorModeIVFPQ`/`VectorModeIVFSQ` (inverted-file cells; Cosine/L2 only) |
| `HybridCacheVectors` | `false` | hybrid only: also cache vectors in RAM for faster search |
| `CompactRatio` | `0` (off) | auto-compact the graph when tombstones reach this ratio of live nodes |

## 2. Document shape

The embedding lives in an ordinary field. Two encodings are accepted:

- **Packed vector** (recommended) — store it as the `anyenc` `TypeVectorF32`
  value, built with `Arena.NewVectorF32([]float32{...})`. It's a single packed
  little-endian `[]float32` blob: ~2× smaller in the document store than a number
  array, and decoded zero-copy (no per-element parsing) on both the write and the
  search path.
- **Number array** — a plain JSON/anyenc array of `Dim` numbers, e.g.
  `[0.12, -0.03, ...]`. Convenient when documents arrive as JSON, but larger on
  disk and slower to decode.

Either way the field must hold exactly `Dim` values. Nested paths work
(`Field: "meta.vec"`). Documents missing the field, or whose value isn't a
`Dim`-sized vector/numeric array, are simply not indexed.

```go
// Packed vector type (preferred): build the document with an Arena.
arena := &anyenc.Arena{}
obj := arena.NewObject()
obj.Set("id", arena.NewNumberFloat64(1))
obj.Set("title", arena.NewString("cat"))
obj.Set("lang", arena.NewString("en"))
obj.Set("embedding", arena.NewVectorF32([]float32{0.12, -0.03, /* ... Dim values */}))
coll.Insert(ctx, obj)

// Or a plain JSON array, when that's what you already have:
coll.Insert(ctx, anyenc.MustParseJson(
    `{"id":2, "title":"dog", "lang":"en", "embedding":[0.12, -0.03, ...]}`))
```

The two encodings are interchangeable within a collection — the index reads both,
and you can mix them or migrate by re-inserting documents. Reuse one `Arena`
across documents and call `arena.Reset()` between batches to avoid allocations.

Inserts are batched: pass many documents to one `Insert(...)` call so they share a
single transaction.

## 3. Query: the `$knn` operator

A query becomes a vector search when a **vector-indexed field** carries a `$knn`
clause:

```go
// The 10 nearest neighbours, closest first (the default order).
iter, err := coll.Find(`{"embedding":{"$knn":{"$query":[0.1, 0.2, ...], "$k":10}}}`).Iter(ctx)
```

`$knn` takes an options object — this is the only accepted form:

| Option | | |
|---|---|---|
| `$query` | **required** | the query vector: a plain number array or a packed `{"$vector":[...]}` value |
| `$k` | **required** | how many neighbours to select, `1..10000` |
| `$ef` | optional | ANN candidate/beam depth (`numCandidates`), `$k..65536`; default auto-sizes from the index and over-fetches ×10 when a residual filter is present |
| `$index` | optional | vector index name — required only when the field has more than one vector index |

Programmatic consumers build the clause with `query.NewKnn(vec, k, opts...)`
wrapped in a `query.Key` naming the field — no JSON involved:

```go
filter := query.Key{Path: []string{"embedding"}, Filter: query.NewKnn(vec, 10)}
iter, err := coll.Find(filter).Iter(ctx)
```

Everything else in the filter is applied as a residual predicate **before** the
k-cut — the query denotes the k nearest *matching* documents (hybrid search):

```go
// ANN + ordinary filters: the 10 nearest docs among those matching lang/year.
coll.Find(`{"embedding":{"$knn":{"$query":[...], "$k":10}}, "lang":"en", "year":{"$gt":2020}}`).Iter(ctx)
```

### `$knn` is a ranked source, not a predicate

"Is this document one of the k nearest?" is a property of the whole candidate
set, not of a document, so `$knn` does not behave like a predicate:

- **`$k` selects. `Sort` orders. `Limit` paginates.** The clause denotes at most
  `$k` documents; `Sort` reorders that set; `Limit`/`Offset` page within it.
  `Limit(20)` over `"$k":10` returns 10 rows; `Offset` at or past `$k` returns
  none.
- **`Count` returns the size of that page** (≤ `$k`) — not "how many documents
  match", because `$knn` has no match set.
- **`Delete`/`Update` mutate exactly the k nearest** — the blast radius is `$k`,
  a number you typed. "Delete everything near X" is unrepresentable.
- **The result is approximate** (recall < 1) except in brute-force mode. With a
  selective residual filter you may get fewer than `$k` rows; the escape valve
  is raising `$ef` (or `$k`).
- **The set is not stable across index churn** (compaction, tombstones, a
  different `$ef`). For exact determinism use brute-force mode, or collect ids
  from `Iter` and act by id.
- `$knn` is legal at the top level or under `$and` (any depth), once per query.
  `$or`/`$nor`/`$not`, a second `$knn`, or combining with `$text` are errors.

Non-`$knn` operators on a vector-indexed field (`$exists`, `$type`, `$eq`
against a packed `{"$vector":[...]}` value, `$size`, …) stay **ordinary
filters** on every verb.

### Distance: the `_distance` field

Each result is decorated with a synthetic `_distance` field (smaller = closer).
You can read it, filter on it, and sort by it.

```go
// Read it per row:
for iter.Next() {
    doc, _ := iter.Doc()
    d := iter.Distance() // == doc's _distance
    _ = doc; _ = d
}

// Filter by a distance threshold — note this is a threshold WITHIN the k
// nearest (a residual filter over the $knn candidates), NOT "all documents
// within 0.35": there is no radius search at any layer.
coll.Find(`{"embedding":{"$knn":{"$query":[...], "$k":100}}, "_distance":{"$lt":0.35}}`).Iter(ctx)

// Sort by distance (ascending is the default when no Sort is given):
coll.Find(`{"embedding":{"$knn":{...}}}`).Sort("_distance").Iter(ctx)   // closest first
coll.Find(`{"embedding":{"$knn":{...}}}`).Sort("-_distance").Iter(ctx)  // farthest first

// Multi-key sort: distance, then a tie-breaker field:
coll.Find(`{"a":{"$gt":42}, "embedding":{"$knn":{...}}}`).Sort("_distance", "-name").Iter(ctx)
```

`Iterator.Distance()` returns the row's distance (0 for non-vector queries).

`_distance` is a reserved synthetic field: on a vector query it is injected into
each result (shadowing any stored field of that name), and referencing it in a
filter or sort *without* a `$knn` clause is an error (`ErrDistanceWithoutVector`).

### Limit, offset, and candidate set size

`Limit`/`Offset` paginate over the (sorted) k-set — they never change *which*
documents `$knn` denotes. The ANN candidate list (`ef`) is a pure function of
the clause: the index default, raised to `$k` (over-fetched ×10 when a residual
filter is present, capped at ~4096 but never below `$k`). An explicit `$ef` is
used verbatim and never capped:

```go
coll.Find(`{"embedding":{"$knn":{"$query":[...], "$k":20, "$ef":200}}, "lang":"en"}`).Iter(ctx)
```

Because `ef` never depends on `Limit`/`Offset`/`Sort` or the verb, every verb
walks the same beam and denotes the same set — `Count == len(Iter)` by
construction.

## 4. Errors for malformed queries

Mistakes are reported rather than silently returning wrong results:

| Query | Error |
|---|---|
| The pre-`$knn` spelling: a bare `Dim`-sized array equality on a vector-indexed field — `{"embedding":[...768 floats...]}` | `ErrLegacyVectorClause` |
| Malformed `$knn`: wrong-dimension / empty / non-finite `$query`, out-of-range `$k` or `$ef` | `ErrInvalidVectorQuery` |
| `$knn` on a field with no vector index (or `$index` names none) | `ErrNoVectorIndex` |
| `$knn` on a field with several vector indexes and no `$index` | `ErrAmbiguousVectorIndex` |
| `$knn` under `$or`/`$nor`/`$not`, nested, bare, or unstrippable | `ErrKnnBadPlacement` |
| `$knn` and `$text` in one query | `ErrKnnWithText` |
| Two `$knn` clauses in one query | `ErrMultipleVectorClauses` |
| `_distance` in a filter or sort with no `$knn` clause | `ErrDistanceWithoutVector` |

All of these fire identically on every verb (`Iter`/`Count`/`Update`/`Delete`/
`Explain`/`Aggregate`), for JSON and programmatically-built filters alike.
Ordinary predicates on a vector field are NOT errors — `{"embedding":"x"}` or a
wrong-dimension array are literal field filters that simply match what they
match.

## 5. Updates, deletes, compaction

Updates and deletes are automatic: re-inserting a document id reindexes its vector;
deleting removes it from results. Deletes and replaces *tombstone* graph nodes
rather than removing them, so storage and search cost slowly grow with churn.
Reclaim them with `CompactRatio` (automatic) or `Collection.CompactVectorIndex(ctx,
name)` (manual, e.g. in a maintenance window). Compaction rebuilds the live set with
the same parallel builder used for index creation (so it's fast — seconds, not
tens of seconds, for tens of thousands of live vectors). See
`VectorParams.CompactRatio` for sizing guidance.

## 6. Choosing a mode (and tuning)

Pick by your dominant constraint. Figures below are file-backed and measured;
treat them as ratios, not absolutes (they scale with N, dim, and hardware).

| Your situation | Use | Why |
|---|---|---|
| **Default / unsure** | `Hybrid` + `Int8`, `EfSearch 64` | the mirror costs only a few MiB and is never slower than btree; int8 is ~4× smaller at ~0.5% recall cost |
| **Lowest latency, RAM to spare** | `Hybrid` + `HybridCacheVectors` + `Int8` (or f32) | vectors served from RAM → sub-0.3 ms p50, ~3–9× the btree QPS; costs ≈ the stored vector size in RAM |
| **Lowest RAM / many indexes / multi-process writers** | `BTree` + `Int8` | no RAM-resident layer beyond the btree page cache |
| **Small set (≲ tens of k) or exact 100% recall** | `BruteForce` | exact O(N) scan, zero index storage, fastest writes; ~7 µs/doc at dim 768 on desktop CPUs |
| **Delete/update-heavy** | any mode + `CompactRatio 0.5` | caps tombstone growth — see below |
| **Recall-tolerant / first-stage retrieval** | lower `Dim` (MRL 768→128) + `Int8` | ~2× faster, ~4× smaller; re-rank survivors at full dim |

### What each mode costs

- **`VectorModeBTree`** — the HNSW graph lives entirely in the btree; no
  RAM-resident layer (just the shared page cache). Multi-process-write safe.
  Baseline search latency.
- **`VectorModeHybrid`** — adds a RAM layer-0 mirror of the graph *adjacency*. Its
  RAM is **a few MiB and independent of the embedding dimension** — it scales with
  node count × layer-0 degree, not vector size — and is pointer-free (no GC
  pressure). Search is ≈ btree to slightly faster: the mirror alone is cheap
  insurance, not the big win.
- **`+ HybridCacheVectors`** — also caches the vectors in RAM (the "vector tier").
  **This is the search win** (sub-0.3 ms p50, several× the QPS), at the cost of
  roughly the stored vector size in RAM: `N × dim × 4` bytes (f32) or
  `N × (dim + 4)` (int8). The tier (and the mirror's tombstone set) grow with the
  label high-water mark, so heavy churn inflates RAM until a compaction — see
  `CompactRatio`.
- **`VectorModeBruteForce`** — no graph, no storage; every query is an exact full
  scan. Fastest writes, exact recall, O(N) search time with O(k) allocations:
  the scan reads each document's vector field lazily (no document parse),
  ranks in-scan, and keeps only the query window when no residual filter or
  explicit sort needs the full ranked set. Measured at 20k docs × 768 dims:
  ~136 ms/query, 137 allocs, 0.7 MB churn on a desktop Ryzen (~3× faster and
  ~99% less allocation than the v2.0.0-alpha.10 scan); ~250-500 ms on
  laptop/server-class cores. The per-query cost is linear in N — past a few
  tens of thousands of documents prefer an ANN mode.

Insert/update/delete throughput is **the same across all modes** — the hybrid
mirror is built lazily on the read path, so it adds no measurable write cost.

### Quantization

`VectorQuantInt8` stores a per-vector scale + int8 components: **~4× smaller** on
disk *and* in the vector tier, for **~0.5%** recall loss (f32 and int8 track each
other across `EfSearch`). Recommended for large or high-dimensional sets — it's the
default choice in the table above.

### CompactRatio — reclaiming tombstones

Deletes and replaces *tombstone* graph nodes (they still route, but never match),
so btree search cost — and the `HybridCacheVectors` tier RAM — grow with churn.
`CompactRatio` rebuilds the graph once tombstones reach that fraction of live nodes
(`0` = off). Measured under sustained update churn:

| CompactRatio | churn throughput | tombstones / search latency / tier RAM | use when |
|---|---|---|---|
| **`0.5`** (recommended) | ~80% of uncompacted | bounded | general default, moderate–heavy churn |
| **`0.25`** | ~60% of uncompacted | tightest cap (best latency, lowest RAM) | read-latency- or RAM-sensitive; best when deletes are light–moderate |
| **`< 0.25`** | collapses | marginal extra benefit | avoid |
| **`0` / large** | fastest | unbounded growth | write-mostly or short-lived indexes |

Auto-compaction is synchronous (a latency spike on the triggering write) and
O(live); for very large indexes prefer `0` and schedule
`Collection.CompactVectorIndex(ctx, name)` in a maintenance window.

### Other knobs

- **`EfSearch`** — query-time candidate list; higher = more recall, slower. ~64 is
  the knee (recall ~0.96 at ~1 ms for dim 768). Override per-query with `$ef`.
- **`EfConstruction` / `M`** — bigger graph, slower build, marginally better
  recall; raise for build-once / query-often.
- **Per-batch insert cache** — repeatedly-read graph vectors are served from a
  capped RAM arena during a write batch (RAM traded for insert/build speed). Sized
  per-index with `Index.SetInsertCacheSize(n)` or globally via the `ASV_VCACHE` env
  var (vectors; `0` disables); default 16384. It roughly **2–4×** batch-insert
  throughput; size it toward the working set for large bulk loads.
- **`Config.CacheSize`** — the btree page cache, in pages (default 5000 ≈ 20 MiB).
  This is the main page-level performance/RAM dial, and it matters **only for
  non-tiered modes** (`BTree`/`Hybrid` without `HybridCacheVectors`): search
  throughput climbs as the cache approaches the index size, then plateaus. Raising
  it toward the index size lifts disk-bound **f32** search materially (large f32
  indexes are 100s of MiB); `Int8` indexes are small and saturate the cache early.
  Tiered (`+HybridCacheVectors`) indexes serve search from the vector tier and are
  **cache-insensitive** — leave `CacheSize` at the default and spend RAM on the tier
  instead.
- **`Config.UseGlobalPageBuffer`** (+ `InitPageBuffer(pageSize, nPages)`) — serve
  page buffers from a pre-allocated fixed slab instead of the GC-managed `sync.Pool`.
  **Throughput-neutral**; its value is a bounded, GC-quiet page-cache footprint.
  Size the slab ≥ `CacheSize` (it falls back to `sync.Pool` on overflow).
- **`anystore.SetVectorBuildConcurrency(n)`** — caps the process-wide
  build/compaction worker budget (default `GOMAXPROCS`); see §1.
- **Document encoding** — store the embedding as `anyenc.TypeVectorF32` (not a
  number array): ~2× smaller docs and a faster pipeline, recall unchanged.
- **`:memory:`** keeps the whole DB resident — fastest, but many× the RAM of a
  file-backed DB.

## 7. End-to-end example

```go
ctx := context.Background()
db, _ := anystore.Open(ctx, ":memory:", nil)
defer db.Close()

coll, _ := db.CreateCollection(ctx, "docs")
coll.EnsureIndex(ctx, anystore.IndexInfo{
    Name:   "emb",
    Kind:   anystore.IndexKindVector,
    Vector: &anystore.VectorParams{Field: "embedding", Dim: 4, Metric: anystore.VectorCosine},
})

// Build documents with the packed anyenc vector type, then insert as one batch.
arena := &anyenc.Arena{}
mkDoc := func(id float64, lang string, vec []float32) *anyenc.Value {
    obj := arena.NewObject()
    obj.Set("id", arena.NewNumberFloat64(id))
    obj.Set("lang", arena.NewString(lang))
    obj.Set("embedding", arena.NewVectorF32(vec))
    return obj
}
coll.Insert(ctx,
    mkDoc(1, "en", []float32{0.9, 0.1, 0.0, 0.2}),
    mkDoc(2, "en", []float32{0.8, 0.2, 0.1, 0.1}),
    mkDoc(3, "fr", []float32{0.0, 0.1, 0.9, 0.7}),
)

iter, _ := coll.Find(`{"embedding":{"$knn":{"$query":[0.85,0.15,0.05,0.15],"$k":2}}, "lang":"en"}`).
    Sort("_distance").Iter(ctx)
defer iter.Close()

for iter.Next() {
    doc, _ := iter.Doc()
    fmt.Printf("dist=%.4f doc=%s\n", iter.Distance(), doc.Value().String())
}
```
