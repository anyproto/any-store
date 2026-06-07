# Vector search

any-store has a built-in vector (embedding) index for approximate nearest-neighbour
(ANN) search, implemented as a btree-resident HNSW graph. It integrates with the
normal `Find()` query pipeline: a vector clause selects candidates, ordinary
filters and sorting apply on top, and each result carries its distance to the
query.

- **Storage-resident** — the graph lives in the btree, so it's crash-safe,
  MVCC-consistent across readers, and multiprocess-safe for writes. No separate
  in-memory index to build or invalidate.
- **Queried through `Find()`** — there is no separate search call; you express the
  query as `Find({ <vectorField>: [...] })` and combine it with any other filter,
  sort, limit, and offset.

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
Creating the index on a non-empty collection builds it from existing documents.

Only `Field` and `Dim` are required. Other `VectorParams`:

| Field | Default | Purpose |
|---|---|---|
| `Metric` | `VectorCosine` | distance measure: `VectorCosine`, `VectorL2`, `VectorDot` |
| `M`, `EfConstruction`, `EfSearch` | sensible defaults (0) | HNSW graph/search tuning |
| `Quantization` | `VectorQuantNone` | `VectorQuantInt8` ≈ 4× smaller, RAM-resident vectors |
| `Mode` | `VectorModeBTree` | `VectorModeHybrid` (RAM layer-0 cache) or `VectorModeBruteForce` (exact O(N) scan, no graph) |
| `HybridCacheVectors` | `false` | hybrid only: also cache vectors in RAM for faster search |
| `CompactRatio` | `0` (off) | auto-compact the graph when tombstones reach this ratio of live nodes |

## 2. Document shape

The embedding is an ordinary field — a JSON/anyenc array of exactly `Dim` numbers.
Nested paths work (`Field: "meta.vec"`). Documents missing the field, or whose
value isn't a `Dim`-sized numeric array, are simply not indexed.

```go
coll.Insert(ctx, anyenc.MustParseJson(
    `{"id":1, "title":"cat", "lang":"en", "embedding":[0.12, -0.03, ...]}`))
```

Inserts are batched: pass many documents to one `Insert(...)` call so they share a
single transaction.

## 3. Query

A query becomes a vector search when a top-level clause is an equality between a
**vector-indexed field** and a `Dim`-sized array:

```go
// k nearest neighbours, closest first (the default order).
iter, err := coll.Find(`{"embedding":[0.1, 0.2, ...]}`).Limit(10).Iter(ctx)
```

Everything else in the filter is applied as a normal predicate on the candidates:

```go
// ANN + ordinary filters.
coll.Find(`{"embedding":[...], "lang":"en", "year":{"$gt":2020}}`).Limit(10).Iter(ctx)
```

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

// Filter by a distance threshold:
coll.Find(`{"embedding":[...], "_distance":{"$lt":0.35}}`).Iter(ctx)

// Sort by distance (ascending is the default when no Sort is given):
coll.Find(`{"embedding":[...]}`).Sort("_distance").Iter(ctx)        // closest first
coll.Find(`{"embedding":[...]}`).Sort("-_distance").Iter(ctx)       // farthest first

// Multi-key sort: distance, then a tie-breaker field:
coll.Find(`{"a":{"$gt":42}, "embedding":[...]}`).Sort("_distance", "-name").Iter(ctx)
```

`Iterator.Distance()` returns the row's distance (0 for non-vector queries).

### Limit, offset, and candidate set size

`Limit`/`Offset` apply over the distance-ordered results. When a residual filter is
selective, the planner over-fetches ANN candidates so the limit still fills.
`VectorEf(n)` overrides the candidate-list size (recall/speed trade-off):

```go
coll.Find(`{"embedding":[...], "lang":"en"}`).Limit(20).VectorEf(200).Iter(ctx)
```

## 4. Errors for malformed queries

Mistakes are reported rather than silently returning wrong results:

| Query | Error |
|---|---|
| Wrong-dimension array on a vector field — `{"embedding":[1,2,3]}` (dim 768) | `ErrInvalidVectorQuery` |
| Non-array / non-equality on a vector field — `{"embedding":"x"}`, `{"embedding":{"$gt":[...]}}` | `ErrInvalidVectorQuery` |
| Two vector clauses in one query | `ErrMultipleVectorClauses` |
| `_distance` in a filter or sort with no vector clause | `ErrDistanceWithoutVector` |

## 5. Updates, deletes, compaction

Updates and deletes are automatic: re-inserting a document id reindexes its vector;
deleting removes it from results. Deletes and replaces *tombstone* graph nodes
rather than removing them, so storage and search cost slowly grow with churn.
Reclaim them with `CompactRatio` (automatic) or `Collection.CompactVectorIndex(ctx,
name)` (manual, e.g. in a maintenance window). See `VectorParams.CompactRatio` for
sizing guidance.

## 6. Tuning notes

- **`Quantization: VectorQuantInt8`** — ~4× smaller vectors, RAM-resident; minor
  recall cost. Good default for large or high-dimensional sets.
- **`Mode: VectorModeHybrid` (+ `HybridCacheVectors`)** — keeps a RAM layer-0
  mirror (and optionally the vectors) for faster search; the btree stays the source
  of truth.
- **`Mode: VectorModeBruteForce`** — exact O(N) scan, zero index storage; fine for
  small collections or 100%-recall needs.
- **Write throughput** — inserts use an internal per-batch vector cache; it's
  automatic and bounded (configurable on the low-level `vindex.Index` via
  `SetInsertCacheSize`, or globally with the `ASV_VCACHE` env var, in vectors).

### Tradeoffs at a glance

Each setting trades among **write** speed, **read** (search) speed, **RAM**, and
**disk**. Defaults are a balanced starting point; tune one axis at a time.

| Setting | Write | Read | RAM | Disk | When to use |
|---|---|---|---|---|---|
| `Quantization: Int8` | ~same | ~same / slightly faster | lower | **~4× smaller** | almost always — recall cost is ~0.5% |
| `Mode: Hybrid` | same | a bit faster | +graph mirror | same | cheap latency win |
| `+ HybridCacheVectors` | same | **fastest** (vectors in RAM) | **+N·dim·4** | same | read-heavy, RAM to spare |
| `Mode: BruteForce` | **fastest** (no graph) | **slowest** (O(N) scan) | lowest | none | small sets, or exact recall |
| `EfSearch` ↑ | — | slower, higher recall | — | — | need recall; `~64` is the knee |
| `EfConstruction`/`M` ↑ | slower build | slightly faster/higher recall | — | larger graph | build-once, query-often |
| `ASV_VCACHE` ↑ | **faster bulk build** | — | +cache (≈ working set·dim·4) | — | large bulk loads |
| dim ↓ (e.g. 768→128) | faster | faster | lower | smaller | recall-tolerant / first-stage retrieval |
| store field as `anyenc` vector type | faster | **faster pipeline** | — | **~2× smaller docs** | embeddings stored in the document |

Rules of thumb: **int8 + dim you can tolerate** for size; **Hybrid+CacheVectors**
when reads dominate and RAM allows; **raise `ASV_VCACHE`** for one-off bulk
builds; **brute-force** only for small collections. In-memory (`:memory:`) keeps
the whole DB resident — far faster but uses many× the RAM of a file-backed DB.

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

coll.Insert(ctx,
    anyenc.MustParseJson(`{"id":1,"lang":"en","embedding":[0.9,0.1,0.0,0.2]}`),
    anyenc.MustParseJson(`{"id":2,"lang":"en","embedding":[0.8,0.2,0.1,0.1]}`),
    anyenc.MustParseJson(`{"id":3,"lang":"fr","embedding":[0.0,0.1,0.9,0.7]}`),
)

iter, _ := coll.Find(`{"embedding":[0.85,0.15,0.05,0.15], "lang":"en"}`).
    Sort("_distance").Limit(2).Iter(ctx)
defer iter.Close()

for iter.Next() {
    doc, _ := iter.Doc()
    fmt.Printf("dist=%.4f doc=%s\n", iter.Distance(), doc.Value().String())
}
```
