### Query Planner (`qplanner`)

The query planner builds an **iterator chain** that executes queries efficiently using a **Cost-Based Optimizer (CBO)** driven by frequency sketches for cardinality estimation.

---

### Architecture Overview

```
Query → BuildPlan (CBO) → Iterator Chain → Results
```

1. **Cost Estimation**: For each candidate index, estimate the number of matching documents using `IndexSketch` (xxhash-based frequency sketch) and compute the total query cost.
2. **Plan Selection**: Generate three candidate plans (FullScan, IndexSeek, IndexScan) and pick the one with the lowest cost.
3. **Execution**: The root iterator is called repeatedly via `Next()` to produce `(key, docId)` pairs.

---

### Cost Model

Constants defined in `cost.go`:

| Constant | Value | Description |
|---|---|---|
| `CostIndexSeek` | 0.0 | Cost of a B-tree traversal (negligible for embedded DB) |
| `CostDocFetch` | 2.0 | Cost of a point lookup in the data B-tree |
| `CostFilter` | 0.5 | Cost of in-memory predicate evaluation |
| `CostSortSwap` | 0.5 | Cost of an in-memory sort swap |
| `DefaultRangeSelectivity` | 0.5 | Default fraction for range queries |

#### Selectivity Estimation

- **Equality** (`a = 1`): `P = sketch.Estimate(encodedValue) / TotalDocs`
- **Range** (`a > 5`): `P = DefaultRangeSelectivity`
- **Combined** (AND): `P_total = P(a) * P(b)` (independence assumption)

#### Plan Cost Formulas

**Plan A: Full Collection Scan**
```
Cost = (TotalDocs × CostDocFetch) + (TotalDocs × CostFilter) + sortCost(EstimatedYield)
```
When idBounds are present (point lookups on `id`), `TotalDocs` is replaced with `len(idBounds)`.

**Plan B: Index Seek (Filtering Priority)**
```
E = sketch.Estimate(value)  // estimated matching docs
Cost = CostIndexSeek + (E × CostDocFetch) + (E × CostFilter) + sortCost(FilteredYield)
```
Sort cost is zero if the index covers the sort order.

**Plan C: Index Scan (Sorting Priority)**
```
With LIMIT: S = min((LIMIT + OFFSET) / P_total, TotalDocs)
Without LIMIT: S = TotalDocs
Cost = (S × CostIndexSeek) + (S × CostDocFetch) + (S × CostFilter)
```
No sort cost since the index provides order.

---

### Iterator Types

Iterators form a composable pipeline:

```
Source (leaf) → Transform → Transform → ... → Root
```

#### Leaf Iterators

| Iterator | Description |
|---|---|
| `FullScanIter` | Scans the data namespace sequentially. Optionally filters inline. |
| `IndexIter` | Scans an index namespace within bounds. Returns `(indexKey, docId)` pairs. |
| `CoverIter` | Point-lookups on a **unique** index. Each bound produces at most one result. |

#### Transform Iterators

| Iterator | Description |
|---|---|
| `FetchIter` | Wraps an index iterator, fetches full documents by docId from data namespace. |
| `FilterIter` | Evaluates a `query.Filter` predicate on fetched documents. |
| `SortIter` | Buffers all upstream results, sorts them in memory. |
| `LimitIter` | Skips `Offset` results, returns at most `Limit` results. |

#### Plan Chains

**Full Scan**: `FullScanIter → [SortIter] → [LimitIter]`

**Index Seek (unique point lookup)**: `CoverIter → [FilterIter] → [SortIter] → [LimitIter]`

**Index Seek (general)**: `IndexIter → FetchIter → [FilterIter] → [SortIter] → [LimitIter]`

**Index Scan (sort covered)**: `IndexIter → FetchIter → [FilterIter] → [LimitIter]`

---

### IndexSketch

The `IndexSketch` (`sketch.go`) is a frequency sketch using `xxhash/v2` for cardinality estimation:

- **Buckets**: `[]uint64` of configurable size (default 1024)
- **Hashing**: `bucket = xxhash.Sum64(encodedValue) % size`
- **API**: `Increment`, `Decrement`, `Estimate`, `MarshalBinary`, `UnmarshalBinary`, `Reset`
- **Storage**: Persisted under `stat_data:<coll>:<index>` in the `_system` namespace
- **Multi-process sync**: Reloaded when `IsDataStale()` detects changes from another process
