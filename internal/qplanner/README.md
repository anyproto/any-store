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

`$text` and `$knn` queries go through their own cost-based selection
(`text_plan.go`, `knn_plan.go`) — see **Full-text and vector plans** below.

---

### Cost Model

Constants defined in `cost.go`:

| Constant | Value | Description |
|---|---|---|
| `CostIndexSeek` | 0.5 | Cost of a B-tree traversal to find a key (per bound/seek) |
| `CostDocFetch` | 3.0 | Cost of a random point lookup in the data B-tree (index → data) |
| `CostSeqRead` | 0.1 | Cost of a sequential cursor read per doc (full scan) |
| `CostFilter` | 0.5 | Cost of in-memory predicate evaluation |
| `CostSortSwap` | 0.25 | Cost of an in-memory sort swap (includes re-fetch overhead) |
| `DefaultRangeSelectivity` | 0.5 | Default fraction for range queries |

#### Selectivity Estimation

- **Equality** (`a = 1`): `P = sketch.Estimate(encodedValue) / TotalDocs`
- **Range** (`a > 5`): `P = DefaultRangeSelectivity`
- **Combined** (AND): `P_total = P(a) * P(b)` (independence assumption)

#### Plan Cost Formulas

**Plan A: Full Collection Scan**
```
Cost = (EffectiveDocs × CostSeqRead) + (EffectiveDocs × CostFilter) + sortCost(EstimatedYield)
```
When idBounds are present (point lookups on the primary key), `EffectiveDocs` is replaced with `len(idBounds)`.

**primary-key-sort optimization**: Sorting by the primary key (asc or desc) is free — FullScan naturally reads in primary-key order. When the sort is primary-key-only, `fullScanNeedSort` is false and no SortIter is created. The `Reverse` flag on FullScanIter handles descending order. This works with or without a filter. (The primary key is the collection's configured key field, default `id`.)

**Limit-aware FullScan**: When the plan is FullScan with primary-key-sort (no sort needed) and a LIMIT is present:
```
EffectiveDocs = min(TotalDocs, (LIMIT + OFFSET) / selectivity)
```
This accounts for early termination — e.g. `Find({"a":5}).Sort("id").Limit(10)` with 1% selectivity only needs to scan ~1000 docs, not the full collection.

**Plan B: Index Seek (Filtering Priority)**
```
E = sketch.Estimate(value)  // estimated matching docs
Cost = nSeeks × CostIndexSeek + (E × CostDocFetch) + (E × CostFilter) + sortCost(FilteredYield)
```
Sort cost is zero if the index covers the sort order.

**Limit-aware IndexSeek**: When the index covers the sort order (`ExactSort`) and a LIMIT is present:
```
scanSel = P_total / idxSel   // fraction of index-matched docs that pass remaining filters
S = min(E, (LIMIT + OFFSET) / scanSel)
Cost = nSeeks × CostIndexSeek + (S × CostDocFetch) + (S × CostFilter)
```
This avoids charging for fetching all E docs when only a few are needed.

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
| `FullScanIter` | Scans the data namespace sequentially. Optionally filters inline. Supports `Reverse` for descending id-order. |
| `IndexIter` | Scans an index namespace within bounds. Returns `(indexKey, docId)` pairs. |
| `CoverIter` | Point-lookups on a **unique** index. Each bound produces at most one result. |

#### Transform Iterators

| Iterator | Description |
|---|---|
| `FetchIter` | Wraps an index iterator, fetches full documents by docId from data namespace. |
| `FilterIter` | Evaluates a `query.Filter` predicate on fetched documents. |
| `SortIter` | Collects upstream results and sorts in memory. When `TopK > 0`, uses a max-heap of size K to keep only the smallest K entries — O(N log K) instead of O(N log N), with O(K) entry memory instead of O(N). |
| `LimitIter` | Skips `Offset` results, returns at most `Limit` results. |
| `CanonicalKeyDedupIter` | Removes duplicate hits from single-field multi-key indexes by emitting each doc only at its canonical (min/max) in-bound array value. O(1) memory, streaming. |
| `SeenSetDedupIter` | Removes duplicate docId hits via a hash-set. O(distinct_results) memory. Used for compound multi-key indexes where canonical-key selection across compound tuples is non-trivial. |

#### Multi-Key Index Deduplication

Array-valued (multi-key) indexes store one entry per array element per document.
A `$in` or range query that matches multiple elements of the same document
therefore yields multiple `(key, docId)` hits for that document. Without dedup,
`Iter()` would surface duplicates and `Count()` would inflate.

The planner picks a dedup strategy by index shape:

| Index shape | Iterator | Memory | Notes |
|---|---|---|---|
| Single-field (`Fields[]` length 1) | `CanonicalKeyDedupIter` | O(1), streaming | Canonical = min/max of doc's in-bound array values. Reverse scan uses max. |
| Compound (`Fields[]` length ≥ 2) | `SeenSetDedupIter` | O(distinct results) | `map[string]struct{}` of emitted docIds. |
| Scalar-only fields | no wrap | O(0) | No duplicates possible. |

Dedup is wired regardless of bound presence — a bound-less index scan
(pure `Sort("tags")` with no filter) still dupes on multi-key fields.
`CanonicalKeyDedupIter` treats empty bounds as "every element qualifies."

For `Count`, the covering-index fast path (direct entry count on the btree
cursor) is disabled whenever `len(idx.Bounds) > 1`, which is the universal
trigger for multi-key duplicate hits under point-lookup bounds.
Single-bound point counts remain safe because within-doc array dedup in
`index.go:insertKeys` guarantees at most one index entry per distinct value
per doc.

#### TopK Sort Optimization

When a query has both Sort and Limit (e.g. `.Sort("name").Limit(10).Offset(5)`), SortIter receives `TopK = Limit + Offset = 15`. Instead of collecting all N entries and sorting them:

1. Fill a max-heap with the first K entries
2. For each remaining entry: if smaller than heap max, replace and sift down; otherwise discard
3. After scanning all N docs, sort only the K heap entries

This reduces the final sort from O(N log N) to O(K log K) and keeps the entries slice at O(K) instead of O(N).

#### Plan Chains

**Full Scan**: `FullScanIter(filter) → [SortIter] → [LimitIter]`

**Full Scan (primary-key-sort)**: `FullScanIter(filter, reverse?) → [LimitIter]` — no SortIter needed

**Index Seek (unique point lookup)**: `CoverIter → [FilterIter] → [SortIter] → [LimitIter]`

**Index Seek (general)**: `IndexIter → FetchIter → [FilterIter] → [DedupIter] → [SortIter] → [LimitIter]`

**Index Scan (sort covered)**: `IndexIter → FetchIter → [FilterIter] → [DedupIter] → [LimitIter]`

`DedupIter` is one of `CanonicalKeyDedupIter` (single-field) or `SeenSetDedupIter` (compound); see the Multi-Key Index Deduplication section above.

---

### Full-text and vector plans

A `$text` predicate has two enforcement forms, chosen by cost (`text_plan.go`):

| form | chain | cost shape |
|---|---|---|
| driver | `FtsIter → Fetch → Filter → …` | `Σ df × CostFtsPosting` + consumed fetches |
| probe | `IdBounds/IndexIter → FtsProbeIter → Fetch → ScoreInject → Filter → …` | candidates × `(CostFtsProbeDoc + terms × CostFtsProbeTerm)` |

The driver walks the posting lists (cost is exact: per-term document
frequencies are read from the index at plan time) and additionally gates
candidates against the query's primary-key bounds before the fetch. The probe
verifies each candidate of another access path against the text index
individually. Both produce bit-identical matches and scores; when relevance
order is observable, `FtsProbeIter` runs in rank mode and re-emits matches in
the driver's `(score desc, IntDocID asc)` order. A `Count` whose residual is
covered by the probe's driver fetches no documents.

A `$knn` clause means "the K nearest among filter-passing documents". The ANN
driver (`VectorIter`) approximates this by post-filtering ef candidates; the
probe form (`knn_plan.go`, `VectorScoreIter`) enumerates the filter candidates
from a bounded access path, scores each stored vector directly, and keeps the
exact K nearest survivors — under a selective filter it is both cheaper and
more accurate, and the cost model switches automatically.

Index hints force plans in both selectors: the fts/vector index name boosts
the driver, the primary-key field name the pk probe, a secondary index name
its probe.

### IndexSketch

The `IndexSketch` (`sketch.go`) is a frequency sketch using `xxhash/v2` for cardinality estimation:

- **Buckets**: `[]uint64` of configurable size (default 1024)
- **Hashing**: `bucket = xxhash.Sum64(encodedValue) % size`
- **API**: `Increment`, `Decrement`, `Estimate`, `MarshalBinary`, `UnmarshalBinary`, `Reset`
- **Storage**: Persisted under `stat_data:<coll>:<index>` in the `_system` namespace
- **Multi-process sync**: Reloaded when `IsDataStale()` detects changes from another process
