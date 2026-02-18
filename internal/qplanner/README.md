### Query Planner (`qplanner`)

The query planner builds an **iterator chain** that executes queries efficiently by choosing which indexes to use, how to scan them, and how to combine filtering, sorting, and pagination.

---

### Architecture Overview

```
Query → ComputeWeights → BuildPlan → Iterator Chain → Results
```

1. **Weight Computation** (`weight.go`): Scores each index based on how well it matches the query filter and sort.
2. **Plan Building** (`planner.go`): Selects the best indexes and constructs an iterator chain.
3. **Execution**: The root iterator is called repeatedly via `Next()` to produce `(key, docId)` pairs.

---

### Iterator Types and Nesting

Iterators form a pipeline. Each iterator wraps a **source** iterator and adds behavior:

```
Source (leaf) → Filter/Transform → Filter/Transform → ... → Root (returned to caller)
```

#### Leaf Iterators (data sources)

| Iterator | Description | Cost |
|---|---|---|
| `FullScanIter` | Scans the entire data namespace sequentially. Optionally applies a filter inline. | **O(N)** — reads every document. Cheap per-doc since no index indirection. |
| `IndexIter` | Scans an index namespace within given bounds. Returns `(indexKey, docId)` pairs. Supports forward and reverse scanning. | **O(M)** where M = matching index entries. Very cheap per-entry (no doc fetch). |
| `CoverIter` | Point-lookups on a **unique** index for fixed-point bounds. Each bound produces at most one result. | **O(B)** where B = number of bounds. The fastest path for unique key lookups. |

#### Transform Iterators (mid-chain)

| Iterator | Description | Cost |
|---|---|---|
| `FilterIter` | Fetches the full document for each `docId` from the data namespace and applies a `query.Filter` predicate. Caches the fetched value in `Plan.DocValue` to avoid double-fetch. | **O(M)** doc fetches. Dominant cost when M is large. |
| `CoverFilterIter` | Uses a secondary index to narrow results without a full data scan. Fetches the doc, extracts index field values, and checks bounds. | **O(M)** — similar to FilterIter but checks index bounds instead of full filter. |
| `SortIter` | Collects **all** upstream results into memory, then sorts them using `query.Sort`. If `PreSorted` is true (partial sort from index), uses this as a hint. | **O(M log M)** — must buffer all results. Blocks until first result is ready. |
| `LimitIter` | Skips `Offset` results, then returns at most `Limit` results. | **O(1)** per call after offset is consumed. |

#### Nesting Patterns

The planner constructs one of these chains depending on available indexes:

**Best case — Unique index cover lookup:**
```
CoverIter → FilterIter → LimitIter
```
Cost: O(1) per bound. No scanning at all.

**Index scan with sort coverage:**
```
IndexIter(bounds, reverse?) → CoverFilterIter(secondary) → FilterIter → LimitIter
```
Cost: O(M) where M = matching entries. No in-memory sort needed.

**Index scan without sort coverage:**
```
IndexIter(bounds) → FilterIter → SortIter → LimitIter
```
Cost: O(M) scan + O(M log M) sort. Must buffer all filtered results.

**Sort-only index (no filter bounds):**
```
IndexIter(full scan, reverse?) → FilterIter → LimitIter
```
Cost: O(N) index entries but avoids in-memory sort.

**Full scan fallback:**
```
FullScanIter(filtered) → SortIter → LimitIter
```
Cost: O(N) scan + O(R log R) sort where R = matching results.

**ID sort optimization:**
```
FullScanIter(reverse?) → LimitIter
```
When sorting by `id` only, the data namespace natural order is used.

---

### Weight Computation

Each index gets a **weight** = `queryWeight + sortWeight`. Higher weight = better index.

#### Query Weight (`indexQueryWeight`)

Uses a **chain model** — compound index fields must match left-to-right:

| Scenario | Weight |
|---|---|
| First field has bounds | `10` |
| Next field in chain has bounds | `weight * 2` |
| Chain breaks (field has no bounds) | `weight - 1` |
| Field after chain break has bounds | `weight + 2` |
| All chain fields matched + unique index | `weight + 1` |

Examples for index `(a, b, c)`:
- Filter `{a:1, b:2, c:3}` → 10 → 20 → 40
- Filter `{a:1, b:2}` → 10 → 20, chain break → 19
- Filter `{a:1, c:3}` → 10, chain break at b → 9, c non-chain → 11
- Filter `{b:2}` → chain break at a → -1, b non-chain → 1

#### Sort Weight (`indexSortWeight`)

Also chain-based, matching sort fields to index fields in order:

| Scenario | Weight |
|---|---|
| First sort field matches index | `11` |
| Next field matches | `weight * 2` |
| Direction matches (`idx.Reverse[i] == sort.Reverse`) | `weight + 2` |
| Field after chain break exists in index | `weight + 5` |

**ExactSort**: All sort fields matched in chain → no in-memory sort needed.
**PartialSort**: Some leading fields match → data is partially pre-sorted.

#### Index Selection

- Indexes are sorted by weight (descending).
- Up to `MaxIndexes` (default 2) are selected.
- An index is "used" if it covers new query fields or sort fields not yet covered.
- `IndexHints` add a `Boost` value to the weight for manual tuning.

---

### Benchmark Results (10,000 documents)

| Operation | FullScan | With Index | Speedup |
|---|---|---|---|
| **Equality filter** (1% match) | 1,775 μs | 36 μs | **49x** |
| **Compound equality** (a=50,b=25) | — | 2.3 μs | — |
| **Range filter** (21% match) | 1,847 μs | 2,116 μs | **0.87x** ⚠️ |
| **Sort all** | 10,483 μs | 3,387 μs | **3.1x** |
| **Sort + Limit 10** | 7,652 μs | 4.4 μs | **1,740x** |
| **Filter + Sort** (21% match) | 3,592 μs | 2,195 μs | **1.6x** |
| **Unique lookup** | 1,690 μs | 1.9 μs | **888x** |
| **Low selectivity** (10% match) | 1,808 μs | 338 μs | **5.3x** |

#### Key Findings

1. **Sort + Limit is the biggest win** (1,740x). The index scan with limit avoids both the full scan and the in-memory sort — it just reads the first 10 entries.

2. **Unique index lookups are extremely fast** (888x). `CoverIter` does a direct point lookup.

3. **Wide range filters can be SLOWER with an index** (0.87x). For a 21% selectivity range (`$gte:40, $lte:60`), the index scan overhead (seek per entry + doc fetch) exceeds a simple sequential scan. The planner should consider selectivity when choosing between index scan and fullscan.

4. **Two-index CoverFilter adds marginal overhead** (40μs vs 38μs for single index). The benefit only appears when the secondary index significantly reduces the candidate set.

5. **Compound index equality is the fastest filter** (2.3μs). Both fields narrow the bounds so precisely that very few entries are scanned.

---

### Known Issues

#### 1. `FilterFullyCovered` bitmap check was inverted (FIXED)

**File**: `weight.go`, line 151

The original code used `allQueryFieldsBits.Subtract(chainFieldsBits)` which checks if chain bits are a subset of query bits (always true). The correct check is `chainFieldsBits.Subtract(allQueryFieldsBits)` — are there query field bits NOT covered by the chain?

**Impact**: `FilterFullyCovered` was always `true` when `chainLen > 0`, causing the planner to potentially skip necessary post-filtering. Fixed in this PR.

#### 2. Reverse index scan direction is inverted

**File**: `planner.go`, lines 94-101

The planner decides scan direction with:
```go
if fields[0].Reverse != primaryIdx.Info.Reverse[0] {
    reverse = true
}
```

But in practice, a reverse index (`-a`) stores inverted keys such that a **forward** btree scan yields values in **ascending** original order. So:
- `Sort("-a")` with index `"-a"`: both Reverse=true → `reverse=false` → forward scan → **ascending** (wrong, should be descending)
- `Sort("a")` with index `"-a"`: Reverse differs → `reverse=true` → reverse scan → **descending** (wrong, should be ascending)

**Impact**: Queries using a reverse index for sorting return results in the wrong order. The condition should likely be `==` instead of `!=`, or the inverted key encoding semantics should be re-examined.

#### 3. Range filter selectivity not considered

The planner always prefers an index with matching bounds over a fullscan, even when selectivity is low (wide range). As benchmarks show, a range filter matching 21% of documents is slower with an index. The planner could estimate selectivity from bound width and fall back to fullscan for wide ranges.

#### 4. CoverFilterIter overhead for equal-weight indexes

When two single-field indexes have equal weight, one becomes the primary scan and the other a `CoverFilterIter`. This adds a doc fetch + field extraction + bounds check per candidate, which may not reduce the result set enough to justify the cost.

---

### Improvement Suggestions

1. **Selectivity-aware planning**: For range queries, estimate the fraction of the index that will be scanned. If > ~15-20%, prefer fullscan.

2. **Fix reverse scan direction**: The `!=` check in `planner.go` should be flipped to `==` (or the encoding semantics clarified).

3. **Sort + Limit early termination**: Currently works well. Could be further optimized by pushing limit awareness into `IndexIter` to stop seeking after limit is reached.

4. **Cost-based model**: Replace the weight heuristic with a cost model that estimates I/O operations: `cost = seekCost * numSeeks + scanCost * numScans + fetchCost * numFetches + sortCost * numResults * log(numResults)`.

5. **Statistics collection**: Maintain per-index cardinality estimates to improve selectivity predictions.
