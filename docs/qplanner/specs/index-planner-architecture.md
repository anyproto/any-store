# Index & Query Planner Architecture

## Overview

Our system uses a **document-oriented** index & query planner on top of a B-tree key-value store (Go port of SQLite's btree.c). This is fundamentally different from SQLite's SQL-based index system:

- **SQLite**: SQL → virtual machine (VDBE) → btree cursors. Indexes are B-trees keyed by column values pointing to rowids.
- **Ours**: JSON filter/sort → weight-based planner → iterator chain → btree cursors. Indexes are btree namespaces keyed by encoded field tuples.

---

## Data Model

### Documents
JSON documents stored in a **data namespace** (btree namespace). Each doc has an `id` field (the primary key). Keys are encoded document IDs, values are the full JSON document.

### Collections
A `Collection` wraps a data namespace and zero or more index namespaces. Collections provide:
```go
Collection.Find(filter any) Query           // returns query builder
Collection.Insert(ctx, docs ...*anyenc.Value) error
Collection.UpdateOne(ctx, doc *anyenc.Value) error
Collection.DeleteId(ctx, id any) error
Collection.EnsureIndex(ctx, info ...IndexInfo) error
Collection.DropIndex(ctx, indexName string) error
Collection.GetIndexes() []Index
```

### Index Definition
```go
type IndexInfo struct {
    Name   string   // Auto-generated as "field1,field2" if empty
    Fields []string // e.g., ["name", "-createdDate"] (- prefix = descending)
    Unique bool     // Enforces unique constraint
    Sparse bool     // Skips documents missing the indexed field
}
```

---

## Index Storage

Indexes live in separate btree namespaces named `"ix:{collectionName}:{indexName}"`.

### Unique Indexes
- **Key** = `Tuple(value1, value2, ...)` (encoded field values)
- **Value** = encoded document ID
- Duplicate key with different docId → `ErrUniqueConstraint`

### Non-Unique Indexes
- **Key** = `Tuple(value1, value2, ..., docId)` (field values + doc ID suffix)
- **Value** = `nil`
- The docId suffix makes every key unique in the btree

### Encoding
Uses `anyenc.Tuple` — a binary format where values are encoded via `v.MarshalTo()`. This preserves comparison order for `bytes.Compare()`.

**Reverse fields** use inverted encoding — a forward btree scan of inverted keys yields values in descending original order.

### Array Fields
When a field contains an array, **multiple index entries** are created (cartesian product):
```
doc: {id:1, a:[1,2], b:3}
index(a,b) entries: key=(1,3), key=(2,3), key=([1,2],3)
```
Array elements are deduplicated within each field position.

### Sparse Indexes
Documents missing the indexed field (or with `null` value) produce **no index entries**.

---

## Query Planner

### Pipeline
```
Query → ComputeWeights() → BuildPlan() → Iterator Chain → Results
```

### Weight Computation (`weight.go`)

Each index gets: `weight = queryWeight + sortWeight + hints`

**Query Weight** — chain model for compound indexes (must match left-to-right):
| Scenario | Weight |
|---|---|
| First field has bounds | 10 |
| Next field continues chain | weight × 2 |
| Chain breaks (field has no bounds) | weight − 1 |
| Field after break has bounds | weight + 2 |
| All chain matched + unique | weight + 1 |

Example for index `(a, b, c)`:
- Filter `{a:1, b:2, c:3}` → 10 → 20 → 40
- Filter `{a:1, c:3}` → 10, break at b → 9, c non-chain → 11
- Filter `{b:2}` → break at a → −1, b non-chain → 1

**Sort Weight** — similar chain model:
| Scenario | Weight |
|---|---|
| First sort field matches index | 11 |
| Next field matches | weight × 2 |
| Direction matches | weight + 2 |
| After break exists in index | weight + 5 |

**ExactSort**: All sort fields matched → no in-memory sort needed.
**PartialSort**: Leading fields match → data partially pre-sorted.

### Index Selection
- Indexes sorted by weight (descending)
- Up to `MaxIndexes` (default 2) selected
- Index is "used" if it covers new query or sort fields
- `IndexHints` add `Boost` to weight for manual tuning

---

## Iterator Types

### Leaf Iterators (data sources)

| Iterator | Description | Cost |
|---|---|---|
| `FullScanIter` | Scans entire data namespace. Optional inline filter. | O(N) |
| `IndexIter` | Scans index namespace within bounds. Forward or reverse. | O(M) matching entries |
| `CoverIter` | Point lookups on unique index for fixed-point bounds. | O(B) bounds — fastest path |

### Transform Iterators (mid-chain)

| Iterator | Description | Cost |
|---|---|---|
| `FilterIter` | Fetches full doc, applies filter predicate. Caches in `Plan.DocValue`. | O(M) doc fetches |
| `CoverFilterIter` | Secondary index filtering. Fetches doc, extracts field, checks bounds. | O(M) |
| `SortIter` | Collects all upstream results, sorts in memory. | O(M log M), blocking |
| `LimitIter` | Skip offset, stop after limit. | O(1) per call |

### Iterator Chain Patterns

**Best case — Unique cover lookup:**
```
CoverIter → FilterIter → LimitIter
```

**Index scan with sort:**
```
IndexIter(bounds, reverse?) → CoverFilterIter → FilterIter → LimitIter
```

**Full scan fallback:**
```
FullScanIter(filtered) → SortIter → LimitIter
```

**ID sort optimization:**
```
FullScanIter(reverse?) → LimitIter
```

---

## Query API

```go
Query interface {
    Limit(limit uint) Query
    Offset(offset uint) Query
    Sort(sort ...any) Query           // e.g., "a", "-b" for asc/desc
    IndexHint(hints ...IndexHint) Query
    Iter(ctx context.Context) (Iterator, error)
    Count(ctx context.Context) (int, error)
    Update(ctx, modifier any) (ModifyResult, error)
    Delete(ctx) (ModifyResult, error)
    Explain(ctx) (Explain, error)
}
```

### Filter Operators
```
$eq, $gt, $gte, $lt, $lte, $ne    // comparison
$in                                  // set membership
$or, $and, $not                      // logical
$exists                              // field existence
$regex                               // regex match
```

### Explain Output
```go
type Explain struct {
    Sql           string           // Human-readable plan description
    SqliteExplain []string         // EXPLAIN-like output
    Indexes       []IndexExplain   // Index details with weights
}
```

---

## Known Issues

1. **Reverse scan direction inverted** (`planner.go:94-101`): The `!=` check should likely be `==`.
2. **Range selectivity not considered**: Wide ranges (>20% selectivity) can be slower with index.
3. **CoverFilterIter overhead**: Secondary index may add cost without reducing result set.
4. **FilterFullyCovered** bitmap check was inverted (FIXED).

---

## Error Sentinels

```go
ErrUniqueConstraint  // duplicate key in unique index
ErrCollectionNotFound
ErrDocumentNotFound
```
