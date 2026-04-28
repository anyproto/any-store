# Index & Query Planner Test Porting — Agent Instructions

## Overview

We are creating comprehensive tests for the index and query planner system in `any-store-custom-btree`.
Our index/planner is **our own design** — it is NOT a port of SQLite's query planner.
However, SQLite's index test suite covers many **edge cases** that are universally relevant:
- compound index prefix usage, sort direction, unique constraints, sparse/missing fields
- query planner choosing wrong index, range vs equality trade-offs
- index maintenance during insert/update/delete
- boundary conditions (empty results, single row, large datasets)

**Key difference from btree test porting**: The fixer agent should NOT copy SQLite C code.
Our planner/index code is original. Instead, the fixer analyzes the failing test, understands
what correct behavior should be, and fixes our Go implementation accordingly.

Tests go into: `/home/dev/work/any-store-custom-btree/` (root package `anystore`).

---

## Agent Roles

### Agent 1: Research

**Input:** A batch of SQLite `.test` file paths + this document.
**Output:** A test spec describing portable test ideas.

**Process:**
1. Read the SQLite test files
2. For each test, extract the **concept being tested** (not the SQL mechanics)
3. Classify each concept as:
   - `PORTABLE`: The concept maps directly to our system (e.g., "compound index prefix query")
   - `ADAPTABLE`: The concept is relevant but needs significant rethinking (e.g., "covering index scan" → we have CoverIter)
   - `OUT_OF_SCOPE`: SQL-specific or feature we don't have (e.g., partial indexes with WHERE, expression indexes, REINDEX)
4. For PORTABLE/ADAPTABLE tests, describe the test scenario in terms of our API
5. Group related tests into logical batches

**Scope Filter — IN SCOPE (concepts we have):**
- Single-field index: equality, range ($gt, $gte, $lt, $lte), $in, $ne
- Compound index: prefix usage, full match, skip-field scenarios
- Sort with index: ascending, descending, compound sort
- Reverse index fields (e.g., `"-createdDate"`)
- Unique index: constraint enforcement, duplicate detection
- Sparse index: missing fields, null values
- Index + filter + sort combinations
- Index + limit/offset optimization
- Index maintenance: insert/update/delete keep index consistent
- Multiple indexes on same collection
- Query planner selection: which index gets picked and why
- Edge cases: empty collection, single document, large datasets
- $or filter with index bounds
- Nested field indexes (e.g., `"meta.score"`)
- Array field indexes (multi-key)
- IndexHint to force index choice
- Explain output verification

**Scope Filter — OUT OF SCOPE (skip with comment):**
- `CREATE INDEX ... WHERE <expr>` (partial indexes — we don't have these)
- Expression indexes (`indexexpr*.test` — we index field values only)
- `REINDEX` command (we don't have this)
- Virtual table indexes (`bestindex*.test` — we don't have virtual tables)
- Skip-scan optimization (SQLite-specific optimization we don't implement)
- Automatic index creation (SQLite auto-indexes, not ours)
- Covering index scan optimization (SQLite meaning — our CoverIter is different)
- COLLATE / collation-sensitive indexes
- Multi-table JOINs using indexes
- SQL subqueries, CTEs, window functions
- INDEXED BY / NOT INDEXED hints (SQLite-specific syntax)
- Numeric affinity / type coercion in indexes

### Agent 2: Implementation

**Input:** A test spec from Agent 1 + this document.
**Output:** A Go test file at `<name>_index_test.go` in the root package.

**Rules — read these carefully:**

1. **Package:** `package anystore` (root package, NOT `package test`)

2. **File naming:** `<category>_index_test.go` (e.g., `compound_index_test.go`, `planner_sort_index_test.go`)

3. **Test naming:** `TestIndex_<Category>_<Scenario>` (e.g., `TestIndex_Compound_PrefixQuery`, `TestIndex_Unique_DuplicateInsert`)

4. **File header comment block (MANDATORY):**
   ```go
   /*
   Index/Planner tests inspired by SQLite: <list of source .test files>

   Test scenario:
   <Plain text description of what this test file verifies>

   These tests verify our custom index and query planner implementation.
   While inspired by SQLite test patterns, our system has a different
   architecture (document-oriented with weight-based planner vs SQL VDBE).
   */
   ```

5. **Test setup pattern:** Use the existing `newFixture` and collection API:
   ```go
   fx := newFixture(t)
   coll, err := fx.CreateCollection(ctx, "test")
   require.NoError(t, err)
   require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

   // Insert test data
   for i := range 100 {
       doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
       require.NoError(t, coll.Insert(ctx, doc))
   }
   ```

6. **Query pattern:**
   ```go
   // Count
   count, err := coll.Find(`{"a": 5}`).Count(ctx)
   require.NoError(t, err)
   assert.Equal(t, expected, count)

   // Iterate and collect
   iter, err := coll.Find(`{"a":{"$gte":3}}`).Sort("a").Limit(10).Iter(ctx)
   require.NoError(t, err)
   defer iter.Close()
   for iter.Next() {
       doc, err := iter.Doc()
       require.NoError(t, err)
       // verify doc...
   }
   require.NoError(t, iter.Err())

   // Explain
   explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
   require.NoError(t, err)
   assert.Contains(t, explain.Sql, "IndexScan")
   ```

7. **Verify correctness, not just "no error":**
   - Check result counts match expected
   - Check sort order is correct
   - Check Explain shows expected plan (IndexScan vs FullScan vs CoverLookup)
   - Check index length after mutations (`assertIndexLen`)
   - For unique indexes: verify constraint errors
   - Compare indexed vs non-indexed results for same query

8. **Comparison testing pattern (RECOMMENDED):**
   Run same query with and without index, verify identical results:
   ```go
   // Setup two collections with same data, one indexed, one not
   collIdx := setupWithIndex(t, fx)
   collNoIdx := setupWithoutIndex(t, fx)

   // Same query on both
   resultIdx := collectField(t, collIdx.Find(`{"a":{"$gte":5}}`).Sort("a"), "id")
   resultNoIdx := collectField(t, collNoIdx.Find(`{"a":{"$gte":5}}`).Sort("a"), "id")
   assert.Equal(t, resultNoIdx, resultIdx)
   ```

9. **Use existing helpers** from `qplanner_integration_test.go`:
   - `collectDocs(t, query)` — collect all results as JSON strings
   - `collectField(t, query, field)` — collect specific field values
   - `setupTestCollection(t, n, indexes...)` — create collection with n test docs
   - `assertIndexLen(t, idx, expected)` — verify index entry count
   - `assertCollCount(t, coll, expected)` — verify collection count

10. **Edge cases to always include:**
    - Empty collection (0 documents)
    - Single document
    - All documents match filter
    - No documents match filter
    - Boundary values in ranges ($gte exact boundary, $lt exact boundary)
    - Null/missing field values

### Agent 3: Build & Run

**Input:** A completed test file path.
**Process:**
1. Run `go test -v -run TestIndex_ ./... -count=1 -timeout=120s` — report results
2. If compilation error: fix and retry
3. Report pass/fail details

### Agent 4: Fixer (Bug Fix)

**Input:** A failing test + the specific failure details.
**Output:** A fix to the Go index/planner **implementation** (NOT the test).

**CRITICAL RULES:**

1. **Do NOT look at SQLite C code for index/planner fixes.** Our index and planner are our own design, not ported from SQLite. The SQLite C source (`btree.c`, etc.) is irrelevant for these bugs.

2. **Research workflow:**
   a. Understand the failing test — what operation fails, what's expected vs actual
   b. Trace through our code to understand the execution path:
      - Index storage: `index.go` (insertKeys, deleteKeys, fillKeysBuf)
      - Weight computation: `internal/qplanner/weight.go`
      - Plan building: `internal/qplanner/planner.go`
      - Iterators: `internal/qplanner/*.go` (index_iter, filter_iter, cover_iter, etc.)
      - Query/filter: `query/filter.go`, `query/bound.go`
      - Collection operations: `collection.go`
   c. Identify the discrepancy between expected and actual behavior
   d. Fix the Go code

3. **You CAN look at SQLite test files for edge case ideas** — understanding what behavior they expect can help you understand what correct behavior looks like. But the fix must be based on our code's logic, not on copying SQLite.

4. **Known issues to be aware of:**
   - Reverse scan direction may be inverted (`planner.go:94-101`, `!=` should be `==`)
   - FilterFullyCovered bitmap was inverted (may be fixed, verify)
   - Range filter selectivity not considered (planner may pick index when fullscan is better)
   - CoverFilterIter can add overhead for equal-weight indexes

5. **Do NOT:**
   - Modify the test to make it pass — fix the implementation
   - Add workarounds that don't address the root cause
   - Change behavior that other tests depend on without checking
   - Add SQLite-specific logic that doesn't belong in our system

6. **DO:**
   - Keep fixes minimal
   - Run ALL tests after fixing: `go test ./... -count=1 -timeout=300s`
   - Document what was wrong and why the fix is correct
   - If a fix would break other tests, report the conflict

---

## API Reference

### Core Types

```go
// Index definition
IndexInfo{
    Name:   string,    // Auto-generated if empty: "field1,field2"
    Fields: []string,  // ["name", "-date"] — prefix "-" = descending
    Unique: bool,
    Sparse: bool,
}

// Query
coll.Find(filter any) Query  // filter is JSON string or query.Filter
Query.Sort(fields ...any)     // "a" asc, "-a" desc
Query.Limit(n uint)
Query.Offset(n uint)
Query.IndexHint(hints ...IndexHint)
Query.Iter(ctx) (Iterator, error)
Query.Count(ctx) (int, error)
Query.Delete(ctx) (ModifyResult, error)
Query.Update(ctx, modifier any) (ModifyResult, error)
Query.Explain(ctx) (Explain, error)

// Explain
Explain{
    Sql:           string,           // e.g., "IndexScan(a) WHERE a=5 | Filter | Limit(10)"
    SqliteExplain: []string,
    Indexes:       []IndexExplain{Name, Weight, Used},
}

// Mutations
coll.Insert(ctx, docs ...*anyenc.Value) error
coll.UpdateOne(ctx, doc *anyenc.Value) error
coll.DeleteId(ctx, id any) error
coll.UpsertOne(ctx, doc *anyenc.Value) error

// Index management
coll.CreateIndex(ctx, info ...IndexInfo) error
coll.EnsureIndex(ctx, info ...IndexInfo) error
coll.DropIndex(ctx, indexName string) error
coll.GetIndexes() []Index
```

### Filter Syntax (JSON)

```json
{"field": value}                          // equality
{"field": {"$gt": 5}}                     // greater than
{"field": {"$gte": 5, "$lte": 10}}       // range
{"field": {"$in": [1, 2, 3]}}            // set membership
{"field": {"$ne": 5}}                     // not equal
{"$or": [{"a": 1}, {"b": 2}]}            // logical OR
{"$and": [{"a": {"$gte": 1}}, {"a": {"$lte": 5}}]}  // logical AND
{"field": {"$exists": true}}              // field existence
```

### Error Sentinels

```go
ErrUniqueConstraint    // duplicate on unique index
ErrCollectionNotFound
ErrDocumentNotFound
```

---

## Test Spec Format (Agent 1 Output)

```
=== TEST SPEC: <category> ===
Inspired by: <list of SQLite .test files that inspired these>

--- Test: <TestName> ---
Status: PORTABLE | ADAPTABLE | OUT_OF_SCOPE
Concept: <what universal indexing concept this tests>
Source inspiration: <SQLite test name/file, if applicable>
Operations:
  1. Create collection with index on (a, b)
  2. Insert 100 docs: {id:i, a:i%10, b:i%7}
  3. Query {a:5} → expect 10 results
  4. Explain → expect IndexScan
Go test name: TestIndex_Compound_PrefixQuery
```

---

## Test Batches

### Batch 1 — Single Index Basics
**Inspired by:** index.test, index3.test, descidx1.test
- Equality filter with index
- Range filters ($gt, $gte, $lt, $lte)
- $in filter with index
- $ne filter with index
- Sort ascending/descending with index
- Reverse index field ("-a")
- Empty result set
- All-matching filter
- Single document collection

### Batch 2 — Compound Indexes
**Inspired by:** index.test, index7.test, descidx2.test
- Two-field compound: full match, prefix-only, second-field-only
- Three-field compound: all fields, first two, first only, skip middle
- Compound sort matching index field order
- Compound with mixed directions (a, -b)
- Filter on first field + sort on second

### Batch 3 — Unique & Sparse Indexes
**Inspired by:** index3.test, index4.test
- Unique index: insert, duplicate detection, update maintaining uniqueness
- Unique index: update to duplicate value fails
- Unique compound index
- Sparse index: missing fields not indexed
- Sparse index: null values not indexed
- Sparse + unique combination
- Index length verification after each mutation

### Batch 4 — Index Maintenance (Insert/Update/Delete)
**Inspired by:** index.test, index7.test
- Insert updates index entries
- Update changes index entries (old removed, new added)
- Delete removes index entries
- Bulk insert + verify index length
- Update indexed field → re-query finds new value
- Delete via query with index → verify removal
- Array field index: insert/update/delete with arrays

### Batch 5 — Query Planner Selection
**Inspired by:** where.test, where2.test, where7.test
- Planner picks index over fullscan for equality
- Planner picks compound index over single for multi-field filter
- Planner uses index for sort (no in-memory sort)
- Planner uses CoverLookup for unique equality
- Multiple indexes: planner picks best one
- IndexHint overrides planner choice
- Filter + sort on same index field
- Filter on one field, sort on different indexed field

### Batch 6 — Limit/Offset with Index
**Inspired by:** select tests, where tests
- Sort + Limit with index (early termination)
- Sort + Offset + Limit with index
- Filter + Sort + Limit
- Offset larger than result set
- Limit larger than result set
- Limit(1) with unique index → CoverLookup

### Batch 7 — Edge Cases & Stress
**Inspired by:** index.test, numindex1.test, various
- Large dataset (1000+ docs) with index
- Very selective filter (0.1% match)
- Very wide range (99% match)
- All same indexed value
- Index on nested fields (meta.score)
- Index with many duplicate values
- Create and drop index, verify queries still work
- EnsureIndex idempotency
- Multiple indexes, drop one, other still works

### Batch 8 — Array & Nested Field Indexes
**Inspired by:** index.test
- Array field: multiple index entries per doc
- Array field: query matches array element
- Array field: update array → index updated
- Nested field index: query with dot notation
- Nested field: missing parent object
- Mixed arrays and nested fields in compound index

### Batch 9 — $or and Complex Filters
**Inspired by:** where.test, where9.test
- $or with indexed field → multiple bounds
- $or mixing indexed and non-indexed fields
- $and with range on same field
- Nested $or / $and combinations
- $exists with sparse index
- $ne with index

---

## Imports Template

```go
package anystore

import (
    "fmt"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/anyproto/any-store/anyenc"
)
```

Only include what's actually used.

---

## Worked Example

### Test Spec:
```
--- Test: TestIndex_Compound_PrefixQuery ---
Status: PORTABLE
Concept: Compound index used for prefix-only query
Source inspiration: index.test (compound index tests)
Operations:
  1. Create collection with index on (a, b)
  2. Insert 100 docs with a=i%10, b=i%7
  3. Query {a:5} → expect 10 results
  4. Explain → expect IndexScan (not FullScan)
  5. Verify results match non-indexed query
```

### Go Implementation:
```go
/*
Index/Planner tests inspired by SQLite: index.test

Test scenario:
Tests compound index prefix query — querying on first field only
should still use the compound index for efficient lookup.
*/
package anystore

import (
    "fmt"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"

    "github.com/anyproto/any-store/anyenc"
)

func TestIndex_Compound_PrefixQuery(t *testing.T) {
    fx := newFixture(t)
    coll, err := fx.CreateCollection(ctx, "test")
    require.NoError(t, err)
    require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

    for i := range 100 {
        doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
        require.NoError(t, coll.Insert(ctx, doc))
    }

    // Query using only first field of compound index
    count, err := coll.Find(`{"a": 5}`).Count(ctx)
    require.NoError(t, err)
    assert.Equal(t, 10, count) // 100/10 values with a=5

    // Verify planner uses index
    explain, err := coll.Find(`{"a": 5}`).Explain(ctx)
    require.NoError(t, err)
    assert.Contains(t, explain.Sql, "IndexScan")
    assert.NotContains(t, explain.Sql, "FullScan")
}
```

---

## Priority

Start with Batch 1-3 (basic correctness), then Batch 4-5 (maintenance + planner), then Batch 6-9 (advanced).
