=== TEST SPEC: Index Batches 4-6 ===
Inspired by: where.test, where2.test, where4.test, where9.test

Research notes:
- where.test: Core WHERE clause index usage — equality, range, compound index prefix,
  IN operator with indexes, search count verification
- where2.test: Index selection heuristics — unique preferred over non-unique,
  compound index with IN on multiple columns, sort avoidance when unique constraint
  guarantees single row, OR→IN optimization, redundant index columns
- where4.test: IS NULL optimization with compound indexes, NULL in IN lists
- where9.test: Multi-index OR optimization, OR with AND external terms,
  OR in DELETE/UPDATE, index selection (equality > OR > inequality)

---

## Batch 4 — Index Maintenance During Insert/Update/Delete

--- Test: TestIndex_Maintenance_InsertUpdatesIndex ---
Status: PORTABLE
Concept: Inserting a document creates corresponding index entries
Inspiration: where.test (setup + verification pattern)
Operations:
  1. Create collection with index on (a)
  2. Insert 10 docs with a=1..10
  3. Verify index length = 10
  4. Query {a:5} → expect 1 result
  5. Explain → expect IndexScan

--- Test: TestIndex_Maintenance_DeleteRemovesIndex ---
Status: PORTABLE
Concept: Deleting a document removes its index entries
Inspiration: where9.test (DELETE with OR clauses using indexes, where9-6.2)
Operations:
  1. Create collection with index on (a)
  2. Insert 20 docs with a=1..20
  3. Verify index length = 20
  4. Delete doc with id=5
  5. Verify index length = 19
  6. Query {a:5} → expect 0 results
  7. Query {a:6} → expect 1 result (other entries unaffected)

--- Test: TestIndex_Maintenance_UpdateChangesIndex ---
Status: PORTABLE
Concept: Updating an indexed field removes old entry and adds new entry
Inspiration: where9.test (UPDATE SET a=a+100 with index usage, where9-6.2.6)
Operations:
  1. Create collection with index on (a)
  2. Insert doc {id:1, a:10}
  3. Verify index length = 1, query {a:10} returns 1
  4. UpdateOne {id:1, a:20}
  5. Verify index length still = 1
  6. Query {a:10} → expect 0 results (old entry removed)
  7. Query {a:20} → expect 1 result (new entry added)

--- Test: TestIndex_Maintenance_UpdateNonIndexedField ---
Status: PORTABLE
Concept: Updating a non-indexed field does not change index entries
Inspiration: General correctness
Operations:
  1. Create collection with index on (a)
  2. Insert doc {id:1, a:10, b:100}
  3. Verify index length = 1
  4. UpdateOne {id:1, a:10, b:200}
  5. Verify index length still = 1
  6. Query {a:10} → expect 1 result, b should be 200

--- Test: TestIndex_Maintenance_BulkInsertIndexLength ---
Status: PORTABLE
Concept: Bulk insert creates correct number of index entries
Inspiration: where.test (100-row table setup, where-1.0)
Operations:
  1. Create collection with index on (x)
  2. Insert 100 docs where x=floor(log2(i)) — many duplicates
  3. Verify index length = 100 (one entry per doc, not per unique value)
  4. Query {x:3} → verify correct count (docs where floor(log2(i))=3, i.e. i=8..15 → 8 docs)
  5. Explain → IndexScan

--- Test: TestIndex_Maintenance_DeleteViaQuery ---
Status: PORTABLE
Concept: Deleting documents through a query filter correctly removes index entries
Inspiration: where9-6.2.2 (DELETE FROM t1 WHERE b IS NULL OR c IS NULL)
Operations:
  1. Create collection with indexes on (a) and (b)
  2. Insert 50 docs, some with a > 40
  3. Delete all docs where a > 40 using coll.Find(`{"a":{"$gt":40}}`).Delete(ctx)
  4. Verify index length on (a) decreased by deleted count
  5. Verify index length on (b) also decreased by same count
  6. Query {a:45} → expect 0 results
  7. Query {a:5} → expect 1 result (unaffected)

--- Test: TestIndex_Maintenance_UpdateViaQuery ---
Status: PORTABLE
Concept: Updating documents through a query filter correctly updates index entries
Inspiration: where9-6.2.6 (UPDATE t1 SET a=a+100 with OR conditions)
Operations:
  1. Create collection with index on (a)
  2. Insert 20 docs with a=1..20
  3. Update docs where a >= 15, set b=999 (non-indexed field change)
  4. Verify index length unchanged
  5. Query {a:15} still returns 1 result, with b=999

--- Test: TestIndex_Maintenance_CompoundInsertDelete ---
Status: PORTABLE
Concept: Compound index entries correctly maintained through insert/delete cycle
Inspiration: where2.test (compound index i1xy on (x,y))
Operations:
  1. Create collection with index on (a, b)
  2. Insert 30 docs with a=i%5, b=i%7
  3. Verify index length = 30
  4. Delete 10 specific docs
  5. Verify index length = 20
  6. Query {a:2, b:3} → verify correct count
  7. Insert 5 new docs
  8. Verify index length = 25

--- Test: TestIndex_Maintenance_UniqueUpdateToExisting ---
Status: PORTABLE
Concept: Updating a document to a value that already exists in a unique index fails
Inspiration: where4.test (unique index semantics)
Operations:
  1. Create collection with unique index on (a)
  2. Insert {id:1, a:10} and {id:2, a:20}
  3. Verify index length = 2
  4. UpdateOne {id:1, a:20} → expect ErrUniqueConstraint
  5. Verify index length still = 2
  6. Verify {id:1, a:10} still exists unchanged

--- Test: TestIndex_Maintenance_SparseInsertMissingField ---
Status: PORTABLE
Concept: Sparse index does not create entries for documents missing the indexed field
Inspiration: where4.test (NULL handling in index)
Operations:
  1. Create collection with sparse index on (a)
  2. Insert {id:1, a:10} and {id:2, b:20} (no 'a' field)
  3. Verify index length = 1 (only doc with 'a' field)
  4. Insert {id:3, a:null} — null also excluded from sparse
  5. Verify index length still = 1

--- Test: TestIndex_Maintenance_ArrayFieldInsertDelete ---
Status: PORTABLE
Concept: Array field index creates multiple entries per document, correctly removed on delete
Inspiration: General multi-key index correctness
Operations:
  1. Create collection with index on (tags)
  2. Insert {id:1, tags:["go","rust","python"]}
  3. Verify index length = 4 (3 elements + 1 array-as-value)
  4. Delete doc id=1
  5. Verify index length = 0

---

## Batch 5 — Planner Selection (Which Index, Why)

--- Test: TestIndex_Planner_EqualityOverFullScan ---
Status: PORTABLE
Concept: Planner picks index for equality filter over full table scan
Inspiration: where-1.1 (SELECT WHERE w=10 uses i1w, search count=3 vs 99 for fullscan)
Operations:
  1. Create collection with index on (w)
  2. Insert 100 docs with w=1..100
  3. Query {w:10} → expect 1 result
  4. Explain → expect IndexScan, NOT FullScan
  5. Compare with unindexed collection: same result, different plan

--- Test: TestIndex_Planner_CompoundOverSingle ---
Status: PORTABLE
Concept: Planner prefers compound index when both fields are in filter
Inspiration: where-1.8 (x=3 AND y=100 uses i1xy not i1w)
Operations:
  1. Create collection with index on (w), and index on (x, y)
  2. Insert 100 docs with w=i, x=floor(log2(i)), y=i*i+2*i+1
  3. Query {x:3, y:144} → expect 1 result
  4. Explain → expect the compound index (x,y) used (higher weight: 10*2=20 vs 10)
  5. Verify result correctness

--- Test: TestIndex_Planner_UniquePreferred ---
Status: PORTABLE
Concept: Unique index preferred over non-unique index on same field
Inspiration: where2-1.1 (unique i1w preferred over non-unique i1xy for w=85)
Operations:
  1. Create collection with non-unique index on (w) and unique index on (w) [second named differently]
  2. Insert 100 docs
  3. Query {w:85} → expect 1 result
  4. Explain → expect the unique index used (gets +1 weight bonus)
  5. Verify CoverLookup in plan (unique + point lookup)

--- Test: TestIndex_Planner_SortUsesIndex ---
Status: PORTABLE
Concept: Index covering sort field avoids in-memory sort
Inspiration: where2-2.1 (unique w=85 → ORDER BY skipped, nosort)
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i
  3. Query Find("").Sort("a").Limit(10) → first 10 in order
  4. Explain → expect IndexScan, no SortIter
  5. Compare: same query without index → FullScan + Sort

--- Test: TestIndex_Planner_SortDescWithReverseIndex ---
Status: PORTABLE
Concept: Descending sort uses reverse index scan instead of collecting+sorting
Inspiration: where2-3.2 (ORDER BY rowid DESC LIMIT 2 → nosort)
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i
  3. Query Sort("-a").Limit(5) → expect [100,99,98,97,96]
  4. Explain → expect IndexScan (reverse), no SortIter

--- Test: TestIndex_Planner_FilterAndSort_SameField ---
Status: PORTABLE
Concept: Filter + sort on same indexed field uses single index scan
Inspiration: where-1.20 (x=3 AND y>=225 uses i1xy compound)
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i%20
  3. Query {a:{"$gte":10,"$lte":15}}, Sort("a") → expect sorted subset
  4. Explain → expect IndexScan with bounds, no SortIter (ExactSort)

--- Test: TestIndex_Planner_FilterAndSort_DifferentField ---
Status: ADAPTABLE
Concept: Filter on one field, sort on different field — planner picks best strategy
Inspiration: where2.test patterns (index on x,y can filter x=3 but not sort by w)
Operations:
  1. Create collection with index on (a) and index on (b)
  2. Insert 100 docs
  3. Query {a:5}, Sort("b") → planner picks (a) for filter, adds SortIter for b
  4. Explain → IndexScan(a) + Sort
  5. Alternative: with compound index (a,b) → IndexScan + no sort

--- Test: TestIndex_Planner_IndexHintOverride ---
Status: PORTABLE
Concept: IndexHint forces the planner to pick a specific index regardless of weight
Inspiration: where9-4.4 (INDEXED BY t1b forces index selection)
Operations:
  1. Create collection with index on (a), index on (b)
  2. Insert 100 docs
  3. Query {a:5, b:10} → default plan picks higher-weight index
  4. Same query with IndexHint("b", 100) → forces index (b)
  5. Explain shows different index used with hint

--- Test: TestIndex_Planner_MultipleIndexes_BestPicked ---
Status: PORTABLE
Concept: With multiple indexes available, planner picks highest weight
Inspiration: where2.test (i1w vs i1xy vs i1zyx — best index picked based on query)
Operations:
  1. Create collection with indexes on (a), (b), (a,b), (b,c)
  2. Insert 100 docs
  3. Query {a:5} → picks (a) or (a,b) — both have weight 10, but (a,b) might win
  4. Query {a:5, b:3} → picks (a,b) compound (weight 20)
  5. Query {b:3, c:7} → picks (b,c) compound
  6. Verify each via Explain

--- Test: TestIndex_Planner_EqualityOverRange_InOR ---
Status: ADAPTABLE
Concept: Equality query preferred over OR-based range query
Inspiration: where9-5.2 (b=1000 preferred over c=31031 OR d IS NULL)
Operations:
  1. Create collection with indexes on (a), (b), (c)
  2. Insert 100 docs
  3. Query {a:50} → IndexScan(a)
  4. Query {"$or":[{"b":30},{"c":40}]} → either index or fullscan
  5. Query {a:50, "$or":[{"b":30},{"c":40}]} → planner should use (a) equality first
  6. Explain to verify

--- Test: TestIndex_Planner_CompoundSort_MatchesIndexOrder ---
Status: PORTABLE
Concept: Compound sort matching index field order avoids in-memory sort
Inspiration: where2-4.6c (ORDER BY x, y with index i1xy → nosort)
Operations:
  1. Create collection with index on (x, y)
  2. Insert 50 docs
  3. Sort("x", "y") → no SortIter needed (ExactSort)
  4. Sort("x", "-y") → SortIter needed (direction mismatch on y)
  5. Sort("y", "x") → SortIter needed (field order mismatch)
  6. Verify via Explain

--- Test: TestIndex_Planner_CompoundSort_PartialMatch ---
Status: PORTABLE
Concept: Sorting by first field of compound index is partially covered
Inspiration: where2.test patterns (compound index covers leading sort fields)
Operations:
  1. Create collection with index on (a, b, c)
  2. Insert 100 docs
  3. Sort("a") → PartialSort (only first field matched)
  4. Sort("a", "b") → PartialSort (2 of 3 fields)
  5. Sort("a", "b", "c") → ExactSort (all fields)
  6. Verify via Explain output

--- Test: TestIndex_Planner_CoverLookup_UniqueEquality ---
Status: PORTABLE
Concept: Unique index with equality produces CoverLookup (fastest path)
Inspiration: where-1.8.3 (COVERING INDEX used for x=? AND y=?)
Operations:
  1. Create collection with unique index on (a)
  2. Insert 100 docs with unique a values
  3. Query {a:42} → expect 1 result
  4. Explain → expect "CoverLookup" in plan (not IndexScan)
  5. Verify result correctness

--- Test: TestIndex_Planner_NoIndexUsed_NoFilter ---
Status: PORTABLE
Concept: Query with no filter and no sort uses FullScan, not an index
Inspiration: General planner correctness
Operations:
  1. Create collection with indexes on (a), (b)
  2. Insert 50 docs
  3. Query Find("") (no filter) → FullScan
  4. Explain → FullScan, no IndexScan

--- Test: TestIndex_Planner_InOperator_WithIndex ---
Status: PORTABLE
Concept: $in operator generates multiple bounds, uses index
Inspiration: where-5.3a (w IN (-1,1,2,3) uses index, search count=12 vs 102 for fullscan)
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i
  3. Query {a:{"$in":[5,10,15,20]}} → expect 4 results
  4. Explain → IndexScan (multiple bounds)
  5. Verify result correctness

---

## Batch 6 — Limit/Offset Optimization with Indexes

--- Test: TestIndex_LimitOffset_SortLimitWithIndex ---
Status: PORTABLE
Concept: Sort + Limit with index enables early termination (no full sort)
Inspiration: where2-3.1 (ORDER BY rowid LIMIT 2 → nosort, only 2 results)
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i
  3. Query Sort("a").Limit(5) → expect [1,2,3,4,5]
  4. Explain → IndexScan + LimitIter (no SortIter)
  5. Query Sort("-a").Limit(3) → expect [100,99,98]

--- Test: TestIndex_LimitOffset_SortOffsetLimitWithIndex ---
Status: PORTABLE
Concept: Sort + Offset + Limit with index
Inspiration: where2-3.1 extended pattern
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i
  3. Query Sort("a").Offset(10).Limit(5) → expect [11,12,13,14,15]
  4. Explain → IndexScan + LimitIter
  5. Query Sort("-a").Offset(5).Limit(3) → expect [95,94,93]

--- Test: TestIndex_LimitOffset_FilterSortLimit ---
Status: PORTABLE
Concept: Filter + Sort + Limit combination with index
Inspiration: Combined pattern from where.test
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i%20
  3. Query {a:{"$gte":10}}, Sort("a"), Limit(5) → first 5 with a>=10
  4. Explain → IndexScan with bounds + LimitIter
  5. Verify correct results

--- Test: TestIndex_LimitOffset_OffsetLargerThanResultSet ---
Status: PORTABLE
Concept: Offset larger than total results returns empty set
Inspiration: Edge case testing
Operations:
  1. Create collection with index on (a)
  2. Insert 10 docs
  3. Query Sort("a").Offset(100).Limit(5) → expect 0 results
  4. No error, just empty iterator

--- Test: TestIndex_LimitOffset_LimitLargerThanResultSet ---
Status: PORTABLE
Concept: Limit larger than total results returns all results
Inspiration: Edge case testing
Operations:
  1. Create collection with index on (a)
  2. Insert 10 docs
  3. Query Sort("a").Limit(100) → expect all 10 results in order
  4. Verify all 10 returned correctly

--- Test: TestIndex_LimitOffset_LimitOneUnique ---
Status: PORTABLE
Concept: Limit(1) with unique index equality is maximally efficient
Inspiration: where2-2.1 (unique constraint → at most 1 row)
Operations:
  1. Create collection with unique index on (a)
  2. Insert 100 docs with unique a values
  3. Query {a:42}.Limit(1) → expect exactly 1 result
  4. Explain → CoverLookup + LimitIter
  5. Verify correct doc returned

--- Test: TestIndex_LimitOffset_PaginationConsistency ---
Status: PORTABLE
Concept: Paginating through results with Offset/Limit returns all docs exactly once
Inspiration: General pagination correctness
Operations:
  1. Create collection with index on (a)
  2. Insert 50 docs with a=i
  3. Page through with Limit(10), Offset(0,10,20,30,40)
  4. Collect all results across pages
  5. Verify all 50 docs returned, no duplicates, no gaps, correct order

--- Test: TestIndex_LimitOffset_LimitWithNoSort ---
Status: PORTABLE
Concept: Limit without explicit sort still works (uses natural order)
Inspiration: Basic LIMIT correctness
Operations:
  1. Create collection with index on (a)
  2. Insert 20 docs
  3. Query Find("").Limit(5) → returns some 5 docs (order unspecified)
  4. Verify exactly 5 returned
  5. Query {a:{"$gte":10}}.Limit(3) → returns 3 matching docs

--- Test: TestIndex_LimitOffset_DescSortWithOffset ---
Status: PORTABLE
Concept: Descending sort with offset skips from the high end
Inspiration: where2-3.2 (DESC LIMIT pattern)
Operations:
  1. Create collection with index on (a)
  2. Insert 100 docs with a=i
  3. Query Sort("-a").Offset(5).Limit(5) → expect [95,94,93,92,91]
  4. Verify correct values

--- Test: TestIndex_LimitOffset_CompoundSortLimit ---
Status: PORTABLE
Concept: Limit with compound sort uses compound index for early termination
Inspiration: where2-4.6 patterns
Operations:
  1. Create collection with index on (x, y)
  2. Insert 100 docs with x=i%10, y=i%7
  3. Query Sort("x","y").Limit(10) → first 10 in compound order
  4. Explain → IndexScan (x,y) + LimitIter, no SortIter
  5. Verify results are correctly ordered by (x, then y)
