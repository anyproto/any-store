# Index Test Spec — Batches 1-3
## Inspired by: index.test, index3.test, index4.test, descidx1.test, descidx2.test, descidx3.test

---

## Batch 1 — Single Index Basics

### Equality, Range, Sort, and Direction Tests

--- Test: TestIndex_Single_Equality ---
Status: PORTABLE
Concept: Single-field index used for equality lookup. Verifies that a query
  filtering on an indexed field uses IndexScan (not FullScan) and returns
  correct results.
Inspiration: index.test (index-4.2 through index-4.12: SELECT WHERE power=4,
  SELECT WHERE cnt=6 — verifying index-assisted equality lookup)
Operations:
  1. Create collection, EnsureIndex on field "a"
  2. Insert 20 docs: {id:i, a:i, b:i*2}
  3. Find({"a": 10}) → expect exactly 1 result with correct b value
  4. Explain → expect IndexScan
  5. Compare result with unindexed collection for same query

--- Test: TestIndex_Single_EqualityDuplicates ---
Status: PORTABLE
Concept: Index with duplicate values returns all matching documents.
  Multiple documents sharing the same indexed value should all be found.
Inspiration: index.test (index-10.0: multiple entries with same key a=1,
  returns b=2 and b=12; index-10.4: 9 entries with a=1)
Operations:
  1. Create collection with index on "a"
  2. Insert docs with duplicate "a" values: {id:1,a:1,b:10}, {id:2,a:1,b:20},
     {id:3,a:1,b:30}, {id:4,a:2,b:40}
  3. Find({"a": 1}).Sort("b") → expect [10,20,30]
  4. Find({"a": 2}) → expect [40]
  5. Explain → IndexScan for both

--- Test: TestIndex_Single_RangeGt ---
Status: PORTABLE
Concept: Index range scan with $gt (strict greater-than).
Inspiration: descidx1.test (descidx1-2.1: WHERE a>3 AND a<7 → returns {4,5,6})
Operations:
  1. Create collection with index on "a"
  2. Insert 8 docs: {id:i, a:i} for i=1..8
  3. Find({"a":{"$gt":3}}).Sort("a") → expect [4,5,6,7,8]
  4. Explain → IndexScan

--- Test: TestIndex_Single_RangeGte ---
Status: PORTABLE
Concept: Index range scan with $gte (inclusive lower bound).
Inspiration: descidx1.test (descidx1-2.3: WHERE a>=3 AND a<7 → includes 3)
Operations:
  1. Create collection with index on "a"
  2. Insert 8 docs: {id:i, a:i}
  3. Find({"a":{"$gte":3}}).Sort("a") → expect [3,4,5,6,7,8]
  4. Find({"a":{"$gte":3,"$lt":7}}).Sort("a") → expect [3,4,5,6]
  5. Explain → IndexScan

--- Test: TestIndex_Single_RangeLt ---
Status: PORTABLE
Concept: Index range scan with $lt (strict less-than).
Inspiration: descidx1.test (descidx1-2.1: a<7 returns up to 6)
Operations:
  1. Create collection with index on "a"
  2. Insert 8 docs
  3. Find({"a":{"$lt":5}}).Sort("a") → expect [1,2,3,4]
  4. Explain → IndexScan

--- Test: TestIndex_Single_RangeLte ---
Status: PORTABLE
Concept: Index range scan with $lte (inclusive upper bound).
Inspiration: descidx1.test (descidx1-2.5: WHERE a>=3 AND a<=7 → includes both)
Operations:
  1. Create collection with index on "a"
  2. Insert 8 docs
  3. Find({"a":{"$lte":5}}).Sort("a") → expect [1,2,3,4,5]
  4. Find({"a":{"$gte":3,"$lte":7}}).Sort("a") → expect [3,4,5,6,7]

--- Test: TestIndex_Single_RangeCombined ---
Status: PORTABLE
Concept: Index range scan with both lower and upper bounds.
Inspiration: descidx1.test (descidx1-2.1: WHERE a>3 AND a<7, descidx1-2.4:
  WHERE a>3 AND a<=7, descidx1-2.5: WHERE a>=3 AND a<=7)
Operations:
  1. Create collection with index on "a", insert 8 docs
  2. Find({"a":{"$gt":3,"$lt":7}}).Sort("a") → expect [4,5,6]
  3. Find({"a":{"$gt":3,"$lte":7}}).Sort("a") → expect [4,5,6,7]
  4. Find({"a":{"$gte":3,"$lt":7}}).Sort("a") → expect [3,4,5,6]
  5. Find({"a":{"$gte":3,"$lte":7}}).Sort("a") → expect [3,4,5,6,7]

--- Test: TestIndex_Single_In ---
Status: PORTABLE
Concept: Index used for $in filter. The planner should use the index to look
  up discrete values efficiently.
Inspiration: descidx3.test (descidx3-4.1: WHERE a IN (1,2) AND b>0 —
  demonstrates IN-list with index)
Operations:
  1. Create collection with index on "a", insert 20 docs
  2. Find({"a":{"$in":[3,7,11]}}).Sort("a") → expect docs with a in {3,7,11}
  3. Explain → expect IndexScan
  4. Verify count matches expected

--- Test: TestIndex_Single_Ne ---
Status: PORTABLE
Concept: Index with $ne filter. The planner may or may not use the index for
  $ne; the key test is correctness of results.
Inspiration: index.test (general concept of filtering with index present)
Operations:
  1. Create collection with index on "a", insert 10 docs a=0..9
  2. Find({"a":{"$ne":5}}).Sort("a") → expect [0,1,2,3,4,6,7,8,9]
  3. Verify count = 9

--- Test: TestIndex_Single_SortAsc ---
Status: PORTABLE
Concept: Index-assisted ascending sort. When sort field matches index field,
  no in-memory sort should be needed.
Inspiration: descidx1.test (descidx1-3.1: ORDER BY a → nosort;
  descidx1-3.7: ORDER BY b → nosort because b has ASC index)
Operations:
  1. Create collection with index on "a"
  2. Insert 8 docs in random order: a values [5,2,8,1,4,7,3,6]
  3. Find("").Sort("a").Iter() → collect results, verify order [1,2,3,4,5,6,7,8]
  4. Explain → should not show "Sort" step (index provides order)

--- Test: TestIndex_Single_SortDesc ---
Status: PORTABLE
Concept: Index-assisted descending sort. An ascending index can be scanned in
  reverse to provide descending order without sorting.
Inspiration: descidx1.test (descidx1-3.3: ORDER BY a DESC → nosort;
  descidx1-3.9: ORDER BY b DESC → nosort)
Operations:
  1. Create collection with index on "a"
  2. Insert 8 docs with a=1..8
  3. Find("").Sort("-a").Iter() → verify order [8,7,6,5,4,3,2,1]
  4. Explain → should not show "Sort" step

--- Test: TestIndex_Single_ReverseField ---
Status: PORTABLE
Concept: Index defined with descending field ("-a"). Natural order is reversed.
  Ascending sort requires reverse scan or in-memory sort.
Inspiration: descidx1.test (descidx1-1.2: CREATE INDEX i2 ON t1(a DESC);
  descidx1-2.1: range queries on DESC index return results in DESC order)
Operations:
  1. Create collection, EnsureIndex on "-a" (descending)
  2. Insert 8 docs with a=1..8
  3. Find({"a":{"$gt":3,"$lt":7}}) → expect results (direction of natural order)
  4. Find("").Sort("-a") → expect [8,7,6,5,4,3,2,1], Explain should show no Sort
  5. Find("").Sort("a") → expect [1,2,3,4,5,6,7,8], may need Sort or reverse scan

--- Test: TestIndex_Single_RangeWithSort ---
Status: PORTABLE
Concept: Range filter combined with sort on the same indexed field.
  The index should provide both filtering and ordering.
Inspiration: descidx1.test (descidx1-3.21..26: WHERE a>3 AND a<8 ORDER BY a ASC/DESC
  → all nosort)
Operations:
  1. Create collection with index on "a", insert 8 docs
  2. Find({"a":{"$gt":3,"$lt":8}}).Sort("a") → expect [4,5,6,7] in order
  3. Find({"a":{"$gt":3,"$lt":8}}).Sort("-a") → expect [7,6,5,4] in order
  4. Explain → should not show Sort step

--- Test: TestIndex_Single_EmptyResult ---
Status: PORTABLE
Concept: Query that matches no documents should return empty result set
  without error, even with index.
Inspiration: index.test (index-10.7: after deleting all matching rows,
  SELECT returns empty)
Operations:
  1. Create collection with index on "a", insert 10 docs with a=1..10
  2. Find({"a": 99}) → count=0
  3. Find({"a":{"$gt":100}}) → count=0
  4. No error returned

--- Test: TestIndex_Single_AllMatch ---
Status: PORTABLE
Concept: When all documents match the filter, the index scan returns
  everything (same as fullscan but through index).
Inspiration: General correctness
Operations:
  1. Create collection with index on "a", insert 10 docs with a=1..10
  2. Find({"a":{"$gte":1,"$lte":10}}).Sort("a") → expect all 10
  3. Count → 10

--- Test: TestIndex_Single_SingleDoc ---
Status: PORTABLE
Concept: Edge case: collection with a single document. Index should work
  correctly for both match and miss.
Inspiration: index4.test (1.7: single row table with index, integrity check ok)
Operations:
  1. Create collection with index on "a"
  2. Insert single doc {id:1, a:42}
  3. Find({"a": 42}) → expect 1 result
  4. Find({"a": 99}) → expect 0 results
  5. assertIndexLen → 1

--- Test: TestIndex_Single_EmptyCollection ---
Status: PORTABLE
Concept: Edge case: queries on empty collection with index should return empty
  results without error.
Inspiration: General robustness
Operations:
  1. Create collection, EnsureIndex on "a" (no docs inserted)
  2. Find({"a": 1}).Count → 0
  3. Find("").Sort("a").Count → 0
  4. assertIndexLen → 0

--- Test: TestIndex_Single_DeleteAndQuery ---
Status: PORTABLE
Concept: After deleting documents, index queries should reflect the deletion.
  Progressive deletion should narrow results.
Inspiration: index.test (index-10.2..10.8: progressive deletion of entries,
  verifying correct remaining results after each delete)
Operations:
  1. Create collection with index on "a"
  2. Insert: {id:1,a:1,b:1}, {id:2,a:1,b:2}, ..., {id:9,a:1,b:9}, {id:10,a:2,b:0}
  3. Find({"a":1}).Sort("b") → expect b=[1..9]
  4. Delete docs with b in {2,4,6,8}
  5. Find({"a":1}).Sort("b") → expect b=[1,3,5,7,9]
  6. Delete b>2
  7. Find({"a":1}).Sort("b") → expect b=[1]
  8. Delete b=1 → Find({"a":1}) → count=0
  9. Find({"a":2}) → expect b=[0] (untouched)

--- Test: TestIndex_Single_NullValues ---
Status: PORTABLE
Concept: Documents with null or missing indexed field should still be
  indexed (with null key) in non-sparse indexes.
Inspiration: index.test (index-14.1..14.3: NULL values in compound index,
  sorted and queried correctly)
Operations:
  1. Create collection with index on "a" (non-sparse)
  2. Insert {id:1,a:null}, {id:2}, {id:3,a:5}
  3. assertIndexLen → 3 (all indexed, including nulls)
  4. Find({"a":5}) → expect 1 result
  5. Compare count with unindexed collection

---

## Batch 2 — Compound Indexes

### Prefix, Full Match, Mixed Direction, and Multi-Field Sort

--- Test: TestIndex_Compound_PrefixQuery ---
Status: PORTABLE
Concept: Compound index on (a,b) used for query on first field only.
  The prefix of a compound index should be usable for single-field queries.
Inspiration: descidx1.test (descidx1-4.3: WHERE a>=2 uses compound index i3
  on (a ASC, b DESC, c ASC))
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Insert 100 docs: {id:i, a:i%10, b:i%7}
  3. Find({"a":5}).Sort("a") → expect 10 results
  4. Explain → IndexScan
  5. Verify results match non-indexed query

--- Test: TestIndex_Compound_FullMatch ---
Status: PORTABLE
Concept: Compound index on (a,b) used for equality on both fields.
  Full match on all index fields should be highly selective.
Inspiration: descidx1.test (descidx1-4.5: WHERE a=2 AND b>'two' uses
  compound index)
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Insert 100 docs: {id:i, a:i%10, b:i%5}
  3. Find({"a":5,"b":3}) → expect small number of results
  4. Explain → IndexScan

--- Test: TestIndex_Compound_SecondFieldOnly ---
Status: PORTABLE
Concept: Query on second field of compound index (a,b) — the index CANNOT
  be used efficiently. Should fall back to FullScan or pick different index.
Inspiration: General index theory — compound index prefix property
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Insert 100 docs
  3. Find({"b":3}) → verify correct results
  4. Explain → expect FullScan (cannot use prefix)
  5. Compare with non-indexed query

--- Test: TestIndex_Compound_ThreeFieldPrefix ---
Status: PORTABLE
Concept: Three-field compound index — test prefix usage at 1, 2, and 3 fields.
Inspiration: descidx1.test (descidx1-5: four-field compound index
  (a DESC, b ASC, c DESC, d ASC), testing various sort/filter combos);
  descidx3.test (descidx3-1.1: three-field compound index)
Operations:
  1. Create collection, EnsureIndex on ["a", "b", "c"]
  2. Insert 100 docs: {id:i, a:i%5, b:i%4, c:i%3}
  3. Find({"a":2}) → uses index prefix
  4. Find({"a":2,"b":1}) → uses two-field prefix
  5. Find({"a":2,"b":1,"c":0}) → uses full index
  6. Find({"b":1}) → cannot use index prefix
  7. Find({"c":0}) → cannot use index prefix
  8. Find({"a":2,"c":0}) → can use prefix on "a", must filter "c"

--- Test: TestIndex_Compound_SortMatchesIndex ---
Status: PORTABLE
Concept: Compound sort that matches the index field order should use the
  index for ordering without in-memory sort.
Inspiration: descidx1.test (descidx1-5.3: ORDER BY a DESC, b ASC, c DESC, d ASC
  matches index order → nosort)
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Insert docs in random order
  3. Find("").Sort("a","b") → verify sorted, Explain shows no Sort step
  4. Find("").Sort("-a","-b") → verify reverse sorted, Explain shows no Sort step

--- Test: TestIndex_Compound_SortReversedIndex ---
Status: PORTABLE
Concept: Sort in exact reverse of index order can use the index in reverse.
Inspiration: descidx1.test (descidx1-5.4: ORDER BY a ASC, b DESC, c ASC, d DESC
  is exact reverse of index → nosort)
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Find("").Sort("-a", "-b") → should use reverse index scan, no Sort step

--- Test: TestIndex_Compound_SortMismatch ---
Status: PORTABLE
Concept: Sort that doesn't match index field order requires in-memory sort.
Inspiration: descidx1.test (descidx1-5.7: ORDER BY a ASC, b DESC, c DESC
  does not match index → sort; descidx1-5.8: ORDER BY a ASC, b ASC, c ASC → sort)
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Find("").Sort("b","a") → field order reversed, may need Sort step
  3. Find("").Sort("a","-b") → mixed direction, check if index supports

--- Test: TestIndex_Compound_MixedDirections ---
Status: PORTABLE
Concept: Compound index with mixed ascending/descending field directions.
  The index (a ASC, -b DESC) should support sorts matching this pattern.
Inspiration: descidx1.test (descidx1-4.1: CREATE INDEX i3 ON t2(a ASC, b DESC, c ASC);
  descidx1-5: t3i1 ON t3(a DESC, b ASC, c DESC, d ASC))
Operations:
  1. Create collection, EnsureIndex on ["a", "-b"]
  2. Insert docs with varied a,b values
  3. Find("").Sort("a","-b") → should match index, no Sort
  4. Find("").Sort("-a","b") → exact reverse, should be nosort
  5. Find("").Sort("a","b") → direction mismatch, needs Sort

--- Test: TestIndex_Compound_FilterFirstSortSecond ---
Status: PORTABLE
Concept: Filter on first field, sort on second field of compound index.
  After fixing equality on first field, the index provides order on second.
Inspiration: descidx1.test (descidx1-4.5: WHERE a=2 AND b>'two' → uses
  compound index for both filter and implicit sort)
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Insert 50 docs: {id:i, a:i%5, b:i}
  3. Find({"a":2}).Sort("b") → expect all docs where a=2, sorted by b
  4. Explain → IndexScan, no Sort step

--- Test: TestIndex_Compound_FilterAndRangeOnSecond ---
Status: PORTABLE
Concept: Equality on first field + range on second field of compound index.
Inspiration: descidx1.test (descidx1-4.6: WHERE a=2 AND b>='two' → 3 results;
  descidx1-4.8: WHERE a=2 AND b<='two' → 2 results)
Operations:
  1. Create collection, EnsureIndex on ["a", "b"]
  2. Insert docs with a=1..3, b=1..10
  3. Find({"a":2,"b":{"$gte":5}}).Sort("b") → verify correct range results
  4. Find({"a":2,"b":{"$lt":5}}).Sort("b") → verify correct range results

---

## Batch 3 — Unique & Sparse Indexes

### Unique Constraints, Sparse Behavior, and Combinations

--- Test: TestIndex_Unique_InsertDuplicate ---
Status: PORTABLE
Concept: Unique index rejects insertion of duplicate value. The original
  insert should succeed, the duplicate should fail with ErrUniqueConstraint.
Inspiration: index3.test (index3-1.2: CREATE UNIQUE INDEX on non-unique column
  fails with "UNIQUE constraint failed"); index4.test (2.2: CREATE UNIQUE
  INDEX fails when duplicates exist)
Operations:
  1. Create collection, EnsureIndex with Unique=true on "a"
  2. Insert {id:1, a:10} → success
  3. Insert {id:2, a:20} → success
  4. Insert {id:3, a:10} → expect ErrUniqueConstraint
  5. assertCollCount → 2 (duplicate was rejected)
  6. assertIndexLen → 2

--- Test: TestIndex_Unique_UpdateToDuplicate ---
Status: PORTABLE
Concept: Unique index rejects update that would create duplicate value.
Inspiration: index.test (index-19.2: INSERT duplicate fails with constraint)
Operations:
  1. Create collection, EnsureIndex Unique on "a"
  2. Insert {id:1,a:1}, {id:2,a:2}, {id:3,a:3}
  3. UpdateOne({id:2,a:1}) → expect ErrUniqueConstraint
  4. Verify doc id:2 still has a:2 (rollback)

--- Test: TestIndex_Unique_UpdateSameValue ---
Status: PORTABLE
Concept: Updating a document with the same unique value it already has
  should succeed (not a self-conflict).
Inspiration: General unique index semantics
Operations:
  1. Create collection, EnsureIndex Unique on "a"
  2. Insert {id:1,a:1,b:10}
  3. UpdateOne({id:1,a:1,b:20}) → should succeed (same id, same a)
  4. FindId(1) → verify b=20

--- Test: TestIndex_Unique_DeleteAndReinsert ---
Status: PORTABLE
Concept: After deleting a document with a unique value, another document
  with the same value can be inserted.
Inspiration: index.test (general delete + insert patterns)
Operations:
  1. Create collection, EnsureIndex Unique on "a"
  2. Insert {id:1,a:42}
  3. DeleteId(1) → success
  4. Insert {id:2,a:42} → should succeed (slot is free)
  5. assertIndexLen → 1

--- Test: TestIndex_Unique_Compound ---
Status: PORTABLE
Concept: Unique compound index — uniqueness is on the combination of fields,
  not individual fields.
Inspiration: index.test (index-16.4: UNIQUE(c,d), PRIMARY KEY(c,d))
Operations:
  1. Create collection, EnsureIndex Unique on ["a","b"]
  2. Insert {id:1,a:1,b:1} → success
  3. Insert {id:2,a:1,b:2} → success (different b)
  4. Insert {id:3,a:2,b:1} → success (different a)
  5. Insert {id:4,a:1,b:1} → ErrUniqueConstraint (same a+b combo)

--- Test: TestIndex_Unique_OnExistingDuplicates ---
Status: PORTABLE
Concept: Creating a unique index on a collection that already has duplicate
  values should fail. The collection should remain unchanged.
Inspiration: index3.test (index3-1.2: BEGIN; CREATE UNIQUE INDEX i1 ON t1(a)
  → fails with "UNIQUE constraint failed: t1.a"); index4.test (2.2: CREATE
  UNIQUE INDEX on table with duplicates fails)
Operations:
  1. Create collection, insert {id:1,a:5} and {id:2,a:5}
  2. EnsureIndex Unique on "a" → expect error
  3. Verify no index was created: GetIndexes() → empty
  4. Verify data is intact: Count → 2

--- Test: TestIndex_Sparse_MissingField ---
Status: PORTABLE
Concept: Sparse index skips documents that don't have the indexed field.
  Only documents with the field present get indexed.
Inspiration: General sparse index semantics (our API supports this natively)
Operations:
  1. Create collection, EnsureIndex Sparse=true on "a"
  2. Insert {id:1,a:10}, {id:2,b:20}, {id:3,a:30}, {id:4,c:40}
  3. assertIndexLen → 2 (only docs with "a" field)
  4. Find({"a":10}) → 1 result
  5. Count all → 4

--- Test: TestIndex_Sparse_NullField ---
Status: PORTABLE
Concept: Sparse index skips documents where the indexed field is null.
  Null is treated the same as missing for sparse indexes.
  DIVERGENCE FROM MongoDB (intentional): MongoDB indexes present-but-null
  values in a sparse index and only skips missing fields; we skip both.
  This keeps sparse indexing consistent with our query matching, where
  {field:null} matches both a null and a missing field. Consequence: the
  planner uses a sparse index only when the query guarantees every indexed
  field is present AND non-null (see qplanner.sparseIndexComplete /
  query.GuaranteesPresence); a field constrained solely by {$exists:true}
  is therefore NOT enough to make a sparse index eligible here.
Inspiration: General sparse semantics (our code checks for null in writeValues)
Operations:
  1. Create collection, EnsureIndex Sparse=true on "a"
  2. Insert {id:1,a:null}, {id:2,a:10}, {id:3}
  3. assertIndexLen → 1 (only {id:2,a:10})

--- Test: TestIndex_Sparse_UpdateFieldAppears ---
Status: PORTABLE
Concept: When a document is updated to add the sparse field, it should be
  added to the index. When the field is removed, it should be removed.
Inspiration: General index maintenance + sparse behavior
Operations:
  1. Create collection, EnsureIndex Sparse=true on "a"
  2. Insert {id:1,b:5} → assertIndexLen → 0
  3. UpdateOne({id:1,a:10,b:5}) → assertIndexLen → 1
  4. UpdateOne({id:1,b:5}) → assertIndexLen → 0 (field removed)

--- Test: TestIndex_Sparse_Unique ---
Status: PORTABLE
Concept: Combination of sparse + unique. Multiple documents can lack the
  field (all skipped in index), but among documents that have it,
  uniqueness must hold.
Inspiration: Combined feature test
Operations:
  1. Create collection, EnsureIndex Sparse=true, Unique=true on "a"
  2. Insert {id:1}, {id:2}, {id:3} → all succeed (no "a" field, sparse skips)
  3. Insert {id:4,a:10} → success
  4. Insert {id:5,a:10} → ErrUniqueConstraint
  5. Insert {id:6,a:20} → success
  6. assertIndexLen → 2 (only docs with "a")

--- Test: TestIndex_Sparse_CompoundBothMissing ---
Status: PORTABLE
Concept: Sparse compound index — if ANY indexed field is null/missing,
  the document is not indexed.
Inspiration: index_test.go fillKeysCases "two fields sparse"
Operations:
  1. Create collection, EnsureIndex Sparse=true on ["a","b"]
  2. Insert {id:1,a:1} → not indexed (b missing)
  3. Insert {id:2,b:2} → not indexed (a missing)
  4. Insert {id:3,a:1,b:2} → indexed
  5. Insert {id:4,a:null,b:2} → not indexed (a is null)
  6. assertIndexLen → 1

--- Test: TestIndex_NonSparse_NullIndexed ---
Status: PORTABLE
Concept: Non-sparse index DOES index documents with null/missing fields.
  The null value is stored as a key in the index.
Inspiration: index.test (index-14.1: compound index with NULL values,
  NULL sorts to a specific position); index_test.go fillKeysCases showing
  null key generation
Operations:
  1. Create collection with non-sparse index on "a"
  2. Insert {id:1,a:null}, {id:2}, {id:3,a:5}
  3. assertIndexLen → 3

--- Test: TestIndex_IndexLenAfterMutations ---
Status: PORTABLE
Concept: Verify index length (entry count) remains consistent through a
  series of inserts, updates, and deletes.
Inspiration: index_test.go TestIndex_Insert/Update/Delete patterns
Operations:
  1. Create collection with index on "a"
  2. Insert 5 docs → assertIndexLen(5)
  3. Delete 2 docs → assertIndexLen(3)
  4. Update 1 doc (change a value) → assertIndexLen(3) (old removed, new added)
  5. Insert 3 more → assertIndexLen(6)
  6. Delete all → assertIndexLen(0)

---

## OUT_OF_SCOPE Tests (for reference)

--- Test: index.test index-5.1 (indexing sqlite_master) ---
Status: OUT_OF_SCOPE
Reason: SQLite internal table restriction, not applicable to our system.

--- Test: index.test index-6.x (duplicate index names) ---
Status: OUT_OF_SCOPE
Reason: Our index naming is auto-generated from fields; SQL-specific naming conflicts.

--- Test: index.test index-12.x (numeric string comparison in indexes) ---
Status: OUT_OF_SCOPE
Reason: SQL type affinity / numeric coercion — our system uses typed JSON values.

--- Test: index.test index-15.x (scientific notation in index) ---
Status: OUT_OF_SCOPE
Reason: SQL numeric affinity — we use native JSON number types.

--- Test: index.test index-16..17 (duplicate constraint single index) ---
Status: OUT_OF_SCOPE
Reason: SQL schema constraint deduplication — not applicable to our API.

--- Test: index.test index-18 (sqlite_ prefix restriction) ---
Status: OUT_OF_SCOPE
Reason: SQLite naming restriction.

--- Test: index.test index-19 (ON CONFLICT policy) ---
Status: OUT_OF_SCOPE
Reason: SQL conflict resolution policy — we have simple error or success.

--- Test: index.test index-20..21 (TEMP index, quoted names) ---
Status: OUT_OF_SCOPE
Reason: SQL-specific features.

--- Test: index.test index-22..23 (expression indexes) ---
Status: OUT_OF_SCOPE
Reason: We don't support expression indexes.

--- Test: index3.test index3-2.x (string vs identifier column names) ---
Status: OUT_OF_SCOPE
Reason: SQL parser backward compatibility, not applicable.

--- Test: index3.test index3-99 (corrupt schema) ---
Status: OUT_OF_SCOPE
Reason: Schema corruption testing — covered by our separate corrupt tests.

--- Test: index4.test 1.x (large blob index, memory limits) ---
Status: OUT_OF_SCOPE
Reason: SQLite memory management specific.

--- Test: descidx2.test (file format downgrade ignoring DESC) ---
Status: OUT_OF_SCOPE
Reason: SQLite file format version behavior.

--- Test: descidx3.test (mixed type sorting with blobs) ---
Status: OUT_OF_SCOPE
Reason: SQLite type affinity sorting (integer < text < blob).
  Our JSON values have different type ordering rules.
