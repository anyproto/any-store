# Index Corruption & Recovery — Research Findings

## Overview

This document summarizes research into SQLite's corruption test files and how their index corruption patterns can be adapted for our system (any-store-custom-btree). Our system uses btree namespaces for indexes where:
- Non-unique index: key = Tuple(values..., docId), val = nil
- Unique index: key = Tuple(values...), val = docId
- `EnsureIndex` rebuilds from scratch (acts as REINDEX)
- `IntegrityCheck` validates btree structure at the page level

---

## SQLite Test Files Analyzed

### corruptG.test — Index Header/Payload Corruption
**What it tests:**
1. Corrupt the header-size varint in an index B-tree cell so it becomes a negative number. Then try to query the index (SELECT ... WHERE a > 'abc'). Expected: "database disk image is malformed".
2. Corrupt a serial_type field in the index record header to indicate a huge payload that overflows the buffer. Expected: malformed error on index seek.
3. PRAGMA integrity_check should also detect the corruption.

**How corruption is introduced:** Direct hex writes to the index root page, targeting cell payload header fields.

**Adaptation for our system:**
- **Concept: Corrupt index key encoding** — Modify the raw bytes of an index entry's tuple key in the index namespace to produce an invalid/oversized varint or malformed tuple.
- **Test:** Insert docs, create index, corrupt a cell in the index namespace page, then try to iterate the index or do an integrity check.

### corruptI.test — Corrupt Index Cell Payload
**What it tests:**
1. Tests 1.x: Create table + index, corrupt the first cell's content on the index page to have a bad varint (7f06), then query via the index. First corruption is benign (query returns empty), second corruption (FFFF7f02) triggers malformed error.
2. Tests 2.x: Similar approach — create table + index, corrupt payload in index leaf, query with numeric comparison. Expected: malformed.
3. Test 7.x: Remove the sqlite_master entry for an autoindex (PRIMARY KEY index). Then try UPDATE — gets "malformed" because the system detects missing index metadata.

**How corruption is introduced:** hex writes to index page cells, targeting varint size fields or record content.

**Adaptation for our system:**
- **Concept: Stale/orphaned index namespace** — Delete the index registration from system metadata but leave the index namespace. Or vice versa: register an index but corrupt/delete its namespace.
- **Concept: Corrupt cell content in index namespace** — Directly write invalid bytes to cells on index namespace pages.

### corruptE.test — Rowid Order Corruption (with index)
**What it tests:** Creates a table with an index (CREATE INDEX t1i1 ON t1(x)), then corrupts single bytes at various offsets in the database file. Each corruption is expected to cause `PRAGMA integrity_check` to report "out of order" errors, meaning the B-tree key ordering invariant is violated.

**How corruption is introduced:** Single-byte overwrites at specific file offsets (targeting both table and index pages).

**Adaptation for our system:**
- **Concept: Key order violation in index namespace** — Swap cell pointers on an index namespace page so keys are out of order. IntegrityCheck should detect this.
- We already have `TestIntegrityCheckKeyOrder` for data namespaces; this would extend it to index namespaces.

### corrupt2.test — Index B-Tree Page Type Corruption
**What it tests:**
1. Tests 7.x: Create table + index. Set the page-flags of an index leaf page to 0x0D (table leaf instead of index leaf). Query the index with ORDER BY — gets malformed.
2. Corrupt the page-header of an index leaf page (cell count field set to 0xFFFF). Query with ORDER BY DESC — gets malformed.
3. Tests 8.x: Set page-flags of a table leaf to 0x0A (index leaf). Query by rowid — gets malformed.

**How corruption is introduced:** Direct writes to page type bytes and page header fields.

**Adaptation for our system:**
- **Concept: Wrong page type on index pages** — Change the page type byte of an index namespace page. IntegrityCheck should catch this.
- **Concept: Corrupt cell count in index page header** — Set nCell to an absurd value on an index page.

### corrupt9.test — Freelist Corruption Affecting Index Rebuild
**What it tests:** Duplicates entries on the freelist, then tries to CREATE INDEX + REINDEX. The freelist corruption causes the index rebuild to fail because it allocates already-used pages.

**How corruption is introduced:** Overwriting freelist trunk page entries with duplicates.

**Adaptation for our system:**
- **Concept: Freelist corruption during index build** — If we corrupt the freelist and then try EnsureIndex (which does a full rebuild), the allocation of new pages for the index namespace may fail or produce corruption.
- We already have `TestIntegrityCheckCorruptFreelist` — this extends it to the "rebuild triggers error" scenario.

### corruptH.test — Cross-linked Pages (table root shared with another table)
**What it tests:** Makes the root page of table t1 appear as a leaf page in table t2. Then iterates t1 while deleting from t2, causing the page to be modified under t1's cursor.

**Adaptation for our system:**
- **Concept: Cross-namespace page sharing** — If an index namespace page is also referenced by the data namespace (or vice versa), operations on one corrupt the other.
- This is very low-level and may not be directly testable without pager-level manipulation.

### corruptM.test — Schema Corruption (index metadata)
**What it tests:** Corrupts sqlite_master entries — changing the `tbl_name` field for an index entry, changing the `type` field, etc. Tests that the system detects schema inconsistencies.

**Adaptation for our system:**
- **Concept: Index metadata inconsistency** — Register an index in our system namespace with wrong field information, then try operations.

### reindex.test — Collation Change + REINDEX
**What it tests:**
1. Basic REINDEX on tables/indexes.
2. Changes a collation function, then verifies integrity_check fails (because index is now out of order with new collation). REINDEX fixes it.
3. REINDEX with missing collation sequence.

**Adaptation for our system:**
- **Concept: Index rebuild fixes ordering** — This maps to our EnsureIndex (drop + recreate). If index entries are out of order (simulated by direct page manipulation), dropping and re-creating the index via EnsureIndex should fix it.

---

## Recommended Test Categories for Batch 16

Based on the research above, here are the test categories adapted for our system:

### Category 1: Index-Data Inconsistency (Stale Index Entries)
**Concept:** Index points to documents that no longer exist, or documents exist but have no index entry.

**Tests:**
1. **StaleIndexEntry** — Insert docs, create index. Then directly delete a doc from the data namespace (bypassing index cleanup). Query via index — the index entry points to a deleted doc.
2. **MissingIndexEntry** — Insert docs, create index. Then directly delete an entry from the index namespace. The doc exists but the index doesn't know about it. A query that should find the doc via index misses it.
3. **IndexPointsToWrongDoc** — Insert docs, create unique index. Modify the value (docId) stored in the index entry to point to a different doc.

### Category 2: Index Key Order Corruption
**Concept:** The B-tree ordering invariant in the index namespace is violated.

**Tests:**
4. **IndexKeyOrderCorrupted** — Create an index, then swap cell pointers on the index root page. IntegrityCheck should detect "key out of order".
5. **IndexRebuildFixesOrder** — After corrupting key order, drop and re-create the index (EnsureIndex). Verify the index is now correct and IntegrityCheck passes.

### Category 3: Index Page-Level Corruption
**Concept:** Corrupt the btree page structure within the index namespace.

**Tests:**
6. **IndexPageTypeCorrupted** — Change the page type byte on an index namespace page. IntegrityCheck should catch it.
7. **IndexCellPointerCorrupted** — Set a cell pointer on an index page to an out-of-range value. IntegrityCheck detects it.
8. **IndexCellContentCorrupted** — Write garbage bytes into a cell on an index page. Operations on the index should error or IntegrityCheck should fail.

### Category 4: Index Namespace Existence Corruption
**Concept:** Mismatch between index registration and actual namespace.

**Tests:**
9. **IndexNamespaceMissing** — Register an index in metadata but delete its namespace. Operations that use the index should handle this gracefully.
10. **OrphanedIndexNamespace** — Create an index, then remove its registration from system metadata (but leave the namespace). The system should not use the orphaned index.

### Category 5: Index Recovery via EnsureIndex
**Concept:** EnsureIndex should rebuild a corrupted or inconsistent index from scratch.

**Tests:**
11. **EnsureIndexRecoversFromStaleEntries** — Create stale index entries (extra entries in index that don't match docs), then call DropIndex + EnsureIndex. Verify correctness.
12. **EnsureIndexRecoversFromMissingEntries** — Delete some index entries directly, then rebuild. Verify all docs are now indexed.
13. **EnsureIndexAfterPageCorruption** — Corrupt an index page, drop the index, recreate via EnsureIndex. Verify IntegrityCheck passes and queries work.

### Category 6: Corruption During Index Operations
**Concept:** What happens if we corrupt things mid-operation.

**Tests:**
14. **InsertWithCorruptedIndex** — Corrupt an index page, then try to insert a new doc. The insert should either succeed (ignoring the corrupt index entry) or fail gracefully.
15. **DeleteWithCorruptedIndex** — Corrupt an index entry, then try to delete the corresponding doc. Should handle gracefully.

---

## Implementation Notes

### How to Introduce Corruption in Tests
Our existing integrity tests (integrity_test.go) show the pattern:
1. Get a writable page via `db.pager.getWritablePage(pageNum)`
2. Modify `pg.data[offset]` directly
3. Commit the transaction
4. Run IntegrityCheck or operations

For index-specific corruption:
- Get the index namespace root page via `idx.ns.rootPage`
- Use the pager to get writable pages within that namespace's tree
- Modify cell pointers, page type bytes, or cell content

### For data-index inconsistency tests:
- Use btree-level WriteTx to directly Put/Delete in the index namespace (bypassing the collection layer)
- Or use btree-level WriteTx to delete from data namespace (bypassing index cleanup)

### Key APIs available:
- `tx.Put(ns, key, val)` / `tx.Delete(ns, key)` — direct namespace operations
- `db.pager.getWritablePage(pgno)` — for page-level corruption
- `db.IntegrityCheck()` — structural validation
- `coll.EnsureIndex(ctx, info)` — rebuild index from scratch
- `coll.DropIndex(ctx, name)` — remove index
- `tx.NewCursor(ns)` — iterate namespace entries

### Important considerations:
- Our IntegrityCheck validates btree structure (page types, cell pointers, key order, freelist) but does NOT cross-check index entries against data entries. So stale/missing index entry tests should focus on query correctness, not IntegrityCheck.
- EnsureIndex drops and recreates, which is the recovery mechanism.
- Page-level corruption tests should use IntegrityCheck.
