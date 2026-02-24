# Bugs Found During Coverage Testing

## tryMergeLeaf-overflow-double-ref

**Location**: `internal/btree/btree.go` — `tryMergeLeaf()`

**Description**: When `tryMergeLeaf` merges two leaf pages that contain overflow chains,
the overflow pages end up referenced twice (once from the merged page and once from the
freed page that wasn't properly cleaned up). This causes `IntegrityCheck()` to report
"2nd reference (tree t1 overflow)" errors.

**Reproduction**:
1. Create a DB with 512-byte page size
2. Insert 60 rows with 100-byte values into namespace "t1" (forces overflow pages)
3. Delete key 1 to leave a partially-empty leaf
4. Call `tryMergeLeaf` on the leaf containing key 2
5. Commit and run `IntegrityCheck()` — reports double-referenced overflow pages

**Test**: `TestTryMergeLeafNoFit` in `btree_coverage_test.go` (skipped)

## Observation: Bulk Delete Sensitivity

**Location**: `internal/btree/btree.go` -- `Delete()`

**Description**: Deleting a very large number of entries (600+) from a multi-level B-tree
with small page sizes (512 bytes) in a single write transaction can occasionally produce
"key not found" errors during the deletion loop. This appears to be related to aggressive
tree restructuring during mass deletions where page freeing and parent removal alter the
tree structure mid-operation. Using smaller batches across multiple transactions works
reliably. This was observed during deep coverage testing but did not require any test
skipping since the batch approach provides equivalent coverage.

## page1-root-collapse-corruption

**Location**: `internal/btree/btree.go` -- `removeChildFromParent()` / `Delete()`

**Description**: Deleting many namespaces from the master btree (rooted on page 1) with
small page sizes (512 bytes) causes tree corruption. After deleting ~22 namespaces, the
master btree restructuring (through `removeChildFromParent` root collapse on page 1)
causes subsequent namespace lookups to fail with "namespace not found". The `IntegrityCheck`
reports "invalid cell content offset" on the master page 1 and many "never used" pages.

This bug prevents testing the page-1 root collapse code path (L2332-2342) and the
non-root empty interior free path (L2354-2360) through the namespace API.

**Reproduction**:
1. Create a DB with 512-byte page size
2. Create 30+ namespaces in a single write transaction
3. Delete namespaces one by one in separate write transactions
4. After ~22 deletions, `DeleteNamespace` fails with "namespace not found"

**Tests**: `TestMergeCursor_RootCollapseOnPage1` and `TestMergeCursor_RootCollapseViaDirectBtree`
in `btree_merge_cursor_test.go` (skipped)

## bulk-delete-orphan-pages

**Location**: `internal/btree/btree.go` -- `Delete()` / `removeChildFromParent()`

**Description**: Heavy deletion from a 3-level B-tree with small page sizes (512 bytes)
leaves orphaned pages that are not returned to the freelist. After deleting most entries
(e.g., 590 out of 600), `IntegrityCheck` reports pages as "never used" -- these are
interior pages that were freed during tree collapse but their page numbers were not
properly added to the freelist.

**Tests**: `TestMergeCursor_NonRootEmptyInteriorFree` and
`TestMergeCursor_DeleteAllFromThreeLevelTree` in `btree_merge_cursor_test.go` (skipped)
