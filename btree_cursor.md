# btree Cursor: Add SeekNear and SeekExact

**Scope:** Only `internal/btree/btree.go` and `internal/btree/btree_test.go`. No changes outside the btree package.

## Problem

`Cursor.Seek(key)` always clears the cursor stack and traverses from the B-tree root to a leaf (lines 2156-2201 in `btree.go`). This means repeated point lookups for nearby keys (e.g., docIds on the same leaf page) each pay the full O(depth) traversal cost, even though the cursor already has the target leaf pinned in memory.

## New Methods (add after `Seek` at line 2201)

### `Cursor.SeekNear(key []byte) error`

Positions cursor at the first key >= `key`, but checks the currently-pinned leaf page first.

**Fast path** (when cursor is valid and has a pinned leaf):
1. Extract first key (cell 0) and last key (cell `cellCount-1`) from the pinned leaf page
2. If `key >= firstKey && key <= lastKey`: call `searchLeafPage(pg, key)` to binary search within the leaf (pure in-memory, no I/O), reposition `cellIdx`
3. Handle edge: if `idx == cellCount` (key sorts after last entry), advance to next leaf via `c.Next()` — same pattern as `Seek` line 2197
4. If key is outside the leaf's range, fall through to `c.Seek(key)`

**Slow path**: `return c.Seek(key)`

Uses existing `searchLeafPage()` (line 350). Needs a private helper `leafKeyAt(pg *page, idx int) ([]byte, error)` to extract a key at a given cell index using `parseLeafCellWithSize`.

### `Cursor.SeekExact(key []byte) error`

Convenience method: calls `SeekNear(key)`, then verifies `Key()` matches exactly. Returns `ErrKeyNotFound` if cursor is invalid or key doesn't match.

```go
func (c *Cursor) SeekExact(key []byte) error {
    if err := c.SeekNear(key); err != nil {
        return err
    }
    if !c.valid {
        return ErrKeyNotFound
    }
    k, err := c.Key()
    if err != nil {
        return err
    }
    if !bytes.Equal(k, key) {
        return ErrKeyNotFound
    }
    return nil
}
```

Note: No `AppendValue` method needed — `Cursor.Value()` already returns a zero-copy slice into the pinned page buffer for non-overflow values.

## Tests (add to `btree_test.go`)

- **`TestCursor_SeekNear`**: Insert N keys, verify SeekNear returns the same positioned key as Seek for every key. Test cases:
  - Key exists on current leaf (fast path)
  - Key exists on different leaf (fallback to Seek)
  - Key not found, returns first key >= target (same as Seek semantics)
  - Empty tree
  - Key before first key in tree
  - Key after last key in tree

- **`TestCursor_SeekExact`**: Verify returns nil for existing keys, `ErrKeyNotFound` for missing keys.

- **`TestCursor_SeekNear_SameLeaf`**: Insert dense keys, call SeekNear for multiple keys on the same leaf page. Verify the fast path works by checking that consecutive SeekNear calls reuse the same leaf (stack length doesn't change).

## Key Existing Code References

- `Cursor` struct: `btree.go:2044-2048`
- `cursorFrame` struct: `btree.go:2050-2054`
- `Cursor.Seek()`: `btree.go:2156-2201`
- `Cursor.Next()`: `btree.go:2278-2363`
- `Cursor.Key()`: `btree.go:2206-2224`
- `Cursor.Value()`: `btree.go:2231-2276`
- `searchLeafPage()`: `btree.go:350-397`
- `parseLeafCellWithSize()`: used throughout for cell parsing

## Verification

```
go test -count=1 -timeout 120s ./internal/btree/
go test -race -count=1 -timeout 120s ./internal/btree/
```
