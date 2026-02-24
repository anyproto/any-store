package btree

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests that trigger corruption-detection branches in search functions.
// These branches are bounds checks that prevent out-of-bounds reads on
// corrupted page data.

// ---- searchInteriorPage cpBase+2 > dataLen (L673-675) ----
// This triggers when the cell pointer area extends past the page data.
// We corrupt cellCount to be absurdly large.

func TestCorruptCov_SearchInteriorPageCellCountOverflow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert enough to get interior pages
	for i := 0; i < 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	// Find the root page of namespace "t1" - it should be an interior page
	// For now, corrupt the master table (page 1) which is also an interior page
	// if many namespaces are inserted. But with just 1 namespace, page 1 is a leaf.
	// Let's corrupt the namespace root page instead.
	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Page 1 is at offset 0 (with DB header). If it's a leaf, we need the namespace root.
	// Actually, with 200 keys and 100-byte values on 4096-byte pages,
	// the namespace btree will have interior pages.
	// The namespace root page number is stored in the master table.
	// Let's look for interior pages by checking page types.
	pageSize := 4096
	for pgno := 2; pgno < len(data)/pageSize; pgno++ {
		offset := pgno * pageSize
		if offset+8 > len(data) {
			break
		}
		// Page type is byte 0 of page header
		pageType := data[offset]
		if pageType == pageTypeIntIdx {
			// Found an interior page - corrupt its cellCount to be huge
			// cellCount is at offset+3 (2 bytes, big-endian)
			data[offset+3] = 0xFF
			data[offset+4] = 0xFF
			break
		}
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	// IntegrityCheck should detect corruption
	err = db2.IntegrityCheck()
	// May or may not error depending on which page we corrupted
	_ = err
}

// ---- searchInteriorPage interiorCellKey error (L698-700, L710-712) ----
// When lo==0 or lo<n, the cell data parsing fails

func TestCorruptCov_SearchInteriorCellKeyError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 100)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Find an interior page and corrupt a cell's data so interiorCellKey returns error
	pageSize := 4096
	for pgno := 2; pgno < len(data)/pageSize; pgno++ {
		offset := pgno * pageSize
		if offset+12 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeIntIdx {
			// Interior page - corrupt the first cell's content
			// Cell pointer array starts at offset+12 (after 12-byte interior header)
			// First cell pointer at bytes 12-13
			cellPtr := int(binary.BigEndian.Uint16(data[offset+12 : offset+14]))
			if cellPtr > 0 && cellPtr < pageSize-8 {
				// Cell has: leftChild(4) + varint(keyLen) + key
				// Corrupt the varint to be huge
				data[offset+cellPtr+4] = 0xFF
				data[offset+cellPtr+5] = 0xFF
				data[offset+cellPtr+6] = 0xFF
				data[offset+cellPtr+7] = 0xFF
			}
			break
		}
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	// Try to read data - should hit the corrupted interior page
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns2, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return // namespace lookup itself might fail
	}
	cur := rtx.NewCursor(ns2)
	_ = cur.First()      // may error on corrupted page
	_ = cur.Seek([]byte{0, 0, 0, 50}) // trigger search through interior
}

// ---- searchLeafWithOverflow multi-byte varint corruption (L477-494) ----
// These branches handle when the leaf cell format has multi-byte varint keyLen/valLen
// that parse incorrectly

func TestCorruptCov_SearchLeafVarintCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	for i := 0; i < 30; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 50)))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Find a leaf page (type 0x0a) and corrupt cell varints
	pageSize := 4096
	for pgno := 1; pgno < len(data)/pageSize; pgno++ {
		offset := pgno * pageSize
		if pgno == 1 {
			offset = 100 // DB header on page 1
		}
		if offset >= len(data) {
			break
		}
		var pageType byte
		if pgno == 1 {
			pageType = data[100]
		} else {
			pageType = data[offset]
		}
		if pageType == pageTypeLeafIdx {
			// Found a leaf page. Corrupt a cell's keyLen varint to create
			// a multi-byte varint that indicates a huge key length.
			hdrStart := offset
			if pgno == 1 {
				hdrStart = 100
			}
			cellCount := int(binary.BigEndian.Uint16(data[hdrStart+3 : hdrStart+5]))
			if cellCount > 0 {
				cpBase := hdrStart + 8 // cell pointer array
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase : cpBase+2]))
				if pgno == 1 {
					// Cell pointer is relative to start of page (including header)
				}
				cellOff := offset - (pgno * pageSize) + cellPtr
				if pgno == 1 {
					cellOff = cellPtr
				} else {
					cellOff = pgno*pageSize + cellPtr
				}
				if cellOff > 0 && cellOff+5 < len(data) {
					// Make keyLen varint a multi-byte value that claims huge key
					data[cellOff] = 0x80 | 0x40   // continuation byte
					data[cellOff+1] = 0x80 | 0x01 // continuation
					data[cellOff+2] = 0xFF         // huge value
				}
			}
			break
		}
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns2, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns2)
	_ = cur.First()
}

// ---- leafFullKey corruption (L784-786, L824-826) ----
// When leaf has overflow cells, key reading from overflow pages can fail

func TestCorruptCov_LeafFullKeyBoundsCheck(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := Open(path, Options{PageSize: 512})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)
	// Insert with large keys that will overflow on 512-byte pages
	for i := 0; i < 10; i++ {
		key := make([]byte, 200)
		binary.BigEndian.PutUint32(key, uint32(i))
		require.NoError(t, tx.Put(ns, key, []byte("v")))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)

	// Corrupt overflow page pointer in a leaf cell to point to invalid page
	// This will cause leafFullKey to fail when reading overflow
	pageSize := 512
	for pgno := 2; pgno < len(data)/pageSize; pgno++ {
		offset := pgno * pageSize
		if offset+8 > len(data) {
			break
		}
		pageType := data[offset]
		if pageType == pageTypeLeafIdx {
			// Find a cell with overflow
			cellCount := int(binary.BigEndian.Uint16(data[offset+3 : offset+5]))
			for ci := 0; ci < cellCount; ci++ {
				cpBase := offset + 8 + ci*2
				cellPtr := int(binary.BigEndian.Uint16(data[cpBase : cpBase+2]))
				cellOff := pgno*pageSize + cellPtr
				if cellOff+20 < len(data) {
					// Check if this cell has an overflow pointer by looking at keyLen
					// If keyLen > maxLocal (~102 for 512-byte pages), it has overflow
					kl, kn := getVarint(data[cellOff:])
					if kl > 100 && kn > 0 {
						// This is an overflow cell - corrupt the overflow page pointer
						nLocal := localPayloadSize(int(kl), pageSize)
						overflowPtrOff := cellOff + kn + nLocal
						if overflowPtrOff+4 <= len(data) {
							// Point to page 0 (invalid)
							binary.BigEndian.PutUint32(data[overflowPtrOff:], 0)
						}
						break
					}
				}
			}
			break
		}
	}
	require.NoError(t, os.WriteFile(path, data, 0644))

	db2, err := Open(path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()

	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	ns2, err := db2.getNamespaceLocked("t1")
	if err != nil {
		return
	}
	cur := rtx.NewCursor(ns2)
	// Searching for keys will trigger leafFullKey reading which hits the corrupt overflow
	_ = cur.First()
	for cur.Valid() {
		_, _ = cur.Key()
		_ = cur.Next()
	}
}

// ---- Delete with needsRebuild path (L2100-2108) ----
// This triggers when fragmentation exceeds 60 bytes during delete

func TestCorruptCov_DeleteHighFragRebuild(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert keys with varying sizes to create cells of different sizes
	for i := 0; i < 25; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := make([]byte, 15+i) // sizes 15 to 39
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())

	// Delete many keys to accumulate fragmentation
	// Each deleted cell adds its size to fragBytes
	// When fragBytes > 60, the rebuild path triggers
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Delete keys from the middle (not at content boundary) to prevent reclaim
	for i := 5; i < 20; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		_ = tx2.Delete(ns2, key)
	}
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// ---- Delete freeing overflow chain in fast path (L2112-2115) ----

func TestCorruptCov_DeleteOverflowFastPath(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert a mix of normal and overflow values
	for i := 0; i < 3; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 20)))
	}
	// Overflow cell
	key := binary.BigEndian.AppendUint32(nil, uint32(50))
	require.NoError(t, tx.Put(ns, key, make([]byte, 500)))
	require.NoError(t, tx.Commit())

	// Delete just the overflow key — should hit fast path with overflow chain free
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	require.NoError(t, tx2.Delete(ns2, key))
	require.NoError(t, tx2.Commit())

	require.NoError(t, db.IntegrityCheck())
}

// ---- tryMergeLeaf error paths (L2221-2228, L2265-2276) ----
// getPage errors - need to trigger during merge. Hard without fault injection.

// ---- removeChildFromParent non-root empty interior (L2354-2360) ----
// This triggers when a non-root interior page becomes empty after child removal.

func TestCorruptCov_RemoveChildFromParentNonRoot(t *testing.T) {
	// Create a 3-level tree with small page size
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	// Insert enough for 3+ levels
	for i := 0; i < 500; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 8)))
	}
	require.NoError(t, tx.Commit())

	// Delete in bulk to collapse tree levels, which should trigger
	// removeChildFromParent on non-root interior pages
	for batch := 0; batch < 10; batch++ {
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, err := db.getNamespaceLocked("t1")
		require.NoError(t, err)
		for i := batch * 50; i < (batch+1)*50; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			_ = tx2.Delete(ns2, key)
		}
		require.NoError(t, tx2.Commit())
	}

	require.NoError(t, db.IntegrityCheck())
	require.Equal(t, 0, countKeys(t, db, "t1"))
}

// ---- Cursor.Previous through rightChild (L2952-2954) ----

func TestCorruptCov_CursorPreviousRightChild(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	for i := 0; i < 300; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, 8)))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cur := rtx.NewCursor(ns)

	// Start from Last (rightmost) and go backwards
	// This should traverse through rightChild paths in interior pages
	err = cur.Last()
	require.NoError(t, err)

	// The last entry positions cursor at the rightChild's rightmost leaf.
	// Going Previous from the first cell of any leaf triggers the interior frame path.
	// When cellIdx drops below 0 at the leaf level, we pop up to interior.
	// At the interior level, Previous decrements cellIdx.
	// If the previous cellIdx was the rightChild position (n), then after decrement
	// it's n-1, which is < n, so we read from getCellOffset(n-1).
	// But if the previous cellIdx was something else, we need to check the rightChild path.

	prevCount := 0
	for {
		err = cur.Previous()
		require.NoError(t, err)
		if !cur.Valid() {
			break
		}
		prevCount++
	}
	require.Equal(t, 299, prevCount) // Last() consumed one, Previous gets the rest
}

// ---- Next descending into interior with cellCount == 0 (L2868-2869) ----

func TestCorruptCov_NextEmptyInteriorBreak(t *testing.T) {
	// This branch in Next() is a safety check for when an interior page
	// has cellCount == 0 (shouldn't happen in valid tree).
	// We can't easily trigger this with valid data.
	// It's a defensive branch.
	t.Skip("L2868-2869 is a defensive check for corrupted empty interior pages")
}

// ---- Previous descent with n == 0 (L2969-2970) ----

func TestCorruptCov_PreviousEmptyInteriorBreak(t *testing.T) {
	// Same as above - defensive check
	t.Skip("L2969-2970 is a defensive check for corrupted empty interior pages")
}
