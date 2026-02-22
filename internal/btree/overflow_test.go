package btree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverflowPutGet(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Create a 10KB value (much larger than maxLocal ~1001 bytes for 4KB pages)
	bigValue := bytes.Repeat([]byte("ABCDEFGHIJ"), 1024) // 10KB

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("bigkey"), bigValue))
	require.NoError(t, tx.Commit())

	// Read it back
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	got, err := rtx.Get(ns3, []byte("bigkey"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, rtx.Rollback())
	require.NoError(t, db.IntegrityCheck())
	_ = ns
}

func TestOverflowUpdateLargeToSmall(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	bigValue := bytes.Repeat([]byte("X"), 5000)
	smallValue := []byte("tiny")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	// Insert large value
	require.NoError(t, tx.Put(ns2, []byte("key"), bigValue))

	// Update to small value (should free overflow pages)
	require.NoError(t, tx.Put(ns2, []byte("key"), smallValue))

	got, err := tx.Get(ns2, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, smallValue, got)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
	_ = ns
}

func TestOverflowUpdateSmallToLarge(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	smallValue := []byte("tiny")
	bigValue := bytes.Repeat([]byte("Y"), 8000)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	require.NoError(t, tx.Put(ns2, []byte("key"), smallValue))
	require.NoError(t, tx.Put(ns2, []byte("key"), bigValue))

	got, err := tx.Get(ns2, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
	_ = ns
}

func TestOverflowDelete(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	bigValue := bytes.Repeat([]byte("Z"), 10000)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("bigkey"), bigValue))
	require.NoError(t, tx.Commit())

	// Delete the key (should free overflow pages)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Delete(ns3, []byte("bigkey")))
	require.NoError(t, tx2.Commit())

	// Verify it's deleted
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")
	_, err = rtx.Get(ns4, []byte("bigkey"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, rtx.Rollback())

	// Verify overflow pages were freed (freelist should have entries)
	assert.True(t, db.pager.header.TotalFreelistPgs > 0)
	_ = ns
}

func TestOverflowCursor(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert a mix of normal and overflow values
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	for i := range 20 {
		k := fmt.Appendf(nil, "key-%02d", i)
		var v []byte
		if i%3 == 0 {
			// Large value (overflow)
			v = bytes.Repeat([]byte(fmt.Sprintf("%02d", i)), 3000)
		} else {
			// Small value (no overflow)
			v = fmt.Appendf(nil, "val-%02d", i)
		}
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Iterate with cursor and verify all values
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns3)

	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		v, verr := cur.Value()
		require.NoError(t, verr)

		i := count
		expectedKey := fmt.Appendf(nil, "key-%02d", i)
		assert.Equal(t, expectedKey, k)

		if i%3 == 0 {
			expectedVal := bytes.Repeat([]byte(fmt.Sprintf("%02d", i)), 3000)
			assert.Equal(t, expectedVal, v, "overflow value mismatch for key %s", k)
		} else {
			expectedVal := fmt.Appendf(nil, "val-%02d", i)
			assert.Equal(t, expectedVal, v)
		}
		count++
	}
	assert.Equal(t, 20, count)
	require.NoError(t, rtx.Rollback())
	require.NoError(t, db.IntegrityCheck())
	_ = ns
}

func TestOverflowPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	bigValue := bytes.Repeat([]byte("PERSIST"), 2000) // 14KB

	// Write and checkpoint
	db, err := Open(path, DefaultOptions())
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("bigkey"), bigValue))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	// Reopen and verify
	db2, err := Open(path, DefaultOptions())
	require.NoError(t, err)
	defer db2.Close()
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns2, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	got, err := rtx.Get(ns2, []byte("bigkey"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, rtx.Rollback())
}

func TestOverflow1MB(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// 1MB value
	bigValue := bytes.Repeat([]byte("MEGABYTE"), 131072) // 1MB

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("1mb"), bigValue))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	got, err := rtx.Get(ns3, []byte("1mb"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, rtx.Rollback())
	require.NoError(t, db.IntegrityCheck())
	_ = ns
}

func TestOverflowMultipleKeys(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	// Insert 10 overflow keys
	for i := range 10 {
		k := fmt.Appendf(nil, "big-%02d", i)
		v := bytes.Repeat([]byte(fmt.Sprintf("%02d-", i)), 2000)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Read all back
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	for i := range 10 {
		k := fmt.Appendf(nil, "big-%02d", i)
		expected := bytes.Repeat([]byte(fmt.Sprintf("%02d-", i)), 2000)
		got, err := rtx.Get(ns3, k)
		require.NoError(t, err)
		assert.Equal(t, expected, got, "mismatch for key big-%02d", i)
	}
	require.NoError(t, rtx.Rollback())
	require.NoError(t, db.IntegrityCheck())
	_ = ns
}

func TestOverflowNamespaceDelete(t *testing.T) {
	db := tempDB(t)

	// Create namespace with overflow values
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("overflow")
	require.NoError(t, err)
	for i := range 5 {
		k := fmt.Appendf(nil, "key-%d", i)
		v := bytes.Repeat([]byte("V"), 5000)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	// Delete namespace (should free all pages including overflow)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.DeleteNamespace("overflow"))
	require.NoError(t, tx2.Commit())

	assert.True(t, db.pager.header.TotalFreelistPgs > 0)
}

// TestOverflowUpdateInlinePageOverflow reproduces a panic in rebuildLeafPage when
// updating an existing key's value to something much larger (but still inline).
// With 4-byte keys and 100-byte values, a leaf page holds exactly 37 cells.
// Updating one cell from val=100 to val=990 (still inline, 4+990 < maxLocal)
// causes the total cell content to exceed usableSize. rebuildLeafPage subtracts
// each cell size from contentOff, which goes negative, resulting in:
//
//	panic: runtime error: slice bounds out of range [-N:]
// TestOverflowConcurrentReaderCorruption reproduces a snapshot isolation bug
// in ReadTx.AppendValue. When a reader holds a snapshot and a concurrent writer
// deletes overflow documents and reuses those pages, the reader's overflow chain
// read uses the global pager walMaxFrame (advanced by the writer's commit)
// instead of the reader's own walMaxFrame. This causes the reader to see the
// new content of reused overflow pages, resulting in data corruption.
//
// The bug is in db.go AppendValue:
//
//	tx.pager.readOverflowChain(cell.overflowPg, ...)
//
// which should be:
//
//	tx.pager.readOverflowChainAt(cell.overflowPg, ..., tx.walMaxFrame)
func TestOverflowConcurrentReaderCorruption(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	_ = ns

	// Step 1: Write several overflow documents (values > maxLocal ~1001 bytes).
	// Use 4KB values to ensure overflow pages are allocated.
	const numDocs = 10
	const valueSize = 4000 // well above maxLocal, requires overflow pages

	origValues := make(map[string][]byte, numDocs)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range numDocs {
		key := fmt.Appendf(nil, "doc-%04d", i)
		// Each doc has a unique repeating byte pattern so corruption is detectable
		val := bytes.Repeat([]byte{byte(i)}, valueSize)
		origValues[string(key)] = val
		require.NoError(t, tx.Put(ns2, key, val))
	}
	require.NoError(t, tx.Commit())

	// Step 2: Open a reader. This captures walMaxFrame at this point in time.
	// The reader should see all original documents with their original values.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")

	// Step 3: While the reader is still open, delete all overflow documents
	// and write new ones that will reuse the freed overflow pages.
	// The freelist recycles pages from deletes, so the new documents'
	// overflow chains will occupy the same page numbers.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")

	// Delete all original docs (frees their overflow pages to freelist)
	for i := range numDocs {
		key := fmt.Appendf(nil, "doc-%04d", i)
		require.NoError(t, tx2.Delete(ns4, key))
	}

	// Write new docs with different data (reuses freed overflow pages)
	for i := range numDocs {
		key := fmt.Appendf(nil, "doc-%04d", i)
		val := bytes.Repeat([]byte{byte(i + 100)}, valueSize) // different pattern
		require.NoError(t, tx2.Put(ns4, key, val))
	}
	require.NoError(t, tx2.Commit())

	// Step 4: Advance the global pager walMaxFrame. The global walMaxFrame
	// is only updated during beginRead(). Opening a new read transaction
	// makes it see the writer's committed frames, advancing the global
	// walMaxFrame past the original reader's snapshot point.
	rtx2, err := db.BeginRead()
	require.NoError(t, err)

	// Step 5: The original reader (still at its old snapshot) reads documents.
	// With the bug, readOverflowChain uses the advanced global walMaxFrame,
	// so it reads the NEW overflow page content instead of the snapshot version.
	for i := range numDocs {
		key := fmt.Appendf(nil, "doc-%04d", i)
		got, err := rtx.Get(ns3, key)
		require.NoError(t, err, "Get(%s) failed", key)
		expected := origValues[string(key)]
		if !bytes.Equal(expected, got) {
			// Find first differing byte for clear error reporting
			diffIdx := -1
			for j := range min(len(expected), len(got)) {
				if expected[j] != got[j] {
					diffIdx = j
					break
				}
			}
			t.Fatalf("snapshot isolation violated for %s: first diff at byte %d, expected 0x%02x got 0x%02x (len expected=%d, got=%d)",
				key, diffIdx, expected[diffIdx], got[diffIdx], len(expected), len(got))
		}
	}
	require.NoError(t, rtx.Rollback())
	require.NoError(t, rtx2.Rollback())
}

// TestOverflowConcurrentReaderStress is a stress test for the overflow page
// snapshot isolation bug. It runs many iterations with concurrent readers and
// writers to trigger the race reliably.
func TestOverflowConcurrentReaderStress(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	_ = ns

	const iterations = 100
	const numDocs = 5
	const valueSize = 2048

	for iter := range iterations {
		// Write docs with iteration-specific data
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		nsw, _ := db.getNamespaceLocked("data")
		for i := range numDocs {
			key := fmt.Appendf(nil, "key-%02d", i)
			val := bytes.Repeat([]byte{byte(iter % 256), byte(i)}, valueSize/2)
			require.NoError(t, tx.Put(nsw, key, val))
		}
		require.NoError(t, tx.Commit())

		// Open reader at this snapshot
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		nsr, _ := db.getNamespaceLocked("data")

		// Writer overwrites all docs with different data
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		nsw2, _ := db.getNamespaceLocked("data")
		for i := range numDocs {
			key := fmt.Appendf(nil, "key-%02d", i)
			// Write completely different data
			val := bytes.Repeat([]byte{byte((iter + 50) % 256), byte(i + 50)}, valueSize/2)
			require.NoError(t, tx2.Put(nsw2, key, val))
		}
		require.NoError(t, tx2.Commit())

		// Advance global walMaxFrame by opening another reader
		rtx2, err := db.BeginRead()
		require.NoError(t, err)

		// Reader must still see the OLD data from its snapshot
		for i := range numDocs {
			key := fmt.Appendf(nil, "key-%02d", i)
			expected := bytes.Repeat([]byte{byte(iter % 256), byte(i)}, valueSize/2)
			got, err := rtx.Get(nsr, key)
			require.NoError(t, err, "iter %d: Get(%s) failed", iter, key)
			if !bytes.Equal(expected, got) {
				t.Fatalf("iter %d: snapshot isolation violated for %s: expected first byte 0x%02x, got 0x%02x",
					iter, key, expected[0], got[0])
			}
		}
		require.NoError(t, rtx.Rollback())
		require.NoError(t, rtx2.Rollback())
	}
}

func TestOverflowUpdateInlinePageOverflow(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Fill one leaf page with 37 cells (4-byte key + 100-byte value each).
	putN(t, db, "t1", 37, 100)

	// Update key 1 to a much larger value that still fits inline.
	// This triggers updateLeafCell → rebuildLeafPage with overflow.
	updateOne(t, db, "t1", 1, 990)

	require.NoError(t, db.IntegrityCheck())
}
