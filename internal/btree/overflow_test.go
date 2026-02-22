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

// TestGetPageAtCacheCoherencyBug demonstrates a cache coherency bug in
// pager.getPageAt that causes overflow data corruption.
//
// Root cause: getPageAt checks `getLatest(pgno) <= walMaxFrame` to decide
// if a cached page is valid. This only verifies the LATEST WAL frame for the
// page is within the caller's snapshot — it does NOT verify the CACHED DATA
// is actually from that latest frame. A reader with an older snapshot can
// populate the cache with old WAL data, and subsequent callers (including the
// writer via collectLeafCells) get that stale data because the latest-frame
// check passes.
//
// Scenario:
//  1. TX1 writes key K with overflow value V1 → overflow pages P at WAL frame F1
//  2. Reader R1 opens (walMaxFrame = F1)
//  3. TX2 updates K to V2 → frees V1's overflow pages, reallocates them from
//     freelist (same page numbers, LIFO) → pages P at WAL frame F2 with V2 data
//  4. Cache cleared (simulates LRU eviction under memory pressure)
//  5. R1 reads K → readOverflowChainAt → getPageAt (cache miss) → populates
//     cache with V1 data from WAL frame F1
//  6. R2 opens (walMaxFrame ≥ F2), reads K → readOverflowChainAt → getPageAt
//     (cache HIT) → getLatest(P) = F2, F2 ≤ R2.walMaxFrame → returns stale V1!
//
// The same bug affects the writer (via collectLeafCells/readOverflowChainAt),
// causing corrupted values to be committed to WAL (on-disk corruption).
func TestGetPageAtCacheCoherencyBug(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             true,
		DisableAutoCheckpoint: true, // prevent WAL reset
	}
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// TX1: Create namespace and insert key with overflow value V1.
	// Value of 5000 bytes requires ~2 overflow pages for 4KB page size
	// (maxLocal ≈ 1001, overflow usable = 4092 per page).
	tx1, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx1.CreateNamespace("data")
	require.NoError(t, err)
	v1 := bytes.Repeat([]byte("A"), 5000)
	require.NoError(t, tx1.Put(ns, []byte("key"), v1))
	require.NoError(t, tx1.Commit())
	_ = ns

	// Open reader R1 at TX1's snapshot.
	rtx1, err := db.BeginRead()
	require.NoError(t, err)
	nsR1, _ := db.getNamespaceLocked("data")

	// TX2: Update key to V2 (different data, same size → same number of overflow pages).
	// The update frees V1's overflow pages and reallocates from the freelist.
	// Due to LIFO freelist ordering, the same physical page numbers are reused
	// (in reverse chain order). After commit, those pages exist at two WAL frames:
	// F1 with V1 data and F2 with V2 data.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	nsW2, _ := db.getNamespaceLocked("data")
	v2 := bytes.Repeat([]byte("B"), 5000)
	require.NoError(t, tx2.Put(nsW2, []byte("key"), v2))
	require.NoError(t, tx2.Commit())

	// Clear entire page cache — simulates LRU eviction under memory pressure.
	db.pager.cache.clear()

	// R1 reads key → readOverflowChainAt → getPageAt (cache miss for each
	// overflow page) → cache.create populates entries with V1 data from WAL
	// frames at R1's snapshot.
	gotR1, err := rtx1.Get(nsR1, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, v1, gotR1, "R1 should see V1 from its snapshot")
	require.NoError(t, rtx1.Rollback())

	// Cache now contains overflow pages with STALE V1 data.
	// Open R2 at current snapshot (walMaxFrame ≥ F2).
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	nsR2, _ := db.getNamespaceLocked("data")
	gotR2, err := rtx2.Get(nsR2, []byte("key"))
	require.NoError(t, err)

	// BUG: getPageAt returns stale V1 data from reader-polluted cache.
	// The check `getLatest(pgno) <= walMaxFrame` passes because the latest
	// frame IS within R2's snapshot, but the cached data is from an older frame.
	if !bytes.Equal(v2, gotR2) {
		// Find first differing byte
		diffIdx := -1
		for i := range min(len(v2), len(gotR2)) {
			if v2[i] != gotR2[i] {
				diffIdx = i
				break
			}
		}
		t.Fatalf("cache coherency bug: R2 sees stale data instead of V2\n"+
			"  expected len=%d first byte=0x%02x\n"+
			"  got      len=%d first byte=0x%02x\n"+
			"  first diff at byte %d",
			len(v2), v2[0], len(gotR2), gotR2[0], diffIdx)
	}
	require.NoError(t, rtx2.Rollback())
}

// TestGetPageAtWriterCorruption demonstrates that the cache coherency bug
// in getPageAt causes the WRITER to commit corrupted data to WAL via
// collectLeafCells during a page split, resulting in persistent on-disk
// corruption.
//
// When a leaf page containing an overflow cell needs to be split, the writer
// calls collectLeafCells which reads ALL cells' overflow data via
// readOverflowChainAt → getPageAt. If the cache was polluted by a reader
// with stale overflow data, collectLeafCells reads the wrong values and
// commits them to WAL.
func TestGetPageAtWriterCorruption(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	opts := Options{
		PageSize:              4096,
		CacheSize:             100,
		InProcess:             true,
		DisableAutoCheckpoint: true,
	}
	db, err := Open(path, opts)
	require.NoError(t, err)
	defer db.Close()

	// TX1: Create namespace. Insert "key-big" with overflow V1 and fill the
	// leaf page near capacity with small keys, so the next insert triggers a split.
	tx1, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx1.CreateNamespace("data")
	require.NoError(t, err)

	v1 := bytes.Repeat([]byte("A"), 5000)
	require.NoError(t, tx1.Put(ns, []byte("key-big"), v1))

	// Fill the page with small keys (100-byte values, ~111 bytes each cell).
	// One overflow cell takes ~490 bytes inline. Available: ~4088-490 = ~3598.
	// Each small cell: ~111 bytes. Fit about 32 small cells.
	for i := 0; i < 30; i++ {
		k := fmt.Appendf(nil, "key-%03d", i)
		v := bytes.Repeat([]byte{byte(i)}, 100)
		require.NoError(t, tx1.Put(ns, k, v))
	}
	require.NoError(t, tx1.Commit())
	_ = ns

	// Open reader R1 at TX1's snapshot.
	rtx1, err := db.BeginRead()
	require.NoError(t, err)
	nsR1, _ := db.getNamespaceLocked("data")

	// TX2: Update "key-big" to V2 (different data, same size).
	// Freelist recycles V1's overflow pages for V2.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	nsW2, _ := db.getNamespaceLocked("data")
	v2 := bytes.Repeat([]byte("B"), 5000)
	require.NoError(t, tx2.Put(nsW2, []byte("key-big"), v2))
	require.NoError(t, tx2.Commit())

	// Clear cache → simulates LRU eviction.
	db.pager.cache.clear()

	// R1 reads "key-big" → populates cache with V1 overflow data.
	gotR1, err := rtx1.Get(nsR1, []byte("key-big"))
	require.NoError(t, err)
	require.Equal(t, v1, gotR1)
	require.NoError(t, rtx1.Rollback())

	// TX3: Insert more small keys to trigger a leaf page split.
	// The split calls collectLeafCells which reads ALL cells including
	// "key-big"'s overflow data via readOverflowChainAt → getPageAt.
	// With stale cache, it reads V1 data instead of V2 → corruption.
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	nsW3, _ := db.getNamespaceLocked("data")
	for i := 30; i < 60; i++ {
		k := fmt.Appendf(nil, "key-%03d", i)
		v := bytes.Repeat([]byte{byte(i)}, 100)
		require.NoError(t, tx3.Put(nsW3, k, v))
	}
	require.NoError(t, tx3.Commit())

	// Read "key-big" back — should be V2 (unchanged by TX3).
	rtx2, err := db.BeginRead()
	require.NoError(t, err)
	nsR2, _ := db.getNamespaceLocked("data")
	gotFinal, err := rtx2.Get(nsR2, []byte("key-big"))
	require.NoError(t, err)

	if !bytes.Equal(v2, gotFinal) {
		diffIdx := -1
		for i := range min(len(v2), len(gotFinal)) {
			if v2[i] != gotFinal[i] {
				diffIdx = i
				break
			}
		}
		t.Fatalf("writer corruption via collectLeafCells:\n"+
			"  expected len=%d first byte=0x%02x (V2)\n"+
			"  got      len=%d first byte=0x%02x\n"+
			"  first diff at byte %d",
			len(v2), v2[0], len(gotFinal), gotFinal[0], diffIdx)
	}
	require.NoError(t, rtx2.Rollback())
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
