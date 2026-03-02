package btree

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSeekKey_ExactMatch verifies that SeekKey returns the exact key when it exists.
func TestSeekKey_ExactMatch(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 10)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	for i := range 10 {
		want := fmt.Appendf(nil, "key-%04d", i)
		got, err := rtx.SeekKey(ns, want)
		require.NoError(t, err)
		assert.Equal(t, want, got, "exact match for key-%04d", i)
	}
}

// TestSeekKey_BetweenKeys verifies that SeekKey returns the next key when
// the prefix falls between two existing keys.
func TestSeekKey_BetweenKeys(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	// Insert even-numbered keys: key-00, key-02, key-04, ...
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 20; i += 2 {
		k := fmt.Appendf(nil, "key-%02d", i)
		require.NoError(t, tx.Put(ns, k, []byte("v")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Seeking for key-03 should land on key-04.
	got, err := rtx.SeekKey(ns, []byte("key-03"))
	require.NoError(t, err)
	assert.Equal(t, []byte("key-04"), got)

	// Seeking for key-09 should land on key-10.
	got, err = rtx.SeekKey(ns, []byte("key-09"))
	require.NoError(t, err)
	assert.Equal(t, []byte("key-10"), got)
}

// TestSeekKey_BeforeAll verifies that seeking before all keys returns the first key.
func TestSeekKey_BeforeAll(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 100)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	got, err := rtx.SeekKey(ns, []byte("aaa"))
	require.NoError(t, err)
	assert.Equal(t, []byte("key-0000"), got)
}

// TestSeekKey_PastAll verifies that seeking past all keys returns ErrKeyNotFound.
// This exercises the rightmost-leaf fast path.
func TestSeekKey_PastAll(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 100)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	_, err = rtx.SeekKey(ns, []byte("zzz"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestSeekKey_EmptyNamespace verifies that seeking in an empty tree returns ErrKeyNotFound.
func TestSeekKey_EmptyNamespace(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	_, err = rtx.SeekKey(ns, []byte("anything"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestSeekKey_SingleKey tests all seek positions relative to a single-key tree.
func TestSeekKey_SingleKey(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("middle"), []byte("val")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Exact.
	got, err := rtx.SeekKey(ns, []byte("middle"))
	require.NoError(t, err)
	assert.Equal(t, []byte("middle"), got)

	// Before.
	got, err = rtx.SeekKey(ns, []byte("aaa"))
	require.NoError(t, err)
	assert.Equal(t, []byte("middle"), got)

	// After.
	_, err = rtx.SeekKey(ns, []byte("zzz"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestSeekKey_MultiLevel tests seeking in a tree deep enough to have interior pages,
// covering every key to verify correctness across page boundaries.
func TestSeekKey_MultiLevel(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	const n = 500
	insertManyKeys(t, db, ns, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Verify every existing key.
	for i := range n {
		want := fmt.Appendf(nil, "key-%04d", i)
		got, err := rtx.SeekKey(ns, want)
		require.NoError(t, err, "i=%d", i)
		assert.Equal(t, want, got)
	}

	// Past the last key.
	_, err = rtx.SeekKey(ns, []byte("key-9999"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestSeekKey_CursorFallback exercises the seekKeyViaCursor path.
// This happens when a prefix lands past all keys on a non-rightmost leaf,
// requiring cross-page navigation to find the next key on the following leaf.
func TestSeekKey_CursorFallback(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	// Use 100-byte values to fill pages quickly: each cell ~110 bytes,
	// so each 4KB leaf holds ~35 cells. 200 keys per group => ~6 leaves each.
	// The gap between groups guarantees the fallback path fires.
	val := make([]byte, 100)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	// Group 1: a-0000 .. a-0199
	for i := range 200 {
		k := fmt.Appendf(nil, "a-%04d", i)
		require.NoError(t, tx.Put(ns, k, val))
	}
	// Group 2: c-0000 .. c-0199 (gap "b-*" between groups)
	for i := range 200 {
		k := fmt.Appendf(nil, "c-%04d", i)
		require.NoError(t, tx.Put(ns, k, val))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// "b-0000" is after all "a-*" keys but before "c-*".
	// The last "a-*" leaf page doesn't contain any "c-*" keys,
	// so searchLeaf returns idx >= cellCount on a non-rightmost leaf.
	// AppendSeekKey must fall back to cursor and return "c-0000".
	got, err := rtx.SeekKey(ns, []byte("b-0000"))
	require.NoError(t, err)
	assert.Equal(t, []byte("c-0000"), got)

	// Also test a key in the gap after c-* but that's the rightmost leaf,
	// which should return ErrKeyNotFound via the fast path.
	_, err = rtx.SeekKey(ns, []byte("d-0000"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestSeekKey_PastAllMultiLevel tests the rightmost fast path with a deeper tree.
func TestSeekKey_PastAllMultiLevel(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	const n = 1000
	insertManyKeys(t, db, ns, n)
	// Checkpoint to ensure reads come from the DB file, not WAL.
	require.NoError(t, db.Checkpoint(CheckpointFull))

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	_, err = rtx.SeekKey(ns, []byte("zzz"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestAppendSeekKey_AppendsToBuf verifies that AppendSeekKey appends to an existing buffer.
func TestAppendSeekKey_AppendsToBuf(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("hello"), []byte("world")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	prefix := []byte("prefix:")
	buf := make([]byte, len(prefix), 64)
	copy(buf, prefix)
	result, err := rtx.AppendSeekKey(ns, []byte("hello"), buf)
	require.NoError(t, err)
	assert.Equal(t, append([]byte("prefix:"), []byte("hello")...), result)
}

// TestAppendSeekKey_ReusesBuf verifies zero-alloc when buf has sufficient capacity.
func TestAppendSeekKey_ReusesBuf(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key"), []byte("val")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	buf := make([]byte, 0, 64)
	result, err := rtx.AppendSeekKey(ns, []byte("key"), buf)
	require.NoError(t, err)
	assert.Equal(t, []byte("key"), result)
	// Should reuse the same backing array.
	assert.Equal(t, &buf[:1][0], &result[:1][0], "should reuse buf capacity")
}

// TestSeekKey_ClosedTx verifies that SeekKey on a closed transaction returns ErrTxClosed.
func TestSeekKey_ClosedTx(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	require.NoError(t, rtx.Rollback())

	_, err = rtx.SeekKey(ns, []byte("anything"))
	assert.ErrorIs(t, err, ErrTxClosed)
}

// TestSeekKey_BinaryKeys tests with binary (non-string) keys using big-endian encoding,
// which is the format used by any-store index tuples.
func TestSeekKey_BinaryKeys(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	const n = 1000
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		k := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, k, nil))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Exact lookups.
	for _, i := range []int{1, 2, 500, 999, 1000} {
		want := binary.BigEndian.AppendUint32(nil, uint32(i))
		got, err := rtx.SeekKey(ns, want)
		require.NoError(t, err)
		assert.Equal(t, want, got, "i=%d", i)
	}

	// Between: seek 0 should return key 1.
	got, err := rtx.SeekKey(ns, binary.BigEndian.AppendUint32(nil, 0))
	require.NoError(t, err)
	assert.Equal(t, binary.BigEndian.AppendUint32(nil, 1), got)

	// Past all: seek N+1 should return ErrKeyNotFound.
	_, err = rtx.SeekKey(ns, binary.BigEndian.AppendUint32(nil, uint32(n+1)))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestSeekKey_MatchesCursor verifies that SeekKey and Cursor.Seek always agree,
// including gap keys across multiple pages.
func TestSeekKey_MatchesCursor(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	const n = 500
	insertManyKeys(t, db, ns, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	cur := rtx.NewCursor(ns)
	defer cur.Close()

	// Test existing keys, gap keys, and boundary keys.
	probes := []string{
		"aaa",     // before all
		"key-0000", "key-0001", "key-0250", "key-0499", // exact
		"key-0000x", "key-0100z", "key-0499z", // gaps
		"zzz", // past all
	}
	for _, p := range probes {
		prefix := []byte(p)

		seekResult, seekErr := rtx.SeekKey(ns, prefix)

		curErr := cur.Seek(prefix)
		require.NoError(t, curErr, "cursor seek for %q", p)

		if cur.Valid() {
			curKey, kerr := cur.Key()
			require.NoError(t, kerr)
			require.NoError(t, seekErr, "SeekKey for %q should succeed", p)
			assert.Equal(t, curKey, seekResult, "mismatch for prefix %q", p)
		} else {
			assert.ErrorIs(t, seekErr, ErrKeyNotFound, "SeekKey for %q should be ErrKeyNotFound", p)
		}
	}
}

// TestSeekKey_AfterCheckpoint verifies that SeekKey works correctly after
// a checkpoint moves data from WAL to the main DB file.
func TestSeekKey_AfterCheckpoint(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 200)
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Insert more keys after checkpoint (these live in WAL).
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 200; i < 300; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, tx.Put(ns, k, []byte("v")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Key from DB file.
	got, err := rtx.SeekKey(ns, []byte("key-0050"))
	require.NoError(t, err)
	assert.Equal(t, []byte("key-0050"), got)

	// Key from WAL.
	got, err = rtx.SeekKey(ns, []byte("key-0250"))
	require.NoError(t, err)
	assert.Equal(t, []byte("key-0250"), got)
}

// TestSeekKey_WriteTx verifies that SeekKey works within a write transaction,
// seeing uncommitted data.
func TestSeekKey_WriteTx(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("aaa"), nil))
	require.NoError(t, tx.Put(ns, []byte("ccc"), nil))

	// Should see uncommitted keys.
	got, err := tx.SeekKey(ns, []byte("bbb"))
	require.NoError(t, err)
	assert.Equal(t, []byte("ccc"), got)

	got, err = tx.SeekKey(ns, []byte("aaa"))
	require.NoError(t, err)
	assert.Equal(t, []byte("aaa"), got)

	require.NoError(t, tx.Commit())
}

// TestSeekKey_CursorFallbackSmallPage uses 512-byte pages to guarantee
// the seekKeyViaCursor fallback fires. With tiny pages, even a few keys
// per group spill across leaf boundaries, making the gap hit certain.
func TestSeekKey_CursorFallbackSmallPage(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	// Group 1: keys "aa-00" .. "aa-49"
	for i := range 50 {
		k := fmt.Appendf(nil, "aa-%02d", i)
		require.NoError(t, tx.Put(ns, k, []byte("value-padding-data")))
	}
	// Group 2: keys "cc-00" .. "cc-49" (gap at "bb-*")
	for i := range 50 {
		k := fmt.Appendf(nil, "cc-%02d", i)
		require.NoError(t, tx.Put(ns, k, []byte("value-padding-data")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Verify via cursor that "bb-00" → "cc-00".
	cur := rtx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek([]byte("bb-00")))
	require.True(t, cur.Valid())
	curKey, err := cur.Key()
	require.NoError(t, err)
	assert.Equal(t, []byte("cc-00"), curKey)

	// SeekKey must give the same answer (via fallback path).
	got, err := rtx.SeekKey(ns, []byte("bb-00"))
	require.NoError(t, err)
	assert.Equal(t, []byte("cc-00"), got)

	// Multiple gap probes.
	for _, probe := range []string{"bb-00", "bb-25", "bb-99"} {
		got, err := rtx.SeekKey(ns, []byte(probe))
		require.NoError(t, err, "probe=%s", probe)
		assert.Equal(t, []byte("cc-00"), got, "probe=%s", probe)
	}
}

// TestSeekKey_LeftmostKeyAfterRightChild exercises the cellIdx+1 == n branch
// in leftmostKeyAfter, where the next child is the interior page's rightChild.
// Uses 3 groups with a gap after the last non-rightChild cell.
func TestSeekKey_LeftmostKeyAfterRightChild(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	// Three groups with gaps. On tiny pages, the interior page separators
	// partition them so that the last group lands under the rightChild.
	for i := range 30 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "aa-%02d", i), []byte("pad")))
	}
	for i := range 30 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "cc-%02d", i), []byte("pad")))
	}
	for i := range 30 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "ee-%02d", i), []byte("pad")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	cur := rtx.NewCursor(ns)
	defer cur.Close()

	// Probe every gap and verify SeekKey matches cursor.
	for _, probe := range []string{"bb-00", "dd-00"} {
		require.NoError(t, cur.Seek([]byte(probe)))
		require.True(t, cur.Valid(), "probe=%s", probe)
		curKey, kerr := cur.Key()
		require.NoError(t, kerr)

		got, err := rtx.SeekKey(ns, []byte(probe))
		require.NoError(t, err, "probe=%s", probe)
		assert.Equal(t, curKey, got, "probe=%s", probe)
	}

	// Past all groups.
	_, err = rtx.SeekKey(ns, []byte("ff-00"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// TestSeekKey_LeftmostKeyAfterOverflow exercises the overflow-key branch inside
// leftmostKeyAfter (cell.overflowPg != 0). All keys are overflow-sized (200 bytes)
// on 512-byte pages, so when leftmostKeyAfter reads the first key of the target
// leaf, it must reconstruct it from overflow pages.
//
// The test inserts 2000 sequential overflow keys, reads the root structure to
// find a subtree whose rightmost leaf's last key is K, then probes K+"\x00"
// to force leftmostKeyAfter to the NEXT subtree — whose first leaf cell[0]
// is guaranteed to be an overflow key.
func TestSeekKey_LeftmostKeyAfterOverflow(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Insert 2000 keys, each 200 bytes (overflow on 512-byte pages).
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := range 2000 {
		key := make([]byte, 200)
		copy(key, fmt.Appendf(nil, "k-%05d", i))
		require.NoError(t, tx.Put(ns, key, nil))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Find a non-rightmost subtree whose rightmost leaf's last key is K.
	// Probing K+"\x00" forces leftmostKeyAfter to the next subtree where
	// cell[0] is an overflow key.
	probe, nextKey := findSubtreeProbe(t, rtx, ns)
	require.NotNil(t, probe, "couldn't find a suitable subtree probe")

	got, err := rtx.SeekKey(ns, probe)
	require.NoError(t, err)
	assert.Equal(t, nextKey, got)
}

// TestSeekKey_LeftmostKeyAfterDeepTree exercises the interior-page descent loop
// in leftmostKeyAfter (lines that handle !isLeaf). With 2000 sequential keys
// on 512-byte pages (3-level tree), the test reads the root to find a subtree
// whose rightmost leaf's last key is K. Probing K+"\x00" forces an overshoot
// on a non-rightmost leaf with the fallback saved at the ROOT. Because the root's
// next child is a level-1 interior page (not a leaf), leftmostKeyAfter must
// descend through it to reach the leftmost leaf.
func TestSeekKey_LeftmostKeyAfterDeepTree(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := range 2000 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "k-%05d", i), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// findSubtreeProbe locates a root cell whose leftChild is an interior page,
	// walks to its rightmost leaf's last key K, and returns probe=K+"\x00".
	// When AppendSeekKey descends:
	//   root:      non-rightChild branch → save fallback at root
	//   interior:  probe > all separators → rightChild → don't update fallback
	//   leaf:      probe > all keys → overshoot → leftmostKeyAfter(root, cellIdx)
	// leftmostKeyAfter then gets the root's next child = level-1 interior page
	// and must descend through it (interior descent loop) to the leftmost leaf.
	probe, nextKey := findSubtreeProbe(t, rtx, ns)
	require.NotNil(t, probe, "couldn't find a suitable subtree probe")

	got, err := rtx.SeekKey(ns, probe)
	require.NoError(t, err)
	assert.Equal(t, nextKey, got)

	// Cross-check with cursor.
	cur := rtx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(probe))
	require.True(t, cur.Valid())
	curKey, kerr := cur.Key()
	require.NoError(t, kerr)
	assert.Equal(t, curKey, got)
}

// TestSeekKey_MatchesCursorExhaustive does a full sweep of gap probes across
// a multi-page dataset to verify SeekKey matches Cursor.Seek for every
// possible inter-key gap, including leaf boundary crossings.
func TestSeekKey_MatchesCursorExhaustive(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir+"/test.db", Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	// Insert even-numbered keys with values to fill pages.
	const n = 200
	for i := 0; i < n; i += 2 {
		k := fmt.Appendf(nil, "k-%04d", i)
		require.NoError(t, tx.Put(ns, k, []byte("value-padding")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	cur := rtx.NewCursor(ns)
	defer cur.Close()

	// Probe every integer in [0, n+2), including gaps between existing keys
	// and past-the-end. This hits every leaf boundary.
	mismatches := 0
	for i := 0; i < n+2; i++ {
		prefix := fmt.Appendf(nil, "k-%04d", i)

		seekResult, seekErr := rtx.SeekKey(ns, prefix)

		require.NoError(t, cur.Seek(prefix))
		if cur.Valid() {
			curKey, kerr := cur.Key()
			require.NoError(t, kerr)
			if seekErr != nil || string(seekResult) != string(curKey) {
				t.Errorf("i=%d: SeekKey=%q err=%v, Cursor=%q", i, seekResult, seekErr, curKey)
				mismatches++
			}
		} else {
			if seekErr == nil {
				t.Errorf("i=%d: SeekKey returned %q but cursor is invalid", i, seekResult)
				mismatches++
			}
		}
	}
	assert.Equal(t, 0, mismatches, "SeekKey and Cursor.Seek disagreed on %d probes", mismatches)
}

// TestSeekKey_OverflowKey tests seeking with keys large enough to cause overflow pages.
func TestSeekKey_OverflowKey(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Create keys large enough to overflow (> ~1/4 of usable page).
	// With 4KB pages, usable ~4000 bytes, overflow threshold ~1000 bytes.
	makeKey := func(prefix byte, size int) []byte {
		k := make([]byte, size)
		k[0] = prefix
		for i := 1; i < size; i++ {
			k[i] = 'x'
		}
		return k
	}

	bigA := makeKey('a', 1500)
	bigC := makeKey('c', 1500)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, bigA, nil))
	require.NoError(t, tx.Put(ns, bigC, nil))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Exact match on overflow key.
	got, err := rtx.SeekKey(ns, bigA)
	require.NoError(t, err)
	assert.Equal(t, bigA, got)

	// Between overflow keys.
	got, err = rtx.SeekKey(ns, makeKey('b', 10))
	require.NoError(t, err)
	assert.Equal(t, bigC, got)

	// Past all overflow keys.
	_, err = rtx.SeekKey(ns, makeKey('d', 10))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

// findSubtreeProbe reads the tree structure to find a probe key that forces
// leftmostKeyAfter to navigate from a root-level fallback through an interior
// page. It finds a root cell whose leftChild is an interior page, walks to
// its rightmost leaf's last key K, and returns (K+"\x00", nextKey).
// nextKey is the key the root separator points to (first key in next subtree).
func findSubtreeProbe(t *testing.T, rtx *ReadTx, ns *Namespace) (probe []byte, nextKey []byte) {
	t.Helper()
	usableSize := rtx.pager.usableSize()
	mvcc := !rtx.writable

	root, err := rtx.txGetPage(ns.rootPage)
	require.NoError(t, err)
	defer rtx.pager.releasePage(root)

	if root.header.isLeaf() {
		t.Skip("root is leaf, need deeper tree")
	}

	nCells := int(root.header.cellCount)
	for i := 0; i < nCells; i++ {
		off := root.getCellOffset(i)
		childPgno := binary.BigEndian.Uint32(root.data[off : off+4])

		child, cerr := rtx.txGetPage(childPgno)
		require.NoError(t, cerr)
		childIsLeaf := child.header.isLeaf()
		rtx.pager.releasePage(child)

		if childIsLeaf {
			continue // need an interior child for the descent loop test
		}

		// Walk to rightmost leaf of this subtree.
		pg, perr := rtx.txGetPage(childPgno)
		require.NoError(t, perr)
		for !pg.header.isLeaf() {
			rc := pg.header.rightChild
			rtx.pager.releasePage(pg)
			pg, perr = rtx.txGetPage(rc)
			require.NoError(t, perr)
		}
		// Get last key on rightmost leaf.
		leafCells := int(pg.header.cellCount)
		require.Greater(t, leafCells, 0)
		lastOff := pg.getCellOffset(leafCells - 1)
		cell, _, cerr2 := parseLeafCellWithSize(pg.data, int(lastOff), usableSize)
		require.NoError(t, cerr2)
		var lastKey []byte
		if cell.overflowPg != 0 {
			lastKey, err = leafFullKey(pg.data, int(lastOff), usableSize, rtx.pager, rtx.walMaxFrame, mvcc)
			require.NoError(t, err)
		} else {
			lastKey = append([]byte(nil), cell.key...)
		}
		rtx.pager.releasePage(pg)

		// Verify the next child (cell[i+1].leftChild or rightChild) is interior.
		var nextPgno uint32
		if i+1 < nCells {
			nextOff := root.getCellOffset(i + 1)
			nextPgno = binary.BigEndian.Uint32(root.data[nextOff : nextOff+4])
		} else {
			nextPgno = root.header.rightChild
		}
		nextPg, nerr := rtx.txGetPage(nextPgno)
		require.NoError(t, nerr)
		nextIsInterior := !nextPg.header.isLeaf()
		rtx.pager.releasePage(nextPg)

		if !nextIsInterior {
			continue // need the next child to be interior for the descent loop
		}

		// Find the expected result: leftmost key of the next subtree.
		nextPg2, nerr := rtx.txGetPage(nextPgno)
		require.NoError(t, nerr)
		for !nextPg2.header.isLeaf() {
			firstOff, oerr := nextPg2.getCellOffsetSafe(0)
			require.NoError(t, oerr)
			firstChild := binary.BigEndian.Uint32(nextPg2.data[firstOff : firstOff+4])
			rtx.pager.releasePage(nextPg2)
			nextPg2, nerr = rtx.txGetPage(firstChild)
			require.NoError(t, nerr)
		}
		require.Greater(t, int(nextPg2.header.cellCount), 0)
		firstOff := nextPg2.getCellOffset(0)
		firstCell, _, fcerr := parseLeafCellWithSize(nextPg2.data, int(firstOff), usableSize)
		require.NoError(t, fcerr)
		if firstCell.overflowPg != 0 {
			nextKey, err = leafFullKey(nextPg2.data, int(firstOff), usableSize, rtx.pager, rtx.walMaxFrame, mvcc)
			require.NoError(t, err)
		} else {
			nextKey = append([]byte(nil), firstCell.key...)
		}
		rtx.pager.releasePage(nextPg2)

		return append(lastKey, 0), nextKey
	}
	return nil, nil
}
