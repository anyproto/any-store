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
		"aaa",                                          // before all
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
	db, err := testOpen(t, dir+"/test.db", Options{PageSize: 512})
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
	db, err := testOpen(t, dir+"/test.db", Options{PageSize: 512})
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
// leftmostKeyAfter. The first key on the sibling leaf is large enough to
// overflow, so leftmostKeyAfter must read overflow pages to reconstruct it.
func TestSeekKey_LeftmostKeyAfterOverflow(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, dir+"/test.db", Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// With 512-byte pages, overflow threshold is ~100 bytes.
	// Group 1: small keys.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	for i := range 20 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "a-%02d", i), []byte("v")))
	}
	// Group 2: first key is an overflow-sized key.
	overflowKey := make([]byte, 200)
	overflowKey[0] = 'c'
	for i := 1; i < len(overflowKey); i++ {
		overflowKey[i] = 'x'
	}
	require.NoError(t, tx.Put(ns, overflowKey, []byte("v")))
	for i := 1; i < 20; i++ {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "d-%02d", i), []byte("v")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	// Seeking "b-00" should land past all "a-*" keys and trigger leftmostKeyAfter.
	// The first key on the next leaf should be the overflow key.
	cur := rtx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek([]byte("b-00")))
	require.True(t, cur.Valid())
	curKey, kerr := cur.Key()
	require.NoError(t, kerr)

	got, err := rtx.SeekKey(ns, []byte("b-00"))
	require.NoError(t, err)
	assert.Equal(t, curKey, got)
}

// TestSeekKey_LeftmostKeyAfterDeepTree exercises leftmostKeyAfter when the
// fallback interior page is several levels above the leaf, requiring the
// descent loop in leftmostKeyAfter to traverse intermediate interior pages.
func TestSeekKey_LeftmostKeyAfterDeepTree(t *testing.T) {
	dir := t.TempDir()
	// 512-byte pages force a deep tree with relatively few keys.
	db, err := testOpen(t, dir+"/test.db", Options{PageSize: 512})
	require.NoError(t, err)
	defer db.Close()

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	// Insert enough keys to create a 3+ level tree on 512-byte pages.
	// Each leaf holds ~15 keys, each interior holds ~30 children.
	// 1000 keys => ~67 leaves => ~3 interior pages => root+interiors+leaves = 3 levels.
	// Two groups with a large gap force the fallback at a high level.
	for i := range 500 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "aaa-%04d", i), []byte("val")))
	}
	for i := range 500 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "zzz-%04d", i), []byte("val")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()

	cur := rtx.NewCursor(ns)
	defer cur.Close()

	// "mmm" is in the gap between groups. The fallback interior page may be
	// the root or a high-level interior, and leftmostKeyAfter must descend
	// through intermediate interior pages to find the first "zzz-*" key.
	require.NoError(t, cur.Seek([]byte("mmm")))
	require.True(t, cur.Valid())
	curKey, kerr := cur.Key()
	require.NoError(t, kerr)

	got, err := rtx.SeekKey(ns, []byte("mmm"))
	require.NoError(t, err)
	assert.Equal(t, curKey, got)
	assert.Equal(t, []byte("zzz-0000"), got)
}

// TestSeekKey_MatchesCursorExhaustive does a full sweep of gap probes across
// a multi-page dataset to verify SeekKey matches Cursor.Seek for every
// possible inter-key gap, including leaf boundary crossings.
func TestSeekKey_MatchesCursorExhaustive(t *testing.T) {
	dir := t.TempDir()
	db, err := testOpen(t, dir+"/test.db", Options{PageSize: 512})
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
