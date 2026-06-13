package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildDeepNS inserts n keyed values ("key-%06d" -> "val-%06d") so the tree is
// multi-level (with the default page size, a few thousand keys yields depth>=3),
// exercising the retained-cursor SeekNear slow path.
func buildDeepNS(t *testing.T, n int) (*DB, *Namespace) {
	t.Helper()
	db, ns := tempDBWithNS(t, "data")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())
	return db, ns
}

// cursorPos captures the observable position of a cursor for differential
// comparison: validity, and (if valid) the current key+value bytes.
type cursorPos struct {
	valid bool
	key   []byte
	val   []byte
}

func capturePos(t *testing.T, c *Cursor) cursorPos {
	t.Helper()
	p := cursorPos{valid: c.Valid()}
	if p.valid {
		k, err := c.Key()
		require.NoError(t, err)
		v, err := c.Value()
		require.NoError(t, err)
		p.key = append([]byte(nil), k...)
		p.val = append([]byte(nil), v...)
	}
	return p
}

// TestCursorReuse_DifferentialRandom is the core retained-cursor invariant: for
// many random keys probed in random order through ONE retained cursor (so
// SeekNear is driven repeatedly: same-leaf fast path or full root Seek), the
// resulting position is byte-identical to a FRESH cursor doing a full root Seek
// for the same key.
func TestCursorReuse_DifferentialRandom(t *testing.T) {
	const n = 8000
	db, ns := buildDeepNS(t, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Confirm the tree is actually multi-level so reuse has something to skip.
	probe := rtx.NewCursor(ns)
	require.NoError(t, probe.Seek([]byte("key-000000")))
	require.GreaterOrEqual(t, len(probe.stack), 2, "tree must be multi-level for this test")
	probe.Close()

	reused := rtx.NewCursor(ns) // retained across all probes -> SeekNear reuse path
	defer reused.Close()

	rng := rand.New(rand.NewSource(0xBADC0DE))
	for iter := 0; iter < 4000; iter++ {
		// Mix exact keys, between-keys, before-all and after-all probes.
		var key []byte
		switch rng.Intn(4) {
		case 0:
			key = fmt.Appendf(nil, "key-%06d", rng.Intn(n))
		case 1:
			key = fmt.Appendf(nil, "key-%06d.5", rng.Intn(n)) // between two keys
		case 2:
			key = []byte("key-") // before all
		default:
			key = []byte("key-999999z") // after all
		}

		// Position via the retained cursor (SeekNear: same-leaf fast path when
		// the key is on the pinned leaf, full root Seek otherwise).
		require.NoError(t, reused.SeekNear(key))
		gotReuse := capturePos(t, reused)

		// Position via a fresh cursor (full root Seek) = ground truth.
		fresh := rtx.NewCursor(ns)
		require.NoError(t, fresh.Seek(key))
		wantFresh := capturePos(t, fresh)
		fresh.Close()

		require.Equal(t, wantFresh.valid, gotReuse.valid, "validity mismatch for %q", key)
		if wantFresh.valid {
			assert.True(t, bytes.Equal(wantFresh.key, gotReuse.key),
				"key mismatch for %q: fresh=%q reuse=%q", key, wantFresh.key, gotReuse.key)
			assert.True(t, bytes.Equal(wantFresh.val, gotReuse.val),
				"value mismatch for %q", key)
		}
	}
}

// TestCursorReuse_AppendValueByKey_Parity verifies that AppendValueByKey through a
// RETAINED cursor (retained across rows, off-leaf keys re-descend) returns byte-identical
// values to the cursor-free ReadTx.AppendValue for every key, in random order.
func TestCursorReuse_AppendValueByKey_Parity(t *testing.T) {
	const n = 6000
	db, ns := buildDeepNS(t, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	reused := rtx.NewCursor(ns)
	defer reused.Close()

	rng := rand.New(rand.NewSource(42))
	order := rng.Perm(n)
	for _, i := range order {
		key := fmt.Appendf(nil, "key-%06d", i)

		gotCursor, cerr := reused.AppendValueByKey(key, nil)
		require.NoError(t, cerr, "cursor AppendValueByKey for %q", key)

		gotFree, ferr := rtx.AppendValue(ns, key, nil)
		require.NoError(t, ferr, "cursor-free AppendValue for %q", key)

		assert.True(t, bytes.Equal(gotFree, gotCursor),
			"value mismatch for %q: free=%q cursor=%q", key, gotFree, gotCursor)
	}
}

// TestCursorReuse_KeyNotFound verifies that a missing key still surfaces
// ErrKeyNotFound via the retained cursor's AppendValueByKey (the deleted-doc-
// still-in-index path FetchIter relies on), including after an intervening
// successful seek to a different leaf that primed the interior stack.
func TestCursorReuse_KeyNotFound(t *testing.T) {
	const n = 5000
	db, ns := buildDeepNS(t, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	c := rtx.NewCursor(ns)
	defer c.Close()

	// Prime the cursor on a real key (fills the interior stack + a leaf).
	_, err = c.AppendValueByKey([]byte("key-002500"), nil)
	require.NoError(t, err)

	// A key that does not exist but sorts BETWEEN existing keys on a different
	// leaf -> the retained cursor re-descends and AppendValueByKey must report miss.
	_, err = c.AppendValueByKey([]byte("key-004999x"), nil)
	require.ErrorIs(t, err, ErrKeyNotFound)

	// And a real key after the miss still works (cursor stays usable).
	got, err := c.AppendValueByKey([]byte("key-000123"), nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("val-000123"), got)
}

// TestCursorReuse_RepeatedAndAdjacentKeys exercises the same-docId re-point
// (repeated key, like an array/multikey index mapping multiple rows to one doc)
// and adjacent keys on the same leaf, then a far jump that forces a re-descend —
// each must match a fresh Seek.
func TestCursorReuse_RepeatedAndAdjacentKeys(t *testing.T) {
	const n = 7000
	db, ns := buildDeepNS(t, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	reused := rtx.NewCursor(ns)
	defer reused.Close()

	check := func(key []byte) {
		require.NoError(t, reused.SeekNear(key))
		gotReuse := capturePos(t, reused)
		fresh := rtx.NewCursor(ns)
		require.NoError(t, fresh.Seek(key))
		wantFresh := capturePos(t, fresh)
		fresh.Close()
		require.Equal(t, wantFresh.valid, gotReuse.valid, "validity for %q", key)
		if wantFresh.valid {
			assert.Equal(t, wantFresh.key, gotReuse.key, "key for %q", key)
			assert.Equal(t, wantFresh.val, gotReuse.val, "val for %q", key)
		}
	}

	k := []byte("key-003456")
	check(k)
	check(k)                    // repeated same key (same-leaf re-point)
	check(k)                    // repeated again
	check([]byte("key-003457")) // adjacent next, same leaf
	check([]byte("key-006999")) // far jump -> re-descend
	check([]byte("key-000001")) // far jump back -> re-descend
	check(k)                    // back to the middle
}

// TestCursorReuse_SingleLevelTree verifies the retained-cursor SeekNear slow
// path works on a single-leaf tree (no interior frames to reuse).
func TestCursorReuse_SingleLevelTree(t *testing.T) {
	db, ns := buildDeepNS(t, 5) // tiny: single leaf

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	c := rtx.NewCursor(ns)
	defer c.Close()
	require.NoError(t, c.Seek([]byte("key-000000")))
	require.Len(t, c.stack, 1, "single-leaf tree has only the leaf frame")

	// Force the SeekNear slow path (off-leaf key won't exist here since it's one
	// page, but the window check will reject and fall into the full Seek slow path).
	require.NoError(t, c.SeekNear([]byte("key-000003")))
	got, err := c.Value()
	require.NoError(t, err)
	assert.Equal(t, []byte("val-000003"), got)
}
