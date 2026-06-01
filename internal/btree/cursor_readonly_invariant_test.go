package btree

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin the structural invariants behind the by-design drift
// documented at docs/btree/NOTES.md#old-drift-readonly-two-state-cursor:
// the Go Cursor is a READ-ONLY, 2-STATE (valid bool), dynamic-stack cursor that
// pins ONLY its leaf frame and has NO save/restore.
//
// In SQLite a BtCursor has 5 states and, before every tree mutation, calls
// saveAllCursors(pBt, pCur->pgnoRoot, pCur) (btree.c:9442 in sqlite3BtreeInsert,
// btree.c:9935 in sqlite3BtreeDelete), which serializes the cursor key, releases
// all pinned cursor pages (btreeReleaseAllCursorPages, btree.c:769-789), and sets
// eState=CURSOR_REQUIRESEEK so the cursor re-seeks on next use. The Go port drops
// this entirely: the cursor keeps frame.pg pinned and frame.cellIdx frozen across
// any mutation.
//
// That omission is SAFE only because the design relies on an unstated invariant:
// the Go cursor is a read-only iterator whose pinned leaf and frozen position are
// never mutated underneath it within the same logical operation. Writes go through
// WriteTx.Put / WriteTx.Delete, which build their OWN writable btree and traverse
// from the root (db.go:1834-1849, btree.go:Put/Delete) rather than driving a
// cursor. These tests assert the load-bearing facts of that contract so that a
// future refactor which (e.g.) adds a write method to Cursor, pins more than the
// leaf, drives mutations through a cursor, or introduces a hidden re-seek state
// fails loudly here.
//
// NONE of these tests change or exercise unsupported production behavior — they
// only observe and pin the existing read-only/2-state/single-leaf-pin contract.

// buildMultiLevelTree inserts n small entries with a 512-byte page size so the
// resulting tree has at least one interior level above the leaves. The cursor
// stack then contains interior frames (pg == nil) below a single pinned leaf
// frame, which is what makes the single-leaf-pin assertion meaningful.
func buildMultiLevelTree(t *testing.T, n int) (*DB, *Namespace) {
	t.Helper()
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())
	return db, ns
}

// assertSingleLeafPin pins the "Only leaf page pinned" invariant
// (NOTES.md:903, 915): in a positioned cursor stack, exactly the top (leaf)
// frame holds a pinned page (pg != nil) and every interior frame below it has
// pg == nil. SQLite pins every page in the stack and relies on
// saveAllCursors/btreeReleaseAllCursorPages to drop them before a mutation; the
// Go design pins only the leaf precisely because it never has to release-and-
// re-seek mid-operation. If a refactor starts pinning interior frames (or stops
// pinning the leaf), the no-save/restore design's assumptions change and this
// fails loudly.
func assertSingleLeafPin(t *testing.T, c *Cursor) {
	t.Helper()
	require.True(t, c.valid, "cursor must be positioned for the pin invariant to apply")
	require.NotEmpty(t, c.stack, "a valid cursor must have a non-empty stack")

	last := len(c.stack) - 1
	for i := range c.stack {
		f := &c.stack[i]
		if i == last {
			require.NotNilf(t, f.pg, "leaf frame %d must pin its page (single-leaf-pin invariant)", i)
			require.Falsef(t, f.pg.header.isInterior(),
				"top frame %d must be a leaf page, got interior pgno=%d", i, f.pgno)
			// The pinned leaf is held with a live reference for the cursor's
			// lifetime — this is exactly the pin SQLite would have dropped via
			// btreeReleaseAllCursorPages before a mutation (btree.c:769-789).
			require.GreaterOrEqualf(t, f.pg.pinCount, 1,
				"leaf frame %d page pgno=%d must stay pinned (pinCount>=1)", i, f.pgno)
		} else {
			require.Nilf(t, f.pg, "interior frame %d must NOT pin a page (only the leaf is pinned)", i)
		}
	}
}

// TestCursorReadOnlyInvariant_OnlyLeafPinned walks the cursor across a
// multi-level tree (First + repeated Next, then Last + repeated Previous) and
// asserts the single-leaf-pin invariant at every position. This is the core
// structural property the no-save/restore drift relies on.
func TestCursorReadOnlyInvariant_OnlyLeafPinned(t *testing.T) {
	db, _ := buildMultiLevelTree(t, 2000)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Forward: the stack must include interior frames at least once (otherwise
	// the "interior frames are unpinned" half of the invariant is vacuous and we
	// have not actually built a multi-level tree).
	cur := rtx.NewCursor(ns)
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())
	sawInterior := false
	for cur.Valid() {
		if len(cur.stack) > 1 {
			sawInterior = true
		}
		assertSingleLeafPin(t, cur)
		require.NoError(t, cur.Next())
	}
	require.True(t, sawInterior,
		"test tree must be multi-level so interior frames exist; increase n")

	// Backward exercises the descend-to-rightmost-leaf branch of Previous().
	cur2 := rtx.NewCursor(ns)
	require.NoError(t, cur2.Last())
	require.True(t, cur2.Valid())
	for cur2.Valid() {
		assertSingleLeafPin(t, cur2)
		require.NoError(t, cur2.Previous())
	}
}

// TestCursorReadOnlyInvariant_NoReSeekStateFrozenPin pins the 2-STATE +
// frozen-pin half of the contract: a positioned cursor stays on the SAME pinned
// *page with a STABLE cellIdx across repeated value reads, and Valid() is exactly
// the 2-state model (no hidden CURSOR_REQUIRESEEK that would silently re-seek and
// swap the pinned page). SQLite's re-seek machinery (saveCursorKey + REQUIRESEEK,
// btree.c:724-789) deliberately invalidates the pinned page; the Go cursor must
// NOT — its readers (Key/Value) point directly into the still-pinned buffer.
func TestCursorReadOnlyInvariant_NoReSeekStateFrozenPin(t *testing.T) {
	db, _ := buildMultiLevelTree(t, 2000)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	cur := rtx.NewCursor(ns)
	require.NoError(t, cur.Seek([]byte("key-001000")))
	require.True(t, cur.Valid())

	// Snapshot the pinned leaf identity and frozen position.
	leaf := &cur.stack[len(cur.stack)-1]
	pinnedPage := leaf.pg
	frozenIdx := leaf.cellIdx
	require.NotNil(t, pinnedPage)

	k0, err := cur.Key()
	require.NoError(t, err)

	// Repeated reads must NOT re-seek (which in SQLite would drop and re-pin the
	// page) — same *page pointer, same cellIdx, identical key/value each time.
	for i := 0; i < 5; i++ {
		require.True(t, cur.Valid(), "read must not flip the 2-state validity")
		require.Same(t, pinnedPage, cur.stack[len(cur.stack)-1].pg,
			"repeated reads must keep the SAME pinned leaf page (no hidden re-seek)")
		require.Equal(t, frozenIdx, cur.stack[len(cur.stack)-1].cellIdx,
			"repeated reads must keep cellIdx frozen (no hidden re-seek)")
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		require.Equal(t, k0, k, "key must be stable across repeated reads")
		_, verr := cur.Value()
		require.NoError(t, verr)
	}

	// Close() is the ONLY thing that drops the leaf pin and clears the position.
	// (Mirrors btreeReleaseAllCursorPages — but driven explicitly by the caller,
	// not implicitly by a mutation.)
	cur.Close()
	require.False(t, cur.Valid(), "Close must move the cursor to the invalid state")
	require.Empty(t, cur.stack, "Close must clear the stack so no released page is re-read")
}

// TestCursorReadOnlyInvariant_WritesBypassCursor pins the write-path
// independence the drift relies on: within a SINGLE write transaction, while a
// cursor is live and positioned on a namespace, WriteTx.Put / WriteTx.Delete
// mutate that SAME namespace through their own writable btree traversal from the
// root (db.go:1834-1849) and NEVER through the cursor. The cursor is purely an
// observer; mutation is not routed through its pinned leaf.
//
// This is the exact "write to the same namespace a live cursor is positioned on,
// within one write transaction" scenario from the invariant statement. We assert
// the design fact that makes the absent saveAllCursors harmless here: the writer
// does its own traversal, so the cursor's pinned page is not the mutation vehicle.
func TestCursorReadOnlyInvariant_WritesBypassCursor(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() {
		if !wtx.closed {
			_ = wtx.Rollback()
		}
	}()
	ns, err := wtx.CreateNamespace("data")
	require.NoError(t, err)

	// Seed a handful of keys so the cursor can position on a real leaf.
	for i := 0; i < 8; i++ {
		require.NoError(t, wtx.Put(ns, fmt.Appendf(nil, "k-%03d", i), []byte("v")))
	}

	// A cursor opened on a WriteTx (WriteTx embeds ReadTx, so NewCursor exists)
	// is still the read-only, writable-snapshot cursor.
	cur := wtx.NewCursor(ns)
	require.True(t, cur.bt.writable,
		"a write-tx cursor reads the writer snapshot (bt.writable) yet exposes no write API")
	require.NoError(t, cur.Seek([]byte("k-004")))
	require.True(t, cur.Valid())
	require.NotNil(t, cur.stack[len(cur.stack)-1].pg)

	// Mutate the SAME namespace via the WriteTx API. This must succeed and must
	// not route through the cursor: Put/Delete build their own bt{writable:true}
	// and traverse from ns.rootPage. We are NOT asserting the cursor stays valid
	// afterwards (the design explicitly does not guarantee that); we are pinning
	// that the write path exists and is independent of the cursor object.
	require.NoError(t, wtx.Put(ns, []byte("k-100"), []byte("inserted")))
	require.NoError(t, wtx.Delete(ns, []byte("k-001")))

	// The Cursor object carries no path into the writer's mutation routines: it
	// has no Put/Delete/Insert/Set/Update/Remove method. A future refactor that
	// adds one (i.e. makes the cursor a write cursor) must consciously revisit
	// the saveAllCursors gap and will trip this guard.
	assertCursorExposesNoWriteMethods(t)

	// After the commit the data reflects the writer's own traversal, confirming
	// the mutations went through Put/Delete, not the cursor.
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	got, err := rtx.Get(ns2, []byte("k-100"))
	require.NoError(t, err)
	require.Equal(t, []byte("inserted"), got)
	_, err = rtx.Get(ns2, []byte("k-001"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// assertCursorExposesNoWriteMethods uses reflection to pin the read-only API
// surface of *Cursor. The Go cursor (NOTES.md:918) has no BTCF_WriteFlag
// equivalent: it must expose only read/seek/navigation methods. The presence of
// any mutation method would mean the cursor became a write cursor, at which point
// the missing saveAllCursors/restore machinery is no longer a benign drift.
func assertCursorExposesNoWriteMethods(t *testing.T) {
	t.Helper()
	forbidden := map[string]struct{}{
		"Put": {}, "Insert": {}, "Set": {}, "Delete": {},
		"Remove": {}, "Update": {}, "Write": {}, "Save": {},
	}
	ct := reflect.TypeOf(&Cursor{})
	for i := 0; i < ct.NumMethod(); i++ {
		name := ct.Method(i).Name
		_, bad := forbidden[name]
		require.Falsef(t, bad,
			"Cursor must remain read-only (no write methods); found mutating method %q — "+
				"if the cursor is now a write cursor, the missing saveAllCursors/restore "+
				"(NOTES #old-drift-readonly-two-state-cursor) must be revisited", name)
	}
}
