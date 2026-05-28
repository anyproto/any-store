package qplanner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
)

// boundForValue constructs a PointLookup bound for the given scalar value
// (single-field index key shape). Same as how planner.go produces $in
// bounds: Start = tuple(value), End = tuple(value) + 0xff (the post-
// AdjustBoundsForNonUnique form).
func boundForValue(v string) query.Bound {
	start := anyenc.AppendAnyValue(nil, v)
	end := append(append([]byte{}, start...), 0xff)
	return query.Bound{Start: start, End: end, StartInclude: true, EndInclude: true}
}

// mkCursors opens k cursors on ns, seeks each to its bound, and returns
// them along with the bound slice. The caller transfers ownership to the
// merge.
func mkCursors(t *testing.T, tx *btree.ReadTx, ns *btree.Namespace, vals []string) ([]*btree.Cursor, query.Bounds) {
	t.Helper()
	bounds := make(query.Bounds, len(vals))
	cursors := make([]*btree.Cursor, len(vals))
	for i, v := range vals {
		bounds[i] = boundForValue(v)
		c := tx.NewCursor(ns)
		require.NoError(t, c.Seek(bounds[i].Start))
		cursors[i] = c
	}
	return cursors, bounds
}

// drainMerge collects all docIds the merge yields.
func drainMerge(t *testing.T, m *kWayDocIdMergeIter) []string {
	t.Helper()
	var out []string
	for {
		id, ok, err := m.Next()
		require.NoError(t, err)
		if !ok {
			return out
		}
		// Copy because the returned slice is invalidated on next Next.
		out = append(out, string(append([]byte{}, id...)))
	}
}

func TestKWayMerge_DisjointBounds(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueMultiKey},
		{field: "a", docId: "d4", value: IndexValueMultiKey},
		{field: "a", docId: "d7", value: IndexValueMultiKey},
		{field: "b", docId: "d2", value: IndexValueMultiKey},
		{field: "b", docId: "d5", value: IndexValueMultiKey},
		{field: "c", docId: "d3", value: IndexValueMultiKey},
		{field: "c", docId: "d6", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"a", "b", "c"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	defer m.Close()

	got := drainMerge(t, m)
	require.Equal(t, []string{"d1", "d2", "d3", "d4", "d5", "d6", "d7"}, got)
}

func TestKWayMerge_OverlappingBounds(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueMultiKey},
		{field: "a", docId: "d2", value: IndexValueMultiKey},
		{field: "b", docId: "d1", value: IndexValueMultiKey},
		{field: "b", docId: "d3", value: IndexValueMultiKey},
		{field: "c", docId: "d2", value: IndexValueMultiKey},
		{field: "c", docId: "d3", value: IndexValueMultiKey},
		{field: "c", docId: "d4", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"a", "b", "c"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	defer m.Close()

	got := drainMerge(t, m)
	require.Equal(t, []string{"d1", "d2", "d3", "d4"}, got)
}

func TestKWayMerge_HeavyOverlap200Docs(t *testing.T) {
	// 200 docs, each tagged with "shared" + a unique "t-N".
	// Query: $in:["shared","t-5","t-10"].
	// bound 0 (shared): all 200 docs
	// bound 1 (t-5):    d005 only
	// bound 2 (t-10):   d010 only
	// distinct: 200 (d000..d199)
	var entries []indexEntry
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("d%03d", i)
		entries = append(entries, indexEntry{
			field: "shared", docId: id, value: IndexValueMultiKey,
		})
	}
	entries = append(entries,
		indexEntry{field: "t-5", docId: "d005", value: IndexValueMultiKey},
		indexEntry{field: "t-10", docId: "d010", value: IndexValueMultiKey},
	)
	db, ns := indexEntryBtree(t, entries)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"shared", "t-5", "t-10"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	defer m.Close()

	got := drainMerge(t, m)
	require.Len(t, got, 200)
	// Verify ascending docId order.
	for i := 1; i < len(got); i++ {
		require.Less(t, got[i-1], got[i])
	}
}

func TestKWayMerge_LegacyNilValueEntries(t *testing.T) {
	// Mix scalar (value byte 0x00), multi-key (0x01), and legacy (nil)
	// entries. The merge does NOT read the value byte; it dedups by docId.
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueScalar},
		{field: "a", docId: "d2", value: nil}, // legacy
		{field: "a", docId: "d3", value: IndexValueMultiKey},
		{field: "b", docId: "d2", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"a", "b"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	defer m.Close()

	got := drainMerge(t, m)
	require.Equal(t, []string{"d1", "d2", "d3"}, got)
}

func TestKWayMerge_Close_Idempotent(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"a"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	m.Close()
	m.Close() // must not panic; must not double-close cursors
}

func TestKWayMerge_K1_Degenerate(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueMultiKey},
		{field: "a", docId: "d2", value: IndexValueMultiKey},
		{field: "a", docId: "d3", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"a"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	defer m.Close()

	got := drainMerge(t, m)
	require.Equal(t, []string{"d1", "d2", "d3"}, got)
}

func TestKWayMerge_EmptyBounds(t *testing.T) {
	// All three bounds reference non-existent values → all cursors
	// immediately exhausted → merge returns nothing in O(k).
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "x", docId: "d1", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"absent-a", "absent-b", "absent-c"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	defer m.Close()

	got := drainMerge(t, m)
	require.Empty(t, got)
}

// TestKWayMerge_FlatAllocs pins the alloc target: O(k + log k) heap setup
// + O(1) per emission (no per-emission docId copy thanks to two-buffer
// scheme). At 200 emissions × k=3, expect well under 100 allocs total.
func TestKWayMerge_FlatAllocs(t *testing.T) {
	var entries []indexEntry
	for i := 0; i < 200; i++ {
		entries = append(entries, indexEntry{
			field: "shared", docId: fmt.Sprintf("d%03d", i), value: IndexValueMultiKey,
		})
	}
	db, ns := indexEntryBtree(t, entries)

	allocs := testing.AllocsPerRun(5, func() {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()

		cursors, bounds := mkCursors(t, rtx, ns, []string{"shared", "absent-a", "absent-b"})
		m := newKWayDocIdMergeIter(cursors, bounds, 1)
		defer m.Close()

		for {
			_, ok, err := m.Next()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
		}
	})
	// Budget: ~3 cursor allocs + heap setup + two-buffer growth + tx
	// setup. Real allocs at this scale ~60-100. Headroom of 200 catches
	// a per-emission alloc regression (would blow to ~250).
	require.Less(t, allocs, float64(200),
		"per-emission alloc regression suspected; got %.0f", allocs)
}

// TestKWayMerge_DispatchCounter pins the perf-counter wiring.
func TestKWayMerge_DispatchCounter(t *testing.T) {
	EnablePerfCounters(true)
	defer EnablePerfCounters(false)
	ResetPerfCounters()

	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cursors, bounds := mkCursors(t, rtx, ns, []string{"a"})
	m := newKWayDocIdMergeIter(cursors, bounds, 1)
	m.Close()

	snap := SnapshotPerfCounters()
	require.Equal(t, uint64(1), snap.MergeDispatches)
}
