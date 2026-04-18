package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
)

// TestExtractDocId covers the corrupt-tuple fallback at index_iter.go:259-263
// and the normal extraction path.
func TestExtractDocId(t *testing.T) {
	a := &anyenc.Arena{}

	t.Run("normal_extraction", func(t *testing.T) {
		var tuple anyenc.Tuple
		tuple = tuple.Append(a.NewString("field0"))
		tuple = tuple.Append(a.NewString("field1"))
		docIdBytes := []byte("doc-42")
		key := append(append([]byte{}, tuple...), docIdBytes...)
		got := extractDocId(anyenc.Tuple(key), 2)
		assert.Equal(t, docIdBytes, []byte(got))
	})
	t.Run("corrupt_tuple_returns_key", func(t *testing.T) {
		// OffsetAfter on a garbage tuple returns error; extractDocId should
		// return the original key as fallback (index_iter.go:261-263).
		bad := anyenc.Tuple([]byte{0xff, 0xff, 0xff})
		got := extractDocId(bad, 2)
		assert.Equal(t, []byte(bad), []byte(got),
			"corrupt tuple must return the original key as fallback")
	})
	t.Run("offset_equals_len_returns_key", func(t *testing.T) {
		// Tuple with exactly N fields and no trailing docId suffix:
		// offset == len(key), so the else branch at index_iter.go:268 fires.
		var tuple anyenc.Tuple
		tuple = tuple.Append(a.NewString("only-field"))
		got := extractDocId(tuple, 1)
		assert.Equal(t, []byte(tuple), []byte(got))
	})
}

// TestIndexIter_String covers IndexIter.String with both forward/reverse and
// with/without bounds.
func TestIndexIter_String(t *testing.T) {
	info := &IndexInfo{Name: "my_idx"}
	t.Run("forward_no_bounds", func(t *testing.T) {
		it := &IndexIter{IdxInfo: info}
		s := it.String()
		assert.Contains(t, s, "IndexScan(my_idx)")
		assert.NotContains(t, s, "reverse")
		assert.NotContains(t, s, "bounds=")
	})
	t.Run("reverse_with_bounds", func(t *testing.T) {
		b := query.Bound{Start: []byte{1}, End: []byte{2}, StartInclude: true, EndInclude: true}
		it := &IndexIter{IdxInfo: info, Reverse: true, Bounds: query.Bounds{b}}
		s := it.String()
		assert.Contains(t, s, "(reverse)")
		assert.Contains(t, s, "bounds=")
	})
}

// TestIndexIter_PerfBranches exercises index_iter.go perf guards and
// asserts counters moved with a real btree-backed index scan.
func TestIndexIter_PerfBranches(t *testing.T) {
	resetPerfCounters()
	setPerfCountersEnabled(true)
	defer func() {
		setPerfCountersEnabled(false)
		resetPerfCounters()
	}()

	db, ns := coverageBtree(t, "idx_perf", []string{"key1", "key2", "key3"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx_perf", FieldNames: []string{"id"}},
	}
	defer it.Close()
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
	}

	s := snapshotPerfCounters()
	assert.Greater(t, s.IndexNextCalls, uint64(0), "IndexNextCalls must be incremented")
	assert.Equal(t, uint64(3), s.IndexYields, "exactly 3 docs yielded")
	assert.Greater(t, s.IndexNextNs, uint64(0), "IndexNextNs must accumulate timing")
}

// TestIndexIter_Forward_WithBounds exercises index_iter.go:86-107
// (forward path with start bound and cursor.Next when StartInclude=false).
func TestIndexIter_Forward_WithBounds(t *testing.T) {
	db, ns := coverageBtree(t, "idx_fwd_bounds",
		[]string{"a", "b", "c", "d", "e"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Bounds: (b, d]  — exclusive start, inclusive end
	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "b"),
		End:          anyenc.AppendAnyValue(nil, "d"),
		StartInclude: false,
		EndInclude:   true,
	}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx_range", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
	}
	defer it.Close()
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"c", "d"}, got, "(b,d] must yield c then d in forward order")
}

// TestIndexIter_Reverse_NoBounds exercises the reverse no-bounds path
// (index_iter.go:155-164).
func TestIndexIter_Reverse_NoBounds(t *testing.T) {
	db, ns := coverageBtree(t, "idx_rev_nobounds", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx_rev", FieldNames: []string{"id"}},
		Reverse: true,
	}
	defer it.Close()
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"c", "b", "a"}, got,
		"reverse no-bounds must yield descending order")
}

// TestIndexIter_Reverse_WithBounds exercises the reverse+bounds path.
func TestIndexIter_Reverse_WithBounds(t *testing.T) {
	db, ns := coverageBtree(t, "idx_rev_bounds",
		[]string{"a", "b", "c", "d", "e"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "b"),
		End:          anyenc.AppendAnyValue(nil, "d"),
		StartInclude: true,
		EndInclude:   true,
	}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx_rev_range", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
		Reverse: true,
	}
	defer it.Close()
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"d", "c", "b"}, got,
		"reverse [b,d] must yield d,c,b")
}

// TestIndexIter_CountEntries_WithBounds exercises the CountEntries batch
// counter with bounds including exclusive-start.
func TestIndexIter_CountEntries_WithBounds(t *testing.T) {
	db, ns := coverageBtree(t, "idx_count",
		[]string{"a", "b", "c", "d", "e"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "b"),
		End:          anyenc.AppendAnyValue(nil, "d"),
		StartInclude: false, // exercises the Next-past-start branch
		EndInclude:   true,
	}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx_count", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
	}
	defer it.Close()
	n, err := it.CountEntries()
	require.NoError(t, err)
	assert.Equal(t, 2, n, "(b,d] must count {c, d}")
}

// TestIndexIter_Forward_StartIncludeTrue covers the branch where the seek
// lands exactly on Start AND StartInclude=true, so the no-skip path at
// index_iter.go:91-96 short-circuits without calling cursor.Next.
func TestIndexIter_Forward_StartIncludeTrue(t *testing.T) {
	db, ns := coverageBtree(t, "idx_start_incl", []string{"b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "b"),
		StartInclude: true,
	}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
	}
	defer it.Close()
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"b", "c"}, got,
		"[b,inf) must yield b,c in forward order (start inclusive)")
}

// TestIndexIter_Reverse_EndPastLastKey covers index_iter.go:64-67 — reverse
// scan where Seek(End) lands past the last key, triggering cursor.Last() fallback.
func TestIndexIter_Reverse_EndPastLastKey(t *testing.T) {
	db, ns := coverageBtree(t, "idx_rev_past_end", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// End = "z" — past every key. Seek("z") → invalid cursor → Last().
	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "a"),
		End:          anyenc.AppendAnyValue(nil, "zzz"),
		StartInclude: true,
		EndInclude:   true,
	}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
		Reverse: true,
	}
	defer it.Close()
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"c", "b", "a"}, got,
		"reverse [a,zzz] via Last() fallback must yield c,b,a")
}

// TestIndexIter_Reverse_EndExclusiveBackUp covers index_iter.go:74-78 — reverse
// scan where Seek(End) lands exactly on End but EndInclude=false, so we
// call Previous to back up.
func TestIndexIter_Reverse_EndExclusiveBackUp(t *testing.T) {
	db, ns := coverageBtree(t, "idx_rev_excl_end", []string{"a", "b", "c", "d"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// End = "c", EndInclude=false. Seek("c") lands on "c" (cmp==0), backs up.
	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "a"),
		End:          anyenc.AppendAnyValue(nil, "c"),
		StartInclude: true,
		EndInclude:   false,
	}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
		Reverse: true,
	}
	defer it.Close()
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"b", "a"}, got,
		"reverse [a,c) must yield b,a (c excluded, Previous back-up)")
}

// TestIndexIter_Reverse_NoEnd covers index_iter.go:82-84 — reverse scan
// with empty End, falls through to cursor.Last().
func TestIndexIter_Reverse_NoEnd(t *testing.T) {
	db, ns := coverageBtree(t, "idx_rev_no_end", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "a"),
		End:          nil, // no upper bound
		StartInclude: true,
	}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
		Reverse: true,
	}
	defer it.Close()
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"c", "b", "a"}, got,
		"reverse [a,inf] must yield c,b,a via Last() fallback")
}

// TestIndexIter_NoBounds_ReverseAlreadyStarted covers the reverse-no-bounds
// `else { Previous }` branch (index_iter.go:166-175) by iterating multiple
// times to reach the started=true loop branch.
func TestIndexIter_NoBounds_ReverseAlreadyStarted(t *testing.T) {
	db, ns := coverageBtree(t, "idx_rev_multistep", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Reverse: true,
	}
	defer it.Close()

	// Consume all entries to ensure the "started → Previous" branch fires.
	var got []string
	for {
		_, docId, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"c", "b", "a"}, got)
}

// ---- Cursor error forcing via invalid namespace (rootPage=0) ----
//
// A zero-value *btree.Namespace has rootPage=0, which is the header page
// and not a valid btree root. Cursor operations against it return
// "btree: invalid page number" errors — perfect for covering the
// cursor-error arms that are otherwise unreachable.

// TestIndexIter_NoBounds_Forward_FirstErr covers index_iter.go:160 (First
// error) via an invalid namespace.
func TestIndexIter_NoBounds_Forward_FirstErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page",
		"error must originate in the cursor call, not NewCursor/extractResult")
}

// TestIndexIter_NoBounds_Reverse_LastErr covers index_iter.go:156 (Last error).
func TestIndexIter_NoBounds_Reverse_LastErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Reverse: true,
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestIndexIter_Bounded_Forward_SeekErr covers index_iter.go:87 (Seek(Start)
// error).
func TestIndexIter_Bounded_Forward_SeekErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{Start: anyenc.AppendAnyValue(nil, "a"), StartInclude: true}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestIndexIter_Bounded_Forward_FirstErr covers index_iter.go:101-103 (First
// error when Start is empty).
func TestIndexIter_Bounded_Forward_FirstErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{End: anyenc.AppendAnyValue(nil, "z"), EndInclude: true}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestIndexIter_Bounded_Reverse_SeekErr covers index_iter.go:60 (Seek(End) err).
func TestIndexIter_Bounded_Reverse_SeekErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{End: anyenc.AppendAnyValue(nil, "z"), EndInclude: true}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
		Reverse: true,
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestIndexIter_Bounded_Reverse_LastErr covers index_iter.go:81-83 (Last error
// when End is empty in reverse).
func TestIndexIter_Bounded_Reverse_LastErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{Start: anyenc.AppendAnyValue(nil, "a"), StartInclude: true}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
		Reverse: true,
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestIndexIter_CountEntries_SeekErr covers CountEntries seek error paths.
func TestIndexIter_CountEntries_SeekErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{Start: anyenc.AppendAnyValue(nil, "a"), StartInclude: true}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
	}
	defer it.Close()
	_, err = it.CountEntries()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestIndexIter_CountEntries_FirstErr covers CountEntries First error path.
func TestIndexIter_CountEntries_FirstErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bound := query.Bound{End: anyenc.AppendAnyValue(nil, "z"), EndInclude: true}
	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		IdxInfo: &IndexInfo{Name: "idx", FieldNames: []string{"id"}},
		Bounds:  query.Bounds{bound},
	}
	defer it.Close()
	_, err = it.CountEntries()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}
