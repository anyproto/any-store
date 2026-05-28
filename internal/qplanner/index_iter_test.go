package qplanner

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/query"
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
		_, docId, _, err := it.Next()
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
	_, _, _, err = it.Next()
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
	_, _, _, err = it.Next()
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
	_, _, _, err = it.Next()
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
	_, _, _, err = it.Next()
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
	_, _, _, err = it.Next()
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
	_, _, _, err = it.Next()
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

// indexEntryBtree opens an in-memory btree namespace and writes index-shaped
// entries with explicit per-entry value bytes. Used to exercise the
// EntryValueIsMultiKey decode path of IndexIter without depending on the
// anystore package's insertKeys.
func indexEntryBtree(t *testing.T, entries []indexEntry) (*btree.DB, *btree.Namespace) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ix.db")
	db, err := btree.Open(path, btree.Options{PageSize: 4096, CacheSize: 128, InMemory: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("ix")
	require.NoError(t, err)
	for _, e := range entries {
		key := append(append([]byte{}, anyenc.AppendAnyValue(nil, e.field)...), []byte(e.docId)...)
		require.NoError(t, wtx.Put(ns, key, e.value))
	}
	require.NoError(t, wtx.Commit())
	return db, ns
}

type indexEntry struct {
	field string // single-field index: scalar field value
	docId string
	value []byte // per-entry value byte: nil (legacy), {0} scalar, {1} multi-key
}

// TestIndexIter_Next_DecodesMultiKeyBit_Scalar pins that an IndexIter walking
// entries written with IndexValueScalar reports multiKey=false.
func TestIndexIter_Next_DecodesMultiKeyBit_Scalar(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},
		{field: "b", docId: "p2", value: IndexValueScalar},
		{field: "c", docId: "p3", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	for i := 0; i < 3; i++ {
		_, docId, mk, err := it.Next()
		require.NoError(t, err)
		require.NotNil(t, docId)
		assert.False(t, mk, "scalar entry %d must report multiKey=false", i)
	}
}

// TestIndexIter_Next_DecodesMultiKeyBit_MultiKey pins the multi-key decode.
func TestIndexIter_Next_DecodesMultiKeyBit_MultiKey(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "tag-a", docId: "p1", value: IndexValueMultiKey},
		{field: "tag-b", docId: "p1", value: IndexValueMultiKey},
		{field: "tag-c", docId: "p1", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	for i := 0; i < 3; i++ {
		_, _, mk, err := it.Next()
		require.NoError(t, err)
		assert.True(t, mk, "multi-key entry %d must report multiKey=true", i)
	}
}

// TestIndexIter_Next_DecodesMultiKeyBit_LegacyEmpty pins the safety
// behaviour for entries written with empty values (pre-bit format): they
// must report multiKey=true so the planner uses the dedup path.
func TestIndexIter_Next_DecodesMultiKeyBit_LegacyEmpty(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: nil}, // legacy: nil value
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	_, _, mk, err := it.Next()
	require.NoError(t, err)
	assert.True(t, mk, "legacy entry (nil value) must report multiKey=true conservatively")
}

// TestEntryValueIsMultiKey covers the decode helper at every recognised
// input shape: empty, scalar, multi-key, and a future-format byte with
// reserved bits set.
func TestEntryValueIsMultiKey(t *testing.T) {
	cases := []struct {
		name string
		val  []byte
		want bool
	}{
		{"empty", []byte{}, true},
		{"nil", nil, true},
		{"scalar", []byte{0x00}, false},
		{"multikey", []byte{0x01}, true},
		{"future_reserved_bits_only", []byte{0x02}, false}, // bit 0 clear → scalar
		{"future_with_multikey", []byte{0x03}, true},       // bit 0 set
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, EntryValueIsMultiKey(c.val))
		})
	}
}

// TestIndexIter_CountEntries_MultiBound_ScalarDocs pins the stream-count
// path for multi-bound queries on indexes whose entries are all scalar
// (no doc has more than one entry). No seen-set should be allocated; the
// count equals the entry count.
func TestIndexIter_CountEntries_MultiBound_ScalarDocs(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "x", docId: "p1", value: IndexValueScalar},
		{field: "y", docId: "p2", value: IndexValueScalar},
		{field: "z", docId: "p3", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Bounds use the standard non-unique adjustment: End gets a trailing
	// 0xff so docId-suffixed entries within the value prefix are included.
	// See AdjustBoundsForNonUnique.
	mk := func(s string) []byte { return anyenc.AppendAnyValue(nil, s) }
	mkEnd := func(s string) []byte { return append(anyenc.AppendAnyValue(nil, s), 0xff) }
	_ = mkEnd
	bounds := query.Bounds{
		{Start: mk("x"), End: mkEnd("x"), StartInclude: true, EndInclude: true},
		{Start: mk("y"), End: mkEnd("y"), StartInclude: true, EndInclude: true},
		{Start: mk("z"), End: mkEnd("z"), StartInclude: true, EndInclude: true},
	}

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
		Bounds:  bounds,
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	assert.Equal(t, 3, n, "3 distinct scalar docs across 3 bounds")
}

// TestIndexIter_CountEntries_MultiBound_MultiKeyDeduped pins that
// overlapping multi-bound queries on a multi-key index dedup correctly:
// a doc whose array values appear in multiple bounds is counted once.
func TestIndexIter_CountEntries_MultiBound_MultiKeyDeduped(t *testing.T) {
	// Doc p1 has array values "a" and "b" → 2 entries, both multi-key.
	// Doc p2 has array values "b" and "c" → 2 entries, both multi-key.
	// Query bounds {a, b, c} should count 2 distinct docs (p1, p2),
	// not 4 entries.
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueMultiKey},
		{field: "b", docId: "p1", value: IndexValueMultiKey},
		{field: "b", docId: "p2", value: IndexValueMultiKey},
		{field: "c", docId: "p2", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Bounds use the standard non-unique adjustment: End gets a trailing
	// 0xff so docId-suffixed entries within the value prefix are included.
	// See AdjustBoundsForNonUnique.
	mk := func(s string) []byte { return anyenc.AppendAnyValue(nil, s) }
	mkEnd := func(s string) []byte { return append(anyenc.AppendAnyValue(nil, s), 0xff) }
	_ = mkEnd
	bounds := query.Bounds{
		{Start: mk("a"), End: mkEnd("a"), StartInclude: true, EndInclude: true},
		{Start: mk("b"), End: mkEnd("b"), StartInclude: true, EndInclude: true},
		{Start: mk("c"), End: mkEnd("c"), StartInclude: true, EndInclude: true},
	}

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
		Bounds:  bounds,
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	assert.Equal(t, 2, n, "p1 and p2 each counted once across overlapping bounds")
}

// TestIndexIter_CountEntries_MultiBound_Mixed pins the mixed case: some
// docs have one entry (scalar bit), others multiple (multi-key bit). The
// scalar entries stream-count without touching the seen-set; only the
// multi-key entries dedup.
func TestIndexIter_CountEntries_MultiBound_Mixed(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		// p1 has one scalar entry "a"
		{field: "a", docId: "p1", value: IndexValueScalar},
		// p2 has two multi-key entries "b" and "c"
		{field: "b", docId: "p2", value: IndexValueMultiKey},
		{field: "c", docId: "p2", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Bounds use the standard non-unique adjustment: End gets a trailing
	// 0xff so docId-suffixed entries within the value prefix are included.
	// See AdjustBoundsForNonUnique.
	mk := func(s string) []byte { return anyenc.AppendAnyValue(nil, s) }
	mkEnd := func(s string) []byte { return append(anyenc.AppendAnyValue(nil, s), 0xff) }
	_ = mkEnd
	bounds := query.Bounds{
		{Start: mk("a"), End: mkEnd("a"), StartInclude: true, EndInclude: true},
		{Start: mk("b"), End: mkEnd("b"), StartInclude: true, EndInclude: true},
		{Start: mk("c"), End: mkEnd("c"), StartInclude: true, EndInclude: true},
	}

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
		Bounds:  bounds,
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	assert.Equal(t, 2, n, "p1 (scalar) + p2 (multi-key, deduped) = 2")
}

// TestIndexIter_CountEntries_MultiBound_LegacyEntries pins the
// conservative path for legacy entries (nil value): treated as multi-key
// and routed through the seen-set so a doc with multiple legacy entries
// is counted once.
func TestIndexIter_CountEntries_MultiBound_LegacyEntries(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: nil},
		{field: "b", docId: "p1", value: nil},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Bounds use the standard non-unique adjustment: End gets a trailing
	// 0xff so docId-suffixed entries within the value prefix are included.
	// See AdjustBoundsForNonUnique.
	mk := func(s string) []byte { return anyenc.AppendAnyValue(nil, s) }
	mkEnd := func(s string) []byte { return append(anyenc.AppendAnyValue(nil, s), 0xff) }
	_ = mkEnd
	bounds := query.Bounds{
		{Start: mk("a"), End: mkEnd("a"), StartInclude: true, EndInclude: true},
		{Start: mk("b"), End: mkEnd("b"), StartInclude: true, EndInclude: true},
	}

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
		Bounds:  bounds,
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"legacy entries are conservatively multi-key — p1's two entries dedup to 1")
}

// TestIndexIter_CountEntries_SingleBound_UsesBatchPath pins that the
// single-bound count keeps the page-batch fast path (no value reads, no
// seen-set). The multi-key bit is irrelevant here because within-doc
// dedup ensures one entry per distinct value per doc.
func TestIndexIter_CountEntries_SingleBound_UsesBatchPath(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "v", docId: "p1", value: IndexValueScalar},
		{field: "v", docId: "p2", value: IndexValueScalar},
		{field: "v", docId: "p3", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Bounds use the standard non-unique adjustment: End gets a trailing
	// 0xff so docId-suffixed entries within the value prefix are included.
	// See AdjustBoundsForNonUnique.
	mk := func(s string) []byte { return anyenc.AppendAnyValue(nil, s) }
	mkEnd := func(s string) []byte { return append(anyenc.AppendAnyValue(nil, s), 0xff) }
	_ = mkEnd
	bounds := query.Bounds{
		{Start: mk("v"), End: mkEnd("v"), StartInclude: true, EndInclude: true},
	}

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
		Bounds:  bounds,
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	assert.Equal(t, 3, n)
}

// drainIndexDocIds drains an IndexIter to completion, returning the docId
// strings and the multiKey flag observed for each emitted entry.
func drainIndexDocIds(t *testing.T, it *IndexIter) (ids []string, multi []bool) {
	t.Helper()
	for {
		_, docId, mk, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		ids = append(ids, string(docId))
		multi = append(multi, mk)
	}
	return ids, multi
}

// TestIndexIter_SkipOffset_AllScalar verifies the cursor-level offset skip on
// an unbounded single-field scalar index: skipOffset(k) absorbs exactly k
// rows (remaining=0) and the subsequent Next() stream resumes at row k. This
// is the fast path that streams OFFSET without fetching the skipped docs.
func TestIndexIter_SkipOffset_AllScalar(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},
		{field: "b", docId: "p2", value: IndexValueScalar},
		{field: "c", docId: "p3", value: IndexValueScalar},
		{field: "d", docId: "p4", value: IndexValueScalar},
		{field: "e", docId: "p5", value: IndexValueScalar},
		{field: "f", docId: "p6", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	remaining, err := it.skipOffset(3)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining, "all 3 skipped entries are scalar → fully absorbed")

	ids, _ := drainIndexDocIds(t, it)
	assert.Equal(t, []string{"p4", "p5", "p6"}, ids,
		"after skipping 3 scalar rows, the stream must resume at the 4th row")
}

// TestIndexIter_SkipOffset_PastEnd verifies that an offset beyond the entry
// count skips everything and yields an empty stream, reporting the unskipped
// remainder.
func TestIndexIter_SkipOffset_PastEnd(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},
		{field: "b", docId: "p2", value: IndexValueScalar},
		{field: "c", docId: "p3", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	remaining, err := it.skipOffset(10)
	require.NoError(t, err)
	assert.Equal(t, 7, remaining, "only 3 scalar rows exist; 7 of the 10 are unskipped")

	ids, _ := drainIndexDocIds(t, it)
	assert.Empty(t, ids, "offset past the end yields nothing")
}

// TestIndexIter_SkipOffset_StopsAtMultiKey is the dangerous-case guard: when
// the skip region contains a multi-key (array) entry, the fast skip MUST stop
// at it, return the unskipped remainder, and leave the cursor ON that entry so
// the dedup-aware path resolves the rest of the offset. Skipping multi-key
// entries at the index level would mis-count logical rows.
func TestIndexIter_SkipOffset_StopsAtMultiKey(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},   // skipped (scalar)
		{field: "b", docId: "p2", value: IndexValueScalar},   // skipped (scalar)
		{field: "m1", docId: "p3", value: IndexValueMultiKey}, // STOP here
		{field: "m2", docId: "p3", value: IndexValueMultiKey},
		{field: "z", docId: "p4", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	// Ask to skip 4. Two scalar entries are skipped, then the multi-key entry
	// stops the fast skip: remaining = 4 - 2 = 2.
	remaining, err := it.skipOffset(4)
	require.NoError(t, err)
	assert.Equal(t, 2, remaining, "stopped at the first multi-key entry after 2 scalar skips")

	// The cursor must be left ON the multi-key entry, so Next() emits it first.
	ids, multi := drainIndexDocIds(t, it)
	assert.Equal(t, []string{"p3", "p3", "p4"}, ids,
		"stream must resume exactly at the multi-key entry that stopped the skip")
	assert.Equal(t, []bool{true, true, false}, multi,
		"multiKey flags must be preserved through the handoff")
}

// TestIndexIter_SkipOffset_FirstEntryMultiKey verifies immediate bail when the
// very first entry is multi-key: nothing is skipped and the full offset is
// returned as remaining, with the cursor positioned at the first entry.
func TestIndexIter_SkipOffset_FirstEntryMultiKey(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "m1", docId: "p1", value: IndexValueMultiKey},
		{field: "m2", docId: "p1", value: IndexValueMultiKey},
		{field: "z", docId: "p2", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	remaining, err := it.skipOffset(2)
	require.NoError(t, err)
	assert.Equal(t, 2, remaining, "first entry is multi-key → skip nothing")

	ids, _ := drainIndexDocIds(t, it)
	assert.Equal(t, []string{"p1", "p1", "p2"}, ids, "no entry consumed by the bailed skip")
}

// TestIndexIter_SkipOffset_LegacyEmptyBails verifies that a legacy empty value
// byte (treated as multi-key for safety) stops the fast skip.
func TestIndexIter_SkipOffset_LegacyEmptyBails(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},
		{field: "b", docId: "p2", value: nil}, // legacy empty → multi-key for safety
		{field: "c", docId: "p3", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	remaining, err := it.skipOffset(3)
	require.NoError(t, err)
	assert.Equal(t, 2, remaining, "1 scalar skipped, then legacy-empty entry stops the skip")

	ids, _ := drainIndexDocIds(t, it)
	assert.Equal(t, []string{"p2", "p3"}, ids)
}

// TestIndexIter_SkipOffset_Reverse verifies the cursor-level skip on a reverse
// scan: it skips from the largest key backward.
func TestIndexIter_SkipOffset_Reverse(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},
		{field: "b", docId: "p2", value: IndexValueScalar},
		{field: "c", docId: "p3", value: IndexValueScalar},
		{field: "d", docId: "p4", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
		Reverse: true,
	}
	defer it.Close()

	// Reverse order is d,c,b,a. Skip 2 → d,c skipped; resume at b,a.
	remaining, err := it.skipOffset(2)
	require.NoError(t, err)
	assert.Equal(t, 0, remaining)

	ids, _ := drainIndexDocIds(t, it)
	assert.Equal(t, []string{"p2", "p1"}, ids, "reverse skip drops the 2 largest, resumes at b,a")
}

// TestIndexIter_SkipOffset_BoundedNoSkip verifies the conservative scope: a
// bounded index scan does NOT fast-skip (returns the full offset), preserving
// the safe fetch-then-skip path. The cursor is untouched, so a normal Next()
// still walks the bounded range from the start.
func TestIndexIter_SkipOffset_BoundedNoSkip(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},
		{field: "b", docId: "p2", value: IndexValueScalar},
		{field: "c", docId: "p3", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
		Bounds: query.Bounds{{
			Start: anyenc.AppendAnyValue(nil, "a"), StartInclude: true,
			End: append(anyenc.AppendAnyValue(nil, "c"), 0xff), EndInclude: true,
		}},
	}
	defer it.Close()

	remaining, err := it.skipOffset(2)
	require.NoError(t, err)
	assert.Equal(t, 2, remaining, "bounded scan must not fast-skip; returns full offset")

	ids, _ := drainIndexDocIds(t, it)
	assert.Equal(t, []string{"p1", "p2", "p3"}, ids, "Next() still walks the full bounded range")
}

// TestIndexIter_SkipOffset_ZeroAndNegative verifies the trivial guards.
func TestIndexIter_SkipOffset_ZeroAndNegative(t *testing.T) {
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "p1", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "ix", FieldNames: []string{"f"}},
	}
	defer it.Close()

	r0, err := it.skipOffset(0)
	require.NoError(t, err)
	assert.Equal(t, 0, r0)
	rNeg, err := it.skipOffset(-5)
	require.NoError(t, err)
	assert.Equal(t, 0, rNeg)

	// Cursor untouched: Next() still yields the single entry.
	ids, _ := drainIndexDocIds(t, it)
	assert.Equal(t, []string{"p1"}, ids)
}

// pointLookupBoundForValue mirrors the post-AdjustBoundsForNonUnique shape
// for a single-field equality bound: Start = tuple(value),
// End = tuple(value) + 0xff (to capture the docId suffix appended by
// non-unique index keys). This matches how planner.go produces bounds for
// $in over a non-unique index after AdjustBoundsForNonUnique runs.
func pointLookupBoundForValue(v string) query.Bound {
	start := anyenc.AppendAnyValue(nil, v)
	end := append(append([]byte{}, start...), 0xff)
	return query.Bound{Start: start, End: end, StartInclude: true, EndInclude: true}
}

// TestIndexIter_CountEntries_PreSizedSeenSet_NoSketch pins that the
// CountUntil pre-pass produces a correctly-sized seen-set in the
// no-sketch case. Uses a real btree fixture populated with multi-key
// entries (value byte = IndexValueMultiKey) across multiple bounds for
// the same docs, so dedup is forced and the seen-set actually grows.
func TestIndexIter_CountEntries_PreSizedSeenSet_NoSketch(t *testing.T) {
	// 1000 docs, each contributing entries on 3 tag values. k=3 → forces
	// multi-bound path. Confirm distinct count = 1000 with capacity hint.
	entries := make([]indexEntry, 0, 3000)
	for i := 0; i < 1000; i++ {
		for _, tag := range []string{"a", "b", "c"} {
			entries = append(entries, indexEntry{
				field: tag,
				docId: fmt.Sprintf("d%04d", i),
				value: IndexValueMultiKey,
			})
		}
	}
	db, ns := indexEntryBtree(t, entries)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo: &IndexInfo{Name: "tags", FieldNames: []string{"f"}},
		Bounds: query.Bounds{
			pointLookupBoundForValue("a"),
			pointLookupBoundForValue("b"),
			pointLookupBoundForValue("c"),
		},
		PointLookup: true,
		// Sketch deliberately nil — exercises the CountUntil pre-pass.
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	require.Equal(t, 1000, n)
}

// TestIndexIter_CountEntries_PreSizedSeenSet_FlatAllocs pins the alloc
// profile after pre-sizing: the map should not grow during the walk.
// Compares against the unsized baseline (lazy 64-cap doublings to ~1024).
func TestIndexIter_CountEntries_PreSizedSeenSet_FlatAllocs(t *testing.T) {
	// 1000 docs × 3 entries per doc = 3000 entries. Without pre-sizing,
	// the map grows from cap 64 through 8 doublings to reach 1000-element
	// capacity (~8 map-growth allocs). With pre-sizing: 1 alloc.
	entries := make([]indexEntry, 0, 3000)
	for i := 0; i < 1000; i++ {
		for _, tag := range []string{"a", "b", "c"} {
			entries = append(entries, indexEntry{
				field: tag,
				docId: fmt.Sprintf("d%04d", i),
				value: IndexValueMultiKey,
			})
		}
	}
	db, ns := indexEntryBtree(t, entries)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	mkIter := func() *IndexIter {
		return &IndexIter{
			Source:  &CursorSource{Tx: rtx, Ns: ns},
			IdxInfo: &IndexInfo{Name: "tags", FieldNames: []string{"f"}},
			Bounds: query.Bounds{
				pointLookupBoundForValue("a"),
				pointLookupBoundForValue("b"),
				pointLookupBoundForValue("c"),
			},
			PointLookup: true,
		}
	}

	allocs := testing.AllocsPerRun(3, func() {
		it := mkIter()
		defer it.Close()
		_, _ = it.CountEntries()
	})
	// Per-entry string(docId) alloc count = 3000 (untouched until merge).
	// Map-growth alloc count: lazy unsized → ~8 doublings; pre-sized → ~1.
	// Use a budget that exercises the win without being noise-sensitive.
	require.Less(t, allocs, float64(3050),
		"pre-sized map must save ~8 doublings vs unsized; got %.0f allocs", allocs)
}

func TestIndexIter_CountEntries_RoutesViaMerge(t *testing.T) {
	EnablePerfCounters(true)
	defer EnablePerfCounters(false)
	ResetPerfCounters()

	// 3 docs, each with two tags overlapping across bounds.
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueMultiKey},
		{field: "b", docId: "d1", value: IndexValueMultiKey},
		{field: "b", docId: "d2", value: IndexValueMultiKey},
		{field: "c", docId: "d2", value: IndexValueMultiKey},
		{field: "a", docId: "d3", value: IndexValueMultiKey},
		{field: "c", docId: "d3", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:      &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo:     &IndexInfo{Name: "tags", FieldNames: []string{"f"}},
		Bounds:      query.Bounds{boundForValue("a"), boundForValue("b")},
		PointLookup: true,
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	require.Equal(t, 3, n) // d1, d2, d3
	require.Equal(t, uint64(1), SnapshotPerfCounters().MergeDispatches,
		"must have routed through the merge")
}

func TestIndexIter_CountEntries_FallsBackWhenKExceedsMax(t *testing.T) {
	EnablePerfCounters(true)
	defer EnablePerfCounters(false)
	ResetPerfCounters()

	// 65 bounds > kWayMergeMax=64. Must use the pre-sized seen-set path.
	var entries []indexEntry
	for i := 0; i < 65; i++ {
		entries = append(entries, indexEntry{
			field: fmt.Sprintf("t%02d", i),
			docId: fmt.Sprintf("d%02d", i),
			value: IndexValueMultiKey,
		})
	}
	db, ns := indexEntryBtree(t, entries)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	bounds := make(query.Bounds, 65)
	for i := range bounds {
		bounds[i] = boundForValue(fmt.Sprintf("t%02d", i))
	}
	it := &IndexIter{
		Source:      &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo:     &IndexInfo{Name: "tags", FieldNames: []string{"f"}},
		Bounds:      bounds,
		PointLookup: true,
	}
	defer it.Close()

	n, err := it.CountEntries()
	require.NoError(t, err)
	require.Equal(t, 65, n)
	require.Equal(t, uint64(0), SnapshotPerfCounters().MergeDispatches,
		"k > kWayMergeMax must skip the merge")
}

func TestIndexIter_CountEntries_FallsBackForCompoundIndex(t *testing.T) {
	EnablePerfCounters(true)
	defer EnablePerfCounters(false)
	ResetPerfCounters()

	// Compound (a, b) index → FieldNames has 2 elements; merge MUST NOT
	// engage.
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a1", docId: "d1", value: IndexValueMultiKey},
		{field: "a2", docId: "d2", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:      &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo:     &IndexInfo{Name: "ab", FieldNames: []string{"a", "b"}}, // compound
		Bounds:      query.Bounds{boundForValue("a1"), boundForValue("a2")},
		PointLookup: true,
	}
	defer it.Close()
	_, err = it.CountEntries()
	require.NoError(t, err)
	require.Equal(t, uint64(0), SnapshotPerfCounters().MergeDispatches,
		"compound index must skip the merge")
}

func TestIndexIter_CountEntries_FallsBackForAllScalar(t *testing.T) {
	EnablePerfCounters(true)
	defer EnablePerfCounters(false)
	ResetPerfCounters()

	// All-scalar entries — the existing peek-then-batch in
	// countEntriesWithDedup is faster; merge MUST NOT engage.
	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueScalar},
		{field: "b", docId: "d2", value: IndexValueScalar},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:      &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo:     &IndexInfo{Name: "f", FieldNames: []string{"f"}},
		Bounds:      query.Bounds{boundForValue("a"), boundForValue("b")},
		PointLookup: true,
	}
	defer it.Close()
	_, err = it.CountEntries()
	require.NoError(t, err)
	require.Equal(t, uint64(0), SnapshotPerfCounters().MergeDispatches,
		"all-scalar bounds must skip the merge")
}

// TestIndexIter_CountEntries_KillSwitch pins that SetKWayMergeMax(0)
// forces all multi-bound multi-key counts through the pre-sized
// seen-set path — the runtime escape hatch.
func TestIndexIter_CountEntries_KillSwitch(t *testing.T) {
	EnablePerfCounters(true)
	defer EnablePerfCounters(false)
	ResetPerfCounters()

	prev := SetKWayMergeMax(0)
	defer SetKWayMergeMax(prev)

	db, ns := indexEntryBtree(t, []indexEntry{
		{field: "a", docId: "d1", value: IndexValueMultiKey},
		{field: "b", docId: "d2", value: IndexValueMultiKey},
	})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	it := &IndexIter{
		Source:      &CursorSource{Tx: rtx, Ns: ns},
		IdxInfo:     &IndexInfo{Name: "tags", FieldNames: []string{"f"}},
		Bounds:      query.Bounds{boundForValue("a"), boundForValue("b")},
		PointLookup: true,
	}
	defer it.Close()
	_, err = it.CountEntries()
	require.NoError(t, err)
	require.Equal(t, uint64(0), SnapshotPerfCounters().MergeDispatches,
		"kill switch must skip the merge")
}

// TestIndexIter_CountEntries_MergeAllocsBudget caps allocs to ~150 at
// k=3 with substantial entries — the headline alloc-reduction claim.
func TestIndexIter_CountEntries_MergeAllocsBudget(t *testing.T) {
	// 200 docs × 3 entries (= 600 entries through the merge).
	var entries []indexEntry
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("d%03d", i)
		for _, tag := range []string{"a", "b", "c"} {
			entries = append(entries, indexEntry{
				field: tag, docId: id, value: IndexValueMultiKey,
			})
		}
	}
	db, ns := indexEntryBtree(t, entries)

	allocs := testing.AllocsPerRun(5, func() {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()

		it := &IndexIter{
			Source:      &CursorSource{Tx: rtx, Ns: ns},
			IdxInfo:     &IndexInfo{Name: "tags", FieldNames: []string{"f"}},
			Bounds:      query.Bounds{boundForValue("a"), boundForValue("b"), boundForValue("c")},
			PointLookup: true,
		}
		defer it.Close()
		_, _ = it.CountEntries()
	})
	// Budget: tx open + 3 cursor allocs + heap + two-buffer + counter.
	// The seen-set walk for the same shape allocs ~600 docId strings +
	// 8 map doublings ≈ ~608. The merge should land 6× under that.
	require.Less(t, allocs, float64(150),
		"merge must hit the alloc target; got %.0f", allocs)
}

// TestPassesMergeMinNGate_SketchPaths covers the three branches of
// passesMergeMinNGate that no other test exercises: (1) sketch nil →
// gate trivially passes; (2) sketch present, sum >= min → passes;
// (3) sketch present, sum < min → fails. The unit-test suite previously
// only ran the nil-sketch branch (all qplanner unit tests build
// IndexIter literals with Sketch:nil), so a regression in the
// comparison sense (>= vs >) or in Estimate() lookup would not surface
// here.
func TestPassesMergeMinNGate_SketchPaths(t *testing.T) {
	bounds := query.Bounds{
		pointLookupBoundForValue("a"),
		pointLookupBoundForValue("b"),
	}

	t.Run("nil sketch passes trivially", func(t *testing.T) {
		require.True(t, passesMergeMinNGate(bounds, nil))
	})

	t.Run("sketch sum below gate fails", func(t *testing.T) {
		prev := SetKWayMergeMinEntries(100)
		defer SetKWayMergeMinEntries(prev)

		s := NewIndexSketch(DefaultSketchSize)
		// Increment each bound value once → Estimate per bound = 1.
		// Sum = 2; gate is 100.
		s.Increment(bounds[0].Start)
		s.Increment(bounds[1].Start)
		require.False(t, passesMergeMinNGate(bounds, s),
			"sum=2 must NOT pass gate=100")
	})

	t.Run("sketch sum at gate boundary passes", func(t *testing.T) {
		prev := SetKWayMergeMinEntries(2)
		defer SetKWayMergeMinEntries(prev)

		s := NewIndexSketch(DefaultSketchSize)
		s.Increment(bounds[0].Start)
		s.Increment(bounds[1].Start)
		require.True(t, passesMergeMinNGate(bounds, s),
			"sum=2 must pass gate=2 (>=, not >)")
	})

	t.Run("sketch sum above gate passes", func(t *testing.T) {
		prev := SetKWayMergeMinEntries(2)
		defer SetKWayMergeMinEntries(prev)

		s := NewIndexSketch(DefaultSketchSize)
		for i := 0; i < 10; i++ {
			s.Increment(bounds[0].Start)
			s.Increment(bounds[1].Start)
		}
		require.True(t, passesMergeMinNGate(bounds, s))
	})

	t.Run("sketch sum zero (cold sketch) fails any positive gate", func(t *testing.T) {
		prev := SetKWayMergeMinEntries(1)
		defer SetKWayMergeMinEntries(prev)

		s := NewIndexSketch(DefaultSketchSize)
		// Never incremented → Estimate returns 0 for every value.
		require.False(t, passesMergeMinNGate(bounds, s))
	})

	t.Run("gate of zero passes everything", func(t *testing.T) {
		prev := SetKWayMergeMinEntries(0)
		defer SetKWayMergeMinEntries(prev)

		s := NewIndexSketch(DefaultSketchSize)
		require.True(t, passesMergeMinNGate(bounds, s),
			"gate=0: sum>=0 always true (kill switch for gate)")
	})
}

// TestCanRunMergeStatic_GatesShape pins every shape that
// canRunMergeStatic decides. Without this, a regression that flips a
// gate condition (e.g., dropping the FieldNames==1 check) would not
// surface as a unit-test failure because the gates are exercised only
// indirectly through CountEntries / buildIndexSeekChain.
func TestCanRunMergeStatic_GatesShape(t *testing.T) {
	twoBounds := query.Bounds{
		pointLookupBoundForValue("a"),
		pointLookupBoundForValue("b"),
	}
	prev := SetKWayMergeMinEntries(0)
	defer SetKWayMergeMinEntries(prev)

	t.Run("happy path passes", func(t *testing.T) {
		require.True(t, canRunMergeStatic(twoBounds, []string{"f"}, true, nil))
	})
	t.Run("non-PointLookup fails", func(t *testing.T) {
		require.False(t, canRunMergeStatic(twoBounds, []string{"f"}, false, nil))
	})
	t.Run("compound (2 fields) fails", func(t *testing.T) {
		require.False(t, canRunMergeStatic(twoBounds, []string{"f", "g"}, true, nil))
	})
	t.Run("k=1 fails (single bound)", func(t *testing.T) {
		one := query.Bounds{pointLookupBoundForValue("a")}
		require.False(t, canRunMergeStatic(one, []string{"f"}, true, nil))
	})
	t.Run("k > kMax fails", func(t *testing.T) {
		prevMax := SetKWayMergeMax(1) // anything >= 2 fails
		defer SetKWayMergeMax(prevMax)
		require.False(t, canRunMergeStatic(twoBounds, []string{"f"}, true, nil))
	})
	t.Run("kMax=0 disables (kill switch)", func(t *testing.T) {
		prevMax := SetKWayMergeMax(0)
		defer SetKWayMergeMax(prevMax)
		require.False(t, canRunMergeStatic(twoBounds, []string{"f"}, true, nil))
	})
}
