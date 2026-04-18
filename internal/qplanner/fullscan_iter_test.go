package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// TestFullScanIter_String_Variants covers every permutation of the four
// descriptor arms: base, reverse, idBounds, offset, filter.
func TestFullScanIter_String_Variants(t *testing.T) {
	t.Run("base", func(t *testing.T) {
		it := &FullScanIter{}
		assert.Equal(t, "FullScan", it.String())
	})
	t.Run("reverse", func(t *testing.T) {
		it := &FullScanIter{Reverse: true}
		assert.Contains(t, it.String(), "(reverse)")
	})
	t.Run("with_bounds", func(t *testing.T) {
		b := query.Bound{Start: []byte{1}, End: []byte{2}}
		it := &FullScanIter{IDBounds: query.Bounds{b}}
		assert.Contains(t, it.String(), "idBounds=")
	})
	t.Run("with_offset", func(t *testing.T) {
		it := &FullScanIter{Offset: 7}
		assert.Contains(t, it.String(), "skip=7")
	})
	t.Run("with_filter", func(t *testing.T) {
		it := &FullScanIter{Filter: query.All{}}
		assert.Contains(t, it.String(), "(filtered)")
	})
}

// TestFullScanIter_DocValueRawValue pins FullScanIter.DocValue and
// FullScanIter.RawValue under a real cursor.
func TestFullScanIter_DocValueRawValue(t *testing.T) {
	db, ns := coverageBtree(t, "fs_docvalue", []string{"a"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FullScanIter{
		Source: &CursorSource{Tx: rtx, Ns: ns},
		Buf:    buf,
	}
	defer it.Close()

	// Advance the cursor by calling Next once.
	_, docId, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, docId)

	raw, err := it.RawValue()
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	// Parse raw bytes independently and assert id=="a".
	decoded, perr := anyenc.Parse(raw)
	require.NoError(t, perr)
	assert.Equal(t, "a", string(decoded.Get("id").GetStringBytes()),
		"RawValue bytes must decode to the doc with id=a")

	val, err := it.DocValue()
	require.NoError(t, err)
	assert.Equal(t, []byte("a"), val.Get("id").GetStringBytes())
}

// TestFullScanIter_WithBounds_Forward exercises FullScanIter with idBounds.
func TestFullScanIter_WithBounds_Forward(t *testing.T) {
	db, ns := coverageBtree(t, "fs_fwd_bounds",
		[]string{"a", "b", "c", "d", "e"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bounds := query.Bounds{
		{Start: anyenc.AppendAnyValue(nil, "b"),
			End:          anyenc.AppendAnyValue(nil, "d"),
			StartInclude: true, EndInclude: true},
	}
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:      buf,
		IDBounds: bounds,
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
	assert.Equal(t, []string{"b", "c", "d"}, got,
		"[b,d] forward must yield b,c,d")
}

// TestFullScanIter_Reverse exercises FullScanIter.nextNoBounds reverse path
// and verifies the order is genuinely reversed, not just that 3 docs visited.
func TestFullScanIter_Reverse(t *testing.T) {
	db, ns := coverageBtree(t, "fs_rev", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FullScanIter{
		Source:  &CursorSource{Tx: rtx, Ns: ns},
		Buf:     buf,
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
		"reverse scan must yield descending key order, not just 3 docs")
}

// TestFullScanIter_WithFilter exercises FullScanIter.nextNoBounds with a
// filter that accepts some docs and rejects others.
func TestFullScanIter_WithFilter(t *testing.T) {
	db, ns := coverageBtree(t, "fs_with_filter", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FullScanIter{
		Source: &CursorSource{Tx: rtx, Ns: ns},
		Buf:    buf,
		Filter: query.MustParseCondition(`{"id":"b"}`),
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
	assert.Equal(t, []string{"b"}, got,
		"filter must reject a and c, yield only b")
}

// TestFullScanIter_Reverse_WithBounds exercises the reverse-with-bounds path.
func TestFullScanIter_Reverse_WithBounds(t *testing.T) {
	db, ns := coverageBtree(t, "fs_rev_bounds",
		[]string{"a", "b", "c", "d", "e"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bounds := query.Bounds{
		{Start: anyenc.AppendAnyValue(nil, "b"),
			End:          anyenc.AppendAnyValue(nil, "d"),
			StartInclude: true, EndInclude: true},
	}
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:      buf,
		IDBounds: bounds,
		Reverse:  true,
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
		"[b,d] reverse must yield d,c,b")
}

// TestFullScanIter_Reverse_EndPastLastKey covers fullscan_iter.go:129-132 —
// reverse scan, IDBounds with End past the last key → invalid cursor → Last()
// fallback.
func TestFullScanIter_Reverse_EndPastLastKey(t *testing.T) {
	db, ns := coverageBtree(t, "fs_rev_past_end", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "a"),
		End:          anyenc.AppendAnyValue(nil, "zzz"),
		StartInclude: true,
		EndInclude:   true,
	}
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:      buf,
		IDBounds: query.Bounds{bound},
		Reverse:  true,
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

// TestFullScanIter_Forward_StartExclusive covers fullscan_iter.go:156-166 —
// forward with Start + StartInclude=false.
func TestFullScanIter_Forward_StartExclusive(t *testing.T) {
	db, ns := coverageBtree(t, "fs_fwd_excl_start", []string{"a", "b", "c", "d"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bound := query.Bound{
		Start:        anyenc.AppendAnyValue(nil, "b"),
		End:          anyenc.AppendAnyValue(nil, "d"),
		StartInclude: false,
		EndInclude:   true,
	}
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:      buf,
		IDBounds: query.Bounds{bound},
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
	assert.Equal(t, []string{"c", "d"}, got,
		"(b,d] must yield c,d in forward order")
}

// TestFullScanIter_Bounded_FilterReject covers fullscan_iter.go:210-214 —
// the filter-reject branch inside nextWithBounds (distinct from the
// nextNoBounds filter-reject at line 98-101).
func TestFullScanIter_Bounded_FilterReject(t *testing.T) {
	db, ns := coverageBtree(t, "fs_bounded_filter", []string{"a", "b", "c", "d"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bounds := query.Bounds{
		{Start: anyenc.AppendAnyValue(nil, "a"),
			End:          anyenc.AppendAnyValue(nil, "d"),
			StartInclude: true, EndInclude: true},
	}
	// Filter rejects a, b, d — only c passes.
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:      buf,
		IDBounds: bounds,
		Filter:   query.MustParseCondition(`{"id":"c"}`),
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
	assert.Equal(t, []string{"c"}, got,
		"bounded+filter must reject a/b/d and yield only c")
}

// TestFullScanIter_Bounded_NilPlan covers fullscan_iter.go:244 — `if Plan != nil`
// inside checkFilter's cache arm. With Plan nil, the DocParsed assignment
// is skipped.
func TestFullScanIter_Bounded_NilPlan(t *testing.T) {
	db, ns := coverageBtree(t, "fs_nil_plan", []string{"a", "b"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FullScanIter{
		Source: &CursorSource{Tx: rtx, Ns: ns},
		Buf:    buf,
		Filter: query.All{}, // passes everything
		Plan:   nil,         // branch target
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
	assert.Equal(t, []string{"a", "b"}, got,
		"all docs pass All{} filter in forward order with Plan nil")
}

// TestFullScanIter_NoBounds_FirstErr covers fullscan_iter.go:50.
func TestFullScanIter_NoBounds_FirstErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FullScanIter{Source: &CursorSource{Tx: rtx, Ns: invalidNamespace()}, Buf: buf}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestFullScanIter_NoBounds_LastErr covers fullscan_iter.go:46 (reverse).
func TestFullScanIter_NoBounds_LastErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FullScanIter{
		Source:  &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		Buf:     buf,
		Reverse: true,
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestFullScanIter_Bounded_SeekErr covers fullscan_iter.go:152/125.
func TestFullScanIter_Bounded_SeekErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bound := query.Bound{Start: anyenc.AppendAnyValue(nil, "a"), StartInclude: true}
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		Buf:      buf,
		IDBounds: query.Bounds{bound},
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestFullScanIter_Bounded_FirstErr covers fullscan_iter.go:167 (no-Start
// forward → cursor.First error).
func TestFullScanIter_Bounded_FirstErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bound := query.Bound{End: anyenc.AppendAnyValue(nil, "z"), EndInclude: true}
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		Buf:      buf,
		IDBounds: query.Bounds{bound},
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}

// TestFullScanIter_Bounded_Reverse_LastErr covers fullscan_iter.go:146.
func TestFullScanIter_Bounded_Reverse_LastErr(t *testing.T) {
	db, _ := openIsolatedBtree(t, []string{"a"})
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	bound := query.Bound{Start: anyenc.AppendAnyValue(nil, "a"), StartInclude: true}
	it := &FullScanIter{
		Source:   &CursorSource{Tx: rtx, Ns: invalidNamespace()},
		Buf:      buf,
		IDBounds: query.Bounds{bound},
		Reverse:  true,
	}
	defer it.Close()
	_, _, err = it.Next()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid page")
}
