package qplanner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/query"
	"github.com/anyproto/any-store/syncpool"
)

// TestSortIter_TopK_Heap exercises the heap path (TopK > 0) in SortIter
// including the replace-root branch at sort_iter.go:126-130 when a new entry
// is smaller than the current max-heap root. Input order 05,04,03,02,01
// ensures the heap fills with the three largest first (05,04,03), then both
// 02 and 01 trigger the replace-root branch.
func TestSortIter_TopK_Heap(t *testing.T) {
	db, ns := coverageBtree(t, "sort_topk",
		[]string{"05", "04", "03", "02", "01"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	plan := &Plan{}
	// Upstream yields docIds in insertion order. Wrap in a fakeIter that
	// sets plan.DocParsed for each hit so SortIter reuses it.
	arena := &anyenc.Arena{}
	makeHit := func(id string) fakeHit {
		o := arena.NewObject()
		o.Set("id", arena.NewString(id))
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id), doc: o}
	}
	source := &fakeIter{
		plan: plan,
		hits: []fakeHit{makeHit("05"), makeHit("04"), makeHit("03"), makeHit("02"), makeHit("01")},
	}

	sort, err := query.ParseSort("id")
	require.NoError(t, err)

	it := &SortIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Sorter: sort,
		Buf:    buf,
		Plan:   plan,
		TopK:   3, // keep smallest 3
	}
	defer it.Close()

	var got []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		// DocParsed is cleared by SortIter; fetch via data cursor manually
		// by looking at the docId bytes.
		got = append(got, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	// TopK=3 ascending → smallest 3 ids.
	assert.Equal(t, []string{"01", "02", "03"}, got)
}

// TestSortIter_FallbackFetch covers collectAndSort's branch where Plan.DocParsed
// is nil on some iterations, forcing a data-cursor fetch (sort_iter.go:100-112).
func TestSortIter_FallbackFetch(t *testing.T) {
	db, ns := coverageBtree(t, "sort_fallback",
		[]string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	plan := &Plan{}
	// fakeIter WITHOUT per-hit doc → Plan.DocParsed stays nil, forcing the
	// fallback fetch inside collectAndSort.
	source := &fakeIter{
		plan: plan,
		hits: []fakeHit{
			{docId: anyenc.AppendAnyValue(nil, "a")},
			{docId: anyenc.AppendAnyValue(nil, "b")},
			{docId: anyenc.AppendAnyValue(nil, "c")},
		},
	}

	sort, err := query.ParseSort("id")
	require.NoError(t, err)

	it := &SortIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Sorter: sort,
		Buf:    buf,
		Plan:   plan,
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
	assert.Equal(t, []string{"a", "b", "c"}, got,
		"sort-by-id must return ascending a,b,c after fallback-fetch")
}

// TestSortIter_SourceError propagates an error from Source.Next through
// collectAndSort via Next's first call.
func TestSortIter_SourceError(t *testing.T) {
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)
	sort, err := query.ParseSort("id")
	require.NoError(t, err)

	it := &SortIter{
		Source: &errIter{err: errors.New("upstream")},
		Buf:    buf,
		Sorter: sort,
		Plan:   &Plan{},
	}
	_, _, _, err = it.Next()
	require.ErrorContains(t, err, "upstream")
}

// TestSortIter_GrowArena_LargeNeed covers sort_iter.go:76-78 (the
// `grow < need` path) where the needed bytes exceed the tiered growth step.
func TestSortIter_GrowArena_LargeNeed(t *testing.T) {
	// Directly exercise growArena with a need larger than any tier step.
	// This keeps the test independent of the iteration machinery.
	it := &SortIter{}
	need := 2 << 20 // 2MB — exceeds the 100KB "grow" step
	it.growArena(need)
	assert.GreaterOrEqual(t, cap(it.arena), need,
		"growArena must accommodate a very large need")
}
