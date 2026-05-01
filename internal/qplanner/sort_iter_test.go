package qplanner

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
	"github.com/anyproto/any-store/v2/syncpool"
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

// multikeyHit is a fakeHit-like struct for the multi-key sort test that
// carries an explicit multiKey bool through to SortIter.
type multikeyHit struct {
	key      []byte
	docId    []byte
	doc      *anyenc.Value
	multiKey bool
}

type multikeyFakeIter struct {
	hits []multikeyHit
	i    int
	plan *Plan
}

func (f *multikeyFakeIter) Next() ([]byte, []byte, bool, error) {
	if f.i >= len(f.hits) {
		return nil, nil, false, nil
	}
	h := f.hits[f.i]
	f.i++
	if f.plan != nil {
		f.plan.DocParsed = h.doc
	}
	return h.key, h.docId, h.multiKey, nil
}

func (f *multikeyFakeIter) Close()         {}
func (f *multikeyFakeIter) String() string { return "multikey-fake" }

// TestSortIter_PreservesMultiKeyAcrossSort pins the propagation invariant:
// when an upstream multi-key index emits the same docId multiple times
// (e.g. an $in over array values), SortIter must preserve the multiKey
// flag through the sort so the consumer's DocDedup can collapse duplicates
// AFTER the sort, in the correct sort order.
//
// Setup: doc p1 has tags=["a","b"] → upstream emits 2 multi-key entries
// for p1; doc p2 has tags=["a"] → 1 multi-key entry. Sort by id ascending
// → p1, p1, p2 emitted in that order. Consumer dedup → p1 once, p2 once,
// in ascending order [p1, p2].
func TestSortIter_PreservesMultiKeyAcrossSort(t *testing.T) {
	db, ns := coverageBtree(t, "sort_multikey",
		[]string{"p1", "p2"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	arena := &anyenc.Arena{}
	mkDoc := func(id string) *anyenc.Value {
		o := arena.NewObject()
		o.Set("id", arena.NewString(id))
		return o
	}
	plan := &Plan{}

	// Two entries for p1 (multi-key), one for p2 (multi-key). All flagged
	// multiKey=true. Upstream emission order is intentionally NOT sort
	// order: p2 (single), then p1 twice. SortIter must place p1 first.
	source := &multikeyFakeIter{
		plan: plan,
		hits: []multikeyHit{
			{docId: anyenc.AppendAnyValue(nil, "p2"), doc: mkDoc("p2"), multiKey: true},
			{docId: anyenc.AppendAnyValue(nil, "p1"), doc: mkDoc("p1"), multiKey: true},
			{docId: anyenc.AppendAnyValue(nil, "p1"), doc: mkDoc("p1"), multiKey: true},
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

	// Pull every entry from SortIter, asserting that the multiKey flag
	// reaches the consumer alongside each emitted entry.
	type emitted struct {
		id string
		mk bool
	}
	var raw []emitted
	for {
		_, docId, mk, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		raw = append(raw, emitted{
			id: string(anyenc.MustParse(docId).GetStringBytes()),
			mk: mk,
		})
	}

	// Sorted output: p1, p1, p2 (the duplicate p1 is preserved at this
	// stage; dedup is the consumer's job).
	assert.Equal(t, []emitted{
		{"p1", true},
		{"p1", true},
		{"p2", true},
	}, raw, "SortIter must preserve every entry's multiKey flag and order by sort key")

	// Now apply consumer-side dedup and confirm the final stream is
	// (p1, p2) in ascending order — the canonical "sort then dedup" path
	// the multi-key fetch chain relies on.
	source.i = 0 // rewind for a second pass
	it2 := &SortIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Sorter: sort,
		Buf:    buf,
		Plan:   plan,
	}
	defer it2.Close()
	var dedup DocDedup
	var deduped []string
	for {
		_, docId, mk, err := it2.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		if !dedup.Accept(docId, mk) {
			continue
		}
		deduped = append(deduped, string(anyenc.MustParse(docId).GetStringBytes()))
	}
	assert.Equal(t, []string{"p1", "p2"}, deduped,
		"after sort+dedup, each doc appears once in ascending sort order")
}
