package qplanner

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"testing"
	"unsafe"

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

// sortKeyOf builds the exact composite key SortIter materializes for a doc:
// the packed sort key (honoring asc/desc bit-inversion) followed by the docId
// bytes as a tiebreaker. The reference oracle below sorts by this same key, so
// the test validates the arena/heap/slot-reuse/compaction machinery rather than
// the (shared) key encoding.
func sortKeyOf(t *testing.T, srt query.Sort, doc *anyenc.Value, docId []byte) []byte {
	t.Helper()
	k := srt.AppendKey(nil, doc)
	k = append(k, docId...)
	return k
}

// runVarLenSort feeds specs (in the given upstream order) through SortIter with
// the given TopK and sort, and returns the emitted docIds in order.
func runVarLenSort(t *testing.T, srt query.Sort, topK int, ids []string, docs []*anyenc.Value, docIds [][]byte) [][]byte {
	t.Helper()
	plan := &Plan{}
	hits := make([]fakeHit, len(ids))
	for i := range ids {
		hits[i] = fakeHit{key: docIds[i], docId: docIds[i], doc: docs[i]}
	}
	src := &fakeIter{plan: plan, hits: hits}
	it := &SortIter{
		Source: src,
		Sorter: srt,
		Plan:   plan,
		TopK:   topK,
	}
	defer it.Close()
	var got [][]byte
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		cp := make([]byte, len(docId))
		copy(cp, docId)
		got = append(got, cp)
	}
	return got
}

// referenceWindow computes the expected docId window: sort all specs by the
// composite (packed-key, docId) ordering, then take [offset, offset+limit).
// When limit<=0 it returns the full sorted order.
func referenceWindow(t *testing.T, srt query.Sort, docs []*anyenc.Value, docIds [][]byte, offset, limit int) [][]byte {
	t.Helper()
	type kd struct {
		key   []byte
		docId []byte
	}
	all := make([]kd, len(docs))
	for i := range docs {
		all[i] = kd{key: sortKeyOf(t, srt, docs[i], docIds[i]), docId: docIds[i]}
	}
	slices.SortFunc(all, func(a, b kd) int { return bytes.Compare(a.key, b.key) })
	var out [][]byte
	end := len(all)
	if limit > 0 && offset+limit < end {
		end = offset + limit
	}
	for i := offset; i < end; i++ {
		out = append(out, all[i].docId)
	}
	return out
}

// TestSortIter_TopK_ArenaBounding_VarLen is the core correctness test for the
// arena-bounding fix. It drives SortIter with VARIABLE-LENGTH packed keys and
// docIds across several adversarial upstream orders, and asserts the emitted
// window matches an independent full-sort oracle. The variable lengths exercise
// every new code path: exact-size free-slot reuse, append-on-miss, and (in the
// strictly-descending all-distinct-length case at scale) the compaction guard.
func TestSortIter_TopK_ArenaBounding_VarLen(t *testing.T) {
	arena := &anyenc.Arena{}
	// mkRow builds {"v": value-string} and an id-encoded docId. The sort field
	// is "v"; docIds are derived from the row index so they are unique and of
	// varying byte length.
	mkRow := func(vlen, idx int) (*anyenc.Value, []byte) {
		o := arena.NewObject()
		// value string of length vlen, content derived from idx so distinct
		// rows get distinct (and order-meaningful) keys.
		vb := make([]byte, vlen)
		for i := range vb {
			vb[i] = byte('a' + (idx+i)%26)
		}
		o.Set("v", arena.NewStringBytes(vb))
		docId := anyenc.AppendAnyValue(nil, fmt.Sprintf("doc-%d", idx))
		return o, docId
	}

	ascSort := query.MustParseSort("v")
	descSort := query.MustParseSort("-v")

	type genMode int
	const (
		shuffled genMode = iota
		descending
		ascending
		fixedLen
	)

	build := func(n int, mode genMode) (docs []*anyenc.Value, docIds [][]byte) {
		docs = make([]*anyenc.Value, n)
		docIds = make([][]byte, n)
		for i := 0; i < n; i++ {
			var vlen, idx int
			switch mode {
			case descending:
				// strictly descending key with all-distinct lengths: forces
				// free-list misses + appends + compaction at scale.
				vlen = 4 + (n - i) // distinct lengths, decreasing content
				idx = n - i
			case ascending:
				vlen = 4 + i
				idx = i
			case fixedLen:
				// all-equal lengths: maximizes exact-size free-slot reuse.
				vlen = 12
				idx = (i * 7919) % n // pseudo-shuffle of content
			default: // shuffled
				vlen = 4 + (i*131)%40
				idx = (i * 2654435761) % n
			}
			docs[i], docIds[i] = mkRow(vlen, idx)
		}
		return docs, docIds
	}

	cases := []struct {
		name   string
		n      int
		offset int
		limit  int
		mode   genMode
		sort   query.Sort
	}{
		{"asc_shuffled_lim100", 5000, 0, 100, shuffled, ascSort},
		{"desc_shuffled_lim100", 5000, 0, 100, shuffled, descSort},
		{"asc_limoff", 5000, 1000, 10, shuffled, ascSort},
		{"desc_limoff", 5000, 1000, 10, shuffled, descSort},
		{"fixedlen_reuse_lim50", 8000, 0, 50, fixedLen, ascSort},
		// strictly descending, all-distinct lengths, large N + small K → the
		// arena would balloon under append-only placement; this is the case
		// the compaction guard is built for. Larger N to clear the 64KiB gate.
		{"descending_compaction", 40000, 0, 20, descending, ascSort},
		{"ascending_compaction", 40000, 0, 20, ascending, descSort},
		// TopK larger than N → all rows retained, must equal full sort.
		{"topk_gt_n", 50, 0, 200, shuffled, ascSort},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			docs, docIds := build(tc.n, tc.mode)
			ids := make([]string, tc.n)
			// The planner passes TopK = Limit+Offset; SortIter retains and emits
			// exactly that many smallest entries (the downstream LimitIter applies
			// the offset-skip + limit-cap). So the oracle is the first TopK of the
			// full sort. This pins the "retain LIMIT+OFFSET, not just LIMIT" rule.
			topK := tc.limit + tc.offset
			got := runVarLenSort(t, tc.sort, topK, ids, docs, docIds)
			want := referenceWindow(t, tc.sort, docs, docIds, 0, topK)
			require.Equal(t, len(want), len(got), "result count")
			for i := range want {
				assert.Truef(t, bytes.Equal(want[i], got[i]),
					"row %d mismatch: want %x got %x", i, want[i], got[i])
			}
		})
	}
}

// TestSortIter_FullSort_NoLimit_Unchanged pins that the TopK<=0 path still
// materializes and sorts ALL rows (behavior unchanged by the arena-bounding
// fix), including with variable-length keys.
func TestSortIter_FullSort_NoLimit_Unchanged(t *testing.T) {
	arena := &anyenc.Arena{}
	n := 2000
	docs := make([]*anyenc.Value, n)
	docIds := make([][]byte, n)
	for i := 0; i < n; i++ {
		o := arena.NewObject()
		vlen := 3 + (i*97)%50
		vb := make([]byte, vlen)
		for j := range vb {
			vb[j] = byte('a' + (i+j)%26)
		}
		o.Set("v", arena.NewStringBytes(vb))
		docs[i] = o
		docIds[i] = anyenc.AppendAnyValue(nil, fmt.Sprintf("d%d", i))
	}
	srt := query.MustParseSort("v")
	ids := make([]string, n)
	got := runVarLenSort(t, srt, 0 /* full sort */, ids, docs, docIds)
	want := referenceWindow(t, srt, docs, docIds, 0, 0)
	require.Equal(t, len(want), len(got))
	for i := range want {
		assert.Truef(t, bytes.Equal(want[i], got[i]), "row %d mismatch", i)
	}
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

// The entries are what the sort swaps and what a TopK heap holds: widening
// the lengths to 32 bits must not grow the entry.
func TestSortEntry_Is12Bytes(t *testing.T) {
	assert.Equal(t, uintptr(12), unsafe.Sizeof(sortEntry{}))
	e := sortEntry{docMK: 70000 | sortEntryMultiKey}
	assert.Equal(t, uint32(70000), e.docLen())
	assert.True(t, e.multiKey())
	e = sortEntry{docMK: 70000}
	assert.Equal(t, uint32(70000), e.docLen())
	assert.False(t, e.multiKey())
}

// setSortArenaLimit lowers the arena limit for one test, so its boundary is
// reached with a few short keys.
func setSortArenaLimit(t *testing.T, n int) {
	old := sortArenaLimit
	sortArenaLimit = uint64(n)
	t.Cleanup(func() { sortArenaLimit = old })
}

// limitRow is one input row of the arena-limit tests: sort field "v" and an
// encoded docId.
type limitRow struct {
	doc   *anyenc.Value
	docId []byte
}

func mkLimitRow(a *anyenc.Arena, v, id string) limitRow {
	o := a.NewObject()
	o.Set("v", a.NewString(v))
	return limitRow{doc: o, docId: anyenc.AppendAnyValue(nil, id)}
}

// packedLen is the arena bytes a row takes when kept.
func packedLen(t *testing.T, srt query.Sort, r limitRow) int {
	return len(sortKeyOf(t, srt, r.doc, r.docId))
}

// sortRows runs rows through a SortIter and returns the emitted ids, the
// error of the first Next, and the iterator, still open.
func sortRows(t *testing.T, srt query.Sort, topK int, rows []limitRow) ([]string, error, *SortIter) {
	t.Helper()
	plan := &Plan{}
	hits := make([]fakeHit, len(rows))
	for i, r := range rows {
		hits[i] = fakeHit{key: r.docId, docId: r.docId, doc: r.doc}
	}
	it := &SortIter{Source: &fakeIter{plan: plan, hits: hits}, Sorter: srt, Plan: plan, TopK: topK}
	// The caller sees what an error left in place; the test closes the
	// iterator at its end.
	t.Cleanup(it.Close)
	var ids []string
	for {
		_, docId, _, err := it.Next()
		if err != nil {
			return ids, err, it
		}
		if docId == nil {
			return ids, nil, it
		}
		ids = append(ids, string(anyenc.MustParse(docId).GetStringBytes()))
	}
}

// The arena limit applies to the rows a sort keeps, when it keeps them: a
// TopK candidate that loses is no error however far past the limit it was
// built, a winner whose tail does not fit is kept after packing the live
// entries, and only rows that do not fit even packed are an error
// (pushOntoSorter decides before it inserts, select.c:833-846).
func TestSortIter_ArenaLimitAppliesOnAdmission(t *testing.T) {
	srt := query.MustParseSort("v")
	a := &anyenc.Arena{}
	// Four kept rows of one length, "b…".
	kept := []limitRow{
		mkLimitRow(a, "b1", "id-1"), mkLimitRow(a, "b2", "id-2"),
		mkLimitRow(a, "b3", "id-3"), mkLimitRow(a, "b4", "id-4"),
	}
	e := packedLen(t, srt, kept[0])
	for _, r := range kept {
		require.Equal(t, e, packedLen(t, srt, r))
	}
	loser := mkLimitRow(a, "c-loses-to-every-kept-row", "id-9")
	winner := mkLimitRow(a, "a0", "id-0x") // one byte longer than a kept row
	require.Equal(t, e+1, packedLen(t, srt, winner))

	t.Run("a loser past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 4*e)
		ids, err, _ := sortRows(t, srt, 4, append(slices.Clone(kept), loser))
		require.NoError(t, err)
		assert.Equal(t, []string{"id-1", "id-2", "id-3", "id-4"}, ids)
	})
	t.Run("a winner whose tail is past the limit", func(t *testing.T) {
		// Live after the eviction: 3e + (e+1). The tail would end at 5e+1.
		setSortArenaLimit(t, 4*e+1)
		ids, err, _ := sortRows(t, srt, 4, append(slices.Clone(kept), winner, loser))
		require.NoError(t, err)
		assert.Equal(t, []string{"id-0x", "id-1", "id-2", "id-3"}, ids)
	})
	t.Run("a winner that does not fit even packed", func(t *testing.T) {
		setSortArenaLimit(t, 4*e)
		_, err, it := sortRows(t, srt, 4, append(slices.Clone(kept), winner))
		require.ErrorIs(t, err, errSortTooLarge)
		assert.Nil(t, it.arena, "a failed sort releases its arena before Close")
	})
	t.Run("kept rows past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 4*e-1)
		_, err, _ := sortRows(t, srt, 4, kept)
		require.ErrorIs(t, err, errSortTooLarge)
	})
	t.Run("full sort at and past the limit", func(t *testing.T) {
		setSortArenaLimit(t, 4*e)
		ids, err, _ := sortRows(t, srt, 0, kept)
		require.NoError(t, err)
		assert.Len(t, ids, 4)
		setSortArenaLimit(t, 4*e-1)
		_, err, it := sortRows(t, srt, 0, kept)
		require.ErrorIs(t, err, errSortTooLarge)
		assert.Nil(t, it.arena, "a failed sort releases its arena before Close")
		assert.Nil(t, it.entries)
	})
}

// Close releases the sort's memory: a caller that keeps a closed iterator
// does not keep its arena.
func TestSortIter_CloseReleasesArena(t *testing.T) {
	srt := query.MustParseSort("v")
	a := &anyenc.Arena{}
	for _, topK := range []int{0, 2} {
		_, err, it := sortRows(t, srt, topK, []limitRow{
			mkLimitRow(a, "x", "1"), mkLimitRow(a, "y", "2"), mkLimitRow(a, "z", "3"),
		})
		require.NoError(t, err)
		require.NotNil(t, it.arena, "TopK %d: the drained sort keeps its rows until Close", topK)
		it.Close()
		assert.Nil(t, it.arena, "TopK %d", topK)
		assert.Nil(t, it.entries, "TopK %d", topK)
	}
}

// The compaction guard compares dead bytes with live ones without doubling
// either: liveBytes*2 overflows an int from half its range on, which on a
// 32-bit platform is 1 GiB of live keys, and the overflowed guard compacts on
// every eviction.
func TestSortNeedsCompact(t *testing.T) {
	const minWaste = 64 << 10
	for _, tc := range []struct {
		arena, live int
		want        bool
	}{
		{0, 0, false},
		{3 * minWaste, minWaste, true},
		{2 * minWaste, minWaste, false}, // waste == live
		{minWaste + 10, 10, true},
		{minWaste - 1 + 5, 5, false}, // waste below minWaste
		{math.MaxInt/2 + 1 + 1<<20, math.MaxInt/2 + 1, false},
		{math.MaxInt, math.MaxInt / 4, true},
	} {
		assert.Equal(t, tc.want, sortNeedsCompact(tc.arena, tc.live), "arena %d, live %d", tc.arena, tc.live)
	}
}
