package qplanner

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// TestFetchIter_Next_SkipsMissingDocId covers the ErrKeyNotFound swallow at
// fetch_iter.go:52-55 and the happy path that parses and caches DocParsed.
func TestFetchIter_Next_SkipsMissingDocId(t *testing.T) {
	db, ns := coverageBtree(t, "data_fetch", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	data := &CursorSource{Tx: rtx, Ns: ns}

	a := &anyenc.Arena{}
	_ = a
	makeHit := func(id string) fakeHit {
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id)}
	}

	// Feed three docIds; "missing" is NOT in the btree so FetchIter should skip it.
	source := &fakeIter{
		hits: []fakeHit{
			makeHit("a"),
			makeHit("missing"), // ErrKeyNotFound swallow arm
			makeHit("b"),
		},
	}

	plan := &Plan{}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FetchIter{
		Source: source,
		Data:   data,
		Buf:    buf,
		Plan:   plan,
	}

	var gotIds []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		// Plan.DocParsed must be populated.
		require.NotNil(t, plan.DocParsed)
		gotIds = append(gotIds, string(plan.DocParsed.Get("id").GetStringBytes()))
	}
	assert.Equal(t, []string{"a", "b"}, gotIds,
		"missing doc must be silently skipped")
}

// TestFetchIter_Close_AndString covers FetchIter.Close (with and without
// Source) and String formatting.
func TestFetchIter_Close_AndString(t *testing.T) {
	tr := &closeTrackingIter{}
	it := &FetchIter{Source: tr}
	it.Close()
	assert.Equal(t, 1, tr.closed)

	// nil-source Close is a no-op.
	(&FetchIter{}).Close()

	// String delegates to Source.String() with a " -> Fetch" suffix.
	src := &fakeIter{}
	it2 := &FetchIter{Source: src}
	assert.Equal(t, fmt.Sprintf("%s -> Fetch", src), it2.String())
}

// TestFetchIter_Next_PropagatesSourceError covers fetch_iter.go:39-41.
func TestFetchIter_Next_PropagatesSourceError(t *testing.T) {
	it := &FetchIter{Source: &errIter{err: errors.New("upstream")}}
	_, _, _, err := it.Next()
	require.ErrorContains(t, err, "upstream")
}

// TestFetchIter_PerfBranches enables perfCountersEnabled and runs a fetch,
// then asserts the counters actually moved — not just that the branches
// executed without panic.
func TestFetchIter_PerfBranches(t *testing.T) {
	resetPerfCounters()
	setPerfCountersEnabled(true)
	defer func() {
		setPerfCountersEnabled(false)
		resetPerfCounters()
	}()

	db, ns := coverageBtree(t, "fetch_perf", []string{"a"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	source := &fakeIter{hits: []fakeHit{{docId: anyenc.AppendAnyValue(nil, "a")}}}
	plan := &Plan{}
	it := &FetchIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:    buf,
		Plan:   plan,
	}
	_, docId, _, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, docId)

	// Drain to end to also exercise the perf defer cleanup on docId==nil.
	_, docId2, _, err := it.Next()
	require.NoError(t, err)
	assert.Nil(t, docId2)

	s := snapshotPerfCounters()
	assert.Greater(t, s.FetchNextCalls, uint64(0), "FetchNextCalls must be incremented")
	assert.Equal(t, uint64(1), s.FetchYields, "exactly 1 doc yielded")
	assert.Greater(t, s.FetchNextNs, uint64(0), "FetchNextNs must accumulate timing")
}

// TestFetchIter_Next_NoPlan_FetchesWithoutParsing covers the branch at
// fetch_iter.go:61 (if it.Plan != nil). With Plan nil, the parse block is
// skipped; but the prior AppendValue at line 48 still runs, so DocBuf ends
// up with the raw fetched bytes. The test pre-seeds DocBuf with a sentinel
// so we can prove (1) fetch overwrote the sentinel and (2) the final bytes
// are a valid serialized doc with id=="x".
func TestFetchIter_Next_NoPlan_FetchesWithoutParsing(t *testing.T) {
	db, ns := coverageBtree(t, "data_fetch_noplan", []string{"x"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	source := &fakeIter{
		hits: []fakeHit{{docId: anyenc.AppendAnyValue(nil, "x")}},
	}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	// Pre-seed DocBuf with a distinct sentinel so we can detect that
	// AppendValue overwrote it.
	sentinel := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	buf.DocBuf = append(buf.DocBuf[:0], sentinel...)

	it := &FetchIter{
		Source: source,
		Data:   &CursorSource{Tx: rtx, Ns: ns},
		Buf:    buf,
		Plan:   nil, // explicitly no plan → parsing block at line 61 must be skipped
	}
	_, docId, _, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, docId, "doc must still be fetched")

	// Post-call DocBuf must contain the fetched doc bytes (AppendValue
	// writes into DocBuf[:0]), not the sentinel we seeded. Decoding those
	// bytes yields the expected doc.
	require.NotEmpty(t, buf.DocBuf, "fetch must have populated DocBuf")
	fetched, perr := anyenc.Parse(buf.DocBuf)
	require.NoError(t, perr,
		"DocBuf must contain a valid serialized anyenc doc, proving fetch ran")
	assert.Equal(t, "x", string(fetched.Get("id").GetStringBytes()),
		"fetched bytes decode to the requested doc")
}

// TestFetchIter_RetainedCursor_RepeatedDocId covers the same-leaf re-point of
// the retained cursor (Step 1): two consecutive Source rows yielding the SAME
// docId (as a multi-key/array index would) must both return the same doc with
// no spurious advance. Mirrors SQLite IndexMoveto case-1 (btree.c:6065).
func TestFetchIter_RetainedCursor_RepeatedDocId(t *testing.T) {
	db, ns := coverageBtree(t, "data_repeat", []string{"a", "b", "c"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	makeHit := func(id string) fakeHit {
		return fakeHit{docId: anyenc.AppendAnyValue(nil, id)}
	}
	source := &fakeIter{hits: []fakeHit{
		makeHit("b"),
		makeHit("b"), // same docId again -> same-leaf re-point
		makeHit("b"), // and again
		makeHit("c"),
	}}

	plan := &Plan{}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FetchIter{Source: source, Data: &CursorSource{Tx: rtx, Ns: ns}, Buf: buf, Plan: plan}
	defer it.Close()

	var got []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(plan.DocParsed.Get("id").GetStringBytes()))
	}
	assert.Equal(t, []string{"b", "b", "b", "c"}, got)
}

// TestFetchIter_RetainedCursor_RandomOrderParity is the Step-1 parity guarantee:
// for a populated namespace, the retained-cursor FetchIter returns, for every
// row in a random fetch order, exactly the bytes the cursor-free
// ReadTx.AppendValue returns. This is the differential safety net that proves
// the cursor reuse never changes the result.
func TestFetchIter_RetainedCursor_RandomOrderParity(t *testing.T) {
	const n = 2000
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("doc-%05d", i)
	}
	db, ns := coverageBtree(t, "data_parity", ids)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	// Random fetch order, uncorrelated with data-key order (like the real query).
	rng := rand.New(rand.NewSource(7))
	order := rng.Perm(n)
	hits := make([]fakeHit, n)
	for j, i := range order {
		hits[j] = fakeHit{docId: anyenc.AppendAnyValue(nil, ids[i])}
	}

	plan := &Plan{}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FetchIter{Source: &fakeIter{hits: hits}, Data: &CursorSource{Tx: rtx, Ns: ns}, Buf: buf, Plan: plan}
	defer it.Close()

	freeCS := &CursorSource{Tx: rtx, Ns: ns}
	rows := 0
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		// Ground truth: cursor-free AppendValue for the same key.
		want, werr := freeCS.AppendValue(docId, nil)
		require.NoError(t, werr)
		assert.Equal(t, want, buf.DocBuf, "row %d docId=%q parity", rows, docId)
		rows++
	}
	assert.Equal(t, n, rows, "every doc fetched exactly once")
}

// TestFetchIter_RetainedCursor_SkipsMissingThenContinues covers the
// ErrKeyNotFound-continue arm through the RETAINED cursor: a docId present in
// the source but absent from the data namespace (deleted-doc-still-in-index)
// is silently skipped and the scan continues with a correct count.
func TestFetchIter_RetainedCursor_SkipsMissingThenContinues(t *testing.T) {
	db, ns := coverageBtree(t, "data_miss", []string{"a", "b", "c", "d"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	makeHit := func(id string) fakeHit { return fakeHit{docId: anyenc.AppendAnyValue(nil, id)} }
	source := &fakeIter{hits: []fakeHit{
		makeHit("a"),
		makeHit("zzz_missing"), // not in data ns -> ErrKeyNotFound -> skip
		makeHit("c"),
		makeHit("yyy_missing"),
		makeHit("d"),
	}}

	plan := &Plan{}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FetchIter{Source: source, Data: &CursorSource{Tx: rtx, Ns: ns}, Buf: buf, Plan: plan}
	defer it.Close()

	var got []string
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
		got = append(got, string(plan.DocParsed.Get("id").GetStringBytes()))
	}
	assert.Equal(t, []string{"a", "c", "d"}, got, "missing docs skipped, others kept in order")
}

// TestFetchIter_Close_ReleasesCursor verifies that Close releases the retained
// cursor (and is idempotent / nil-safe). A leaked cursor would pin a leaf page
// for the life of the DB; here we assert the cursor field is cleared and the
// underlying btree cursor reports no position after Close.
func TestFetchIter_Close_ReleasesCursor(t *testing.T) {
	db, ns := coverageBtree(t, "data_close", []string{"a", "b"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	source := &fakeIter{hits: []fakeHit{{docId: anyenc.AppendAnyValue(nil, "a")}}}
	plan := &Plan{}
	sp := syncpool.NewSyncPool(1024)
	buf := sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	it := &FetchIter{Source: source, Data: &CursorSource{Tx: rtx, Ns: ns}, Buf: buf, Plan: plan}

	_, docId, _, err := it.Next()
	require.NoError(t, err)
	require.NotNil(t, docId)
	require.NotNil(t, it.cursor, "cursor minted on first lookup")

	// Capture the underlying cursor before Close nils the field, so we can
	// assert Close released its pinned position (releasePages -> not Valid).
	c := it.cursor
	it.Close()
	assert.Nil(t, it.cursor, "Close must clear the retained cursor field")
	assert.False(t, c.Valid(), "released cursor reports no position")

	// Idempotent / nil-safe second Close.
	assert.NotPanics(t, it.Close)
}
