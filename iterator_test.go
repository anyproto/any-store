package anystore

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
)

func TestIterator_Doc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	docs := []*anyenc.Value{anyenc.MustParseJson(`{"id":1,"a":"a"}`), anyenc.MustParseJson(`{"id":2,"a":"b"}`), anyenc.MustParseJson(`{"id":3,"a":"c"}`)}
	require.NoError(t, coll.Insert(ctx, docs...))
	t.Run("error", func(t *testing.T) {
		iter, err := coll.Find("not valid").Iter(ctx)
		assert.Error(t, err)
		assert.Nil(t, iter)
	})
	t.Run("ok", func(t *testing.T) {
		iter, err := coll.Find(nil).Sort("id").Iter(ctx)
		require.NoError(t, err)
		var d Doc
		var i int
		for iter.Next() {
			d, err = iter.Doc()
			require.NoError(t, err)
			assert.Equal(t, docs[i].String(), d.Value().String())
			i++
		}
		require.NoError(t, iter.Err())
		require.NoError(t, iter.Close())
	})
}

// --- Coverage tests from iterator_lifecycle_coverage_test.go ---

// TestIterator_Coverage_DoubleCloseNoPanic verifies that calling Close() twice
// on a live query iterator does not panic or double-release the underlying
// resources. The concrete plan iterator (e.g. FullScanIter) is expected to be
// idempotent by construction — it nil-checks its cursor before releasing.
// The public Iterator wrapper additionally surfaces ErrIterClosed on the
// second call.
//
// Gap item 69: double Close() on an iterator.
func TestIterator_Coverage_DoubleCloseNoPanic(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":"x"}`),
		anyenc.MustParseJson(`{"id":2,"a":"y"}`),
	))

	// coll.Find(nil) routes through FullScanIter.
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)

	// Drive the iterator to open the underlying cursor before Close.
	require.True(t, iter.Next(), "expected at least one doc")

	var firstErr, secondErr error
	assert.NotPanics(t, func() {
		firstErr = iter.Close()
	}, "first Close must not panic")
	assert.NotPanics(t, func() {
		secondErr = iter.Close()
	}, "second Close must not panic")

	assert.NoError(t, firstErr, "first Close should succeed")
	assert.ErrorIs(t, secondErr, ErrIterClosed,
		"second Close must be a no-op returning ErrIterClosed (idempotent semantics at the public layer)")
}

// TestIterator_String covers planIterator.String at iterator.go:144-146.
// It must return the underlying plan's String() output so Explain-style
// introspection works.
func TestIterator_String(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_string")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	pi, ok := it.(*planIterator)
	require.True(t, ok, "public Iterator should be a *planIterator")

	s := pi.String()
	assert.Equal(t, pi.plan.String(), s,
		"planIterator.String must return plan.String() verbatim (no wrapping/composition)")
}

// TestIterator_Err_HidesEOF pins iterator.go:114-118 — when pi.err is io.EOF
// (used internally to signal normal termination), the public Err() returns
// nil rather than surfacing the sentinel.
func TestIterator_Err_HidesEOF(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_eof")
	require.NoError(t, err)

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	pi, ok := it.(*planIterator)
	require.True(t, ok)

	// Inject io.EOF directly and verify Err() returns nil.
	pi.err = io.EOF
	assert.Nil(t, it.Err(), "Err must treat io.EOF as normal termination → nil")

	// With a non-EOF sentinel, Err() must propagate.
	pi.err = errors.New("other-error")
	assert.NotNil(t, it.Err())
}

// TestIterator_NextAfterClose pins iterator.go:43-45 — once Close() has set
// pi.closed, subsequent Next() calls return false without advancing the plan.
func TestIterator_NextAfterClose(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_next_close")
	require.NoError(t, err)
	// Insert ≥2 docs so post-Close Next()==false can't be explained by
	// accidental cursor exhaustion (only 1 doc would be ambiguous).
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	// First Next should see a doc.
	assert.True(t, it.Next())
	require.NoError(t, it.Close())
	// After Close, Next must report done.
	assert.False(t, it.Next(), "Next after Close must return false")
	// And no use-after-close error should leak through pi.err.
	pi, ok := it.(*planIterator)
	require.True(t, ok)
	assert.Nil(t, pi.err, "post-Close Next must not set pi.err")
}

// TestIterator_NextAfterError pins iterator.go:43-45 — a sticky pi.err
// short-circuits subsequent Next calls.
func TestIterator_NextAfterError(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_next_err")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	pi, ok := it.(*planIterator)
	require.True(t, ok)
	// Simulate a transient error mid-iteration.
	pi.err = errors.New("injected")
	assert.False(t, it.Next(), "Next with sticky err must return false")
	assert.Equal(t, "injected", it.Err().Error())
}

// TestIterator_Doc_PerfCountersPath enables the pipeline perf counters and
// exercises Doc()'s perf-guarded branches (iterator.go:63-65, 71-73, 77-78,
// 84-86, 90-91, 99-100, 104-105). Uses the same fallback trick as
// TestIterator_Doc_FallbackPath to also cover the fallback perf sections.
func TestIterator_Doc_PerfCountersPath(t *testing.T) {
	setPipelinePerfEnabled(true)
	defer setPipelinePerfEnabled(false)
	resetPipelinePerfCounters()

	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_perf")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":"x"}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()
	require.True(t, it.Next())
	// First call hits the DocParsed branch (perf: docCalls++, docParsedHits++).
	_, err = it.Doc()
	require.NoError(t, err)

	// Second call with fallback path (perf: docCalls++, docFallbacks++,
	// docFallbackSeekNs++, docFallbackParseNs++).
	pi := it.(*planIterator)
	parsed := pi.plan.DocParsed
	require.NotNil(t, parsed)
	reconstructed, err := newItem(parsed)
	require.NoError(t, err)
	pi.docId = reconstructed.appendId(pi.docId[:0])
	pi.plan.DocParsed = nil
	_, err = it.Doc()
	require.NoError(t, err)

	// Verify the perf counters actually recorded our calls — exactly two
	// Doc() invocations, one hit, one fallback. Tight equality so a
	// reimplementation that double-counted would fail.
	snap := snapshotPipelinePerfCounters()
	assert.Equal(t, uint64(2), snap.DocCalls)
	assert.Equal(t, uint64(1), snap.DocParsedHits)
	assert.Equal(t, uint64(1), snap.DocFallbacks)
	// The fallback path must have fired both timing branches
	// (iterator.go:84-86 seek timing and 99-101 parse timing).
	assert.Greater(t, snap.DocFallbackSeekNs, uint64(0),
		"fallback seek-timing branch (iterator.go:84-86) did not fire")
	assert.Greater(t, snap.DocFallbackParseNs, uint64(0),
		"fallback parse-timing branch (iterator.go:99-101) did not fire")
}

// TestIterator_Doc_ErrorPropagates pins iterator.go:67-69 — when pi.err is
// a non-EOF error, Doc() returns it immediately without attempting fetch.
func TestIterator_Doc_ErrorPropagates(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_doc_err")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	pi := it.(*planIterator)
	pi.err = errors.New("boom")

	_, err = it.Doc()
	require.Error(t, err)
	assert.Equal(t, "boom", err.Error())
}

// TestIterator_Doc_Fallback_SeekErr pins iterator.go:87-88 — when the data
// cursor SeekExact fails (invalid docId), Doc() returns the btree error.
func TestIterator_Doc_Fallback_SeekErr(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_doc_seek_err")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	pi := it.(*planIterator)
	require.True(t, it.Next())
	// Force fallback with a docId that does NOT exist in the data namespace.
	pi.docId = append(pi.docId[:0], "nonexistent-doc-id"...)
	pi.plan.DocParsed = nil

	_, err = it.Doc()
	require.ErrorIs(t, err, btree.ErrKeyNotFound,
		"fallback with missing docId must surface the specific btree.ErrKeyNotFound sentinel")
}

// TestIterator_Doc_FallbackPath forces the Doc() fallback branch
// (iterator.go:76-109) by clearing plan.DocParsed after Next() and ensuring
// pi.docId is populated. This simulates a plan whose upstream iterator does
// not set DocParsed — the fallback re-reads the document from data via
// pi.docId.
func TestIterator_Doc_FallbackPath(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "iter_doc_fallback")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":"hello"}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it.Close()

	_, ok := it.(*planIterator)
	require.True(t, ok)
	require.True(t, it.Next())

	// The normal plan path populates DocParsed, so Next skips the docId copy
	// at iterator.go:55-58. To exercise the fallback at 76-109 we must:
	//  (a) populate pi.docId to what we want to re-read,
	//  (b) clear plan.DocParsed.
	// This simulates a plan variant whose upstream does not set DocParsed.
	doc, err := it.Doc()
	require.NoError(t, err)
	parsedID := doc.Value().Get("id")
	require.NotNil(t, parsedID, "expected id from first Doc")

	// Manually populate docId and clear DocParsed to take the fallback.
	// Take the doc's id bytes via the item's internal idBuf — we reconstruct
	// by calling newItem on the parsed value and using its appendId.
	it2, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	defer it2.Close()
	pi2 := it2.(*planIterator)
	require.True(t, it2.Next())
	// After Next, DocParsed is populated. Save it, clear it, and stash docId.
	parsed := pi2.plan.DocParsed
	require.NotNil(t, parsed)
	// newItem(parsed).appendId(nil) gives us the docId bytes used by the
	// data namespace.
	reconstructedItem, err := newItem(parsed)
	require.NoError(t, err)
	pi2.docId = reconstructedItem.appendId(pi2.docId[:0])
	pi2.plan.DocParsed = nil

	doc2, err := it2.Doc()
	require.NoError(t, err)
	assert.Equal(t, `{"id":1,"a":"hello"}`, doc2.Value().String(),
		"fallback must re-read the same document via docId")
}
