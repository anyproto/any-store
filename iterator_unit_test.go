package anystore

import (
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

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
	assert.NotEmpty(t, s, "planIterator.String must return plan.String()")
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
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))

	it, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	// First Next should see one doc.
	assert.True(t, it.Next())
	require.NoError(t, it.Close())
	// After Close, Next must report done.
	assert.False(t, it.Next(), "Next after Close must return false")
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

	// Verify the perf counters actually recorded our calls.
	snap := snapshotPipelinePerfCounters()
	assert.GreaterOrEqual(t, snap.DocCalls, uint64(2))
	assert.GreaterOrEqual(t, snap.DocParsedHits, uint64(1))
	assert.GreaterOrEqual(t, snap.DocFallbacks, uint64(1))
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
	require.Error(t, err, "fallback with missing docId must surface btree error")
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
