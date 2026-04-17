package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
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
