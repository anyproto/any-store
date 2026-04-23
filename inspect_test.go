package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestInspectIndexSketch_BasicReturnsCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}

	insp, ok := fx.DB.(IndexSketchInspector)
	require.True(t, ok, "db must implement IndexSketchInspector")

	info, err := insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, 1024, info.Size)
	assert.Equal(t, uint64(50), info.DocCount)
	require.Len(t, info.Buckets, 1024)

	var sum uint64
	for _, b := range info.Buckets {
		sum += b
	}
	assert.Equal(t, uint64(50), sum)
}

func TestInspectIndexSketch_UnknownIndexReturnsErrNotFound(t *testing.T) {
	fx := newFixture(t)
	_, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	insp := fx.DB.(IndexSketchInspector)
	_, err = insp.InspectIndexSketch(ctx, "test", "nonexistent")
	assert.ErrorIs(t, err, ErrIndexNotFound)
}

func TestInspectIndexSketch_AccumulatesOnInsertAndDelete(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 30 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%3),
		)))
	}

	insp := fx.DB.(IndexSketchInspector)
	info, err := insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, uint64(30), info.DocCount)

	for i := range 10 {
		require.NoError(t, coll.DeleteId(ctx, i))
	}

	info, err = insp.InspectIndexSketch(ctx, "test", "a")
	require.NoError(t, err)
	assert.Equal(t, uint64(20), info.DocCount)
}
