package anystore

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollection_Drop(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Drop(ctx))
	// TODO: add indexed and data
	_, err = fx.OpenCollection(ctx, "test")
	assert.ErrorIs(t, err, ErrCollectionNotFound)
}

func TestCollection_Rename(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	const newName = "newName"
	require.NoError(t, err)
	require.NoError(t, coll.Rename(ctx, newName))
	assert.Equal(t, coll.Name(), newName)

	collections, err := fx.GetCollectionNames(ctx)
	require.NoError(t, err)
	assert.Equal(t, []string{newName}, collections)
}
