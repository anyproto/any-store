package anystore

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Collection and index names are capped so the derived namespace names
// (widest: "ftx:"+coll+":"+index+":vocab") can never overflow a master-table
// cell at the default page size.

func TestNameLengthCap_Collection(t *testing.T) {
	fx := newFixture(t)

	okName := strings.Repeat("c", 255)
	coll, err := fx.CreateCollection(ctx, okName)
	require.NoError(t, err)
	require.NoError(t, coll.Close())

	tooLong := strings.Repeat("c", 256)
	_, err = fx.CreateCollection(ctx, tooLong)
	assert.ErrorIs(t, err, ErrInvalidCollectionName)

	short, err := fx.CreateCollection(ctx, "short")
	require.NoError(t, err)
	assert.ErrorIs(t, short.Rename(ctx, tooLong), ErrInvalidCollectionName)
	assert.Equal(t, "short", short.Name())
}

func TestNameLengthCap_Index(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)

	okName := strings.Repeat("i", 255)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: okName, Fields: []string{"a"}}))

	tooLong := strings.Repeat("i", 256)
	err = coll.EnsureIndex(ctx, IndexInfo{Name: tooLong, Fields: []string{"b"}})
	assert.ErrorIs(t, err, ErrInvalidIndexName)

	// A name synthesized from the field list is capped the same way.
	longField := strings.Repeat("f", 300)
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{longField}})
	assert.ErrorIs(t, err, ErrInvalidIndexName)

	// Full-text index creation goes through the same validation.
	err = coll.EnsureIndex(ctx, IndexInfo{Name: tooLong, Fields: []string{"body"}, Kind: IndexKindFulltext})
	assert.ErrorIs(t, err, ErrInvalidIndexName)

	require.NoError(t, fx.IntegrityCheck(ctx))
}
