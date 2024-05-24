package anystore

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollQuery_Count(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, `{"a":1}`, `{"a":2}`, `{"a":3}`, `{"a":4}`, `{"a":5}`))

	t.Run("no filter", func(t *testing.T) {
		count, err := coll.Query().Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count)
	})

	t.Run("filter", func(t *testing.T) {
		count, err := coll.Query().Cond(`{"a":{"$in":[2,3,4]}}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 3, count)
	})

}
