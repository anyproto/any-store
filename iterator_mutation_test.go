package anystore

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Mutating the iterated collection through the same write tx is UNDEFINED
// (see Iterator): which rows the traversal visits is not asserted here. This
// pins the SAFE half of the contract — no panic, no iterator error, and the
// committed store stays fully consistent (count matches what was actually
// deleted, structural integrity clean).
func TestIteratorDeleteDuringIterationStaysConsistent(t *testing.T) {
	const n = 500
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":%d}`, i, i%7))))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	iter, err := coll.Find(nil).Iter(tx.Context())
	require.NoError(t, err)
	deleted := 0
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		id := doc.Value().GetInt("id")
		require.NoError(t, coll.DeleteId(tx.Context(), id))
		deleted++
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	require.NoError(t, tx.Commit())

	// The committed state is exactly consistent with the deletes performed.
	cnt, err := coll.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, n-deleted, cnt)
	visited := 0
	fresh, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	for fresh.Next() {
		visited++
	}
	require.NoError(t, fresh.Err())
	require.NoError(t, fresh.Close())
	assert.Equal(t, cnt, visited)
	require.NoError(t, fx.IntegrityCheck(ctx))
}
