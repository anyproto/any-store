package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// TestValidateIndexField covers every branch of validateIndexField.
func TestValidateIndexField(t *testing.T) {
	t.Run("empty_is_error", func(t *testing.T) {
		err := validateIndexField("")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})
	t.Run("dash_only_is_error", func(t *testing.T) {
		// A lone "-" is the reverse-marker with no field name after it.
		err := validateIndexField("-")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "empty")
	})
	t.Run("dollar_prefix_is_error", func(t *testing.T) {
		err := validateIndexField("$meta")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid")
	})
	t.Run("valid_field", func(t *testing.T) {
		require.NoError(t, validateIndexField("name"))
		require.NoError(t, validateIndexField("-createdAt"))
		require.NoError(t, validateIndexField("nested.path"))
	})
}

// TestIndex_Close pins the trivial Close implementation (returns nil).
// If Close ever grows side effects, this test documents the current contract.
func TestIndex_Close(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_close_test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "ix_a", Fields: []string{"a"}}))

	indexes := coll.GetIndexes()
	require.Len(t, indexes, 1)
	idx := indexes[0].(*index)
	err = idx.Close()
	assert.NoError(t, err, "Close is a no-op today; returns nil")
}

// TestIndex_InsertKeys_IdempotentSameDoc directly calls insertKeys twice with
// the same item in the same transaction to exercise the "same doc, idempotent"
// branch at index.go:157. This branch is not reachable via UpsertOne because
// collection.update short-circuits on anyencutil.Equal before insertKeys runs.
func TestIndex_InsertKeys_IdempotentSameDoc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_idempotent_direct")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ix_uq", Fields: []string{"a"}, Unique: true,
	}))

	idx := coll.GetIndexes()[0].(*index)
	it, itErr := newItem(anyenc.MustParseJson(`{"id":1,"a":42}`))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	btWtx := wrTx.btreeWriteTx()

	// First insert: populates the unique index.
	require.NoError(t, idx.insertKeys(btWtx, it))
	// Second insert with the same item: unique seek finds the existing entry
	// matching fullKeyBuf → takes the idempotent continue at index.go:157.
	require.NoError(t, idx.insertKeys(btWtx, it),
		"re-inserting the same (key, docId) pair must hit the idempotent branch")

	require.NoError(t, wrTx.Rollback())

	// Cross-doc collision via the public API still rejects.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":99}`)))
	require.ErrorIs(t,
		coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":99}`)),
		ErrUniqueConstraint,
		"cross-doc duplicate must be rejected through the full pipeline")
}

// TestIndex_DeleteKeys_SwallowsErrKeyNotFound directly calls deleteKeys on an
// item whose index entry was never inserted, so tx.Delete returns
// btree.ErrKeyNotFound. The function must swallow that error (index.go:184-187).
// This branch is not reachable via the public update path because the sparse
// code path produces an empty keysBuf and skips the Delete call entirely.
func TestIndex_DeleteKeys_SwallowsErrKeyNotFound(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_delete_missing_direct")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ix_a", Fields: []string{"a"},
	}))

	idx := coll.GetIndexes()[0].(*index)
	// Build an item with a present field "a" so fillKeysBuf produces a real
	// key. The index namespace is empty (we inserted nothing via the public
	// API), so tx.Delete on that key returns ErrKeyNotFound.
	it, itErr := newItem(anyenc.MustParseJson(`{"id":1,"a":"never-inserted"}`))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	btWtx := wrTx.btreeWriteTx()

	// Must succeed without surfacing ErrKeyNotFound.
	require.NoError(t, idx.deleteKeys(btWtx, it),
		"deleteKeys must swallow ErrKeyNotFound from tx.Delete")

	require.NoError(t, wrTx.Rollback())
}
