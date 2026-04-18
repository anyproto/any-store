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

// TestIndex_Insert_Unique_IdempotentSameDoc covers the "same doc, idempotent"
// branch in insertKeys at index.go:157 — re-inserting the same (docId, key)
// pair does not trigger ErrUniqueConstraint.
func TestIndex_Insert_Unique_IdempotentSameDoc(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_idempotent")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ix_a_unique", Fields: []string{"a"}, Unique: true,
	}))

	// Insert once
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)))
	// Re-upsert the SAME doc with the SAME value via UpsertOne — this exercises
	// the "same doc, idempotent" branch in insertKeys (index.go:157) because
	// the unique seek finds the existing key and matches the doc's full key.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":42}`)),
		"upserting same doc with same value must be idempotent")

	// Update to a different value — removes old key, adds new.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"a":99}`)))

	// Cross-doc collision remains an error.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":100}`)))
	require.ErrorIs(t,
		coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":99}`)),
		ErrUniqueConstraint,
		"cross-doc duplicate must still be rejected")
}

// TestIndex_Delete_NonExistentKey_Swallowed covers deleteKeys at
// index.go:184-188 — a Delete that returns ErrKeyNotFound is silently
// swallowed rather than propagated. Exercised by deleting a document whose
// index entries were manipulated out-of-band (simulated via sparse index
// where some docs have no index entry).
func TestIndex_Delete_NonExistentKey_Swallowed(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "idx_delete_missing")
	require.NoError(t, err)
	// Sparse index on field "a": docs without "a" have no index entry.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name: "ix_a_sparse", Fields: []string{"a"}, Sparse: true,
	}))

	// Insert a doc that doesn't have field "a" — no index entry is created.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":"x"}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Update the doc to still lack "a" — deleteKeys tries to remove a key
	// that never existed; deleteKeys' ErrKeyNotFound swallow keeps this valid.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":1,"b":"y"}`)),
		"update of sparse-index doc without the field must succeed")
}
