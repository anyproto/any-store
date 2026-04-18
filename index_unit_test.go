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
		// The error must surface the rejected input so callers can see what
		// was wrong — a bare "invalid" is not enough.
		assert.Contains(t, err.Error(), "$meta",
			"error message must include the literal rejected input")
	})
	t.Run("dollar_mid_string_ok", func(t *testing.T) {
		// Dollar not at prefix is legal — proves the reject rule is a
		// HasPrefix check, not a substring/Contains check.
		require.NoError(t, validateIndexField("a$b"))
	})
	t.Run("valid_field", func(t *testing.T) {
		require.NoError(t, validateIndexField("name"))
		require.NoError(t, validateIndexField("-createdAt"))
		require.NoError(t, validateIndexField("nested.path"))
	})
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
	countAfterFirst, err := btWtx.Count(idx.ns)
	require.NoError(t, err)
	require.Equal(t, 1, countAfterFirst, "first insert must produce exactly one index row")

	// Capture sketch state pre-re-insert. If sketch is nil in this test
	// setup, we simply skip the sketch assertion — no harm done.
	//
	// Note: we only assert on the per-key bucket, NOT on DocCount. The
	// production insertKeys calls IncrementDocCount() unconditionally after
	// the per-key loop (index.go:169-172), so DocCount does advance on the
	// idempotent path too — that's an existing quirk, not something the
	// idempotent branch is expected to suppress. The per-key Increment IS
	// inside the loop and IS skipped via `continue` on idempotent re-insert,
	// so that's the counter we can meaningfully pin here.
	var sketchKeyCountBefore uint64
	// Snapshot the encoded key by value — keysBuf is reused by the second
	// insertKeys call and its backing storage will be overwritten in place.
	var keyCopy []byte
	if idx.sketch != nil {
		keyCopy = append(keyCopy, idx.keysBuf[0]...)
		sketchKeyCountBefore = idx.sketch.Estimate(keyCopy)
	}

	// Second insert with the same item: unique seek finds the existing entry
	// matching fullKeyBuf → takes the idempotent continue at index.go:157.
	require.NoError(t, idx.insertKeys(btWtx, it),
		"re-inserting the same (key, docId) pair must hit the idempotent branch")

	// Core idempotency proof: the namespace still holds exactly one row.
	// Nothing was re-inserted, nothing was duplicated.
	countAfterSecond, err := btWtx.Count(idx.ns)
	require.NoError(t, err)
	require.Equal(t, 1, countAfterSecond,
		"idempotent re-insert must not add a second index row")

	// If the index has a sketch, the per-key bucket must not advance on the
	// idempotent path — otherwise per-value selectivity estimates would drift
	// on every duplicate re-insert.
	if idx.sketch != nil {
		assert.Equal(t, sketchKeyCountBefore, idx.sketch.Estimate(keyCopy),
			"sketch per-key bucket must not increment on idempotent re-insert")
	}

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

	// Populate one real doc so the index has an entry. If we skipped this,
	// we could not distinguish "deleteKeys swallowed ErrKeyNotFound" from
	// "deleteKeys short-circuited because the namespace was empty".
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"other","a":"real"}`)))

	// Build an item with a present field "a" that was NEVER inserted.
	// fillKeysBuf will produce a real key for it, and tx.Delete on that
	// key returns ErrKeyNotFound.
	it, itErr := newItem(anyenc.MustParseJson(`{"id":"never-inserted-id","a":"never-inserted"}`))
	require.NoError(t, itErr)

	wrTx, err := coll.WriteTx(ctx)
	require.NoError(t, err)
	btWtx := wrTx.btreeWriteTx()

	// Must succeed without surfacing ErrKeyNotFound.
	require.NoError(t, idx.deleteKeys(btWtx, it),
		"deleteKeys must swallow ErrKeyNotFound from tx.Delete")

	// The "other" doc's index entry must still be present — we didn't
	// accidentally blast unrelated keys while swallowing ErrKeyNotFound.
	countAfter, err := btWtx.Count(idx.ns)
	require.NoError(t, err)
	require.Equal(t, 1, countAfter,
		"deleteKeys on a never-inserted doc must not touch unrelated index rows")

	require.NoError(t, wrTx.Rollback())
}
