package anystore

/*
Audit tests for the "corruption-recovery" domain (docs/qplanner/audit/actionable_by_domain.json).
These pin documented limitations (index/data trust boundaries) and prove that
DropIndex + EnsureIndex rebuild recovers. Direct-namespace corruption uses the
same internal pattern as index_corruption_test.go.

  act-38  A stale unique entry (data row deleted directly) falsely blocks reinsert; rebuild fixes it.
  act-39  API delete only removes keys recomputed from the doc's current values; a spurious
          same-docId entry is orphaned, not self-healed; rebuild fixes it.
  act-40  DeleteId on a doc whose data row is already gone fails fast (ErrDocNotFound), index untouched.
  act-41  A missing trailing compound field == explicit null padding; rebuild restores a deleted padding entry.
  act-42  EnsureIndex on a same-named index with a changed Unique flag is a swallowed no-op.
*/

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

// act-38
func TestIndex_Corruption_UniqueStaleEntryBlocksReinsert(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":100}`)))

	// Delete the data row directly, leaving the phantom unique index entry.
	c := coll.(*collection)
	idKey := anyenc.AppendAnyValue(anyenc.Tuple(nil), 1)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	}))

	// Unique insertKeys trusts the index entry (no data-liveness check) → false reject.
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":100}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assertCollCount(t, coll, 0)

	// Recovery: rebuild from (now-empty) data.
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":2,"a":100}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 1)
	assertQueryCount(t, coll.Find(`{"a":100}`), 1)
}

// act-39
func TestIndex_Corruption_DeleteLeavesOrphanedSpuriousEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":100}`)))

	c, idx := findIndex(t, coll, "a")
	// Inject a spurious entry: same docId=1, different value 777.
	injectRawIndexEntry(t, c, idx, buildIndexFullKey([]any{777}, 1), []byte{0x00})
	assertIndexLen(t, idx, 2)

	// API delete recomputes keys from the doc's CURRENT value (a=100); the 777
	// entry is never visited → orphaned, not self-healed.
	require.NoError(t, coll.DeleteId(ctx, 1))
	assertCollCount(t, coll, 0)
	assertIndexLen(t, idx, 1)

	// Recovery: rebuild from (empty) data drops the orphan.
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

// act-40
func TestIndex_Corruption_DeleteIdAfterDataRowGone(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 1; i <= 3; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			`{"id":`+itoa(i)+`,"a":`+itoa(i*10)+`}`)))
	}

	c := coll.(*collection)
	idKey := anyenc.AppendAnyValue(anyenc.Tuple(nil), 2)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	}))
	assertIndexLen(t, coll.GetIndexes()[0], 3)

	// loadById maps ErrKeyNotFound → ErrDocNotFound and returns before deleteItem.
	err = coll.DeleteId(ctx, 2)
	require.ErrorIs(t, err, ErrDocNotFound)
	assertIndexLen(t, coll.GetIndexes()[0], 3) // stale entry untouched
	assertCollCount(t, coll, 2)

	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 2)
	assertCollCount(t, coll, 2)
}

// act-41
func TestIndex_Corruption_CompoundRebuildRestoresNullPadding(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":2}`),
		anyenc.MustParseJson(`{"id":2,"a":1}`),      // missing b → null padding
		anyenc.MustParseJson(`{"id":3,"a":1,"b":null}`),
	))
	c, idx := findIndex(t, coll, "a,b")
	assertIndexLen(t, idx, 3)

	ids := func(filter string) []string {
		return collectField(t, coll.Find(filter).Sort("id"), "id")
	}
	assert.ElementsMatch(t, []string{"2", "3"}, ids(`{"a":1,"b":null}`))
	assertQueryCount(t, coll.Find(`{"a":1,"b":2}`), 1)

	// Directly delete id=2's (1,null) compound entry.
	full := buildIndexFullKey([]any{1, nil}, 2)
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, full)
	}))
	assertIndexLen(t, idx, 2)
	assert.ElementsMatch(t, []string{"3"}, ids(`{"a":1,"b":null}`))

	// Recovery: rebuild restores the deleted null-padding entry.
	require.NoError(t, coll.DropIndex(ctx, "a,b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))
	assert.ElementsMatch(t, []string{"2", "3"}, ids(`{"a":1,"b":null}`))
	assertQueryCount(t, coll.Find(`{"a":1,"b":2}`), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 3)
}

// act-42
func TestIndex_EnsureIndex_SameNameChangedDefinitionIsNoOp(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":5}`),
		anyenc.MustParseJson(`{"id":2,"a":5}`),
	))

	// Index name == join(Fields), so a changed Unique flag does NOT change the name;
	// EnsureIndex swallows ErrIndexExists and keeps the old (non-unique) definition.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	assert.False(t, coll.GetIndexes()[0].Info().Unique)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":5}`))) // still allowed

	// Upgrade path: drop then ensure-unique — backfill fails on the existing dups.
	require.NoError(t, coll.DropIndex(ctx, "a"))
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true})
	require.ErrorIs(t, err, ErrUniqueConstraint)
	assert.Empty(t, coll.GetIndexes())

	// Remove the duplicates, then the unique index builds and enforces.
	require.NoError(t, coll.DeleteId(ctx, 2))
	require.NoError(t, coll.DeleteId(ctx, 3))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))
	assert.True(t, coll.GetIndexes()[0].Info().Unique)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":4,"a":5}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
}

// itoa avoids importing strconv just for a couple of conversions.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
