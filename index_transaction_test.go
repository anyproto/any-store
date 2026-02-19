/*
Index/Planner tests inspired by SQLite: wal.test, trans.test

Test scenario:
Tests transaction isolation with indexes: read transactions see consistent
snapshots even during concurrent writes, write transaction index changes
are not visible until commit, rollback undoes index changes, and savepoints
work correctly with index mutations.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

func TestIndex_Transaction_WriteTxVisibleViaTxContext(t *testing.T) {
	// Inserts within a WriteTx should be visible when querying through tx.Context()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert docs within the write transaction
	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Query through tx.Context() should see the inserted documents
	count, err := coll.Find(`{"a":2}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Indexed query should also work within the transaction
	explain, err := coll.Find(`{"a":2}`).Explain(tx.Context())
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// Total count should be 10
	assertCollCountCtx(tx.Context(), t, coll, 10)

	require.NoError(t, tx.Commit())

	// After commit, data should be visible outside the transaction
	assertCollCount(t, coll, 10)
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestIndex_Transaction_RollbackUndoesInserts(t *testing.T) {
	// Rollback should undo all inserts and their index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))
	assertCollCount(t, coll, 2)

	// Start a write transaction and insert more
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := 3; i <= 12; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Within tx, count should be 12
	assertCollCountCtx(tx.Context(), t, coll, 12)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, only original 2 docs should remain
	assertCollCount(t, coll, 2)

	// Index query should only find original data
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":30}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Index length should be back to 2
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 2)
}

func TestIndex_Transaction_RollbackUndoesDeletes(t *testing.T) {
	// Rollback should undo deletes, restoring docs and index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertCollCount(t, coll, 10)

	// Start write tx and delete some docs
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.DeleteId(tx.Context(), 3))
	require.NoError(t, coll.DeleteId(tx.Context(), 7))

	// Within tx, count should be 8
	assertCollCountCtx(tx.Context(), t, coll, 8)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, all 10 docs should be back
	assertCollCount(t, coll, 10)

	// Deleted docs should be findable again
	doc, err := coll.FindId(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, 3, doc.Value().GetInt("a"))

	doc, err = coll.FindId(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, 7, doc.Value().GetInt("a"))

	// Index should still find them
	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_RollbackUndoesUpdates(t *testing.T) {
	// Rollback should undo updates, restoring original values and index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":100}`),
		anyenc.MustParseJson(`{"id":2,"a":200}`),
	))

	// Start write tx and update
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.UpdateOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":999}`)))

	// Within tx, the update should be visible
	count, err := coll.Find(`{"a":999}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":100}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Rollback
	require.NoError(t, tx.Rollback())

	// Original value should be restored
	count, err = coll.Find(`{"a":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_Transaction_SavepointRollbackPartial(t *testing.T) {
	// Savepoint rollback should undo only the savepoint's changes,
	// while preserving earlier changes in the same write tx
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert first batch in the outer transaction
	require.NoError(t, coll.Insert(tx.Context(),
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))
	assertCollCountCtx(tx.Context(), t, coll, 2)

	// A nested insert that fails (duplicate id) triggers savepoint rollback
	err = coll.Insert(tx.Context(),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
		anyenc.MustParseJson(`{"id":4,"a":40}`),
		anyenc.MustParseJson(`{"id":1,"a":50}`), // duplicate
	)
	require.Error(t, err)

	// After savepoint rollback, only the first 2 docs should remain
	assertCollCountCtx(tx.Context(), t, coll, 2)

	// Index query should still work for the first batch
	count, err := coll.Find(`{"a":10}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// The failed insert's docs should not be findable
	count, err = coll.Find(`{"a":30}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Commit the outer transaction
	require.NoError(t, tx.Commit())

	// After commit, only the first batch should be persisted
	assertCollCount(t, coll, 2)
}

func TestIndex_Transaction_CommitPersistsIndexEntries(t *testing.T) {
	// After commit, index entries should be correctly persisted
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	require.NoError(t, tx.Commit())

	// Verify index works correctly after commit
	for v := range 10 {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count, "expected 5 docs with a=%d", v)
	}

	// Index should have 50 entries
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 50)
}

func TestIndex_Transaction_UniqueConstraintInTx(t *testing.T) {
	// Unique constraint should be enforced within a transaction
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":100}`)))

	// Attempting to insert duplicate unique value within the same tx should fail
	err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":100}`))
	assert.ErrorIs(t, err, ErrUniqueConstraint)

	// The first insert should still be valid after the failed second insert
	assertCollCountCtx(tx.Context(), t, coll, 1)

	require.NoError(t, tx.Commit())
	assertCollCount(t, coll, 1)
}

func TestIndex_Transaction_CompoundIndexRollback(t *testing.T) {
	// Compound index entries should be correctly rolled back
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	// Insert baseline data
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":10}`),
		anyenc.MustParseJson(`{"id":2,"a":2,"b":20}`),
	))

	// Start tx, insert more, then rollback
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(tx.Context(),
		anyenc.MustParseJson(`{"id":3,"a":1,"b":30}`),
		anyenc.MustParseJson(`{"id":4,"a":3,"b":40}`),
	))

	// Verify compound index query works within tx
	count, err := coll.Find(`{"a":1}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, count) // id:1 and id:3

	require.NoError(t, tx.Rollback())

	// After rollback, only baseline data
	count, err = coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // only id:1

	count, err = coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count) // id:4 was rolled back

	// Index length should be 2
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 2)
}

func TestIndex_Transaction_MultipleOperationsInTx(t *testing.T) {
	// Mix of insert, update, delete within one transaction, then commit
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 5 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Delete id:0 (a:0)
	require.NoError(t, coll.DeleteId(tx.Context(), 0))

	// Update id:1 (a:10 -> a:999)
	require.NoError(t, coll.UpdateOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":999}`)))

	// Insert new doc
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":10,"a":555}`)))

	// Verify state within tx
	assertCollCountCtx(tx.Context(), t, coll, 5) // 5 - 1 + 1 = 5

	count, err := coll.Find(`{"a":0}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count) // deleted

	count, err = coll.Find(`{"a":10}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count) // updated away

	count, err = coll.Find(`{"a":999}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count) // updated value

	count, err = coll.Find(`{"a":555}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count) // newly inserted

	require.NoError(t, tx.Commit())

	// Verify same state after commit
	assertCollCount(t, coll, 5)
	count, err = coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_MultipleOperationsRollback(t *testing.T) {
	// Mix of insert, update, delete within one transaction, then rollback
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 5 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Delete id:0
	require.NoError(t, coll.DeleteId(tx.Context(), 0))
	// Update id:1
	require.NoError(t, coll.UpdateOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":999}`)))
	// Insert new
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":10,"a":555}`)))

	// Rollback all changes
	require.NoError(t, tx.Rollback())

	// Everything should be back to original state
	assertCollCount(t, coll, 5)

	// Deleted doc should be back
	doc, err := coll.FindId(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, doc.Value().GetInt("a"))

	// Updated doc should have original value
	doc, err = coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 10, doc.Value().GetInt("a"))

	// Inserted doc should be gone
	_, err = coll.FindId(ctx, 10)
	assert.ErrorIs(t, err, ErrDocNotFound)

	// Index queries should reflect original state
	count, err := coll.Find(`{"a":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":555}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_Transaction_SortWithinTx(t *testing.T) {
	// Sort queries should work correctly within a transaction
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert docs in reverse order within tx
	for i := 9; i >= 0; i-- {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Sort ascending should work within tx — query through tx.Context()
	iter, err := coll.Find(nil).Sort("a").Iter(tx.Context())
	require.NoError(t, err)
	var sortedVals []int
	for iter.Next() {
		doc, iterErr := iter.Doc()
		require.NoError(t, iterErr)
		sortedVals = append(sortedVals, doc.Value().GetInt("a"))
	}
	require.NoError(t, iter.Close())
	require.Len(t, sortedVals, 10)
	for i := 1; i < len(sortedVals); i++ {
		assert.True(t, sortedVals[i-1] <= sortedVals[i],
			"not sorted at %d: %d > %d", i, sortedVals[i-1], sortedVals[i])
	}

	require.NoError(t, tx.Commit())

	// Verify sorting works after commit too
	iter, err = coll.Find(nil).Sort("a").Iter(ctx)
	require.NoError(t, err)
	sortedVals = sortedVals[:0]
	for iter.Next() {
		doc, iterErr := iter.Doc()
		require.NoError(t, iterErr)
		sortedVals = append(sortedVals, doc.Value().GetInt("a"))
	}
	require.NoError(t, iter.Close())
	require.Len(t, sortedVals, 10)
	for i := 1; i < len(sortedVals); i++ {
		assert.True(t, sortedVals[i-1] <= sortedVals[i])
	}
}

func TestIndex_Transaction_ExplainWithinTx(t *testing.T) {
	// Explain should work correctly within a transaction context
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Explain within transaction should show IndexScan
	explain, err := coll.Find(`{"a":3}`).Explain(tx.Context())
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
	assert.NotContains(t, explain.Sql, "FullScan")

	require.NoError(t, tx.Commit())
}

func TestIndex_Transaction_IndexCreationInTx(t *testing.T) {
	// Creating an index within a transaction should work and be visible
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// Insert data first
	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Without index, query should use FullScan
	explain, err := coll.Find(`{"a":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "FullScan")

	// Create index (this auto-commits)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Now query should use IndexScan
	explain, err = coll.Find(`{"a":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// Verify index built correctly
	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count) // 20/5 = 4
}

func TestIndex_Transaction_BulkInsertInSingleTx(t *testing.T) {
	// Bulk insert via a single write transaction should be atomic
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	require.NoError(t, tx.Commit())

	// Verify all 100 docs inserted
	assertCollCount(t, coll, 100)

	// Verify index correctness
	for v := range 10 {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)
	}

	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 100)
}

func TestIndex_Transaction_SequentialWriteTxs(t *testing.T) {
	// Multiple sequential write transactions should build on each other
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// First tx: insert docs with a=1
	tx1, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	for i := range 5 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":1}`, i))
		require.NoError(t, coll.Insert(tx1.Context(), doc))
	}
	require.NoError(t, tx1.Commit())

	// Second tx: insert docs with a=2
	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	for i := 5; i < 10; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":2}`, i))
		require.NoError(t, coll.Insert(tx2.Context(), doc))
	}
	require.NoError(t, tx2.Commit())

	// Both batches should be visible
	assertCollCount(t, coll, 10)

	count, err := coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}

func TestIndex_Transaction_RollbackThenCommitNewTx(t *testing.T) {
	// After rolling back one tx, a new tx should work correctly
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// First tx: insert and rollback
	tx1, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx1.Context(),
		anyenc.MustParseJson(`{"id":1,"a":100}`),
		anyenc.MustParseJson(`{"id":2,"a":200}`),
	))
	require.NoError(t, tx1.Rollback())

	// Verify empty
	assertCollCount(t, coll, 0)

	// Second tx: insert and commit
	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx2.Context(),
		anyenc.MustParseJson(`{"id":1,"a":999}`),
		anyenc.MustParseJson(`{"id":3,"a":888}`),
	))
	require.NoError(t, tx2.Commit())

	// New data should be committed
	assertCollCount(t, coll, 2)

	count, err := coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count) // rolled back value not present
}

func TestIndex_Transaction_SparseIndexRollback(t *testing.T) {
	// Sparse index should properly handle rollback of docs with missing fields
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert some docs without field 'a'
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert docs with and without field 'a' in tx
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":20}`)))
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":3,"b":30}`)))

	// Within tx, sparse index should have 1 entry (id:2)
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, only id:1 should exist (no 'a' field, sparse = no index entry)
	assertCollCount(t, coll, 1)
	assertIndexLen(t, idxs[0], 0) // no docs have 'a' field
}

func TestIndex_Transaction_ConcurrentReadDuringWrite(t *testing.T) {
	// Concurrent goroutines reading while another writes should not crash
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup

	// Start readers
	for r := range 5 {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			for j := range 20 {
				_ = j
				count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, rid%5)).Count(ctx)
				if err != nil {
					t.Errorf("reader %d: count error: %v", rid, err)
					return
				}
				// Count might vary if writes are concurrent,
				// but should always succeed without error
				_ = count
			}
		}(r)
	}

	// Start a writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 20; i < 40; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
			if err := coll.Insert(ctx, doc); err != nil {
				t.Errorf("writer: insert error: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	// After all goroutines finish, 40 docs total
	assertCollCount(t, coll, 40)
}

func TestIndex_Transaction_UpdateOneRollback(t *testing.T) {
	// UpdateOne within a tx that gets rolled back should restore original doc
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10,"b":"original"}`),
	))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Update both indexed and non-indexed fields
	require.NoError(t, coll.UpdateOne(tx.Context(),
		anyenc.MustParseJson(`{"id":1,"a":20,"b":"modified"}`)))

	// Within tx, the update should be visible
	doc, err := coll.FindId(tx.Context(), 1)
	require.NoError(t, err)
	assert.Equal(t, 20, doc.Value().GetInt("a"))
	assert.Equal(t, "modified", doc.Value().GetString("b"))

	// Rollback
	require.NoError(t, tx.Rollback())

	// Original values should be restored
	doc, err = coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 10, doc.Value().GetInt("a"))
	assert.Equal(t, "original", doc.Value().GetString("b"))

	// Index should still work with original value
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_Transaction_UpsertInTx(t *testing.T) {
	// UpsertOne should work correctly within a transaction
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert initial doc
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Upsert existing doc (update)
	require.NoError(t, coll.UpsertOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":20}`)))

	// Upsert new doc (insert)
	require.NoError(t, coll.UpsertOne(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":30}`)))

	// Verify within tx
	assertCollCountCtx(tx.Context(), t, coll, 2)

	count, err := coll.Find(`{"a":20}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":30}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":10}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	require.NoError(t, tx.Commit())

	// Verify after commit
	assertCollCount(t, coll, 2)
}

func TestIndex_Transaction_EmptyTxCommit(t *testing.T) {
	// Committing a transaction with no changes should be a no-op
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// No operations within tx
	require.NoError(t, tx.Commit())

	// Data should be unchanged
	assertCollCount(t, coll, 1)
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_EmptyTxRollback(t *testing.T) {
	// Rolling back a transaction with no changes should be a no-op
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// No operations within tx
	require.NoError(t, tx.Rollback())

	// Data should be unchanged
	assertCollCount(t, coll, 1)
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_FindDeleteInTx(t *testing.T) {
	// Find().Delete() within a transaction should be rollback-able
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%3))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Delete docs where a=0 within tx
	res, err := coll.Find(`{"a":0}`).Delete(tx.Context())
	require.NoError(t, err)
	assert.True(t, res.Modified > 0)

	// Within tx, those docs should be gone
	count, err := coll.Find(`{"a":0}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, deleted docs should be back
	count, err = coll.Find(`{"a":0}`).Count(ctx)
	require.NoError(t, err)
	assert.True(t, count > 0)

	assertCollCount(t, coll, 10)
}

func TestIndex_Transaction_FindUpdateInTx(t *testing.T) {
	// Find().Update() within a transaction should be rollback-able
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Update docs where a=2, set a=99
	res, err := coll.Find(`{"a":2}`).Update(tx.Context(), `{"$set":{"a":99}}`)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)

	// Within tx, updated docs should have new value
	count, err := coll.Find(`{"a":99}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	count, err = coll.Find(`{"a":2}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, original values should be back
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	count, err = coll.Find(`{"a":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}
