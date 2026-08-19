/*
Index/Planner tests inspired by SQLite: corruptG.test, corruptI.test, corruptE.test,
corrupt2.test, corrupt9.test, reindex.test

Test scenario:
Index corruption and recovery scenarios: stale index entries pointing to
deleted docs, missing index entries, index-data inconsistency, integrity
check on corrupted index pages, and EnsureIndex (drop+recreate) as recovery.

These tests verify that our system handles index corruption gracefully and
that EnsureIndex can rebuild a corrupted index from scratch.
*/
package anystore

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
	"strconv"
)

// --- Category 1: Stale Index Entries ---

// TestIndex_Corruption_StaleIndexEntry creates an index, then directly deletes
// a document from the data namespace (bypassing index cleanup). The index
// still has an entry pointing to the deleted doc. A query via index may
// return fewer results than expected. EnsureIndex rebuild fixes it.
// Inspired by corruptI.test section 7 (missing metadata causes stale references).
func TestIndex_Corruption_StaleIndexEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Insert 10 docs
	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}
	assertCollCount(t, coll, 10)

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Directly delete doc id=5 from the data namespace, bypassing index cleanup.
	// This leaves a stale entry in the index for a=50/docId=5.
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, 5)
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	})
	require.NoError(t, err)

	// Data namespace now has 9 docs
	assertCollCount(t, coll, 9)
	// But index still has 10 entries (stale entry for doc id=5)
	assertIndexLen(t, idx, 10)

	// FindId for deleted doc should fail
	_, err = coll.FindId(ctx, 5)
	require.ErrorIs(t, err, ErrDocNotFound)

	// Recovery: drop and recreate index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// After rebuild, index should have 9 entries
	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 9)

	// Queries should be consistent
	count, err := coll.Find(`{"a":50}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count, "stale entry for deleted doc should be gone after rebuild")

	count, err = coll.Find(`{"a":30}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestIndex_Corruption_MissingIndexEntry creates an index, then directly deletes
// an entry from the index namespace. The doc exists but the index doesn't know
// about it. A filtered query via index misses the doc.
// Inspired by corruptE.test (out-of-order/missing entries detected by integrity check).
func TestIndex_Corruption_MissingIndexEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Directly delete the index entry for doc id=7 (a=70).
	// For non-unique index: key = Tuple(a_value, docId)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 70) // a=70
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, 7) // docId=7
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, idKey...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)

	// Data has 10, index has 9
	assertCollCount(t, coll, 10)
	assertIndexLen(t, idx, 9)

	// Doc id=7 still exists in data
	doc, err := coll.FindId(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, `{"id":7,"a":70}`, doc.Value().String())

	// Recovery: drop and recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 10)

	// Now the query should find all 10 docs
	count, err := coll.Find(`{"a":70}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestIndex_Corruption_UniqueIndexWrongDocId creates a unique index, then
// modifies the value (docId) stored in an index entry to point to a different doc.
// Inspired by corruptG.test (corrupt index cell payload).
func TestIndex_Corruption_UniqueIndexWrongDocId(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":100}`),
		anyenc.MustParseJson(`{"id":2,"a":200}`),
		anyenc.MustParseJson(`{"id":3,"a":300}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 3)

	// For unique index: key = Tuple(a_value), value = docId
	// Overwrite the value for a=200 to point to docId=999 (non-existent doc)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 200)
	wrongDocId := anyenc.Tuple(nil)
	wrongDocId = anyenc.AppendAnyValue(wrongDocId, 999)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(idx.ns, idxKey, wrongDocId)
	})
	require.NoError(t, err)

	// Recovery: rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 3)

	// Verify all docs are properly indexed after rebuild
	for _, a := range []int{100, 200, 300} {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, a)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "a=%d should have 1 result after rebuild", a)
	}
}

// --- Category 2: Extra/Duplicate Index Entries ---

// TestIndex_Corruption_ExtraIndexEntries inserts extra spurious entries into the
// index namespace that don't correspond to any document. EnsureIndex rebuild
// should clean them up.
// Inspired by corrupt9.test (freelist corruption causes extra/duplicate pages
// during index rebuild).
func TestIndex_Corruption_ExtraIndexEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 5)

	// Insert 3 extra spurious entries into the index namespace
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, fakeA := range []int{999, 998, 997} {
			fakeIdxKey := anyenc.Tuple(nil)
			fakeIdxKey = anyenc.AppendAnyValue(fakeIdxKey, fakeA)
			fakeDocId := anyenc.Tuple(nil)
			fakeDocId = anyenc.AppendAnyValue(fakeDocId, fakeA) // non-existent docId
			fullKey := append(anyenc.Tuple(nil), fakeIdxKey...)
			fullKey = append(fullKey, fakeDocId...)
			if err := tx.Put(idx.ns, fullKey, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Index now has 8 entries (5 real + 3 fake)
	assertIndexLen(t, idx, 8)

	// Recovery: rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 5)
}

// --- Category 3: Index-Data Count Mismatch ---

// TestIndex_Corruption_CountMismatch verifies that after corruption the index
// count diverges from doc count, and rebuild restores consistency.
func TestIndex_Corruption_CountMismatch(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 20; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertCollCount(t, coll, 20)
	assertIndexLen(t, idx, 20)

	// Delete 5 docs from data only (bypass index)
	for i := 1; i <= 5; i++ {
		idKey := anyenc.Tuple(nil)
		idKey = anyenc.AppendAnyValue(idKey, i)
		err := c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.ns, idKey)
		})
		require.NoError(t, err)
	}

	// Data has 15, index has 20 — mismatch
	assertCollCount(t, coll, 15)
	assertIndexLen(t, idx, 20)

	// Rebuild fixes it
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertCollCount(t, coll, 15)
	assertIndexLen(t, newIdx, 15)
}

// --- Category 4: Recovery via EnsureIndex ---

// TestIndex_Corruption_EnsureIndexRecoversMissingEntries deletes multiple index
// entries directly, then rebuilds via drop+ensure. All docs should be re-indexed.
// Inspired by reindex.test (REINDEX rebuilds from scratch).
func TestIndex_Corruption_EnsureIndexRecoversMissingEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}))

	names := []string{"alice", "bob", "carol", "dave", "eve"}
	for i, name := range names {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"name":"%s"}`, i+1, name))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 5)

	// Delete index entries for "bob" and "dave" directly
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, entry := range []struct {
			name string
			id   int
		}{{"bob", 2}, {"dave", 4}} {
			idxKey := anyenc.Tuple(nil)
			idxKey = anyenc.AppendAnyValue(idxKey, entry.name)
			docId := anyenc.Tuple(nil)
			docId = anyenc.AppendAnyValue(docId, entry.id)
			fullKey := append(anyenc.Tuple(nil), idxKey...)
			fullKey = append(fullKey, docId...)
			if err := tx.Delete(idx.ns, fullKey); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 3)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "name"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"name"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 5)

	// All names should be findable
	for _, name := range names {
		count, err := coll.Find(fmt.Sprintf(`{"name":"%s"}`, name)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "name=%s should be found after rebuild", name)
	}
}

// TestIndex_Corruption_EnsureIndexRecoversStaleEntries adds stale entries
// to the index and rebuilds. The stale entries should be eliminated.
func TestIndex_Corruption_EnsureIndexRecoversStaleEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))

	for i := 1; i <= 8; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"x":%d}`, i, i))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]

	// Delete docs 3 and 6 from data only
	for _, docId := range []int{3, 6} {
		idKey := anyenc.Tuple(nil)
		idKey = anyenc.AppendAnyValue(idKey, docId)
		err := c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.ns, idKey)
		})
		require.NoError(t, err)
	}

	// Also add 2 fake entries
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, fakeX := range []int{100, 200} {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, fakeX)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, fakeX)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Put(idx.ns, full, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Data: 6, Index: 10 (8 original + 2 fake, but 2 are stale = 10)
	assertCollCount(t, coll, 6)
	assertIndexLen(t, idx, 10)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "x"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"x"}}))

	newIdx := coll.GetIndexes()[0]
	assertCollCount(t, coll, 6)
	assertIndexLen(t, newIdx, 6)
}

// --- Category 5: Compound Index Corruption ---

// TestIndex_Corruption_CompoundIndexMissingEntry corrupts a compound index by
// removing an entry, then rebuilds.
func TestIndex_Corruption_CompoundIndexMissingEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%3, i%5))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Delete compound index entry for doc id=4 (a=1, b=4)
	// Compound non-unique key: Tuple(a_value, b_value, docId)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 1) // a = 4%3 = 1
	idxKey = anyenc.AppendAnyValue(idxKey, 4) // b = 4%5 = 4
	docId := anyenc.Tuple(nil)
	docId = anyenc.AppendAnyValue(docId, 4)
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, docId...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 9)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "a,b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 10)

	// Doc id=4 should be findable via compound query
	count, err := coll.Find(`{"a":1,"b":4}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 6: Unique Index Corruption and Recovery ---

// TestIndex_Corruption_UniqueIndexDuplicateEntries inserts a duplicate key
// directly into a unique index namespace (bypassing constraint check).
// After rebuild, the correct unique constraint should be restored.
func TestIndex_Corruption_UniqueIndexDuplicateEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"email":"alice@test.com"}`),
		anyenc.MustParseJson(`{"id":2,"email":"bob@test.com"}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 2)

	// Corrupt: overwrite the unique index entry for "bob@test.com" to point to doc id=1
	// This means both alice and bob's emails map to doc id=1 in the index
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, "bob@test.com")
	wrongDocId := anyenc.Tuple(nil)
	wrongDocId = anyenc.AppendAnyValue(wrongDocId, 1)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(idx.ns, idxKey, wrongDocId)
	})
	require.NoError(t, err)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "email"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 2)

	// Unique constraint should work again
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"email":"alice@test.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// Both original docs should be findable
	count, err := coll.Find(`{"email":"alice@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"email":"bob@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 7: Multiple Index Corruption ---

// TestIndex_Corruption_MultipleIndexesCorrupted corrupts multiple indexes
// on the same collection, then rebuilds them all.
func TestIndex_Corruption_MultipleIndexesCorrupted(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := 1; i <= 15; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i, i*2))))
	}

	c := coll.(*collection)
	idxA := c.loadIndexes()[0]
	idxB := c.loadIndexes()[1]
	assertIndexLen(t, idxA, 15)
	assertIndexLen(t, idxB, 15)

	// Corrupt index A: delete entries for docs 1-5
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for i := 1; i <= 5; i++ {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, i)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, i)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Delete(idxA.ns, full); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	// Corrupt index B: add 3 fake entries
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, fakeB := range []int{900, 901, 902} {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, fakeB)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, fakeB)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Put(idxB.ns, full, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	assertIndexLen(t, idxA, 10)
	assertIndexLen(t, idxB, 18)

	// Rebuild both
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.DropIndex(ctx, "b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	indexes := coll.GetIndexes()
	require.Len(t, indexes, 2)
	for _, idx := range indexes {
		assertIndexLen(t, idx, 15)
	}
}

// --- Category 8: Sparse Index Corruption ---

// TestIndex_Corruption_SparseIndexExtraEntries adds entries to a sparse index
// for docs that don't have the indexed field. Rebuild should remove them.
func TestIndex_Corruption_SparseIndexExtraEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert mix: some with "a", some without
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"b":20}`),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
		anyenc.MustParseJson(`{"id":4,"c":40}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 2) // only docs 1 and 3

	// Corrupt: add fake entries for docs 2 and 4 (which don't have "a")
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		for _, entry := range []struct {
			val int
			id  int
		}{{99, 2}, {88, 4}} {
			k := anyenc.Tuple(nil)
			k = anyenc.AppendAnyValue(k, entry.val)
			d := anyenc.Tuple(nil)
			d = anyenc.AppendAnyValue(d, entry.id)
			full := append(anyenc.Tuple(nil), k...)
			full = append(full, d...)
			if err := tx.Put(idx.ns, full, nil); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 4)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Sparse: true}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 2) // back to 2
}

// --- Category 9: Corruption After Mutations ---

// TestIndex_Corruption_InsertAfterStaleCorruption inserts a new doc after
// creating stale index entries. The insert should succeed normally through
// the collection API.
func TestIndex_Corruption_InsertAfterStaleCorruption(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)

	// Delete doc 3 from data only (stale index entry remains)
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, 3)
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(c.ns, idKey)
	})
	require.NoError(t, err)

	// Insert new docs — should work fine despite stale index entries
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":6,"a":60}`),
		anyenc.MustParseJson(`{"id":7,"a":70}`),
	))

	assertCollCount(t, coll, 6) // 5-1+2 = 6

	// New docs should be findable
	count, err := coll.Find(`{"a":60}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":70}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// TestIndex_Corruption_DeleteAfterExtraEntries deletes a doc normally after
// extra entries were injected into the index.
func TestIndex_Corruption_DeleteAfterExtraEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 5; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]

	// Add fake entries
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		k := anyenc.Tuple(nil)
		k = anyenc.AppendAnyValue(k, 555)
		d := anyenc.Tuple(nil)
		d = anyenc.AppendAnyValue(d, 555)
		full := append(anyenc.Tuple(nil), k...)
		full = append(full, d...)
		return tx.Put(idx.ns, full, nil)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 6) // 5 + 1 fake

	// Normal delete should still work
	require.NoError(t, coll.DeleteId(ctx, 2))
	assertCollCount(t, coll, 4)
	assertIndexLen(t, idx, 5) // 6 - 1 (deleted doc 2's entry)

	// Rebuild to clean up fake entry
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 4)
}

// --- Category 10: Large Scale Corruption Recovery ---

// TestIndex_Corruption_LargeScaleRecovery corrupts a large index and verifies
// complete recovery via rebuild.
func TestIndex_Corruption_LargeScaleRecovery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"v"}}))

	// Insert 200 docs
	for i := 1; i <= 200; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"v":%d}`, i, i%20))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 200)

	// Delete 50 docs from data only (bypass index)
	for i := 1; i <= 50; i++ {
		idKey := anyenc.Tuple(nil)
		idKey = anyenc.AppendAnyValue(idKey, i)
		err := c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
			return tx.Delete(c.ns, idKey)
		})
		require.NoError(t, err)
	}

	assertCollCount(t, coll, 150)
	assertIndexLen(t, idx, 200) // stale

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "v"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"v"}}))

	newIdx := coll.GetIndexes()[0]
	assertCollCount(t, coll, 150)
	assertIndexLen(t, newIdx, 150)

	// Verify query correctness for each value bucket
	for v := 0; v < 20; v++ {
		count, err := coll.Find(fmt.Sprintf(`{"v":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		// docs 51-200 that have v=i%20: count how many i in [51,200] where i%20==v
		expected := 0
		for i := 51; i <= 200; i++ {
			if i%20 == v {
				expected++
			}
		}
		assert.Equal(t, expected, count, "v=%d", v)
	}
}

// --- Category 11: Array Index Corruption ---

// TestIndex_Corruption_ArrayIndexMissingEntries corrupts an array-based index
// by removing some multi-key entries. Rebuild should restore them.
func TestIndex_Corruption_ArrayIndexMissingEntries(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"tags":["go","rust"]}`),
		anyenc.MustParseJson(`{"id":2,"tags":["python","java"]}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	// Each doc: 2 elements + 1 array-as-value = 3 entries, total 6
	assertIndexLen(t, idx, 6)

	// Delete the "go" index entry for doc 1
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, "go")
	docId := anyenc.Tuple(nil)
	docId = anyenc.AppendAnyValue(docId, 1)
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, docId...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 5)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "tags"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 6)

	// "go" should be findable again
	count, err := coll.Find(`{"tags":"go"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 12: Index Namespace Deleted Entirely ---

// TestIndex_Corruption_IndexNamespaceCleared empties the index namespace entirely,
// then rebuilds via drop+ensure.
func TestIndex_Corruption_IndexNamespaceCleared(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := 1; i <= 10; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	c := coll.(*collection)
	idx := c.loadIndexes()[0]
	assertIndexLen(t, idx, 10)

	// Clear all entries from the index namespace
	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		cursor := tx.NewCursor(idx.ns)
		if err := cursor.First(); err != nil {
			return err
		}
		var keysToDelete [][]byte
		for cursor.Valid() {
			key, err := cursor.Key()
			if err != nil {
				return err
			}
			keysToDelete = append(keysToDelete, append([]byte(nil), key...))
			if err := cursor.Next(); err != nil {
				return err
			}
		}
		for _, key := range keysToDelete {
			if err := tx.Delete(idx.ns, key); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 0)

	// Data still has 10 docs
	assertCollCount(t, coll, 10)

	// Rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	newIdx := coll.GetIndexes()[0]
	assertIndexLen(t, newIdx, 10)

	// All docs findable via index
	for i := 1; i <= 10; i++ {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, i)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, count, "a=%d should be found", i)
	}
}

// --- Category 13: Update on Corrupted Index ---

// TestIndex_Corruption_UpdateWithMissingIndexEntry updates a doc whose index
// entry was previously deleted. The update should still work through the
// collection API (which deletes old + inserts new index entries).
func TestIndex_Corruption_UpdateWithMissingIndexEntry(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))

	c := coll.(*collection)
	idx := c.loadIndexes()[0]

	// Delete index entry for doc 1 (a=10)
	idxKey := anyenc.Tuple(nil)
	idxKey = anyenc.AppendAnyValue(idxKey, 10)
	docId := anyenc.Tuple(nil)
	docId = anyenc.AppendAnyValue(docId, 1)
	fullKey := append(anyenc.Tuple(nil), idxKey...)
	fullKey = append(fullKey, docId...)

	err = c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Delete(idx.ns, fullKey)
	})
	require.NoError(t, err)
	assertIndexLen(t, idx, 1)

	// Update doc 1: a=10 -> a=99.
	// The old index entry delete will silently fail (already deleted).
	// The new index entry for a=99 should be created.
	require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(`{"id":1,"a":99}`)))

	// Index should now have 2 entries (a=99/1 and a=20/2)
	assertIndexLen(t, idx, 2)

	count, err := coll.Find(`{"a":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// --- Category 14: High-Level Recovery via Drop/Recreate ---

// TestIndex_Corruption_RecoveryViaDropRecreate verifies that dropping and
// recreating an index via EnsureIndex rebuilds it correctly from existing data,
// producing the same query results as before.
func TestIndex_Corruption_RecoveryViaDropRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_drop_recreate")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i*3),
		)))
	}

	// Capture results before drop
	countBefore, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, countBefore)

	resultsBefore := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a"), "id")
	require.True(t, len(resultsBefore) > 0)

	assertIndexLen(t, coll.GetIndexes()[0], 50)

	// Drop and recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)

	// Verify same results after rebuild
	countAfter, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countBefore, countAfter)

	resultsAfter := collectField(t, coll.Find(`{"a":{"$gte":3,"$lte":7}}`).Sort("a"), "id")
	assert.Equal(t, resultsBefore, resultsAfter)

	assertIndexLen(t, coll.GetIndexes()[0], 50)

	// Verify index is used
	explain, err := coll.Find(`{"a":5}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

// TestIndex_Corruption_EnsureIndexRebuildsFromData verifies that EnsureIndex
// builds an index retroactively from existing documents, and that drop+recreate
// produces the same results.
func TestIndex_Corruption_EnsureIndexRebuildsFromData(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_rebuild")
	require.NoError(t, err)

	// Insert docs WITHOUT index
	for i := range 40 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%8),
		)))
	}

	// Capture non-indexed results
	countNoIdx, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, countNoIdx) // 40/8 = 5

	resultsNoIdx := collectField(t, coll.Find(`{"a":{"$gte":2,"$lte":5}}`).Sort("a"), "id")

	// Now create index retroactively
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 40)

	// Verify same results with index
	countIdx, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, countIdx)

	resultsIdx := collectField(t, coll.Find(`{"a":{"$gte":2,"$lte":5}}`).Sort("a"), "id")
	assert.Equal(t, resultsNoIdx, resultsIdx)

	// Drop, recreate again — still works
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	countAgain, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countNoIdx, countAgain)

	resultsAgain := collectField(t, coll.Find(`{"a":{"$gte":2,"$lte":5}}`).Sort("a"), "id")
	assert.Equal(t, resultsNoIdx, resultsAgain)
	assertIndexLen(t, coll.GetIndexes()[0], 40)
}

// --- Category 15: Crash/Unclean Close Recovery ---

// TestIndex_Corruption_CrashRecoveryWithIndex verifies that after closing the
// database (simulating shutdown), reopening recovers the index and queries work.
func TestIndex_Corruption_CrashRecoveryWithIndex(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-crash-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1: create, insert, index — then close (no explicit checkpoint)
	func() {
		db, err := Open(ctx, dbPath, nil)
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

		for i := range 30 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%6),
			)))
		}

		count, err := coll.Find(`{"a":2}`).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count)

		require.NoError(t, db.Close())
	}()

	// Phase 2: reopen and verify
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)

	count, err := coll2.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	assertCollCount(t, coll2, 30)
	assertIndexLen(t, indexes[0], 30)

	explain, err := coll2.Find(`{"a":2}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

// TestIndex_Corruption_PersistenceAfterUncleanClose verifies that indexes
// survive an unclean close (default CommitSync=false mode) and WAL recovery restores them.
func TestIndex_Corruption_PersistenceAfterUncleanClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-unclean-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1: insert data with index using default CommitSync=false
	func() {
		conf := &Config{}
		db, err := Open(ctx, dbPath, conf)
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true}))

		for i := range 50 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, i),
			)))
		}

		assertIndexLen(t, coll.GetIndexes()[0], 50)
		require.NoError(t, db.Close())
	}()

	// Phase 2: reopen and verify
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assert.Equal(t, "a", indexes[0].Info().Name)
	assert.True(t, indexes[0].Info().Unique)

	assertCollCount(t, coll2, 50)
	assertIndexLen(t, indexes[0], 50)

	count, err := coll2.Find(`{"a":25}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Unique constraint still holds
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":999,"a":0}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	count, err = coll2.Find(`{"a":{"$gte":10,"$lt":20}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)
}

// --- Category 16: Drop/Recreate After Deletions ---

// TestIndex_Corruption_DropAndRecreateFixesStaleData verifies that after
// deleting some docs, dropping and recreating the index produces an index
// that matches the current data state exactly.
func TestIndex_Corruption_DropAndRecreateFixesStaleData(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_stale")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 60 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10),
		)))
	}
	assertIndexLen(t, coll.GetIndexes()[0], 60)

	// Delete all docs where a=3 (6 docs)
	res, err := coll.Find(`{"a":3}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, res.Modified)
	assertCollCount(t, coll, 54)
	assertIndexLen(t, coll.GetIndexes()[0], 54)

	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Delete a=7 (6 docs)
	res, err = coll.Find(`{"a":7}`).Delete(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, res.Modified)
	assertCollCount(t, coll, 48)

	// Drop and recreate index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	assertIndexLen(t, coll.GetIndexes()[0], 48)

	count, err = coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":7}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, count)

	total := 0
	for v := range 10 {
		if v == 3 || v == 7 {
			continue
		}
		c, err := coll.Find(fmt.Sprintf(`{"a":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		total += c
	}
	assert.Equal(t, 48, total)
}

// --- Category 17: Multiple Drop/Recreate Cycles ---

// TestIndex_Corruption_MultipleDropRecreate verifies that multiple cycles of
// drop/recreate with interleaved data modifications produce a consistent index.
func TestIndex_Corruption_MultipleDropRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_multi_cycle")
	require.NoError(t, err)

	// Cycle 1: create index, insert data
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}
	assertIndexLen(t, coll.GetIndexes()[0], 20)
	count, err := coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count)

	// Cycle 2: drop, insert more, recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	for i := 20; i < 40; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}
	assertCollCount(t, coll, 40)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 40)
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 8, count) // 40/5

	// Cycle 3: drop, delete some, insert more, recreate
	require.NoError(t, coll.DropIndex(ctx, "a"))
	_, err = coll.Find(`{"a":0}`).Delete(ctx) // delete 8 docs with a=0
	require.NoError(t, err)
	for i := 40; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	assertCollCount(t, coll, 42) // 40 - 8 + 10
	assertIndexLen(t, coll.GetIndexes()[0], 42)

	// a=0: 8 deleted, 2 added (40,45) = 2
	count, err = coll.Find(`{"a":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// a=2: originally 8, none deleted, 2 more (42,47) = 10
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	explain, err := coll.Find(`{"a":2}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
}

// --- Category 18: EnsureIndex Idempotency ---

// TestIndex_Corruption_EnsureIndexIdempotentAfterCorruption verifies that
// EnsureIndex is a no-op when the index already exists, and works correctly
// after drop to rebuild.
func TestIndex_Corruption_EnsureIndexIdempotentAfterCorruption(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_idempotent")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := range 25 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5),
		)))
	}

	assertIndexLen(t, coll.GetIndexes()[0], 25)

	// EnsureIndex again — should be a no-op
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 25)

	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	// Drop and then EnsureIndex — should rebuild
	require.NoError(t, coll.DropIndex(ctx, "a"))
	assert.Len(t, coll.GetIndexes(), 0)

	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 25)

	count, err = coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	// EnsureIndex after rebuild — still idempotent
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.Len(t, coll.GetIndexes(), 1)
	assertIndexLen(t, coll.GetIndexes()[0], 25)
}

// --- Category 19: Crash Recovery With Multiple Indexes ---

// TestIndex_Corruption_CrashRecoveryMultipleIndexes verifies WAL recovery
// handles multiple indexes on the same collection.
func TestIndex_Corruption_CrashRecoveryMultipleIndexes(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-crash-multi-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1
	func() {
		db, err := Open(ctx, dbPath, &Config{})
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}, Unique: true}))
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

		for i := range 25 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%5, i),
			)))
		}

		require.NoError(t, db.Close())
	}()

	// Phase 2: verify all indexes survived
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 3)

	assertCollCount(t, coll2, 25)

	count, err := coll2.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	count, err = coll2.Find(`{"b":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// Unique constraint on b
	err = coll2.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":0,"b":0}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)
}

// --- Category 20: Compound and Unique Index Recovery ---

// TestIndex_Corruption_RecoveryWithCompoundIndex verifies drop/recreate
// recovery works with compound indexes.
func TestIndex_Corruption_RecoveryWithCompoundIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_compound_recovery")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	for i := range 60 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%6, i%4),
		)))
	}

	countBefore, err := coll.Find(`{"a":2,"b":1}`).Count(ctx)
	require.NoError(t, err)

	resultsBefore := collectField(t, coll.Find(`{"a":3}`).Sort("b"), "id")

	require.NoError(t, coll.DropIndex(ctx, "a,b"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	countAfter, err := coll.Find(`{"a":2,"b":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countBefore, countAfter)

	resultsAfter := collectField(t, coll.Find(`{"a":3}`).Sort("b"), "id")
	assert.Equal(t, resultsBefore, resultsAfter)

	assertIndexLen(t, coll.GetIndexes()[0], 60)
}

// TestIndex_Corruption_RecoveryWithUniqueIndex verifies drop/recreate
// recovery preserves unique constraint semantics.
func TestIndex_Corruption_RecoveryWithUniqueIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_unique_recovery")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	for i := range 30 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"email":"user%d@test.com"}`, i, i),
		)))
	}

	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"email":"user0@test.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	require.NoError(t, coll.DropIndex(ctx, "email"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	// Unique constraint still holds after rebuild
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"email":"user0@test.com"}`))
	require.ErrorIs(t, err, ErrUniqueConstraint)

	// But a new unique value works
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"email":"new@test.com"}`)))
	assertIndexLen(t, coll.GetIndexes()[0], 31)

	count, err := coll.Find(`{"email":"user15@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

// --- Category 21: Reopen After Multiple Write Batches ---

// TestIndex_Corruption_ReopenAfterMultipleWrites verifies that index
// consistency is maintained after multiple write batches and a reopen.
func TestIndex_Corruption_ReopenAfterMultipleWrites(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "idx-multi-write-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")

	// Phase 1: create, insert in multiple batches
	func() {
		db, err := Open(ctx, dbPath, nil)
		require.NoError(t, err)

		coll, err := db.CreateCollection(ctx, "test")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

		// Batch 1: inserts
		for i := range 20 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%4),
			)))
		}

		// Batch 2: updates
		for i := range 10 {
			require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, 100+i),
			)))
		}

		// Batch 3: deletes
		for i := 15; i < 20; i++ {
			require.NoError(t, coll.DeleteId(ctx, i))
		}

		require.NoError(t, db.Close())
	}()

	// Phase 2: reopen and verify consistency
	db2, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)
	defer db2.Close()

	coll2, err := db2.OpenCollection(ctx, "test")
	require.NoError(t, err)

	assertCollCount(t, coll2, 15) // 20 - 5 deleted

	indexes := coll2.GetIndexes()
	require.Len(t, indexes, 1)
	assertIndexLen(t, indexes[0], 15)

	// Updated docs findable via new values
	count, err := coll2.Find(`{"a":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // id=0 was updated to a=100

	// Deleted docs should not be found
	_, err = coll2.FindId(ctx, 17)
	require.ErrorIs(t, err, ErrDocNotFound)
}

// --- Category 22: Empty Collection Edge Case ---

// TestIndex_Corruption_EmptyCollectionDropRecreate verifies that
// drop/recreate works on empty collections.
func TestIndex_Corruption_EmptyCollectionDropRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_empty")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Drop and recreate on empty collection
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 0)

	// Now insert data, drop, recreate
	for i := range 5 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i),
		)))
	}
	assertIndexLen(t, coll.GetIndexes()[0], 5)

	// Delete all docs, drop, recreate
	_, err = coll.Find(nil).Delete(ctx)
	require.NoError(t, err)
	assertCollCount(t, coll, 0)

	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assertIndexLen(t, coll.GetIndexes()[0], 0)
}

// --- Category 23: Sparse Index Recovery ---

// TestIndex_Corruption_RecoveryWithSparseIndex verifies drop/recreate
// recovery works correctly with sparse indexes.
func TestIndex_Corruption_RecoveryWithSparseIndex(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_sparse_recovery")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"score"}, Sparse: true}))

	for i := range 20 {
		if i%3 == 0 {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"name":"no_score_%d"}`, i, i),
			)))
		} else {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"score":%d}`, i, i*10),
			)))
		}
	}

	// i=0,3,6,9,12,15,18 → 7 docs without score, 13 with score
	idxLenBefore, err := coll.GetIndexes()[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, 13, idxLenBefore)

	require.NoError(t, coll.DropIndex(ctx, "score"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"score"}, Sparse: true}))

	idxLenAfter, err := coll.GetIndexes()[0].Len(ctx)
	require.NoError(t, err)
	assert.Equal(t, idxLenBefore, idxLenAfter)

	assertCollCount(t, coll, 20)
}

// --- Category 24: Drop/Recreate With Updated Docs ---

// TestIndex_Corruption_DropRecreateWithUpdatedDocs verifies that after
// updating indexed fields, drop+recreate builds the index with the
// current (updated) values.
func TestIndex_Corruption_DropRecreateWithUpdatedDocs(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_updated")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"status"}}))

	for i := range 20 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"status":"active"}`, i),
		)))
	}

	count, err := coll.Find(`{"status":"active"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 20, count)

	// Update half to "inactive"
	for i := range 10 {
		require.NoError(t, coll.UpdateOne(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"status":"inactive"}`, i),
		)))
	}

	count, err = coll.Find(`{"status":"active"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count, err = coll.Find(`{"status":"inactive"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	require.NoError(t, coll.DropIndex(ctx, "status"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"status"}}))

	count, err = coll.Find(`{"status":"active"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	count, err = coll.Find(`{"status":"inactive"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, count)

	assertIndexLen(t, coll.GetIndexes()[0], 20)
}

// --- Category 25: Multi-Index Drop One, Recreate ---

// TestIndex_Corruption_MultiIndexDropOneRecreate verifies that dropping and
// recreating one index doesn't affect another index on the same collection.
func TestIndex_Corruption_MultiIndexDropOneRecreate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test_multi_drop")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 40 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%8, i%5),
		)))
	}

	require.Len(t, coll.GetIndexes(), 2)

	countA, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, countA)

	countB, err := coll.Find(`{"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 8, countB)

	// Drop only "a" index
	require.NoError(t, coll.DropIndex(ctx, "a"))
	require.Len(t, coll.GetIndexes(), 1)
	assert.Equal(t, "b", coll.GetIndexes()[0].Info().Name)

	// "b" index should still work
	countB2, err := coll.Find(`{"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countB, countB2)

	// Recreate "a"
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.Len(t, coll.GetIndexes(), 2)

	countA2, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countA, countA2)

	countB3, err := coll.Find(`{"b":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, countB, countB3)
}

// injectRawIndexEntry writes (key, value) directly into the index
// namespace, bypassing insertKeys. Used to simulate legacy nil-value
// entries (or any other raw entry shape) for backward-compat tests.
//
// Mirrors the pattern used in index_corruption_test.go (search for
// "tx.Put(idx.ns") — same internals access via collection.indexes +
// db.doWriteTx.
func injectRawIndexEntry(t *testing.T, c *collection, idx *index, fullKey, value []byte) {
	t.Helper()
	require.NoError(t, c.db.doWriteTx(ctx, func(tx *btree.WriteTx) error {
		return tx.Put(idx.ns, fullKey, value)
	}))
}

// buildIndexFullKey returns a Tuple-encoded (idxFieldVal..., docId) key
// suitable for direct injection into a non-unique index namespace. This
// matches what insertKeys builds (key + idKey concatenation).
func buildIndexFullKey(idxFieldValues []any, docId any) []byte {
	idxKey := anyenc.Tuple(nil)
	for _, v := range idxFieldValues {
		idxKey = anyenc.AppendAnyValue(idxKey, v)
	}
	idKey := anyenc.Tuple(nil)
	idKey = anyenc.AppendAnyValue(idKey, docId)
	full := append(anyenc.Tuple(nil), idxKey...)
	full = append(full, idKey...)
	return full
}

// findIndex returns the named index pointer from a collection. Test-only
// helper that mirrors the access pattern in readRawIndexEntries.
func findIndex(t *testing.T, coll Collection, indexName string) (*collection, *index) {
	t.Helper()
	c := coll.(*collection)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, i := range c.loadIndexes() {
		if i.info.Name == indexName {
			return c, i
		}
	}
	t.Fatalf("index %q not found", indexName)
	return nil, nil
}

// --- Subtest 1: pure decoder pin ---

// TestAudit14_LegacyNilValue_DecodeMultiKey pins the decoder behavior
// of qplanner.EntryValueIsMultiKey for all four shapes of input:
//
//   - nil           → true  (legacy, conservative)
//   - []byte{}      → true  (legacy, conservative)
//   - []byte{0x00}  → false (new format, scalar)
//   - []byte{0x01}  → true  (new format, multi-key)
//
// If this contract changes, the entire backward-compat story breaks:
// queries on partially-migrated indexes would either over-count (miss
// dedup on legacy entries) or skip valid scalar entries.
func TestAudit14_LegacyNilValue_DecodeMultiKey(t *testing.T) {
	assert.True(t, qplanner.EntryValueIsMultiKey(nil),
		"nil value must be reported as multi-key (legacy/safe)")
	assert.True(t, qplanner.EntryValueIsMultiKey([]byte{}),
		"empty value must be reported as multi-key (legacy/safe)")
	assert.False(t, qplanner.EntryValueIsMultiKey([]byte{0x00}),
		"0x00 (IndexValueScalar) must be reported as scalar (not multi-key)")
	assert.True(t, qplanner.EntryValueIsMultiKey([]byte{0x01}),
		"0x01 (IndexValueMultiKey) must be reported as multi-key")

	// Round-trip via the public sentinels too — guards against drift in
	// the sentinel byte values themselves.
	assert.False(t, qplanner.EntryValueIsMultiKey(qplanner.IndexValueScalar),
		"qplanner.IndexValueScalar sentinel must decode as scalar")
	assert.True(t, qplanner.EntryValueIsMultiKey(qplanner.IndexValueMultiKey),
		"qplanner.IndexValueMultiKey sentinel must decode as multi-key")
}

// --- Subtest 2: real doc + injected legacy entry, query stays correct ---

// TestAudit14_LegacyNilValue_QueryWithLegacyMix simulates a
// partially-migrated index. We insert a real array doc via the public
// API (gets new bit-set entries), then directly inject one nil-value
// entry simulating a leftover legacy row pointing at the SAME doc.
//
// A query Find({tags:"a"}).Count must still return 1: the legacy entry
// (decoded as multi-key) triggers conservative dedup, so the doc is not
// over-counted across the matching bound.
func TestAudit14_LegacyNilValue_QueryWithLegacyMix(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_mix")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert one doc with a 2-element array. Per AUDIT01 this produces
	// 3 raw entries (2 element entries + 1 whole-array entry), each
	// tagged IndexValueMultiKey (0x01).
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Inject a nil-value entry simulating a leftover legacy row for the
	// same doc on a different key value ("c"). This is what a pre-bit
	// version of the writer would have produced for a single insert.
	legacyKey := buildIndexFullKey([]any{"c"}, "d1")
	injectRawIndexEntry(t, c, idx, legacyKey, nil)

	// Index now has 4 entries: 3 bit-set (0x01) for "a"/"b" + whole-array
	// + 1 nil-value (legacy) for "c".
	entries := readRawIndexEntries(t, fx.DB, "audit14_mix", "tags")
	require.Len(t, entries, 4,
		"expected 3 new + 1 injected legacy entry, got %d", len(entries))

	// Sanity: confirm at least one legacy (len==0) and one bit-set entry
	// are present, exercising EntryValueIsMultiKey across both shapes.
	var legacyCount, bitSetCount int
	for _, e := range entries {
		if len(e.Value) == 0 {
			legacyCount++
		} else {
			bitSetCount++
		}
	}
	assert.Equal(t, 1, legacyCount, "exactly one nil-value (legacy) entry expected")
	assert.Equal(t, 3, bitSetCount, "three bit-set entries expected from the public-API insert")

	// Query must still return 1 — the legacy entry on "c" doesn't match
	// {"tags":"a"}, so this verifies the matching-bound case stays
	// correct in the presence of legacy data on other keys.
	n, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"Find({tags:a}).Count must be 1 (legacy entries elsewhere don't affect this match)")

	// And cross-bound: Find({tags:{$in:["a","b"]}}) hits BOTH "a" and "b"
	// bounds for d1. Without dedup the count would be 2; with dedup
	// (which all entries route through because at least one is decoded
	// as multi-key), it must be 1.
	n, err = coll.Find(`{"tags":{"$in":["a","b"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"$in over [a,b] on a single doc whose tags overlap both bounds must "+
			"dedup to 1 (got %d) — legacy entries must not break dedup", n)
}

// --- Subtest 3: update via public API rewrites legacy → bit-set ---

// TestAudit14_LegacyNilValue_OverwriteOnUpdate verifies that an update
// via the public API REPLACES a legacy nil-value entry with a new
// bit-set value. The mechanism: collection.update calls deleteKeys
// (removes by key — value-byte irrelevant) followed by insertKeys
// (writes the new bit-set value).
//
// We must change the doc value (not just rewrite identical content) —
// collection.update has an early-out via anyencutil.Equal that skips
// the deleteKeys/insertKeys cycle when the old and new values are
// identical. So we insert {a:10}, inject a legacy entry at the same
// key, then update to {a:20}: deleteKeys removes the old key (10,d1)
// regardless of its value byte, and insertKeys writes (20,d1) with the
// new bit-set value.
//
// This proves there's a natural migration path: any doc that gets
// touched (with a real value change) will have its index entries
// normalized to the new format.
func TestAudit14_LegacyNilValue_OverwriteOnUpdate(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_update")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_a",
		Fields: []string{"a"},
	}))

	// Insert a scalar doc — produces one entry tagged IndexValueScalar.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","a":10}`),
	))

	c, idx := findIndex(t, coll, "ix_a")

	// Replace that bit-set entry with a nil-value one to simulate a
	// pre-upgrade write for the same (key, docId) pair. Direct overwrite
	// at the same key is safe — it just changes the value.
	legacyKey := buildIndexFullKey([]any{10}, "d1")
	injectRawIndexEntry(t, c, idx, legacyKey, nil)

	// Confirm the entry is now legacy (nil value).
	entries := readRawIndexEntries(t, fx.DB, "audit14_update", "ix_a")
	require.Len(t, entries, 1, "exactly one entry expected")
	assert.Empty(t, entries[0].Value,
		"after injection the entry value must be empty (legacy)")

	// Update the doc via the public API. We MUST change the value (10→20)
	// so collection.update doesn't early-out on the equality check. The
	// deleteKeys call removes the old (10,d1) entry by key (value byte
	// irrelevant), and insertKeys writes (20,d1) with the new bit-set
	// value.
	require.NoError(t, coll.UpdateOne(ctx,
		anyenc.MustParseJson(`{"id":"d1","a":20}`)))

	entries = readRawIndexEntries(t, fx.DB, "audit14_update", "ix_a")
	require.Len(t, entries, 1, "still exactly one entry after update")
	assert.Equal(t, qplanner.IndexValueScalar, entries[0].Value,
		"after public-API update the legacy nil entry must be replaced "+
			"with IndexValueScalar (0x00) — this is the migration path")

	// Query still works for the new value.
	n, err := coll.Find(`{"a":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	// And the old key is gone (no orphaned entry).
	n, err = coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n,
		"after update the old key (a:10) must produce 0 hits — the legacy "+
			"entry there must have been deleted by deleteKeys")
}

// --- Subtest 4: fully-pre-upgrade index, multi-bound $in still correct ---

// TestAudit14_LegacyNilValue_AllNilSimulatesPreUpgrade simulates an
// index where EVERY entry was written by old code (nil values). We
// inject several nil-value entries directly (no public-API inserts on
// this index), then run a multi-bound $in query.
//
// Because every entry decodes as multi-key (len==0), the consumer-side
// DocDedup must collapse cross-bound duplicates. Without that, an $in
// query with overlapping bounds would over-count.
//
// We use the trick of inserting docs into the data namespace via a
// SECOND collection that doesn't share the indexed field, then we hand-
// build the index entries. Simpler approach: insert minimal docs (with
// no array), then inject ONLY legacy index entries for them.
func TestAudit14_LegacyNilValue_AllNilSimulatesPreUpgrade(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_allnil")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert docs with NO "tags" field. The public-API insert path will
	// not write any index entries for them (sparse-like behavior on a
	// missing field path). Then we'll inject legacy entries by hand.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1"}`),
		anyenc.MustParseJson(`{"id":"d2"}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Sanity: no entries from the public-API path (the docs don't have
	// "tags", and the index is non-sparse but the field is missing —
	// writeValues calls Get which returns a typeNull-ish value; the
	// actual behavior depends on writeValues, but to be robust we verify
	// the count rather than asserting zero).
	pre := readRawIndexEntries(t, fx.DB, "audit14_allnil", "tags")
	preLen := len(pre)

	// Inject legacy entries: d1.tags = ["a","b"] and d2.tags = ["b","c"]
	// — i.e. d1 matches {a,b}, d2 matches {b,c}, both overlap on "b".
	// This reproduces the layout a pre-value-byte writer produced: per-element
	// keys PLUS the canonical whole-array key, all with nil value bytes.
	// writeValues has emitted the canonical (0x06-prefixed) whole-array key
	// since before the value byte existed, so real legacy multi-key data
	// always carries it; the count path's canonical-key probe detects it by
	// key prefix regardless of the (nil) value byte.
	for _, kv := range []struct {
		key string
		doc string
	}{
		{"a", "d1"},
		{"b", "d1"},
		{"b", "d2"},
		{"c", "d2"},
	} {
		full := buildIndexFullKey([]any{kv.key}, kv.doc)
		injectRawIndexEntry(t, c, idx, full, nil)
	}
	// Canonical whole-array keys (0x06 prefix), nil value — built via the
	// array Value's MarshalTo since AppendAnyValue handles scalars only.
	for _, kv := range []struct {
		arr string
		doc string
	}{
		{`["a","b"]`, "d1"},
		{`["b","c"]`, "d2"},
	} {
		canonical := append(anyenc.MustParseJson(kv.arr).MarshalTo(nil),
			anyenc.AppendAnyValue(nil, kv.doc)...)
		injectRawIndexEntry(t, c, idx, canonical, nil)
	}

	post := readRawIndexEntries(t, fx.DB, "audit14_allnil", "tags")
	require.Equal(t, preLen+6, len(post),
		"expected %d (pre) + 4 per-element + 2 canonical entries, got %d", preLen, len(post))
	for _, e := range post[preLen:] {
		assert.Empty(t, e.Value,
			"all injected entries must have empty (legacy) value")
	}

	// Multi-bound $in over [a,b,c]: d1 matches "a" and "b" (2 hits), d2
	// matches "b" and "c" (2 hits). Without dedup, count = 4. The canonical
	// 0x06 keys make the count path's probe report multi-key, routing to the
	// sort-dedup count, so the distinct-doc count is 2.
	n, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n,
		"distinct doc count over legacy index must be 2 (d1+d2). "+
			"4 would mean dedup is broken on the legacy path; got %d", n)
}

// --- Subtest 5: rebuild via DropIndex + EnsureIndex normalizes everything ---

// TestAudit14_LegacyNilValue_RebuildNormalizes injects a mix of legacy
// (nil) and new (bit-set) entries, then drops and recreates the index.
// The rebuild must walk the data namespace and rewrite EVERY entry with
// the new bit-set format — no nil values must remain.
//
// This is the explicit migration knob: DropIndex + EnsureIndex has
// always been the documented index-rebuild path; we pin that it also
// upgrades the entry-value-byte format.
func TestAudit14_LegacyNilValue_RebuildNormalizes(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_rebuild")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert two real docs via the public API → bit-set entries.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d2","tags":["c"]}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Now overwrite a couple of those entries with nil values to
	// simulate leftover legacy data co-existing with new data.
	overwrite := buildIndexFullKey([]any{"a"}, "d1")
	injectRawIndexEntry(t, c, idx, overwrite, nil)
	overwrite2 := buildIndexFullKey([]any{"c"}, "d2")
	injectRawIndexEntry(t, c, idx, overwrite2, nil)

	pre := readRawIndexEntries(t, fx.DB, "audit14_rebuild", "tags")
	var preLegacy, preBitSet int
	for _, e := range pre {
		if len(e.Value) == 0 {
			preLegacy++
		} else {
			preBitSet++
		}
	}
	require.GreaterOrEqual(t, preLegacy, 2,
		"setup expected at least 2 legacy entries before rebuild")
	require.GreaterOrEqual(t, preBitSet, 1,
		"setup expected at least 1 bit-set entry before rebuild")

	// Drop + EnsureIndex — the documented rebuild path.
	require.NoError(t, coll.DropIndex(ctx, "tags"))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Every entry must now be bit-set. No nil values may remain.
	post := readRawIndexEntries(t, fx.DB, "audit14_rebuild", "tags")
	require.NotEmpty(t, post, "rebuild must produce some entries")
	for i, e := range post {
		require.NotEmptyf(t, e.Value,
			"entry %d after rebuild must NOT have empty value (got %d entries total)",
			i, len(post))
	}

	// Sanity: queries still work.
	n, err := coll.Find(`{"tags":"a"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// --- Subtest 6: planner routes via SeenSet/DocDedup on legacy index ---

// TestAudit14_LegacyNilValue_PlannerRoutesViaSeenSet pins the end-to-end
// behavior: when the index is fully legacy (nil values), a multi-bound
// $in query with OVERLAPPING bounds against the same doc must still
// produce the correct distinct-doc count via Count AND yield each doc
// exactly once via Iter.
//
// The mechanism: every legacy entry decodes as multi-key (true), so
// DocDedup at the consumer side collapses repeated docIds across
// bounds. Without that — if EntryValueIsMultiKey reported false on
// nil — both Count and Iter would over-count.
//
// Setup: insert d1 WITH tags = [a,b,c] (so the data namespace has a
// real, filter-matching doc). Then overwrite EVERY bit-set entry in
// the index with a nil value, simulating an index that was written by
// the old code but whose data was somehow up to date — i.e. a fully
// pre-upgrade index that hasn't yet been touched (no inserts/updates
// migrate it; no EnsureIndex rebuild).
func TestAudit14_LegacyNilValue_PlannerRoutesViaSeenSet(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit14_seenset")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "tags",
		Fields: []string{"tags"},
	}))

	// Insert a real doc with the array — the public-API path writes
	// bit-set entries for each key (3 element entries + 1 whole-array).
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`),
	))

	c, idx := findIndex(t, coll, "tags")

	// Now overwrite EVERY entry's value with nil to simulate a fully
	// pre-upgrade index. The keys stay the same (so they still point at
	// d1 correctly); only the value byte changes from 0x01 → empty.
	pre := readRawIndexEntries(t, fx.DB, "audit14_seenset", "tags")
	require.NotEmpty(t, pre, "public-API insert must produce some entries")
	for _, e := range pre {
		injectRawIndexEntry(t, c, idx, e.Key, nil)
	}

	// Verify all entries are now legacy (nil values).
	post := readRawIndexEntries(t, fx.DB, "audit14_seenset", "tags")
	require.Equal(t, len(pre), len(post),
		"overwrite must keep entry count constant (same keys, different values)")
	for i, e := range post {
		require.Emptyf(t, e.Value,
			"entry %d after overwrite must have empty (legacy) value, got %v",
			i, e.Value)
	}

	// Count must dedup → 1. Without dedup (if nil decoded as scalar
	// false) the count loop would tally each matching bound separately
	// and report >1.
	n, err := coll.Find(`{"tags":{"$in":["a","b"]}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n,
		"distinct-doc Count over fully-legacy index with overlapping $in "+
			"bounds must be 1 (got %d) — legacy path must route via DocDedup", n)

	// Iter must yield d1 exactly once (in any order). Same dedup logic,
	// different code path (planIterator.Next + DocDedup vs. Count's loop).
	ids := collectField(t, coll.Find(`{"tags":{"$in":["a","b"]}}`), "id")
	sort.Strings(ids)
	assert.Equal(t, []string{`"d1"`}, ids,
		"Iter over fully-legacy index with overlapping $in bounds must yield "+
			"d1 exactly once (got %v)", ids)
}

// ── Corruption-recovery audit (act-38..42) ──
/*
Audit tests for the "corruption-recovery" domain (any-store-tests:docs/any-store/qplanner/audit/actionable_by_domain.json).
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
			`{"id":`+strconv.Itoa(i)+`,"a":`+strconv.Itoa(i*10)+`}`)))
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
//
// Updated for the index-staleness fix: a same-name EnsureIndex with a CHANGED
// definition is no longer a silent no-op. registerIndex now compares the
// persisted definition against the request and returns ErrIndexMismatch when
// they differ (fields / unique / sparse) — surfaced even under ensure=true — so
// a redefinition is never silently dropped. The upgrade path remains explicit:
// drop, then create with the new definition.
func TestIndex_EnsureIndex_SameNameChangedDefinitionErrors(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":5}`),
		anyenc.MustParseJson(`{"id":2,"a":5}`),
	))

	// Index name == join(Fields), so a changed Unique flag does NOT change the
	// name. The request collides with the existing "a" index but carries a
	// different definition, so EnsureIndex returns ErrIndexMismatch (rather than
	// silently keeping the old non-unique definition) and the index is unchanged.
	err = coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}, Unique: true})
	require.ErrorIs(t, err, ErrIndexMismatch)
	assert.False(t, coll.GetIndexes()[0].Info().Unique)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":3,"a":5}`))) // still non-unique

	// An identical EnsureIndex (same definition) remains an idempotent no-op.
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	assert.False(t, coll.GetIndexes()[0].Info().Unique)

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

