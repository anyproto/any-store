package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRebalanceEmptyPageRemoval(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert enough keys to create multi-level tree
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 500 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Delete all keys from a specific range to empty leaf pages
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	for i := range 500 {
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, tx2.Delete(ns3, k))
	}
	require.NoError(t, tx2.Commit())

	// Tree should still be consistent (root page exists)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns4)
	require.NoError(t, cur.First())
	assert.False(t, cur.Valid()) // all deleted
	require.NoError(t, rtx.Rollback())

	// Freed pages should be in freelist
	assert.True(t, db.pager.header.TotalFreelistPgs > 0)
	_ = ns
}

func TestRebalanceRootCollapse(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert keys to create a multi-level tree
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 200 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Delete most keys leaving only a few
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	for i := range 200 {
		if i == 100 {
			continue // keep one key
		}
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, tx2.Delete(ns3, k))
	}
	require.NoError(t, tx2.Commit())

	// Verify the remaining key is accessible
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")
	v, err := rtx.Get(ns4, []byte("key-0100"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0100"), v)

	// Verify cursor works
	cur := rtx.NewCursor(ns4)
	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	assert.Equal(t, 1, count)
	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestRebalanceStress(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert 10K keys
	n := 10000
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range n {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Delete 80% of keys
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	deleteCount := 0
	for i := range n {
		if i%5 == 0 {
			continue // keep every 5th key
		}
		k := fmt.Appendf(nil, "key-%06d", i)
		require.NoError(t, tx2.Delete(ns3, k))
		deleteCount++
	}
	require.NoError(t, tx2.Commit())

	// Verify remaining keys are correct
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns4)

	remaining := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		v, verr := cur.Value()
		require.NoError(t, verr)

		// Verify key format
		assert.True(t, len(k) > 0)
		assert.True(t, len(v) > 0)
		remaining++
	}
	expected := n - deleteCount
	assert.Equal(t, expected, remaining, "expected %d keys, got %d", expected, remaining)

	// Verify random access still works
	for i := 0; i < n; i += 5 {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		got, err := rtx.Get(ns4, k)
		require.NoError(t, err, "key %s not found", k)
		assert.Equal(t, v, got)
	}
	require.NoError(t, rtx.Rollback())

	// Note: with underfull merge disabled, sparse pages are not freed
	// Only completely empty pages get freed (which may or may not happen
	// depending on key distribution across pages)
	_ = ns
}

func TestRebalanceDeleteAndReinsert(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert, delete, reinsert cycle
	for round := range 3 {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns2, _ := db.getNamespaceLocked("data")
		for i := range 100 {
			k := fmt.Appendf(nil, "r%d-key-%04d", round, i)
			v := fmt.Appendf(nil, "r%d-val-%04d", round, i)
			require.NoError(t, tx.Put(ns2, k, v))
		}
		require.NoError(t, tx.Commit())

		// Delete all keys from this round
		tx2, err := db.BeginWrite()
		require.NoError(t, err)
		ns3, _ := db.getNamespaceLocked("data")
		for i := range 100 {
			k := fmt.Appendf(nil, "r%d-key-%04d", round, i)
			require.NoError(t, tx2.Delete(ns3, k))
		}
		require.NoError(t, tx2.Commit())
	}

	// Insert final batch and verify
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 50 {
		k := fmt.Appendf(nil, "final-%04d", i)
		v := fmt.Appendf(nil, "final-val-%04d", i)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	for i := range 50 {
		k := fmt.Appendf(nil, "final-%04d", i)
		v := fmt.Appendf(nil, "final-val-%04d", i)
		got, err := rtx.Get(ns3, k)
		require.NoError(t, err)
		assert.Equal(t, v, got)
	}
	require.NoError(t, rtx.Rollback())
	_ = ns
}
