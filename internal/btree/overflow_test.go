package btree

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOverflowPutGet(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Create a 10KB value (much larger than maxLocal ~1001 bytes for 4KB pages)
	bigValue := bytes.Repeat([]byte("ABCDEFGHIJ"), 1024) // 10KB

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("bigkey"), bigValue))
	require.NoError(t, tx.Commit())

	// Read it back
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	got, err := rtx.Get(ns3, []byte("bigkey"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestOverflowUpdateLargeToSmall(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	bigValue := bytes.Repeat([]byte("X"), 5000)
	smallValue := []byte("tiny")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	// Insert large value
	require.NoError(t, tx.Put(ns2, []byte("key"), bigValue))

	// Update to small value (should free overflow pages)
	require.NoError(t, tx.Put(ns2, []byte("key"), smallValue))

	got, err := tx.Get(ns2, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, smallValue, got)
	require.NoError(t, tx.Commit())
	_ = ns
}

func TestOverflowUpdateSmallToLarge(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	smallValue := []byte("tiny")
	bigValue := bytes.Repeat([]byte("Y"), 8000)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	require.NoError(t, tx.Put(ns2, []byte("key"), smallValue))
	require.NoError(t, tx.Put(ns2, []byte("key"), bigValue))

	got, err := tx.Get(ns2, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, tx.Commit())
	_ = ns
}

func TestOverflowDelete(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	bigValue := bytes.Repeat([]byte("Z"), 10000)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("bigkey"), bigValue))
	require.NoError(t, tx.Commit())

	// Delete the key (should free overflow pages)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Delete(ns3, []byte("bigkey")))
	require.NoError(t, tx2.Commit())

	// Verify it's deleted
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")
	_, err = rtx.Get(ns4, []byte("bigkey"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, rtx.Rollback())

	// Verify overflow pages were freed (freelist should have entries)
	assert.True(t, db.pager.header.TotalFreelistPgs > 0)
	_ = ns
}

func TestOverflowCursor(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Insert a mix of normal and overflow values
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	for i := range 20 {
		k := fmt.Appendf(nil, "key-%02d", i)
		var v []byte
		if i%3 == 0 {
			// Large value (overflow)
			v = bytes.Repeat([]byte(fmt.Sprintf("%02d", i)), 3000)
		} else {
			// Small value (no overflow)
			v = fmt.Appendf(nil, "val-%02d", i)
		}
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Iterate with cursor and verify all values
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns3)

	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		v, verr := cur.Value()
		require.NoError(t, verr)

		i := count
		expectedKey := fmt.Appendf(nil, "key-%02d", i)
		assert.Equal(t, expectedKey, k)

		if i%3 == 0 {
			expectedVal := bytes.Repeat([]byte(fmt.Sprintf("%02d", i)), 3000)
			assert.Equal(t, expectedVal, v, "overflow value mismatch for key %s", k)
		} else {
			expectedVal := fmt.Appendf(nil, "val-%02d", i)
			assert.Equal(t, expectedVal, v)
		}
		count++
	}
	assert.Equal(t, 20, count)
	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestOverflowPersistence(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	bigValue := bytes.Repeat([]byte("PERSIST"), 2000) // 14KB

	// Write and checkpoint
	db, err := Open(path, DefaultOptions())
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("bigkey"), bigValue))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint())
	require.NoError(t, db.Close())

	// Reopen and verify
	db2, err := Open(path, DefaultOptions())
	require.NoError(t, err)
	defer db2.Close()
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns2, err := db2.getNamespaceLocked("data")
	require.NoError(t, err)
	got, err := rtx.Get(ns2, []byte("bigkey"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, rtx.Rollback())
}

func TestOverflow1MB(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// 1MB value
	bigValue := bytes.Repeat([]byte("MEGABYTE"), 131072) // 1MB

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("1mb"), bigValue))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	got, err := rtx.Get(ns3, []byte("1mb"))
	require.NoError(t, err)
	assert.Equal(t, bigValue, got)
	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestOverflowMultipleKeys(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")

	// Insert 10 overflow keys
	for i := range 10 {
		k := fmt.Appendf(nil, "big-%02d", i)
		v := bytes.Repeat([]byte(fmt.Sprintf("%02d-", i)), 2000)
		require.NoError(t, tx.Put(ns2, k, v))
	}
	require.NoError(t, tx.Commit())

	// Read all back
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	for i := range 10 {
		k := fmt.Appendf(nil, "big-%02d", i)
		expected := bytes.Repeat([]byte(fmt.Sprintf("%02d-", i)), 2000)
		got, err := rtx.Get(ns3, k)
		require.NoError(t, err)
		assert.Equal(t, expected, got, "mismatch for key big-%02d", i)
	}
	require.NoError(t, rtx.Rollback())
	_ = ns
}

func TestOverflowNamespaceDelete(t *testing.T) {
	db := tempDB(t)

	// Create namespace with overflow values
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("overflow")
	require.NoError(t, err)
	for i := range 5 {
		k := fmt.Appendf(nil, "key-%d", i)
		v := bytes.Repeat([]byte("V"), 5000)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	// Delete namespace (should free all pages including overflow)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.DeleteNamespace("overflow"))
	require.NoError(t, tx2.Commit())

	assert.True(t, db.pager.header.TotalFreelistPgs > 0)
}
