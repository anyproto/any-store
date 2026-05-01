package btree

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tempDB(t *testing.T) *DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func tempDBWithNS(t *testing.T, nsName string) (*DB, *Namespace) {
	t.Helper()
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace(nsName)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	return db, ns
}

// === Open/Close Tests ===

func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	require.NoError(t, db.Close())
	_, err = os.Stat(path)
	require.NoError(t, err)
}

func TestOpenInvalidPageSize(t *testing.T) {
	dir := t.TempDir()
	_, err := testOpen(t, filepath.Join(dir, "t.db"), Options{PageSize: 100})
	assert.Error(t, err)
	_, err = testOpen(t, filepath.Join(dir, "t.db"), Options{PageSize: 3000})
	assert.Error(t, err)
}

func TestOpenDefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	assert.Equal(t, uint32(DefaultPageSize), opts.PageSize)
	assert.Equal(t, defaultCacheSize, opts.CacheSize)
}

func TestCloseDouble(t *testing.T) {
	db := tempDB(t)
	require.NoError(t, db.Close())
	assert.ErrorIs(t, db.Close(), ErrClosed)
}

func TestDBPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	assert.Equal(t, path, db.Path())
	db.Close()
}

// === Reopen / Persistence Tests ===

func TestReopenDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("items")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("key1"), []byte("val1")))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	db2, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db2.Close()
	rtx, err := db2.BeginRead()
	require.NoError(t, err)
	ns2, err := db2.getNamespaceLocked("items")
	require.NoError(t, err)
	val, err := rtx.Get(ns2, []byte("key1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val1"), val)
	require.NoError(t, rtx.Rollback())
}

func TestReopenMultipleNamespaces(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns1, err := tx.CreateNamespace("a")
	require.NoError(t, err)
	ns2, err := tx.CreateNamespace("b")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns1, []byte("k1"), []byte("v1")))
	require.NoError(t, tx.Put(ns2, []byte("k2"), []byte("v2")))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	db2, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer db2.Close()
	names, err := db2.ListNamespaces()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, names)
}

// === Namespace Tests ===

func TestNamespaces(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns1, err := tx.CreateNamespace("ns1")
	require.NoError(t, err)
	ns2, err := tx.CreateNamespace("ns2")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns1, []byte("a"), []byte("1")))
	require.NoError(t, tx.Put(ns2, []byte("b"), []byte("2")))
	require.NoError(t, tx.Commit())
	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"ns1", "ns2"}, names)
}

func TestCreateManyNamespaces(t *testing.T) {
	db := tempDB(t)
	// Creating >233 namespaces triggers a page 1 split in the master table btree.
	// This verifies namespace operations work when page 1 is an interior node.
	const count = 300
	for i := range count {
		name := fmt.Sprintf("ns-%04d", i)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = tx.CreateNamespace(name)
		require.NoError(t, err, "namespace %d (%s)", i, name)
		require.NoError(t, tx.Commit())
	}
	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.Len(t, names, count)

	// Verify all namespaces are retrievable
	for i := range count {
		name := fmt.Sprintf("ns-%04d", i)
		ns, err := db.GetNamespace(name)
		require.NoError(t, err, "GetNamespace %s", name)
		assert.Equal(t, name, ns.Name())
	}
}

func TestNamespaceDuplicate(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("dup")
	require.NoError(t, err)
	_, err = tx.CreateNamespace("dup")
	assert.ErrorIs(t, err, ErrNamespaceExists)
	require.NoError(t, tx.Commit())
}

func TestNamespaceNotFound(t *testing.T) {
	db := tempDB(t)
	_, err := db.GetNamespace("nonexistent")
	assert.ErrorIs(t, err, ErrNamespaceNotFound)
}

func TestNamespaceGetAfterCreate(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("test")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	ns, err := db.GetNamespace("test")
	require.NoError(t, err)
	assert.Equal(t, "test", ns.Name())
	assert.NotEqual(t, uint32(0), ns.RootPage())
}

func TestDeleteNamespace(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("temp")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.DeleteNamespace("temp"))
	require.NoError(t, tx2.Commit())

	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestDeleteNamespaceNotFound(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	assert.ErrorIs(t, tx.DeleteNamespace("missing"), ErrNamespaceNotFound)
	require.NoError(t, tx.Rollback())
}

func TestListNamespacesEmpty(t *testing.T) {
	db := tempDB(t)
	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestNamespaceIsolation(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns1, err := tx.CreateNamespace("a")
	require.NoError(t, err)
	ns2, err := tx.CreateNamespace("b")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns1, []byte("key"), []byte("val-a")))
	require.NoError(t, tx.Put(ns2, []byte("key"), []byte("val-b")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns1r, _ := db.getNamespaceLocked("a")
	ns2r, _ := db.getNamespaceLocked("b")
	v1, err := rtx.Get(ns1r, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-a"), v1)
	v2, err := rtx.Get(ns2r, []byte("key"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-b"), v2)
	require.NoError(t, rtx.Rollback())
}

// === Put/Get/Delete Tests ===

func TestPutGetDelete(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("hello"), []byte("world")))
	val, err := tx.Get(ns, []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), val)
	require.NoError(t, tx.Delete(ns, []byte("hello")))
	_, err = tx.Get(ns, []byte("hello"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, tx.Commit())
}

func TestUpdate(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v1")))
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v2")))
	val, err := tx.Get(ns, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), val)
	require.NoError(t, tx.Commit())
}

func TestDeleteNonExistent(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	err = tx.Delete(ns, []byte("nonexistent"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, tx.Rollback())
}

func TestGetNonExistent(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	_, err = rtx.Get(ns, []byte("nope"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, rtx.Rollback())
}

func TestHas(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("exists"), []byte("yes")))

	has, err := tx.Has(ns, []byte("exists"))
	require.NoError(t, err)
	assert.True(t, has)

	has, err = tx.Has(ns, []byte("missing"))
	require.NoError(t, err)
	assert.False(t, has)
	require.NoError(t, tx.Commit())
}

// TestReadTx_Has_NoValueAlloc pins the alloc-free guarantee of the
// rewritten Has — a hot-cache existence probe must not allocate. If a
// future change reintroduces a per-call alloc (e.g. wrapping Has in a
// checksum-scratch layer that allocs a buffer), this test fails.
func TestReadTx_Has_NoValueAlloc(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%05d", i)
		v := fmt.Appendf(nil, "val-%05d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, _ := db.getNamespaceLocked("data")

	probe := []byte("key-00050")
	// Warm: load pages into pcache.
	for range 10 {
		_, err := rtx.Has(ns2, probe)
		require.NoError(t, err)
	}

	allocs := testing.AllocsPerRun(100, func() {
		_, _ = rtx.Has(ns2, probe)
	})
	assert.Zero(t, allocs, "tx.Has must not allocate on hot cache")
}

// TestReadTx_Has_NotFound pins (false, nil) for missing keys via ReadTx.
func TestReadTx_Has_NotFound(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("a"), []byte("1")))
	require.NoError(t, wtx.Put(ns, []byte("c"), []byte("3")))
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, _ := db.getNamespaceLocked("data")

	has, err := rtx.Has(ns2, []byte("b"))
	require.NoError(t, err)
	assert.False(t, has)

	has, err = rtx.Has(ns2, []byte("a"))
	require.NoError(t, err)
	assert.True(t, has)
}

// TestReadTx_Has_OverflowKey exercises the overflow-key compare path in
// searchLeafWithOverflow — the same descent helper AppendValue uses for
// overflow keys, now exercised through Has.
func TestReadTx_Has_OverflowKey(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t1")
	require.NoError(t, err)

	bigKey := bytes.Repeat([]byte("k"), 200)
	require.NoError(t, tx.Put(ns, bigKey, make([]byte, 400)))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	has, err := rtx.Has(ns, bigKey)
	require.NoError(t, err)
	assert.True(t, has, "overflow key must be findable via Has")

	// Different long key that doesn't exist.
	otherBig := bytes.Repeat([]byte("z"), 200)
	has, err = rtx.Has(ns, otherBig)
	require.NoError(t, err)
	assert.False(t, has)
}

// TestReadTx_Has_MultiLevelDescent exercises Has on a tree tall enough to
// require interior-page descent (not just a single leaf). Pins that the
// interior loop and page-release pairing are correct.
func TestReadTx_Has_MultiLevelDescent(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	// Enough keys to force at least one split. With 512-byte pages and
	// ~30-byte values, ~50+ keys forces an interior page.
	const n = 200
	for i := range n {
		k := fmt.Appendf(nil, "key-%05d", i)
		v := bytes.Repeat([]byte{byte(i % 253)}, 30)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, _ := db.getNamespaceLocked("data")

	for i := range n {
		k := fmt.Appendf(nil, "key-%05d", i)
		has, err := rtx.Has(ns2, k)
		require.NoError(t, err)
		assert.True(t, has, "key-%05d must exist", i)
	}
	has, err := rtx.Has(ns2, []byte("key-99999"))
	require.NoError(t, err)
	assert.False(t, has)
}

func TestMultipleKeys(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%03d", i)
		v := fmt.Appendf(nil, "val-%03d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%03d", i)
		v := fmt.Appendf(nil, "val-%03d", i)
		got, err := rtx.Get(ns2, k)
		require.NoError(t, err)
		assert.Equal(t, v, got)
	}
	require.NoError(t, rtx.Rollback())
}

func TestLargeDataset(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("big")
	require.NoError(t, err)
	n := 500
	for i := range n {
		k := fmt.Appendf(nil, "key-%05d", i)
		v := fmt.Appendf(nil, "value-%05d-padding-to-make-it-bigger", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("big")
	for i := range n {
		k := fmt.Appendf(nil, "key-%05d", i)
		v := fmt.Appendf(nil, "value-%05d-padding-to-make-it-bigger", i)
		got, err := rtx.Get(ns2, k)
		require.NoError(t, err, "failed to get key-%05d", i)
		assert.Equal(t, v, got)
	}
	require.NoError(t, rtx.Rollback())
}

func TestUpdateMultiple(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%03d", i)
		require.NoError(t, tx.Put(ns, k, []byte("original")))
	}
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%03d", i)
		require.NoError(t, tx.Put(ns, k, []byte("updated")))
	}
	for i := range 50 {
		k := fmt.Appendf(nil, "key-%03d", i)
		v, err := tx.Get(ns, k)
		require.NoError(t, err)
		assert.Equal(t, []byte("updated"), v)
	}
	require.NoError(t, tx.Commit())
}

func TestDeleteAllKeys(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%03d", i)
		require.NoError(t, tx.Put(ns, k, []byte("val")))
	}
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%03d", i)
		require.NoError(t, tx.Delete(ns, k))
	}
	for i := range 20 {
		k := fmt.Appendf(nil, "key-%03d", i)
		has, err := tx.Has(ns, k)
		require.NoError(t, err)
		assert.False(t, has)
	}
	require.NoError(t, tx.Commit())
}

// === Transaction Tests ===

func TestRollback(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns2, []byte("k"), []byte("changed")))
	require.NoError(t, tx2.Rollback())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	val, err := rtx.Get(ns3, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), val)
	require.NoError(t, rtx.Rollback())
}

func TestClosedTxErrors(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	// Write tx
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.ErrorIs(t, tx.Commit(), ErrTxClosed)
	assert.ErrorIs(t, tx.Rollback(), ErrTxClosed)
	assert.ErrorIs(t, tx.Put(ns, []byte("k"), []byte("v")), ErrTxClosed)
	assert.ErrorIs(t, tx.Delete(ns, []byte("k")), ErrTxClosed)
	_, err = tx.Get(ns, []byte("k"))
	assert.ErrorIs(t, err, ErrTxClosed)
	_, err = tx.Has(ns, []byte("k"))
	assert.ErrorIs(t, err, ErrTxClosed)
	_, err = tx.Savepoint()
	assert.ErrorIs(t, err, ErrTxClosed)

	// Read tx
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	require.NoError(t, rtx.Rollback())
	assert.ErrorIs(t, rtx.Rollback(), ErrTxClosed)
	_, err = rtx.Get(ns, []byte("k"))
	assert.ErrorIs(t, err, ErrTxClosed)
}

func TestClosedDBErrors(t *testing.T) {
	db := tempDB(t)
	require.NoError(t, db.Close())

	_, err := db.BeginRead()
	assert.ErrorIs(t, err, ErrClosed)
	_, err = db.BeginWrite()
	assert.ErrorIs(t, err, ErrClosed)
	assert.ErrorIs(t, db.Checkpoint(CheckpointFull), ErrClosed)
}

// === Savepoint Tests ===

func TestSavepoint(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("a"), []byte("1")))
	sp, err := tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("b"), []byte("2")))
	require.NoError(t, tx.RollbackToSavepoint(sp))
	has, err := tx.Has(ns, []byte("a"))
	require.NoError(t, err)
	assert.True(t, has)
	require.NoError(t, tx.Commit())
}

func TestSavepointRelease(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	sp, err := tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("x"), []byte("y")))
	require.NoError(t, tx.ReleaseSavepoint(sp))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	val, err := rtx.Get(ns2, []byte("x"))
	require.NoError(t, err)
	assert.Equal(t, []byte("y"), val)
	require.NoError(t, rtx.Rollback())
}

func TestNestedSavepoints(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)

	require.NoError(t, tx.Put(ns, []byte("a"), []byte("1")))

	sp1, err := tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("b"), []byte("2")))

	sp2, err := tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("c"), []byte("3")))

	// Rollback to sp2 should keep a and b
	require.NoError(t, tx.RollbackToSavepoint(sp2))
	has, _ := tx.Has(ns, []byte("a"))
	assert.True(t, has)
	has, _ = tx.Has(ns, []byte("b"))
	assert.True(t, has)

	// Rollback to sp1 should keep only a
	require.NoError(t, tx.RollbackToSavepoint(sp1))
	has, _ = tx.Has(ns, []byte("a"))
	assert.True(t, has)

	require.NoError(t, tx.Commit())
}

func TestSavepointInvalidID(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	assert.ErrorIs(t, tx.RollbackToSavepoint(-1), ErrInvalidSavepoint)
	assert.ErrorIs(t, tx.RollbackToSavepoint(99), ErrInvalidSavepoint)
	assert.ErrorIs(t, tx.ReleaseSavepoint(-1), ErrInvalidSavepoint)
	assert.ErrorIs(t, tx.ReleaseSavepoint(99), ErrInvalidSavepoint)
	require.NoError(t, tx.Rollback())
}

// === Cursor Tests ===

func TestCursorFirst(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("c"), []byte("3")))
	require.NoError(t, tx.Put(ns, []byte("a"), []byte("1")))
	require.NoError(t, tx.Put(ns, []byte("b"), []byte("2")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.First())
	assert.True(t, cur.Valid())
	k, _ := cur.Key()
	assert.Equal(t, []byte("a"), k)
	v, _ := cur.Value()
	assert.Equal(t, []byte("1"), v)
	require.NoError(t, rtx.Rollback())
}

func TestCursorLast(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("a"), []byte("1")))
	require.NoError(t, tx.Put(ns, []byte("b"), []byte("2")))
	require.NoError(t, tx.Put(ns, []byte("c"), []byte("3")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.Last())
	assert.True(t, cur.Valid())
	k, _ := cur.Key()
	assert.Equal(t, []byte("c"), k)
	require.NoError(t, rtx.Rollback())
}

func TestCursorIteration(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for _, k := range keys {
		require.NoError(t, tx.Put(ns, []byte(k), []byte("v-"+k)))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	var collected []string
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, _ := cur.Key()
		collected = append(collected, string(k))
	}
	assert.Equal(t, keys, collected)
	require.NoError(t, rtx.Rollback())
}

func TestCursorSeek(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := range 10 {
		k := fmt.Appendf(nil, "key-%02d", i*2) // 00, 02, 04, ...
		require.NoError(t, tx.Put(ns, k, []byte("v")))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Seek to existing key
	require.NoError(t, cur.Seek([]byte("key-04")))
	assert.True(t, cur.Valid())
	k, _ := cur.Key()
	assert.Equal(t, []byte("key-04"), k)

	// Seek between keys (should land on next)
	require.NoError(t, cur.Seek([]byte("key-03")))
	assert.True(t, cur.Valid())
	k, _ = cur.Key()
	assert.Equal(t, []byte("key-04"), k)

	// Seek to first
	require.NoError(t, cur.Seek([]byte("key-00")))
	assert.True(t, cur.Valid())
	k, _ = cur.Key()
	assert.Equal(t, []byte("key-00"), k)

	require.NoError(t, rtx.Rollback())
}

func TestCursorEmptyTree(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	require.NoError(t, cur.First())
	assert.False(t, cur.Valid())
	require.NoError(t, cur.Last())
	assert.False(t, cur.Valid())
	_, err = cur.Key()
	assert.ErrorIs(t, err, ErrKeyNotFound)
	_, err = cur.Value()
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, rtx.Rollback())
}

func TestCursorNextPastEnd(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("only"), []byte("one")))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.First())
	assert.True(t, cur.Valid())
	require.NoError(t, cur.Next())
	assert.False(t, cur.Valid())
	require.NoError(t, rtx.Rollback())
}

// === Checkpoint Tests ===

func TestCheckpoint(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("ck")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("a"), []byte("b")))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))
}

func TestCheckpointAndContinue(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("before"), []byte("ckpt")))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.Checkpoint(CheckpointFull))

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx2.Put(ns2, []byte("after"), []byte("ckpt")))
	require.NoError(t, tx2.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	v1, err := rtx.Get(ns3, []byte("before"))
	require.NoError(t, err)
	assert.Equal(t, []byte("ckpt"), v1)
	v2, err := rtx.Get(ns3, []byte("after"))
	require.NoError(t, err)
	assert.Equal(t, []byte("ckpt"), v2)
	require.NoError(t, rtx.Rollback())
}

// === Multi-level B-tree Cursor Tests ===
// These tests insert enough data to force page splits, creating interior pages.

// insertManyKeys inserts n keys (key-0000 .. key-NNNN) into the namespace within a write tx.
func insertManyKeys(t *testing.T, db *DB, ns *Namespace, n int) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := range n {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())
}

func TestCursorMultiLevelFirst(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.First())
	assert.True(t, cur.Valid())
	k, err := cur.Key()
	require.NoError(t, err)
	assert.Equal(t, []byte("key-0000"), k)
	require.NoError(t, rtx.Rollback())
}

func TestCursorMultiLevelLast(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)
	require.NoError(t, cur.Last())
	assert.True(t, cur.Valid())
	k, err := cur.Key()
	require.NoError(t, err)
	assert.Equal(t, []byte("key-0499"), k)
	require.NoError(t, rtx.Rollback())
}

func TestCursorMultiLevelFullIteration(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	n := 500
	insertManyKeys(t, db, ns, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	var keys []string
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		keys = append(keys, string(k))
	}
	assert.Len(t, keys, n)
	// Verify sorted order
	for i := 1; i < len(keys); i++ {
		assert.True(t, keys[i-1] < keys[i], "keys not sorted: %s >= %s", keys[i-1], keys[i])
	}
	assert.Equal(t, "key-0000", keys[0])
	assert.Equal(t, "key-0499", keys[n-1])
	require.NoError(t, rtx.Rollback())
}

func TestCursorMultiLevelSeek(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Seek to exact key in the middle
	require.NoError(t, cur.Seek([]byte("key-0250")))
	assert.True(t, cur.Valid())
	k, _ := cur.Key()
	assert.Equal(t, []byte("key-0250"), k)

	// Seek between keys (should land on next)
	require.NoError(t, cur.Seek([]byte("key-0250x")))
	assert.True(t, cur.Valid())
	k, _ = cur.Key()
	assert.Equal(t, []byte("key-0251"), k)

	// Seek to first key
	require.NoError(t, cur.Seek([]byte("key-0000")))
	assert.True(t, cur.Valid())
	k, _ = cur.Key()
	assert.Equal(t, []byte("key-0000"), k)

	// Seek to last key
	require.NoError(t, cur.Seek([]byte("key-0499")))
	assert.True(t, cur.Valid())
	k, _ = cur.Key()
	assert.Equal(t, []byte("key-0499"), k)

	require.NoError(t, rtx.Rollback())
}

func TestCursorSeekPastAllKeys(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Seek past all keys
	require.NoError(t, cur.Seek([]byte("zzz")))
	assert.False(t, cur.Valid())
	require.NoError(t, rtx.Rollback())
}

func TestCursorSeekBeforeAllKeys(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Seek before all keys should land on first
	require.NoError(t, cur.Seek([]byte("aaa")))
	assert.True(t, cur.Valid())
	k, _ := cur.Key()
	assert.Equal(t, []byte("key-0000"), k)
	require.NoError(t, rtx.Rollback())
}

func TestCursorMultiLevelNextPastEnd(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	require.NoError(t, cur.Last())
	assert.True(t, cur.Valid())
	require.NoError(t, cur.Next())
	assert.False(t, cur.Valid())
	require.NoError(t, rtx.Rollback())
}

func TestCursorMultiLevelSeekAndIterate(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	n := 500
	insertManyKeys(t, db, ns, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Seek to middle then iterate to end
	require.NoError(t, cur.Seek([]byte("key-0400")))
	assert.True(t, cur.Valid())

	var count int
	for cur.Valid() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		expected := fmt.Appendf(nil, "key-%04d", 400+count)
		assert.Equal(t, expected, k)
		count++
		require.NoError(t, cur.Next())
	}
	assert.Equal(t, 100, count)
	require.NoError(t, rtx.Rollback())
}

func TestCursorValueOnMultiLevel(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	require.NoError(t, cur.Seek([]byte("key-0123")))
	assert.True(t, cur.Valid())
	v, err := cur.Value()
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0123"), v)
	require.NoError(t, rtx.Rollback())
}

// === SeekNear / SeekExact Tests ===

func TestCursor_SeekNear(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	n := 500
	insertManyKeys(t, db, ns, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Verify SeekNear matches Seek for every existing key.
	for i := range n {
		k := fmt.Appendf(nil, "key-%04d", i)
		seekCur := rtx.NewCursor(ns2)
		require.NoError(t, seekCur.Seek(k))
		require.NoError(t, cur.SeekNear(k))
		assert.Equal(t, seekCur.Valid(), cur.Valid(), "valid mismatch for key %s", k)
		if cur.Valid() {
			got, _ := cur.Key()
			want, _ := seekCur.Key()
			assert.Equal(t, want, got, "key mismatch for %s", k)
		}
	}

	// Key not found — SeekNear returns first key >= target (gap key).
	require.NoError(t, cur.Seek([]byte("key-0100"))) // position on leaf
	require.NoError(t, cur.SeekNear([]byte("key-0100x")))
	assert.True(t, cur.Valid())
	k, _ := cur.Key()
	assert.Equal(t, []byte("key-0101"), k)

	// Key before all keys in tree — falls back to Seek.
	require.NoError(t, cur.SeekNear([]byte("aaa")))
	assert.True(t, cur.Valid())
	k, _ = cur.Key()
	assert.Equal(t, []byte("key-0000"), k)

	// Key after all keys in tree.
	require.NoError(t, cur.SeekNear([]byte("zzz")))
	assert.False(t, cur.Valid())
}

func TestCursor_SeekNear_EmptyTree(t *testing.T) {
	db, ns := tempDBWithNS(t, "empty")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	cur := rtx.NewCursor(ns)

	require.NoError(t, cur.SeekNear([]byte("anything")))
	assert.False(t, cur.Valid())
}

func TestCursor_SeekExact(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	n := 200
	insertManyKeys(t, db, ns, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Existing keys succeed.
	for i := range n {
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, cur.SeekExact(k), "SeekExact should succeed for %s", k)
		assert.True(t, cur.Valid())
		got, _ := cur.Key()
		assert.Equal(t, k, got)
	}

	// Missing keys return ErrKeyNotFound.
	assert.ErrorIs(t, cur.SeekExact([]byte("key-9999")), ErrKeyNotFound)
	assert.ErrorIs(t, cur.SeekExact([]byte("nonexistent")), ErrKeyNotFound)
	assert.ErrorIs(t, cur.SeekExact([]byte("aaa")), ErrKeyNotFound)
}

func TestCursor_SeekExact_EmptyTree(t *testing.T) {
	db, ns := tempDBWithNS(t, "empty")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	cur := rtx.NewCursor(ns)
	assert.ErrorIs(t, cur.SeekExact([]byte("anything")), ErrKeyNotFound)
}

func TestCursor_SeekNear_SameLeaf(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	n := 500
	insertManyKeys(t, db, ns, n)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns2, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns2)

	// Position cursor on a leaf in the middle of the tree.
	require.NoError(t, cur.Seek([]byte("key-0250")))
	require.True(t, cur.Valid())
	stackLen := len(cur.stack)
	leafPgno := cur.stack[stackLen-1].pgno

	// SeekNear to nearby keys on the same leaf should reuse the pinned page.
	// Try a few keys that are close and likely on the same leaf.
	for delta := -3; delta <= 3; delta++ {
		idx := 250 + delta
		if idx < 0 || idx >= n {
			continue
		}
		k := fmt.Appendf(nil, "key-%04d", idx)
		require.NoError(t, cur.SeekNear(k))
		require.True(t, cur.Valid(), "should be valid for %s", k)
		got, _ := cur.Key()
		assert.Equal(t, k, got)
		// Stack length shouldn't change — still on same leaf.
		if cur.stack[len(cur.stack)-1].pgno == leafPgno {
			assert.Equal(t, stackLen, len(cur.stack), "stack length changed for nearby key %s", k)
		}
	}
}

// === Delete from multi-level tree ===

func TestDeleteFromMultiLevelTree(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	// Delete some keys from interior-backed tree
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%04d", i*5) // delete every 5th
		require.NoError(t, tx.Delete(ns2, k))
	}
	require.NoError(t, tx.Commit())

	// Verify remaining keys
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	cur := rtx.NewCursor(ns3)

	var count int
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	assert.Equal(t, 400, count)
	require.NoError(t, rtx.Rollback())
}

func TestDeleteNonExistentFromMultiLevel(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	err = tx.Delete(ns2, []byte("nonexistent"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, tx.Rollback())
}

// === btree.Get and btree.Has direct tests ===

func TestBtreeGetDirect(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("hello"), []byte("world")))
	require.NoError(t, tx.Commit())

	// Use btree.Get directly (starts its own read tx)
	bt := &btree{pager: db.pager, rootPage: ns.rootPage}
	val, err := bt.Get([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, []byte("world"), val)

	_, err = bt.Get([]byte("missing"))
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestBtreeHasDirect(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	require.NoError(t, tx.Put(ns2, []byte("exists"), []byte("yes")))
	require.NoError(t, tx.Commit())

	bt := &btree{pager: db.pager, rootPage: ns.rootPage}
	exists, err := bt.Has([]byte("exists"))
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = bt.Has([]byte("nope"))
	require.NoError(t, err)
	assert.False(t, exists)
}

func TestBtreeGetFromMultiLevelTree(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	insertManyKeys(t, db, ns, 500)

	// btree.Get traverses interior pages
	bt := &btree{pager: db.pager, rootPage: ns.rootPage}
	val, err := bt.Get([]byte("key-0250"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0250"), val)

	val, err = bt.Get([]byte("key-0000"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0000"), val)

	val, err = bt.Get([]byte("key-0499"))
	require.NoError(t, err)
	assert.Equal(t, []byte("val-0499"), val)
}

// === Cursor on Next with empty stack ===

func TestCursorNextOnInvalid(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	// Next on fresh cursor (not positioned)
	require.NoError(t, cur.Next())
	assert.False(t, cur.Valid())
	require.NoError(t, rtx.Rollback())
}

// === Low-level B-tree function tests ===

// tempPager creates a pager with a write tx for low-level tests.
// Returns pager and cleanup function that properly ends transactions.
func tempPager(t *testing.T) *pager {
	t.Helper()
	resetPageBufferPool()
	p := newPager(filepath.Join(t.TempDir(), "t.db"), 4096, 100, true)
	require.NoError(t, p.open())
	_, slot, err := p.beginRead()
	require.NoError(t, err)
	require.NoError(t, p.beginWrite(WalIndexHdr{}))
	t.Cleanup(func() {
		_ = p.rollback()
		p.endRead(slot)
		_ = p.close()
	})
	return p
}

func TestCollectInteriorCells(t *testing.T) {
	p := tempPager(t)

	pg, err := p.allocatePage()
	require.NoError(t, err)

	bt := &btree{pager: p, rootPage: pg.pgno}

	cells := []cellData{
		{leftChild: 10, key: []byte("bbb")},
		{leftChild: 20, key: []byte("ddd")},
		{leftChild: 30, key: []byte("fff")},
	}
	bt.rebuildInteriorPage(pg, cells, 40)
	p.releasePage(pg)

	pg2, err := p.getPage(pg.pgno)
	require.NoError(t, err)
	got := bt.collectInteriorCells(pg2)
	p.releasePage(pg2)

	require.Len(t, got, 3)
	assert.Equal(t, uint32(10), got[0].leftChild)
	assert.Equal(t, []byte("bbb"), got[0].key)
	assert.Equal(t, uint32(20), got[1].leftChild)
	assert.Equal(t, []byte("ddd"), got[1].key)
	assert.Equal(t, uint32(30), got[2].leftChild)
	assert.Equal(t, []byte("fff"), got[2].key)
}

func TestSearchInteriorPageAllGreater(t *testing.T) {
	p := tempPager(t)

	pg, err := p.allocatePage()
	require.NoError(t, err)

	bt := &btree{pager: p, rootPage: pg.pgno}
	cells := []cellData{
		{leftChild: 10, key: []byte("mmm")},
		{leftChild: 20, key: []byte("zzz")},
	}
	bt.rebuildInteriorPage(pg, cells, 30)

	child, idx, serr := searchInteriorPage(pg, []byte("aaa"))
	assert.NoError(t, serr)
	assert.Equal(t, uint32(10), child)
	assert.Equal(t, 0, idx)

	p.releasePage(pg)
}

func TestSearchInteriorPageBetweenKeys(t *testing.T) {
	p := tempPager(t)

	pg, err := p.allocatePage()
	require.NoError(t, err)

	bt := &btree{pager: p, rootPage: pg.pgno}
	cells := []cellData{
		{leftChild: 10, key: []byte("bbb")},
		{leftChild: 20, key: []byte("ddd")},
		{leftChild: 30, key: []byte("fff")},
	}
	bt.rebuildInteriorPage(pg, cells, 40)

	child, idx, serr := searchInteriorPage(pg, []byte("ccc"))
	assert.NoError(t, serr)
	assert.Equal(t, uint32(20), child)
	assert.Equal(t, 1, idx)

	child, idx, serr = searchInteriorPage(pg, []byte("eee"))
	assert.NoError(t, serr)
	assert.Equal(t, uint32(30), child)
	assert.Equal(t, 2, idx)

	child, idx, serr = searchInteriorPage(pg, []byte("ggg"))
	assert.NoError(t, serr)
	assert.Equal(t, uint32(40), child)
	assert.Equal(t, 3, idx)

	child, idx, serr = searchInteriorPage(pg, []byte("ddd"))
	assert.NoError(t, serr)
	assert.Equal(t, uint32(30), child)
	assert.Equal(t, 2, idx)

	p.releasePage(pg)
}

func TestRebuildInteriorPageOnPage1(t *testing.T) {
	p := tempPager(t)

	pg, err := p.getWritablePage(1)
	require.NoError(t, err)

	bt := &btree{pager: p, rootPage: 1}
	cells := []cellData{
		{leftChild: 5, key: []byte("ns1")},
	}
	bt.rebuildInteriorPage(pg, cells, 6)

	assert.Equal(t, byte('B'), pg.data[0])
	assert.Equal(t, uint8(pageTypeIntIdx), pg.data[dbHeaderSize])

	p.releasePage(pg)
}

func TestRebuildInteriorPageEmpty(t *testing.T) {
	p := tempPager(t)

	pg, err := p.allocatePage()
	require.NoError(t, err)

	bt := &btree{pager: p, rootPage: pg.pgno}
	bt.rebuildInteriorPage(pg, nil, 99)

	assert.Equal(t, uint16(0), pg.header.cellCount)
	assert.Equal(t, uint32(99), pg.header.rightChild)
	assert.Equal(t, uint16(4096), pg.header.cellContentOff)

	p.releasePage(pg)
}

func TestInsertLeafCellAtPage1(t *testing.T) {
	// insertLeafCellAt on page 1 must write header at dbHeaderSize offset.
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)

	// Master table is on page 1 - create namespace writes cells there
	_, err = tx.CreateNamespace("test_ns")
	require.NoError(t, err)

	// Verify page 1 header offset (page type at dbHeaderSize)
	pg, pgerr := db.pager.getPage(1)
	require.NoError(t, pgerr)
	assert.True(t, pg.header.cellCount > 0)
	db.pager.releasePage(pg)
	require.NoError(t, tx.Commit())
}

// === Varint Test (kept for backward compat) ===

func TestVarint(t *testing.T) {
	tests := []uint64{0, 1, 127, 128, 16383, 16384, 1<<21 - 1, 1 << 21, 1<<63 - 1}
	buf := make([]byte, 9)
	for _, v := range tests {
		n := putVarint(buf, v)
		got, m := getVarint(buf)
		assert.Equal(t, n, m, "varint size mismatch for %d", v)
		assert.Equal(t, v, got, "varint value mismatch")
	}
}

// === Pager Error State Tests ===

func TestPagerGetPageZero(t *testing.T) {
	db := tempDB(t)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	_, err = db.pager.getPage(0)
	assert.ErrorIs(t, err, ErrInvalidPage)
	require.NoError(t, rtx.Rollback())
}

func TestPagerWriteWithoutWriteTx(t *testing.T) {
	db := tempDB(t)
	// pager is in open state, not writer state
	_, err := db.pager.getWritablePage(1)
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestPagerAllocateWithoutWriteTx(t *testing.T) {
	db := tempDB(t)
	_, err := db.pager.allocatePage()
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestPagerCommitWithoutWriteTx(t *testing.T) {
	db := tempDB(t)
	_, _, _, err := db.pager.commit(false, false)
	assert.ErrorIs(t, err, ErrReadOnly)
}

func TestDB_PageSizeAndDatabaseSize(t *testing.T) {
	db, ns := tempDBWithNS(t, "data")

	if ps := db.PageSize(); ps != DefaultPageSize {
		t.Fatalf("PageSize = %d, want %d", ps, DefaultPageSize)
	}
	if sz := db.DatabaseSize(); sz < 2 {
		t.Fatalf("DatabaseSize = %d, want >= 2", sz)
	}

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	// Use fat values to guarantee the leaf splits and allocates new pages,
	// forcing dbSize to grow beyond the initial header+root layout.
	fat := make([]byte, 512)
	for i := 0; i < 200; i++ {
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, tx.Put(ns, k, fat))
	}
	require.NoError(t, tx.Commit())
	if sz := db.DatabaseSize(); sz < 3 {
		t.Fatalf("DatabaseSize after inserts = %d, want >= 3", sz)
	}
}
