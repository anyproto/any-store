package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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

	// In-memory databases report an empty path, matching SQLite's
	// sqlite3BtreeGetFilename -> sqlite3PagerFilename(pPager, nullIfMemDb=1).
	memDB, err := testOpen(t, "ignored.db", Options{InMemory: true})
	require.NoError(t, err)
	assert.Equal(t, "", memDB.Path())
	memDB.Close()
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
	got, err := bt.collectInteriorCells(pg2)
	require.NoError(t, err)
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

	// Page 1 carries the database header; the rebuild must not clobber it.
	// Literal on purpose: a rename or edit of dbMagicV2 must fail here rather
	// than pass by comparing the constant against itself.
	assert.Equal(t, "any-store v2\x00\x00\x00\x00", string(pg.data[0:16]))
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

// === Descent zero-cell interior invariant (moved from btree_descent_invariant_test.go) ===

// assertDescentNeverHitsZeroCellInterior pins the read-path invariant behind
// drift-11 (docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-):
// on a VALID tree, no cursor/seek descent step ever lands on a zero-cell
// (nCell<1) interior page. SQLite enforces this with moveToChild's
// `pPage->nCell<1 -> SQLITE_CORRUPT_PGNO` guard (btree.c:5477-5482, inlined at
// btree.c:6253-6258); the Go descent paths carry no such per-child guard, so the
// guard is "kept by-design" (drift-11) ONLY because a valid any-store tree never
// produces a descendable zero-cell interior. This helper replays every descent
// path the cursor uses — First (cell-0 child), Last (rightChild), and Seek
// (searchInterior) — over a set of probe keys and asserts cellCount>=1 on every
// interior page it touches, including the root, mirroring the exact child-pointer
// arithmetic in Cursor.First/Last/Seek (btree.go:3344-3366, 3391-3405,
// 3430-3447) and searchInterior (btree.go:936-989).
func assertDescentNeverHitsZeroCellInterior(t *testing.T, bt *btree, probeKeys [][]byte) {
	t.Helper()

	// checkInterior fails if pg is an interior page with 0 cells: that is exactly
	// the page SQLite's moveToChild would reject, and the page searchInterior
	// would mis-read (n==0 skips the loop, then dereferences the cleared first
	// cell-pointer slot as a child pointer instead of routing to rightChild).
	checkInterior := func(pg *page, via string) {
		t.Helper()
		if pg.header.isInterior() {
			require.NotZero(t, int(pg.header.cellCount),
				"%s descent reached a zero-cell interior page %d (drift-11 invariant violated)", via, pg.pgno)
		}
	}

	// First: descend through the cell-0 child pointer to the leftmost leaf.
	descendFirst := func() {
		pg, err := bt.getPage(bt.rootPage)
		require.NoError(t, err)
		for pg.header.isInterior() {
			checkInterior(pg, "First")
			off := int(pg.getCellOffset(0))
			require.LessOrEqual(t, off+4, len(pg.data))
			child := binary.BigEndian.Uint32(pg.data[off : off+4])
			bt.pager.releasePage(pg)
			pg, err = bt.descendChild(child)
			require.NoError(t, err)
		}
		bt.pager.releasePage(pg)
	}

	// Last: descend through rightChild to the rightmost leaf.
	descendLast := func() {
		pg, err := bt.getPage(bt.rootPage)
		require.NoError(t, err)
		for pg.header.isInterior() {
			checkInterior(pg, "Last")
			child := pg.header.rightChild
			bt.pager.releasePage(pg)
			pg, err = bt.descendChild(child)
			require.NoError(t, err)
		}
		bt.pager.releasePage(pg)
	}

	// Seek(key): descend via searchInterior (the binary-search router used by
	// Cursor.Seek) to the target leaf, checking every interior page on the way.
	descendSeek := func(key []byte) {
		pg, err := bt.getPage(bt.rootPage)
		require.NoError(t, err)
		for pg.header.isInterior() {
			checkInterior(pg, "Seek")
			child, _, serr := bt.searchInterior(pg, key)
			require.NoError(t, serr)
			bt.pager.releasePage(pg)
			pg, err = bt.descendChild(child)
			require.NoError(t, err)
		}
		bt.pager.releasePage(pg)
	}

	descendFirst()
	descendLast()
	for _, k := range probeKeys {
		descendSeek(k)
	}
}

// TestMoveToChild_DescentNeverHitsZeroCellInterior pins the drift-11 read-path
// invariant: on a valid any-store tree, cursor/seek descent never lands on a
// zero-cell (nCell<1) interior page, so the missing moveToChild nCell>=1 guard
// (btree.c:5477-5482) is unreachable by-design rather than unsafe.
//
// It builds a multi-level tree, then drives a heavy delete workload. Deletes are
// the only operations that can transiently produce a zero-cell non-root interior
// (finishParentRemoval, btree.go), and the delete-rebalance cascade
// (completeMergeUpward) must eliminate every such node before commit. The test
// asserts the invariant three ways after EACH committed mutation phase:
//
//  1. structural: no non-root interior page persists with 0 dividers
//     (assertNoDegenerateInterior — a descendable zero-cell interior can only
//     exist if such a degenerate node survived);
//  2. descent-path: replaying First/Last/Seek descent over probe keys never
//     touches a zero-cell interior (assertDescentNeverHitsZeroCellInterior);
//  3. behavioral: the real Cursor.First/Next/Last/Seek agree with the expected
//     survivor set, proving no descent silently bailed at a zero-cell interior.
func TestMoveToChild_DescentNeverHitsZeroCellInterior(t *testing.T) {
	// 512-byte pages (minimum) give a low fanout, so the tree is multi-level and
	// deletes empty whole leaves / parents — the case that can transiently create
	// a zero-cell interior the cascade must remove before commit.
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	const total = 2000
	putN(t, db, "t1", total, 40)
	require.NoError(t, db.IntegrityCheck(), "integrity after insert")

	rootOf := func() uint32 {
		ns, nerr := db.getNamespaceLocked("t1")
		require.NoError(t, nerr)
		return ns.rootPage
	}

	keyOf := func(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) }

	// checkInvariant builds a read-snapshot btree (the same snapshot a cursor
	// uses) and asserts the structural + descent-path invariants, then drives the
	// real cursor against the expected survivor set.
	checkInvariant := func(phase string, survivors [][]byte) {
		t.Helper()
		require.NoError(t, db.IntegrityCheck(), "integrity %s", phase)

		rtx, rerr := db.BeginRead()
		require.NoError(t, rerr)
		defer func() { _ = rtx.Rollback() }()
		ns, nerr := db.getNamespaceLocked("t1")
		require.NoError(t, nerr)
		bt := &btree{pager: db.pager, cache: rtx.cache, rootPage: ns.rootPage, walMaxFrame: rtx.walMaxFrame}

		// (1) No degenerate single-child non-root interior survived a commit.
		assertNoDegenerateInterior(t, bt, rootOf(), true)

		// (2) Descent never lands on a zero-cell interior. Probe with the
		// survivor keys plus boundary keys (below-min, above-max, and gaps).
		probes := make([][]byte, 0, len(survivors)+3)
		probes = append(probes, keyOf(0), keyOf(total+1))
		probes = append(probes, survivors...)
		// Also probe between survivors (deleted-key gaps) to exercise the
		// "descend to first key >= probe" router on absent keys.
		for i := 1; i <= total; i += 3 {
			probes = append(probes, keyOf(i))
		}
		assertDescentNeverHitsZeroCellInterior(t, bt, probes)

		// (3) Behavioral: real cursor forward scan == sorted survivor set, and
		// Seek/SeekExact land correctly — proving no descent silently bailed.
		cur := rtx.NewCursor(ns)
		fwd := make([][]byte, 0, len(survivors))
		for cerr := cur.First(); cerr == nil && cur.Valid(); cerr = cur.Next() {
			k, kerr := cur.Key()
			require.NoError(t, kerr)
			fwd = append(fwd, append([]byte(nil), k...))
		}
		require.Equal(t, len(survivors), len(fwd), "%s: forward scan count", phase)
		for i := range survivors {
			require.True(t, bytes.Equal(survivors[i], fwd[i]),
				"%s: forward scan mismatch at %d", phase, i)
		}
		// Each survivor is reachable by Seek (descent succeeded to its leaf).
		for _, k := range survivors {
			require.NoError(t, cur.SeekExact(k), "%s: SeekExact survivor", phase)
			require.True(t, cur.Valid(), "%s: cursor valid after SeekExact", phase)
		}
		// Last lands on the max survivor.
		if len(survivors) > 0 {
			require.NoError(t, cur.Last())
			require.True(t, cur.Valid())
			lk, lerr := cur.Key()
			require.NoError(t, lerr)
			require.True(t, bytes.Equal(survivors[len(survivors)-1], lk),
				"%s: Last == max survivor", phase)
		}
	}

	sortedSurvivors := func(keep func(int) bool) [][]byte {
		s := make([][]byte, 0, total)
		for i := 1; i <= total; i++ {
			if keep(i) {
				s = append(s, keyOf(i))
			}
		}
		slices.SortFunc(s, bytes.Compare)
		return s
	}

	// Phase 0: full tree, all keys present.
	checkInvariant("after-insert", sortedSurvivors(func(int) bool { return true }))

	// Phase 1: delete a sparse subset (every 2nd key) — thins many leaves and
	// empties some, forcing rebalancing/merges that transiently create and then
	// cascade away zero-cell interiors.
	keep1 := func(i int) bool { return i%2 == 0 }
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= total; i++ {
		if !keep1(i) {
			require.NoError(t, tx.Delete(ns, keyOf(i)))
		}
	}
	require.NoError(t, tx.Commit())
	checkInvariant("after-delete-half", sortedSurvivors(keep1))

	// Phase 2: aggressive delete — keep only every 7th of the ORIGINAL keys
	// (i.e. drop almost everything still present), driving deep upward cascades
	// that repeatedly empty parents. keep7 is a subset of keep1's survivors.
	keep7 := func(i int) bool { return i%2 == 0 && i%7 == 0 }
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 1; i <= total; i++ {
		if keep1(i) && !keep7(i) {
			require.NoError(t, tx.Delete(ns, keyOf(i)))
		}
	}
	require.NoError(t, tx.Commit())
	checkInvariant("after-delete-aggressive", sortedSurvivors(keep7))
}

// === Search-leaf overflow prefix-shortcut invariant (moved from btree_search_overflow_invariant_test.go) ===

// TestSearchLeafOverflow_PrefixShortcutInvariant pins the by-design drift
// documented at
// docs/btree/NOTES.md#old-drift-binsearch-rawbytes-prefix-no-overflow-cache.
//
// searchLeafWithOverflow optimizes the binary search over overflow cells by
// comparing only the on-page local key prefix first (btree.go:632-655). The
// invariant a correct refactor MUST preserve:
//
//	A truncated prefix decision (prefixCmp != 0 over cmpLen = min(localKeyBytes,
//	len(key))) MAY short-circuit and skip the full overflow read, BUT whenever
//	the prefix compares EQUAL on cmpLen the code MUST fall through to a full
//	leafFullKey read so that NO ordering decision is ever made on truncated
//	bytes.
//
// We pin it via the test-only searchLeafOverflowProbe hook, which records
// whether each overflow cell was decided by the prefix shortcut
// (searchProbePrefixShortCircuit) or by a full key read (searchProbeFullKeyRead).
// A future refactor that decides ordering on equal-but-truncated bytes (e.g.
// dropping the leafFullKey fall-through) would change the recorded events and
// fail this test loudly. Production behavior is unchanged: the probe is nil
// outside tests.
func TestSearchLeafOverflow_PrefixShortcutInvariant(t *testing.T) {
	// Small page so a modest key overflows on-page.
	p := tempPagerWithPageSize(t, 512)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	// Build a single overflow leaf cell whose KEY spills to overflow pages.
	// The stored full key is distinct in every byte position so we can derive
	// case-specific search keys below. value is tiny and irrelevant.
	keyLen := maxLocal + 80 // > maxLocal => key overflows
	storedKey := make([]byte, keyLen)
	for i := range storedKey {
		// Keep bytes in the middle of the range so we can craft both a strictly
		// smaller and a strictly larger byte at any position.
		storedKey[i] = byte('m')
	}
	value := []byte("v")

	require.NoError(t, bt.rebuildLeafPage(pg, []cellData{{key: storedKey, value: value}}))

	// Derive the local-key split for the overflow cell exactly as the search
	// code does, so test cases target the prefix vs overflow regions precisely.
	totalPayload := keyLen + len(value)
	require.Greater(t, totalPayload, maxLocal, "stored key must overflow")
	nLocal := localPayloadSize(totalPayload, usableSize)
	localKeyBytes := min(nLocal, keyLen)
	require.Less(t, localKeyBytes, keyLen,
		"test requires the key itself to overflow (localKeyBytes < keyLen)")
	require.Greater(t, localKeyBytes, 1, "need at least 2 local prefix bytes")

	// Install the probe to record which decision path each cell takes.
	var events []int
	searchLeafOverflowProbe = func(event int) { events = append(events, event) }
	t.Cleanup(func() { searchLeafOverflowProbe = nil })

	// search runs one probe-recorded search and returns the recorded event
	// sequence, the matched index, and whether an exact match was found.
	search := func(key []byte) (gotEvents []int, idx int, found bool) {
		events = nil
		i, f, serr := searchLeafWithOverflow(pg, key, usableSize, p, 0, nil)
		require.NoError(t, serr)
		return events, i, f
	}

	t.Run("differing byte within local prefix short-circuits with no overflow read", func(t *testing.T) {
		// Key differs from stored at a position inside the local prefix.
		// The prefix alone determines ordering => short-circuit, no full read.
		smaller := bytes.Clone(storedKey)
		smaller[1] = 'a' // 'a' < 'm' within the local prefix
		ev, _, found := search(smaller)
		require.Equal(t, []int{searchProbePrefixShortCircuit}, ev,
			"differing byte in the local prefix must be decided WITHOUT a full overflow read")
		require.False(t, found)

		larger := bytes.Clone(storedKey)
		larger[1] = 'z' // 'z' > 'm' within the local prefix
		ev, _, found = search(larger)
		require.Equal(t, []int{searchProbePrefixShortCircuit}, ev,
			"differing byte in the local prefix must be decided WITHOUT a full overflow read")
		require.False(t, found)
	})

	t.Run("equal full keys: prefix equal falls through to full read => found", func(t *testing.T) {
		// Searching the exact stored key: prefix is equal on cmpLen, so the code
		// MUST take the full read and return EQUAL (found).
		ev, _, found := search(bytes.Clone(storedKey))
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"equal-on-prefix must fall through to a full key read, not a truncated decision")
		require.True(t, found, "exact overflow key must be found via the full read")
	})

	t.Run("differing byte beyond local prefix: prefix equal => full read disambiguates", func(t *testing.T) {
		// Search key equals stored on the whole local prefix but differs in the
		// overflow region. The prefix shortcut sees equality and MUST fall
		// through; only the full read can decide ordering correctly.
		largerInOverflow := bytes.Clone(storedKey)
		largerInOverflow[localKeyBytes+5] = 'z' // 'z' > 'm', beyond the local prefix
		ev, _, found := search(largerInOverflow)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"a difference beyond the local prefix must be decided by a full read")
		require.False(t, found, "search key > stored => no exact match")

		smallerInOverflow := bytes.Clone(storedKey)
		smallerInOverflow[localKeyBytes+5] = 'a' // 'a' < 'm', beyond the local prefix
		ev, _, found = search(smallerInOverflow)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"a difference beyond the local prefix must be decided by a full read")
		require.False(t, found)
	})

	t.Run("search key SHORTER than local prefix: cmpLen truncates to len(key), prefix equal => full read", func(t *testing.T) {
		// len(key) < localKeyBytes: cmpLen == len(key). The truncated compare is
		// equal, so the code MUST NOT decide on it; it falls through to a full
		// read where the longer stored key is correctly ordered as the greater.
		shortKey := bytes.Clone(storedKey[:localKeyBytes-2])
		ev, _, found := search(shortKey)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"a search key shorter than the local prefix must not be decided on truncated bytes")
		require.False(t, found, "shorter key is a strict prefix => not equal to the longer stored key")
	})

	t.Run("search key LONGER than localKeyBytes but equal on the local portion => full read", func(t *testing.T) {
		// len(key) > localKeyBytes, equal on the whole local prefix. cmpLen ==
		// localKeyBytes, prefix equal => full read disambiguates against the
		// overflow bytes.
		longerEqualLocal := bytes.Clone(storedKey)
		longerEqualLocal = append(longerEqualLocal, 'x') // strictly longer than stored
		ev, _, found := search(longerEqualLocal)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"equal on the local portion must fall through to a full read")
		require.False(t, found, "stored key is a strict prefix of the search key => not equal")
	})

	t.Run("empty search key: cmpLen==0, no slice panic, full read taken", func(t *testing.T) {
		// len(key)==0 => cmpLen==0. bytes.Compare on empty slices is 0, so the
		// prefix is "equal"; the code must fall through to a full read (no
		// truncated decision) and must not panic on the empty-slice indexing.
		ev, _, found := search([]byte{})
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"empty key yields cmpLen==0 (equal); must fall through to a full read")
		require.False(t, found, "non-empty stored key is never equal to the empty key")
	})
}

// TestSearchLeafOverflow_StoredKeyExtendsSearchKey pins the specific case where
// the search key is a TRUE PREFIX of a longer stored overflow key: the prefix
// compares equal on cmpLen, the full read is taken, and full bytes.Compare must
// report stored > search. This is a focused complement to the table above so a
// regression in the "stored extends search" direction is unmissable.
func TestSearchLeafOverflow_StoredKeyExtendsSearchKey(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	storedKey := bytes.Repeat([]byte("p"), maxLocal+80) // overflows
	require.NoError(t, bt.rebuildLeafPage(pg, []cellData{{key: storedKey, value: []byte("v")}}))

	nLocal := localPayloadSize(len(storedKey)+1, usableSize)
	localKeyBytes := min(nLocal, len(storedKey))

	var events []int
	searchLeafOverflowProbe = func(event int) { events = append(events, event) }
	t.Cleanup(func() { searchLeafOverflowProbe = nil })

	// Search key is the stored key truncated past the local prefix: a true
	// prefix of the longer overflow key. cmpLen == len(searchKey) == prefix is
	// equal, so the full read must run and order stored > search.
	searchKey := bytes.Clone(storedKey[:localKeyBytes+10])
	events = nil
	idx, found, serr := searchLeafWithOverflow(pg, searchKey, usableSize, p, 0, nil)
	require.NoError(t, serr)
	require.Equal(t, []int{searchProbeFullKeyRead}, events,
		"a true prefix of a longer stored key must be resolved by a full read, not a truncated decision")
	require.False(t, found, "search key is a strict prefix => not equal to the longer stored key")
	// stored > search => insertion point is before the cell (index 0).
	require.Equal(t, 0, idx)

	// Sanity: the full key the search path reconstructs equals the stored key,
	// confirming the full-read path (not a truncated guess) drives the decision.
	off := int(pg.data[pg.cellPointerOffset()])<<8 | int(pg.data[pg.cellPointerOffset()+1])
	full, ferr := leafFullKey(pg.data, off, usableSize, p, 0, nil)
	require.NoError(t, ferr)
	require.Equal(t, storedKey, full)
	require.Equal(t, 1, bytes.Compare(full, searchKey), "stored key must order after its strict prefix")
}

// TestSearchLeafOverflow_ProbeNilInProduction documents that the test hook is
// inert unless a test installs it: searchLeafWithOverflow must produce correct
// results with searchLeafOverflowProbe == nil (the production configuration).
func TestSearchLeafOverflow_ProbeNilInProduction(t *testing.T) {
	require.Nil(t, searchLeafOverflowProbe, "probe must be nil by default (production behavior)")

	p := tempPagerWithPageSize(t, 512)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	storedKey := bytes.Repeat([]byte("q"), maxLocal+80)
	require.NoError(t, bt.rebuildLeafPage(pg, []cellData{{key: storedKey, value: []byte("v")}}))

	// Exact match still found with the probe nil.
	idx, found, serr := searchLeafWithOverflow(pg, bytes.Clone(storedKey), usableSize, p, 0, nil)
	require.NoError(t, serr)
	require.True(t, found)
	require.Equal(t, 0, idx)
}

// === Cursor read-only / two-state invariants (moved from cursor_readonly_invariant_test.go) ===

// These tests pin the structural invariants behind the by-design drift
// documented at docs/btree/NOTES.md#old-drift-readonly-two-state-cursor:
// the Go Cursor is a READ-ONLY, 2-STATE (valid bool), dynamic-stack cursor that
// pins ONLY its leaf frame and has NO save/restore.
//
// In SQLite a BtCursor has 5 states and, before every tree mutation, calls
// saveAllCursors(pBt, pCur->pgnoRoot, pCur) (btree.c:9442 in sqlite3BtreeInsert,
// btree.c:9935 in sqlite3BtreeDelete), which serializes the cursor key, releases
// all pinned cursor pages (btreeReleaseAllCursorPages, btree.c:769-789), and sets
// eState=CURSOR_REQUIRESEEK so the cursor re-seeks on next use. The Go port drops
// this entirely: the cursor keeps frame.pg pinned and frame.cellIdx frozen across
// any mutation.
//
// That omission is SAFE only because the design relies on an unstated invariant:
// the Go cursor is a read-only iterator whose pinned leaf and frozen position are
// never mutated underneath it within the same logical operation. Writes go through
// WriteTx.Put / WriteTx.Delete, which build their OWN writable btree and traverse
// from the root (db.go:1834-1849, btree.go:Put/Delete) rather than driving a
// cursor. These tests assert the load-bearing facts of that contract so that a
// future refactor which (e.g.) adds a write method to Cursor, pins more than the
// leaf, drives mutations through a cursor, or introduces a hidden re-seek state
// fails loudly here.
//
// NONE of these tests change or exercise unsupported production behavior — they
// only observe and pin the existing read-only/2-state/single-leaf-pin contract.

// buildMultiLevelTree inserts n small entries with a 512-byte page size so the
// resulting tree has at least one interior level above the leaves. The cursor
// stack then contains interior frames (pg == nil) below a single pinned leaf
// frame, which is what makes the single-leaf-pin assertion meaningful.
func buildMultiLevelTree(t *testing.T, n int) (*DB, *Namespace) {
	t.Helper()
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		k := fmt.Appendf(nil, "key-%06d", i)
		v := fmt.Appendf(nil, "val-%06d", i)
		require.NoError(t, tx2.Put(ns, k, v))
	}
	require.NoError(t, tx2.Commit())
	return db, ns
}

// assertSingleLeafPin pins the "Only leaf page pinned" invariant
// (NOTES.md:903, 915): in a positioned cursor stack, exactly the top (leaf)
// frame holds a pinned page (pg != nil) and every interior frame below it has
// pg == nil. SQLite pins every page in the stack and relies on
// saveAllCursors/btreeReleaseAllCursorPages to drop them before a mutation; the
// Go design pins only the leaf precisely because it never has to release-and-
// re-seek mid-operation. If a refactor starts pinning interior frames (or stops
// pinning the leaf), the no-save/restore design's assumptions change and this
// fails loudly.
func assertSingleLeafPin(t *testing.T, c *Cursor) {
	t.Helper()
	require.True(t, c.valid, "cursor must be positioned for the pin invariant to apply")
	require.NotEmpty(t, c.stack, "a valid cursor must have a non-empty stack")

	last := len(c.stack) - 1
	for i := range c.stack {
		f := &c.stack[i]
		if i == last {
			require.NotNilf(t, f.pg, "leaf frame %d must pin its page (single-leaf-pin invariant)", i)
			require.Falsef(t, f.pg.header.isInterior(),
				"top frame %d must be a leaf page, got interior pgno=%d", i, f.pgno)
			// The pinned leaf is held with a live reference for the cursor's
			// lifetime — this is exactly the pin SQLite would have dropped via
			// btreeReleaseAllCursorPages before a mutation (btree.c:769-789).
			require.GreaterOrEqualf(t, f.pg.pinCount, 1,
				"leaf frame %d page pgno=%d must stay pinned (pinCount>=1)", i, f.pgno)
		} else {
			require.Nilf(t, f.pg, "interior frame %d must NOT pin a page (only the leaf is pinned)", i)
		}
	}
}

// TestCursorReadOnlyInvariant_OnlyLeafPinned walks the cursor across a
// multi-level tree (First + repeated Next, then Last + repeated Previous) and
// asserts the single-leaf-pin invariant at every position. This is the core
// structural property the no-save/restore drift relies on.
func TestCursorReadOnlyInvariant_OnlyLeafPinned(t *testing.T) {
	db, _ := buildMultiLevelTree(t, 2000)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	// Forward: the stack must include interior frames at least once (otherwise
	// the "interior frames are unpinned" half of the invariant is vacuous and we
	// have not actually built a multi-level tree).
	cur := rtx.NewCursor(ns)
	require.NoError(t, cur.First())
	require.True(t, cur.Valid())
	sawInterior := false
	for cur.Valid() {
		if len(cur.stack) > 1 {
			sawInterior = true
		}
		assertSingleLeafPin(t, cur)
		require.NoError(t, cur.Next())
	}
	require.True(t, sawInterior,
		"test tree must be multi-level so interior frames exist; increase n")

	// Backward exercises the descend-to-rightmost-leaf branch of Previous().
	cur2 := rtx.NewCursor(ns)
	require.NoError(t, cur2.Last())
	require.True(t, cur2.Valid())
	for cur2.Valid() {
		assertSingleLeafPin(t, cur2)
		require.NoError(t, cur2.Previous())
	}
}

// TestCursorReadOnlyInvariant_NoReSeekStateFrozenPin pins the 2-STATE +
// frozen-pin half of the contract: a positioned cursor stays on the SAME pinned
// *page with a STABLE cellIdx across repeated value reads, and Valid() is exactly
// the 2-state model (no hidden CURSOR_REQUIRESEEK that would silently re-seek and
// swap the pinned page). SQLite's re-seek machinery (saveCursorKey + REQUIRESEEK,
// btree.c:724-789) deliberately invalidates the pinned page; the Go cursor must
// NOT — its readers (Key/Value) point directly into the still-pinned buffer.
func TestCursorReadOnlyInvariant_NoReSeekStateFrozenPin(t *testing.T) {
	db, _ := buildMultiLevelTree(t, 2000)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err := db.getNamespaceLocked("data")
	require.NoError(t, err)

	cur := rtx.NewCursor(ns)
	require.NoError(t, cur.Seek([]byte("key-001000")))
	require.True(t, cur.Valid())

	// Snapshot the pinned leaf identity and frozen position.
	leaf := &cur.stack[len(cur.stack)-1]
	pinnedPage := leaf.pg
	frozenIdx := leaf.cellIdx
	require.NotNil(t, pinnedPage)

	k0, err := cur.Key()
	require.NoError(t, err)

	// Repeated reads must NOT re-seek (which in SQLite would drop and re-pin the
	// page) — same *page pointer, same cellIdx, identical key/value each time.
	for i := 0; i < 5; i++ {
		require.True(t, cur.Valid(), "read must not flip the 2-state validity")
		require.Same(t, pinnedPage, cur.stack[len(cur.stack)-1].pg,
			"repeated reads must keep the SAME pinned leaf page (no hidden re-seek)")
		require.Equal(t, frozenIdx, cur.stack[len(cur.stack)-1].cellIdx,
			"repeated reads must keep cellIdx frozen (no hidden re-seek)")
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		require.Equal(t, k0, k, "key must be stable across repeated reads")
		_, verr := cur.Value()
		require.NoError(t, verr)
	}

	// Close() is the ONLY thing that drops the leaf pin and clears the position.
	// (Mirrors btreeReleaseAllCursorPages — but driven explicitly by the caller,
	// not implicitly by a mutation.)
	cur.Close()
	require.False(t, cur.Valid(), "Close must move the cursor to the invalid state")
	require.Empty(t, cur.stack, "Close must clear the stack so no released page is re-read")
}

// TestCursorReadOnlyInvariant_WritesBypassCursor pins the write-path
// independence the drift relies on: within a SINGLE write transaction, while a
// cursor is live and positioned on a namespace, WriteTx.Put / WriteTx.Delete
// mutate that SAME namespace through their own writable btree traversal from the
// root (db.go:1834-1849) and NEVER through the cursor. The cursor is purely an
// observer; mutation is not routed through its pinned leaf.
//
// This is the exact "write to the same namespace a live cursor is positioned on,
// within one write transaction" scenario from the invariant statement. We assert
// the design fact that makes the absent saveAllCursors harmless here: the writer
// does its own traversal, so the cursor's pinned page is not the mutation vehicle.
func TestCursorReadOnlyInvariant_WritesBypassCursor(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() {
		if !wtx.closed {
			_ = wtx.Rollback()
		}
	}()
	ns, err := wtx.CreateNamespace("data")
	require.NoError(t, err)

	// Seed a handful of keys so the cursor can position on a real leaf.
	for i := 0; i < 8; i++ {
		require.NoError(t, wtx.Put(ns, fmt.Appendf(nil, "k-%03d", i), []byte("v")))
	}

	// A cursor opened on a WriteTx (WriteTx embeds ReadTx, so NewCursor exists)
	// is still the read-only, writable-snapshot cursor.
	cur := wtx.NewCursor(ns)
	require.True(t, cur.bt.writable,
		"a write-tx cursor reads the writer snapshot (bt.writable) yet exposes no write API")
	require.NoError(t, cur.Seek([]byte("k-004")))
	require.True(t, cur.Valid())
	require.NotNil(t, cur.stack[len(cur.stack)-1].pg)

	// Mutate the SAME namespace via the WriteTx API. This must succeed and must
	// not route through the cursor: Put/Delete build their own bt{writable:true}
	// and traverse from ns.rootPage. We are NOT asserting the cursor stays valid
	// afterwards (the design explicitly does not guarantee that); we are pinning
	// that the write path exists and is independent of the cursor object.
	require.NoError(t, wtx.Put(ns, []byte("k-100"), []byte("inserted")))
	require.NoError(t, wtx.Delete(ns, []byte("k-001")))

	// The Cursor object carries no path into the writer's mutation routines: it
	// has no Put/Delete/Insert/Set/Update/Remove method. A future refactor that
	// adds one (i.e. makes the cursor a write cursor) must consciously revisit
	// the saveAllCursors gap and will trip this guard.
	assertCursorExposesNoWriteMethods(t)

	// After the commit the data reflects the writer's own traversal, confirming
	// the mutations went through Put/Delete, not the cursor.
	require.NoError(t, wtx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("data")
	require.NoError(t, err)
	got, err := rtx.Get(ns2, []byte("k-100"))
	require.NoError(t, err)
	require.Equal(t, []byte("inserted"), got)
	_, err = rtx.Get(ns2, []byte("k-001"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// assertCursorExposesNoWriteMethods uses reflection to pin the read-only API
// surface of *Cursor. The Go cursor (NOTES.md:918) has no BTCF_WriteFlag
// equivalent: it must expose only read/seek/navigation methods. The presence of
// any mutation method would mean the cursor became a write cursor, at which point
// the missing saveAllCursors/restore machinery is no longer a benign drift.
func assertCursorExposesNoWriteMethods(t *testing.T) {
	t.Helper()
	forbidden := map[string]struct{}{
		"Put": {}, "Insert": {}, "Set": {}, "Delete": {},
		"Remove": {}, "Update": {}, "Write": {}, "Save": {},
	}
	ct := reflect.TypeOf(&Cursor{})
	for i := 0; i < ct.NumMethod(); i++ {
		name := ct.Method(i).Name
		_, bad := forbidden[name]
		require.Falsef(t, bad,
			"Cursor must remain read-only (no write methods); found mutating method %q — "+
				"if the cursor is now a write cursor, the missing saveAllCursors/restore "+
				"(NOTES #old-drift-readonly-two-state-cursor) must be revisited", name)
	}
}
