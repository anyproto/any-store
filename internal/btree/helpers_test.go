package btree

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// tempDBWithPageSize creates a temp DB with a custom page size and registers cleanup.
func tempDBWithPageSize(t *testing.T, pageSize uint32) *DB {
	t.Helper()
	resetPageBufferPool()
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: pageSize})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// testOpen resets the page buffer pool and opens a database. Use in tests that
// call Open directly instead of tempDBWithPageSize.
func testOpen(t testing.TB, path string, opts Options) (*DB, error) {
	t.Helper()
	resetPageBufferPool()
	return Open(path, opts)
}

// putN inserts rows 1..n with 4-byte big-endian keys and zero-filled values of valSize.
func putN(t *testing.T, db *DB, nsName string, n int, valSize int) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked(nsName)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, valSize)))
	}
	require.NoError(t, tx.Commit())
}

// updateAll overwrites all n rows (keys 1..n) with a new value of valSize.
func updateAll(t *testing.T, db *DB, nsName string, n int, valSize int) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked(nsName)
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, make([]byte, valSize)))
	}
	require.NoError(t, tx.Commit())
}

// updateOne overwrites a single row (key=idx) with a new value of valSize.
func updateOne(t *testing.T, db *DB, nsName string, idx int, valSize int) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked(nsName)
	require.NoError(t, err)
	key := binary.BigEndian.AppendUint32(nil, uint32(idx))
	require.NoError(t, tx.Put(ns, key, make([]byte, valSize)))
	require.NoError(t, tx.Commit())
}

// corruptByte reads a file, patches one byte at offset, and writes it back.
func corruptByte(t *testing.T, path string, offset int, val byte) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[offset] = val
	require.NoError(t, os.WriteFile(path, data, 0644))
}

// countKeys counts the number of keys in a namespace using a cursor in a read tx.
func countKeys(t *testing.T, db *DB, nsName string) int {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := db.getNamespaceLocked(nsName)
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	count := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		count++
	}
	return count
}

// stressRebalance creates namespace "t1", inserts count rows at initSize,
// shrinks all to shrinkSize, then expands row target to expandSize,
// and runs IntegrityCheck.
func stressRebalance(t *testing.T, db *DB, count, initSize, shrinkSize, expandSize, target int) {
	t.Helper()

	// Create namespace "t1"
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	putN(t, db, "t1", count, initSize)
	updateAll(t, db, "t1", count, shrinkSize)
	updateOne(t, db, "t1", target, expandSize)
	require.NoError(t, db.IntegrityCheck())
}

// forgetPager removes a pager's registry entry for tests that close its
// files by hand instead of through pager.close.
func forgetPager(p *pager) {
	if p.registryKey != "" {
		openDBs.CompareAndDelete(p.registryKey, p)
		p.registryKey = ""
	}
}

// createNS creates namespace name in its own write transaction.
func createNS(t *testing.T, db *DB, name string) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace(name)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

// putKey writes one row with a 4-byte big-endian key in its own write transaction.
func putKey(t *testing.T, db *DB, nsName string, key uint32, val []byte) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked(nsName)
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, key), val))
	require.NoError(t, tx.Commit())
}

// useLegacyFormat makes the databases this test creates schema format 5
// files: zero-padded magic and the index payload rule (997 bytes on 4 KiB
// pages) on leaf cells. For tests of that rule, which existing files keep,
// and for fixtures built around it.
func useLegacyFormat(t testing.TB) {
	t.Helper()
	SetCreateLegacyFormatForTest(true)
	t.Cleanup(func() { SetCreateLegacyFormatForTest(legacyFormatSuite) })
}

// useCurrentFormat makes the databases created from here on in this test
// files of the current schema format, whatever the suite runs on.
func useCurrentFormat(t testing.TB) {
	t.Helper()
	SetCreateLegacyFormatForTest(false)
	t.Cleanup(func() { SetCreateLegacyFormatForTest(legacyFormatSuite) })
}

// legacyFormatSuite is set when the whole suite runs on schema format 5
// (ANYSTORE_TEST_LEGACY_FORMAT=1, see TestMain).
var legacyFormatSuite bool

// skipInLegacyFormatSuite skips a test that is about schema format 6.
func skipInLegacyFormatSuite(t testing.TB) {
	t.Helper()
	if legacyFormatSuite {
		t.Skip("schema format 6 only")
	}
}

// TestMain runs the suite on schema format 5 files when
// ANYSTORE_TEST_LEGACY_FORMAT=1, so the rule existing files keep stays covered
// by every test, not only the ones that pin it.
func TestMain(m *testing.M) {
	if os.Getenv("ANYSTORE_TEST_LEGACY_FORMAT") == "1" {
		legacyFormatSuite = true
		SetCreateLegacyFormatForTest(true)
	}
	os.Exit(m.Run())
}
