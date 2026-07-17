package btree

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A master-table cell overflows once len(name)+4 > maxLocalPayload(pageSize)
// (102 at ps=512), pushing the 4-byte root value into the overflow chain.
// These tests pin that such namespaces stay resolvable: the readers follow the
// chain the way SQLite's accessPayload does for sqlite_master rows.

func longNsName(n int) string {
	return "long-" + strings.Repeat("x", n-5)
}

func TestCreateNamespaceLongNameOverflowCell(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)

	// Lengths straddle the ps=512 threshold (102) and include a value split
	// across the local/overflow boundary as well as wholly-spilled values.
	names := []string{longNsName(99), longNsName(103), longNsName(151), longNsName(511)}
	for _, name := range names {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace(name)
		require.NoError(t, err, "len=%d", len(name))
		require.NoError(t, tx.Put(ns, []byte("k"), []byte(name)))
		require.NoError(t, tx.Commit())
	}

	for _, name := range names {
		ns, err := db.GetNamespace(name)
		require.NoError(t, err, "len=%d", len(name))
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		v, err := rtx.Get(ns, []byte("k"))
		require.NoError(t, err)
		assert.Equal(t, []byte(name), v)
		require.NoError(t, rtx.Rollback())
	}
	require.NoError(t, db.IntegrityCheck())

	// Reopen: the overflowing master cells must survive a cold start.
	require.NoError(t, db.Close())
	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()
	for _, name := range names {
		_, err := db2.GetNamespace(name)
		require.NoError(t, err, "after reopen, len=%d", len(name))
	}
	require.NoError(t, db2.IntegrityCheck())
}

func TestRenameNamespaceLongNameOverflowCell(t *testing.T) {
	resetPageBufferPool()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	db, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("short")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	require.NoError(t, tx.Commit())
	oldRoot := ns.RootPage()

	longName := longNsName(151)
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.RenameNamespace("short", longName))
	require.NoError(t, tx2.Commit())

	renamed, err := db.GetNamespace(longName)
	require.NoError(t, err)
	assert.Equal(t, oldRoot, renamed.RootPage(), "root page must be reused")
	_, err = db.GetNamespace("short")
	assert.ErrorIs(t, err, ErrNamespaceNotFound)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	v, err := rtx.Get(renamed, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), v)
	require.NoError(t, rtx.Rollback())

	require.NoError(t, db.IntegrityCheck())

	require.NoError(t, db.Close())
	db2, err := testOpen(t, path, Options{PageSize: 512})
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()
	ns2, err := db2.GetNamespace(longName)
	require.NoError(t, err)
	assert.Equal(t, oldRoot, ns2.RootPage())
	require.NoError(t, db2.IntegrityCheck())
}
