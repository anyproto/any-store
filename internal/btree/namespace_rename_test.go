package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenameNamespace(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("old")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k1"), []byte("v1")))
	require.NoError(t, tx.Put(ns, []byte("k2"), []byte("v2")))
	require.NoError(t, tx.Commit())
	oldRoot := ns.RootPage()

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.RenameNamespace("old", "new"))
	require.NoError(t, tx2.Commit())

	renamed, err := db.GetNamespace("new")
	require.NoError(t, err)
	assert.Equal(t, oldRoot, renamed.RootPage(), "root page must be reused")

	_, err = db.GetNamespace("old")
	assert.ErrorIs(t, err, ErrNamespaceNotFound)

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	v1, err := rtx.Get(renamed, []byte("k1"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v1"), v1)
	v2, err := rtx.Get(renamed, []byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v2)
	require.NoError(t, rtx.Rollback())

	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"new"}, names)

	require.NoError(t, db.IntegrityCheck())
}

func TestRenameNamespaceNotFound(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	assert.ErrorIs(t, tx.RenameNamespace("missing", "whatever"), ErrNamespaceNotFound)
	// The tx stays usable after a failed rename of a missing namespace.
	_, err = tx.CreateNamespace("still-works")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
}

func TestRenameNamespaceExists(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	src, err := tx.CreateNamespace("src")
	require.NoError(t, err)
	dst, err := tx.CreateNamespace("dst")
	require.NoError(t, err)
	require.NoError(t, tx.Put(dst, []byte("k"), []byte("dst-data")))
	require.NoError(t, tx.Commit())
	_ = src

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	assert.ErrorIs(t, tx2.RenameNamespace("src", "dst"), ErrNamespaceExists)
	require.NoError(t, tx2.Rollback())

	// Target namespace data untouched.
	dstNs, err := db.GetNamespace("dst")
	require.NoError(t, err)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	v, err := rtx.Get(dstNs, []byte("k"))
	require.NoError(t, err)
	assert.Equal(t, []byte("dst-data"), v)
	require.NoError(t, rtx.Rollback())
}

func TestRenameNamespaceClosedTx(t *testing.T) {
	db, _ := tempDBWithNS(t, "a")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.ErrorIs(t, tx.RenameNamespace("a", "b"), ErrTxClosed)

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.Rollback())
	assert.ErrorIs(t, tx2.RenameNamespace("a", "b"), ErrTxClosed)
}

func TestRenameNamespaceSameName(t *testing.T) {
	db, _ := tempDBWithNS(t, "same")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	// Same-name rename of an existing namespace is a validated no-op.
	require.NoError(t, tx.RenameNamespace("same", "same"))
	// Same-name rename of a missing namespace still reports not-found.
	assert.ErrorIs(t, tx.RenameNamespace("missing", "missing"), ErrNamespaceNotFound)
	require.NoError(t, tx.Commit())

	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"same"}, names)
}

func TestRenameNamespaceInteriorMaster(t *testing.T) {
	db := tempDB(t)
	// >233 namespaces split page 1 into an interior node (see
	// TestCreateManyNamespaces); rename must handle the multi-level master table.
	const count = 300
	roots := make(map[string]uint32, count)
	for i := range count {
		name := fmt.Sprintf("ns-%04d", i)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace(name)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		roots[name] = ns.RootPage()
	}

	renames := map[string]string{
		"ns-0000": "renamed-first",
		"ns-0150": "renamed-middle",
		fmt.Sprintf("ns-%04d", count-1): "renamed-last",
	}
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for old, next := range renames {
		require.NoError(t, tx.RenameNamespace(old, next))
	}
	require.NoError(t, tx.Commit())

	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.Len(t, names, count)
	for old, next := range renames {
		_, err = db.GetNamespace(old)
		assert.ErrorIs(t, err, ErrNamespaceNotFound, "old name %s", old)
		ns, err := db.GetNamespace(next)
		require.NoError(t, err, "new name %s", next)
		assert.Equal(t, roots[old], ns.RootPage(), "%s must keep root of %s", next, old)
	}

	require.NoError(t, db.IntegrityCheck())
}

func TestRenameNamespaceUncommittedVisibility(t *testing.T) {
	db, _ := tempDBWithNS(t, "old")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.RenameNamespace("old", "new"))
	// Writer path sees the rename within the same tx.
	ns, err := tx.GetNamespace("new")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	_, err = tx.GetNamespace("old")
	assert.ErrorIs(t, err, ErrNamespaceNotFound)
	require.NoError(t, tx.Commit())

	got, err := db.GetNamespace("new")
	require.NoError(t, err)
	assert.Equal(t, ns.RootPage(), got.RootPage())
}

func TestRenameThenDeleteInterplay(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("a")
	require.NoError(t, err)
	// Enough data to occupy several pages so the eventual delete frees some.
	for i := range 500 {
		require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "key-%04d", i), make([]byte, 100)))
	}
	require.NoError(t, tx.Commit())

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.RenameNamespace("a", "b"))
	assert.ErrorIs(t, tx2.DeleteNamespace("a"), ErrNamespaceNotFound)
	require.NoError(t, tx2.Commit())

	// A fresh namespace can take the vacated old name; isolation preserved.
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	fresh, err := tx3.CreateNamespace("a")
	require.NoError(t, err)
	require.NoError(t, tx3.Put(fresh, []byte("key-0000"), []byte("fresh")))
	require.NoError(t, tx3.Commit())

	renamed, err := db.GetNamespace("b")
	require.NoError(t, err)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	v, err := rtx.Get(renamed, []byte("key-0000"))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 100), v)
	freshR, err := rtx.GetNamespace("a")
	require.NoError(t, err)
	fv, err := rtx.Get(freshR, []byte("key-0000"))
	require.NoError(t, err)
	assert.Equal(t, []byte("fresh"), fv)
	require.NoError(t, rtx.Rollback())

	// Deleting the renamed namespace frees its tree pages.
	tx4, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx4.DeleteNamespace("b"))
	require.NoError(t, tx4.Commit())
	assert.Greater(t, db.pager.header.TotalFreelistPgs, uint32(0),
		"delete after rename must free the tree pages")

	require.NoError(t, db.IntegrityCheck())
}

func TestRenameNamespaceRollback(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("old")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	require.NoError(t, tx.Commit())
	oldRoot := ns.RootPage()

	// Full-tx rollback: the master-table re-key is ordinary page writes the
	// pager reverts — this pins that assumption.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.RenameNamespace("old", "new"))
	require.NoError(t, tx2.Rollback())

	got, err := db.GetNamespace("old")
	require.NoError(t, err)
	assert.Equal(t, oldRoot, got.RootPage())
	_, err = db.GetNamespace("new")
	assert.ErrorIs(t, err, ErrNamespaceNotFound)

	// Savepoint rollback inside a surviving tx.
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	sp, err := tx3.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx3.RenameNamespace("old", "sp-new"))
	require.NoError(t, tx3.RollbackToSavepoint(sp))
	_, err = tx3.GetNamespace("sp-new")
	assert.ErrorIs(t, err, ErrNamespaceNotFound)
	got3, err := tx3.GetNamespace("old")
	require.NoError(t, err)
	assert.Equal(t, oldRoot, got3.RootPage())
	require.NoError(t, tx3.Commit())

	require.NoError(t, db.IntegrityCheck())
}

func TestRenameNamespaceFreelistNeutral(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := range 50 {
		_, err = tx.CreateNamespace(fmt.Sprintf("ns-%02d", i))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	// A rename-heavy sequence must not leak or corrupt freelist accounting
	// (IntegrityCheck validates it); exact counts are not asserted because
	// master-table splits may allocate legitimately.
	for round := range 3 {
		tx, err = db.BeginWrite()
		require.NoError(t, err)
		for i := range 50 {
			old := fmt.Sprintf("ns-%02d", i)
			next := fmt.Sprintf("ns-%02d-r%d", i, round)
			if round > 0 {
				old = fmt.Sprintf("ns-%02d-r%d", i, round-1)
			}
			require.NoError(t, tx.RenameNamespace(old, next))
		}
		require.NoError(t, tx.Commit())
	}

	names, err := db.ListNamespaces()
	require.NoError(t, err)
	assert.Len(t, names, 50)
	require.NoError(t, db.IntegrityCheck())
}
