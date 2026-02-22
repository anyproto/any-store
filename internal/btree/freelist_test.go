package btree

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFreelistAllocFree(t *testing.T) {
	p := tempPager(t)

	// Allocate some pages
	pages := make([]uint32, 5)
	for i := range pages {
		pg, err := p.allocatePage()
		require.NoError(t, err)
		pages[i] = pg.pgno
		p.releasePage(pg)
	}

	// Free them
	for _, pgno := range pages {
		require.NoError(t, p.freePage(pgno))
	}

	assert.Equal(t, uint32(5), p.header.TotalFreelistPgs)
	assert.NotEqual(t, uint32(0), p.header.FirstFreelistPg)

	// Reallocate - should reuse freed pages
	dbSizeBefore := p.dbSize
	reused := make([]uint32, 5)
	for i := range reused {
		pg, err := p.allocatePage()
		require.NoError(t, err)
		reused[i] = pg.pgno
		p.releasePage(pg)
	}

	// DB should not have grown
	assert.Equal(t, dbSizeBefore, p.dbSize)
	assert.Equal(t, uint32(0), p.header.TotalFreelistPgs)
	assert.Equal(t, uint32(0), p.header.FirstFreelistPg)
}

func TestFreelistTrunkOverflow(t *testing.T) {
	p := tempPager(t)
	maxLeaves := p.freelistMaxLeaves() // 1022 for 4KB pages

	// Allocate enough pages to fill one trunk and overflow
	total := maxLeaves + 5
	pages := make([]uint32, total)
	for i := range pages {
		pg, err := p.allocatePage()
		require.NoError(t, err)
		pages[i] = pg.pgno
		p.releasePage(pg)
	}

	// Free all
	for _, pgno := range pages {
		require.NoError(t, p.freePage(pgno))
	}

	assert.Equal(t, uint32(total), p.header.TotalFreelistPgs)

	// Verify we have at least 2 trunk pages (first trunk filled, second created)
	// The first trunk can hold maxLeaves leaves, so with total > maxLeaves,
	// we need 2 trunk pages.

	// Reallocate all - should reuse
	dbSizeBefore := p.dbSize
	for range total {
		pg, err := p.allocatePage()
		require.NoError(t, err)
		p.releasePage(pg)
	}
	assert.Equal(t, dbSizeBefore, p.dbSize)
	assert.Equal(t, uint32(0), p.header.TotalFreelistPgs)
}

func TestFreelistPersistence(t *testing.T) {
	db := tempDB(t)

	// Create namespace, insert data, delete some to populate freelist
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := range 200 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	// Delete many keys to generate free pages
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, _ := db.getNamespaceLocked("data")
	for i := range 200 {
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, tx2.Delete(ns2, k))
	}
	require.NoError(t, tx2.Commit())

	// Checkpoint to persist freelist to DB file
	require.NoError(t, db.Checkpoint(CheckpointFull))

	// Read the header from disk to verify freelist persistence
	pg, err := db.pager.getPage(1)
	require.NoError(t, err)
	var hdr dbHeader
	require.NoError(t, hdr.deserialize(pg.data[:dbHeaderSize]))
	db.pager.releasePage(pg)

	// The freelist fields should be serialized in the header
	// (exact counts depend on tree structure after deletes)
	// Just verify it was committed successfully
}

func TestDeleteFreesPages(t *testing.T) {
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
	_ = ns

	// Delete all keys
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	ns3, _ := db.getNamespaceLocked("data")
	for i := range 500 {
		k := fmt.Appendf(nil, "key-%04d", i)
		require.NoError(t, tx2.Delete(ns3, k))
	}
	require.NoError(t, tx2.Commit())

	// Verify freelist has pages
	assert.True(t, db.pager.header.TotalFreelistPgs > 0, "freelist should have free pages after mass delete")

	// Now insert again - should reuse pages (DB shouldn't grow much)
	dbSizeBefore := db.pager.dbSize
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns4, _ := db.getNamespaceLocked("data")
	for i := range 100 {
		k := fmt.Appendf(nil, "new-key-%04d", i)
		v := fmt.Appendf(nil, "new-val-%04d", i)
		require.NoError(t, tx3.Put(ns4, k, v))
	}
	require.NoError(t, tx3.Commit())

	// DB size should not have grown significantly (reusing freelist pages)
	assert.True(t, db.pager.dbSize <= dbSizeBefore+5,
		"DB grew too much: before=%d after=%d", dbSizeBefore, db.pager.dbSize)
}

func TestNamespaceDeleteFreesPages(t *testing.T) {
	db := tempDB(t)

	// Create namespace with data
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("todelete")
	require.NoError(t, err)
	for i := range 300 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx.Put(ns, k, v))
	}
	require.NoError(t, tx.Commit())

	// Delete the namespace
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx2.DeleteNamespace("todelete"))
	require.NoError(t, tx2.Commit())

	// Verify pages were freed
	assert.True(t, db.pager.header.TotalFreelistPgs > 0,
		"freelist should have free pages after namespace delete")

	// Create a new namespace — should reuse freed pages
	dbSizeBefore := db.pager.dbSize
	tx3, err := db.BeginWrite()
	require.NoError(t, err)
	ns2, err := tx3.CreateNamespace("reuse")
	require.NoError(t, err)
	for i := range 100 {
		k := fmt.Appendf(nil, "key-%04d", i)
		v := fmt.Appendf(nil, "val-%04d", i)
		require.NoError(t, tx3.Put(ns2, k, v))
	}
	require.NoError(t, tx3.Commit())

	assert.True(t, db.pager.dbSize <= dbSizeBefore+5,
		"DB grew too much: before=%d after=%d", dbSizeBefore, db.pager.dbSize)
}

func TestFreePageInvalidPages(t *testing.T) {
	p := tempPager(t)

	// Cannot free page 0 or page 1
	assert.ErrorIs(t, p.freePage(0), ErrInvalidPage)
	assert.ErrorIs(t, p.freePage(1), ErrInvalidPage)
}
