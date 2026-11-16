package btree

// The header a write transaction works from comes from page 1 at every begin,
// as lockBtree reads page 1 at the start of every SQLite transaction
// (sqlite3BtreeBeginTrans, btree.c:3707 -> lockBtree, btree.c:3278-3298).

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPage1Rule_HeaderFromPage1AtEveryBegin: the header a write transaction
// works from is page 1's, read at every begin as lockBtree reads it at the
// start of every SQLite transaction (btree.c:3707, 3278-3298), not what the
// pager's struct held after the last transaction.
func TestPage1Rule_HeaderFromPage1AtEveryBegin(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, _ := openRestartDB(t, m)
		restartPut(t, db, 0, 10, 1)
		committed := db.pager.header
		db.pager.header.UserVersion = committed.UserVersion + 7
		db.pager.header.TotalFreelistPgs = committed.TotalFreelistPgs + 3
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		got := db.pager.header
		require.NoError(t, tx.Rollback())
		require.Equal(t, committed, got)
	})
}

// TestPage1Rule_RollbackKeepsTheEffectivePageCount: a header whose size is
// not trusted (0) leaves the page count to the pager's count at begin; a
// rollback must go back to that count, not to the header's 0.
func TestPage1Rule_RollbackKeepsTheEffectivePageCount(t *testing.T) {
	forEachRestartMode(t, func(t *testing.T, m restartMode) {
		db, _ := openRestartDB(t, m)
		restartPut(t, db, 0, 50, 1)
		size := db.pager.dbSize.Load()
		// The committed page 1 with an untrusted size, as a release that left
		// the field 0 would write it.
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		pg1, err := db.pager.getWritablePage(1)
		require.NoError(t, err)
		binary.BigEndian.PutUint32(pg1.data[28:32], 0)
		db.pager.releasePage(pg1)
		db.pager.savedHeader.DatabaseSize = 0
		db.pager.header.DatabaseSize = 0
		require.NoError(t, tx.Rollback())
		require.Equal(t, size, db.pager.dbSize.Load(), "the count the transaction began with")
		restartPut(t, db, 50, 60, 2)
		restartCheck(t, db, 0, 50, 1)
		restartCheck(t, db, 50, 60, 2)
		require.NoError(t, db.IntegrityCheck())
	})
}
