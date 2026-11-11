package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pageHeaderOf returns the header of page pgno as the writer sees it.
func pageHeaderOf(t *testing.T, db *DB, pgno uint32) pageHeader {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	bt := &btree{pager: db.pager, rootPage: pgno, walMaxFrame: tx.walHdr.mxFrame, writable: true}
	pg, err := bt.getPage(pgno)
	require.NoError(t, err)
	defer db.pager.releasePage(pg)
	return pg.header
}

// createNamespaces creates the named namespaces in one transaction.
func createNamespaces(t *testing.T, db *DB, names ...string) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for _, name := range names {
		_, err = tx.CreateNamespace(name)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func requireNamespaces(t *testing.T, db *DB, names ...string) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	for _, name := range names {
		_, err = rtx.GetNamespace(name)
		require.NoError(t, err, "namespace %.8q", name)
	}
}

// longNames returns n distinct namespace names of the given length.
func longNames(n, length int) []string {
	names := make([]string, n)
	for i := range names {
		names[i] = fmt.Sprintf("%02d", i) + strings.Repeat("n", length-2)
	}
	return names
}

// The insert fit check counts the new cell's pointer twice, so the cell that
// fills a root leaf to its last byte is sent to the split path. The child made
// by balance_deeper then holds every cell and balance-shallower must copy it
// back: the root stays a leaf and the child is freed.
func TestRootLeaf_ExactFitCollapsesBack(t *testing.T) {
	// given: a 512-byte page holds 28 cells of 16 bytes exactly (8 + 28*18)
	db := tempDBWithPageSize(t, 512)
	createNamespaces(t, db, "t1")
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	putN(t, db, "t1", 27, 10)
	before := db.pager.balanceNonrootDispatchCount.Load()

	// when
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, 28), make([]byte, 10)))
	require.NoError(t, tx.Commit())

	// then
	assert.Greater(t, db.pager.balanceNonrootDispatchCount.Load(), before, "the last cell must take the split path")
	hdr := pageHeaderOf(t, db, ns.rootPage)
	assert.True(t, hdr.isLeaf(), "the root is a leaf again")
	assert.Equal(t, uint16(28), hdr.cellCount)
	assert.Equal(t, uint32(1), db.pager.header.TotalFreelistPgs, "the child is freed")
	assert.Equal(t, 28, countKeys(t, db, "t1"))
	require.NoError(t, db.IntegrityCheck())
}

// On page 1 the root has dbHeaderSize less room than its child. Four names of
// 93 bytes fill it exactly (108 + 4*101); the child that takes them has exactly
// dbHeaderSize bytes free, so it fits back.
func TestRootLeaf_Page1ExactFitCollapsesBack(t *testing.T) {
	// given
	db := tempDBWithPageSize(t, 512)
	names := longNames(4, 93)
	createNamespaces(t, db, names[:3]...)
	before := db.pager.balanceNonrootDispatchCount.Load()

	// when
	createNamespaces(t, db, names[3])

	// then
	assert.Greater(t, db.pager.balanceNonrootDispatchCount.Load(), before, "the last cell must take the split path")
	hdr := pageHeaderOf(t, db, 1)
	assert.True(t, hdr.isLeaf(), "page 1 is a leaf again")
	assert.Equal(t, uint16(4), hdr.cellCount)
	requireNamespaces(t, db, names...)
	require.NoError(t, db.IntegrityCheck())
}

// Five names of 76 bytes need 528 bytes on page 1 and 428 on its child, which
// then has fewer than dbHeaderSize bytes free: it cannot be copied back, and
// page 1 stays an interior page with no cells and one child.
func TestRootLeaf_Page1ChildDoesNotFitBack(t *testing.T) {
	// given
	db := tempDBWithPageSize(t, 512)
	names := longNames(6, 76)
	createNamespaces(t, db, names[:4]...)

	// when
	createNamespaces(t, db, names[4])

	// then
	hdr := pageHeaderOf(t, db, 1)
	require.True(t, hdr.isInterior(), "page 1 keeps its single child")
	assert.Equal(t, uint16(0), hdr.cellCount)
	child := pageHeaderOf(t, db, hdr.rightChild)
	assert.True(t, child.isLeaf())
	assert.Equal(t, uint16(5), child.cellCount)
	requireNamespaces(t, db, names[:5]...)
	require.NoError(t, db.IntegrityCheck())

	// and the tree keeps working under the zero-cell root
	createNamespaces(t, db, names[5])
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	require.NoError(t, tx.DeleteNamespace(names[0]))
	require.NoError(t, tx.Commit())
	requireNamespaces(t, db, names[1:]...)
	require.NoError(t, db.IntegrityCheck())
}

// An update that grows a cell past its page, on a leaf that has siblings, is
// balanced with them rather than split in two.
func TestUpdateLeafCell_GrowOnNonRootLeafBalances(t *testing.T) {
	// given
	db := tempDB(t)
	createNamespaces(t, db, "t1")
	const n = 300
	putN(t, db, "t1", n, 100)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	rootHdr := pageHeaderOf(t, db, ns.rootPage)
	require.True(t, rootHdr.isInterior(), "the fixture needs more than one leaf")
	before := db.pager.balanceNonrootDispatchCount.Load()
	big := bytes.Repeat([]byte{'w'}, 900)

	// when: neighbouring keys grow until their leaf overflows
	grown := 0
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 100; i < 120 && db.pager.balanceNonrootDispatchCount.Load() == before; i++ {
		require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, uint32(i)), big))
		grown++
	}
	require.NoError(t, tx.Commit())

	// then
	require.Greater(t, db.pager.balanceNonrootDispatchCount.Load(), before, "no update overflowed its leaf")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	for i := 1; i <= n; i++ {
		got, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
		require.NoError(t, err, "key %d", i)
		want := make([]byte, 100)
		if i >= 100 && i < 100+grown {
			want = big
		}
		require.Equal(t, want, got, "key %d", i)
	}
	require.NoError(t, rtx.Rollback())
	require.NoError(t, db.IntegrityCheck())
}

// Every rebuild of the root-leaf split and of a growing update is failed in
// turn. Each failure must leave a transaction that rolls back to the committed
// state with a sound file.
func TestRootLeafPaths_RebuildFailureRollsBack(t *testing.T) {
	errInjected := errors.New("injected rebuild failure")
	for _, tc := range []struct {
		name string
		key  uint32 // 29 is new, 14 exists
	}{
		{name: "insert into a full root leaf", key: 29},
		{name: "update that grows a cell", key: 14},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given: a root leaf of 28 cells that a 200-byte value overflows
			db := tempDBWithPageSize(t, 512)
			createNamespaces(t, db, "t1")
			putN(t, db, "t1", 28, 10)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			key := binary.BigEndian.AppendUint32(nil, tc.key)
			value := bytes.Repeat([]byte{'w'}, 200)

			failures := 0
			for nth := 1; ; nth++ {
				// when: the nth rebuild of this pager's pages fails
				calls := 0
				setRebuildPageFaultHook(func(pg *page) error {
					if pg.cache != db.pager.writerCache {
						return nil
					}
					calls++
					if calls == nth {
						return errInjected
					}
					return nil
				})
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				err = tx.Put(ns, key, value)
				setRebuildPageFaultHook(nil)
				if err == nil {
					require.NoError(t, tx.Commit())
					break
				}

				// then
				require.ErrorIs(t, err, errInjected, "rebuild %d", nth)
				require.NoError(t, tx.Rollback())
				failures++
				require.NoError(t, db.IntegrityCheck(), "after failing rebuild %d", nth)
				require.Equal(t, 28, countKeys(t, db, "t1"), "after failing rebuild %d", nth)
				hdr := pageHeaderOf(t, db, ns.rootPage)
				require.True(t, hdr.isLeaf(), "after failing rebuild %d", nth)
			}
			require.GreaterOrEqual(t, failures, 4, "the root, the child twice and the sibling are each rebuilt")
			require.NoError(t, db.IntegrityCheck())
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			got, err := rtx.Get(ns, key)
			require.NoError(t, err)
			assert.Equal(t, value, got)
			require.NoError(t, rtx.Rollback())
		})
	}
}

// A write descends through interior pages like a read and must refuse a
// corrupt child pointer rather than follow it.
func TestPut_RefusesCorruptChildPointer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		child func(db *DB) uint32
	}{
		{name: "zero", child: func(*DB) uint32 { return 0 }},
		{name: "past the end of the file", child: func(db *DB) uint32 { return db.pager.dbSize.Load() + 100 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			db := tempDBWithPageSize(t, 512)
			createNamespaces(t, db, "t1")
			putN(t, db, "t1", 200, 10)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			root, err := db.pager.getWritablePage(ns.rootPage)
			require.NoError(t, err)
			require.True(t, root.header.isInterior())
			off := int(root.getCellOffset(0))
			binary.BigEndian.PutUint32(root.data[off:], tc.child(db))
			db.pager.releasePage(root)

			// when: the smallest key descends through the first cell
			err = tx.Put(ns, binary.BigEndian.AppendUint32(nil, 0), []byte("v"))

			// then
			require.ErrorIs(t, err, ErrCorrupt)
		})
	}
}

// A balance can replace short parent dividers with long ones. The 2/3 target
// then leaves more on the right than a page holds; the split index must be
// moved until both halves fit.
func TestInteriorSplitPointFitting_LongTail(t *testing.T) {
	// given: 266 eight-byte dividers followed by three of 1000 bytes
	const usable = 4096
	var cells []cellData
	for i := 0; i < 266; i++ {
		cells = append(cells, cellData{leftChild: uint32(i + 2), key: fmt.Appendf(nil, "%08d", i)})
	}
	for i := 0; i < 3; i++ {
		cells = append(cells, cellData{leftChild: uint32(i + 300), key: bytes.Repeat([]byte{'z'}, 1000)})
	}
	size := func(cs []cellData) int {
		total := 12
		for _, c := range cs {
			total += interiorCellSizeWithOverflow(c.key, usable) + 2
		}
		return total
	}
	target := interiorSplitPoint(cells, usable)
	require.Greater(t, size(cells[target+1:]), usable, "the fixture needs a 2/3 split whose right half does not fit")

	// when
	mid := interiorSplitPointFitting(cells, usable, 0)

	// then
	assert.LessOrEqual(t, size(cells[:mid]), usable)
	assert.LessOrEqual(t, size(cells[mid+1:]), usable)

	// and with the left half on page 1
	mid = interiorSplitPointFitting(cells, usable, dbHeaderSize)
	assert.LessOrEqual(t, size(cells[:mid]), usable-dbHeaderSize)
	assert.LessOrEqual(t, size(cells[mid+1:]), usable)
}

// When a gathered sibling cannot be read, every sibling's pin is released,
// not only those up to the one that failed.
func TestBalanceNonroot_PoolFailureReleasesAllSiblings(t *testing.T) {
	// given: a root with three leaves; the first one's first cell is malformed
	db := tempDBWithPageSize(t, 512)
	createNamespaces(t, db, "t1")
	putN(t, db, "t1", 60, 10)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	p := db.pager
	root, err := p.getWritablePage(ns.rootPage)
	require.NoError(t, err)
	require.True(t, root.header.isInterior())
	require.GreaterOrEqual(t, int(root.header.cellCount), 2)
	var children []uint32
	for i := 0; i < 2; i++ {
		off := int(root.getCellOffset(i))
		children = append(children, binary.BigEndian.Uint32(root.data[off:]))
	}
	if root.header.cellCount == 2 {
		children = append(children, root.header.rightChild)
	} else {
		off := int(root.getCellOffset(2))
		children = append(children, binary.BigEndian.Uint32(root.data[off:]))
	}
	first, err := p.getWritablePage(children[0])
	require.NoError(t, err)
	// a key length varint that never ends
	off := int(first.getCellOffset(0))
	for i := 0; i < 9; i++ {
		first.data[off+i] = 0xff
	}
	p.releasePage(first)
	bt := &btree{pager: p, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: true}

	// when: the middle leaf is balanced with its neighbours
	err = bt.balanceNonroot(root, 1, true, nil, injectedCell{})
	p.releasePage(root)

	// then
	require.ErrorIs(t, err, ErrCorrupt)
	for _, pgno := range children {
		requireUnpinned(t, p, pgno)
	}
}
