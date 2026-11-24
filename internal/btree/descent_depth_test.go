package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The descents that follow child pointers stop at btCursorMaxDepth levels, as
// SQLite's moveToChild does (btree.c:5448, inlined at btree.c:6227). The tests
// here build a tree whose interior pages form a cycle — every page well formed
// on its own — and require each descent to answer ErrCorrupt instead of
// following the cycle forever.

// descentOpTimeout is how long an operation on a cyclic tree may take before
// the test calls it a hang. A bounded descent reads at most btCursorMaxDepth
// pages.
const descentOpTimeout = 3 * time.Second

// returnsWithin runs fn on its own goroutine and returns its error. When fn
// has not returned in descentOpTimeout it panics, which ends the test binary:
// an unbounded descent cannot be stopped, holds its transaction's locks, and
// the write descents among them grow their path by an entry per turn of the
// cycle — gigabytes within seconds — so the run must not go on beside one.
func returnsWithin(t *testing.T, what string, fn func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		return err
	case <-time.After(descentOpTimeout):
		panic(fmt.Sprintf("%s: %s did not return within %v: the descent follows the cycle", t.Name(), what, descentOpTimeout))
	}
}

// descentCloseTimeout bounds Close on its own: it checkpoints the WAL, which
// is disk I/O and not a descent.
const descentCloseTimeout = 2 * time.Minute

// closesWithin closes db and fails the test when Close has not returned in
// descentCloseTimeout: a descent left running holds a lock Close waits for.
func closesWithin(t *testing.T, db *DB) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(descentCloseTimeout):
		panic(fmt.Sprintf("%s: Close did not return within %v", t.Name(), descentCloseTimeout))
	}
}

// openSmallPages opens a new database with 1024-byte pages, so that a tree
// of a few thousand rows has three levels. The page buffer pool is process
// wide and keeps the page size of the last database opened: it is reset
// again when the test ends, for the tests after it that open with the
// default size.
func openSmallPages(t *testing.T, name string) *DB {
	t.Helper()
	resetPageBufferPool()
	t.Cleanup(resetPageBufferPool)
	db, err := Open(filepath.Join(t.TempDir(), name), Options{PageSize: 1024})
	require.NoError(t, err)
	return db
}

func descentKey(i int) []byte {
	return binary.BigEndian.AppendUint32(nil, uint32(i))
}

type descentCycle int

const (
	// cycleToAncestor: a level-2 interior page's first child is the root.
	cycleToAncestor descentCycle = iota
	// cycleSelf: a level-2 interior page's first child is the page itself.
	cycleSelf
	// cycleTwoPages: two level-2 interior pages name each other as first
	// child; the root is not part of the cycle.
	cycleTwoPages
)

func (k descentCycle) String() string {
	switch k {
	case cycleToAncestor:
		return "ancestor"
	case cycleSelf:
		return "self"
	default:
		return "two-pages"
	}
}

// firstChild returns the left child of cell idx of interior page pgno, as the
// writer sees it.
func firstChild(t *testing.T, p *pager, pgno uint32, idx int) uint32 {
	t.Helper()
	pg, err := p.getPage(pgno)
	require.NoError(t, err)
	defer p.releasePage(pg)
	require.True(t, pg.header.isInterior(), "page %d is not interior", pgno)
	require.Greater(t, int(pg.header.cellCount), idx)
	off := int(pg.getCellOffset(idx))
	return binary.BigEndian.Uint32(pg.data[off : off+4])
}

// setFirstChild points the left child of cell 0 of interior page pgno at
// child. Only the pointer changes: the page stays well formed.
func setFirstChild(t *testing.T, p *pager, pgno, child uint32) {
	t.Helper()
	pg, err := p.getWritablePage(pgno)
	require.NoError(t, err)
	defer p.releasePage(pg)
	require.True(t, pg.header.isInterior(), "page %d is not interior", pgno)
	off := int(pg.getCellOffset(0))
	binary.BigEndian.PutUint32(pg.data[off:off+4], child)
}

// openCyclicTree builds a three-level tree in namespace "t", then rewrites
// child pointers so that the descent for the smallest key meets the cycle.
func openCyclicTree(t *testing.T, kind descentCycle) (*DB, *Namespace) {
	t.Helper()
	db := openSmallPages(t, "cycle.db")

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	val := make([]byte, 80)
	for i := 1; i <= 6000; i++ {
		require.NoError(t, tx.Put(ns, descentKey(i), val))
	}
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	p := tx.pager
	root := ns.rootPage
	a := firstChild(t, p, root, 0)
	// a must be interior for the tree to have three levels; firstChild checks.
	_ = firstChild(t, p, a, 0)
	switch kind {
	case cycleToAncestor:
		setFirstChild(t, p, a, root)
	case cycleSelf:
		setFirstChild(t, p, a, a)
	case cycleTwoPages:
		b := firstChild(t, p, root, 1)
		_ = firstChild(t, p, b, 0)
		setFirstChild(t, p, a, b)
		setFirstChild(t, p, b, a)
	}
	require.NoError(t, tx.Commit())
	return db, ns
}

func TestDescentDepthBound_Cycle(t *testing.T) {
	key := descentKey(1) // the smallest key: every descent takes the first child
	read := func(fn func(tx *ReadTx, ns *Namespace) error) func(db *DB, ns *Namespace) error {
		return func(db *DB, ns *Namespace) error {
			tx, err := db.BeginRead()
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			return fn(tx, ns)
		}
	}
	write := func(fn func(tx *WriteTx, ns *Namespace) error) func(db *DB, ns *Namespace) error {
		return func(db *DB, ns *Namespace) error {
			tx, err := db.BeginWrite()
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback() }()
			return fn(tx, ns)
		}
	}
	ops := []struct {
		name string
		run  func(db *DB, ns *Namespace) error
	}{
		{"ReadTx.Get", read(func(tx *ReadTx, ns *Namespace) error {
			_, err := tx.Get(ns, key)
			return err
		})},
		{"ReadTx.Has", read(func(tx *ReadTx, ns *Namespace) error {
			_, err := tx.Has(ns, key)
			return err
		})},
		{"ReadTx.SeekKey", read(func(tx *ReadTx, ns *Namespace) error {
			_, err := tx.SeekKey(ns, key)
			return err
		})},
		{"btree.Get", read(func(tx *ReadTx, ns *Namespace) error {
			bt := &btree{pager: tx.pager, cache: tx.cache, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame}
			_, err := bt.Get(key)
			return err
		})},
		{"WriteTx.Get", write(func(tx *WriteTx, ns *Namespace) error {
			_, err := tx.Get(ns, key)
			return err
		})},
		{"WriteTx.Put", write(func(tx *WriteTx, ns *Namespace) error {
			return tx.Put(ns, key, []byte("v"))
		})},
		{"WriteTx.Put/new-key", write(func(tx *WriteTx, ns *Namespace) error {
			return tx.Put(ns, descentKey(0), []byte("v"))
		})},
		{"WriteTx.Delete", write(func(tx *WriteTx, ns *Namespace) error {
			return tx.Delete(ns, key)
		})},
		{"btree.pathToChild", write(func(tx *WriteTx, ns *Namespace) error {
			bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: true}
			// No page has child 0, so the descent never meets its target.
			_, err := bt.pathToChild(key, 0)
			return err
		})},
		// The cursor and the counting walks were bounded before the point
		// descents were; they stay in the table as the reference behaviour.
		{"Cursor.First", read(func(tx *ReadTx, ns *Namespace) error {
			c := tx.NewCursor(ns)
			defer c.Close()
			return c.First()
		})},
		{"Cursor.Seek", read(func(tx *ReadTx, ns *Namespace) error {
			c := tx.NewCursor(ns)
			defer c.Close()
			return c.Seek(key)
		})},
		{"Cursor.RangeFraction", read(func(tx *ReadTx, ns *Namespace) error {
			c := tx.NewCursor(ns)
			defer c.Close()
			_, err := c.RangeFraction(key, nil)
			return err
		})},
	}
	for _, kind := range []descentCycle{cycleToAncestor, cycleSelf, cycleTwoPages} {
		for _, op := range ops {
			t.Run(fmt.Sprintf("%s/%s", kind, op.name), func(t *testing.T) {
				db, ns := openCyclicTree(t, kind)
				err := returnsWithin(t, op.name, func() error { return op.run(db, ns) })
				require.ErrorIs(t, err, ErrCorrupt)
				// A second operation still gets its lock, and Close returns:
				// the first one left nothing held.
				err = returnsWithin(t, op.name+" again", func() error { return op.run(db, ns) })
				require.ErrorIs(t, err, ErrCorrupt)
				closesWithin(t, db)
			})
		}
	}
}

// TestDescentDepthBound_SeekKeyNextSubtree covers the second half of
// AppendSeekKey: when the leaf the key routes to holds nothing at or after it,
// the lookup goes down the leftmost edge of the next subtree, and that walk
// continues the same root-to-leaf chain.
func TestDescentDepthBound_SeekKeyNextSubtree(t *testing.T) {
	db := openSmallPages(t, "seek.db")
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	val := make([]byte, 80)
	for i := 1; i <= 6000; i++ {
		require.NoError(t, tx.Put(ns, descentKey(i), val))
	}
	// The root's first divider is the boundary between its first two
	// subtrees. Remove the key just below it: a seek for that key routes to
	// the last leaf of the first subtree, finds nothing there, and continues
	// with the leftmost key of the second.
	root := ns.rootPage
	rootPg, err := tx.pager.getPage(root)
	require.NoError(t, err)
	require.True(t, rootPg.header.isInterior())
	divider, _, err := interiorCellKey(rootPg.data, int(rootPg.getCellOffset(0)))
	require.NoError(t, err)
	gap := descentKey(int(binary.BigEndian.Uint32(divider)) - 1)
	tx.pager.releasePage(rootPg)
	b := firstChild(t, tx.pager, root, 1)
	_ = firstChild(t, tx.pager, b, 0) // b is interior: three levels
	require.NoError(t, tx.Delete(ns, gap))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	got, err := rtx.SeekKey(ns, gap)
	require.NoError(t, err)
	require.Greater(t, binary.BigEndian.Uint32(got), binary.BigEndian.Uint32(gap))
	require.NoError(t, rtx.Rollback())

	// The second subtree's leftmost edge now leads back to its own top.
	setFirstChildCommitted(t, db, b, 0, b)

	err = returnsWithin(t, "ReadTx.SeekKey", func() error {
		rtx, err := db.BeginRead()
		if err != nil {
			return err
		}
		defer func() { _ = rtx.Rollback() }()
		_, err = rtx.SeekKey(ns, gap)
		return err
	})
	require.ErrorIs(t, err, ErrCorrupt)
	closesWithin(t, db)
}

// setFirstChildCommitted points the left child of cell idx of interior page
// pgno at child, in a transaction of its own.
func setFirstChildCommitted(t *testing.T, db *DB, pgno uint32, idx int, child uint32) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	pg, err := tx.pager.getWritablePage(pgno)
	require.NoError(t, err)
	require.True(t, pg.header.isInterior())
	require.Greater(t, int(pg.header.cellCount), idx)
	off := int(pg.getCellOffset(idx))
	binary.BigEndian.PutUint32(pg.data[off:off+4], child)
	tx.pager.releasePage(pg)
	require.NoError(t, tx.Commit())
}

// TestDescentDepthBound_MasterTreeCycle: the namespace lookup descends the
// master tree (root page 1) and stops at the same bound.
func TestDescentDepthBound_MasterTreeCycle(t *testing.T) {
	db := openSmallPages(t, "master.db")

	name := func(i int) string { return fmt.Sprintf("namespace-%04d-with-a-long-name-to-fill-the-page", i) }
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 200; i++ {
		_, err := tx.CreateNamespace(name(i))
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())

	// Page 1 is now an interior root; make its first child page 1 itself.
	setFirstChildCommitted(t, db, 1, 0, 1)

	err = returnsWithin(t, "ReadTx.GetNamespace", func() error {
		tx, err := db.BeginRead()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		_, err = tx.GetNamespace(name(0))
		return err
	})
	require.ErrorIs(t, err, ErrCorrupt)

	err = returnsWithin(t, "WriteTx.GetNamespace", func() error {
		tx, err := db.BeginWrite()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		_, err = tx.GetNamespace(name(0))
		return err
	})
	require.ErrorIs(t, err, ErrCorrupt)

	err = returnsWithin(t, "WriteTx.CreateNamespace", func() error {
		tx, err := db.BeginWrite()
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback() }()
		_, err = tx.CreateNamespace("a-name-before-every-other")
		return err
	})
	require.ErrorIs(t, err, ErrCorrupt)
	closesWithin(t, db)
}

// TestDescentDepthBound_AcceptsMaxDepth pins the off-by-one: a chain of
// btCursorMaxDepth pages (the root, btCursorMaxDepth-2 interior pages below it
// and a leaf) is as deep as SQLite's cursor goes (iPage reaches
// BTCURSOR_MAX_DEPTH-1) and is read; one more level is ErrCorrupt.
func TestDescentDepthBound_AcceptsMaxDepth(t *testing.T) {
	build := func(t *testing.T, levels int) (*DB, *Namespace) {
		db := openSmallPages(t, "deep.db")
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t")
		require.NoError(t, err)
		p := tx.pager
		bt := &btree{pager: p, rootPage: ns.rootPage, writable: true}

		// Bottom up: the leaf, then a chain of one-cell interior pages, the
		// last of which is written over the namespace root.
		leaf, err := p.allocatePage()
		require.NoError(t, err)
		require.NoError(t, bt.rebuildLeafPage(leaf, []cellData{{key: []byte("b"), value: []byte("1")}}))
		child, right := leaf.pgno, uint32(0)
		p.releasePage(leaf)
		for l := levels - 1; l >= 1; l-- {
			// The right child holds the keys >= "m"; give each level its own
			// empty leaf so the page is a sound interior page.
			rl, err := p.allocatePage()
			require.NoError(t, err)
			require.NoError(t, bt.rebuildLeafPage(rl, nil))
			right = rl.pgno
			p.releasePage(rl)

			var pg *page
			if l == 1 {
				pg, err = p.getWritablePage(ns.rootPage)
			} else {
				pg, err = p.allocatePage()
			}
			require.NoError(t, err)
			require.NoError(t, bt.rebuildInteriorPage(pg, []cellData{{leftChild: child, key: []byte("m")}}, right))
			child = pg.pgno
			p.releasePage(pg)
		}
		require.NoError(t, tx.Commit())
		return db, ns
	}
	ops := func(t *testing.T, db *DB, ns *Namespace) map[string]error {
		res := map[string]error{}
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		_, res["ReadTx.Get"] = rtx.Get(ns, []byte("b"))
		_, res["ReadTx.Has"] = rtx.Has(ns, []byte("b"))
		_, res["ReadTx.SeekKey"] = rtx.SeekKey(ns, []byte("a"))
		c := rtx.NewCursor(ns)
		res["Cursor.Seek"] = c.Seek([]byte("b"))
		c.Close()
		require.NoError(t, rtx.Rollback())

		wtx, err := db.BeginWrite()
		require.NoError(t, err)
		res["WriteTx.Put"] = wtx.Put(ns, []byte("b"), []byte("2"))
		res["WriteTx.Delete"] = wtx.Delete(ns, []byte("a"))
		require.NoError(t, wtx.Rollback())
		return res
	}

	t.Run("at the bound", func(t *testing.T) {
		db, ns := build(t, btCursorMaxDepth)
		defer db.Close()
		for name, err := range ops(t, db, ns) {
			if name == "WriteTx.Delete" {
				require.ErrorIs(t, err, ErrKeyNotFound, name)
				continue
			}
			require.NoError(t, err, name)
		}
	})
	t.Run("one level deeper", func(t *testing.T) {
		db, ns := build(t, btCursorMaxDepth+1)
		defer db.Close()
		for name, err := range ops(t, db, ns) {
			require.ErrorIs(t, err, ErrCorrupt, name)
		}
	})
}

// descentChain builds, bottom up, a chain of n one-cell interior pages over a
// leaf that holds key, and returns the top page: each interior page has the
// page below as the left child of its one divider and an empty leaf as its
// right child, so the chain is n+1 pages long and a descent for key, or down
// its leftmost edge, visits all of them.
func descentChain(t *testing.T, bt *btree, n int, key, divider []byte) uint32 {
	t.Helper()
	p := bt.pager
	leaf, err := p.allocatePage()
	require.NoError(t, err)
	require.NoError(t, bt.rebuildLeafPage(leaf, []cellData{{key: key, value: []byte("1")}}))
	child := leaf.pgno
	p.releasePage(leaf)
	for i := 0; i < n; i++ {
		child = descentInterior(t, bt, 0, []cellData{{leftChild: child, key: divider}})
	}
	return child
}

// descentInterior writes an interior page with the given cells and an empty
// leaf as its right child, on page pgno or on a new page when pgno is 0, and
// returns its page number.
func descentInterior(t *testing.T, bt *btree, pgno uint32, cells []cellData) uint32 {
	t.Helper()
	p := bt.pager
	right, err := p.allocatePage()
	require.NoError(t, err)
	require.NoError(t, bt.rebuildLeafPage(right, nil))
	rightPgno := right.pgno
	p.releasePage(right)
	var pg *page
	if pgno == 0 {
		pg, err = p.allocatePage()
	} else {
		pg, err = p.getWritablePage(pgno)
	}
	require.NoError(t, err)
	require.NoError(t, bt.rebuildInteriorPage(pg, cells, rightPgno))
	pgno = pg.pgno
	p.releasePage(pg)
	return pgno
}

// TestDescentDepthBound_SeekKeyNextSubtreeDepth pins the count the seek
// carries into the next subtree. The page it restarts at is not the root, so
// the walk down the sibling starts with child pointers already followed: a
// successor on page 20 of its root-to-leaf chain is returned, one on page 21
// is ErrCorrupt. A walk that started its count over at the restart page would
// read the second.
func TestDescentDepthBound_SeekKeyNextSubtreeDepth(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pages int // root-to-leaf pages of the successor's path
		ok    bool
	}{
		{"successor on page 20", btCursorMaxDepth, true},
		{"successor on page 21", btCursorMaxDepth + 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openSmallPages(t, "seekdepth.db")
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.CreateNamespace("t")
			require.NoError(t, err)
			bt := &btree{pager: tx.pager, rootPage: ns.rootPage, writable: true}

			// root -> a -> { l0: "a" | s: the chain down to "d" | empty }.
			// A seek for "b" routes root -> a -> l0, finds nothing at or
			// after it there, and goes on from a (one child pointer below
			// the root) into s. The successor's path is root, a, then the
			// chain: pages-3 interior pages and the leaf.
			l0, err := tx.pager.allocatePage()
			require.NoError(t, err)
			require.NoError(t, bt.rebuildLeafPage(l0, []cellData{{key: []byte("a"), value: []byte("1")}}))
			l0Pgno := l0.pgno
			tx.pager.releasePage(l0)
			s := descentChain(t, bt, tc.pages-3, []byte("d"), []byte("g"))
			a := descentInterior(t, bt, 0, []cellData{
				{leftChild: l0Pgno, key: []byte("c")},
				{leftChild: s, key: []byte("h")},
			})
			descentInterior(t, bt, ns.rootPage, []cellData{{leftChild: a, key: []byte("m")}})
			require.NoError(t, tx.Commit())

			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			got, err := rtx.SeekKey(ns, []byte("b"))
			if tc.ok {
				require.NoError(t, err)
				require.Equal(t, "d", string(got))
				// The same leaf by a point lookup and by a cursor.
				_, err = rtx.Get(ns, []byte("d"))
				require.NoError(t, err)
				c := rtx.NewCursor(ns)
				defer c.Close()
				require.NoError(t, c.Seek([]byte("b")))
				k, err := c.Key()
				require.NoError(t, err)
				require.Equal(t, "d", string(k))
			} else {
				require.ErrorIs(t, err, ErrCorrupt)
				_, err = rtx.Get(ns, []byte("d"))
				require.ErrorIs(t, err, ErrCorrupt)
				c := rtx.NewCursor(ns)
				defer c.Close()
				require.ErrorIs(t, c.Seek([]byte("b")), ErrCorrupt)
			}
			require.NoError(t, rtx.Rollback())
			closesWithin(t, db)
		})
	}
}

// TestDescentDepthBound_RootSplitAtMaxDepth: an insert whose split reaches
// the root makes the tree a level deeper with no look at its height, as
// SQLite's balance calls balance_deeper (btree.c:9134-9145). On a tree of
// btCursorMaxDepth-1 levels that gives the deepest tree the descents read. On
// one of btCursorMaxDepth levels the Put returns nil and leaves leaves no
// descent reaches: the next lookup of a key on them is ErrCorrupt, as
// moveToChild's test makes it in SQLite (btree.c:5448-5450).
func TestDescentDepthBound_RootSplitAtMaxDepth(t *testing.T) {
	// 200-byte keys: four cells fill a 1024-byte page, leaf or interior.
	bigKey := func(class, i byte) []byte {
		k := bytes.Repeat([]byte{class}, 200)
		k[1] = i
		return k
	}
	// build makes a path of the given number of pages on which every page
	// is full, and so are the two right siblings of each page below the
	// root: a balance spreads cells over three siblings before it adds a
	// page and a divider. The other children are empty leaves.
	build := func(t *testing.T, levels int) (*DB, *Namespace, [][]byte) {
		db := openSmallPages(t, "rootsplit.db")
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("t")
		require.NoError(t, err)
		p := tx.pager
		bt := &btree{pager: p, rootPage: ns.rootPage, writable: true}

		var keys [][]byte
		leaf := func(class byte, idx ...byte) uint32 {
			pg, err := p.allocatePage()
			require.NoError(t, err)
			var cells []cellData
			for _, i := range idx {
				cells = append(cells, cellData{key: bigKey(class, i)})
				keys = append(keys, bigKey(class, i))
			}
			require.NoError(t, bt.rebuildLeafPage(pg, cells))
			pgno := pg.pgno
			p.releasePage(pg)
			return pgno
		}
		l0 := leaf(0x10, 0, 2, 4, 6)
		l1 := leaf(0x11, 0, 1, 2, 3)
		l2 := leaf(0x12, 0, 1, 2, 3)
		empty := func() uint32 { return leaf(0) }
		// The leaves' parent: interior level levels-1, counted from the root.
		child := descentInterior(t, bt, 0, []cellData{
			{leftChild: l0, key: bigKey(0x11, 0)},
			{leftChild: l1, key: bigKey(0x12, 0)},
			{leftChild: l2, key: bigKey(0x13, 0)},
			{leftChild: empty(), key: bigKey(0x13, 1)},
		})
		// The interior pages above it, up to the root: a deeper page has
		// the smaller dividers. The second and third child of each are full
		// interior pages, the siblings of the page on the path.
		for l := levels - 2; l >= 1; l-- {
			class := byte(0x20 + levels - l)
			sibling := func(i byte) uint32 {
				var cells []cellData
				for j := byte(1); j <= 4; j++ {
					k := bigKey(class, i)
					k[3] = 0xf0 + j // above the divider (class, i), below the next
					cells = append(cells, cellData{leftChild: empty(), key: k})
				}
				return descentInterior(t, bt, 0, cells)
			}
			pgno := uint32(0)
			if l == 1 {
				pgno = ns.rootPage
			}
			child = descentInterior(t, bt, pgno, []cellData{
				{leftChild: child, key: bigKey(class, 0)},
				{leftChild: sibling(0), key: bigKey(class, 1)},
				{leftChild: sibling(1), key: bigKey(class, 2)},
				{leftChild: empty(), key: bigKey(class, 3)},
			})
		}
		require.NoError(t, tx.Commit())
		return db, ns, keys
	}
	readAll := func(t *testing.T, db *DB, ns *Namespace, keys [][]byte) {
		t.Helper()
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		for _, k := range keys {
			_, err := rtx.Get(ns, k)
			require.NoError(t, err, "key %x %x", k[0], k[1])
		}
	}
	newKey := bigKey(0x10, 1)

	t.Run("to the bound", func(t *testing.T) {
		db, ns, keys := build(t, btCursorMaxDepth-1)
		readAll(t, db, ns, keys)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		rootBefore := firstChild(t, tx.pager, ns.rootPage, 0)
		require.NoError(t, tx.Put(ns, newKey, nil))
		require.NoError(t, tx.Commit())
		// The root was split: its old content is one level down.
		require.NotEqual(t, rootBefore, firstChild(t, db.pager, ns.rootPage, 0), "the insert did not split the root")
		readAll(t, db, ns, append(keys, newKey))
		closesWithin(t, db)
	})
	t.Run("past the bound", func(t *testing.T) {
		db, ns, keys := build(t, btCursorMaxDepth)
		readAll(t, db, ns, keys)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		rootBefore := firstChild(t, tx.pager, ns.rootPage, 0)
		require.NoError(t, tx.Put(ns, newKey, nil))
		require.NoError(t, tx.Commit())
		require.NotEqual(t, rootBefore, firstChild(t, db.pager, ns.rootPage, 0), "the insert did not split the root")
		// Every key sat on a leaf at level btCursorMaxDepth; the split moved
		// each one level down, one step past the bound.
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		for _, k := range append(keys, newKey) {
			_, err := rtx.Get(ns, k)
			require.ErrorIs(t, err, ErrCorrupt, "key %x %x", k[0], k[1])
		}
		_, err = rtx.SeekKey(ns, newKey)
		require.ErrorIs(t, err, ErrCorrupt)
		c := rtx.NewCursor(ns)
		require.ErrorIs(t, c.Seek(newKey), ErrCorrupt)
		c.Close()
		require.NoError(t, rtx.Rollback())
		closesWithin(t, db)
	})
}
