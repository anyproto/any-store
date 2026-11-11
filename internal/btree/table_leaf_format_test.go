package btree

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// overflowPageOf returns the first overflow page of key's cell, 0 when the
// whole cell is on its leaf.
func overflowPageOf(t *testing.T, db *DB, ns *Namespace, key []byte) uint32 {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { require.NoError(t, tx.Rollback()) }()
	bt := &btree{pager: db.pager, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: true}
	pg, err := bt.getPage(bt.rootPage)
	require.NoError(t, err)
	for pg.header.isInterior() {
		child, _, err := bt.searchInterior(pg, key)
		require.NoError(t, err)
		db.pager.releasePage(pg)
		pg, err = bt.descendChild(child)
		require.NoError(t, err)
	}
	defer db.pager.releasePage(pg)
	idx, found, err := bt.searchLeaf(pg, key)
	require.NoError(t, err)
	require.True(t, found, "key not found")
	cell, _, err := parseLeafCellWithSize(pg.data, int(pg.getCellOffset(idx)), bt.leafLimits())
	require.NoError(t, err)
	return cell.overflowPg
}

func u32key(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) }

// filled returns n bytes that differ by seed, so a misplaced value is noticed.
func filled(n, seed int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(seed + i)
	}
	return b
}

func TestTableLeafFormat_NewFile(t *testing.T) {
	skipInLegacyFormatSuite(t)
	// given
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)

	// when: several commits, each rewriting page 1
	for i := 0; i < 3; i++ {
		createNamespaces(t, db, fmt.Sprintf("ns%d", i))
	}
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())

	// then
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "any-store v2 fmt", string(data[:16]))
	assert.Equal(t, uint32(6), binary.BigEndian.Uint32(data[44:48]))
	assert.Equal(t, []byte{2, 2}, data[18:20], "write and read version")

	db, err = testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()
	lim := db.pager.leafLimits()
	assert.Equal(t, db.pager.usableSize()-35, lim.maxLocal)
	assert.Equal(t, minLocalPayload(db.pager.usableSize()), lim.minLocal)
}

// A schema format 5 file keeps its magic, its format and the index rule on its
// leaves however long this binary writes to it, new namespaces included.
func TestLegacyFormat_KeptAcrossWrites(t *testing.T) {
	// given
	useLegacyFormat(t)
	path := filepath.Join(t.TempDir(), "t.db")
	db, err := testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	createNamespaces(t, db, "old")
	require.NoError(t, db.Close())
	useCurrentFormat(t) // the file decides from here on

	// when
	db, err = testOpen(t, path, DefaultOptions())
	require.NoError(t, err)
	createNamespaces(t, db, "new")
	value := filled(2000, 1)
	for _, name := range []string{"old", "new"} {
		ns, err := db.getNamespaceLocked(name)
		require.NoError(t, err)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, []byte("k"), value))
		require.NoError(t, tx.Commit())
	}

	// then
	assert.Equal(t, indexLimits(db.pager.usableSize()), db.pager.leafLimits())
	for _, name := range []string{"old", "new"} {
		ns, err := db.getNamespaceLocked(name)
		require.NoError(t, err)
		assert.NotZero(t, overflowPageOf(t, db, ns, []byte("k")), "a 2000-byte value overflows under the index rule (%s)", name)
	}
	require.NoError(t, db.IntegrityCheck())
	require.NoError(t, db.Checkpoint(CheckpointFull))
	require.NoError(t, db.Close())
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, "any-store v2\x00\x00\x00\x00", string(data[:16]))
	assert.Equal(t, uint32(5), binary.BigEndian.Uint32(data[44:48]))
}

// Payloads on each side of the table-leaf limit: where the cell lives, and
// that every operation returns it whole.
func TestTableLeafFormat_PayloadBoundaries(t *testing.T) {
	skipInLegacyFormatSuite(t)
	db := tempDB(t)
	createNamespaces(t, db, "t1")
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	lim := db.pager.leafLimits()
	const keyLen = 4
	oldMax := maxLocalPayload(db.pager.usableSize())

	sizes := []struct {
		payload int
		local   bool
	}{
		{oldMax, true},
		{oldMax + 1, true},
		{2000, true},
		{lim.maxLocal - 1, true},
		{lim.maxLocal, true},
		{lim.maxLocal + 1, false},
		{5000, false},
		{3 * db.pager.usableSize(), false},
	}
	put := func(i, payload int) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, u32key(i), filled(payload-keyLen, i)))
		require.NoError(t, tx.Commit())
	}
	get := func(i int) []byte {
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { require.NoError(t, rtx.Rollback()) }()
		v, err := rtx.Get(ns, u32key(i))
		require.NoError(t, err)
		return bytes.Clone(v)
	}

	// insert
	for i, s := range sizes {
		put(i, s.payload)
		assert.Equal(t, s.local, overflowPageOf(t, db, ns, u32key(i)) == 0, "payload %d", s.payload)
		assert.Equal(t, filled(s.payload-keyLen, i), get(i), "payload %d", s.payload)
	}
	require.NoError(t, db.IntegrityCheck())

	// cursor, both ways
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	n := 0
	for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
		v, verr := cur.Value()
		require.NoError(t, verr)
		assert.Equal(t, filled(sizes[n].payload-keyLen, n), v)
		n++
	}
	assert.Equal(t, len(sizes), n)
	for err := cur.Last(); err == nil && cur.Valid(); err = cur.Previous() {
		n--
		v, verr := cur.Value()
		require.NoError(t, verr)
		assert.Equal(t, filled(sizes[n].payload-keyLen, n), v)
	}
	assert.Zero(t, n)
	cur.Close()
	require.NoError(t, rtx.Rollback())

	// update: each cell takes the next size, then the previous one (grow and shrink)
	for round, shift := range []int{1, len(sizes) - 1} {
		for i := range sizes {
			s := sizes[(i+shift)%len(sizes)]
			put(i, s.payload)
			assert.Equal(t, s.local, overflowPageOf(t, db, ns, u32key(i)) == 0, "round %d payload %d", round, s.payload)
			assert.Equal(t, filled(s.payload-keyLen, i), get(i))
		}
		require.NoError(t, db.IntegrityCheck())
	}

	// delete
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := range sizes {
		require.NoError(t, tx.Delete(ns, u32key(i)))
	}
	require.NoError(t, tx.Commit())
	assert.Zero(t, countKeys(t, db, "t1"))
	require.NoError(t, db.IntegrityCheck())
}

// Cells of nearly a page each: a leaf holds one, the root fills and splits,
// inserts land between full leaves, deletes merge one-cell leaves.
func TestTableLeafFormat_PageSizedCells(t *testing.T) {
	skipInLegacyFormatSuite(t)
	for _, pageSize := range []uint32{512, 4096} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			db := tempDBWithPageSize(t, pageSize)
			createNamespaces(t, db, "t1")
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			big := db.pager.leafLimits().maxLocal - 4 // the largest value that stays on the page
			const n = 40

			// even keys first, then odd keys between them
			for _, start := range []int{0, 1} {
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				for i := start; i < n; i += 2 {
					require.NoError(t, tx.Put(ns, u32key(i), filled(big, i)))
				}
				require.NoError(t, tx.Commit())
				require.NoError(t, db.IntegrityCheck())
			}
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				v, err := rtx.Get(ns, u32key(i))
				require.NoError(t, err)
				require.Equal(t, filled(big, i), v, "key %d", i)
			}
			require.NoError(t, rtx.Rollback())
			for i := 0; i < n; i++ {
				require.Zero(t, overflowPageOf(t, db, ns, u32key(i)), "key %d", i)
			}

			// delete every other cell, then the rest
			for _, start := range []int{1, 0} {
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				for i := start; i < n; i += 2 {
					require.NoError(t, tx.Delete(ns, u32key(i)))
				}
				require.NoError(t, tx.Commit())
				require.NoError(t, db.IntegrityCheck())
			}
			assert.Zero(t, countKeys(t, db, "t1"))
		})
	}
}

// A cell of nearly a page inserted between two small ones that share a leaf
// with it in key order: the three cannot share two pages either side of it.
func TestTableLeafFormat_LargeCellBetweenNeighbours(t *testing.T) {
	skipInLegacyFormatSuite(t)
	for _, grow := range []bool{false, true} {
		name := "insert"
		if grow {
			name = "update"
		}
		t.Run(name, func(t *testing.T) {
			// given: a root leaf nearly full of small cells
			db := tempDBWithPageSize(t, 512)
			createNamespaces(t, db, "t1")
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			const n = 8
			for i := 0; i < n; i++ {
				if i == n/2 && !grow {
					continue
				}
				require.NoError(t, tx.Put(ns, u32key(i), filled(50, i)))
			}
			require.NoError(t, tx.Commit())
			big := filled(db.pager.leafLimits().maxLocal-4, 99)

			// when
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			require.NoError(t, tx.Put(ns, u32key(n/2), big))
			require.NoError(t, tx.Commit())

			// then
			require.NoError(t, db.IntegrityCheck())
			assert.Zero(t, overflowPageOf(t, db, ns, u32key(n/2)))
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			for i := 0; i < n; i++ {
				v, err := rtx.Get(ns, u32key(i))
				require.NoError(t, err)
				want := filled(50, i)
				if i == n/2 {
					want = big
				}
				require.Equal(t, want, v, "key %d", i)
			}
			require.NoError(t, rtx.Rollback())
		})
	}
}

// The catalog's root is page 1, which has dbHeaderSize less room: a cell that
// fits any other page does not fit it even when it is empty.
func TestTableLeafFormat_Page1Root(t *testing.T) {
	skipInLegacyFormatSuite(t)
	db := tempDB(t)
	// 4 bytes of root page number follow the name in the catalog cell
	longest := db.pager.leafLimits().maxLocal - 4
	names := []string{
		"a" + strings.Repeat("x", longest-1),
		"b" + strings.Repeat("x", longest-1),
		"c" + strings.Repeat("x", 2000),
		"d",
	}

	// when
	createNamespaces(t, db, names...)

	// then
	requireNamespaces(t, db, names...)
	require.NoError(t, db.IntegrityCheck())
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for _, name := range names[:3] {
		require.NoError(t, tx.DeleteNamespace(name))
	}
	require.NoError(t, tx.Commit())
	requireNamespaces(t, db, "d")
	require.NoError(t, db.IntegrityCheck())
}

func TestTableLeafFormat_SavepointRollback(t *testing.T) {
	skipInLegacyFormatSuite(t)
	db := tempDB(t)
	createNamespaces(t, db, "t1")
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	big := db.pager.leafLimits().maxLocal - 4
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, tx.Put(ns, u32key(i), filled(big, i)))
	}
	sp, err := tx.Savepoint()
	require.NoError(t, err)

	// when: grow, shrink, add and remove cells, then undo all of it
	for i := 0; i < 10; i += 2 {
		require.NoError(t, tx.Put(ns, u32key(i), filled(2*big, i+1)))
	}
	for i := 1; i < 10; i += 4 {
		require.NoError(t, tx.Put(ns, u32key(i), filled(10, i+1)))
	}
	require.NoError(t, tx.Delete(ns, u32key(3)))
	for i := 10; i < 20; i++ {
		require.NoError(t, tx.Put(ns, u32key(i), filled(big, i)))
	}
	require.NoError(t, tx.RollbackToSavepoint(sp))
	require.NoError(t, tx.Commit())

	// then
	require.NoError(t, db.IntegrityCheck())
	assert.Equal(t, 10, countKeys(t, db, "t1"))
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		v, err := rtx.Get(ns, u32key(i))
		require.NoError(t, err)
		require.Equal(t, filled(big, i), v, "key %d", i)
	}
	require.NoError(t, rtx.Rollback())
}

// Random puts, updates and deletes checked against a map. Keys are short or
// index-shaped (long key, one-byte value); values run from empty to several
// pages. Runs on both formats' rules through the suite switch.
func TestLeafCells_RandomMutationsAgainstModel(t *testing.T) {
	for _, pageSize := range []uint32{512, 4096} {
		t.Run(fmt.Sprint(pageSize), func(t *testing.T) {
			db := tempDBWithPageSize(t, pageSize)
			createNamespaces(t, db, "t1")
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			rng := rand.New(rand.NewSource(int64(pageSize)))
			usable := db.pager.usableSize()
			model := map[string][]byte{}
			randKey := func() []byte {
				id := rng.Intn(300)
				if rng.Intn(4) == 0 {
					// index-shaped: a long key that differs only at its end
					return append(bytes.Repeat([]byte{'k'}, 1+rng.Intn(usable)), u32key(id)...)
				}
				return u32key(id)
			}
			randValue := func(key []byte) []byte {
				if len(key) > 4 {
					return []byte{1}
				}
				switch rng.Intn(4) {
				case 0:
					return filled(rng.Intn(60), rng.Int())
				case 1:
					return filled(rng.Intn(usable), rng.Int())
				case 2:
					return filled(usable-40+rng.Intn(20), rng.Int()) // around the table-leaf limit
				default:
					return filled(rng.Intn(3*usable), rng.Int())
				}
			}
			var keys []string
			for round := 0; round < 30; round++ {
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				for op := 0; op < 40; op++ {
					if len(keys) > 0 && rng.Intn(3) == 0 {
						k := keys[rng.Intn(len(keys))]
						if _, ok := model[k]; ok {
							require.NoError(t, tx.Delete(ns, []byte(k)))
							delete(model, k)
						}
						continue
					}
					k := randKey()
					if len(keys) > 0 && rng.Intn(2) == 0 {
						k = []byte(keys[rng.Intn(len(keys))]) // update or re-insert
					}
					v := randValue(k)
					require.NoError(t, tx.Put(ns, k, v))
					if _, ok := model[string(k)]; !ok {
						keys = append(keys, string(k))
					}
					model[string(k)] = v
				}
				require.NoError(t, tx.Commit())
				require.NoError(t, db.IntegrityCheck(), "round %d", round)

				want := make([]string, 0, len(model))
				for k := range model {
					want = append(want, k)
				}
				sort.Strings(want)
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				cur := rtx.NewCursor(ns)
				i := 0
				for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
					k, kerr := cur.Key()
					require.NoError(t, kerr)
					v, verr := cur.Value()
					require.NoError(t, verr)
					require.Less(t, i, len(want), "round %d: more keys than the model", round)
					require.Equal(t, want[i], string(k), "round %d", round)
					require.Equal(t, model[want[i]], v, "round %d key %d", round, i)
					i++
				}
				require.Equal(t, len(want), i, "round %d", round)
				cur.Close()
				require.NoError(t, rtx.Rollback())
			}
		})
	}
}

// Backing a schema format 5 database up into a newly created file leaves a
// schema format 5 file: page 1 is the source's, and the handle follows it.
func TestBackup_LegacySourceIntoNewFile(t *testing.T) {
	// given
	useLegacyFormat(t)
	dir := t.TempDir()
	src, err := testOpen(t, filepath.Join(dir, "src.db"), DefaultOptions())
	require.NoError(t, err)
	defer func() { require.NoError(t, src.Close()) }()
	createNamespaces(t, src, "t1")
	ns, err := src.getNamespaceLocked("t1")
	require.NoError(t, err)
	tx, err := src.BeginWrite()
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, tx.Put(ns, u32key(i), filled(2000, i)))
	}
	require.NoError(t, tx.Commit())
	useCurrentFormat(t)
	dstPath := filepath.Join(dir, "dst.db")
	dst, err := Open(dstPath, DefaultOptions())
	require.NoError(t, err)
	require.True(t, dst.pager.tableLeaf.Load(), "the destination starts as a schema format 6 file")

	// when
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
	require.NoError(t, b.Finish())

	// then: the open handle reads the copy under the source's rule
	assert.False(t, dst.pager.tableLeaf.Load())
	require.NoError(t, dst.IntegrityCheck())
	dns, err := dst.getNamespaceLocked("t1")
	require.NoError(t, err)
	rtx, err := dst.BeginRead()
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		v, err := rtx.Get(dns, u32key(i))
		require.NoError(t, err)
		require.Equal(t, filled(2000, i), v)
	}
	require.NoError(t, rtx.Rollback())
	require.NoError(t, dst.Checkpoint(CheckpointFull))
	require.NoError(t, dst.Close())

	// and so does a fresh open of it
	data, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	assert.Equal(t, "any-store v2\x00\x00\x00\x00", string(data[:16]))
	assert.Equal(t, uint32(5), binary.BigEndian.Uint32(data[44:48]))
	dst, err = testOpen(t, dstPath, DefaultOptions())
	require.NoError(t, err)
	require.NoError(t, dst.IntegrityCheck())
	require.NoError(t, dst.Close())
}
