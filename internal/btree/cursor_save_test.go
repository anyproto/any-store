package btree

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// cursor_save_test.go — a write transaction's cursor across the writes of
// its transaction: saved before the tree changes under it, restored by its
// next move or read (saveAllCursors / restoreCursorPosition, btree.c:806,
// 896). The expected sequence is SQLite's: Next yields the smallest key
// greater than the key the cursor stood on, Previous the largest key
// smaller, over the keys the tree holds at the time of the move.

func saveKey(i int) []byte { return fmt.Appendf(nil, "key-%06d", i) }

// saveTree is a committed multi-level tree of n keys key-000000..key-n-1
// and a write transaction on it.
func saveTree(t *testing.T, n int) (*DB, *Namespace, *WriteTx) {
	t.Helper()
	db, ns := buildMultiLevelTree(t, n)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	t.Cleanup(func() {
		if !tx.closed {
			_ = tx.Rollback()
		}
	})
	return db, ns, tx
}

func cursorKeyIs(t *testing.T, c *Cursor, want []byte) {
	t.Helper()
	require.True(t, c.Valid())
	k, err := c.Key()
	require.NoError(t, err)
	require.Equal(t, string(want), string(k))
}

// drain walks the cursor forward to the end and returns the keys it yields,
// starting with the one it stands on.
func drain(t *testing.T, c *Cursor) []string {
	t.Helper()
	var got []string
	// A saved cursor is restored by its first move or read: the first key
	// is the one it stands on, or its successor when that one is gone.
	require.NoError(t, c.restored())
	if c.state == cursorSkipNext {
		require.NoError(t, c.Next())
	}
	for c.Valid() {
		k, err := c.Key()
		require.NoError(t, err)
		got = append(got, string(k))
		require.NoError(t, c.Next())
	}
	return got
}

func wantKeys(from, to int) []string {
	var w []string
	for i := from; i < to; i++ {
		w = append(w, string(saveKey(i)))
	}
	return w
}

// A cursor on a single-leaf root survives the insert that splits the root
// in place (splitRoot turns the pinned leaf into an interior page): the
// scan goes on with every key, the inserted ones included.
func TestCursorSave_RootLeafSplitUnderCursor(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 8; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), []byte("v")))
	}
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.First())
	require.Len(t, cur.stack, 1, "the tree must be one root leaf")

	for i := 8; i < 200; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), bytes.Repeat([]byte("v"), 40)))
	}
	require.Equal(t, cursorRequireSeek, cur.state)
	require.Empty(t, cur.stack, "a saved cursor pins no page")

	require.Equal(t, wantKeys(0, 200), drain(t, cur))
	require.NoError(t, cur.Next(), "an exhausted cursor stays exhausted")
	require.False(t, cur.Valid())
}

// The entry the cursor stands on is deleted: Next yields its successor
// without stepping over it, Previous its predecessor.
func TestCursorSave_DeleteCurrent(t *testing.T) {
	t.Run("forward", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Seek(saveKey(1000)))
		require.NoError(t, tx.Delete(ns, saveKey(1000)))
		require.NoError(t, cur.Next())
		cursorKeyIs(t, cur, saveKey(1001))
		require.Equal(t, wantKeys(1001, 2000), drain(t, cur))
	})
	t.Run("reverse", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Seek(saveKey(1000)))
		require.NoError(t, tx.Delete(ns, saveKey(1000)))
		require.NoError(t, cur.Previous())
		cursorKeyIs(t, cur, saveKey(999))
		require.NoError(t, cur.Previous())
		cursorKeyIs(t, cur, saveKey(998))
	})
	t.Run("forwardThenBack", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Seek(saveKey(1000)))
		require.NoError(t, tx.Delete(ns, saveKey(1000)))
		require.NoError(t, cur.Next())
		cursorKeyIs(t, cur, saveKey(1001))
		require.NoError(t, cur.Previous())
		cursorKeyIs(t, cur, saveKey(999))
	})
}

// The last entry, which the cursor stands on, is deleted: the restored
// cursor stands on the new last entry, before its saved key. Previous
// yields it without stepping; Next finds nothing after it.
func TestCursorSave_DeleteLast(t *testing.T) {
	t.Run("previous", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Last())
		require.NoError(t, tx.Delete(ns, saveKey(1999)))
		require.NoError(t, cur.Previous())
		cursorKeyIs(t, cur, saveKey(1998))
		require.NoError(t, cur.Next())
		require.False(t, cur.Valid())
	})
	t.Run("next", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Last())
		require.NoError(t, tx.Delete(ns, saveKey(1999)))
		require.NoError(t, cur.Next())
		require.False(t, cur.Valid())
	})
	t.Run("everyRemaining", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Seek(saveKey(1500)))
		for i := 1500; i < 2000; i++ {
			require.NoError(t, tx.Delete(ns, saveKey(i)))
		}
		require.NoError(t, cur.Next())
		require.False(t, cur.Valid())
		require.NoError(t, cur.Seek(saveKey(1500)))
		require.False(t, cur.Valid())
		require.NoError(t, cur.Last())
		cursorKeyIs(t, cur, saveKey(1499))
	})
	t.Run("emptyTree", func(t *testing.T) {
		db := tempDBWithPageSize(t, 512)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		defer func() { _ = tx.Rollback() }()
		ns, err := tx.CreateNamespace("data")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, saveKey(1), []byte("v")))
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.First())
		require.NoError(t, tx.Delete(ns, saveKey(1)))
		require.NoError(t, cur.Previous())
		require.False(t, cur.Valid())
		require.NoError(t, cur.Next())
		require.False(t, cur.Valid())
	})
}

// A key inserted after the cursor's position is visited; one inserted
// before it is not.
func TestCursorSave_InsertAroundPosition(t *testing.T) {
	_, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	ahead := []byte("key-001000-ahead")
	behind := []byte("key-000999-behind")
	require.NoError(t, tx.Put(ns, ahead, []byte("v")))
	require.NoError(t, tx.Put(ns, behind, []byte("v")))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, ahead)
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1001))
	require.NoError(t, cur.Previous())
	cursorKeyIs(t, cur, ahead)
	require.NoError(t, cur.Previous())
	cursorKeyIs(t, cur, saveKey(1000))
	require.NoError(t, cur.Previous())
	cursorKeyIs(t, cur, behind)
}

// Only the cursors on the written tree are saved; a cursor on another
// tree keeps its pin. A reader's cursor is never on the list.
func TestCursorSave_OnlyTheWrittenTree(t *testing.T) {
	db, ns, tx := saveTree(t, 2000)
	other, err := tx.CreateNamespace("other")
	require.NoError(t, err)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	pinned := cur.stack[len(cur.stack)-1].pg

	require.NoError(t, tx.Put(other, []byte("k"), []byte("v")))
	require.Equal(t, cursorValid, cur.state)
	require.Same(t, pinned, cur.stack[len(cur.stack)-1].pg)

	require.NoError(t, tx.Put(ns, []byte("key-001000-ahead"), []byte("v")))
	require.Equal(t, cursorRequireSeek, cur.state)
	require.Empty(t, cur.stack)
	require.Equal(t, 0, pinned.pinCount)

	require.Same(t, cur, db.pager.cursors)
	require.Nil(t, cur.nextCursor)
	cur.Close()
	require.Nil(t, db.pager.cursors)
	cur.Close()
	require.Nil(t, db.pager.cursors, "a second Close is a no-op")
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	rcur := rtx.NewCursor(ns)
	defer rcur.Close()
	require.NoError(t, rcur.First())
	require.Nil(t, db.pager.cursors, "a reader's cursor is not linked")
}

// The list holds every open writer cursor and loses each at its Close,
// in any order.
func TestCursorSave_ListUnlinksInAnyOrder(t *testing.T) {
	db, ns, tx := saveTree(t, 100)
	a, b, c := tx.NewCursor(ns), tx.NewCursor(ns), tx.NewCursor(ns)
	require.Same(t, c, db.pager.cursors)
	b.Close()
	require.Same(t, c, db.pager.cursors)
	require.Same(t, a, c.nextCursor)
	c.Close()
	require.Same(t, a, db.pager.cursors)
	a.Close()
	require.Nil(t, db.pager.cursors)
	require.NoError(t, tx.Rollback())
}

// A cursor left open as the transaction ends is forgotten with it.
func TestCursorSave_ListResetAtTxEnd(t *testing.T) {
	for _, end := range []string{"commit", "rollback", "emptyCommit"} {
		t.Run(end, func(t *testing.T) {
			db, ns, tx := saveTree(t, 100)
			cur := tx.NewCursor(ns)
			if end != "emptyCommit" {
				require.NoError(t, tx.Put(ns, []byte("z"), []byte("v")))
			}
			require.Same(t, cur, db.pager.cursors)
			if end == "rollback" {
				require.NoError(t, tx.Rollback())
			} else {
				require.NoError(t, tx.Commit())
			}
			require.Nil(t, db.pager.cursors)
		})
	}
}

// Seek, First and Last position afresh: a saved key is dropped.
func TestCursorSave_SeekClearsSaved(t *testing.T) {
	_, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	require.NoError(t, tx.Delete(ns, saveKey(1000)))
	require.Equal(t, cursorRequireSeek, cur.state)
	require.NoError(t, cur.Seek(saveKey(1500)))
	require.Empty(t, cur.savedKey)
	cursorKeyIs(t, cur, saveKey(1500))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1501))

	require.NoError(t, tx.Delete(ns, saveKey(1501)))
	require.NoError(t, cur.First())
	cursorKeyIs(t, cur, saveKey(0))
	require.NoError(t, tx.Delete(ns, saveKey(0)))
	require.NoError(t, cur.Last())
	cursorKeyIs(t, cur, saveKey(1999))
}

// A read of a cursor whose entry is gone reports ErrKeyNotFound and keeps
// the position: the move that follows yields the neighbour.
func TestCursorSave_ReadOfRemovedEntry(t *testing.T) {
	_, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	require.NoError(t, tx.Delete(ns, saveKey(1000)))
	_, err := cur.Key()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.Equal(t, cursorSkipNext, cur.state)
	require.False(t, cur.Valid())
	_, err = cur.Value()
	require.ErrorIs(t, err, ErrKeyNotFound)
	_, err = cur.AppendValue(nil)
	require.ErrorIs(t, err, ErrKeyNotFound)

	// A second save keeps the side the cursor landed on.
	require.NoError(t, tx.Delete(ns, saveKey(1500)))
	require.Equal(t, cursorRequireSeek, cur.state)
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1001))

	// The neighbour it landed on goes too: the move finds the next one.
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1002))
	require.NoError(t, tx.Delete(ns, saveKey(1002)))
	_, err = cur.Key()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, tx.Delete(ns, saveKey(1003)))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1004))

	// A read of a cursor restored onto its own key is the key.
	require.NoError(t, tx.Put(ns, saveKey(3000), []byte("v")))
	k, err := cur.Key()
	require.NoError(t, err)
	require.Equal(t, string(saveKey(1004)), string(k))
	v, err := cur.Value()
	require.NoError(t, err)
	require.Equal(t, "val-001004", string(v))
}

// Skip counts the restored cursor's first step the way Next does.
func TestCursorSave_SkipAfterWrite(t *testing.T) {
	t.Run("forward", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Seek(saveKey(1000)))
		require.NoError(t, tx.Delete(ns, saveKey(1000)))
		require.NoError(t, cur.Skip(1))
		cursorKeyIs(t, cur, saveKey(1001))
		require.NoError(t, tx.Delete(ns, saveKey(1001)))
		require.NoError(t, cur.Skip(3))
		cursorKeyIs(t, cur, saveKey(1004))
		require.NoError(t, tx.Put(ns, saveKey(5000), []byte("v")))
		require.NoError(t, cur.Skip(2))
		cursorKeyIs(t, cur, saveKey(1006))
	})
	t.Run("backward", func(t *testing.T) {
		_, ns, tx := saveTree(t, 2000)
		cur := tx.NewCursor(ns)
		defer cur.Close()
		require.NoError(t, cur.Last())
		require.NoError(t, tx.Delete(ns, saveKey(1999)))
		require.NoError(t, cur.SkipBackward(1))
		cursorKeyIs(t, cur, saveKey(1998))
		require.NoError(t, tx.Delete(ns, saveKey(1998)))
		require.NoError(t, cur.SkipBackward(3))
		cursorKeyIs(t, cur, saveKey(1995))
		require.NoError(t, tx.Delete(ns, saveKey(1000)))
		require.NoError(t, cur.SkipBackward(2))
		cursorKeyIs(t, cur, saveKey(1993))
	})
}

// CountUntil counts from the restored cursor's key: an entry it landed on
// after the key is the first counted, one before it is not.
func TestCursorSave_CountUntilAfterWrite(t *testing.T) {
	_, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	require.NoError(t, tx.Delete(ns, saveKey(1000)))
	n, err := cur.CountUntil(saveKey(1010), true)
	require.NoError(t, err)
	require.Equal(t, 10, n)

	require.NoError(t, cur.Last())
	require.NoError(t, tx.Delete(ns, saveKey(1999)))
	n, err = cur.CountUntil(nil, true)
	require.NoError(t, err)
	require.Equal(t, 0, n)

	require.NoError(t, cur.Seek(saveKey(1500)))
	require.NoError(t, tx.Put(ns, saveKey(5000), []byte("v")))
	n, err = cur.CountUntil(nil, true)
	require.NoError(t, err)
	require.Equal(t, 2000-1500-1+1, n) // 1500..1998 (1999 is deleted above) and the new key
}

// A key that spills into overflow pages is saved whole.
func TestCursorSave_OverflowKey(t *testing.T) {
	db := tempDBWithPageSize(t, 512)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	long := func(i int) []byte {
		return append(fmt.Appendf(nil, "k-%03d-", i), bytes.Repeat([]byte("x"), 1500)...)
	}
	for i := 0; i < 20; i++ {
		require.NoError(t, tx.Put(ns, long(i), []byte("v")))
	}
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(long(10)))
	require.NoError(t, tx.Delete(ns, long(10)))
	require.Equal(t, long(10), cur.savedKey)
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, long(11))
	require.NoError(t, tx.Delete(ns, long(12)))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, long(13))
}

// The rollback of a savepoint saves the open cursors before the pager
// plays it back: a cursor positioned before the savepoint stands on
// nothing it discards and goes on in the restored tree.
func TestCursorSave_RollbackToSavepoint(t *testing.T) {
	_, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	sp, err := tx.Savepoint()
	require.NoError(t, err)
	for i := 2000; i < 2600; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), bytes.Repeat([]byte("v"), 60)))
	}
	require.NoError(t, tx.Delete(ns, saveKey(1000)))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1001))
	require.NoError(t, tx.RollbackToSavepoint(sp))
	require.Equal(t, cursorRequireSeek, cur.state)
	require.NoError(t, tx.ReleaseSavepoint(sp))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1002))
	require.NoError(t, cur.Previous())
	cursorKeyIs(t, cur, saveKey(1001))
	require.NoError(t, cur.Previous())
	cursorKeyIs(t, cur, saveKey(1000)) // the rollback brought the deleted key back
	require.Equal(t, wantKeys(1000, 2000), drain(t, cur))
}

// Dropping the namespace a cursor is open on ends the cursor: its pages
// are released before they are freed, and every use reports the drop.
func TestCursorSave_DropNamespaceTrips(t *testing.T) {
	db, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	pinned := cur.stack[len(cur.stack)-1].pg
	require.NoError(t, tx.DeleteNamespace("data"))
	require.Equal(t, 0, pinned.pinCount)
	require.ErrorIs(t, cur.Next(), ErrNamespaceNotFound)
	require.ErrorIs(t, cur.Previous(), ErrNamespaceNotFound)
	_, err := cur.Key()
	require.ErrorIs(t, err, ErrNamespaceNotFound)
	require.ErrorIs(t, cur.First(), ErrNamespaceNotFound)
	require.ErrorIs(t, cur.Seek(saveKey(1)), ErrNamespaceNotFound)
	require.False(t, cur.Valid())
	cur.Close()
	require.Nil(t, db.pager.cursors)
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

// Random writes between random moves, against the keys the tree holds:
// Next yields the smallest key greater than the key the cursor stood on,
// Previous the largest smaller; an exhausted cursor stays so until it is
// positioned afresh.
func TestCursorSave_Randomized(t *testing.T) {
	seeds := 40
	if testing.Short() {
		seeds = 8
	}
	for seed := 0; seed < seeds; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(seed)))
			db := tempDBWithPageSize(t, 512)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			defer func() { _ = tx.Rollback() }()
			ns, err := tx.CreateNamespace("data")
			require.NoError(t, err)

			keys := make([]string, 0, 4000)
			vals := map[string][]byte{}
			put := func(k string) {
				v := bytes.Repeat([]byte(k[len(k)-1:]), 1+rng.Intn(120))
				require.NoError(t, tx.Put(ns, []byte(k), v))
				vals[k] = v
				if i, found := slices.BinarySearch(keys, k); !found {
					keys = slices.Insert(keys, i, k)
				}
			}
			del := func(k string) {
				require.NoError(t, tx.Delete(ns, []byte(k)))
				if i, found := slices.BinarySearch(keys, k); found {
					keys = slices.Delete(keys, i, i+1)
				}
			}
			randKey := func() string { return fmt.Sprintf("k%05d", rng.Intn(3000)) }
			for i := 0; i < 1500; i++ {
				put(randKey())
			}

			cur := tx.NewCursor(ns)
			defer cur.Close()
			require.NoError(t, cur.First())
			pos := ""          // the key the cursor stood on last
			exhausted := false // Next or Previous found nothing
			check := func() {
				if exhausted {
					require.False(t, cur.Valid())
					return
				}
				k, err := cur.Key()
				require.NoError(t, err)
				require.Equal(t, pos, string(k))
				v, err := cur.Value()
				require.NoError(t, err)
				require.Equal(t, string(vals[pos]), string(v))
			}
			if len(keys) > 0 {
				pos = keys[0]
			} else {
				exhausted = true
			}
			check()
			for step := 0; step < 600; step++ {
				switch r := rng.Intn(10); {
				case r < 3:
					put(randKey())
				case r < 6 && len(keys) > 0:
					if _, have := slices.BinarySearch(keys, pos); have && !exhausted && rng.Intn(3) == 0 {
						del(pos)
					} else {
						del(keys[rng.Intn(len(keys))])
					}
				case r < 8:
					require.NoError(t, cur.Next())
					if exhausted {
						require.False(t, cur.Valid())
						continue
					}
					i, _ := slices.BinarySearch(keys, pos)
					for i < len(keys) && keys[i] <= pos {
						i++
					}
					if i == len(keys) {
						exhausted = true
					} else {
						pos = keys[i]
					}
					check()
				case r < 9:
					require.NoError(t, cur.Previous())
					if exhausted {
						require.False(t, cur.Valid())
						continue
					}
					i, _ := slices.BinarySearch(keys, pos)
					i--
					if i < 0 {
						exhausted = true
					} else {
						pos = keys[i]
					}
					check()
				default:
					k := randKey()
					require.NoError(t, cur.Seek([]byte(k)))
					i, _ := slices.BinarySearch(keys, k)
					if i == len(keys) {
						exhausted = true
					} else {
						exhausted = false
						pos = keys[i]
					}
					check()
				}
			}
			require.NoError(t, tx.Commit())
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// A write whose key or value is a slice of the page a cursor on the same
// tree stands on: the save lets the page go, the write's own descent can
// evict it under a small cache, and the write reads the copy it took
// before.
func TestCursorSave_WriteWithCursorSlices(t *testing.T) {
	for _, cache := range []int{8, 64} {
		t.Run(fmt.Sprintf("cache=%d", cache), func(t *testing.T) {
			resetPageBufferPool()
			db, err := Open(filepath.Join(t.TempDir(), "t.db"), Options{PageSize: 512, CacheSize: cache})
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.CreateNamespace("data")
			require.NoError(t, err)
			other, err := tx.CreateNamespace("other")
			require.NoError(t, err)
			val := func(i int) []byte { return fmt.Appendf(nil, "val-%06d", i) }
			const n = 3000
			for i := 0; i < n; i++ {
				require.NoError(t, tx.Put(ns, saveKey(i), val(i)))
			}
			cur := tx.NewCursor(ns)
			defer cur.Close()
			require.NoError(t, cur.First())
			for i := 0; cur.Valid(); i++ {
				k, err := cur.Key()
				require.NoError(t, err)
				v, err := cur.Value()
				require.NoError(t, err)
				require.Equal(t, string(saveKey(i)), string(k))
				switch i % 3 {
				case 0:
					require.NoError(t, tx.Delete(ns, k))
				case 1:
					require.NoError(t, tx.Put(ns, k, append([]byte("updated-"), v...)))
				default:
					require.NoError(t, tx.Put(other, k, v))
				}
				require.NoError(t, tx.Put(other, fmt.Appendf(nil, "churn-%06d", i), bytes.Repeat([]byte("c"), 200)))
				require.NoError(t, cur.Next())
				if !cur.Valid() {
					require.Equal(t, n-1, i)
				}
			}
			cur.Close()
			require.NoError(t, tx.Commit())
			require.NoError(t, db.IntegrityCheck())

			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			for i := 0; i < n; i++ {
				got, err := rtx.Get(ns, saveKey(i))
				switch i % 3 {
				case 0:
					require.ErrorIs(t, err, ErrKeyNotFound, "key %d", i)
				case 1:
					require.NoError(t, err)
					require.Equal(t, "updated-"+string(val(i)), string(got), "key %d", i)
				default:
					require.NoError(t, err)
					require.Equal(t, string(val(i)), string(got), "key %d", i)
					copied, err := rtx.Get(other, saveKey(i))
					require.NoError(t, err)
					require.Equal(t, string(val(i)), string(copied), "copy of key %d", i)
				}
			}
		})
	}
}

// A move that fails ends the cursor: every later use reports the error,
// and neither a write to the tree nor a savepoint rollback is held up by
// it.
func TestCursorSave_FailedMoveEndsCursor(t *testing.T) {
	resetPageBufferPool()
	db, err := Open(filepath.Join(t.TempDir(), "t.db"), Options{PageSize: 512, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("data")
	require.NoError(t, err)
	for i := 0; i < 3000; i++ {
		require.NoError(t, tx.Put(ns, saveKey(i), []byte("v")))
	}
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	other, err := tx.CreateNamespace("other")
	require.NoError(t, err)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.First())
	// At the leaf's last entry the next step re-reads the parent.
	for leaf := cur.stack[len(cur.stack)-1]; leaf.cellIdx+1 < int(leaf.pg.header.cellCount); leaf = cur.stack[len(cur.stack)-1] {
		require.NoError(t, cur.Next())
	}
	require.GreaterOrEqual(t, len(cur.stack), 2)
	parent := cur.stack[len(cur.stack)-2].pgno
	db.pager.writerCache.discard(parent)
	frame := mustWiGet(t, db.pager.wal.index, parent, db.pager.wal.nFrame.Load())
	fault := errors.New("read fault")
	setWalReadFrameFaultHook(func(f uint32) error {
		if f == frame {
			return fault
		}
		return nil
	})
	err = cur.Next()
	setWalReadFrameFaultHook(nil)
	require.ErrorIs(t, err, fault)
	require.Equal(t, cursorFault, cur.state)
	require.Empty(t, cur.stack)
	require.ErrorIs(t, cur.Next(), fault)
	require.ErrorIs(t, cur.Previous(), fault)
	_, err = cur.Key()
	require.ErrorIs(t, err, fault)
	require.ErrorIs(t, cur.Seek(saveKey(1)), fault)

	require.NoError(t, tx.Put(ns, saveKey(5), []byte("x")))
	sp, err := tx.Savepoint()
	require.NoError(t, err)
	require.NoError(t, tx.Put(other, []byte("k"), []byte("v")))
	require.NoError(t, tx.RollbackToSavepoint(sp))
	_, err = tx.Get(other, []byte("k"))
	require.ErrorIs(t, err, ErrKeyNotFound)
}

// A cursor left open as its transaction ends is forgotten with it: its
// late Close touches no list, the next transaction's included.
func TestCursorSave_LateCloseTouchesNoList(t *testing.T) {
	db, ns, tx := saveTree(t, 100)
	old := tx.NewCursor(ns)
	require.True(t, old.linked)
	require.NoError(t, tx.Rollback())
	require.False(t, old.linked)

	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = tx2.Rollback() }()
	ns2, err := tx2.GetNamespace("data")
	require.NoError(t, err)
	c2 := tx2.NewCursor(ns2)
	defer c2.Close()
	old.Close()
	require.Same(t, c2, db.pager.cursors)
	require.Nil(t, c2.nextCursor)
}

// Zero moves leave the side a restored cursor landed on alone.
func TestCursorSave_SkipZeroKeepsSide(t *testing.T) {
	_, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	require.NoError(t, tx.Delete(ns, saveKey(1000)))
	_, err := cur.Key()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, cur.Skip(0))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1001))

	require.NoError(t, cur.Last())
	require.NoError(t, tx.Delete(ns, saveKey(1999)))
	_, err = cur.Key()
	require.ErrorIs(t, err, ErrKeyNotFound)
	require.NoError(t, cur.SkipBackward(0))
	require.NoError(t, cur.Previous())
	cursorKeyIs(t, cur, saveKey(1998))
}

// A savepoint's rollback undoes the schema changes made inside it: the
// cursors a drop ended stand on their keys again, an exhausted one stays
// exhausted, and a cursor on a tree created inside is ended for good.
func TestCursorSave_SavepointRollbackOfSchemaChanges(t *testing.T) {
	db, ns, tx := saveTree(t, 2000)
	cur := tx.NewCursor(ns)
	defer cur.Close()
	require.NoError(t, cur.Seek(saveKey(1000)))
	exhausted := tx.NewCursor(ns)
	defer exhausted.Close()
	require.NoError(t, exhausted.Last())
	require.NoError(t, exhausted.Next())
	require.False(t, exhausted.Valid())

	sp, err := tx.Savepoint()
	require.NoError(t, err)
	fresh, err := tx.CreateNamespace("fresh")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, tx.Put(fresh, saveKey(i), []byte("v")))
	}
	fc := tx.NewCursor(fresh)
	defer fc.Close()
	require.NoError(t, fc.First())
	require.NoError(t, tx.DeleteNamespace("data"))
	require.ErrorIs(t, cur.Next(), ErrNamespaceNotFound)
	_, err = cur.RangeFraction(saveKey(1), saveKey(5))
	require.ErrorIs(t, err, ErrNamespaceNotFound)
	require.ErrorIs(t, exhausted.Next(), ErrNamespaceNotFound)

	require.NoError(t, tx.RollbackToSavepoint(sp))
	require.NoError(t, tx.ReleaseSavepoint(sp))
	require.Equal(t, cursorRequireSeek, cur.state, "revived onto its saved key")
	k, err := cur.Key()
	require.NoError(t, err)
	require.Equal(t, string(saveKey(1000)), string(k))
	require.NoError(t, cur.Next())
	cursorKeyIs(t, cur, saveKey(1001))
	require.NoError(t, exhausted.Next())
	require.False(t, exhausted.Valid())
	require.ErrorIs(t, fc.Next(), ErrNamespaceNotFound)
	_, err = tx.GetNamespace("fresh")
	require.ErrorIs(t, err, ErrNamespaceNotFound)
	require.NoError(t, cur.First())
	require.Equal(t, wantKeys(0, 2000), drain(t, cur))
	cur.Close()
	exhausted.Close()
	fc.Close()
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}
