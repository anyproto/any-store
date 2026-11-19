package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// Put refuses a record the cell parsers would reject. The limit is 1 GiB;
// the tests lower it (payloadLimit) so that the records on both sides of it
// are a few pages long.

// setPayloadLimit lowers the record size limit for the test.
func setPayloadLimit(t *testing.T, n int) {
	t.Helper()
	prev := payloadLimit
	payloadLimit = n
	t.Cleanup(func() { payloadLimit = prev })
}

func payloadBytes(n int, seed byte) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = seed + byte(i*7)
	}
	return b
}

// TestPayloadLimit_RealConstants pins the limit both sides use and shows, on
// the real constants and without a record of that size, that the leaf and
// interior cell parsers accept a cell exactly when payloadFits — Put's
// test — accepts its lengths. The lengths are the 64-bit values a cell can
// declare, those over 32 bits included.
func TestPayloadLimit_RealConstants(t *testing.T) {
	require.Equal(t, 1<<30, maxPayloadAlloc)
	require.Equal(t, maxPayloadAlloc, payloadLimit)
	require.Equal(t, maxPayloadAlloc, maxKeySize)

	const usable = 4096
	leafLim := indexLimits(usable)
	const L = uint64(maxPayloadAlloc)
	for _, tc := range []struct {
		keyLen, valLen uint64
		fits           bool
	}{
		{0, 0, true},
		{1, L - 1, true},
		{L - 1, 1, true},
		{L, 0, true},
		{0, L, true},
		{L / 2, L / 2, true},
		{L/2 + 1, L / 2, false},
		{L / 2, L/2 + 1, false},
		{1, L, false},
		{L, 1, false},
		{L + 1, 0, false},
		{0, L + 1, false},
		{L, L, false},
		// Lengths a 32-bit int narrows to a small number.
		{1<<32 + 1, 0, false},
		{0, 1<<32 + 1, false},
		{1, 1 << 32, false},
		{1 << 32, 1 << 32, false},
		{1 << 63, 0, false},
		// Lengths whose 64-bit sum wraps to a small number.
		{math.MaxUint64, 1, false},
		{1, math.MaxUint64, false},
		{math.MaxUint64, math.MaxUint64, false},
		{1<<63 + 1, 1 << 63, false},
	} {
		name := fmt.Sprintf("key=%d,value=%d", tc.keyLen, tc.valLen)
		require.Equal(t, tc.fits, payloadFits(tc.keyLen, tc.valLen), name)
		// A leaf cell that declares these lengths: the header, the local
		// part of the payload and the overflow pointer (any page but 0) are
		// all a parser looks at.
		cell := bytes.Repeat([]byte{1}, usable)
		n := putVarint(cell, tc.keyLen)
		putVarint(cell[n:], tc.valLen)
		c, _, err := parseLeafCellWithSize(cell, 0, leafLim)
		if tc.fits {
			require.NoError(t, err, name)
		} else {
			require.ErrorIs(t, err, ErrCorrupt, name)
			require.Empty(t, c.key, name)
			require.Empty(t, c.value, name)
		}
		// An interior cell has a key only.
		if tc.valLen == 0 {
			icell := bytes.Repeat([]byte{1}, usable)
			putVarint(icell[4:], tc.keyLen)
			_, _, err := parseInteriorCell(icell, 0, usable)
			if tc.fits {
				require.NoError(t, err, name)
			} else {
				require.ErrorIs(t, err, ErrCorrupt, name)
			}
		}
	}
}

// TestPayloadLimit_LengthsCheckedBeforeNarrowing shows what the 64-bit test
// is for without needing a 32-bit build. narrowed is the test as it was when
// the parsers converted a declared length to int first, written out for a
// 32-bit int: it accepts a cell that declares 4 GiB and one byte, because
// that converts to 1.
func TestPayloadLimit_LengthsCheckedBeforeNarrowing(t *testing.T) {
	narrowed := func(keyLen, valLen uint64) bool {
		k, v := int32(keyLen), int32(valLen)
		limit := int32(maxPayloadAlloc)
		return k >= 0 && v >= 0 && v <= limit && k <= limit-v
	}
	for _, tc := range [][2]uint64{
		{1<<32 + 1, 0},
		{1, 1 << 32},
		{1 << 32, 1 << 32},
		{5<<32 + 7, 3<<32 + 9},
	} {
		require.True(t, narrowed(tc[0], tc[1]), "the narrowed test rejects %v: not the case this test is about", tc)
		require.False(t, payloadFits(tc[0], tc[1]), "payloadFits accepts %v", tc)
	}
	// Within the limit the two agree.
	for _, tc := range [][2]uint64{{0, 0}, {1, 1<<30 - 1}, {1 << 30, 0}, {1 << 30, 1}, {0, 1<<30 + 1}} {
		require.Equal(t, narrowed(tc[0], tc[1]), payloadFits(tc[0], tc[1]), "%v", tc)
	}
}

func TestPayloadLimit_PutRefusesWhatReadRejects(t *testing.T) {
	// Three overflow pages and a bit on a 4096-byte page.
	const limit = 3*4096 + 17
	for _, tc := range []struct {
		name           string
		keyLen, valLen int // the record exactly at the limit
		growKey        bool
		tooLarge       error
	}{
		{"value overflows", 1, limit - 1, false, ErrValueTooLarge},
		// With an empty key the refused value is over the limit by itself.
		{"value alone over the limit", 0, limit, false, ErrValueTooLarge},
		{"key overflows", limit, 0, true, ErrKeyTooLarge},
		{"key and value overflow", 5000, limit - 5000, false, ErrValueTooLarge},
		{"key and value overflow, key grows", 5000, limit - 5000, true, ErrValueTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setPayloadLimit(t, limit)
			path := filepath.Join(t.TempDir(), "limit.db")
			db, err := testOpen(t, path, Options{PageSize: 4096})
			require.NoError(t, err)
			defer func(db *DB) { _ = db.Close() }(db)

			neighbours := [][]byte{{0x00, 'j'}, {0xff, 'l'}}
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.CreateNamespace("t")
			require.NoError(t, err)
			for _, k := range neighbours {
				require.NoError(t, tx.Put(ns, k, []byte("neighbour")))
			}
			require.NoError(t, tx.Commit())

			// The first byte keeps the key between the neighbours; the
			// empty key sorts before both.
			key := payloadBytes(tc.keyLen, 3)
			if len(key) > 0 {
				key[0] = 0x50
			}
			value := payloadBytes(tc.valLen, 11)
			overKey, overValue := key, value
			if tc.growKey {
				overKey = append(bytes.Clone(key), 0x01)
			} else {
				overValue = append(bytes.Clone(value), 0x01)
			}

			sound := func(want int) {
				t.Helper()
				require.NoError(t, db.IntegrityCheck())
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				defer func() { _ = rtx.Rollback() }()
				n, err := rtx.Count(ns)
				require.NoError(t, err)
				require.Equal(t, want, n)
				for _, k := range neighbours {
					v, err := rtx.Get(ns, k)
					require.NoError(t, err)
					require.Equal(t, "neighbour", string(v))
				}
			}

			// One byte over: refused, and nothing is written.
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			require.ErrorIs(t, tx.Put(ns, overKey, overValue), tc.tooLarge)
			require.False(t, tx.HasChanges(), "a refused Put changed a page")
			require.NoError(t, tx.Commit())
			sound(2)

			// Exactly at the limit: written and read back whole.
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			require.NoError(t, tx.Put(ns, key, value))
			require.NoError(t, tx.Commit())
			readBack := func(db *DB) {
				t.Helper()
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				defer func() { _ = rtx.Rollback() }()
				ns, err := rtx.GetNamespace("t")
				require.NoError(t, err)
				got, err := rtx.Get(ns, key)
				require.NoError(t, err)
				require.True(t, bytes.Equal(value, got), "value read back differs")
				c := rtx.NewCursor(ns)
				defer c.Close()
				require.NoError(t, c.Seek(key))
				require.True(t, c.Valid())
				k, err := c.Key()
				require.NoError(t, err)
				require.True(t, bytes.Equal(key, k), "key read back differs")
				v, err := c.Value()
				require.NoError(t, err)
				require.True(t, bytes.Equal(value, v), "cursor value differs")
			}
			readBack(db)
			sound(3)

			// Replacing the record with one over the limit is refused the
			// same way and leaves the stored one.
			if !tc.growKey {
				tx, err = db.BeginWrite()
				require.NoError(t, err)
				require.ErrorIs(t, tx.Put(ns, key, overValue), tc.tooLarge)
				require.False(t, tx.HasChanges(), "a refused Put changed a page")
				require.NoError(t, tx.Rollback())
				readBack(db)
			}

			require.NoError(t, db.Close())
			db, err = testOpen(t, path, Options{PageSize: 4096})
			require.NoError(t, err)
			defer func(db *DB) { _ = db.Close() }(db)
			readBack(db)
			require.NoError(t, db.IntegrityCheck())

			// The read side has the same boundary: with the limit one byte
			// lower the stored record is the one Put would have refused.
			// Every read of it reports the cell as corrupt, the existence
			// probe included, and the neighbours are still found. The
			// transaction is rolled back on every way out: Close waits for
			// an open reader.
			func() {
				payloadLimit = limit - 1
				defer func() { payloadLimit = limit }()
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				defer func() { _ = rtx.Rollback() }()
				ns, err := rtx.GetNamespace("t")
				require.NoError(t, err)
				_, err = rtx.Get(ns, key)
				require.ErrorIs(t, err, ErrCorrupt)
				_, err = rtx.Has(ns, key)
				require.ErrorIs(t, err, ErrCorrupt, "Has reports a record Get rejects")
				c := rtx.NewCursor(ns)
				defer c.Close()
				if err = c.Seek(key); err == nil {
					_, err = c.Value()
				}
				require.ErrorIs(t, err, ErrCorrupt)
				// A search for a neighbour may probe the cell. It tests the
				// two lengths singly, so the neighbours of a cell that is
				// over the limit only by their sum are found.
				if tc.keyLen > limit-1 || tc.valLen > limit-1 {
					return
				}
				for _, k := range neighbours {
					ok, err := rtx.Has(ns, k)
					require.NoError(t, err)
					require.True(t, ok)
					v, err := rtx.Get(ns, k)
					require.NoError(t, err)
					require.Equal(t, "neighbour", string(v))
				}
			}()

			// And it can be deleted.
			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err = tx.GetNamespace("t")
			require.NoError(t, err)
			require.NoError(t, tx.Delete(ns, key))
			require.NoError(t, tx.Commit())
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// TestPayloadLimit_NamespaceName: the master tree is written through the
// same Put, so a namespace name is held to the same limit, by CreateNamespace
// and by RenameNamespace.
func TestPayloadLimit_NamespaceName(t *testing.T) {
	const limit = 2*4096 + 5
	setPayloadLimit(t, limit)
	path := filepath.Join(t.TempDir(), "ns.db")
	db, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func(db *DB) { _ = db.Close() }(db)

	// The record is the name and a 4-byte root page number.
	created := string(bytes.Repeat([]byte("c"), limit-4))
	renamed := string(bytes.Repeat([]byte("r"), limit-4))
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace(created + "x")
	require.ErrorIs(t, err, ErrValueTooLarge)
	require.False(t, tx.HasChanges(), "a refused CreateNamespace changed a page")
	// Exactly at the limit: created, and usable.
	ns, err := tx.CreateNamespace(created)
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, []byte("k"), []byte("v")))
	_, err = tx.CreateNamespace("short")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// A rename to a name over the limit is refused before the old name is
	// removed; one to a name at the limit goes through.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	require.ErrorIs(t, tx.RenameNamespace("short", renamed+"x"), ErrValueTooLarge)
	require.False(t, tx.HasChanges(), "a refused RenameNamespace changed a page")
	require.NoError(t, tx.RenameNamespace("short", renamed))
	require.NoError(t, tx.Commit())

	resolve := func(db *DB) {
		t.Helper()
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		ns, err := rtx.GetNamespace(created)
		require.NoError(t, err)
		v, err := rtx.Get(ns, []byte("k"))
		require.NoError(t, err)
		require.Equal(t, "v", string(v))
		_, err = rtx.GetNamespace(renamed)
		require.NoError(t, err)
		_, err = rtx.GetNamespace("short")
		require.ErrorIs(t, err, ErrNamespaceNotFound)
		names, err := db.ListNamespaces()
		require.NoError(t, err)
		require.ElementsMatch(t, []string{created, renamed}, names)
		require.NoError(t, db.IntegrityCheck())
	}
	resolve(db)
	require.NoError(t, db.Close())
	db, err = testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer func(db *DB) { _ = db.Close() }(db)
	resolve(db)

	// One byte lower and both names are records the lookup rejects.
	func() {
		payloadLimit = limit - 1
		defer func() { payloadLimit = limit }()
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		_, err = rtx.GetNamespace(created)
		require.ErrorIs(t, err, ErrCorrupt)
	}()
}

// payloadFixtureLeaf makes the root of namespace "t" a leaf with one cell
// that declares a key of keyLen bytes and a value of valLen bytes, far more
// than the file holds, with a local part and a pointer to an overflow page
// that is no chain. Each page is well formed; only the lengths are wrong.
func payloadFixtureLeaf(t *testing.T, db *DB, keyLen, valLen int) *Namespace {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	pg, err := tx.pager.getWritablePage(ns.rootPage)
	require.NoError(t, err)
	lim := tx.pager.leafLimits()
	nLocal := localPayloadSize(keyLen+valLen, lim)
	cell := make([]byte, 0, nLocal+32)
	var vb [9]byte
	cell = append(cell, vb[:putVarint(vb[:], uint64(keyLen))]...)
	cell = append(cell, vb[:putVarint(vb[:], uint64(valLen))]...)
	cell = append(cell, bytes.Repeat([]byte("k"), nLocal)...) // local key, then local value
	cell = binary.BigEndian.AppendUint32(cell, 2)             // "overflow" page: a b-tree page
	off := tx.pager.usableSize() - len(cell)
	clear(pg.data)
	copy(pg.data[off:], cell)
	pg.header.pageType = pageTypeLeafIdx
	pg.header.cellCount = 1
	pg.header.cellContentOff = uint16(off)
	pg.header.serialize(pg.data)
	binary.BigEndian.PutUint16(pg.data[pg.cellPointerOffset():], uint16(off))
	tx.pager.releasePage(pg)
	require.NoError(t, tx.Commit())
	return ns
}

// allocatedBy returns the bytes fn allocated.
func allocatedBy(fn func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	fn()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestPayloadLimit_DeclaredLengthOverFileSize: a cell may declare a payload
// within the record size limit and far over what the file can hold. A reader
// that sized its buffer from the declared length would allocate that much
// before the overflow chain turned out short; each one reports ErrCorrupt
// first.
func TestPayloadLimit_DeclaredLengthOverFileSize(t *testing.T) {
	const declared = 256 << 20 // within the 1 GiB limit, in a file of a few pages
	const allowed = 4 << 20    // what a read of this file may allocate
	type read struct {
		name string
		run  func(rtx *ReadTx, ns *Namespace) error
	}
	check := func(t *testing.T, keyLen, valLen int, reads []read) {
		db, err := testOpen(t, filepath.Join(t.TempDir(), "declared.db"), Options{PageSize: 4096})
		require.NoError(t, err)
		defer func(db *DB) { _ = db.Close() }(db)
		ns := payloadFixtureLeaf(t, db, keyLen, valLen)
		for _, r := range reads {
			var rerr error
			n := allocatedBy(func() {
				rtx, err := db.BeginRead()
				if err != nil {
					rerr = err
					return
				}
				defer func() { _ = rtx.Rollback() }()
				rerr = r.run(rtx, ns)
			})
			require.ErrorIs(t, rerr, ErrCorrupt, r.name)
			require.Less(t, n, uint64(allowed), "%s allocated %d MiB for a cell of a %d-page file", r.name, n>>20, db.pager.dbSize.Load())
		}
	}
	cursorAt := func(rtx *ReadTx, ns *Namespace, fn func(c *Cursor) error) error {
		c := rtx.NewCursor(ns)
		defer c.Close()
		if err := c.First(); err != nil {
			return err
		}
		return fn(c)
	}

	t.Run("value", func(t *testing.T) {
		key := []byte("k")
		check(t, 1, declared, []read{
			{"ReadTx.Get", func(rtx *ReadTx, ns *Namespace) error {
				_, err := rtx.Get(ns, key)
				return err
			}},
			{"btree.Get", func(rtx *ReadTx, ns *Namespace) error {
				bt := &btree{pager: rtx.pager, cache: rtx.cache, rootPage: ns.rootPage, walMaxFrame: rtx.walHdr.mxFrame}
				_, err := bt.Get(key)
				return err
			}},
			{"Cursor.Value", func(rtx *ReadTx, ns *Namespace) error {
				return cursorAt(rtx, ns, func(c *Cursor) error {
					_, err := c.Value()
					return err
				})
			}},
			{"Cursor.AppendValue", func(rtx *ReadTx, ns *Namespace) error {
				return cursorAt(rtx, ns, func(c *Cursor) error {
					_, err := c.AppendValue(nil)
					return err
				})
			}},
			{"Cursor.AppendValueByKey", func(rtx *ReadTx, ns *Namespace) error {
				c := rtx.NewCursor(ns)
				defer c.Close()
				_, err := c.AppendValueByKey(key, nil)
				return err
			}},
		})
	})
	t.Run("key", func(t *testing.T) {
		// The local part of the key is all "k": a search for a longer run
		// of "k" matches it and has to read the rest of the key.
		probe := bytes.Repeat([]byte("k"), 8000)
		check(t, declared, 0, []read{
			{"ReadTx.Get", func(rtx *ReadTx, ns *Namespace) error {
				_, err := rtx.Get(ns, probe)
				return err
			}},
			{"ReadTx.Has", func(rtx *ReadTx, ns *Namespace) error {
				_, err := rtx.Has(ns, probe)
				return err
			}},
			{"ReadTx.SeekKey", func(rtx *ReadTx, ns *Namespace) error {
				_, err := rtx.SeekKey(ns, []byte("a"))
				return err
			}},
			{"Cursor.Key", func(rtx *ReadTx, ns *Namespace) error {
				return cursorAt(rtx, ns, func(c *Cursor) error {
					_, err := c.Key()
					return err
				})
			}},
			{"IntegrityCheck", func(rtx *ReadTx, ns *Namespace) error {
				// It reports what it finds as text; that it found the
				// cell is all that is asked here.
				if err := rtx.db.IntegrityCheck(); err != nil {
					return ErrCorrupt
				}
				return nil
			}},
		})
	})
}

// TestPayloadLimit_OverflowRecordWithinFileSize: the bound does not touch a
// sound record. One of several megabytes in a file that holds it is read by
// every path, by the writer inside the transaction that wrote it and by a
// reader after it.
func TestPayloadLimit_OverflowRecordWithinFileSize(t *testing.T) {
	db, err := testOpen(t, filepath.Join(t.TempDir(), "big.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	defer func(db *DB) { _ = db.Close() }(db)
	key := payloadBytes(9000, 5) // overflows too
	value := payloadBytes(3<<20, 9)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, key, value))
	got, err := tx.Get(ns, key)
	require.NoError(t, err)
	require.True(t, bytes.Equal(value, got), "the writer reads back another value")
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	got, err = rtx.Get(ns, key)
	require.NoError(t, err)
	require.True(t, bytes.Equal(value, got))
	c := rtx.NewCursor(ns)
	defer c.Close()
	require.NoError(t, c.First())
	k, err := c.Key()
	require.NoError(t, err)
	require.True(t, bytes.Equal(key, k))
	v, err := c.Value()
	require.NoError(t, err)
	require.True(t, bytes.Equal(value, v))
	require.NoError(t, db.IntegrityCheck())
}
