package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/big"
	"path/filepath"
	"runtime"
	"strings"
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

// TestPayloadLimit_SpilledLocalSize: for a pair of lengths whose sum is over
// maxLocal, the only pairs it is called for, spilledLocalSize is
// localPayloadSize when an int holds the sum, and for 64-bit lengths it is
// btreePayloadToLocal's formula on the exact sum (math/big).
func TestPayloadLimit_SpilledLocalSize(t *testing.T) {
	for _, lim := range []payloadLimits{indexLimits(4096), tableLeafLimits(4096), tableLeafLimits(1024 - 8)} {
		over := uint64(lim.maxLocal) + 1
		for _, tc := range [][2]uint64{{0, over}, {over, 0}, {1, over - 1}, {300, 4000}, {5001, 20000}, {1, 1<<30 - 1}, {1 << 30, 0}, {1 << 29, 1 << 29}} {
			require.Equal(t, localPayloadSize(int(tc[0]+tc[1]), lim), spilledLocalSize(tc[0], tc[1], lim), "%v %+v", tc, lim)
		}
		m := big.NewInt(int64(overflowPageUsable(lim.usable)))
		minLocal := big.NewInt(int64(lim.minLocal))
		for _, tc := range [][2]uint64{{5001, 1 << 63}, {5001, math.MaxUint64}, {1 << 30, math.MaxUint64}, {math.MaxUint64, math.MaxUint64}, {0, math.MaxUint64}} {
			surplus := new(big.Int).Add(new(big.Int).SetUint64(tc[0]), new(big.Int).SetUint64(tc[1]))
			surplus.Sub(surplus, minLocal)
			surplus.Mod(surplus, m)
			surplus.Add(surplus, minLocal)
			want := lim.minLocal
			if surplus.Cmp(big.NewInt(int64(lim.maxLocal))) <= 0 {
				want = int(surplus.Int64())
			}
			require.Equal(t, want, spilledLocalSize(tc[0], tc[1], lim), "%v %+v", tc, lim)
		}
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
				// A search for a neighbour may probe the cell. It bounds
				// the key alone, so the neighbours are found unless the key
				// itself is over the limit. Whether a lookup probes the cell
				// depends on where it sits: the empty key of "value alone
				// over the limit" sorts first and is not probed;
				// TestPayloadLimit_NeighbourOfDamagedCellIsFound makes the
				// probe certain.
				if tc.keyLen > limit-1 {
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
// that declares a key of keyLen bytes and a value of valLen bytes, with a
// local part and overflowPg for the first overflow page: no chain of that
// length follows it. Each page is well formed; only the cell is wrong.
func payloadFixtureLeaf(t *testing.T, db *DB, keyLen, valLen int, overflowPg uint32) *Namespace {
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
	cell = binary.BigEndian.AppendUint32(cell, overflowPg)
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
		// The "overflow" page is page 2, a b-tree page.
		ns := payloadFixtureLeaf(t, db, keyLen, valLen, 2)
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
				// It reports what it finds as text. The fixture's overflow
				// pointer names a b-tree page, which it reports too; the
				// finding asked for here is the key it could not read.
				err := rtx.db.IntegrityCheck()
				var ie *IntegrityError
				if !errors.As(err, &ie) {
					return fmt.Errorf("not an integrity report: %v", err)
				}
				for _, msg := range ie.Errors {
					if strings.Contains(msg, "cell 0: reading the leaf key failed: ") {
						return ErrCorrupt
					}
				}
				return fmt.Errorf("the report does not name the cell's key: %v", err)
			}},
		})
	})
}

// TestPayloadLimit_SpilledKeyWithNoOverflowPage: a cell whose key spills and
// whose first overflow pointer names no page of the file is ErrCorrupt before
// a buffer for the key is allocated, in a file large enough to hold the key
// it declares. Each read rebuilds that key: a cursor's Key, a lookup whose
// key ties with the local part (the reader's and the writer's), and SeekKey.
func TestPayloadLimit_SpilledKeyWithNoOverflowPage(t *testing.T) {
	const declared = 4 << 20
	const allowed = 1 << 20 // a quarter of the key; a rebuild allocates twice the key
	probe := bytes.Repeat([]byte("k"), 8000)
	reads := []struct {
		name string
		run  func(db *DB, ns *Namespace) error
	}{
		{"Cursor.Key", func(db *DB, ns *Namespace) error {
			rtx, err := db.BeginRead()
			if err != nil {
				return err
			}
			defer func() { _ = rtx.Rollback() }()
			c := rtx.NewCursor(ns)
			defer c.Close()
			if err := c.First(); err != nil {
				return err
			}
			_, err = c.Key()
			return err
		}},
		{"ReadTx.Get", func(db *DB, ns *Namespace) error {
			rtx, err := db.BeginRead()
			if err != nil {
				return err
			}
			defer func() { _ = rtx.Rollback() }()
			_, err = rtx.Get(ns, probe)
			return err
		}},
		{"ReadTx.SeekKey", func(db *DB, ns *Namespace) error {
			rtx, err := db.BeginRead()
			if err != nil {
				return err
			}
			defer func() { _ = rtx.Rollback() }()
			_, err = rtx.SeekKey(ns, []byte("a"))
			return err
		}},
		{"WriteTx.Get", func(db *DB, ns *Namespace) error {
			wtx, err := db.BeginWrite()
			if err != nil {
				return err
			}
			defer func() { _ = wtx.Rollback() }()
			_, err = wtx.Get(ns, probe)
			return err
		}},
	}
	for _, tc := range []struct {
		name       string
		overflowPg uint32
	}{
		{"pointer 0", 0},
		{"pointer past the file", math.MaxUint32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, err := testOpen(t, filepath.Join(t.TempDir(), "nopage.db"), Options{PageSize: 4096})
			require.NoError(t, err)
			defer func(db *DB) { _ = db.Close() }(db)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			filler, err := tx.CreateNamespace("filler")
			require.NoError(t, err)
			require.NoError(t, tx.Put(filler, []byte("f"), payloadBytes(declared+declared/2, 1)))
			require.NoError(t, tx.Commit())
			ns := payloadFixtureLeaf(t, db, declared, 0, tc.overflowPg)
			require.GreaterOrEqual(t, uint64(db.pager.dbSize.Load())*uint64(db.pager.pageSize), uint64(declared),
				"the file cannot hold the key: the file-size bound would refuse it first")
			for _, r := range reads {
				var rerr error
				n := allocatedBy(func() { rerr = r.run(db, ns) })
				require.ErrorIs(t, rerr, ErrCorrupt, r.name)
				require.Less(t, n, uint64(allowed), "%s allocated %d KiB for a key with no overflow page", r.name, n>>10)
			}
		})
	}
}

// declareValLen rewrites, in place, the value length that cell idx of the
// leaf page pgno declares. The new length takes a varint of the same width
// and leaves the cell's local part the size it was: the cell stays well
// formed and its key and overflow chain are where they were; only the length
// is wrong.
func declareValLen(t *testing.T, tx *WriteTx, pgno uint32, idx int, valLen uint64) {
	t.Helper()
	pg, err := tx.pager.getWritablePage(pgno)
	require.NoError(t, err)
	defer tx.pager.releasePage(pg)
	require.True(t, pg.header.isLeaf())
	off := int(pg.getCellOffset(idx))
	keyLen, kn := getVarint(pg.data[off:])
	oldLen, vn := getVarint(pg.data[off+kn:])
	lim := tx.pager.leafLimits()
	require.Equal(t, localPayloadSize(int(keyLen+oldLen), lim), localPayloadSize(int(keyLen+valLen), lim),
		"the local part would change size")
	var vb [9]byte
	require.Equal(t, vn, putVarint(vb[:], valLen), "the length varint would change width")
	copy(pg.data[off+kn:], vb[:vn])
}

// overFileValLen returns the largest value length with a 3-byte varint whose
// cell, with a key of keyLen bytes, keeps the local part a value of oldLen
// gives it: about 2 MiB, more than the files of these tests hold.
func overFileValLen(t *testing.T, lim payloadLimits, keyLen, oldLen int) uint64 {
	t.Helper()
	for v := 1<<21 - 1; v >= 1<<20; v-- {
		if localPayloadSize(keyLen+v, lim) == localPayloadSize(keyLen+oldLen, lim) {
			return uint64(v)
		}
	}
	t.Fatal("no value length keeps the local part")
	return 0
}

// The damaged-cell fixture: two keys that share their first 5000 bytes, more
// than a 4 KiB page keeps, so both spill and their local parts are all "k".
// A search for the smaller one probes the larger one first, finds the local
// parts equal and rebuilds the larger key from its overflow chain to compare
// it. A cursor that steps from the first reaches the second.
var (
	healthyKey = append(bytes.Repeat([]byte("k"), 5000), '1')
	damagedKey = append(bytes.Repeat([]byte("k"), 5000), '2')
	healthyVal = []byte("healthy")
)

// damagedValLen is the value length the fixture writes for the damaged key
// unless a test asks for another: a 3-byte length varint, with which the
// cell keeps minLocal bytes on the page.
const damagedValLen = 20000

// writtenValLen writes damagedValLen.
func writtenValLen(payloadLimits) int { return damagedValLen }

// congruentValLen returns a value length near damagedValLen with which the
// cell keeps on its page what it would keep declaring the 64-bit length
// declared: the two are equal modulo the overflow page's payload size, so
// redeclareValLen can put declared in its place.
func congruentValLen(declared uint64) func(payloadLimits) int {
	return func(lim payloadLimits) int {
		m := uint64(overflowPageUsable(lim.usable))
		return damagedValLen + int((declared%m+m-damagedValLen%m)%m)
	}
}

// damagedFixture writes the healthy record and the one to be damaged, whose
// value has valLenFor(the file's leaf limits) bytes, into the root leaf of a
// new namespace and commits. The healthy cell is written first and lies at
// the end of the page, the other below it, at the start of the cell content
// area.
func damagedFixture(t *testing.T, valLenFor func(payloadLimits) int) (*DB, *Namespace) {
	t.Helper()
	db, err := testOpen(t, filepath.Join(t.TempDir(), "neighbour.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, healthyKey, healthyVal))
	require.NoError(t, tx.Put(ns, damagedKey, payloadBytes(valLenFor(tx.pager.leafLimits()), 3)))
	require.NoError(t, tx.Commit())
	return db, ns
}

// redeclareValLen rewrites the value length that cell idx of the leaf page
// pgno declares, at whatever varint width valLen takes. The cell must start
// the cell content area, which grows down by the extra width, and the new
// length must give the cell's local part the size it was written with: the
// key, its local part and the overflow chain stay as they were; only the
// length is wrong.
func redeclareValLen(t *testing.T, tx *WriteTx, pgno uint32, idx int, valLen uint64) {
	t.Helper()
	pg, err := tx.pager.getWritablePage(pgno)
	require.NoError(t, err)
	defer tx.pager.releasePage(pg)
	require.True(t, pg.header.isLeaf())
	off := int(pg.getCellOffset(idx))
	require.Equal(t, int(pg.header.cellContentOff), off, "the cell does not start the content area")
	keyLen, kn := getVarint(pg.data[off:])
	oldLen, vn := getVarint(pg.data[off+kn:])
	lim := tx.pager.leafLimits()
	require.Greater(t, int(keyLen+oldLen), lim.maxLocal, "the cell does not spill")
	nLocal := localPayloadSize(int(keyLen+oldLen), lim)
	require.Equal(t, nLocal, spilledLocalSize(keyLen, valLen, lim), "the local part would change size")
	var vb [9]byte
	w := putVarint(vb[:], valLen)
	newOff := off - (w - vn)
	require.GreaterOrEqual(t, newOff, pg.cellPointerOffset()+2*int(pg.header.cellCount))
	cell := append([]byte(nil), pg.data[off:off+kn]...)
	cell = append(cell, vb[:w]...)
	cell = append(cell, pg.data[off+kn+vn:off+kn+vn+nLocal+overflowPtrSize]...)
	copy(pg.data[newOff:], cell)
	pg.header.cellContentOff = uint16(newOff)
	pg.header.serialize(pg.data)
	binary.BigEndian.PutUint16(pg.data[pg.cellPointerOffset()+2*idx:], uint16(newOff))
}

// findHealthy requires the healthy key, beside the damaged one, to be found
// by Get, Has, SeekKey, a cursor seek and the writer's Get, and the damaged
// key's Get to be ErrCorrupt.
func findHealthy(t *testing.T, db *DB, ns *Namespace) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	got, err := rtx.Get(ns, healthyKey)
	require.NoError(t, err, "ReadTx.Get")
	require.Equal(t, healthyVal, got)
	ok, err := rtx.Has(ns, healthyKey)
	require.NoError(t, err, "ReadTx.Has")
	require.True(t, ok)
	k, err := rtx.SeekKey(ns, healthyKey)
	require.NoError(t, err, "ReadTx.SeekKey")
	require.Equal(t, healthyKey, k)
	c := rtx.NewCursor(ns)
	require.NoError(t, c.Seek(healthyKey), "Cursor.Seek")
	k, err = c.Key()
	require.NoError(t, err)
	require.Equal(t, healthyKey, k)
	c.Close()
	_, err = rtx.Get(ns, damagedKey)
	require.ErrorIs(t, err, ErrCorrupt, "the damaged cell's value")
	require.NoError(t, rtx.Rollback())

	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = wtx.Rollback() }()
	got, err = wtx.Get(ns, healthyKey)
	require.NoError(t, err, "WriteTx.Get")
	require.Equal(t, healthyVal, got)
}

// valueLengthDamages are the ways the fixture's damaged cell is damaged: each
// one leaves the key and the layout of the cell's local part as written and
// makes the value unreadable.
var valueLengthDamages = []struct {
	name string
	// valLenFor is the value length the fixture writes.
	valLenFor func(payloadLimits) int
	// damage damages the committed fixture.
	damage func(t *testing.T, db *DB, ns *Namespace)
}{
	{"declared value over the file", writtenValLen, func(t *testing.T, db *DB, ns *Namespace) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		valLen := overFileValLen(t, tx.pager.leafLimits(), len(damagedKey), damagedValLen)
		declareValLen(t, tx, ns.rootPage, 1, valLen)
		require.NoError(t, tx.Commit())
		require.Greater(t, valLen, uint64(db.pager.dbSize.Load())*uint64(db.pager.pageSize), "the file holds the value")
	}},
	// A release without the write-side limit could store this cell: each
	// length is within the limit, their sum is not.
	{"key and value over the limit together", writtenValLen, func(t *testing.T, db *DB, ns *Namespace) {
		setPayloadLimit(t, damagedValLen)
	}},
	{"value over the limit by itself", writtenValLen, func(t *testing.T, db *DB, ns *Namespace) {
		setPayloadLimit(t, damagedValLen-1)
	}},
	// A length no int holds, and none of whose sums a uint64 holds.
	{"declared value 1<<63", congruentValLen(1 << 63), func(t *testing.T, db *DB, ns *Namespace) {
		redeclare(t, db, ns, 1<<63)
	}},
	{"declared value MaxUint64", congruentValLen(math.MaxUint64), func(t *testing.T, db *DB, ns *Namespace) {
		redeclare(t, db, ns, math.MaxUint64)
	}},
}

func redeclare(t *testing.T, db *DB, ns *Namespace, valLen uint64) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	redeclareValLen(t, tx, ns.rootPage, 1, valLen)
	require.NoError(t, tx.Commit())
}

// TestPayloadLimit_NeighbourOfDamagedCellIsFound: a search that has to
// compare a cell's spilled key reads that key and nothing else, so only the
// key is bounded there. For each of the fixture's damages to the value
// length, Get, Has, SeekKey, a cursor seek and the writer's Get find the key
// beside the damaged cell, whose own Get stays ErrCorrupt.
func TestPayloadLimit_NeighbourOfDamagedCellIsFound(t *testing.T) {
	for _, d := range valueLengthDamages {
		t.Run(d.name, func(t *testing.T) {
			db, ns := damagedFixture(t, d.valLenFor)
			d.damage(t, db, ns)
			findHealthy(t, db, ns)
		})
	}
	// Has answers from the key and the record size limit: a value the file
	// cannot hold is tested where it is read.
	t.Run("Has of a value over the file", func(t *testing.T) {
		db, ns := damagedFixture(t, writtenValLen)
		valueLengthDamages[0].damage(t, db, ns)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer func() { _ = rtx.Rollback() }()
		ok, err := rtx.Has(ns, damagedKey)
		require.NoError(t, err)
		require.True(t, ok)
	})
}

// TestPayloadLimit_CursorKeyOfDamagedCell: a cursor that steps onto a cell,
// with no search that would test the record, reads its key and nothing else,
// so Key returns the key of a cell whose value length is damaged in any of
// the fixture's ways, and Value is ErrCorrupt.
func TestPayloadLimit_CursorKeyOfDamagedCell(t *testing.T) {
	for _, d := range valueLengthDamages {
		t.Run(d.name, func(t *testing.T) {
			db, ns := damagedFixture(t, d.valLenFor)
			d.damage(t, db, ns)
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			c := rtx.NewCursor(ns)
			defer c.Close()
			require.NoError(t, c.First())
			k, err := c.Key()
			require.NoError(t, err)
			require.Equal(t, healthyKey, k)
			require.NoError(t, c.Next())
			require.True(t, c.Valid())
			k, err = c.Key()
			require.NoError(t, err, "Cursor.Key of the damaged cell")
			require.Equal(t, damagedKey, k)
			_, err = c.Value()
			require.ErrorIs(t, err, ErrCorrupt, "Cursor.Value of the damaged cell")
		})
	}
}

// TestPayloadLimit_SeekKeyOfDamagedCell: SeekKey reads the key of the cell it
// lands on and nothing else, and agrees with a cursor's Seek and Key. A prefix
// just below the damaged key lands on it and returns the key, for each of the
// fixture's damages; the damaged key itself is an exact match, which the
// search holds to the record size limit.
func TestPayloadLimit_SeekKeyOfDamagedCell(t *testing.T) {
	below := append(bytes.Clone(healthyKey), 0) // above the healthy key, below the damaged one
	for _, d := range valueLengthDamages {
		t.Run(d.name, func(t *testing.T) {
			db, ns := damagedFixture(t, d.valLenFor)
			d.damage(t, db, ns)
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			k, err := rtx.SeekKey(ns, below)
			require.NoError(t, err, "SeekKey below the damaged key")
			require.Equal(t, damagedKey, k)
			for _, p := range []struct {
				name   string
				prefix []byte
			}{{"below the damaged key", below}, {"the damaged key", damagedKey}} {
				k, err := rtx.SeekKey(ns, p.prefix)
				c := rtx.NewCursor(ns)
				cerr := c.Seek(p.prefix)
				var ck []byte
				if cerr == nil {
					ck, cerr = c.Key()
				}
				c.Close()
				if cerr != nil {
					require.ErrorIs(t, cerr, ErrCorrupt, "%s: the cursor", p.name)
					require.ErrorIs(t, err, ErrCorrupt, "%s: SeekKey", p.name)
					continue
				}
				require.NoError(t, err, "%s: SeekKey", p.name)
				require.Equal(t, ck, k, p.name)
			}
		})
	}
}

// TestPayloadLimit_SeekKeyNextLeafStartsWithDamagedCell: when the leaf a
// SeekKey reaches holds no key at or after the prefix, it reads the first key
// of the next leaf, and that key alone. A record over the record size limit
// there, by its sum or by its value, does not fail it.
func TestPayloadLimit_SeekKeyNextLeafStartsWithDamagedCell(t *testing.T) {
	const val = 100
	for _, d := range []struct {
		name  string
		limit func(keyLen int) int
	}{
		{"key and value over the limit together", func(keyLen int) int { return keyLen + val - 1 }},
		{"value over the limit by itself", func(int) int { return val - 1 }},
	} {
		t.Run(d.name, func(t *testing.T) {
			db, err := testOpen(t, filepath.Join(t.TempDir(), "next.db"), Options{PageSize: 4096})
			require.NoError(t, err)
			defer func(db *DB) { _ = db.Close() }(db)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.CreateNamespace("t")
			require.NoError(t, err)
			for i := range 200 {
				require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "k-%04d", i), payloadBytes(val, byte(i))))
			}
			require.NoError(t, tx.Commit())

			// The prefix sorts after the last key of the root's first child
			// leaf and before the root's first divider: the search for it
			// ends on that leaf with no key at or after it.
			var divider, prefix, next []byte
			func() {
				rtx, err := db.BeginRead()
				require.NoError(t, err)
				defer func() { _ = rtx.Rollback() }()
				root, err := rtx.pager.getPageReader(ns.rootPage, rtx.walHdr.mxFrame, rtx.cache)
				require.NoError(t, err)
				require.True(t, root.header.isInterior(), "the records fit the root leaf")
				d0, left, err := interiorCellKey(root.data, int(root.getCellOffset(0)))
				require.NoError(t, err)
				divider = bytes.Clone(d0)
				rtx.pager.releasePage(root)
				leaf, err := rtx.pager.getPageReader(left, rtx.walHdr.mxFrame, rtx.cache)
				require.NoError(t, err)
				defer rtx.pager.releasePage(leaf)
				require.True(t, leaf.header.isLeaf(), "the tree is deeper than two levels")
				last, err := leafKeyAt(leaf, int(leaf.header.cellCount)-1, rtx.pager.leafLimits())
				require.NoError(t, err)
				prefix = append(bytes.Clone(last), 0)
				require.Positive(t, bytes.Compare(divider, prefix), "the search for the prefix does not take the first child")
				c := rtx.NewCursor(ns)
				defer c.Close()
				require.NoError(t, c.Seek(prefix))
				k, err := c.Key()
				require.NoError(t, err)
				next = bytes.Clone(k)
			}()

			setPayloadLimit(t, d.limit(len(next)))
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			_, err = rtx.Get(ns, next)
			require.ErrorIs(t, err, ErrCorrupt, "the record is within the limit")
			k, err := rtx.SeekKey(ns, prefix)
			require.NoError(t, err)
			require.Equal(t, next, k)
		})
	}
}

// TestPayloadLimit_CountUntilOverDamagedCell: a range count whose leaf ends
// with a damaged cell reads that cell's key, not the whole cell, and counts it.
func TestPayloadLimit_CountUntilOverDamagedCell(t *testing.T) {
	for _, d := range valueLengthDamages {
		t.Run(d.name, func(t *testing.T) {
			db, ns := damagedFixture(t, d.valLenFor)
			d.damage(t, db, ns)
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			c := rtx.NewCursor(ns)
			defer c.Close()
			require.NoError(t, c.First())
			n, err := c.CountUntil(damagedKey, true)
			require.NoError(t, err)
			require.Equal(t, 2, n)
			// Exclusive of the damaged key: only the cell before it.
			require.NoError(t, c.First())
			n, err = c.CountUntil(damagedKey, false)
			require.NoError(t, err)
			require.Equal(t, 1, n)
		})
	}
}

// TestPayloadLimit_SplitBesideDamagedValueLengths: a split takes the key of
// the first cell of each new page for the divider, and reads only that key.
// On a leaf whose cells declare values over the file, the key that splits it
// is written and read back.
func TestPayloadLimit_SplitBesideDamagedValueLengths(t *testing.T) {
	// The record sizes below place the cells for schema format 6's local
	// payload limits.
	useCurrentFormat(t)
	db, err := testOpen(t, filepath.Join(t.TempDir(), "split.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	defer func(db *DB) { _ = db.Close() }(db)
	// Six records whose keys spill take about 3000 bytes of the root leaf;
	// a seventh with a 2000-byte value, sorted before them, splits it, and
	// every page after the first starts with one of the six.
	key := func(i byte) []byte { return append(bytes.Repeat([]byte("k"), 1100), i) }
	// A 3-byte length varint; each cell keeps minLocal bytes on the page.
	const val = 19500
	const records = 6
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	for i := byte(1); i <= records; i++ {
		require.NoError(t, tx.Put(ns, key(i), payloadBytes(val, i)))
	}
	require.NoError(t, tx.Commit())

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	root, err := tx.pager.getPage(ns.rootPage)
	require.NoError(t, err)
	require.True(t, root.header.isLeaf(), "the records do not fit the root leaf")
	n := int(root.header.cellCount)
	tx.pager.releasePage(root)
	require.Equal(t, records, n)
	valLen := overFileValLen(t, tx.pager.leafLimits(), len(key(1)), val)
	for i := 0; i < n; i++ {
		declareValLen(t, tx, ns.rootPage, i, valLen)
	}
	require.NoError(t, tx.Commit())
	require.Greater(t, valLen, uint64(db.pager.dbSize.Load())*uint64(db.pager.pageSize), "the file holds the values")

	tx, err = db.BeginWrite()
	require.NoError(t, err)
	newKey, newVal := key(0), payloadBytes(2000, 0)
	require.NoError(t, tx.Put(ns, newKey, newVal))
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	root, err = rtx.pager.getPageReader(ns.rootPage, rtx.walHdr.mxFrame, rtx.cache)
	require.NoError(t, err)
	split := root.header.isInterior()
	rtx.pager.releasePage(root)
	require.True(t, split, "the insert did not split the root leaf")
	got, err := rtx.Get(ns, newKey)
	require.NoError(t, err)
	require.Equal(t, newVal, got)
	for i := byte(1); i <= records; i++ {
		_, err := rtx.Get(ns, key(i))
		require.ErrorIs(t, err, ErrCorrupt, "the value of record %d", i)
	}
}

// TestPayloadLimit_OverflowRecordWithinFileSize: the bound does not touch a
// sound record. One of several megabytes in a file that holds it, its key
// spilled too, is read back by the writer's Get inside the transaction that
// wrote it, and after it by ReadTx.Get and Has, a cursor's Key, Value,
// AppendValue and AppendValueByKey, and IntegrityCheck. The tree has one
// leaf, so no interior key is read.
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
	v, err = c.AppendValue(nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(value, v))
	ok, err := rtx.Has(ns, key)
	require.NoError(t, err)
	require.True(t, ok)
	c2 := rtx.NewCursor(ns)
	defer c2.Close()
	v, err = c2.AppendValueByKey(key, nil)
	require.NoError(t, err)
	require.True(t, bytes.Equal(value, v))
	require.NoError(t, db.IntegrityCheck())
}
