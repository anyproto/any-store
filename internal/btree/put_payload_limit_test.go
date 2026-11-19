package btree

import (
	"bytes"
	"fmt"
	"path/filepath"
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
// test — accepts its lengths.
func TestPayloadLimit_RealConstants(t *testing.T) {
	require.Equal(t, 1<<30, maxPayloadAlloc)
	require.Equal(t, maxPayloadAlloc, payloadLimit)
	require.Equal(t, maxPayloadAlloc, maxKeySize)

	const usable = 4096
	leafLim := indexLimits(usable)
	const L = maxPayloadAlloc
	for _, tc := range []struct {
		keyLen, valLen int
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
		{-1, 0, false},
		{0, -1, false},
	} {
		name := fmt.Sprintf("key=%d,value=%d", tc.keyLen, tc.valLen)
		require.Equal(t, tc.fits, payloadFits(tc.keyLen, tc.valLen), name)
		if tc.keyLen < 0 || tc.valLen < 0 {
			continue
		}
		// A leaf cell that declares these lengths: the header, the local
		// part of the payload and the overflow pointer (any page but 0) are
		// all a parser looks at.
		cell := bytes.Repeat([]byte{1}, usable)
		n := putVarint(cell, uint64(tc.keyLen))
		n += putVarint(cell[n:], uint64(tc.valLen))
		_, _, err := parseLeafCellWithSize(cell, 0, leafLim)
		if tc.fits {
			require.NoError(t, err, name)
		} else {
			require.ErrorIs(t, err, ErrCorrupt, name)
		}
		// An interior cell has a key only.
		if tc.valLen == 0 {
			icell := bytes.Repeat([]byte{1}, usable)
			putVarint(icell[4:], uint64(tc.keyLen))
			_, _, err := parseInteriorCell(icell, 0, usable)
			if tc.fits {
				require.NoError(t, err, name)
			} else {
				require.ErrorIs(t, err, ErrCorrupt, name)
			}
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
		{"key overflows", limit, 0, true, ErrKeyTooLarge},
		{"key and value overflow", 5000, limit - 5000, false, ErrValueTooLarge},
		{"key and value overflow, key grows", 5000, limit - 5000, true, ErrValueTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setPayloadLimit(t, limit)
			path := filepath.Join(t.TempDir(), "limit.db")
			db, err := testOpen(t, path, Options{PageSize: 4096})
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			neighbours := [][]byte{{0x00, 'j'}, {0xff, 'l'}}
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.CreateNamespace("t")
			require.NoError(t, err)
			for _, k := range neighbours {
				require.NoError(t, tx.Put(ns, k, []byte("neighbour")))
			}
			require.NoError(t, tx.Commit())

			// The first byte keeps the key between the neighbours.
			key := payloadBytes(tc.keyLen, 3)
			key[0] = 0x50
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
			defer func() { _ = db.Close() }()
			readBack(db)
			require.NoError(t, db.IntegrityCheck())

			// The read side has the same boundary: with the limit one byte
			// lower the stored record is the one Put would have refused,
			// and the read reports the cell as corrupt.
			payloadLimit = limit - 1
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			ns, err = rtx.GetNamespace("t")
			require.NoError(t, err)
			_, err = rtx.Get(ns, key)
			require.ErrorIs(t, err, ErrCorrupt)
			require.NoError(t, rtx.Rollback())
			payloadLimit = limit

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
// same Put, so a namespace name is held to the same limit.
func TestPayloadLimit_NamespaceName(t *testing.T) {
	const limit = 2*4096 + 5
	setPayloadLimit(t, limit)
	db, err := testOpen(t, filepath.Join(t.TempDir(), "ns.db"), Options{PageSize: 4096})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// The record is the name and a 4-byte root page number.
	atLimit := string(bytes.Repeat([]byte("n"), limit-4))
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace(atLimit + "x")
	require.ErrorIs(t, err, ErrValueTooLarge)
	require.False(t, tx.HasChanges(), "a refused CreateNamespace changed a page")
	_, err = tx.CreateNamespace("short")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// A rename to a name over the limit is refused before the old name is
	// removed.
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	require.ErrorIs(t, tx.RenameNamespace("short", atLimit+"x"), ErrValueTooLarge)
	require.False(t, tx.HasChanges(), "a refused RenameNamespace changed a page")
	require.NoError(t, tx.RenameNamespace("short", atLimit))
	require.NoError(t, tx.Commit())
	_, err = db.GetNamespace(atLimit)
	require.NoError(t, err)
	_, err = db.GetNamespace("short")
	require.ErrorIs(t, err, ErrNamespaceNotFound)
	require.NoError(t, db.IntegrityCheck())
}
