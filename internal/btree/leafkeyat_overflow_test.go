package btree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// spilledIndexKey returns an index-like key of keyLen bytes: a constant run
// followed by v as ten decimal digits, so keys differ only in their last bytes,
// far past the local part of a spilled cell, and sort by v.
func spilledIndexKey(v, keyLen int) []byte {
	return fmt.Appendf(bytes.Repeat([]byte{'k'}, keyLen-10), "%010d", v)
}

// spilledKeyLen is a key length that spills with an empty value.
func spilledKeyLen(usableSize int) int {
	return maxLocalPayload(usableSize) + 100
}

func TestLeafKeyAt_SpilledKey(t *testing.T) {
	p := tempPager(t)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	defer p.releasePage(pg)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usable := p.usableSize()
	keyLen := spilledKeyLen(usable)

	// Six index cells whose keys continue in overflow chains, then one data
	// cell whose short key is local while its value spills.
	const nSpilled = 6
	var cells []cellData
	for i := range nSpilled {
		cells = append(cells, cellData{key: spilledIndexKey(i, keyLen)})
	}
	localKey := []byte("local-key")
	cells = append(cells, cellData{key: localKey, value: bytes.Repeat([]byte{'v'}, 3*maxLocalPayload(usable))})
	require.NoError(t, bt.rebuildLeafPage(pg, cells))

	for i := range nSpilled {
		off, oerr := pg.getCellOffsetSafe(i)
		require.NoError(t, oerr)
		c, _, cerr := parseLeafCellWithSize(pg.data, int(off), usable)
		require.NoError(t, cerr)
		require.NotZero(t, c.overflowPg, "cell %d spills", i)
		require.Less(t, len(c.key), keyLen, "cell %d keeps only a key prefix on the page", i)

		got, kerr := leafKeyAt(pg, i, usable)
		assert.ErrorIs(t, kerr, errLeafKeySpilled, "cell %d", i)
		assert.Nil(t, got, "cell %d", i)
	}

	got, kerr := leafKeyAt(pg, nSpilled, usable)
	require.NoError(t, kerr)
	assert.Equal(t, localKey, got)
}

// An exact seek on a spilled index key finds it: the equality check reads the
// key reassembled from the overflow chain, not the page bytes after the local
// prefix.
func TestCursor_SeekExactSpilledKey(t *testing.T) {
	db, ns := tempDBWithNS(t, "idx")
	keyLen := spilledKeyLen(db.pager.usableSize())
	const n = 6 // one leaf
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := range n {
		require.NoError(t, tx.Put(ns, spilledIndexKey(2*i, keyLen), nil))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	rns, err := db.getNamespaceLocked("idx")
	require.NoError(t, err)
	cur := rtx.NewCursor(rns)
	defer cur.Close()

	for i := range n {
		key := spilledIndexKey(2*i, keyLen)
		require.NoError(t, cur.SeekExact(key), "key %d", i)
		got, kerr := cur.Key()
		require.NoError(t, kerr)
		assert.Equal(t, key, got, "key %d", i)
		_, verr := cur.AppendValueByKey(key, nil)
		assert.NoError(t, verr, "key %d", i)
	}

	// An absent key that lands on a present one and differs only in its last
	// byte is still a miss.
	absent := spilledIndexKey(3, keyLen)
	assert.ErrorIs(t, cur.SeekExact(absent), ErrKeyNotFound)
	_, verr := cur.AppendValueByKey(absent, nil)
	assert.ErrorIs(t, verr, ErrKeyNotFound)
}

// SeekNear positions correctly from a leaf whose first and last keys spill:
// the in-page range check cannot use them, and the seek takes the full path.
func TestCursor_SeekNearSpilledKeys(t *testing.T) {
	db, ns := tempDBWithNS(t, "idx")
	keyLen := spilledKeyLen(db.pager.usableSize())
	const n = 40 // several leaves
	keys := make([][]byte, n)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	for i := range n {
		keys[i] = spilledIndexKey(2*i, keyLen)
		require.NoError(t, tx.Put(ns, keys[i], nil))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	rns, err := db.getNamespaceLocked("idx")
	require.NoError(t, err)
	cur := rtx.NewCursor(rns)
	defer cur.Close()

	// after(i) sorts between keys[i] and keys[i+1].
	after := func(i int) []byte { return spilledIndexKey(2*i+1, keyLen) }
	for _, from := range []int{0, 10, 25, n - 1} {
		targets := []struct {
			target []byte
			want   int // index of the first key >= target; n means past the end
		}{
			{keys[from], from},
			{after(from), from + 1},
			{keys[(from+1)%n], (from + 1) % n},
			{keys[(from+n/2)%n], (from + n/2) % n},
			{keys[0], 0},
			{keys[n-1], n - 1},
			{after(n - 1), n},
		}
		for _, tc := range targets {
			// Pin the leaf holding keys[from], then seek near the target.
			require.NoError(t, cur.Seek(keys[from]))
			require.True(t, cur.Valid())
			require.NoError(t, cur.SeekNear(tc.target), "from %d", from)
			if tc.want == n {
				assert.False(t, cur.Valid(), "from %d: a target past the end leaves the cursor invalid", from)
				continue
			}
			require.True(t, cur.Valid(), "from %d want %d", from, tc.want)
			got, kerr := cur.Key()
			require.NoError(t, kerr)
			assert.Equal(t, keys[tc.want], got, "from %d want %d", from, tc.want)
		}
	}
}
