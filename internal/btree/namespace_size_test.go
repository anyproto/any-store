package btree

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadTx_NamespaceSize(t *testing.T) {
	t.Run("empty namespace", func(t *testing.T) {
		db, _ := tempDBWithNS(t, "ns")
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer rtx.Rollback()

		ns, err := rtx.GetNamespace("ns")
		require.NoError(t, err)
		sz, err := rtx.NamespaceSize(ns)
		require.NoError(t, err)
		assert.Equal(t, 1, sz.Pages) // a single empty root leaf
		assert.Equal(t, 0, sz.Entries)
		assert.Equal(t, 0, sz.OverflowPages)
		assert.Equal(t, 0, sz.PayloadBytes)
	})

	t.Run("small entries", func(t *testing.T) {
		db, ns := tempDBWithNS(t, "ns")
		const n = 500
		wantPayload := 0
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		for i := 0; i < n; i++ {
			key := []byte(fmt.Sprintf("key-%05d", i))
			val := []byte(fmt.Sprintf("value-%05d", i))
			require.NoError(t, tx.Put(ns, key, val))
			wantPayload += len(key) + len(val)
		}
		require.NoError(t, tx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer rtx.Rollback()
		ns2, err := rtx.GetNamespace("ns")
		require.NoError(t, err)

		sz, err := rtx.NamespaceSize(ns2)
		require.NoError(t, err)
		assert.Equal(t, n, sz.Entries)
		assert.Equal(t, wantPayload, sz.PayloadBytes)
		assert.Equal(t, 0, sz.OverflowPages)
		// 500 entries must span more than one leaf, so there is at least one
		// interior page plus several leaves.
		assert.Greater(t, sz.Pages, 1)

		// Entry count agrees with the existing Count walk.
		cnt, err := rtx.Count(ns2)
		require.NoError(t, err)
		assert.Equal(t, cnt, sz.Entries)
	})

	t.Run("overflow value", func(t *testing.T) {
		db, ns := tempDBWithNS(t, "ns")
		key := []byte("big")
		val := bytes.Repeat([]byte("x"), 64*1024) // far exceeds one page

		tx, err := db.BeginWrite()
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, key, val))
		require.NoError(t, tx.Commit())

		rtx, err := db.BeginRead()
		require.NoError(t, err)
		defer rtx.Rollback()
		ns2, err := rtx.GetNamespace("ns")
		require.NoError(t, err)

		sz, err := rtx.NamespaceSize(ns2)
		require.NoError(t, err)
		assert.Equal(t, 1, sz.Entries)
		assert.Equal(t, len(key)+len(val), sz.PayloadBytes)
		assert.Greater(t, sz.OverflowPages, 0)
	})
}
