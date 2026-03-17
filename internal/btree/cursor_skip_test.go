package btree

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCursorSkip(t *testing.T) {
	db, _ := tempDBWithNS(t, "data")

	const N = 200
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, _ := db.getNamespaceLocked("data")
	for i := 0; i < N; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		require.NoError(t, tx.Put(ns, key, []byte("v")))
	}
	require.NoError(t, tx.Commit())

	newCursor := func(t *testing.T) *Cursor {
		t.Helper()
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns2, _ := db.getNamespaceLocked("data")
		cur := rtx.NewCursor(ns2)
		t.Cleanup(func() {
			cur.Close()
			_ = rtx.Rollback()
		})
		return cur
	}

	keyVal := func(t *testing.T, cur *Cursor) uint32 {
		t.Helper()
		k, err := cur.Key()
		require.NoError(t, err)
		return binary.BigEndian.Uint32(k)
	}

	t.Run("SkipWithinPage", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.First())
		require.True(t, cur.Valid())
		require.NoError(t, cur.Skip(3))
		require.True(t, cur.Valid())
		require.Equal(t, uint32(3), keyVal(t, cur))
	})

	t.Run("SkipAcrossPages", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.First())
		require.NoError(t, cur.Skip(100))
		require.True(t, cur.Valid())
		require.Equal(t, uint32(100), keyVal(t, cur))
	})

	t.Run("SkipZero", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.First())
		require.NoError(t, cur.Skip(0))
		require.True(t, cur.Valid())
		require.Equal(t, uint32(0), keyVal(t, cur))
	})

	t.Run("SkipPastEnd", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.First())
		require.NoError(t, cur.Skip(N+10))
		require.False(t, cur.Valid())
	})

	t.Run("SkipExactEnd", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.First())
		require.NoError(t, cur.Skip(N-1))
		require.True(t, cur.Valid())
		require.Equal(t, uint32(N-1), keyVal(t, cur))
		require.NoError(t, cur.Next())
		require.False(t, cur.Valid())
	})

	t.Run("SkipBackwardWithinPage", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.Last())
		require.True(t, cur.Valid())
		require.NoError(t, cur.SkipBackward(3))
		require.True(t, cur.Valid())
		require.Equal(t, uint32(N-4), keyVal(t, cur))
	})

	t.Run("SkipBackwardAcrossPages", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.Last())
		require.NoError(t, cur.SkipBackward(100))
		require.True(t, cur.Valid())
		require.Equal(t, uint32(N-101), keyVal(t, cur))
	})

	t.Run("SkipBackwardPastBeginning", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.Last())
		require.NoError(t, cur.SkipBackward(N+10))
		require.False(t, cur.Valid())
	})

	t.Run("SkipThenIterate", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.First())
		require.NoError(t, cur.Skip(50))
		require.True(t, cur.Valid())
		for i := 50; i < 55; i++ {
			require.Equal(t, uint32(i), keyVal(t, cur), fmt.Sprintf("at i=%d", i))
			require.NoError(t, cur.Next())
		}
	})

	t.Run("SkipBackwardThenIterate", func(t *testing.T) {
		cur := newCursor(t)
		require.NoError(t, cur.Last())
		require.NoError(t, cur.SkipBackward(50))
		require.True(t, cur.Valid())
		for i := N - 51; i > N-56; i-- {
			require.Equal(t, uint32(i), keyVal(t, cur), fmt.Sprintf("at i=%d", i))
			require.NoError(t, cur.Previous())
		}
	})
}
