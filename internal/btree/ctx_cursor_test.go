package btree

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// tripCtx fails on its n-th Err poll.
type tripCtx struct {
	context.Context
	after, polls int
}

func (c *tripCtx) Err() error {
	c.polls++
	if c.polls >= c.after {
		return context.Canceled
	}
	return nil
}

func fillLeaves(t *testing.T, db *DB, ns *Namespace, n int) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	val := make([]byte, 64)
	for i := 1; i <= n; i++ {
		require.NoError(t, tx.Put(ns, binary.BigEndian.AppendUint32(nil, uint32(i)), val))
	}
	require.NoError(t, tx.Commit())
}

// A cursor with a ctx polls it once per leaf it leaves; a failed move leaves
// the cursor on its entry, readable and movable within the leaf.
func TestCursorContext_PollsOncePerLeaf(t *testing.T) {
	db, ns := tempDBWithNS(t, "ns")
	fillLeaves(t, db, ns, 5000)
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	tc := &tripCtx{Context: context.Background(), after: 3}
	c := rtx.NewCursorContext(tc, ns)
	defer c.Close()
	require.NoError(t, c.First())
	var last []byte
	moved := 0
	for {
		k, err := c.Key()
		require.NoError(t, err)
		last = append(last[:0], k...)
		if err := c.Next(); err != nil {
			require.ErrorIs(t, err, context.Canceled)
			break
		}
		moved++
		require.True(t, c.Valid())
	}
	require.Equal(t, 3, tc.polls, "one poll per leaf left")
	require.Greater(t, moved, 2)

	// Still on the entry it stood on, readable; a later move fails again.
	require.True(t, c.Valid())
	k, err := c.Key()
	require.NoError(t, err)
	require.Equal(t, last, k)
	_, err = c.Value()
	require.NoError(t, err)
	require.ErrorIs(t, c.Next(), context.Canceled)
	k, err = c.Key()
	require.NoError(t, err)
	require.Equal(t, last, k)

	// Moves within the leaf go on; Skip and CountUntil fail at the leaf's end.
	require.NoError(t, c.Previous())
	require.True(t, c.Valid())
	require.ErrorIs(t, c.Skip(10_000), context.Canceled)
	_, err = c.CountUntil(nil, true)
	require.ErrorIs(t, err, context.Canceled)
}

// A writer's cursor survives the poll's failure as it survives a write: it
// is saved and restored on the next read.
func TestCursorContext_WriterCursorSurvivesPoll(t *testing.T) {
	db, ns := tempDBWithNS(t, "ns")
	fillLeaves(t, db, ns, 5000)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	defer func() { _ = wtx.Rollback() }()

	tc := &tripCtx{Context: context.Background(), after: 2}
	c := wtx.NewCursorContext(tc, ns)
	defer c.Close()
	require.NoError(t, c.First())
	var last []byte
	for {
		k, err := c.Key()
		require.NoError(t, err)
		last = append(last[:0], k...)
		if err := c.Next(); err != nil {
			require.ErrorIs(t, err, context.Canceled)
			break
		}
	}
	// A write saves the cursor; the read restores it on the same key.
	require.NoError(t, wtx.Put(ns, binary.BigEndian.AppendUint32(nil, 100_000), []byte("x")))
	k, err := c.Key()
	require.NoError(t, err)
	require.Equal(t, last, k)
	require.ErrorIs(t, c.Next(), context.Canceled)
	require.NoError(t, wtx.Commit())
}
