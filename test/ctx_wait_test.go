package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// A transaction wait honours the caller's context: the writer lock and the
// reader slots are taken with the ctx of the operation, on the explicit
// ReadTx/WriteTx paths, the own-transaction paths and DDL alike.

func shortCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

func TestCtxWait_WriteLockHeld(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "waits")
	require.NoError(t, err)
	holder, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	short := shortCtx(t)
	_, err = fx.WriteTx(short)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	err = coll.Insert(short, anyenc.MustParseJson(`{"id": 1}`))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	err = coll.EnsureIndex(short, anystore.IndexInfo{Fields: []string{"a"}})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = fx.CreateCollection(short, "other")
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, holder.Commit())
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id": 1}`)))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestCtxWait_ReaderSlotsHeld(t *testing.T) {
	fx := newFixture(t, &anystore.Config{ReadConcurrency: 1})
	coll, err := fx.CreateCollection(ctx, "waits")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id": 1}`)))
	holder, err := fx.ReadTx(ctx)
	require.NoError(t, err)

	short := shortCtx(t)
	_, err = fx.ReadTx(short)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = coll.FindId(short, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = coll.Find(nil).Count(short)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	_, err = coll.Find(nil).Iter(short)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, holder.Commit())
	doc, err := coll.FindId(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, doc.Value().GetInt("id"))
}

func TestCtxWait_TxContextJoinsWithoutWait(t *testing.T) {
	fx := newFixture(t, &anystore.Config{ReadConcurrency: 1})
	coll, err := fx.CreateCollection(ctx, "waits")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id": 1}`)))
	tx, err := fx.ReadTx(ctx) // holds the only reader slot
	require.NoError(t, err)

	// A ctx derived from the transaction's joins it: no slot, no wait.
	txCtx, cancel := context.WithTimeout(tx.Context(), 50*time.Millisecond)
	defer cancel()
	doc, err := coll.FindId(txCtx, 1)
	require.NoError(t, err)
	require.Equal(t, 1, doc.Value().GetInt("id"))
	count, err := coll.Find(nil).Count(txCtx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	require.NoError(t, tx.Commit())
}

func TestCtxWait_PreCancelled(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "waits")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id": 1}`)))
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = fx.WriteTx(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = fx.ReadTx(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = coll.FindId(cancelled, 1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = coll.Find(nil).Count(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	err = coll.Insert(cancelled, anyenc.MustParseJson(`{"id": 2}`))
	require.ErrorIs(t, err, context.Canceled)
	err = coll.EnsureIndex(cancelled, anystore.IndexInfo{Fields: []string{"a"}})
	require.ErrorIs(t, err, context.Canceled)

	// Nothing happened, and the resources are free.
	require.Empty(t, coll.GetIndexes())
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestTxFromContext(t *testing.T) {
	fx := newFixture(t)
	_, ok := anystore.TxFromContext(ctx)
	require.False(t, ok)

	rtx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	got, ok := anystore.TxFromContext(rtx.Context())
	require.True(t, ok)
	_, isWrite := got.(anystore.WriteTx)
	require.False(t, isWrite)
	require.False(t, got.Done())
	require.NoError(t, rtx.Commit())
	require.True(t, got.Done(), "the carried handle is the transaction itself")

	wtx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	derived, cancel := context.WithCancel(wtx.Context())
	defer cancel()
	got, ok = anystore.TxFromContext(derived)
	require.True(t, ok)
	_, isWrite = got.(anystore.WriteTx)
	require.True(t, isWrite)
	// A savepoint's context carries the transaction it sits on.
	sp, err := fx.WriteTx(wtx.Context())
	require.NoError(t, err)
	got, ok = anystore.TxFromContext(sp.Context())
	require.True(t, ok)
	_, isWrite = got.(anystore.WriteTx)
	require.True(t, isWrite)
	require.NoError(t, sp.Commit())
	require.NoError(t, wtx.Commit())
}

func TestCtxWait_CloseEndsBackgroundWaiter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "waits")
	require.NoError(t, err)
	_, err = fx.WriteTx(ctx) // held and never ended
	require.NoError(t, err)
	errCh := make(chan error, 1)
	go func() { errCh <- coll.Insert(ctx, anyenc.MustParseJson(`{"id": 1}`)) }()
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, fx.Close())
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, anystore.ErrDBIsClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("the waiter did not return after Close")
	}
}
