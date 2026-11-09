package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

// A running find, count, update or delete polls its context: the btree
// cursor once per leaf it leaves, the namespace count once per page. A
// trippingCtx fails on its n-th poll, which makes these tests deterministic:
// n is past the entry checks, so the scan is under way when it trips.

type trippingCtx struct {
	context.Context
	after int32
	polls atomic.Int32
	done  chan struct{}
	once  sync.Once
}

func newTrippingCtx(parent context.Context, after int32) *trippingCtx {
	return &trippingCtx{Context: parent, after: after, done: make(chan struct{})}
}

func (c *trippingCtx) tripped() bool { return c.polls.Load() >= c.after }

func (c *trippingCtx) Err() error {
	if c.polls.Add(1) >= c.after {
		c.once.Do(func() { close(c.done) })
		return context.Canceled
	}
	return c.Context.Err()
}

func (c *trippingCtx) Done() <-chan struct{} { return c.done }

const scanDocs = 30000

// newScanFixture is a one-slot DB with a collection large enough for a scan
// to cross many leaves.
func newScanFixture(t *testing.T) (*fixture, anystore.Collection) {
	t.Helper()
	fx := newFixture(t, &anystore.Config{ReadConcurrency: 1})
	coll, err := fx.CreateCollection(ctx, "scan")
	require.NoError(t, err)
	docs := make([]*anyenc.Value, 0, scanDocs)
	for i := 0; i < scanDocs; i++ {
		docs = append(docs, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":"x"}`, i, i%100)))
	}
	require.NoError(t, coll.Insert(ctx, docs...))
	return fx, coll
}

// slotFree asserts the only reader slot, the cancelled scan's, is free again.
func slotFree(t *testing.T, fx *fixture) {
	t.Helper()
	tx, err := fx.ReadTx(shortCtx(t))
	require.NoError(t, err, "the reader slot of the cancelled scan must be free")
	require.NoError(t, tx.Commit())
}

// walRestarts asserts a TRUNCATE checkpoint empties the WAL: no read mark of
// the cancelled scan pins it.
func walRestarts(t *testing.T, fx *fixture) {
	t.Helper()
	if fx.tmpDir == "" {
		return
	}
	require.NoError(t, fx.Flush(ctx, 0, anystore.FlushModeCheckpointTruncate))
	st, err := os.Stat(filepath.Join(fx.tmpDir, "any-store-test.db-wal"))
	require.NoError(t, err)
	require.Zero(t, st.Size(), "the WAL must be empty after the truncate checkpoint")
}

func TestCtxScan_FullScanNothingMatches(t *testing.T) {
	fx, coll := newScanFixture(t)
	tc := newTrippingCtx(ctx, 4)
	it, err := coll.Find(`{"b": "nope"}`).Iter(tc)
	require.NoError(t, err)
	require.False(t, it.Next())
	require.ErrorIs(t, it.Err(), context.Canceled)
	require.True(t, tc.tripped())
	require.NoError(t, it.Close())
	slotFree(t, fx)
	walRestarts(t, fx)
}

func TestCtxScan_SortCollects(t *testing.T) {
	fx, coll := newScanFixture(t)
	tc := newTrippingCtx(ctx, 4)
	it, err := coll.Find(nil).Sort("-a").Iter(tc)
	require.NoError(t, err)
	require.False(t, it.Next())
	require.ErrorIs(t, it.Err(), context.Canceled)
	require.NoError(t, it.Close())
	slotFree(t, fx)
}

func TestCtxScan_Count(t *testing.T) {
	fx, coll := newScanFixture(t)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	for name, q := range map[string]anystore.Query{
		"filterless": coll.Find(nil),
		"filtered":   coll.Find(`{"b": "nope"}`),
		"index":      coll.Find(`{"a": {"$gte": 0}}`).IndexHint(anystore.IndexHint{IndexName: "a", Boost: 1_000_000}),
	} {
		t.Run(name, func(t *testing.T) {
			tc := newTrippingCtx(ctx, 4)
			_, err := q.Count(tc)
			require.ErrorIs(t, err, context.Canceled)
			require.True(t, tc.tripped())
			slotFree(t, fx)
		})
	}
}

func TestCtxScan_OffsetSkip(t *testing.T) {
	fx, coll := newScanFixture(t)
	tc := newTrippingCtx(ctx, 4)
	it, err := coll.Find(nil).Offset(scanDocs - 10).Limit(1).Iter(tc)
	require.NoError(t, err)
	require.False(t, it.Next())
	require.ErrorIs(t, it.Err(), context.Canceled)
	require.NoError(t, it.Close())
	slotFree(t, fx)
}

func TestCtxScan_UpdateScanRollsBack(t *testing.T) {
	fx, coll := newScanFixture(t)
	tc := newTrippingCtx(ctx, 4)
	_, err := coll.Find(`{"b": "x"}`).Update(tc, `{"$set": {"c": 1}}`)
	require.ErrorIs(t, err, context.Canceled)
	n, err := coll.Find(`{"c": 1}`).Count(ctx)
	require.NoError(t, err)
	require.Zero(t, n)
	// The writer lock is free.
	require.NoError(t, coll.Insert(shortCtx(t), anyenc.MustParseJson(`{"id": -1}`)))
	_ = fx
}

func TestCtxScan_UpdateMutationLoopPolls(t *testing.T) {
	_, coll := newScanFixture(t)
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	calls := 0
	mod := query.ModifyFunc(func(a *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		calls++
		if calls == 5 {
			cancel() // the scan is over; the mutation loop polls every 1024 ids
		}
		v.Set("c", a.NewNumberInt(1))
		return v, true, nil
	})
	_, err := coll.Find(nil).Update(cctx, mod)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, calls, scanDocs)
	n, err := coll.Find(`{"c": 1}`).Count(ctx)
	require.NoError(t, err)
	require.Zero(t, n, "the mutations before the ctx ended are rolled back")
}

func TestCtxScan_DeleteInUserTxRollsBackSavepoint(t *testing.T) {
	fx, coll := newScanFixture(t)
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	tc := newTrippingCtx(tx.Context(), 4)
	_, err = coll.Find(nil).Delete(tc)
	require.ErrorIs(t, err, context.Canceled)
	// The transaction stays open and usable.
	n, err := coll.Find(nil).Count(tx.Context())
	require.NoError(t, err)
	require.Equal(t, scanDocs, n)
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id": -1}`)))
	require.NoError(t, tx.Commit())
	n, err = coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, scanDocs+1, n)
}

func TestCtxScan_PreCancelledInTx(t *testing.T) {
	fx, coll := newScanFixture(t)
	tx, err := fx.ReadTx(ctx)
	require.NoError(t, err)
	cancelled, cancel := context.WithCancel(tx.Context())
	cancel()
	_, err = coll.Find(nil).Iter(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	_, err = coll.Find(nil).Count(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	n, err := coll.Find(nil).Count(tx.Context())
	require.NoError(t, err)
	require.Equal(t, scanDocs, n)
	require.NoError(t, tx.Commit())
}

func TestCtxScan_AggregatePushedDownMatch(t *testing.T) {
	fx, coll := newScanFixture(t)
	tc := newTrippingCtx(ctx, 4)
	// The $match is pushed into the find: no row reaches the stages, whose
	// own polling counts consumed rows, so the scan's polling has to end it.
	it, err := coll.Aggregate(`[{"$match": {"b": "nope"}}, {"$count": "n"}]`).Iter(tc)
	require.NoError(t, err)
	require.False(t, it.Next())
	require.ErrorIs(t, it.Err(), context.Canceled)
	require.NoError(t, it.Close())
	slotFree(t, fx)
}

func TestCtxScan_ReverseScan(t *testing.T) {
	fx, coll := newScanFixture(t)
	tc := newTrippingCtx(ctx, 4)
	it, err := coll.Find(nil).Sort("-id").Iter(tc) // primary-key order reversed: a reverse scan
	require.NoError(t, err)
	n := 0
	for it.Next() {
		n++
	}
	require.ErrorIs(t, it.Err(), context.Canceled)
	require.Positive(t, n, "the first leaves are yielded before the poll trips")
	require.Less(t, n, scanDocs)
	require.NoError(t, it.Close())
	slotFree(t, fx)
}

func TestCtxScan_IndexCountBoundedEnd(t *testing.T) {
	fx, coll := newScanFixture(t)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Name: "a", Fields: []string{"a"}}))
	tc := newTrippingCtx(ctx, 4)
	_, err := coll.Find(`{"a": {"$gte": 0, "$lte": 90}}`).
		IndexHint(anystore.IndexHint{IndexName: "a", Boost: 1_000_000}).Count(tc)
	require.ErrorIs(t, err, context.Canceled)
	slotFree(t, fx)
}

func TestCtxScan_ExplainCountWalk(t *testing.T) {
	_, coll := newScanFixture(t)
	// No index, so Explain's document count walks the namespace, polling
	// once per page.
	tc := newTrippingCtx(ctx, 3)
	_, err := coll.Find(`{"b": "nope"}`).Explain(tc)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, tc.tripped())
}

func TestCtxScan_TxContextEndedBeginsNothing(t *testing.T) {
	fx, coll := newScanFixture(t)
	bctx, cancel := context.WithCancel(ctx)
	tx, err := fx.WriteTx(bctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id": -1}`)))
	cancel()
	// Every operation run with the transaction's context begins nothing.
	require.ErrorIs(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id": -2}`)), context.Canceled)
	_, err = coll.FindId(tx.Context(), -1)
	require.ErrorIs(t, err, context.Canceled)
	_, err = coll.Find(nil).Count(tx.Context())
	require.ErrorIs(t, err, context.Canceled)
	_, err = fx.WriteTx(tx.Context())
	require.ErrorIs(t, err, context.Canceled, "a savepoint begins nothing either")
	// The transaction stays open: Commit does not consult the context.
	require.NoError(t, tx.Commit())
	n, err := coll.Find(`{"id": -1}`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}
