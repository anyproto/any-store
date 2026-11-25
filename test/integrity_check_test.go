package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// IntegrityCheck's report of a damaged database matches ErrCorrupt, so a
// caller handles it as it handles damage met by a read. A sound database
// passes, and a cancelled check is the context's error, not corruption.
func TestIntegrityCheck_DamageMatchesErrCorrupt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "a.db")
	db, err := anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 300; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"pad":"%0400d"}`, i, i))))
	}
	require.NoError(t, db.IntegrityCheck(ctx))

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	err = db.IntegrityCheck(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, anystore.ErrCorrupt)
	require.NoError(t, db.Close())

	// Damage the last page of the file: a page of the collection, which
	// the open does not read.
	info, err := os.Stat(path)
	require.NoError(t, err)
	flipByte(t, path, info.Size()-2000)

	db, err = anystore.Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	err = db.IntegrityCheck(ctx)
	require.ErrorIs(t, err, anystore.ErrCorrupt)
}

// IntegrityCheck on a closed database is ErrDBIsClosed: a check that cannot
// run says nothing about the file, and must not read as damage.
func TestIntegrityCheck_ClosedIsErrDBIsClosed(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "a.db"), nil)
	require.NoError(t, err)
	_, err = db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, db.Close())

	err = db.IntegrityCheck(ctx)
	require.ErrorIs(t, err, anystore.ErrDBIsClosed)
	require.NotErrorIs(t, err, anystore.ErrCorrupt)
}

// IntegrityCheck runs in the read transaction its context carries, and so
// needs no reader slot of its own: with one slot, held by the caller's
// transaction, it runs. Without a transaction in the context it waits for a
// slot until the context ends. A write transaction in the context holds no
// reader slot, and the check reads the committed state beside it.
func TestIntegrityCheck_ReadSlot(t *testing.T) {
	ctx := context.Background()
	db, err := anystore.Open(ctx, filepath.Join(t.TempDir(), "a.db"), &anystore.Config{ReadConcurrency: 1})
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d}`, i))))
	}

	within := func(t *testing.T, d time.Duration, f func() error) error {
		t.Helper()
		done := make(chan error, 1)
		go func() { done <- f() }()
		select {
		case err := <-done:
			return err
		case <-time.After(d):
			t.Fatal("IntegrityCheck did not return")
			return nil
		}
	}

	t.Run("in the caller's read transaction", func(t *testing.T) {
		rtx, err := db.ReadTx(ctx)
		require.NoError(t, err)
		defer rtx.Commit()
		require.NoError(t, within(t, 10*time.Second, func() error { return db.IntegrityCheck(rtx.Context()) }))
	})
	t.Run("waiting for a slot another reader holds", func(t *testing.T) {
		rtx, err := db.ReadTx(ctx)
		require.NoError(t, err)
		defer rtx.Commit()
		dctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		err = within(t, 10*time.Second, func() error { return db.IntegrityCheck(dctx) })
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.NotErrorIs(t, err, anystore.ErrCorrupt)
	})
	t.Run("beside the caller's write transaction", func(t *testing.T) {
		wtx, err := db.WriteTx(ctx)
		require.NoError(t, err)
		defer wtx.Rollback()
		require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":"uncommitted"}`)))
		require.NoError(t, within(t, 10*time.Second, func() error { return db.IntegrityCheck(wtx.Context()) }))
	})
	// Beside a write transaction the check does not wait for the slot: its
	// caller holds the writer, and here the slot too, in a read transaction
	// it ends only once the check has returned. With no deadline in ctx a
	// wait would never end.
	t.Run("beside the caller's write transaction, the slot held", func(t *testing.T) {
		rtx, err := db.ReadTx(ctx)
		require.NoError(t, err)
		wtx, err := db.WriteTx(ctx)
		if err != nil {
			_ = rtx.Commit()
			require.NoError(t, err)
		}
		defer wtx.Rollback()
		// First: the slot given back ends a check that waits for it.
		defer rtx.Commit()
		err = within(t, 10*time.Second, func() error { return db.IntegrityCheck(wtx.Context()) })
		require.ErrorIs(t, err, anystore.ErrBusy)
		require.NotErrorIs(t, err, anystore.ErrCorrupt)
		require.NoError(t, wtx.Rollback())
		require.NoError(t, rtx.Commit())
		require.NoError(t, db.IntegrityCheck(ctx))
	})
}
