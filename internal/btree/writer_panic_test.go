package btree

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A panic inside Commit/Rollback (btree code can panic on corrupt pages) must
// not leak writeMu/db.mu: the tx has already marked itself closed, so a later
// Rollback is an ErrTxClosed no-op and no caller-side recover can release the
// locks — every subsequent BeginWrite and Close would deadlock permanently.
// The release lives in a defer; these tests panic at the exact pre-pager spot
// via testWriterFinishHook and then require the DB to keep working.

func testWriterPanicReleasesLocks(t *testing.T, finish func(tx *WriteTx) error) {
	db := tempDB(t)

	// A committed row from before the panic must survive it.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := wtx.CreateNamespace("t")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("k0"), []byte("v0")))
	require.NoError(t, wtx.Commit())

	wtx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = wtx.GetNamespace("t")
	require.NoError(t, err)
	require.NoError(t, wtx.Put(ns, []byte("k1"), []byte("v1")))

	testWriterFinishHook = func() { panic("injected pager panic") }
	defer func() { testWriterFinishHook = nil }()
	func() {
		defer func() {
			require.NotNil(t, recover(), "the injected panic must propagate to the caller")
		}()
		_ = finish(wtx)
	}()
	testWriterFinishHook = nil

	// The headline symptom: BeginWrite and Close deadlocked forever. Run the
	// full cycle in a goroutine and require it to finish.
	done := make(chan error, 1)
	go func() {
		wtx2, werr := db.BeginWrite()
		if werr != nil {
			done <- werr
			return
		}
		ns2, werr := wtx2.GetNamespace("t")
		if werr != nil {
			_ = wtx2.Rollback()
			done <- werr
			return
		}
		if werr = wtx2.Put(ns2, []byte("k2"), []byte("v2")); werr != nil {
			_ = wtx2.Rollback()
			done <- werr
			return
		}
		done <- wtx2.Commit()
	}()
	select {
	case err = <-done:
		require.NoError(t, err, "the writer after the panic must succeed")
	case <-time.After(10 * time.Second):
		t.Fatal("DEADLOCK: the writer locks leaked through the panic")
	}

	// The interrupted tx's writes were discarded; earlier and later commits
	// are intact.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err = rtx.GetNamespace("t")
	require.NoError(t, err)
	v, err := rtx.Get(ns, []byte("k0"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v0"), v)
	v, err = rtx.Get(ns, []byte("k2"))
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), v)
	_, err = rtx.Get(ns, []byte("k1"))
	assert.ErrorIs(t, err, ErrKeyNotFound, "the panicked tx's write must be discarded")
	require.NoError(t, rtx.Rollback())

	require.NoError(t, db.Close(), "Close must not deadlock after a writer panic")
}

func TestWriterPanicInCommitReleasesLocks(t *testing.T) {
	testWriterPanicReleasesLocks(t, func(tx *WriteTx) error { return tx.Commit() })
}

func TestWriterPanicInRollbackReleasesLocks(t *testing.T) {
	testWriterPanicReleasesLocks(t, func(tx *WriteTx) error { return tx.Rollback() })
}
