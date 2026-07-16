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

// TestWriterPanicMidCommit exercises the unwind from INSIDE pager.commit, on
// both sides of the publish boundary — the pre-pager hook alone would leave
// the writerOpMu interplay and the published-commit branch of unwindWriter
// untested:
//
//   - pre-publish (before wal.writeFrames): the transaction must be DISCARDED,
//     exactly like the pre-pager panic;
//   - post-publish (after wal.writeFrames wrote the commit frame): the
//     transaction is durably COMMITTED — the unwind must NOT roll it back.
//     Rewinding here would zero WAL-index hash entries below the advanced
//     mxCommitFrame and break the checksum chain: committed data silently
//     lost, on disk too.
//
// Both paths must release every lock (follow-up writer + Close complete) and
// keep the on-disk file consistent across reopen.
func TestWriterPanicMidCommit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		hook          *func()
		wantCommitted bool
	}{
		{"pre-publish", &testCommitPrePublishHook, false},
		{"post-publish", &testCommitPostPublishHook, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := dir + "/panic.db"
			db, err := Open(path, Options{PageSize: 4096, CacheSize: 128})
			require.NoError(t, err)

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

			*tc.hook = func() { panic("injected mid-commit panic") }
			defer func() { *tc.hook = nil }()
			func() {
				defer func() {
					require.NotNil(t, recover(), "the injected panic must propagate")
				}()
				_ = wtx.Commit()
			}()
			*tc.hook = nil

			require.NoError(t, db.LastWriterUnwindError(),
				"the unwind cleanup itself must succeed")

			// Locks must be free: a full follow-up write cycle completes.
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
				t.Fatal("DEADLOCK: writer locks (Go mutexes or the WAL write lock) leaked")
			}

			checkRow := func(rtx *ReadTx, key string, want []byte, present bool, msg string) {
				t.Helper()
				nsr, gerr := rtx.GetNamespace("t")
				require.NoError(t, gerr)
				v, gerr := rtx.Get(nsr, []byte(key))
				if present {
					require.NoError(t, gerr, msg)
					assert.Equal(t, want, v, msg)
				} else {
					assert.ErrorIs(t, gerr, ErrKeyNotFound, msg)
				}
			}

			rtx, err := db.BeginRead()
			require.NoError(t, err)
			checkRow(rtx, "k0", []byte("v0"), true, "pre-panic commit intact")
			checkRow(rtx, "k2", []byte("v2"), true, "post-panic commit intact")
			checkRow(rtx, "k1", []byte("v1"), tc.wantCommitted,
				"pre-publish panics discard the tx; post-publish panics must PRESERVE the published commit")
			require.NoError(t, rtx.Rollback())

			require.NoError(t, db.Close(), "Close must not deadlock")

			// Reopen from disk: WAL recovery must agree with what the live
			// handle reported — a broken frame/checksum chain would surface
			// here as lost or resurrected rows.
			db2, err := Open(path, Options{PageSize: 4096, CacheSize: 128})
			require.NoError(t, err)
			rtx2, err := db2.BeginRead()
			require.NoError(t, err)
			checkRow(rtx2, "k0", []byte("v0"), true, "reopen: pre-panic commit")
			checkRow(rtx2, "k2", []byte("v2"), true, "reopen: post-panic commit")
			checkRow(rtx2, "k1", []byte("v1"), tc.wantCommitted, "reopen: panicked tx")
			require.NoError(t, rtx2.Rollback())
			require.NoError(t, db2.Close())
		})
	}
}
