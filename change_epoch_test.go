//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// Another process's commit reaches this process's sketches whichever
// transaction of this process is the first to begin after it, and a failed
// or busy Backup leaves what it found. The peer is a real process (re-exec).

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

const (
	changePeerEnv     = "ANYSTORE_TEST_CHANGE_PEER_DB"
	changePeerHoldEnv = "ANYSTORE_TEST_CHANGE_PEER_HOLD"
)

// changePeer is the other process: one insert into the indexed collection.
// With changePeerHoldEnv set it instead holds a write transaction on the
// database until the file named there appears, after creating <file>.held.
func changePeer(t *testing.T) bool {
	path := os.Getenv(changePeerEnv)
	if path == "" {
		return false
	}
	ctx := context.Background()
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	if stop := os.Getenv(changePeerHoldEnv); stop != "" {
		wtx, err := db.WriteTx(ctx)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(stop+".held", nil, 0o644))
		for i := 0; i < 6000; i++ {
			if _, err := os.Stat(stop); err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		require.NoError(t, wtx.Rollback())
		require.NoError(t, db.Close())
		return true
	}
	for _, name := range []string{"c", "c2"} {
		c, err := db.OpenCollection(ctx, name)
		if name == "c2" && err != nil {
			continue // only some tests have it
		}
		require.NoError(t, err)
		require.NoError(t, c.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":7}`, 1000+os.Getpid()))))
	}
	require.NoError(t, db.Close())
	return true
}

func runChangePeer(t *testing.T, path string) {
	t.Helper()
	name := t.Name()
	if i := bytes.IndexByte([]byte(name), '/'); i >= 0 {
		name = name[:i]
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), changePeerEnv+"="+path)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "peer failed:\n%s", out)
}

// openSketchDB makes a database with an indexed collection of 50 documents
// and returns it reopened, so that no collection handle is open.
func openSketchDB(t *testing.T) (DB, string) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "p.db")
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	for i := 0; i < 50; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}
	require.NoError(t, db.Close())
	db, err = Open(ctx, path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func sketchDocCount(t *testing.T, coll Collection) uint64 {
	t.Helper()
	st, err := coll.Stats(context.Background())
	require.NoError(t, err)
	require.Len(t, st.Indexes, 1)
	return st.Indexes[0].SketchDocCount
}

// The sketches of an open collection follow another process's commit,
// whatever kind of transaction of this process is the first to begin after
// it: one that reloads sketches, or one that never looks at them (a fast
// read by id, a btree-level read, a backup, an integrity check).
func TestChangeEpoch_PeerCommitReloadsSketches(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	for _, first := range []string{"stats", "find-id", "btree-read", "write", "backup", "integrity"} {
		t.Run(first, func(t *testing.T) {
			d, path := openSketchDB(t)
			coll, err := d.OpenCollection(ctx, "c")
			require.NoError(t, err)
			require.Equal(t, uint64(50), sketchDocCount(t, coll))

			runChangePeer(t, path)

			switch first {
			case "stats":
			case "find-id":
				_, err := coll.FindId(ctx, 1)
				require.NoError(t, err)
			case "btree-read":
				rtx, err := d.(*db).btreeDB.BeginRead()
				require.NoError(t, err)
				require.NoError(t, rtx.Rollback())
			case "write":
				wtx, err := d.WriteTx(ctx)
				require.NoError(t, err)
				require.NoError(t, wtx.Rollback())
			case "backup":
				require.NoError(t, d.Backup(ctx, filepath.Join(t.TempDir(), "b.db")))
			case "integrity":
				require.NoError(t, d.IntegrityCheck(ctx))
			}

			n, err := coll.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, 51, n)
			require.Equal(t, uint64(51), sketchDocCount(t, coll), "the sketches were loaded again after the peer's commit")
		})
	}
}

// A collection opened through a transaction that began before another
// process's commit loads the sketches of that older snapshot. They are not
// stamped as current: the next transaction loads them again.
func TestChangeEpoch_CollectionOpenedThroughAnOldTx(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	d, path := openSketchDB(t)

	old, err := d.ReadTx(ctx) // before the peer's commit
	require.NoError(t, err)
	runChangePeer(t, path)
	// The commit is counted by another transaction of this process.
	rtx, err := d.(*db).btreeDB.BeginRead()
	require.NoError(t, err)
	require.NoError(t, rtx.Rollback())

	coll, err := d.OpenCollection(old.Context(), "c") // loads at the old snapshot
	require.NoError(t, err)
	n, err := coll.Count(old.Context())
	require.NoError(t, err)
	require.Equal(t, 50, n, "setup: the old transaction does not see the peer's document")
	require.NoError(t, old.Commit())

	require.Equal(t, uint64(51), sketchDocCount(t, coll), "sketches loaded at an older snapshot are loaded again")
}

// Backup with a write transaction of this database open: at once when the
// caller's ctx carries it, and after the busy timeout when another goroutine
// holds it. The error matches ErrBusy, a destination that existed keeps its
// content, a new one is not removed, and the backup works once the
// transaction has ended.
func TestBackup_BusyBehindAWriteTx(t *testing.T) {
	ctx := context.Background()
	d, _ := openSketchDB(t)
	dir := t.TempDir()
	existing := filepath.Join(dir, "existing.db")
	require.NoError(t, d.Backup(ctx, existing)) // a valid backup: 50 documents

	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	wtx, err := d.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":500,"a":1}`)))

	// The caller's own transaction: no wait, and no destination is opened.
	fresh := filepath.Join(dir, "fresh.db")
	start := time.Now()
	err = d.Backup(wtx.Context(), fresh)
	require.ErrorIs(t, err, ErrBusy)
	require.Less(t, time.Since(start), time.Second)
	_, statErr := os.Stat(fresh)
	require.True(t, os.IsNotExist(statErr), "no destination is opened for a backup that cannot start")

	// Another caller: it waits out the budget.
	prev := backupBusyTimeout
	backupBusyTimeout = 300 * time.Millisecond
	defer func() { backupBusyTimeout = prev }()
	start = time.Now()
	err = d.Backup(ctx, existing)
	require.ErrorIs(t, err, ErrBusy)
	require.GreaterOrEqual(t, time.Since(start), backupBusyTimeout)
	require.Less(t, time.Since(start), 5*time.Second)

	// A cancel ends the wait.
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	err = d.Backup(cctx, existing)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.False(t, errors.Is(err, ErrBusy))

	// The destination that existed is as it was.
	bk, err := Open(ctx, existing, nil)
	require.NoError(t, err)
	bcoll, err := bk.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err := bcoll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 50, n, "a failed backup leaves an existing destination's content")
	require.NoError(t, bk.Close())

	// With the transaction ended the same call succeeds.
	require.NoError(t, wtx.Commit())
	require.NoError(t, d.Backup(ctx, existing))
	bk, err = Open(ctx, existing, nil)
	require.NoError(t, err)
	bcoll, err = bk.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err = bcoll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 51, n)
	require.NoError(t, bk.Close())
}

// A backup that fails after it opened a new destination leaves the file: it
// cannot know the file is its own to remove.
func TestBackup_FailureLeavesTheDestination(t *testing.T) {
	ctx := context.Background()
	d, _ := openSketchDB(t)
	wtx, err := d.WriteTx(ctx)
	require.NoError(t, err)
	defer func() { _ = wtx.Rollback() }()

	prev := backupBusyTimeout
	backupBusyTimeout = 50 * time.Millisecond
	defer func() { backupBusyTimeout = prev }()
	path := filepath.Join(t.TempDir(), "new.db")
	require.ErrorIs(t, d.Backup(ctx, path), ErrBusy)
	_, err = os.Stat(path)
	require.NoError(t, err, "the destination is left in place")
	bk, err := Open(ctx, path, nil) // an empty database
	require.NoError(t, err)
	require.NoError(t, bk.Close())
}

// The sketches of an open collection are not stamped current by a
// transaction whose snapshot is older than a change the process has already
// counted: such a transaction plans with what is published and loads
// nothing, and the next transaction loads the newer sketches.
func TestChangeEpoch_RefreshThroughAnOldTxStampsNothing(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	d, path := openSketchDB(t)
	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, uint64(50), sketchDocCount(t, coll))

	old, err := d.ReadTx(ctx) // before the peer's commit
	require.NoError(t, err)
	runChangePeer(t, path)
	rtx, err := d.(*db).btreeDB.BeginRead() // counts the commit
	require.NoError(t, err)
	require.NoError(t, rtx.Rollback())

	// The old transaction plans with the collection: a refresh through it
	// would load the older sketches and stamp them with the new epoch.
	n, err := coll.Find(`{"a":1}`).Count(old.Context())
	require.NoError(t, err)
	require.Equal(t, 10, n)
	require.NoError(t, old.Commit())

	require.Equal(t, uint64(51), sketchDocCount(t, coll), "the next transaction loads the sketches of the peer's commit")
}

// The copy is committed in one step; what Backup does on the copy after
// that can still be busy, behind another process that opened the new copy.
// Backup then returns an error matching ErrBusy, and the destination holds
// the completed copy: the "keeps its content" promise ends at the commit.
func TestBackup_BusyAfterTheCopyIsCommitted(t *testing.T) {
	if changePeer(t) {
		return
	}
	if testing.Short() {
		t.Skip("waits out the busy timeout of a write begin")
	}
	ctx := context.Background()
	d, _ := openSketchDB(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "dst.db")
	stop := filepath.Join(dir, "stop")

	var peer *exec.Cmd
	testBackupCopiedHook = func() {
		testBackupCopiedHook = nil
		peer = exec.Command(os.Args[0], "-test.run=^TestBackup_BusyAfterTheCopyIsCommitted$", "-test.timeout=120s")
		peer.Env = append(os.Environ(), changePeerEnv+"="+path, changePeerHoldEnv+"="+stop)
		peer.Stdout, peer.Stderr = os.Stderr, os.Stderr
		require.NoError(t, peer.Start())
		require.Eventually(t, func() bool {
			_, err := os.Stat(stop + ".held")
			return err == nil
		}, 30*time.Second, 10*time.Millisecond, "the peer never took the destination's write lock")
	}
	defer func() { testBackupCopiedHook = nil }()

	err := d.Backup(ctx, path)
	require.NotNil(t, peer, "setup: the hook ran")
	require.NoError(t, os.WriteFile(stop, nil, 0o644))
	require.NoError(t, peer.Wait())
	require.ErrorIs(t, err, ErrBusy)

	bk, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = bk.Close() }()
	bcoll, err := bk.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err := bcoll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 50, n, "the completed copy is in the destination")
	require.False(t, errors.Is(err, ErrBusy))
}

// A sketch refresh and a collection open that run while a publication of
// this process is pending, through a transaction at the header the
// publication is replacing. The change state then still holds that header,
// next to an epoch that already counts what the new header brings: the
// transaction's snapshot equals the retained header and is not exact. The
// refresh loads nothing and the open stamps nothing, so the transactions
// after the publication load the newer sketches.
//
// The publication is a rebuild of the wal-index (another process committed
// and then left the header torn), which runs beside live readers; it is
// paused once its header is written and before the change state is settled.
func TestChangeEpoch_ConsumersDuringAPendingPublication(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "p.db")
	d, err := Open(ctx, path, nil)
	require.NoError(t, err)
	for _, name := range []string{"c", "c2"} {
		coll, err := d.CreateCollection(ctx, name)
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
		}
	}
	require.NoError(t, d.Close())
	d, err = Open(ctx, path, nil)
	require.NoError(t, err)
	defer func() { _ = d.Close() }()
	bt := d.(*db).btreeDB

	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, uint64(50), sketchDocCount(t, coll))
	// Everything in the database file: the old transaction below reads no
	// WAL frame, so it can read while the rebuild holds the WAL.
	require.NoError(t, bt.Checkpoint(btree.CheckpointPassive))

	old, err := d.ReadTx(ctx) // at H0
	require.NoError(t, err)
	runChangePeer(t, path) // one document more in c and in c2
	bt.TestSeam().TearHeader()

	var c2 Collection
	ran := false
	btree.SetTestPublishHeaderHook(func(step string) {
		if step != "stored" || ran {
			return
		}
		ran = true
		// Announced and written, not settled.
		n, err := coll.Find(`{"a":1}`).Count(old.Context()) // plans with c: a refresh through the old tx
		require.NoError(t, err)
		require.Equal(t, 10, n)
		c2, err = d.OpenCollection(old.Context(), "c2") // loads c2's sketches through the old tx
		require.NoError(t, err)
	})
	defer btree.SetTestPublishHeaderHook(nil)
	rtx, err := bt.BeginRead() // rebuilds the wal-index
	btree.SetTestPublishHeaderHook(nil)
	require.NoError(t, err)
	require.NoError(t, rtx.Rollback())
	require.True(t, ran, "setup: the begin rebuilt the wal-index")
	require.NoError(t, old.Commit())

	require.Equal(t, uint64(51), sketchDocCount(t, coll), "a refresh at the replaced header stamped nothing")
	require.NotNil(t, c2)
	require.Equal(t, uint64(51), sketchDocCount(t, c2), "an open at the replaced header stamped nothing")
}
