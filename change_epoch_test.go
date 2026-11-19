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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
)

const (
	changePeerEnv     = "ANYSTORE_TEST_CHANGE_PEER_DB"
	changePeerHoldEnv = "ANYSTORE_TEST_CHANGE_PEER_HOLD"
	changePeerBulkEnv = "ANYSTORE_TEST_CHANGE_PEER_BULK"
)

// changePeer is the other process: one insert into the indexed collection.
// With changePeerHoldEnv set it instead holds a write transaction on the
// database until the file named there appears, after creating <file>.held.
// With changePeerBulkEnv set it instead inserts 100 documents into c and
// creates a collection: a schema change beside the writes.
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
	if os.Getenv(changePeerBulkEnv) != "" {
		c, err := db.OpenCollection(ctx, "c")
		require.NoError(t, err)
		wtx, err := db.WriteTx(ctx)
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			require.NoError(t, c.Insert(wtx.Context(), anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":7}`, 1000+i))))
		}
		require.NoError(t, wtx.Commit())
		_, err = db.CreateCollection(ctx, "made-by-the-peer")
		require.NoError(t, err)
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

func runChangePeer(t *testing.T, path string, env ...string) {
	t.Helper()
	name := t.Name()
	if i := bytes.IndexByte([]byte(name), '/'); i >= 0 {
		name = name[:i]
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+name+"$", "-test.timeout=60s")
	cmd.Env = append(append(os.Environ(), changePeerEnv+"="+path), env...)
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

// storedSketch is the stored row of c's one index, decoded.
func storedSketch(t *testing.T, d DB, coll Collection) IndexSketchInfo {
	t.Helper()
	info, err := d.(IndexSketchInspector).InspectIndexSketch(context.Background(), "c", coll.GetIndexes()[0].Info().Name)
	require.NoError(t, err)
	return info
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

// A write of this process goes on top of the sketch row another process
// committed, whatever this process did between that commit and the write. A
// reader that plans first loads the row into the published sketch only; the
// writer still has to load it into the live one, or its commit stores the
// counts from before the other process's commit over that row, for good.
//
// 50 documents, one more from the peer, then the steps of each case: the
// stored row, the published sketch and the sketch a fresh open loads all
// count every document.
func TestChangeEpoch_WriterLoadsThePeersRowAfterAReader(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":3}`, id))
	}
	find := func(t *testing.T, ctx context.Context, coll Collection) {
		t.Helper()
		_, err := coll.Find(`{"a":1}`).Count(ctx)
		require.NoError(t, err)
	}
	cases := []struct {
		name string
		// late: the handle is opened after the peer's commit.
		late bool
		// steps returns the number of documents it committed.
		steps func(t *testing.T, d DB, coll Collection) int
	}{
		{name: "write", steps: func(t *testing.T, d DB, coll Collection) int {
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "find, write", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "stats, write", steps: func(t *testing.T, d DB, coll Collection) int {
			require.Equal(t, uint64(51), sketchDocCount(t, coll))
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "two readers, write", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			require.Equal(t, uint64(51), sketchDocCount(t, coll))
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "handle opened after the commit, find, write", late: true, steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "write, stats, write", steps: func(t *testing.T, d DB, coll Collection) int {
			require.NoError(t, coll.Insert(ctx, doc(100)))
			require.Equal(t, uint64(52), sketchDocCount(t, coll))
			require.NoError(t, coll.Insert(ctx, doc(101)))
			return 2
		}},
		{name: "find, a write tx that plans between its writes", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, coll.Insert(wtx.Context(), doc(100)))
			find(t, wtx.Context(), coll)
			require.NoError(t, coll.Insert(wtx.Context(), doc(101)))
			require.NoError(t, wtx.Commit())
			return 2
		}},
		{name: "find, a write tx rolled back, write", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, coll.Insert(wtx.Context(), doc(100), doc(101)))
			require.NoError(t, wtx.Rollback())
			require.NoError(t, coll.Insert(ctx, doc(100)))
			return 1
		}},
		{name: "find, a statement rolled back in the write tx", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			// The first use of the collection in the transaction is inside
			// the statement's savepoint, and the statement fails on its
			// last document.
			require.ErrorIs(t, coll.Insert(wtx.Context(), doc(100), doc(101), doc(1)), ErrDocExists)
			require.NoError(t, coll.Insert(wtx.Context(), doc(100)))
			require.NoError(t, wtx.Commit())
			return 1
		}},
		{name: "find, write, own writes after", steps: func(t *testing.T, d DB, coll Collection) int {
			find(t, ctx, coll)
			for i := 0; i < 5; i++ {
				require.NoError(t, coll.Insert(ctx, doc(100+i)))
				find(t, ctx, coll)
			}
			return 5
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, path := openSketchDB(t)
			var coll Collection
			var err error
			// The row before the peer's commit: the peer's document has a
			// value of the indexed field none of these has, in a bucket
			// that is empty here.
			before, err := d.(IndexSketchInspector).InspectIndexSketch(ctx, "c", "a")
			require.NoError(t, err)
			if !tc.late {
				coll, err = d.OpenCollection(ctx, "c")
				require.NoError(t, err)
				require.Equal(t, uint64(50), sketchDocCount(t, coll))
			}
			runChangePeer(t, path)
			if tc.late {
				coll, err = d.OpenCollection(ctx, "c")
				require.NoError(t, err)
			}

			want := uint64(51 + tc.steps(t, d, coll))

			n, err := coll.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, int(want), n, "setup: the documents")
			stored := storedSketch(t, d, coll)
			require.Equal(t, want, stored.DocCount, "the stored row holds the peer's counts and this process's")
			require.Equal(t, want, sketchDocCount(t, coll), "the published sketch")
			// The counts by value, not the total alone: the peer's value has
			// its bucket, and every count that was added is the peer's one
			// or this process's.
			require.Len(t, stored.Buckets, len(before.Buckets))
			peers, added := 0, uint64(0)
			for i, n := range stored.Buckets {
				require.GreaterOrEqual(t, n, before.Buckets[i])
				added += n - before.Buckets[i]
				if before.Buckets[i] == 0 && n == 1 {
					peers++
				}
			}
			require.Equal(t, 1, peers, "the bucket of the peer's value")
			require.Equal(t, want-50, added, "the peer's entry and this process's")
			pub := coll.(*collection).loadIndexes()[0].loadPubSketch()
			require.True(t, slices.Equal(stored.Buckets, pub.Snapshot(nil)[:len(stored.Buckets)]), "the published sketch has the stored row's counts by value")

			require.NoError(t, d.Close())
			d2, err := Open(ctx, path, nil)
			require.NoError(t, err)
			defer func() { _ = d2.Close() }()
			coll2, err := d2.OpenCollection(ctx, "c")
			require.NoError(t, err)
			require.Equal(t, want, sketchDocCount(t, coll2), "the sketch a fresh open loads")
			pub2 := coll2.(*collection).loadIndexes()[0].loadPubSketch()
			require.True(t, slices.Equal(stored.Buckets, pub2.Snapshot(nil)[:len(stored.Buckets)]), "a fresh open loads the stored row's counts by value")
		})
	}
}

// The change epoch can move while a write transaction is open: a reader of
// this process that took its snapshot at an older commit of the peer and is
// only now counting it. Nothing committed under the writer, and it loaded
// the peer's row before its first write to the collection: it does not load
// the row again over its own deltas.
func TestChangeEpoch_EpochMovesUnderAWriteTx(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	d, path := openSketchDB(t)
	bt := d.(*db).btreeDB
	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.Equal(t, uint64(50), sketchDocCount(t, coll))
	epoch := func() uint64 {
		rtx, err := bt.BeginRead()
		require.NoError(t, err)
		e, _ := rtx.ChangeEpoch()
		require.NoError(t, rtx.Rollback())
		return e
	}

	runChangePeer(t, path) // the peer's first commit
	// A reader takes its snapshot there and stops before it counts it.
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	btree.SetTestChangeSwapHook(func() {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	})
	defer btree.SetTestChangeSwapHook(nil)
	delayed := make(chan error, 1)
	go func() {
		rtx, err := bt.BeginRead()
		if err == nil {
			err = rtx.Rollback()
		}
		delayed <- err
	}()
	<-entered
	runChangePeer(t, path) // the peer's second commit

	wtx, err := d.WriteTx(ctx) // counts the second commit
	require.NoError(t, err)
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":100,"a":3}`)))
	e1 := epoch()
	close(release)
	require.NoError(t, <-delayed)
	require.Equal(t, e1+1, epoch(), "setup: the delayed reader moved the epoch under the write transaction")
	// The next uses of the collection in the transaction: a plan and a write.
	_, err = coll.Find(`{"a":1}`).Count(wtx.Context())
	require.NoError(t, err)
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":101,"a":3}`)))
	require.NoError(t, wtx.Commit())

	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 54, n, "setup: the documents")
	require.Equal(t, uint64(54), storedSketch(t, d, coll).DocCount, "both of the transaction's documents are in the stored row")
	require.Equal(t, uint64(54), sketchDocCount(t, coll))
}

// A read of the stored row that fails is not a load. A writer that cannot
// load the row fails the operation before it writes: its deltas would go on
// counts that miss the peer's, and be stored over the peer's row. A reader
// plans with what is published and loads on its next use. A handle opened
// while the read fails is not one whose sketches are current.
func TestChangeEpoch_SketchRowReadFails(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	injected := errors.New("injected: sketch row read")
	failOnce := func(t *testing.T) *int {
		failed := new(int)
		testHookSketchRead = func([]byte) error {
			if *failed == 0 {
				*failed++
				return injected
			}
			return nil
		}
		t.Cleanup(func() { testHookSketchRead = nil })
		return failed
	}

	t.Run("writer", func(t *testing.T) {
		d, path := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		runChangePeer(t, path)
		require.Equal(t, uint64(51), sketchDocCount(t, coll)) // a reader loaded the row

		failed := failOnce(t)
		err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`))
		require.Equal(t, 1, *failed, "setup: the writer read the row")
		require.ErrorIs(t, err, injected, "the write fails before it puts its deltas on unloaded counts")
		require.Equal(t, uint64(51), storedSketch(t, d, coll).DocCount)
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`)), "the next write loads the row")
		require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
		require.Equal(t, uint64(52), sketchDocCount(t, coll))
	})

	t.Run("reader", func(t *testing.T) {
		d, path := openSketchDB(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.Equal(t, uint64(50), sketchDocCount(t, coll))
		runChangePeer(t, path)

		failed := failOnce(t)
		require.Equal(t, uint64(50), sketchDocCount(t, coll), "the published sketch as it was")
		require.Equal(t, 1, *failed, "setup: the reader read the row")
		require.Equal(t, uint64(51), sketchDocCount(t, coll), "the next reader loads the row")
	})

	t.Run("open through a reader", func(t *testing.T) {
		d, _ := openSketchDB(t)
		failed := failOnce(t)
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.Equal(t, 1, *failed, "setup: the open read the row")
		require.Equal(t, uint64(50), sketchDocCount(t, coll), "the first use loads the row")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`)))
		require.Equal(t, uint64(51), storedSketch(t, d, coll).DocCount)
	})

	t.Run("open through a writer", func(t *testing.T) {
		d, _ := openSketchDB(t)
		wtx, err := d.WriteTx(ctx)
		require.NoError(t, err)
		failed := failOnce(t)
		_, err = d.OpenCollection(wtx.Context(), "c")
		require.Equal(t, 1, *failed, "setup: the open read the row")
		require.ErrorIs(t, err, injected)
		coll, err := d.OpenCollection(wtx.Context(), "c")
		require.NoError(t, err)
		require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":100,"a":3}`)))
		require.NoError(t, wtx.Commit())
		require.Equal(t, uint64(51), storedSketch(t, d, coll).DocCount)
	})
}

// A handle loaded through a read transaction has live sketches no writer
// has verified: the snapshot they were read at may miss a commit of this
// process, which moves no epoch. The first write through the handle loads
// the stored row.
func TestSketch_HandleOpenedThroughAReaderIsLoadedByItsFirstWriter(t *testing.T) {
	ctx := context.Background()
	doc := func(id int) *anyenc.Value {
		return anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":3}`, id))
	}

	// A commit of this process between the load of the handle and its
	// registration, through a handle that is closed again.
	t.Run("commit between load and registration", func(t *testing.T) {
		d, _ := openSketchDB(t)
		hooked := false
		testHookBeforeRegister = func(name string) {
			if name != "c" || hooked {
				return
			}
			hooked = true
			h, err := d.OpenCollection(ctx, "c")
			require.NoError(t, err)
			require.NoError(t, h.Insert(ctx, doc(100)))
			require.NoError(t, h.Close())
		}
		t.Cleanup(func() { testHookBeforeRegister = nil })
		coll, err := d.OpenCollection(ctx, "c")
		require.NoError(t, err)
		require.True(t, hooked)
		testHookBeforeRegister = nil

		require.NoError(t, coll.Insert(ctx, doc(101)))
		n, err := coll.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 52, n, "setup: the documents")
		require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
		require.Equal(t, uint64(52), sketchDocCount(t, coll))
	})

	// In memory there is no other process and every snapshot passes for
	// current: a handle opened through a read transaction older than a
	// commit of this process.
	t.Run("in memory, through an old read tx", func(t *testing.T) {
		d, err := Open(ctx, ":memory:", &Config{InMemory: true})
		require.NoError(t, err)
		defer func() { _ = d.Close() }()
		coll, err := d.CreateCollection(ctx, "c")
		require.NoError(t, err)
		require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
		for i := 0; i < 50; i++ {
			require.NoError(t, coll.Insert(ctx, doc(i)))
		}
		old, err := d.ReadTx(ctx)
		require.NoError(t, err)
		require.NoError(t, coll.Insert(ctx, doc(100)))
		require.NoError(t, coll.Close())

		coll, err = d.OpenCollection(old.Context(), "c") // loads at 50 documents
		require.NoError(t, err)
		require.NoError(t, old.Commit())
		require.NoError(t, coll.Insert(ctx, doc(101)))
		n, err := coll.Count(ctx)
		require.NoError(t, err)
		require.Equal(t, 52, n, "setup: the documents")
		require.Equal(t, uint64(52), storedSketch(t, d, coll).DocCount)
		require.Equal(t, uint64(52), sketchDocCount(t, coll))
	})
}

// Stats right after a write transaction rolled back: the documents and the
// sketch agree. The live sketch keeps the aborted counts until the next
// write transaction begins; readers load the stored row instead.
func TestSketch_StatsAfterARollback(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	for _, peer := range []bool{true, false} {
		name := "own commits only"
		if peer {
			name = "after a peer's commit and a reader"
		}
		t.Run(name, func(t *testing.T) {
			d, path := openSketchDB(t)
			coll, err := d.OpenCollection(ctx, "c")
			require.NoError(t, err)
			want := uint64(51)
			if peer {
				runChangePeer(t, path)
			} else {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":99,"a":3}`)))
			}
			require.Equal(t, want, sketchDocCount(t, coll))

			wtx, err := d.WriteTx(ctx)
			require.NoError(t, err)
			require.NoError(t, coll.Insert(wtx.Context(),
				anyenc.MustParseJson(`{"id":100,"a":3}`), anyenc.MustParseJson(`{"id":101,"a":3}`)))
			// Inside the transaction a plan sees its writes.
			ex, err := coll.Find(`{"a":3}`).Explain(wtx.Context())
			require.NoError(t, err)
			require.Contains(t, ex.Plan, "est_rows", "setup: a planned query")
			inTx := coll.(*collection).loadIndexes()[0].loadPubSketch().GetDocCount()
			require.Equal(t, want+2, inTx, "the transaction plans with its own writes")
			require.NoError(t, wtx.Rollback())

			st, err := coll.Stats(ctx)
			require.NoError(t, err)
			require.Equal(t, int(want), st.DocCount)
			require.Equal(t, want, st.Indexes[0].SketchDocCount, "no count of the rolled-back transaction")

			require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`)))
			require.Equal(t, want+1, storedSketch(t, d, coll).DocCount)
			require.Equal(t, want+1, sketchDocCount(t, coll))
		})
	}
}

// A sketch left dirty by a rolled-back transaction is not this
// transaction's to store. Another process changed the schema, so the begin
// could not rebase the collection (its handle is unverified); a reader
// verifies it while the write transaction is open; the transaction writes
// elsewhere and commits. The stored row of the collection stays the peer's.
func TestSketch_CommitStoresNoSketchOfAnEarlierTx(t *testing.T) {
	if changePeer(t) {
		return
	}
	ctx := context.Background()
	d, path := openSketchDB(t)
	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	other, err := d.CreateCollection(ctx, "other")
	require.NoError(t, err)
	require.Equal(t, uint64(50), sketchDocCount(t, coll))

	wtx, err := d.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":100,"a":3}`)))
	require.NoError(t, wtx.Rollback()) // c's live sketch: 51, dirty

	runChangePeer(t, path, changePeerBulkEnv+"=1") // 100 documents into c, and a schema change

	wtx, err = d.WriteTx(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(150), sketchDocCount(t, coll), "a reader verifies the handle and loads the peer's row")
	require.NoError(t, other.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":1}`)))
	require.NoError(t, wtx.Commit())

	require.Equal(t, uint64(150), storedSketch(t, d, coll).DocCount, "the peer's row")
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":100,"a":3}`)))
	require.Equal(t, uint64(151), storedSketch(t, d, coll).DocCount)
	require.Equal(t, uint64(151), sketchDocCount(t, coll))
}
