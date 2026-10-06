package btree

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// A reader's snapshot counters are the ones its snapshot contains, after a
// WAL restart too: a checkpoint RESTART and a schema-changing commit —
// this process's own, or another's — land between the reader's claim of
// its slot and its read of page 1. The new generation's frames sit at
// numbers the snapshot's window spans; only the snapshot's own floor
// excludes them.
func TestBeginRead_SnapshotCookieAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	opts := DefaultOptions()
	db1, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	db2 := db1

	// A few committed frames, all checkpointed: the next reader claims slot
	// 0, its content the file's.
	wtx, err := db1.BeginWrite()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = wtx.CreateNamespace("a"); err != nil {
		t.Fatal(err)
	}
	wtx.MarkSchemaChanged()
	if err = wtx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db1.Checkpoint(CheckpointPassive); err != nil {
		t.Fatal(err)
	}
	rtx, err := db1.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	_, cookieBefore, _ := rtx.SnapshotHeaderCounters()
	_ = rtx.Rollback()

	var hookErr error
	testReadSlotClaimedHook = func() {
		testReadSlotClaimedHook = nil
		// The other connection: restart the WAL, then a schema change.
		if hookErr = db2.Checkpoint(CheckpointRestart); hookErr != nil {
			return
		}
		w, err := db2.BeginWrite()
		if err != nil {
			hookErr = err
			return
		}
		if _, err = w.CreateNamespace("b"); err != nil {
			hookErr = err
			return
		}
		w.MarkSchemaChanged()
		hookErr = w.Commit()
	}
	defer func() { testReadSlotClaimedHook = nil }()

	rtx, err = db1.BeginRead()
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	_, cookie, _ := rtx.SnapshotHeaderCounters()
	if cookie != cookieBefore {
		t.Fatalf("snapshot cookie %d, want %d: the counters of the other connection's commit, which the snapshot does not contain", cookie, cookieBefore)
	}
	if _, err = rtx.GetNamespace("b"); err == nil {
		t.Fatal("the snapshot has the namespace the other connection created after it")
	}
	if rtx.DiskSchemaCookie() != cookieBefore+1 {
		t.Fatalf("disk cookie %d, want %d: the raised read did not notice the other connection's commit", rtx.DiskSchemaCookie(), cookieBefore+1)
	}
}

// The in-process read begin re-validates the frame numbers and the restart
// count after taking its slot, on the slot-0 fast path too: a restart and a
// commit between its lock-free reads and the lock would key the file's
// content as the new generation's commit at the same frame number, and
// the readers of that commit would then take the stale page cache and
// counters remembered under the key.
func TestBeginRead_InProcessFastPathRetriesAcrossRestart(t *testing.T) {
	for _, mode := range []string{"inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			opts := DefaultOptions()
			path := filepath.Join(t.TempDir(), "db")
			if mode == "inmemory" {
				opts.InMemory = true
				path = ""
			} else {
				opts.InProcess = true
			}
			db, err := Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			commit := func(ns string) {
				w, err := db.BeginWrite()
				if err != nil {
					t.Fatal(err)
				}
				if _, err = w.CreateNamespace(ns); err != nil {
					t.Fatal(err)
				}
				w.MarkSchemaChanged()
				if err = w.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			commit("a")
			if err = db.Checkpoint(CheckpointPassive); err != nil {
				t.Fatal(err)
			}
			rtx, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			_, cookieBefore, _ := rtx.SnapshotHeaderCounters()
			_ = rtx.Rollback()

			var hookErr error
			testInProcessFramesReadHook = func() {
				testInProcessFramesReadHook = nil
				// The old generation fully backfilled, its frame numbers
				// read: a restart, then a commit that ends on the same
				// frame number, before the fast path takes its lock.
				if hookErr = db.Checkpoint(CheckpointRestart); hookErr != nil {
					return
				}
				w, err := db.BeginWrite()
				if err != nil {
					hookErr = err
					return
				}
				if _, err = w.CreateNamespace("b"); err != nil {
					hookErr = err
					return
				}
				w.MarkSchemaChanged()
				hookErr = w.Commit()
			}
			defer func() { testInProcessFramesReadHook = nil }()
			rtx, err = db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			if hookErr != nil {
				t.Fatal(hookErr)
			}
			_, cookie, _ := rtx.SnapshotHeaderCounters()
			_, hasB := rtx.GetNamespace("b")
			_ = rtx.Rollback()
			// A snapshot is the content its key names: the commit's, or the
			// file's — not the file's content under the commit's key.
			if hasB == nil && cookie != cookieBefore+1 {
				t.Fatalf("snapshot has the commit at cookie %d, want %d", cookie, cookieBefore+1)
			}
			if hasB != nil && cookie != cookieBefore {
				t.Fatalf("snapshot lacks the commit at cookie %d, want %d", cookie, cookieBefore)
			}
			// The readers of the commit: the content under its key.
			fast, err := db.BeginReadFast()
			if err != nil {
				t.Fatal(err)
			}
			defer fast.Rollback()
			if _, err = fast.GetNamespace("b"); err != nil {
				t.Fatal("a reader after the commit does not see it")
			}
			if _, c, _ := fast.SnapshotHeaderCounters(); c != cookieBefore+1 {
				t.Fatalf("a reader after the commit reports cookie %d, want %d", c, cookieBefore+1)
			}
		})
	}
}

// A fresh reader slot's mark is claimed before the slot is shared: a
// checkpoint skips a slot whose mark is unused, so with the mark stored
// later a checkpoint between the lock and the store backfills past the
// snapshot, and the reader then takes newer content from the file for the
// pages its WAL window does not cover — a torn snapshot.
func TestBeginRead_InProcessFreshSlotMarkBeforeShare(t *testing.T) {
	for _, mode := range []string{"inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			opts := DefaultOptions()
			path := filepath.Join(t.TempDir(), "db")
			if mode == "inmemory" {
				opts.InMemory = true
				path = ""
			} else {
				opts.InProcess = true
			}
			db, err := Open(path, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var ns *Namespace
			// Values of a page each: the two keys on different pages, so one
			// can come from the WAL and the other from the file.
			big := func(v string) []byte { return append([]byte(v), make([]byte, 3000)...) }
			put := func(key, val string) {
				w, err := db.BeginWrite()
				if err != nil {
					t.Fatal(err)
				}
				if ns == nil {
					if ns, err = w.CreateNamespace("n"); err != nil {
						t.Fatal(err)
					}
				}
				if err = w.Put(ns, []byte(key), big(val)); err != nil {
					t.Fatal(err)
				}
				if err = w.Commit(); err != nil {
					t.Fatal(err)
				}
			}
			// A tree of several leaves, "a" and "b" on different ones. A
			// restart leaves every mark unused; the next reader claims a
			// fresh slot.
			for i := range 40 {
				put(fmt.Sprintf("k%02d", i), "k1")
			}
			put("a", "a1")
			put("z", "b1")
			if err = db.Checkpoint(CheckpointRestart); err != nil {
				t.Fatal(err)
			}
			put("a", "a2")

			var hookErr error
			var commitFrame uint32
			testInProcessFreshSlotHook = func() {
				testInProcessFreshSlotHook = nil
				// Backfill, a commit, backfill: a slot without a mark holds
				// nothing back.
				if hookErr = db.Checkpoint(CheckpointPassive); hookErr != nil {
					return
				}
				w, err := db.BeginWrite()
				if err != nil {
					hookErr = err
					return
				}
				if err = w.Put(ns, []byte("z"), big("b2")); err != nil {
					hookErr = err
					return
				}
				if hookErr = w.Commit(); hookErr != nil {
					return
				}
				commitFrame = db.pager.wal.index.mxCommitFrame.LoadLocal()
				hookErr = db.Checkpoint(CheckpointPassive)
			}
			defer func() { testInProcessFreshSlotHook = nil }()
			rtx, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			defer rtx.Rollback()
			if hookErr != nil {
				t.Fatal(hookErr)
			}
			a, err := rtx.Get(ns, []byte("a"))
			if err != nil {
				t.Fatal(err)
			}
			a = a[:2]
			b, err := rtx.Get(ns, []byte("z"))
			if err != nil {
				t.Fatal(err)
			}
			b = b[:2]
			// One snapshot: a2 with b1 (before the commit) or a2 with b2
			// (after it, had the begin retried onto it) — never a2 with b1
			// from the WAL and b2 from the file.
			if string(a) != "a2" {
				t.Fatalf("a = %q, want a2", a)
			}
			if string(b) != "b1" && string(b) != "b2" {
				t.Fatalf("b = %q", b)
			}
			if string(b) == "b2" && rtx.walMaxFrame != commitFrame {
				// The commit's content under a window that does not reach
				// it: read from the file, past the snapshot.
				t.Fatalf("b2 read at a snapshot whose window ends at frame %d; the commit is frame %d", rtx.walMaxFrame, commitFrame)
			}
		})
	}
}

// inProcessFixture opens an in-process or in-memory database for the
// read-begin tests, with a namespace of page-sized values.
type inProcessFixture struct {
	t  *testing.T
	db *DB
	ns *Namespace
}

func openInProcess(t *testing.T, mode string) *inProcessFixture {
	opts := DefaultOptions()
	path := filepath.Join(t.TempDir(), "db")
	if mode == "inmemory" {
		opts.InMemory = true
		path = ""
	} else {
		opts.InProcess = true
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &inProcessFixture{t: t, db: db}
}

func (f *inProcessFixture) put(kv ...string) {
	w, err := f.db.BeginWrite()
	if err != nil {
		f.t.Fatal(err)
	}
	if f.ns == nil {
		if f.ns, err = w.CreateNamespace("n"); err != nil {
			f.t.Fatal(err)
		}
	}
	for i := 0; i < len(kv); i += 2 {
		if err = w.Put(f.ns, []byte(kv[i]), append([]byte(kv[i+1]), make([]byte, 3000)...)); err != nil {
			f.t.Fatal(err)
		}
	}
	if err = w.Commit(); err != nil {
		f.t.Fatal(err)
	}
}

func (f *inProcessFixture) get(r *ReadTx, k string) string {
	v, err := r.Get(f.ns, []byte(k))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(v[:2])
}

// A read begin that lands inside a WAL restart — the frame numbers zeroed,
// the restart count not yet moved on — retries: with the old count it
// would take the key of the generation's start snapshot for the file at
// the generation's end, and a reader cache or counters remembered under
// that key by a reader of the start snapshot would serve it.
func TestBeginRead_InProcessRetriesInsideRestart(t *testing.T) {
	for _, mode := range []string{"inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			f := openInProcess(t, mode)
			db := f.db
			for i := range 40 {
				f.put(fmt.Sprintf("k%02d", i), "k1")
			}
			f.put("a", "a1", "z", "b1")
			if err := db.Checkpoint(CheckpointRestart); err != nil {
				t.Fatal(err)
			}
			// The generation's start snapshot, with a commit between its
			// slot claim and its data-version load: its cache and counters
			// go to the pool under its key.
			testReadSlotClaimedHook = func() {
				testReadSlotClaimedHook = nil
				f.put("a", "a2", "z", "b2")
			}
			r1, err := db.BeginRead()
			testReadSlotClaimedHook = nil
			if err != nil {
				t.Fatal(err)
			}
			if got := f.get(r1, "z"); got != "b1" {
				t.Fatalf("the start snapshot reads z=%q, want b1", got)
			}
			_ = r1.Rollback()

			// A reader beginning inside the next restart: it claims no slot
			// until the restart is over.
			var r2 *ReadTx
			var r2err error
			done := make(chan struct{})
			testResetMidHook = func() {
				testResetMidHook = nil
				claimed := make(chan struct{})
				testReadSlotClaimedHook = func() {
					testReadSlotClaimedHook = nil
					close(claimed)
				}
				go func() {
					defer close(done)
					r2, r2err = db.BeginRead()
				}()
				select {
				case <-claimed:
					t.Error("the reader claimed a slot inside the restart")
				case <-time.After(50 * time.Millisecond):
				}
			}
			defer func() { testResetMidHook = nil; testReadSlotClaimedHook = nil }()
			if err := db.Checkpoint(CheckpointRestart); err != nil {
				t.Fatal(err)
			}
			<-done
			if r2err != nil {
				t.Fatal(r2err)
			}
			defer r2.Rollback()
			if a, z := f.get(r2, "a"), f.get(r2, "z"); a != "a2" || z != "b2" {
				t.Fatalf("the reader reads a=%s z=%s: one commit wrote a2 and b2 together", a, z)
			}
		})
	}
}

// A read begin whose slot-0 fast path finds read-0 held — a checkpoint
// backfilling — goes on to the reader slots, as SQLite does, instead of
// failing the begin with ErrBusy.
func TestBeginRead_InProcessFastPathBusyGoesToTheSlots(t *testing.T) {
	for _, mode := range []string{"inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			f := openInProcess(t, mode)
			db := f.db
			idx := db.pager.wal.index
			f.put("a", "a1")
			if err := db.Checkpoint(CheckpointPassive); err != nil {
				t.Fatal(err)
			}
			testInProcessReadSnapshotHook = func() {
				testInProcessReadSnapshotHook = nil
				if err := idx.lock(lockRead0, lockExclusive); err != nil {
					t.Error(err)
				}
			}
			defer func() { testInProcessReadSnapshotHook = nil }()
			r, err := db.BeginRead()
			_ = idx.unlock(lockRead0, lockExclusive)
			if err != nil {
				t.Fatalf("BeginRead: %v", err)
			}
			defer r.Rollback()
			if r.walSlot == 0 {
				t.Fatal("the reader holds read-0, which was held exclusive")
			}
			if got := f.get(r, "a"); got != "a1" {
				t.Fatalf("a=%q", got)
			}
		})
	}
}

// A reader whose snapshot is past every slot's mark claims a free slot for
// it instead of reusing a slot with a lower mark: readers that overlap on a
// reused slot would hold the checkpoint at that mark for as long as they
// keep coming, and the WAL would never restart.
func TestBeginRead_InProcessClaimsASlotForANewerSnapshot(t *testing.T) {
	for _, mode := range []string{"inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			f := openInProcess(t, mode)
			db := f.db
			idx := db.pager.wal.index
			for i := range 10 {
				f.put(fmt.Sprintf("k%02d", i), "k1")
			}
			if err := db.Checkpoint(CheckpointPassive); err != nil {
				t.Fatal(err)
			}
			f.put("a", "a1")
			prev, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			// A chain of overlapping readers, a commit and a checkpoint
			// per round.
			for round := range 20 {
				f.put("a", fmt.Sprintf("a%d", round))
				next, err := db.BeginRead()
				if err != nil {
					t.Fatal(err)
				}
				_ = prev.Rollback()
				prev = next
				if err := db.Checkpoint(CheckpointPassive); err != nil {
					t.Fatal(err)
				}
			}
			mx := idx.mxCommitFrame.LoadLocal()
			nb := idx.nBackfill.Load()
			snapshot := prev.walMaxFrame
			_ = prev.Rollback()
			// The live reader pins its own snapshot only: the checkpoint
			// lags by at most what that reader cannot see.
			if lag := mx - nb; lag > mx-snapshot+5 {
				t.Fatalf("after 20 rounds of overlapping readers nBackfill=%d, mxCommit=%d, the live reader's snapshot %d", nb, mx, snapshot)
			}
		})
	}
}

func openInProcessN(t *testing.T, mode string, maxReaders int) *inProcessFixture {
	opts := DefaultOptions()
	opts.MaxReaders = maxReaders
	path := filepath.Join(t.TempDir(), "db")
	if mode == "inmemory" {
		opts.InMemory = true
		path = ""
	} else {
		opts.InProcess = true
	}
	db, err := Open(path, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return &inProcessFixture{t: t, db: db}
}

// The reuse of a reader slot re-validates the slot's mark after the shared
// lock. With every slot held, a reader reuses the one with the best mark;
// a checkpoint between its scan and its lock resets the slot, freed
// meanwhile, to unused — its backfill held off by a slot-0 reader, so the
// frame counters do not move — and a reader holding a slot without a mark
// is one the next checkpoint backfills past.
func TestBeginRead_InProcessReusedSlotMarkRechecked(t *testing.T) {
	for _, mode := range []string{"inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			f := openInProcessN(t, mode, 8)
			db := f.db
			for i := range 40 {
				f.put(fmt.Sprintf("k%02d", i), "k1")
			}
			f.put("a", "a1", "z", "b1")
			if err := db.Checkpoint(CheckpointRestart); err != nil {
				t.Fatal(err)
			}
			f.put("a", "a2")
			if err := db.Checkpoint(CheckpointPassive); err != nil {
				t.Fatal(err)
			}
			l, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			lDone := false
			defer func() {
				if !lDone {
					_ = l.Rollback()
				}
			}()
			if l.walSlot != 0 {
				t.Fatalf("the slot-0 reader is on slot %d", l.walSlot)
			}
			var held []*ReadTx
			for i := range 4 {
				f.put(fmt.Sprintf("k%02d", i), "k2")
				r, err := db.BeginRead()
				if err != nil {
					t.Fatal(err)
				}
				held = append(held, r)
			}
			defer func() {
				for _, r := range held {
					_ = r.Rollback()
				}
			}()
			f.put("k05", "k2")
			testInProcessReuseSlotHook = func() {
				testInProcessReuseSlotHook = nil
				for _, r := range held {
					_ = r.Rollback()
				}
				held = nil
				if err := db.Checkpoint(CheckpointPassive); err != nil {
					t.Error(err)
				}
			}
			defer func() { testInProcessReuseSlotHook = nil }()
			r, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			defer r.Rollback()
			_ = l.Rollback()
			lDone = true
			f.put("z", "b2")
			if err := db.Checkpoint(CheckpointPassive); err != nil {
				t.Fatal(err)
			}
			if got := f.get(r, "z"); got != "b1" {
				t.Fatalf("the reader reads z=%s past its snapshot", got)
			}
			if got := f.get(r, "a"); got != "a2" {
				t.Fatalf("a=%s", got)
			}
		})
	}
}

// A read begin re-validates the restart count once it holds its slot: with
// a restart and a commit that ends on the same frame number between its
// read of the count and its reads of the frame numbers, the old count
// would key the new commit as an older snapshot, whose pooled cache and
// counters it would then take.
func TestBeginRead_InProcessRestartCountRechecked(t *testing.T) {
	for _, mode := range []string{"inprocess", "inmemory"} {
		t.Run(mode, func(t *testing.T) {
			f := openInProcess(t, mode)
			db := f.db
			commitNS := func(ns string, onc func(uint32, uint32)) error {
				w, err := db.BeginWrite()
				if err != nil {
					return err
				}
				if _, err = w.CreateNamespace(ns); err != nil {
					return err
				}
				w.MarkSchemaChanged()
				if onc != nil {
					w.OnCommitted(onc)
				}
				return w.Commit()
			}
			if err := commitNS("x", nil); err != nil {
				t.Fatal(err)
			}
			if err := db.Checkpoint(CheckpointRestart); err != nil {
				t.Fatal(err)
			}
			if err := commitNS("a", nil); err != nil {
				t.Fatal(err)
			}
			// A reader of the generation's first commit: its cache goes to
			// the pool under that key.
			y, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := y.GetNamespace("a"); err != nil {
				t.Fatal(err)
			}
			_, cookieA, _ := y.SnapshotHeaderCounters()
			_ = y.Rollback()

			// A begin that read the count: a restart, then a commit at the
			// same frame number, still being published when the begin goes
			// on.
			done := make(chan error, 1)
			testInProcessReadSnapshotHook = func() {
				testInProcessReadSnapshotHook = nil
				if err := db.Checkpoint(CheckpointRestart); err != nil {
					t.Error(err)
					return
				}
				published := make(chan struct{})
				go func() {
					done <- commitNS("b", func(uint32, uint32) {
						close(published)
						time.Sleep(200 * time.Millisecond)
					})
				}()
				<-published
			}
			defer func() { testInProcessReadSnapshotHook = nil }()
			x, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			defer x.Rollback()
			_, cookieX, _ := x.SnapshotHeaderCounters()
			_, errB := x.GetNamespace("b")
			if (errB == nil) != (cookieX == cookieA+1) {
				t.Fatalf("the snapshot's content and counters disagree: has b=%v, cookie %d (a's is %d)", errB == nil, cookieX, cookieA)
			}
		})
	}
}
