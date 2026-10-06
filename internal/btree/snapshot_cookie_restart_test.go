package btree

import (
	"fmt"
	"path/filepath"
	"testing"
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
			testInProcessReadSnapshotHook = func() {
				testInProcessReadSnapshotHook = nil
				// The old generation fully backfilled: a restart, then a
				// commit that ends on the same frame number.
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
			defer func() { testInProcessReadSnapshotHook = nil }()
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

// The reuse of a reader slot re-validates the slot's mark after the shared
// lock: a checkpoint between the scan and the lock resets a free slot to
// unused (its backfill held off by a slot-0 reader, so nBackfill does not
// move), and a reader holding a slot without a mark is one the next
// checkpoint backfills past.
func TestBeginRead_InProcessReusedSlotMarkRechecked(t *testing.T) {
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
			for i := range 40 {
				put(fmt.Sprintf("k%02d", i), "k1")
			}
			put("a", "a1")
			put("z", "b1")
			if err = db.Checkpoint(CheckpointRestart); err != nil {
				t.Fatal(err)
			}
			idx := db.pager.wal.index
			put("a", "a2")
			if err = db.Checkpoint(CheckpointPassive); err != nil {
				t.Fatal(err)
			}
			// A slot-0 reader: the checkpoints below cannot backfill.
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
			put("k05", "k2")
			ra, err := db.BeginRead() // a fresh claim: slot 1
			if err != nil {
				t.Fatal(err)
			}
			_ = ra.Rollback()
			put("k06", "k2")
			// A reader whose reuse of slot 1 meets the slot held exclusive
			// for an instant (a checkpointer's per-slot lock) claims slot 2.
			testInProcessReuseSlotHook = func() {
				testInProcessReuseSlotHook = nil
				if err := idx.lock(lockRead0+1, lockExclusive); err != nil {
					t.Error(err)
				}
			}
			rb, err := db.BeginRead()
			testInProcessReuseSlotHook = nil
			_ = idx.unlock(lockRead0+1, lockExclusive)
			if err != nil {
				t.Fatal(err)
			}
			_ = rb.Rollback()
			put("k07", "k2")

			// The reader under test picks slot 2; a checkpoint between its
			// scan and its lock resets the free slot.
			testInProcessReuseSlotHook = func() {
				testInProcessReuseSlotHook = nil
				if err := db.Checkpoint(CheckpointPassive); err != nil {
					t.Error(err)
				}
			}
			defer func() { testInProcessReuseSlotHook = nil }()
			r2, err := db.BeginRead()
			if err != nil {
				t.Fatal(err)
			}
			defer r2.Rollback()
			if mark := idx.aReadMark[r2.walSlot].Load(); r2.walSlot != 0 && (mark == readMarkNotUsed || mark < r2.walMaxFrame) {
				t.Fatalf("the reader holds slot %d with mark %d for a window up to %d", r2.walSlot, mark, r2.walMaxFrame)
			}
			_ = l.Rollback()
			lDone = true

			put("z", "b2")
			if err = db.Checkpoint(CheckpointPassive); err != nil {
				t.Fatal(err)
			}
			b, err := r2.Get(ns, []byte("z"))
			if err != nil {
				t.Fatal(err)
			}
			if string(b[:2]) != "b1" {
				t.Fatalf("the reader reads z=%q, want b1: a commit past its snapshot", b[:2])
			}
		})
	}
}
