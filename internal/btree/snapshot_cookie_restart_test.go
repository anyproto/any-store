package btree

import (
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
