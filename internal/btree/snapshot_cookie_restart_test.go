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
