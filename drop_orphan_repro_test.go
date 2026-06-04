package anystore

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestDropOrphanStaleIndexesRepro reproduces the Drop orphan bug: collection.Drop
// must delete index namespaces enumerated from the SAME on-disk source
// (idx:<coll>: keys) that removeCollection deletes, not from the in-memory index
// snapshot, which can lag the on-disk metadata (a peer handle's create, or this
// handle's own create that committed but is not yet reflected in the snapshot).
//
// Scenario (matches the forensic mechanism):
//  1. Handle A creates collection + EnsureIndex{val}; in-memory set == [val].
//  2. A second index exists on disk that A's in-memory set does not know about
//     (here we force that stale state directly; in the wild it is produced by
//     another OS process creating the index, since any-store uses InProcess:false
//     and the open lock forbids a second in-process handle).
//  3. Handle A drops the collection. removeCollection deletes BOTH index metadata
//     keys (on-disk scan); the namespace-delete must also delete BOTH namespaces,
//     including the one A never learned about.
//  4. After reopen there must be NO orphaned ix:<coll>: namespace.
//
// On the unpatched code the loop iterated the stale in-memory set and skipped the
// second index's namespace, leaving an orphan -> reopen + EnsureIndex on the
// recreated collection fails with "btree: namespace already exists". With the fix
// (enumerate idx: keys on disk) no orphan can survive.
func TestDropOrphanStaleIndexesRepro(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "store.db")

	const collName = "dropcoll_a"

	dbA, err := Open(ctx, path, nil)
	if err != nil {
		t.Fatalf("open A: %v", err)
	}
	collA, err := dbA.Collection(ctx, collName)
	if err != nil {
		t.Fatalf("A.Collection: %v", err)
	}
	if err = collA.EnsureIndex(ctx, IndexInfo{Fields: []string{"val"}}); err != nil {
		t.Fatalf("A.EnsureIndex(val): %v", err)
	}
	if err = collA.EnsureIndex(ctx, IndexInfo{Fields: []string{"other"}}); err != nil {
		t.Fatalf("A.EnsureIndex(other): %v", err)
	}

	// Force A's in-memory snapshot stale: drop "other" from the snapshot ONLY
	// (its idx: metadata key and ix: namespace remain on disk), simulating an
	// index this handle never learned about.
	cImpl := collA.(*collection)
	cImpl.mu.Lock()
	var kept []*index
	for _, idx := range cImpl.loadIndexes() {
		if idx.info.createName() != "other" {
			kept = append(kept, idx)
		}
	}
	cImpl.storeIndexes(kept)
	cImpl.mu.Unlock()

	if got := len(collA.GetIndexes()); got != 1 {
		t.Fatalf("precondition: expected A to know about exactly 1 index (stale), got %d", got)
	}

	// Operation under test.
	if err = collA.Drop(ctx); err != nil {
		t.Fatalf("A.Drop: %v", err)
	}

	// The second index namespace must NOT survive on disk.
	d := dbA.(*db)
	names, err := d.btreeDB.ListNamespaces()
	if err != nil {
		t.Fatalf("ListNamespaces: %v", err)
	}
	ixPrefix := "ix:" + collName + ":"
	var orphans []string
	for _, n := range names {
		if strings.HasPrefix(n, ixPrefix) {
			orphans = append(orphans, n)
		}
	}
	if len(orphans) != 0 {
		t.Fatalf("ORPHAN index namespace(s) survived Drop: %v (all namespaces: %v)", orphans, names)
	}

	if err = dbA.Close(); err != nil {
		t.Fatalf("A.Close: %v", err)
	}

	// Reopen + recreate: with an orphan this fails "btree: namespace already exists".
	dbC, err := Open(ctx, path, nil)
	if err != nil {
		t.Fatalf("reopen C: %v", err)
	}
	defer dbC.Close()
	collC, err := dbC.Collection(ctx, collName)
	if err != nil {
		t.Fatalf("C.Collection: %v", err)
	}
	if err = collC.EnsureIndex(ctx, IndexInfo{Fields: []string{"val"}}); err != nil {
		t.Fatalf("C.EnsureIndex(val) after drop+reopen failed (orphan namespace?): %v", err)
	}
	if err = collC.EnsureIndex(ctx, IndexInfo{Fields: []string{"other"}}); err != nil {
		t.Fatalf("C.EnsureIndex(other) after drop+reopen failed (orphan namespace?): %v", err)
	}
}
