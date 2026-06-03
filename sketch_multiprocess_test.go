//go:build (linux || darwin) && (amd64 || arm64)

package anystore

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// TestSketch_MultiProcess_WriteConsistency pins the cross-process write
// consistency invariant for index sketches: when two OS processes open the
// same database file and both commit inserts, the on-disk sketch — and each
// process's in-memory sketch on its next write — reflects every insert from
// both processes. No writer's commit silently clobbers a peer's increments.
//
// This is what checkStaleForWrite (db.go) guarantees by reloading the
// sketch from disk before insertKeys/deleteKeys: a writer caught by a
// peer's commit re-reads the peer's persisted sketch and applies its own
// delta on top, instead of overwriting with (stale-base + own-delta).
func TestSketch_MultiProcess_WriteConsistency(t *testing.T) {
	dbPath := os.Getenv("TEST_MULTIPROCESS_SKETCH_PATH")
	if dbPath != "" {
		sketchMultiProcessChild(t, dbPath)
		return
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	const (
		parentInitial = 10
		childInserts  = 15
		parentFinal   = 1
		expectedTotal = parentInitial + childInserts + parentFinal
	)

	// Parent: open, create collection+index, insert parentInitial docs.
	parent, err := Open(ctx, path, nil)
	require.NoError(t, err)

	coll, err := parent.CreateCollection(ctx, "items")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "byK", Fields: []string{"k"}}))

	for i := 0; i < parentInitial; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, i, i%5))))
	}

	c := coll.(*collection)
	c.mu.Lock()
	parentBefore := c.indexes[0].readSketch().GetDocCount()
	c.mu.Unlock()
	require.Equal(t, uint64(parentInitial), parentBefore)

	// Spawn child process to insert childInserts docs into the same DB.
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestSketch_MultiProcess_WriteConsistency$",
		"-test.v",
		"-test.timeout=30s",
	)
	cmd.Env = append(os.Environ(), "TEST_MULTIPROCESS_SKETCH_PATH="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Run(), "child process failed")

	// Parent does one more insert. checkStaleForWrite must reload the
	// on-disk sketch (which now includes child's inserts) before
	// incrementing, so the commit ends up at the true total.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":9000,"k":9000}`)))

	c.mu.Lock()
	parentAfter := c.indexes[0].readSketch().GetDocCount()
	c.mu.Unlock()
	require.Equal(t, uint64(expectedTotal), parentAfter,
		"parent's in-memory sketch after its post-child write must include child's inserts")

	require.NoError(t, parent.Close())

	// Reopen and verify the on-disk sketch (the persisted source of truth)
	// equals the true count.
	parent2, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer parent2.Close()

	insp := parent2.(IndexSketchInspector)
	info, err := insp.InspectIndexSketch(ctx, "items", "byK")
	require.NoError(t, err)
	require.Equal(t, uint64(expectedTotal), info.DocCount,
		"on-disk sketch must reflect every insert across both processes")
}

func sketchMultiProcessChild(t *testing.T, dbPath string) {
	const (
		parentInitial = 10
		childInserts  = 15
	)

	db, err := Open(ctx, dbPath, nil)
	require.NoError(t, err)

	coll, err := db.OpenCollection(ctx, "items")
	require.NoError(t, err)

	// On open, loadSketch reads disk (= parentInitial). Child inserts
	// childInserts more; in-memory should end at the sum.
	for i := 0; i < childInserts; i++ {
		require.NoError(t, coll.Insert(ctx,
			anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"k":%d}`, 100+i, i%5))))
	}

	c := coll.(*collection)
	c.mu.Lock()
	got := c.indexes[0].readSketch().GetDocCount()
	c.mu.Unlock()
	require.Equal(t, uint64(parentInitial+childInserts), got,
		"child's in-memory sketch after its inserts must include parent's pre-existing docs (loaded from disk on open)")

	require.NoError(t, db.Close())
}
