package anystore

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// Cross-process I-16 guards: a peer that renames/drops a collection must
// invalidate this process's cached handle at the next tx begin — pre-fix the
// stale handle silently committed UNINDEXED writes (durable corruption).
// Pattern: the test binary re-execs itself as the child (see stats_mp_test.go);
// scenarios are sequential — the child runs to completion, then the parent
// asserts.

// runRenameMpChild re-execs the named test as a child process with the given
// mode and DB path, and waits for it.
func runRenameMpChild(t *testing.T, testName, mode, path string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "RENAME_MP_PATH="+path, "RENAME_MP_MODE="+mode)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child failed:\n%s", out)
}

// renameMpChild executes the child side of a scenario against the shared DB.
func renameMpChild(t *testing.T, path, mode string) {
	store, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer store.Close()
	switch mode {
	case "rename":
		coll, cErr := store.OpenCollection(ctx, "A")
		require.NoError(t, cErr)
		require.NoError(t, coll.Rename(ctx, "B"))
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"child","a":30}`)))
	case "rename-reuse":
		coll, cErr := store.OpenCollection(ctx, "A")
		require.NoError(t, cErr)
		require.NoError(t, coll.Rename(ctx, "B"))
		fresh, cErr := store.CreateCollection(ctx, "A")
		require.NoError(t, cErr)
		require.NoError(t, fresh.Insert(ctx, anyenc.MustParseJson(`{"id":"new-a"}`)))
	case "drop-recreate":
		coll, cErr := store.OpenCollection(ctx, "A")
		require.NoError(t, cErr)
		require.NoError(t, coll.Drop(ctx))
		fresh, cErr := store.CreateCollection(ctx, "A")
		require.NoError(t, cErr)
		require.NoError(t, fresh.Insert(ctx, anyenc.MustParseJson(`{"id":"fresh"}`)))
	default:
		t.Fatalf("unknown RENAME_MP_MODE %q", mode)
	}
}

// renameMpParentSetup opens a fresh DB with collection "A" (index on "a",
// two seed docs) and returns the store, the held handle and the path.
func renameMpParentSetup(t *testing.T) (DB, Collection, string) {
	t.Helper()
	if testing.Short() {
		t.Skip("multi-process test skipped in -short mode")
	}
	path := filepath.Join(t.TempDir(), "rename_mp.db")
	store, err := Open(ctx, path, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	coll, err := store.CreateCollection(ctx, "A")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Name: "a", Fields: []string{"a"}}))
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"seed1","a":10}`),
		anyenc.MustParseJson(`{"id":"seed2","a":20}`),
	))
	return store, coll, path
}

// The durable-corruption repro from the issue: pre-fix the parent's write via
// the stale handle silently committed with NO index entries under the renamed
// collection's metadata.
func TestRenameMultiprocess_PeerInvalidation(t *testing.T) {
	if path := os.Getenv("RENAME_MP_PATH"); path != "" {
		renameMpChild(t, path, os.Getenv("RENAME_MP_MODE"))
		return
	}
	store, coll, path := renameMpParentSetup(t)
	runRenameMpChild(t, "TestRenameMultiprocess_PeerInvalidation", "rename", path)

	// The held handle must be invalidated at the next tx begin — the write
	// must ERROR, not silently commit unindexed.
	err := coll.Insert(ctx, anyenc.MustParseJson(`{"id":"stale-write","a":40}`))
	require.Error(t, err, "write via stale handle must not silently commit")
	assert.ErrorIs(t, err, ErrCollectionClosed)
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	_, err = store.OpenCollection(ctx, "A")
	assert.ErrorIs(t, err, ErrCollectionNotFound)

	// The renamed collection has both seed and child docs, all indexed.
	renamed, err := store.OpenCollection(ctx, "B")
	require.NoError(t, err)
	cnt, err := renamed.Find(`{"a":{"$gte":0}}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, cnt, "every write must be reachable through the index")
	require.NoError(t, store.IntegrityCheck(ctx))
}

// Rename-away followed by a fresh create under the SAME name: the catalog key
// exists again, so only the data-namespace root-page discriminator can tell
// the held handle it now points at a different collection.
func TestRenameMultiprocess_NameReuse(t *testing.T) {
	if path := os.Getenv("RENAME_MP_PATH"); path != "" {
		renameMpChild(t, path, os.Getenv("RENAME_MP_MODE"))
		return
	}
	store, coll, path := renameMpParentSetup(t)
	runRenameMpChild(t, "TestRenameMultiprocess_NameReuse", "rename-reuse", path)

	err := coll.Insert(ctx, anyenc.MustParseJson(`{"id":"stale-write"}`))
	assert.ErrorIs(t, err, ErrCollectionClosed, "held handle must not attach to the new A")

	freshA, err := store.OpenCollection(ctx, "A")
	require.NoError(t, err)
	cnt, err := freshA.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "new A holds only the child's doc")
	_, err = freshA.FindId(ctx, "new-a")
	require.NoError(t, err)

	renamed, err := store.OpenCollection(ctx, "B")
	require.NoError(t, err)
	cnt, err = renamed.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, cnt, "B holds the seed docs")
	require.NoError(t, store.IntegrityCheck(ctx))
}

// Peer drop+recreate — the pre-existing latent hole the same discriminator
// closes: without it the held handle silently attaches to the new collection.
func TestRenameMultiprocess_PeerDropRecreate(t *testing.T) {
	if path := os.Getenv("RENAME_MP_PATH"); path != "" {
		renameMpChild(t, path, os.Getenv("RENAME_MP_MODE"))
		return
	}
	store, coll, path := renameMpParentSetup(t)
	runRenameMpChild(t, "TestRenameMultiprocess_PeerDropRecreate", "drop-recreate", path)

	err := coll.Insert(ctx, anyenc.MustParseJson(`{"id":"stale-write"}`))
	assert.ErrorIs(t, err, ErrCollectionClosed)

	freshA, err := store.OpenCollection(ctx, "A")
	require.NoError(t, err)
	cnt, err := freshA.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, cnt, "recreated A holds only the child's doc")
	require.NoError(t, store.IntegrityCheck(ctx))
}

// The read path invalidates too: a read tx that observes the cookie bump
// completes on its consistent snapshot, and every subsequent op on the handle
// fails — including FindId's BeginReadFast path, which itself never runs the
// staleness check.
func TestRenameMultiprocess_ReadTxInvalidates(t *testing.T) {
	if path := os.Getenv("RENAME_MP_PATH"); path != "" {
		renameMpChild(t, path, os.Getenv("RENAME_MP_MODE"))
		return
	}
	store, coll, path := renameMpParentSetup(t)
	runRenameMpChild(t, "TestRenameMultiprocess_ReadTxInvalidates", "rename", path)

	// The first read after the peer commit runs the staleness pass at its tx
	// begin and invalidates the handle; the read itself is snapshot-consistent
	// and allowed to succeed either way — assert only that nothing corrupts.
	_, _ = coll.Count(ctx)

	// After that read, the handle is dead for every path.
	_, err := coll.FindId(ctx, "seed1")
	assert.ErrorIs(t, err, ErrCollectionClosed)
	_, err = coll.Find(`{"a":10}`).Count(ctx)
	assert.ErrorIs(t, err, ErrCollectionClosed)
	err = coll.Insert(ctx, anyenc.MustParseJson(`{"id":"stale-write"}`))
	assert.ErrorIs(t, err, ErrCollectionClosed)

	renamed, err := store.OpenCollection(ctx, "B")
	require.NoError(t, err)
	cnt, err := renamed.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, cnt)
	require.NoError(t, store.IntegrityCheck(ctx))
}
