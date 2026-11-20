//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// What a commit stores for the collections on the writer's dirty list
// beside another process that commits to the same file. The peer is a real
// process (re-exec, see changePeer).

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

// A sketch left dirty by a rolled-back transaction is not this
// transaction's to store. Another process changed the schema, so the begin
// could not rebase the collection (its handle is unverified); a reader
// verifies it while the write transaction is open; the transaction writes
// elsewhere and commits. The stored row of the collection stays the peer's.
func TestWriteTx_CommitStoresNoSketchOfAnEarlierTx(t *testing.T) {
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
