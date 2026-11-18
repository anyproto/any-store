//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// Another process's schema change is found by whichever transaction of this
// process is the first to pin it: its snapshot's schema cookie is one this
// process has not seen, and a new schema generation starts. The peer is a
// real process (re-exec).

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

const ddlPeerEnv = "ANYSTORE_TEST_DDL_PEER_DB"

// ddlPeer is the other process: an index on the collection and a new
// collection.
func ddlPeer(t *testing.T) bool {
	path := os.Getenv(ddlPeerEnv)
	if path == "" {
		return false
	}
	ctx := context.Background()
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	c, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, c.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))
	made, err := db.CreateCollection(ctx, "made")
	require.NoError(t, err)
	require.NoError(t, made.Insert(ctx, anyenc.MustParseJson(`{"id":1}`)))
	require.NoError(t, db.Close())
	return true
}

func TestSchemaCookie_PeerDDLStartsAGeneration(t *testing.T) {
	if ddlPeer(t) {
		return
	}
	ctx := context.Background()
	for _, first := range []string{"read-tx", "find-id", "write-tx"} {
		t.Run(first, func(t *testing.T) {
			d, path := openSketchDB(t)
			dbi := d.(*db)
			coll, err := d.OpenCollection(ctx, "c")
			require.NoError(t, err)
			// Read transactions at the committed snapshot: its counters are
			// remembered, and the begins after the first read no page 1.
			for i := 0; i < 3; i++ {
				rtx, err := d.ReadTx(ctx)
				require.NoError(t, err)
				require.NoError(t, rtx.Commit())
				_, err = coll.FindId(ctx, 1)
				require.NoError(t, err)
			}
			require.Len(t, coll.GetIndexes(), 1)
			before := dbi.epoch.Load()

			cmd := exec.Command(os.Args[0], "-test.run=^TestSchemaCookie_PeerDDLStartsAGeneration$", "-test.timeout=60s")
			cmd.Env = append(os.Environ(), ddlPeerEnv+"="+path)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, "peer failed:\n%s", out)
			require.Equal(t, before, dbi.epoch.Load(), "setup: nothing of this process has begun since")

			switch first {
			case "read-tx":
				rtx, err := d.ReadTx(ctx)
				require.NoError(t, err)
				require.NoError(t, rtx.Commit())
			case "find-id":
				_, err := coll.FindId(ctx, 1)
				require.NoError(t, err)
			case "write-tx":
				wtx, err := d.WriteTx(ctx)
				require.NoError(t, err)
				require.NoError(t, wtx.Rollback())
			}

			after := dbi.epoch.Load()
			require.Equal(t, before.gen+1, after.gen, "the peer's schema change started no generation")
			require.NotEqual(t, before.known, after.known)
			require.Len(t, coll.GetIndexes(), 2, "the peer's index")
			made, err := d.OpenCollection(ctx, "made")
			require.NoError(t, err)
			n, err := made.Count(ctx)
			require.NoError(t, err)
			require.Equal(t, 1, n)
			require.Equal(t, before.gen+1, dbi.epoch.Load().gen, "one generation for one peer change")
		})
	}
}
