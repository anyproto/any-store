//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// Another process's schema change is found by whichever transaction of this
// process is the first to pin it: its snapshot's schema cookie is one this
// process has not seen, and a new schema generation starts. The peer is a
// real process (re-exec).

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

const (
	ddlPeerEnv       = "ANYSTORE_TEST_DDL_PEER_DB"
	dropPeerEnv      = "ANYSTORE_TEST_DROP_PEER_DB"
	dropPeerIndexEnv = "ANYSTORE_TEST_DROP_PEER_INDEX"
)

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

// dropPeer is the other process: it drops the index dropPeerIndexEnv names
// from the collection "docs".
func dropPeer(t *testing.T) bool {
	path := os.Getenv(dropPeerEnv)
	if path == "" {
		return false
	}
	ctx := context.Background()
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	c, err := db.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, c.DropIndex(ctx, os.Getenv(dropPeerIndexEnv)))
	require.NoError(t, db.Close())
	return true
}

// A filter that matches nothing ({"$all":[]}, {"$in":[]}) is answered without
// a run, but its $text or $knn source is checked against the schema a run
// would use. When another process has dropped the index the source names,
// every verb returns the error it returns for a filter that matches
// something, and an $out after it keeps its destination. This process's
// cached schema still has the index: nothing of this process begins a
// transaction between the drop and the verb.
func TestPeerDropsIndex_SourceErrorsBeforeEmptyResult(t *testing.T) {
	if dropPeer(t) {
		return
	}
	ctx := context.Background()
	for _, src := range []struct {
		name, filter string
		index        IndexInfo
		want         error
	}{
		{"text", `{"$text":{"$search":"hello"},"tags":{"$all":[]}}`,
			IndexInfo{Name: "fts", Kind: IndexKindFulltext, Fields: []string{"body"}}, ErrNoFulltextIndex},
		{"knn", `{"v":{"$knn":{"$query":[1,0,0,0],"$k":1}},"tags":{"$in":[]}}`,
			IndexInfo{Name: "emb", Kind: IndexKindVector, Vector: &VectorParams{Field: "v", Dim: 4, Metric: VectorL2, Mode: VectorModeBruteForce}},
			ErrNoVectorIndex},
	} {
		for _, verb := range []string{"Count", "Iter", "Explain", "Update", "Delete", "Aggregate $out"} {
			t.Run(src.name+"/"+verb, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "p.db")
				d, err := Open(ctx, path, nil)
				require.NoError(t, err)
				t.Cleanup(func() { _ = d.Close() })
				coll, err := d.CreateCollection(ctx, "docs")
				require.NoError(t, err)
				require.NoError(t, coll.CreateIndex(ctx, src.index))
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"body":"hello world","tags":["x"],"v":[1,0,0,0]}`)))
				dst, err := d.CreateCollection(ctx, "published")
				require.NoError(t, err)
				require.NoError(t, dst.Insert(ctx, anyenc.MustParseJson(`{"id":"kept"}`)))
				n, err := coll.Find(src.filter).Count(ctx)
				require.NoError(t, err, "with the index")
				require.Zero(t, n)

				cmd := exec.Command(os.Args[0], "-test.run=^TestPeerDropsIndex_SourceErrorsBeforeEmptyResult$", "-test.timeout=60s")
				cmd.Env = append(os.Environ(), dropPeerEnv+"="+path, dropPeerIndexEnv+"="+src.index.Name)
				out, err := cmd.CombinedOutput()
				require.NoError(t, err, "peer failed:\n%s", out)

				switch verb {
				case "Count":
					_, err = coll.Find(src.filter).Count(ctx)
				case "Iter":
					var it Iterator
					if it, err = coll.Find(src.filter).Iter(ctx); err == nil {
						_ = it.Close()
					}
				case "Explain":
					_, err = coll.Find(src.filter).Explain(ctx)
				case "Update":
					_, err = coll.Find(src.filter).Update(ctx, `{"$set":{"touched":true}}`)
				case "Delete":
					_, err = coll.Find(src.filter).Delete(ctx)
				case "Aggregate $out":
					_, err = coll.Aggregate(fmt.Sprintf(`[{"$match":%s},{"$out":"published"}]`, src.filter)).Count(ctx)
				}
				require.ErrorIs(t, err, src.want, verb)
				n, err = dst.Count(ctx)
				require.NoError(t, err)
				require.Equal(t, 1, n, "the $out destination lost its document")
			})
		}
	}
}
