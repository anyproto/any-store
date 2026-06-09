package anystore

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestFtsMultiprocessConsistency verifies strong cross-process consistency of
// the full-text index: because the per-tx write-back buffer is flushed into the
// SAME atomic btree commit as the documents, a separate OS process that opens
// the database after a commit observes a complete, consistent index — every
// committed insert/update/delete is visible, with no partial state.
//
// The parent writes; a re-exec'd child process opens the same file and asserts
// what $text queries return.
func TestFtsMultiprocessConsistency(t *testing.T) {
	if expect := os.Getenv("FTS_MP_EXPECT"); expect != "" {
		ftsMpChild(t, os.Getenv("FTS_MP_PATH"), expect)
		return
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "fts_mp.db")

	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Kind: IndexKindFulltext, Fields: []string{"body"}}))

	runChild := func(expect string) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestFtsMultiprocessConsistency$", "-test.v=true")
		cmd.Env = append(os.Environ(), "FTS_MP_PATH="+path, "FTS_MP_EXPECT="+expect)
		done := make(chan struct{})
		var out []byte
		var cerr error
		go func() { out, cerr = cmd.CombinedOutput(); close(done) }()
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatalf("child timed out")
		}
		require.NoError(t, cerr, "child failed:\n%s", out)
	}

	// Batch insert in ONE tx (exercises the write-back buffer), then commit.
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":"a","body":"hindenburg zeppelin disaster"}`),
		anyenc.MustParseJson(`{"id":"b","body":"boeing flight crash"}`),
		anyenc.MustParseJson(`{"id":"c","body":"london fog landing"}`),
	))
	// A fresh process sees the whole committed batch.
	runChild("zeppelin=a;crash=b;fog=c;airbus=")

	// Delta-update one doc; the new term appears, the old term disappears.
	require.NoError(t, coll.UpsertOne(ctx, anyenc.MustParseJson(`{"id":"b","body":"airbus runway incident"}`)))
	runChild("zeppelin=a;airbus=b;crash=;fog=c")

	// Delete one doc; it vanishes for the other process.
	require.NoError(t, coll.DeleteId(ctx, "a"))
	runChild("zeppelin=;airbus=b;fog=c")
}

// ftsMpChild runs in the child process: it opens the same DB file and asserts
// each "term=id" expectation (empty id means the term must have no matches).
func ftsMpChild(t *testing.T, path, expect string) {
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.OpenCollection(ctx, "docs")
	require.NoError(t, err)

	for _, pair := range strings.Split(expect, ";") {
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		term, wantID := kv[0], kv[1]
		ids := ftsSearchIDs(t, coll, term)
		if wantID == "" {
			require.Emptyf(t, ids, "child: term %q expected no matches, got %v", term, ids)
		} else {
			require.Equalf(t, []string{wantID}, ids, "child: term %q", term)
		}
	}
}
