package anystore

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/stretchr/testify/require"
)

// TestStatsMultiprocessReader reproduces the reported scenario: one OS
// process writes a few large documents to a collection while a second
// process (like the CLI) periodically calls collection.Stats(). The reader
// must never see "btree: database is corrupt".
//
// The child process is the WRITER; the parent is the polling READER.
func TestStatsMultiprocessReader(t *testing.T) {
	if path := os.Getenv("STATS_MP_WRITER_PATH"); path != "" {
		statsMpWriterChild(t, path)
		return
	}
	if testing.Short() {
		t.Skip("multi-process test skipped in -short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "stats_mp.db")

	// Reader opens with defaults, like the CLI does.
	readerDB, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer readerDB.Close()
	coll, err := readerDB.CreateCollection(ctx, "agent_debug_log")
	require.NoError(t, err)

	// Seed one doc so the collection btrees exist.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"seed","body":"x"}`)))

	cmd := exec.Command(os.Args[0], "-test.run=^TestStatsMultiprocessReader$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), "STATS_MP_WRITER_PATH="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	childDone := make(chan error, 1)
	go func() { childDone <- cmd.Wait() }()

	polls := 0
	var lastDocCount int
	for done := false; !done; {
		select {
		case cErr := <-childDone:
			require.NoError(t, cErr, "writer child failed")
			done = true
		default:
		}
		stats, sErr := coll.Stats(ctx)
		if sErr != nil {
			statsMpDiagnose(t, readerDB.(*db), coll, polls, lastDocCount, sErr)
		}
		lastDocCount = stats.DocCount
		polls++
	}
	t.Logf("completed %d Stats() polls without corruption, final DocCount=%d", polls, lastDocCount)
}

// statsMpDiagnose breaks the failed Stats poll into its sub-operations to
// identify exactly which step fails, then retries to see whether the error
// is transient (race window) or persistent (durable bad state).
func statsMpDiagnose(t *testing.T, d *db, coll Collection, polls, lastDocCount int, origErr error) {
	t.Helper()
	c := coll.(*collection)
	for attempt := 0; attempt < 3; attempt++ {
		var steps []string
		_ = d.doReadTx(ctx, func(tx *btree.ReadTx) error {
			steps = append(steps, fmt.Sprintf("snapshot: walMaxFrame=%d dbSize=%d", tx.WalMaxFrame(), tx.DatabaseSize()))
			cursor := tx.NewCursor(c.ns)
			defer cursor.Close()
			if err := cursor.First(); err != nil {
				steps = append(steps, fmt.Sprintf("cursor.First: %v", err))
				return nil
			}
			n := 0
			for cursor.Valid() {
				if _, err := cursor.Value(); err != nil {
					steps = append(steps, fmt.Sprintf("cursor.Value(doc %d): %v", n, err))
					return nil
				}
				n++
				if err := cursor.Next(); err != nil {
					steps = append(steps, fmt.Sprintf("cursor.Next(doc %d): %v", n, err))
					return nil
				}
			}
			steps = append(steps, fmt.Sprintf("scan ok: %d docs", n))
			if _, err := tx.NamespaceSize(c.ns); err != nil {
				steps = append(steps, fmt.Sprintf("NamespaceSize: %v", err))
				return nil
			}
			steps = append(steps, "NamespaceSize ok")
			return nil
		})
		t.Logf("diagnose attempt %d: %v", attempt, steps)
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("poll %d (lastDocCount=%d): Stats: %v", polls, lastDocCount, origErr)
}

// statsMpWriterChild writes a few large (~1MB, incompressible) documents per
// transaction in a loop, like an application logging large payloads.
func statsMpWriterChild(t *testing.T, path string) {
	writerDB, err := Open(ctx, path, nil)
	require.NoError(t, err)
	coll, err := writerDB.OpenCollection(ctx, "agent_debug_log")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(1))
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 "
	bigBody := func() string {
		var sb strings.Builder
		sb.Grow(1 << 20)
		for sb.Len() < 1<<20 {
			sb.WriteByte(alphabet[rng.Intn(len(alphabet))])
		}
		return sb.String()
	}

	deadline := time.Now().Add(6 * time.Second)
	iter := 0
	for time.Now().Before(deadline) {
		docs := make([]*anyenc.Value, 0, 3)
		arena := &anyenc.Arena{}
		for i := 0; i < 3; i++ {
			obj := arena.NewObject()
			obj.Set("id", arena.NewString(fmt.Sprintf("doc-%d-%d", iter, i)))
			obj.Set("body", arena.NewString(bigBody()))
			docs = append(docs, obj)
		}
		require.NoError(t, coll.Insert(ctx, docs...))
		iter++
	}
	t.Logf("writer child: %d insert batches", iter)
	require.NoError(t, writerDB.Close())
}
