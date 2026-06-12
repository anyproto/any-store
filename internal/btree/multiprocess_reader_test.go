//go:build (linux || darwin) && (amd64 || arm64)

package btree

// Reproduces the "btree: database is corrupt" error reported when one OS
// process writes large documents to a collection while a second process
// (e.g. the CLI) periodically runs read-only full scans (collection.Stats).
//
// Writer process: commits a few ~100KB values per transaction in a loop with
// a low auto-checkpoint threshold, so the WAL is frequently backfilled and
// RESTARTed (frame numbers recycle).
//
// Reader process (this test's parent): loops BeginRead + full cursor scan +
// NamespaceSize, holding a persistent reader page cache between transactions,
// exactly like a long-lived CLI connection.

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const mpReaderNumDocs = 3

func TestMultiProcessReaderScanCorruption(t *testing.T) {
	dbPath := os.Getenv("TEST_MPREADER_DB_PATH")
	if dbPath != "" {
		mpReaderWriterChild(t, dbPath)
		return
	}
	if testing.Short() {
		t.Skip("multi-process test skipped in -short mode")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	// Reader uses defaults comparable to the CLI: file-backed SHM,
	// persistent per-connection cache.
	opts := Options{
		PageSize:  4096,
		CacheSize: 2000,
		InProcess: false,
	}

	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	defer db.Close()

	// Seed the namespace so the child can GetNamespace it.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("docs")
	require.NoError(t, err)
	for i := 0; i < mpReaderNumDocs; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		val := bytes.Repeat([]byte{0}, 100*1024)
		require.NoError(t, tx.Put(ns, key, val))
	}
	tx.MarkDataChanged()
	require.NoError(t, tx.Commit())

	// Spawn the writer child process.
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestMultiProcessReaderScanCorruption$",
		"-test.timeout=60s",
	)
	cmd.Env = append(os.Environ(), "TEST_MPREADER_DB_PATH="+path)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	childDone := make(chan error, 1)
	go func() { childDone <- cmd.Wait() }()

	// Reader loop: full scan + NamespaceSize per read tx, like Stats().
	scans := 0
	for done := false; !done; {
		select {
		case cErr := <-childDone:
			require.NoError(t, cErr, "writer child failed")
			done = true
		default:
		}

		rtx, rErr := db.BeginRead()
		require.NoError(t, rErr)
		rns, nsErr := rtx.GetNamespace("docs")
		if nsErr != nil {
			rtx.Rollback()
			t.Fatalf("scan %d: GetNamespace: %v", scans, nsErr)
		}
		if scanErr := mpReaderScan(rtx, rns); scanErr != nil {
			rtx.Rollback()
			t.Fatalf("scan %d (walMaxFrame=%d): %v", scans, rtx.WalMaxFrame(), scanErr)
		}
		require.NoError(t, rtx.Rollback())
		scans++
	}
	t.Logf("completed %d scans without corruption", scans)
}

// mpReaderScan mirrors collection.Stats: full cursor scan reading every
// value, then a NamespaceSize page sweep.
func mpReaderScan(rtx *ReadTx, ns *Namespace) error {
	cursor := rtx.NewCursor(ns)
	defer cursor.Close()
	if err := cursor.First(); err != nil {
		return fmt.Errorf("cursor.First: %w", err)
	}
	docs := 0
	for cursor.Valid() {
		val, err := cursor.Value()
		if err != nil {
			return fmt.Errorf("cursor.Value doc %d: %w", docs, err)
		}
		// The writer fills each value with a single byte; a mixed-snapshot
		// read of the overflow chain shows up as a non-uniform value.
		for j := 1; j < len(val); j++ {
			if val[j] != val[0] {
				return fmt.Errorf("doc %d: torn value: byte %d = %d, byte 0 = %d (len %d)",
					docs, j, val[j], val[0], len(val))
			}
		}
		docs++
		if err := cursor.Next(); err != nil {
			return fmt.Errorf("cursor.Next doc %d: %w", docs, err)
		}
	}
	if docs != mpReaderNumDocs {
		return fmt.Errorf("expected %d docs, scanned %d", mpReaderNumDocs, docs)
	}
	if _, err := rtx.NamespaceSize(ns); err != nil {
		return fmt.Errorf("NamespaceSize: %w", err)
	}
	return nil
}

// mpReaderWriterChild overwrites the same few large documents in a loop.
// The low AutoCheckpointAfter forces frequent passive checkpoints followed
// by best-effort WAL RESTARTs, so WAL frame numbers recycle while the
// parent process is reading.
func mpReaderWriterChild(t *testing.T, dbPath string) {
	opts := Options{
		PageSize:            4096,
		CacheSize:           2000,
		InProcess:           false,
		AutoCheckpointAfter: 64,
	}

	db, err := testOpen(t, dbPath, opts)
	require.NoError(t, err)

	deadline := time.Now().Add(5 * time.Second)
	iter := 0
	for time.Now().Before(deadline) {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.GetNamespace("docs")
		require.NoError(t, err)
		fill := byte(1 + iter%250)
		for i := 0; i < mpReaderNumDocs; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			val := bytes.Repeat([]byte{fill}, 100*1024)
			require.NoError(t, tx.Put(ns, key, val))
		}
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
		iter++
	}
	t.Logf("writer child: %d commits", iter)
	require.NoError(t, db.Close())
}
