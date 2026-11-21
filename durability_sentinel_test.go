//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// The durability sentinel (Config.Durability.Sentinel) with several
// processes on one file. Only a Close that checkpointed as the last handle
// on the database file clears it. The other processes are real ones
// (re-exec of this test binary).

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

const (
	sentinelPeerEnvPath = "ANYSTORE_TEST_SENTINEL_PEER_DB"
	sentinelPeerEnvMode = "ANYSTORE_TEST_SENTINEL_PEER_MODE"
)

func sentinelConfig() *Config {
	return &Config{Durability: DurabilityConfig{Sentinel: true}}
}

func sentinelWaitFile(t *testing.T, path string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return err == nil
	}, 30*time.Second, 5*time.Millisecond, "waiting for %s", path)
}

// sentinelPeer runs the peer process's part when the test binary was
// started as one, and reports whether it was.
func sentinelPeer(t *testing.T) bool {
	path := os.Getenv(sentinelPeerEnvPath)
	if path == "" {
		return false
	}
	ctx := context.Background()
	d, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	coll, err := d.OpenCollection(ctx, "c")
	require.NoError(t, err)
	switch mode := os.Getenv(sentinelPeerEnvMode); mode {
	case "write-and-close": // commits once and closes while the parent is attached
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"peer-1"}`)))
		require.NoError(t, d.Close())
	default:
		t.Fatalf("unknown mode %q", mode)
	}
	return true
}

func startSentinelPeer(t *testing.T, testName, path, mode string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+testName+"$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), sentinelPeerEnvPath+"="+path, sentinelPeerEnvMode+"="+mode)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	require.NoError(t, cmd.Start())
	return cmd
}

// The last close removes a marker another process created, although this
// process found none at open and wrote nothing: the marker is shared, and its
// checkpoint persisted the other process's commit too.
func TestDurabilitySentinel_LastCloseRemovesAPeersMarker(t *testing.T) {
	if sentinelPeer(t) {
		return
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	marker := path + ".lock"

	// setup: a clean database with the collection
	setup, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	_, err = setup.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, setup.Close())
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "setup: the last close left the sentinel: %v", err)

	// This process opens and only reads.
	reader, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	coll, err := reader.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Zero(t, n)

	// Another process writes and closes first: its close is not the last,
	// and the marker stays.
	peer := startSentinelPeer(t, "TestDurabilitySentinel_LastCloseRemovesAPeersMarker", path, "write-and-close")
	require.NoError(t, peer.Wait())
	_, err = os.Stat(marker)
	require.NoError(t, err, "setup: the peer's close, not the last, kept its marker")

	// The reader's close is the last and checkpoints the peer's commit.
	require.NoError(t, reader.Close())
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "the last close, which checkpointed, left another process's marker: %v", err)

	next, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	stats, err := next.Stats(ctx)
	require.NoError(t, err)
	require.False(t, stats.DirtyOnOpen)
	coll, err = next.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err = coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the peer's commit")
	require.NoError(t, next.Close())
}
