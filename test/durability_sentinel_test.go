//go:build (linux || darwin) && (amd64 || arm64)

package test

// The durability sentinel (Config.Durability.Sentinel) is cleared by a Close
// only when that close checkpointed as the last handle on the database file.
// A Close while another process has the database open runs no checkpoint and
// leaves the sentinel, so when the remaining process is killed the next open
// runs the quick check. The other process is a real one (re-exec), killed
// with SIGKILL.

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

const sentinelPeerEnv = "ANYSTORE_TEST_SENTINEL_PEER_DB"

func sentinelConfig() *anystore.Config {
	return &anystore.Config{Durability: anystore.DurabilityConfig{Sentinel: true}}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path)
		return err == nil
	}, 30*time.Second, 5*time.Millisecond, "waiting for %s", path)
}

// sentinelPeer is the process that gets killed: it commits once, tells the
// parent, waits for the parent's close, commits again and waits to be killed.
func sentinelPeer(t *testing.T) bool {
	path := os.Getenv(sentinelPeerEnv)
	if path == "" {
		return false
	}
	ctx := context.Background()
	db, err := anystore.Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"peer-1"}`)))
	require.NoError(t, os.WriteFile(path+".peer-ready", nil, 0o644))
	waitForFile(t, path+".peer-go")
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"peer-2"}`)))
	require.NoError(t, os.WriteFile(path+".peer-done", nil, 0o644))
	time.Sleep(time.Hour) // killed by the parent
	return true
}

func TestDurabilitySentinel_CloseThatIsNotTheLastKeepsIt(t *testing.T) {
	if sentinelPeer(t) {
		return
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	marker := path + ".lock"

	first, err := anystore.Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	coll, err := first.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"first-1"}`)))
	_, err = os.Stat(marker)
	require.NoError(t, err, "setup: a write sets the sentinel")

	// The second handle, in another process, has committed too: both
	// processes know the sentinel as set.
	peer := exec.Command(os.Args[0], "-test.run=^TestDurabilitySentinel_CloseThatIsNotTheLastKeepsIt$", "-test.timeout=120s")
	peer.Env = append(os.Environ(), sentinelPeerEnv+"="+path)
	peer.Stdout, peer.Stderr = os.Stderr, os.Stderr
	require.NoError(t, peer.Start())
	killed := false
	kill := func() {
		if !killed {
			killed = true
			_ = peer.Process.Kill()
			_ = peer.Wait()
		}
	}
	defer kill()
	waitForFile(t, path+".peer-ready")

	// The close that is not the last: no checkpoint, the sentinel stays.
	require.NoError(t, first.Close())
	_, err = os.Stat(marker)
	require.NoError(t, err, "a close with another process attached cleared the sentinel")

	// The other handle commits and its process is killed.
	require.NoError(t, os.WriteFile(path+".peer-go", nil, 0o644))
	waitForFile(t, path+".peer-done")
	kill()

	// The next open finds the sentinel and runs the quick check.
	next, err := anystore.Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	stats, err := next.Stats(ctx)
	require.NoError(t, err)
	require.True(t, stats.DirtyOnOpen, "the open after a killed process ran no quick check")
	coll, err = next.OpenCollection(ctx, "c")
	require.NoError(t, err)
	n, err := coll.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, n, "the commits of both processes")

	// This close is the last and checkpoints: it clears the sentinel.
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"next-1"}`)))
	_, err = os.Stat(marker)
	require.NoError(t, err, "setup: a write sets the sentinel")
	require.NoError(t, next.Close())
	_, err = os.Stat(marker)
	require.True(t, os.IsNotExist(err), "the last close, which checkpointed, left the sentinel: %v", err)

	clean, err := anystore.Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	stats, err = clean.Stats(ctx)
	require.NoError(t, err)
	require.False(t, stats.DirtyOnOpen)
	require.NoError(t, clean.Close())
}
