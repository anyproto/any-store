//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// The durability sentinel (Config.Durability.Sentinel) with several
// processes on one file. Only a Close that checkpointed as the last handle
// on the database file clears it. The other processes are real ones
// (re-exec of this test binary).

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/durability"
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
	case "commit-twice-then-wait": // commits, waits for the parent's close, commits again, waits to be killed
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"peer-1"}`)))
		require.NoError(t, os.WriteFile(path+".peer-ready", nil, 0o644))
		sentinelWaitFile(t, path+".peer-go")
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"peer-2"}`)))
		require.NoError(t, os.WriteFile(path+".peer-done", nil, 0o644))
		time.Sleep(time.Hour) // killed by the parent
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

// A Close while another process has the database open runs no checkpoint and
// leaves the sentinel, so when the remaining process is killed with SIGKILL
// the next open runs the quick check.
func TestDurabilitySentinel_CloseThatIsNotTheLastKeepsIt(t *testing.T) {
	if sentinelPeer(t) {
		return
	}
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	marker := path + ".lock"

	first, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	coll, err := first.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"first-1"}`)))
	_, err = os.Stat(marker)
	require.NoError(t, err, "setup: a write sets the sentinel")

	// The second handle, in another process, has committed too: both
	// processes know the sentinel as set.
	peer := startSentinelPeer(t, "TestDurabilitySentinel_CloseThatIsNotTheLastKeepsIt", path, "commit-twice-then-wait")
	killed := false
	kill := func() {
		if !killed {
			killed = true
			_ = peer.Process.Kill()
			_ = peer.Wait()
		}
	}
	defer kill()
	sentinelWaitFile(t, path+".peer-ready")

	// The close that is not the last: no checkpoint, the sentinel stays.
	require.NoError(t, first.Close())
	_, err = os.Stat(marker)
	require.NoError(t, err, "a close with another process attached cleared the sentinel")

	// The other handle commits and its process is killed.
	require.NoError(t, os.WriteFile(path+".peer-go", nil, 0o644))
	sentinelWaitFile(t, path+".peer-done")
	kill()

	// The next open finds the sentinel and runs the quick check: damage
	// injected into the check's read fails the open with ErrQuickCheckFailed.
	errInjected := fmt.Errorf("%w: injected into the quick check's read", ErrPageIntegrity)
	testQuickCheckReadHook = func() error { return errInjected }
	t.Cleanup(func() { testQuickCheckReadHook = nil })
	failed, err := Open(ctx, path, sentinelConfig())
	testQuickCheckReadHook = nil
	if err == nil {
		_ = failed.Close()
	}
	require.ErrorIs(t, err, ErrQuickCheckFailed, "the open after a killed process ran no quick check")
	require.ErrorIs(t, err, errInjected)
	_, err = os.Stat(marker)
	require.NoError(t, err, "an open whose quick check failed cleared the sentinel")

	// Without the failure the check passes.
	next, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	stats, err := next.Stats(ctx)
	require.NoError(t, err)
	require.True(t, stats.DirtyOnOpen)
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

	clean, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	stats, err = clean.Stats(ctx)
	require.NoError(t, err)
	require.False(t, stats.DirtyOnOpen)
	require.NoError(t, clean.Close())
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

// lockProbeSentinel stands in for a handle's sentinel. When the close removes
// it, it records whether the database file was still locked exclusively: a
// process that attaches takes the file shared first, so while the lock is
// held none can attach and commit before the marker is gone.
type lockProbeSentinel struct {
	dbPath          string
	removed, locked bool
}

func (s *lockProbeSentinel) OnOpen(context.Context) (bool, error) { return false, nil }
func (s *lockProbeSentinel) MarkDirty()                          {}
func (s *lockProbeSentinel) MarkClean()                          {}

func (s *lockProbeSentinel) Remove() {
	s.removed = true
	f, err := os.Open(s.dbPath)
	if err != nil {
		return
	}
	defer func() { _ = f.Close() }()
	s.locked = errors.Is(syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB), syscall.EWOULDBLOCK)
}

// The last close removes the sentinel before it releases the database file.
// Removed after, it could remove the marker of a process that opened, passed
// its quick check and committed in between, and that process's commit would
// have no marker if it were then killed.
func TestDurabilitySentinel_LastCloseRemovesItUnderTheFileLock(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")
	d, err := Open(ctx, path, sentinelConfig())
	require.NoError(t, err)
	coll, err := d.CreateCollection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":"1"}`)))

	// The handle's sentinel is replaced by the probe.
	dbi := d.(*db)
	require.NoError(t, dbi.recoveryController.Stop())
	probe := &lockProbeSentinel{dbPath: path}
	dbi.recoveryController = durability.NewController(durability.Options{Sentinel: probe})

	require.NoError(t, d.Close())
	require.True(t, probe.removed, "the last close, which checkpointed, did not clear the sentinel")
	require.True(t, probe.locked, "the last close cleared the sentinel after it had released the database file")
}
