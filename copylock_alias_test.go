//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// A staging name that is another name of a live database's -wal-shm. The
// copy must not open it to try its lock: closing a descriptor of a file
// drops every POSIX record lock the process holds on that file, whichever
// descriptor took them (os_unix.c:1246-1254 at version-3.52.0), and the
// -wal-shm carries the database's record locks. Those are seen from another
// process, as the database's peers see them (re-exec).

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

const stagingAliasPeerEnv = "ANYSTORE_TEST_STAGING_ALIAS_SHM"

// The record-lock bytes of a -wal-shm the peer tries: the write lock (slot
// 0 of the lock bytes at offset 120) and the dead-man switch after them,
// held shared for as long as the database is open (internal/btree
// shm_mmap.go).
var stagingAliasLockBytes = []int64{120, 128}

// The peer tries an exclusive record lock, without waiting, on each lock
// byte of the -wal-shm named in the environment, and prints what it got.
// Its locks go when it exits.
func stagingAliasPeer(t *testing.T) bool {
	shm := os.Getenv(stagingAliasPeerEnv)
	if shm == "" {
		return false
	}
	f, err := os.OpenFile(shm, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	for _, off := range stagingAliasLockBytes {
		lk := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: off, Len: 1}
		got := "granted"
		if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lk); err != nil {
			got = "held: " + err.Error()
		}
		fmt.Printf("record lock %d: %s\n", off, got)
	}
	return true
}

// A database open in this process, in a write transaction, and the copy's
// staging name made a symbolic link, then a hard link, to its -wal-shm: the
// copy fails, opening nothing and removing nothing, and another process
// still finds every record lock of the database held once the call has
// returned. The database goes on committing. A symbolic link that replaces
// a regular staging file after the copy has looked at it, before the open,
// is refused by the open, which does not follow it.
func TestCopyToLatestFormat_StagingAliasOfLiveShm(t *testing.T) {
	if stagingAliasPeer(t) {
		return
	}
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)

	for _, alias := range []string{"symbolic link", "hard link", "symbolic link after the Lstat"} {
		t.Run(alias, func(t *testing.T) {
			dir := t.TempDir()
			otherPath := filepath.Join(dir, "other.db")
			other, err := Open(ctx, otherPath, nil)
			require.NoError(t, err)
			defer func() { _ = other.Close() }()
			oc, err := other.CreateCollection(ctx, "other")
			require.NoError(t, err)
			insertJSON(t, oc, `{"id":1}`)
			wtx, err := other.WriteTx(ctx)
			require.NoError(t, err)
			defer func() { _ = wtx.Rollback() }()
			require.NoError(t, oc.Insert(wtx.Context(), anyenc.MustParseJson(`{"id":2}`)))

			shm := otherPath + "-wal-shm"
			peer := func() string {
				t.Helper()
				cmd := exec.Command(os.Args[0], "-test.run=^TestCopyToLatestFormat_StagingAliasOfLiveShm$", "-test.timeout=60s")
				cmd.Env = append(os.Environ(), stagingAliasPeerEnv+"="+shm)
				out, err := cmd.CombinedOutput()
				require.NoError(t, err, "peer failed:\n%s", out)
				return string(out)
			}
			for _, off := range stagingAliasLockBytes {
				require.Contains(t, peer(), fmt.Sprintf("record lock %d: held", off), "setup: the database holds the lock")
			}

			dstPath := filepath.Join(dir, "x")
			stage := dstPath + ".copy-staging"
			switch alias {
			case "symbolic link":
				require.NoError(t, os.Symlink(shm, stage))
			case "hard link":
				require.NoError(t, os.Link(shm, stage))
			default:
				require.NoError(t, os.WriteFile(stage, []byte("left"), 0o644))
				testHookStagingLstat = func(path string) {
					require.NoError(t, os.Remove(path))
					require.NoError(t, os.Symlink(shm, path))
				}
				defer func() { testHookStagingLstat = nil }()
			}

			report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
			require.Error(t, err)
			assert.ErrorContains(t, err, "the staging file "+stage)
			assert.NotErrorIs(t, err, ErrBusy)
			assert.Zero(t, report)
			assert.Equal(t, []string{"other.db", "other.db-wal", "other.db-wal-shm", "x.copy-staging"}, dirNames(t, dir), "the copy touches nothing")
			target, err := os.Readlink(stage)
			if alias == "hard link" {
				assert.Error(t, err, "the staging name is a hard link")
			} else if assert.NoError(t, err) {
				assert.Equal(t, shm, target)
			}

			out := peer()
			for _, off := range stagingAliasLockBytes {
				assert.Contains(t, out, fmt.Sprintf("record lock %d: held", off), "the database still holds the lock")
			}
			require.NoError(t, wtx.Commit())
			n, err := oc.Count(ctx)
			require.NoError(t, err)
			assert.Equal(t, 2, n)
		})
	}
}
