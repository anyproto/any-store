//go:build (linux || darwin) && (amd64 || arm64)

package anystore

// A name a copy keeps for itself that is another name of a live database's
// -wal-shm. The copy must not open it: closing a descriptor of a file drops
// every POSIX record lock the process holds on that file, whichever
// descriptor took them (os_unix.c:1246-1254 at version-3.52.0), and the
// -wal-shm carries the database's record locks. Those are seen from another
// process, as the database's peers see them (re-exec).

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

const reservedAliasPeerEnv = "ANYSTORE_TEST_RESERVED_ALIAS_SHM"

// The record-lock bytes of a -wal-shm the peer tries: the write lock (slot
// 0 of the lock bytes at offset 120) and the dead-man switch after them,
// held shared for as long as the database is open (internal/btree
// shm_mmap.go).
var reservedAliasLockBytes = []int64{120, 128}

// The peer prints a digest of the content of the -wal-shm named in the
// environment, then tries an exclusive record lock, without waiting, on
// each of its lock bytes, and prints what it got. Its locks go when it
// exits. The test reads the -wal-shm only through the peer: a descriptor
// of its own, closed, would drop the database's locks.
func reservedAliasPeer(t *testing.T) bool {
	shm := os.Getenv(reservedAliasPeerEnv)
	if shm == "" {
		return false
	}
	f, err := os.OpenFile(shm, os.O_RDWR, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()
	h := sha256.New()
	_, err = io.Copy(h, f)
	require.NoError(t, err)
	fmt.Printf("content: %x\n", h.Sum(nil))
	for _, off := range reservedAliasLockBytes {
		lk := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: off, Len: 1}
		got := "granted"
		if err := syscall.FcntlFlock(f.Fd(), syscall.F_SETLK, &lk); err != nil {
			got = "held: " + err.Error()
		}
		fmt.Printf("record lock %d: %s\n", off, got)
	}
	return true
}

// A database open in this process, in a write transaction, and a name the
// copy keeps for itself made a symbolic link, then a hard link, to its
// -wal-shm: the staging name, the destination's lock file, and the lock
// file of the database a sidecar destination (y-wal) would be a file of.
// The copy fails, opening, writing and removing nothing there, and another
// process still finds every record lock of the database held once the call
// has returned. The database goes on committing. A symbolic link that
// replaces a regular file at the name after the copy has looked at it,
// before the open, is refused by the open, which does not follow it.
func TestCopyToLatestFormat_ReservedNameAliasOfLiveShm(t *testing.T) {
	if reservedAliasPeer(t) {
		return
	}
	src, _ := openSource(t, nil)
	coll, err := src.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	insertJSON(t, coll, `{"id":1}`)

	for _, name := range []struct{ what, dst, reserved string }{
		{"staging file", "x", "x.copy-staging"},
		{"lock file", "x", "x.copy-lock"},
		{"lock file", "y-wal", "y.copy-lock"},
	} {
		for _, alias := range []string{"symbolic link", "hard link", "symbolic link after the Lstat"} {
			t.Run(name.reserved+", "+alias, func(t *testing.T) {
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
					cmd := exec.Command(os.Args[0], "-test.run=^TestCopyToLatestFormat_ReservedNameAliasOfLiveShm$", "-test.timeout=60s")
					cmd.Env = append(os.Environ(), reservedAliasPeerEnv+"="+shm)
					out, err := cmd.CombinedOutput()
					require.NoError(t, err, "peer failed:\n%s", out)
					return string(out)
				}
				content := func(out string) string {
					t.Helper()
					line, _, _ := strings.Cut(out, "\n")
					require.True(t, strings.HasPrefix(line, "content: "), out)
					return line
				}
				before := peer()
				for _, off := range reservedAliasLockBytes {
					require.Contains(t, before, fmt.Sprintf("record lock %d: held", off), "setup: the database holds the lock")
				}

				dstPath := filepath.Join(dir, name.dst)
				reserved := filepath.Join(dir, name.reserved)
				switch alias {
				case "symbolic link":
					require.NoError(t, os.Symlink(shm, reserved))
				case "hard link":
					require.NoError(t, os.Link(shm, reserved))
				default:
					require.NoError(t, os.WriteFile(reserved, []byte("left"), 0o644))
					replaced := false
					testHookReservedLstat = func(path string) {
						if path != reserved || replaced {
							return
						}
						replaced = true
						require.NoError(t, os.Remove(path))
						require.NoError(t, os.Symlink(shm, path))
					}
					defer func() { testHookReservedLstat = nil }()
				}

				report, err := CopyToLatestFormat(ctx, src, dstPath, nil)
				require.Error(t, err)
				assert.ErrorContains(t, err, reserved)
				if alias != "symbolic link after the Lstat" {
					assert.ErrorIs(t, err, errReservedNameAlias)
					assert.ErrorContains(t, err, "the "+name.what+" "+reserved)
				}
				assert.NotErrorIs(t, err, ErrBusy)
				assert.Zero(t, report)
				assert.Equal(t, []string{"other.db", "other.db-wal", "other.db-wal-shm", name.reserved}, dirNames(t, dir), "the copy touches nothing")
				if alias == "hard link" {
					fi, err := os.Lstat(reserved)
					require.NoError(t, err)
					shmInfo, err := os.Lstat(shm)
					require.NoError(t, err)
					assert.True(t, os.SameFile(fi, shmInfo), "the hard link stays")
				} else {
					target, err := os.Readlink(reserved)
					if assert.NoError(t, err) {
						assert.Equal(t, shm, target)
					}
				}

				out := peer()
				assert.Equal(t, content(before), content(out), "nothing is written to the -wal-shm")
				for _, off := range reservedAliasLockBytes {
					assert.Contains(t, out, fmt.Sprintf("record lock %d: held", off), "the database still holds the lock")
				}
				require.NoError(t, wtx.Commit())
				n, err := oc.Count(ctx)
				require.NoError(t, err)
				assert.Equal(t, 2, n)
			})
		}
	}
}
