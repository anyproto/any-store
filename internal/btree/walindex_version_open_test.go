//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const futureIndexEnvPath = "TEST_FUTURE_INDEX_DB_PATH"

// plantFutureIndexVersion rewrites both copies of the attached wal-index
// header as a consistent, checksummed header of the next wal-index version:
// what a newer build attached to the same database would publish.
func plantFutureIndexVersion(t *testing.T, db *DB) {
	t.Helper()
	w := db.pager.wal
	hdr, valid := w.index.readHeader()
	require.True(t, valid)
	region, err := w.index.shm.region(0, true)
	require.NoError(t, err)
	future := hdr
	future.iVersion = walVersion + 1
	future.computeCksum()
	var buf [walIndexHdrSize]byte
	future.serialize(buf[:])
	copy(region[walIndexHdrSize:walIndexHdrSize*2], buf[:])
	copy(region[0:walIndexHdrSize], buf[:])
	_, state := w.index.readHeaderState()
	require.Equal(t, walHdrBadVersion, state)
}

// futureIndexHolder is the peer process: it opens the database, plants a
// future-version wal-index header, reports it and stays attached until its
// stdin is closed.
func futureIndexHolder(t *testing.T) bool {
	path := os.Getenv(futureIndexEnvPath)
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	plantFutureIndexVersion(t, db)
	os.Stdout.WriteString("\nHOLDING-FUTURE-INDEX\n")
	_, _ = bufio.NewReader(os.Stdin).ReadByte()
	os.Exit(0) // no Close: the index stays as planted
	return true
}

// TestWalIndexFutureVersionRefusedAtOpen: a live process holds a wal-index of
// a newer version. Opening the database from another process is refused
// with ErrWALVersion, and the index is not rebuilt: SQLite's walIndexReadHdr
// returns SQLITE_CANTOPEN for a consistent header of another version and
// runs no recovery (wal.c:2722-2728).
func TestWalIndexFutureVersionRefusedAtOpen(t *testing.T) {
	if futureIndexHolder(t) {
		return
	}
	path := filepath.Join(t.TempDir(), "test.db")
	db, err := testOpen(t, path, tornHdrOpts)
	require.NoError(t, err)
	dmsPutRange(t, db, "t1", 0, tornHdrSeedRows)
	require.NoError(t, db.Close())

	cmd := exec.Command(os.Args[0], "-test.run=^TestWalIndexFutureVersionRefusedAtOpen$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), futureIndexEnvPath+"="+path)
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	defer func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	}()

	ready := make(chan bool, 1)
	var childOut bytes.Buffer
	go func() {
		sc := bufio.NewScanner(stdout)
		held := false
		for sc.Scan() {
			childOut.WriteString(sc.Text() + "\n")
			if sc.Text() == "HOLDING-FUTURE-INDEX" && !held {
				held = true
				ready <- true
			}
		}
		if !held {
			ready <- false
		}
	}()
	select {
	case ok := <-ready:
		if !ok {
			// The output is complete only once the child is reaped: the
			// scanner has returned and exec's stderr copier has finished.
			_ = cmd.Wait()
			t.Fatalf("the holder exited early:\n%s%s", childOut.String(), stderr.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the holder did not report")
	}

	shmPath := path + "-wal-shm"
	before, err := os.ReadFile(shmPath)
	require.NoError(t, err)

	db2, err := testOpen(t, path, tornHdrOpts)
	if err == nil {
		_ = db2.Close()
	}
	require.ErrorIs(t, err, ErrWALVersion)

	after, err := os.ReadFile(shmPath)
	require.NoError(t, err)
	require.True(t, bytes.Equal(before, after), "the other version's wal-index must not be rewritten")
}
