//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testDMSProbeEnv          = "TEST_SHM_DMS_PROBE_PATH"
	testRefusedOpenReaderEnv = "TEST_REFUSED_OPEN_READER_DIR"
)

// shmProbe is what ANOTHER process sees of the live handle's shm: the
// lock type on the DMS byte (F_RDLCK while a live attacher holds it,
// F_UNLCK once nobody does) and the wal-index header plus checkpoint
// info (region 0, first 136 bytes). Same-process probes always report
// F_UNLCK, and opening the shm in-process drops the live handle's record
// locks, hence the child.
type shmProbe struct {
	dms    string
	size   int
	header []byte
}

func (p shmProbe) String() string {
	u := func(off int) uint32 { return binary.LittleEndian.Uint32(p.header[off:]) }
	return fmt.Sprintf("dms=%s size=%d mxFrame=%d nBackfill=%d aReadMark=[%d %d %d %d %d] nBackfillAttempted=%d",
		p.dms, p.size, u(16), u(96), u(100), u(104), u(108), u(112), u(116), u(128))
}

func probeShmFromChild(t *testing.T, shmPath string) shmProbe {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestMmap_RefusedSameProcessOpenLeavesLiveShmIntact$", "-test.v")
	cmd.Env = append(os.Environ(), testDMSProbeEnv+"="+shmPath)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
	var p shmProbe
	for _, line := range strings.Split(string(out), "\n") {
		switch {
		case strings.HasPrefix(line, "DMS="):
			p.dms = strings.TrimSpace(strings.TrimPrefix(line, "DMS="))
		case strings.HasPrefix(line, "SIZE="):
			_, err = fmt.Sscanf(line, "SIZE=%d", &p.size)
			require.NoError(t, err)
		case strings.HasPrefix(line, "HDR="):
			p.header, err = hex.DecodeString(strings.TrimSpace(strings.TrimPrefix(line, "HDR=")))
			require.NoError(t, err)
		}
	}
	require.NotEmpty(t, p.dms, "no DMS line in child output:\n%s", out)
	require.Len(t, p.header, 136)
	return p
}

func shmProbeChild(t *testing.T, shmPath string) {
	f, err := os.OpenFile(shmPath, os.O_RDWR, 0)
	require.NoError(t, err)
	defer f.Close()
	fl := syscall.Flock_t{Type: syscall.F_WRLCK, Whence: 0, Start: shmDMSOffset, Len: 1}
	require.Zero(t, fcntlFlock(f.Fd(), syscall.F_GETLK, &fl))
	name := map[int16]string{syscall.F_RDLCK: "F_RDLCK", syscall.F_WRLCK: "F_WRLCK", syscall.F_UNLCK: "F_UNLCK"}[fl.Type]
	raw, err := os.ReadFile(shmPath)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(raw), 136)
	fmt.Printf("DMS=%s\nSIZE=%d\nHDR=%s\n", name, len(raw), hex.EncodeToString(raw[:136]))
}

// A second Open of a DB this process already holds is refused before it
// takes a lock or opens the WAL and shm, so the live handle sees nothing
// of it: its wal-index header, its DMS attachment and its readers'
// snapshots stay as they were. A same-process attacher would win the DMS
// election (record locks are per process), truncate the live shm and, on
// closing its fd, drop every record lock the process holds on it.
func TestMmap_RefusedSameProcessOpenLeavesLiveShmIntact(t *testing.T) {
	if shmPath := os.Getenv(testDMSProbeEnv); shmPath != "" {
		shmProbeChild(t, shmPath)
		return
	}
	path := filepath.Join(t.TempDir(), "live.db")
	db, err := testOpen(t, path, Options{DisableAutoCheckpoint: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	shmPath := path + "-wal-shm"

	const ns = "ns"
	createNS(t, db, ns)
	putN(t, db, ns, 4000, 64)
	require.NoError(t, db.Checkpoint(CheckpointTruncate)) // everything in the DB file, WAL empty

	valA := bytes.Repeat([]byte{0xAA}, 64)
	valB := bytes.Repeat([]byte{0xBB}, 64)
	putKey(t, db, ns, 1, valA) // frames <= m
	rtx, err := db.BeginRead() // snapshot m
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })
	putKey(t, db, ns, 4000, valB)                        // frames > m, on a leaf outside the snapshot's WAL frames
	require.NoError(t, db.Checkpoint(CheckpointPassive)) // backfills <= m, pinned by rtx

	before := probeShmFromChild(t, shmPath)
	t.Logf("live shm before refused open: %s", before)
	assert.Equal(t, "F_RDLCK", before.dms, "live handle holds the DMS shared lock")

	for i := 0; i < 2; i++ {
		db2, err := Open(path, Options{})
		if db2 != nil {
			_ = db2.Close()
		}
		require.ErrorIs(t, err, ErrDatabaseOpen)
	}

	after := probeShmFromChild(t, shmPath)
	t.Logf("live shm after refused open:  %s", after)
	assert.Equal(t, before.header, after.header, "refused open rewrote the live wal-index: before %s, after %s", before, after)
	assert.Equal(t, "F_RDLCK", after.dms, "refused open dropped the live handle's DMS lock")

	// With rtx's read mark intact the next checkpoint stops at the snapshot.
	require.NoError(t, db.Checkpoint(CheckpointPassive))
	t.Logf("live shm after next checkpoint: %s", probeShmFromChild(t, shmPath))
	nsRef, err := db.getNamespaceLocked(ns)
	require.NoError(t, err)
	got, err := rtx.Get(nsRef, binary.BigEndian.AppendUint32(nil, 4000))
	require.NoError(t, err)
	assert.Equal(t, make([]byte, 64), got, "snapshot read of key 4000 returned a value committed after the snapshot")
	got, err = rtx.Get(nsRef, binary.BigEndian.AppendUint32(nil, 1))
	require.NoError(t, err)
	assert.Equal(t, valA, got)
}

// A live reader that keeps hitting a multi-region wal-index while refused
// opens run must not fault: a refused open never truncates the live shm.
// Runs the scenario in a child process, where a fault kills it.
func TestMmap_RefusedSameProcessOpenLiveReaderSurvives(t *testing.T) {
	if dir := os.Getenv(testRefusedOpenReaderEnv); dir != "" {
		refusedOpenLiveReaderChild(t, dir)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMmap_RefusedSameProcessOpenLiveReaderSurvives$", "-test.v", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), testRefusedOpenReaderEnv+"="+t.TempDir())
	out, err := cmd.CombinedOutput()
	if err != nil {
		text := string(out)
		if i := strings.Index(text, "goroutine "); i > 0 && len(text)-i > 6000 {
			text = text[:i+6000] + "\n… (truncated)"
		}
		t.Fatalf("child failed: %v\n%s", err, text)
	}
	t.Logf("child: %s", lastLines(string(out), 4))
}

func refusedOpenLiveReaderChild(t *testing.T, dir string) {
	path := filepath.Join(dir, "live.db")
	db, err := testOpen(t, path, Options{DisableAutoCheckpoint: true})
	require.NoError(t, err)
	defer db.Close()
	const ns, n = "ns", 20000
	createNS(t, db, ns)
	putN(t, db, ns, n, 64)
	for i := 0; i < 12; i++ {
		updateAll(t, db, ns, n, 64)
	}
	mx := db.pager.wal.index.maxFrame.Load()
	fmt.Printf("live wal frames=%d\n", mx)
	require.Greater(t, mx, uint32(htNPageOne), "the wal-index must span more than one region")

	nsRef, err := db.getNamespaceLocked(ns)
	require.NoError(t, err)
	stop := make(chan struct{})
	started := make(chan struct{})
	readerDone := make(chan error, 1)
	go func() {
		rnd := rand.New(rand.NewSource(1))
		for round := 0; ; round++ {
			select {
			case <-stop:
				readerDone <- nil
				return
			default:
			}
			rtx, err := db.BeginRead()
			if err != nil {
				readerDone <- err
				return
			}
			for i := 0; i < 64; i++ {
				key := binary.BigEndian.AppendUint32(nil, uint32(rnd.Intn(n))+1)
				if _, err := rtx.Get(nsRef, key); err != nil {
					_ = rtx.Rollback()
					readerDone <- err
					return
				}
			}
			_ = rtx.Rollback()
			if round == 0 {
				close(started)
			}
		}
	}()
	select {
	case <-started:
	case err := <-readerDone:
		t.Fatalf("reader stopped before the opens: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("reader did not start")
	}
	for i := 0; i < 30; i++ {
		db2, err := Open(path, Options{})
		if db2 != nil {
			_ = db2.Close()
		}
		require.ErrorIs(t, err, ErrDatabaseOpen)
	}
	close(stop)
	select {
	case err := <-readerDone:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("reader did not stop")
	}
	fmt.Println("live reader survived 30 refused opens")
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
