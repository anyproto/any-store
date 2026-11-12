package btree

// A process killed at a chosen point, then recovered two ways. In a schema
// format 6 file every case first leaves the newest page-1 frame behind
// acknowledged commits that do not write page 1, so recovery must take the
// header from an older frame; format 5 runs the same points after ordinary
// commits, all of which write page 1.
//
// The child stops at a barrier hook and the parent kills it (SIGKILL):
//
//	spill          after a spilled frame of an open transaction
//	pre-publish    a commit about to write its frames
//	post-publish   a commit published, before its bookkeeping
//	between-copies a commit between the two wal-index header copies
//	reset          a TRUNCATE checkpoint after the reset header, before the
//	               WAL is truncated
//	restart        a writer that started the log over, before its header
//
// Recovery: (a) a fresh open, in this process, of a copy of the database file
// and the WAL taken right after the kill, without the wal-index, so the index
// is rebuilt from the WAL before any checkpoint; (b) the parent, attached
// throughout, checkpoints with TRUNCATE and then reopens. Every acknowledged
// commit must be there with its values. The in-flight transaction is there
// with all its values when it was synced before the kill (post-publish,
// between-copies), and absent otherwise; a key counts as absent only on
// ErrKeyNotFound. The structure must be sound, and a growing commit afterwards
// must not reuse a page.

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const p1CrashChildEnv = "BTREE_TEST_P1_CRASH_CHILD"

var p1CrashPoints = []string{"spill", "pre-publish", "post-publish", "between-copies", "reset", "restart"}

// p1CrashCommit is one acknowledged commit of the child: values of size
// bytes, all of fill, at keys.
type p1CrashCommit struct {
	keys []int
	fill byte
	size int
}

// p1CrashCommits: a growing commit, which writes page 1, then three in-place
// commits, which in format 6 do not.
func p1CrashCommits() []p1CrashCommit {
	return []p1CrashCommit{
		{keys: []int{100, 101, 102}, fill: 2, size: 3000},
		{keys: []int{0, 1}, fill: 3, size: 20},
		{keys: []int{2, 3}, fill: 4, size: 20},
		{keys: []int{0, 4}, fill: 5, size: 20},
	}
}

// p1CrashInflight is what the transaction the child is killed in writes.
var p1CrashInflight = p1CrashCommit{keys: []int{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009}, fill: 9, size: 3000}

func p1CrashOpts(codec bool) Options {
	opts := Options{PageSize: 4096, CacheSize: 10, DisableAutoCheckpoint: true}
	if codec {
		opts.Key = []byte("page-1 crash test passphrase")
		opts.KDFIterations = 2
	}
	return opts
}

func p1CrashPut(tx *WriteTx, c p1CrashCommit) error {
	ns, err := tx.GetNamespace("t")
	if err != nil {
		return err
	}
	for _, k := range c.keys {
		if err := tx.Put(ns, p1Key(k), bytes.Repeat([]byte{c.fill}, c.size)); err != nil {
			return err
		}
	}
	return nil
}

func TestPage1Crash_Matrix(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, codec := range []bool{false, true} {
			for _, point := range p1CrashPoints {
				t.Run(fmt.Sprintf("format5=%v/codec=%v/%s", legacy, codec, point), func(t *testing.T) {
					if legacy {
						useLegacyFormat(t)
					} else {
						skipInLegacyFormatSuite(t)
					}
					runP1Crash(t, codec, point)
				})
			}
		}
	}
}

func runP1Crash(t *testing.T, codec bool, point string) {
	opts := p1CrashOpts(codec)
	path := filepath.Join(t.TempDir(), "crash.db")
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	// The base: small values at 0..19, all committed and checkpointed.
	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		ns, err := tx.CreateNamespace("t")
		require.NoError(t, err)
		for i := 0; i < 20; i++ {
			require.NoError(t, tx.Put(ns, p1Key(i), bytes.Repeat([]byte{1}, 20)))
		}
	})
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	model := map[int][]byte{}
	for i := 0; i < 20; i++ {
		model[i] = bytes.Repeat([]byte{1}, 20)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestPage1Crash_Child$", "-test.timeout=120s")
	cmd.Env = append(os.Environ(), p1CrashChildEnv+"="+path+"|"+strconv.FormatBool(codec)+"|"+point)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	log := &bytes.Buffer{}
	cmd.Stderr = log
	require.NoError(t, cmd.Start())
	acked := 0
	var page1Frames []string
	sc := bufio.NewScanner(stdout)
	reached := false
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "ACK ") {
			acked++
			continue
		}
		if strings.HasPrefix(line, "P1FRAME ") {
			page1Frames = append(page1Frames, strings.Fields(line)[2])
			continue
		}
		if line == "BARRIER "+point {
			reached = true
			break
		}
	}
	require.True(t, reached, "the child did not reach %s: %s", point, log)
	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	go func() { _, _ = io.Copy(io.Discard, stdout) }()
	require.Equal(t, len(p1CrashCommits()), acked, "every commit before the barrier was acknowledged")
	require.Len(t, page1Frames, acked)
	if db.pager.page1OnChange() {
		// The in-place commits left page 1 out: its newest frame is the
		// growing commit's, older than three acknowledged commits.
		for i := 1; i < acked; i++ {
			require.Equal(t, page1Frames[0], page1Frames[i], "commit %d wrote page 1", i)
		}
	} else {
		for i := 1; i < acked; i++ {
			require.NotEqual(t, page1Frames[i-1], page1Frames[i], "commit %d left page 1 out", i)
		}
	}
	for _, c := range p1CrashCommits()[:acked] {
		for _, k := range c.keys {
			model[k] = bytes.Repeat([]byte{c.fill}, c.size)
		}
	}

	// The in-flight commit's frames were written and synced before the kill
	// at these points, so recovery must keep it whole.
	inflightSynced := point == "post-publish" || point == "between-copies"
	check := func(t *testing.T, cdb *DB, where string) {
		t.Helper()
		rtx, err := cdb.BeginRead()
		require.NoError(t, err)
		ns, err := rtx.GetNamespace("t")
		require.NoError(t, err)
		for k, want := range model {
			got, err := rtx.Get(ns, p1Key(k))
			require.NoError(t, err, "%s: acknowledged key %d", where, k)
			require.Equal(t, want, got, "%s: acknowledged key %d", where, k)
		}
		inflight := bytes.Repeat([]byte{p1CrashInflight.fill}, p1CrashInflight.size)
		present := 0
		for _, k := range p1CrashInflight.keys {
			got, err := rtx.Get(ns, p1Key(k))
			if err != nil {
				require.ErrorIs(t, err, ErrKeyNotFound, "%s: in-flight key %d", where, k)
				continue
			}
			require.Equal(t, inflight, got, "%s: in-flight key %d", where, k)
			present++
		}
		require.NoError(t, rtx.Rollback())
		want := 0
		if inflightSynced {
			want = len(p1CrashInflight.keys)
		}
		require.Equal(t, want, present, "%s: in-flight keys present (synced before the kill: %v)", where, inflightSynced)
		require.NoError(t, cdb.IntegrityCheck(), where)
		p1Write(t, cdb, func(tx *WriteTx, ns *Namespace) { // grows
			for i := 0; i < 5; i++ {
				require.NoError(t, tx.Put(ns, p1Key(5000+i), p1Big(7)))
			}
		})
		require.NoError(t, cdb.IntegrityCheck(), where+", after a growing commit")
	}

	// (a) A fresh open of a copy of the database and the WAL (no
	// wal-index), before any checkpoint.
	copyPath := restartCopyFiles(t, path, "fresh")
	fresh, err := testOpen(t, copyPath, opts)
	require.NoError(t, err)
	check(t, fresh, "fresh open of a copy")
	require.NoError(t, fresh.Close())

	// (b) The parent, attached throughout: TRUNCATE, then a reopen.
	require.NoError(t, db.Checkpoint(CheckpointTruncate))
	check(t, db, "attached parent")
	for k := 0; k < 5; k++ {
		model[5000+k] = p1Big(7)
	}
	if inflightSynced {
		for _, k := range p1CrashInflight.keys {
			model[k] = bytes.Repeat([]byte{p1CrashInflight.fill}, p1CrashInflight.size)
		}
	}
	require.NoError(t, db.Close())
	db, err = testOpen(t, path, opts)
	require.NoError(t, err)
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	ns, err := rtx.GetNamespace("t")
	require.NoError(t, err)
	for k, want := range model {
		got, err := rtx.Get(ns, p1Key(k))
		require.NoError(t, err, "reopen: key %d", k)
		require.Equal(t, want, got, "reopen: key %d", k)
	}
	if !inflightSynced {
		for _, k := range p1CrashInflight.keys {
			_, err := rtx.Get(ns, p1Key(k))
			require.ErrorIs(t, err, ErrKeyNotFound, "reopen: in-flight key %d", k)
		}
	}
	require.NoError(t, rtx.Rollback())
	require.NoError(t, db.IntegrityCheck())
}

// TestPage1Crash_Child is the child of TestPage1Crash_Matrix.
func TestPage1Crash_Child(t *testing.T) {
	arg := os.Getenv(p1CrashChildEnv)
	if arg == "" {
		t.Skip("child process only")
	}
	f := strings.Split(arg, "|")
	path, point := f[0], f[2]
	codec, err := strconv.ParseBool(f[1])
	require.NoError(t, err)
	db, err := Open(path, p1CrashOpts(codec))
	require.NoError(t, err)
	for i, c := range p1CrashCommits() {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		require.NoError(t, p1CrashPut(tx, c))
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
		fmt.Printf("ACK %d\n", i)
		w := db.pager.wal
		frame, err := w.index.get(1, w.nFrame.Load(), w.index.liveMinFrame())
		require.NoError(t, err)
		fmt.Printf("P1FRAME %d %d\n", i, frame)
	}
	stop := func(name string) {
		fmt.Printf("BARRIER %s\n", name)
		_ = os.Stdout.Sync()
		select {}
	}
	inflight := func() {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		require.NoError(t, p1CrashPut(tx, p1CrashInflight))
		tx.MarkDataChanged()
		require.NoError(t, tx.Commit())
		t.Fatalf("the barrier %s was not reached", point)
	}
	switch point {
	case "spill":
		testAfterSpillHook = func() { stop(point) }
		inflight()
	case "pre-publish":
		testCommitPrePublishHook = func() { stop(point) }
		inflight()
	case "post-publish":
		testCommitPostPublishHook = func() { stop(point) }
		inflight()
	case "between-copies":
		testPublishHeaderHook = func(step string) {
			if step == "between" {
				stop(point)
			}
		}
		inflight()
	case "reset":
		testResetWALHook = func(step string) {
			if step == "truncate" {
				stop(point)
			}
		}
		require.NoError(t, db.Checkpoint(CheckpointTruncate))
		t.Fatalf("the barrier %s was not reached", point)
	case "restart":
		require.NoError(t, db.Checkpoint(CheckpointPassive))
		testRestartLogHook = func(step string) {
			if step == "restarted" {
				stop(point)
			}
		}
		inflight()
	default:
		t.Fatalf("unknown point %q", point)
	}
}
