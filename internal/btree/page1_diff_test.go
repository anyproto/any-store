package btree

// The schema format 5 disk-output differential: a deterministic workload on
// a format 5 file, dumping every WAL frame record each commit wrote (page,
// commit mark, content hash; salts and checksums, which are random per log,
// left out). Run it on two builds and compare the dumps: format 5 commits
// must be byte-for-byte what they were. The file is self-contained so that it
// can be copied into an older tree.
//
// Ordering is fixed: the workload uses no savepoint rollback, whose page
// restore iterates a Go map; whole-transaction rollbacks, spills (a 10-page
// cache) and writer restarts (a checkpoint every 25 commits) keep their order.
// Run the same build twice first: the dumps must match.
//
//	BTREE_TEST_P1_DIFF_OUT=/path/dump.txt go test -run '^TestPage1Diff_Format5Dump$' ./internal/btree/

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPage1Diff_Format5Dump(t *testing.T) {
	out := os.Getenv("BTREE_TEST_P1_DIFF_OUT")
	if out == "" {
		t.Skip("set BTREE_TEST_P1_DIFF_OUT to write the dump")
	}
	prev := createLegacyFormat.Load()
	SetCreateLegacyFormatForTest(true)
	defer SetCreateLegacyFormatForTest(prev)
	path := filepath.Join(t.TempDir(), "diff.db")
	db, err := testOpen(t, path, Options{PageSize: 4096, CacheSize: 10, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, uint32(5), db.SchemaFormat())

	var dump bytes.Buffer
	rnd := rand.New(rand.NewSource(42))
	key := func(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) }
	val := func() []byte {
		n := 10 + rnd.Intn(200)
		if rnd.Intn(4) == 0 {
			n = 500 + rnd.Intn(12000)
		}
		v := make([]byte, n)
		rnd.Read(v)
		return v
	}
	names := []string{}
	commits := 0
	for i := 0; i < 300; i++ {
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		work := append([]string(nil), names...)
		if len(work) == 0 || rnd.Intn(20) == 0 {
			name := fmt.Sprintf("n%d", i)
			_, err := tx.CreateNamespace(name)
			require.NoError(t, err)
			work = append(work, name)
		}
		if len(work) > 2 && rnd.Intn(30) == 0 {
			j := rnd.Intn(len(work))
			require.NoError(t, tx.DeleteNamespace(work[j]))
			work = append(work[:j], work[j+1:]...)
		}
		ns, err := tx.GetNamespace(work[rnd.Intn(len(work))])
		require.NoError(t, err)
		for j := rnd.Intn(20); j >= 0; j-- {
			k := key(rnd.Intn(300))
			if rnd.Intn(4) == 0 {
				if err := tx.Delete(ns, k); err != nil {
					require.ErrorIs(t, err, ErrKeyNotFound)
				}
				continue
			}
			require.NoError(t, tx.Put(ns, k, val()))
		}
		if rnd.Intn(10) == 0 {
			require.NoError(t, tx.Rollback())
			continue
		}
		if rnd.Intn(2) == 0 {
			tx.MarkDataChanged()
		}
		require.NoError(t, tx.Commit())
		names = work
		commits++
		p1DiffDumpCommit(t, &dump, path, db, commits)
		if commits%25 == 0 {
			require.NoError(t, db.Checkpoint(CheckpointPassive))
		}
	}
	require.NoError(t, db.IntegrityCheck())
	require.NoError(t, os.WriteFile(out, dump.Bytes(), 0o644))
	t.Logf("%d commits dumped to %s", commits, out)
}

// p1DiffDumpCommit writes the frames of db's last commit: those after the
// previous commit mark, up to its commit frame.
func p1DiffDumpCommit(t *testing.T, w *bytes.Buffer, path string, db *DB, n int) {
	t.Helper()
	data, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	pageSize := int(db.pager.pageSize)
	last := int(db.pager.wal.nFrame.Load())
	type rec struct {
		pgno, nTrunc uint32
		sum          [32]byte
	}
	frames := make([]rec, 0, last)
	for i, off := 0, walHeaderSize; i < last; i, off = i+1, off+walFrameSize+pageSize {
		require.LessOrEqual(t, off+walFrameSize+pageSize, len(data))
		h := data[off : off+walFrameSize]
		frames = append(frames, rec{
			pgno:   binary.BigEndian.Uint32(h[0:4]),
			nTrunc: binary.BigEndian.Uint32(h[4:8]),
			sum:    sha256.Sum256(data[off+walFrameSize : off+walFrameSize+pageSize]),
		})
	}
	require.NotZero(t, frames[len(frames)-1].nTrunc)
	start := len(frames) - 1
	for start > 0 && frames[start-1].nTrunc == 0 {
		start--
	}
	fmt.Fprintf(w, "commit %d: %d frames\n", n, len(frames)-start)
	for _, f := range frames[start:] {
		fmt.Fprintf(w, "  page %d mark %d %x\n", f.pgno, f.nTrunc, f.sum[:8])
	}
}
