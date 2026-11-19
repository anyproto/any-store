//go:build (linux || darwin) && (amd64 || arm64)

package btree

// A writer that dies inside a transaction, after it has written dirty pages
// to the WAL to free memory, leaves the entries of those frames in the
// wal-index hash table: nothing rolls them back. They are above the log's
// last frame, so no reader uses them, but they occupy slots in their pages'
// probe chains. The next writer removes them before it adds an entry of its
// own (walIndexAppend, wal.c:1321-1330: `if( sLoc.aPgno[idx-1] ){
// walCleanupHash(pWal); }`).

import (
	"encoding/binary"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// deadWriterIndexes returns a wal-index of each kind: shared memory and heap.
func deadWriterIndexes(t *testing.T, f func(t *testing.T, wi *walIndex)) {
	for _, inProcess := range []bool{false, true} {
		name := "shared-memory"
		if inProcess {
			name = "heap"
		}
		t.Run(name, func(t *testing.T) {
			wi, err := newWalIndex(filepath.Join(t.TempDir(), "test.shm"), inProcess)
			require.NoError(t, err)
			defer func() { _ = wi.close(true) }()
			f(t, wi)
		})
	}
}

// pgnoWithHash returns a page number whose first hash slot is h.
func pgnoWithHash(t *testing.T, h int) uint32 {
	t.Helper()
	for pgno := uint32(2); pgno < 2+htNSlot; pgno++ {
		if int(pgno*htHash1)&(htNSlot-1) == h {
			return pgno
		}
	}
	t.Fatalf("no page number hashes to slot %d", h)
	return 0
}

// The dead writer's entries form one run of occupied slots. The next writer's
// first entry hashes to the start of the run: its probe may pass as many
// occupied slots as the segment has frames, and the run is longer.
func TestWalIndexDeadWriter_NoFalseCorrupt(t *testing.T) {
	deadWriterIndexes(t, func(t *testing.T, wi *walIndex) {
		const h0, nDead = 1000, 300
		require.NoError(t, wi.set(pgnoWithHash(t, 5000), 1)) // the log: one committed frame

		for k := 0; k < nDead; k++ { // the dead writer's frames 2..nDead+1
			require.NoError(t, wi.set(pgnoWithHash(t, h0+k), uint32(2+k)))
		}
		// It died: no rollback. The next writer starts from the committed
		// header (beginWriteWithSnapshot).
		wi.maxFrame.Store(1)

		pgno := pgnoWithHash(t, h0) + htNSlot // same slot as the dead writer's first page
		require.NoError(t, wi.set(pgno, 2), "the dead writer's entries are not part of the log")

		frame, err := wi.shmHashGet(pgno, 2, 1)
		require.NoError(t, err)
		require.Equal(t, uint32(2), frame)
		region, err := wi.shm.region(0, true)
		require.NoError(t, err)
		for idx := 2; idx < htNPageOne; idx++ {
			require.Zero(t, binary.LittleEndian.Uint32(region[htPgnoOffset(0, idx):]), "aPgno[%d] is above the log's last frame", idx)
		}
		used := 0
		for h := 0; h < htNSlot; h++ {
			if binary.LittleEndian.Uint16(region[htHashArrayOff+h*2:]) != 0 {
				used++
			}
		}
		require.Equal(t, 2, used, "one hash slot per frame of the log")
	})
}

// The dead writer wrote page P. The next writer commits P: its entry goes
// behind the dead one in P's probe chain. A later transaction writes a frame
// and rolls back, which zeroes the entries above the committed frame, the
// dead one among them: the chain must still reach the committed entry.
func TestWalIndexDeadWriter_CommittedEntryStaysReachable(t *testing.T) {
	deadWriterIndexes(t, func(t *testing.T, wi *walIndex) {
		const p, q = uint32(3), uint32(4)
		require.NoError(t, wi.set(p, 1)) // committed

		require.NoError(t, wi.set(q, 2)) // the dead writer: frames 2..4
		require.NoError(t, wi.set(q+1, 3))
		require.NoError(t, wi.set(p, 4))
		wi.maxFrame.Store(1) // it died

		require.NoError(t, wi.set(p, 2)) // the next writer commits P at frame 2
		frame, err := wi.shmHashGet(p, 2, 1)
		require.NoError(t, err)
		require.Equal(t, uint32(2), frame)

		require.NoError(t, wi.set(q, 3)) // a transaction writes a frame
		wi.rollbackToFrame(2)            // and rolls back

		frame, err = wi.shmHashGet(p, 2, 1)
		require.NoError(t, err)
		require.Equal(t, uint32(2), frame, "the committed frame of page %d is the one before the commit", p)
	})
}

// The same through the pager, the dead writer's entries planted as a kill
// leaves them: the committed row is read back, also by the writer that next
// changes its page, and after recovery.
func TestWalIndexDeadWriter_PlantedSlots(t *testing.T) {
	db, path := openRestartDB(t, restartModes[0]) // shared memory
	restartPut(t, db, 0, 200, 1)
	wi := db.pager.wal.index
	committed := wi.mxCommitFrame.LoadLocal()

	// The writer that dies: it rewrites every row and writes every page out.
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	restartPutTx(t, wtx, 0, 200, 9)
	restartSpillAll(t, db)
	require.Greater(t, db.pager.wal.nFrame.Load(), committed+2, "setup: the transaction wrote frames")
	region, err := wi.shm.region(0, true)
	require.NoError(t, err)
	left := append([]byte(nil), region[htPgnoOff0:]...)
	require.NoError(t, wtx.Rollback())
	copy(region[htPgnoOff0:], left) // no rollback happened

	deadWriterSurvivor(t, db, path)
}

// deadWriterSurvivor is what the surviving process does once the writer that
// rewrote rows [0,200) with fill 9 is dead, its frames in the WAL and in the
// wal-index.
func deadWriterSurvivor(t *testing.T, db *DB, path string) {
	t.Helper()
	restartPut(t, db, 5, 6, 2) // a page the dead writer wrote
	restartCheck(t, db, 5, 6, 2)

	wtx, err := db.BeginWrite() // a frame is written and rolled back
	require.NoError(t, err)
	restartPutTx(t, wtx, 150, 151, 3)
	restartSpillAll(t, db)
	require.NoError(t, wtx.Rollback())
	restartCheck(t, db, 5, 6, 2)

	restartPut(t, db, 6, 7, 4) // the same leaf: the writer reads the page
	restartCheck(t, db, 5, 6, 2)
	restartCheck(t, db, 6, 7, 4)

	require.NoError(t, db.Close())
	db2, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	restartCheck(t, db2, 0, 5, 1)
	restartCheck(t, db2, 5, 6, 2)
	restartCheck(t, db2, 6, 7, 4)
	restartCheck(t, db2, 7, 200, 1)
	require.NoError(t, db2.IntegrityCheck())
}

const deadWriterEnvPath = "ANYSTORE_DEAD_WRITER_DB"

// deadWriterChild is the writer that gets killed: it rewrites every row,
// writes every dirty page to the WAL, reports it and waits.
func deadWriterChild(t *testing.T) bool {
	path := os.Getenv(deadWriterEnvPath)
	if path == "" {
		return false
	}
	db, err := testOpen(t, path, restartModes[0].opts)
	require.NoError(t, err)
	wtx, err := db.BeginWrite()
	require.NoError(t, err)
	restartPutTx(t, wtx, 0, 200, 9)
	restartSpillAll(t, db)
	require.NoError(t, os.WriteFile(path+".spilled", binary.BigEndian.AppendUint32(nil, db.pager.wal.nFrame.Load()), 0o644))
	for {
		time.Sleep(time.Hour) // killed by the parent
	}
}

// A writer in another process is killed (SIGKILL) inside a transaction that
// wrote pages to the WAL, while this process stays attached.
func TestWalIndexDeadWriter_KilledProcess(t *testing.T) {
	if deadWriterChild(t) {
		return
	}
	db, path := openRestartDB(t, restartModes[0])
	restartPut(t, db, 0, 200, 1)
	committed := db.pager.wal.index.mxCommitFrame.LoadLocal()

	cmd := exec.Command(os.Args[0], "-test.run=^TestWalIndexDeadWriter_KilledProcess$", "-test.v", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), deadWriterEnvPath+"="+path)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	require.NoError(t, cmd.Start())
	killed := false
	defer func() {
		if !killed {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	require.Eventually(t, func() bool {
		_, err := os.Stat(path + ".spilled")
		return err == nil
	}, 30*time.Second, 10*time.Millisecond, "the peer never wrote its pages out")
	require.NoError(t, cmd.Process.Kill())
	_ = cmd.Wait()
	killed = true

	b, err := os.ReadFile(path + ".spilled")
	require.NoError(t, err)
	require.Len(t, b, 4)
	spilled := binary.BigEndian.Uint32(b)
	require.Greater(t, spilled, committed+2, "setup: the peer wrote frames")
	hdr, valid := db.pager.wal.index.readHeader()
	require.True(t, valid)
	require.Equal(t, committed, hdr.mxFrame, "setup: the peer committed nothing")
	region, err := db.pager.wal.index.shm.region(0, true)
	require.NoError(t, err)
	_, idx := htFrameSegIdx(spilled)
	require.NotZero(t, binary.LittleEndian.Uint32(region[htPgnoOffset(0, idx):]), "setup: the peer's entries are in the wal-index")

	deadWriterSurvivor(t, db, path)
}
