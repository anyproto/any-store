package btree

import (
	"os"
	"path/filepath"
	"testing"
)

func openWalForDedupTest(t *testing.T) (*wal, *os.File) {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "t.db")
	dbFile, err := os.OpenFile(dbPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := dbFile.Truncate(1 << 20); err != nil {
		t.Fatalf("truncate db: %v", err)
	}
	w := newWal(filepath.Join(dir, "t.wal"), 4096)
	if err := w.open(); err != nil {
		t.Fatalf("open wal: %v", err)
	}
	t.Cleanup(func() { _ = dbFile.Close() })
	return w, dbFile
}

// commitPage commits one new frame for (pgno). Real signatures:
//   - (w *wal) beginWrite() (stateChanged bool, err error)
//   - (w *wal) writeFrames(pages []*page, commit bool, dbSize uint32) error
//   - (w *wal) endWrite()
func commitPage(t *testing.T, w *wal, pgno uint32, data []byte) {
	t.Helper()
	if _, err := w.beginWrite(); err != nil {
		t.Fatalf("beginWrite: %v", err)
	}
	pg := &page{pgno: pgno, data: data}
	if err := w.writeFrames([]*page{pg}, true, pgno); err != nil {
		t.Fatalf("writeFrames: %v", err)
	}
	w.endWrite()
}

// TestWalBuildBackfillMap_DedupsRewrites asserts that for a rewrite-heavy
// workload (same pgno committed many times), the backfill dedup helper
// returns exactly one entry per distinct pgno, holding the LATEST frame
// index for that page. Matches SQLite's walIteratorNext semantics
// (wal.c:1758-1786).
func TestWalBuildBackfillMap_DedupsRewrites(t *testing.T) {
	w, _ := openWalForDedupTest(t)
	t.Cleanup(func() { _ = w.close(false) })

	pageData := make([]byte, w.pageSize)
	const nPgnos = 3
	const nRewrites = 5
	for r := 0; r < nRewrites; r++ {
		for p := uint32(1); p <= nPgnos; p++ {
			pageData[0] = byte(r)
			pageData[1] = byte(p)
			commitPage(t, w, p, pageData)
		}
	}

	latest, err := w.buildBackfillMap(0, w.nFrame.Load())
	if err != nil {
		t.Fatalf("buildBackfillMap: %v", err)
	}
	if len(latest) != nPgnos {
		t.Fatalf("expected %d distinct pgnos, got %d: %v", nPgnos, len(latest), latest)
	}
	// Total frames = nPgnos * nRewrites = 15. The latest frame for each
	// pgno is the one in the final rewrite round; each round commits
	// nPgnos frames in order (1,2,3), so:
	//   pgno 1 → frame (nRewrites-1)*nPgnos + 0 = 12
	//   pgno 2 → frame (nRewrites-1)*nPgnos + 1 = 13
	//   pgno 3 → frame (nRewrites-1)*nPgnos + 2 = 14
	expect := map[uint32]uint32{1: 12, 2: 13, 3: 14}
	for pgno, frame := range expect {
		if got := latest[pgno]; got != frame {
			t.Fatalf("pgno %d: expected frame %d, got %d", pgno, frame, got)
		}
	}
}

// TestWalBuildBackfillMap_InsertHeavyIsIdentity asserts that an
// insert-only workload (every frame a fresh pgno) produces a map with
// one entry per frame — the already-optimal case must not regress.
func TestWalBuildBackfillMap_InsertHeavyIsIdentity(t *testing.T) {
	w, _ := openWalForDedupTest(t)
	t.Cleanup(func() { _ = w.close(false) })

	pageData := make([]byte, w.pageSize)
	const nFrames = 10
	for p := uint32(1); p <= nFrames; p++ {
		pageData[0] = byte(p)
		commitPage(t, w, p, pageData)
	}

	latest, err := w.buildBackfillMap(0, w.nFrame.Load())
	if err != nil {
		t.Fatalf("buildBackfillMap: %v", err)
	}
	if uint32(len(latest)) != nFrames {
		t.Fatalf("expected %d entries (one per fresh pgno), got %d", nFrames, len(latest))
	}
	// Each pgno p was committed in exactly the (p-1)th commit → frame (p-1).
	for p := uint32(1); p <= nFrames; p++ {
		if got := latest[p]; got != p-1 {
			t.Fatalf("pgno %d: expected frame %d, got %d", p, p-1, got)
		}
	}
}

// TestCheckpoint_RewriteHeavyEndToEnd is an end-to-end sanity check:
// after a rewrite-heavy workload is checkpointed, the DB file contains
// the LATEST version of each page. Guards against dedup that picks the
// wrong frame.
func TestCheckpoint_RewriteHeavyEndToEnd(t *testing.T) {
	w, dbFile := openWalForDedupTest(t)
	t.Cleanup(func() { _ = w.close(false) })

	pageData := make([]byte, w.pageSize)
	const nPgnos = 3
	const nRewrites = 5
	for r := 0; r < nRewrites; r++ {
		for p := uint32(1); p <= nPgnos; p++ {
			pageData[0] = byte(r)
			pageData[1] = byte(p)
			commitPage(t, w, p, pageData)
		}
	}

	if err := w.checkpointWithMode(dbFile, nil, CheckpointFull, nil); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	// Verify: DB file page p starts with (nRewrites-1, p) — the latest
	// version committed. Pages are 4096 bytes each; pgno 1 starts at
	// offset 0, pgno p starts at (p-1)*4096.
	buf := make([]byte, int(w.pageSize))
	for p := uint32(1); p <= nPgnos; p++ {
		off := int64(p-1) * int64(w.pageSize)
		if _, err := dbFile.ReadAt(buf, off); err != nil {
			t.Fatalf("read db page %d: %v", p, err)
		}
		wantR := byte(nRewrites - 1)
		wantP := byte(p)
		if buf[0] != wantR || buf[1] != wantP {
			t.Fatalf("db page %d: expected (r=%d, p=%d), got (r=%d, p=%d) — checkpoint picked wrong frame",
				p, wantR, wantP, buf[0], buf[1])
		}
	}
}
