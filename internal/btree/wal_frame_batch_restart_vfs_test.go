//go:build vfs && (linux || darwin) && (amd64 || arm64)

package btree

// The chunked frame writes (wal_frame_batch_vfs_test.go) with the writer's log
// restart (wal_writer_restart_test.go) and the schema format 6 page-1 rule.
// writeFrames must keep walFrames' order (wal.c:4067-4115): the restart
// before any frame, a restarted log's header written and synced before its
// first frame, then the frames in chunks, then the sync, and only then the
// commit published. A chunk that fails leaves the last commit as it was, and
// after a restart the transaction unwinds to the start of the new log.

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

var errInjectedChunk = errors.New("injected WAL chunk write failure")

// chunkRows rows of 300 bytes fill about 200 leaves of 4 KiB: three chunks of
// walFrameBatchMax or more.
const chunkRows = 2500

// chunkModes are restartModes with a cache that holds a whole chunkRows
// commit, so that the commit writes it rather than the cache spilling it page
// by page.
func chunkModes() []restartMode {
	out := make([]restartMode, len(restartModes))
	for i, m := range restartModes {
		m.opts.CacheSize = 4000
		out[i] = m
	}
	return out
}

// walEvent is a write or a sync of the WAL file, or a writer step (a
// testRestartLogHook step, "pre-publish" from testCommitPublishErrHook), with
// the committed frame at that instant and, with shared memory, the live
// wal-index header's mxFrame.
type walEvent struct {
	kind  string
	off   int64
	n     int
	mx    uint32
	shmMx uint32
}

// orderedWAL logs the WAL file's writes and syncs while armed, and can fail
// one chunk: a write of whole frames past the WAL header.
type orderedWAL struct {
	File
	w         *wal
	shm       bool
	frameSize int

	mu      sync.Mutex
	armed   bool
	fail    string // "", "second": the second chunk, "commit": the chunk carrying the commit mark
	failed  bool
	chunks  int
	events  []walEvent
	onChunk func(n int) // after the n-th chunk landed, outside mu
}

// wrapWAL puts an orderedWAL under w and logs the writer steps in it.
func wrapWAL(t *testing.T, w *wal) *orderedWAL {
	t.Helper()
	ow := &orderedWAL{File: w.file, w: w, shm: !w.inProcess && !w.inMemory, frameSize: int(walFrameSize) + int(w.pageSize)}
	w.file = ow
	testRestartLogHook = func(step string) { ow.step(step) }
	testCommitPublishErrHook = func() error { ow.step("pre-publish"); return nil }
	t.Cleanup(func() {
		testRestartLogHook = nil
		testCommitPublishErrHook = nil
	})
	return ow
}

func (f *orderedWAL) event(kind string, off int64, n int) walEvent {
	e := walEvent{kind: kind, off: off, n: n, mx: f.w.index.mxCommitFrame.LoadLocal()}
	if f.shm {
		if hdr, ok := f.w.index.readHeader(); ok {
			e.shmMx = hdr.mxFrame
		}
	}
	return e
}

func (f *orderedWAL) step(kind string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.armed {
		f.events = append(f.events, f.event(kind, 0, 0))
	}
}

func (f *orderedWAL) WriteAt(b []byte, off int64) (int, error) {
	f.mu.Lock()
	chunk := 0
	if f.armed {
		f.events = append(f.events, f.event("write", off, len(b)))
		if off >= walHeaderSize && len(b) > 0 && len(b)%f.frameSize == 0 {
			f.chunks++
			chunk = f.chunks
			last := b[len(b)-f.frameSize:]
			commitChunk := binary.BigEndian.Uint32(last[4:8]) != 0
			if !f.failed && (f.fail == "second" && chunk == 2 || f.fail == "commit" && commitChunk) {
				f.failed = true
				f.events[len(f.events)-1].kind = "failed-write"
				f.mu.Unlock()
				return 0, errInjectedChunk
			}
		}
	}
	onChunk := f.onChunk
	f.mu.Unlock()
	n, err := f.File.WriteAt(b, off)
	if chunk > 0 && err == nil && onChunk != nil {
		onChunk(chunk)
	}
	return n, err
}

func (f *orderedWAL) Sync() error {
	f.step("sync")
	return f.File.Sync()
}

func (f *orderedWAL) arm(fail string) {
	f.mu.Lock()
	f.armed, f.fail, f.failed, f.chunks, f.events = true, fail, false, 0, nil
	f.mu.Unlock()
}

func (f *orderedWAL) disarm() ([]walEvent, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armed = false
	return f.events, f.failed
}

// checkChunks checks the chunk writes of one commit's writeFrames call that
// appends after frame start: contiguous from start's end, whole frames, at
// most walFrameBatchMax each, and every one written while the committed frame
// (and the live wal-index header) still named committed, the transaction's
// base. It returns their frame count and their number.
func checkChunks(t *testing.T, ev []walEvent, start, committed uint32, frameSize int, shm bool) (frames, chunks int) {
	t.Helper()
	next := int64(walHeaderSize) + int64(start)*int64(frameSize)
	for i, e := range ev {
		if (e.kind != "write" && e.kind != "failed-write") || e.off < walHeaderSize || e.n%frameSize != 0 {
			continue
		}
		require.Equal(t, next, e.off, "event %d: chunk %d not contiguous with the frames before it", i, chunks+1)
		require.LessOrEqual(t, e.n, walFrameBatchMax, "event %d", i)
		require.Equal(t, committed, e.mx, "event %d: chunk %d written after the commit was published", i, chunks+1)
		if shm {
			require.Equal(t, committed, e.shmMx, "event %d: chunk %d written after the wal-index header named the commit", i, chunks+1)
		}
		next += int64(e.n)
		frames += e.n / frameSize
		chunks++
	}
	return frames, chunks
}

// checkRestartOrder checks one commit's writeFrames that started the log over:
// nothing is written before the restart, the new log's header is written and
// synced before the first chunk (wal.c:4077-4115), the chunks hold the
// commit's frames numbered from 1, and the sync after the last chunk comes
// before the commit is published.
func checkRestartOrder(t *testing.T, ev []walEvent, frameSize int, shm bool, wantFrames uint32) {
	t.Helper()
	at := func(kind string) int {
		for i, e := range ev {
			if e.kind == kind {
				return i
			}
		}
		return -1
	}
	iRestarted := at("restarted")
	require.GreaterOrEqual(t, iRestarted, 0, "the commit started the log over: %v", ev)
	for i := 0; i < iRestarted; i++ {
		require.NotEqual(t, "write", ev[i].kind, "event %d: a write before the restart: %v", i, ev)
	}
	rest := ev[iRestarted+1:]
	require.GreaterOrEqual(t, len(rest), 3, "%v", ev)
	require.Equal(t, walEvent{kind: "write", off: 0, n: walHeaderSize}, walEvent{kind: rest[0].kind, off: rest[0].off, n: rest[0].n},
		"the first write after the restart is the new log's header: %v", ev)
	require.Equal(t, "sync", rest[1].kind, "the header is synced before any frame: %v", ev)
	require.Equal(t, "header-synced", rest[2].kind, "%v", ev)
	frames, chunks := checkChunks(t, rest[3:], 0, 0, frameSize, shm)
	require.Equal(t, int(wantFrames), frames, "the chunks hold the commit's frames, from frame 1")
	require.GreaterOrEqual(t, chunks, 3, "setup: the commit spans several chunks")

	tail := rest[3+chunks:]
	var kinds []string
	for _, e := range tail {
		kinds = append(kinds, e.kind)
	}
	want := []string{"sync", "pre-publish"}
	if shm {
		want = append(want, "publish")
	}
	require.Equal(t, want, kinds, "after the last chunk: the sync, then the publication")
	require.Zero(t, tail[1].mx, "the commit is not published before its sync")
}

func restartAbsent(t *testing.T, db *DB, from, to int) {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	ns, err := rtx.GetNamespace("t1")
	require.NoError(t, err)
	for i := from; i < to; i++ {
		_, err := rtx.Get(ns, binary.BigEndian.AppendUint32(nil, uint32(i)))
		require.ErrorIs(t, err, ErrKeyNotFound, "row %d of the failed commit", i)
	}
}

func readerPageCount(t *testing.T, db *DB) uint32 {
	t.Helper()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { require.NoError(t, rtx.Rollback()) }()
	return db.pager.readerDbSizeBound(rtx.cache)
}

// A commit that starts the log over and spans several chunks, in the three
// modes: the restart comes first, the new header is written and synced before
// the first chunk, the chunks hold frames 1..n, and the commit is published
// after the sync. The files as a crash leaves them (database and WAL, no
// shm) recover to the last commit at every point: with the new header and no
// frame, with the first chunk only, and after the commit.
func TestWriteFrames_RestartThenChunkedCommit(t *testing.T) {
	for _, m := range chunkModes() {
		t.Run(m.name, func(t *testing.T) {
			db, path := openRestartDB(t, m)
			restartPut(t, db, 0, 200, 1)
			require.NoError(t, db.Checkpoint(CheckpointPassive))
			w := db.pager.wal
			old := w.index.mxCommitFrame.LoadLocal()
			require.Equal(t, old, w.index.liveNBackfill(), "setup: the whole log is backfilled")
			gen := w.logGen.Load()

			var ow *orderedWAL
			var atHeader, atChunk1 string
			if !m.opts.InMemory {
				ow = wrapWAL(t, w)
				ow.onChunk = func(n int) {
					if n == 1 {
						atChunk1 = restartCopyFiles(t, path, "at-chunk-1")
					}
				}
				testRestartLogHook = func(step string) {
					ow.step(step)
					if step == "header-synced" {
						atHeader = restartCopyFiles(t, path, "at-header")
					}
				}
				ow.arm("")
			}
			restartPut(t, db, 0, chunkRows, 2)

			require.Equal(t, gen+1, w.logGen.Load(), "the commit started the log over")
			frames := w.index.mxCommitFrame.LoadLocal()
			require.Equal(t, frames, w.nFrame.Load())
			perChunk := walFrameBatchMax / (int(walFrameSize) + int(w.pageSize))
			require.Greater(t, int(frames), 2*perChunk, "setup: the commit spans three chunks or more")
			if m.opts.InMemory {
				require.Len(t, w.memFrames, int(frames), "the commit's frames start the in-memory log")
			} else {
				ev, _ := ow.disarm()
				checkRestartOrder(t, ev, ow.frameSize, ow.shm, frames)
			}
			restartCheck(t, db, 0, chunkRows, 2)
			require.NoError(t, db.IntegrityCheck())

			restartRecoverCopy(t, path, "after-commit", m, func(cdb *DB) {
				require.Equal(t, frames, cdb.pager.wal.index.mxCommitFrame.LoadLocal(), "recovery found the commit's frames from frame 1")
				restartCheck(t, cdb, 0, chunkRows, 2)
			})
			if !m.opts.InMemory {
				for _, p := range []string{atHeader, atChunk1} {
					require.NotEmpty(t, p, "setup: a crash copy at each point")
					cdb, err := testOpen(t, p, m.opts)
					require.NoError(t, err, p)
					restartCheck(t, cdb, 0, 200, 1)
					restartAbsent(t, cdb, 200, chunkRows)
					require.NoError(t, cdb.IntegrityCheck(), p)
					require.NoError(t, cdb.Close())
				}
			}
			db = restartReopen(t, db, path, m)
			restartCheck(t, db, 0, chunkRows, 2)
			require.NoError(t, db.IntegrityCheck())
		})
	}
}

// A chunk that fails after the log was started over in the same transaction
// (by the commit itself, or by a spill before it): the transaction unwinds to
// walBase(), frame 0 of the new log with its seed checksums and the size the
// transaction began with, not to the frame number it began at. The last
// commit stays readable and recoverable, and the retry writes a log that a
// crash copy recovers.
func TestWriteFrames_ChunkFaultAfterRestart(t *testing.T) {
	cases := []struct {
		name  string
		spill bool
		fail  string
	}{
		{"restart-at-commit/second-chunk", false, "second"},
		{"restart-at-commit/commit-chunk", false, "commit"},
		{"restart-at-spill/second-chunk", true, "second"},
		{"restart-at-spill/commit-chunk", true, "commit"},
	}
	for _, m := range chunkModes()[:2] { // the modes with a WAL file
		for _, c := range cases {
			t.Run(m.name+"/"+c.name, func(t *testing.T) {
				db, path := openRestartDB(t, m)
				restartPut(t, db, 0, 200, 1)
				require.NoError(t, db.Checkpoint(CheckpointPassive))
				w := db.pager.wal
				old := w.index.mxCommitFrame.LoadLocal()
				require.Equal(t, old, w.index.liveNBackfill(), "setup: the whole log is backfilled")
				gen := w.logGen.Load()
				_, size := w.index.mxCommitFrame.LoadPos()
				ow := wrapWAL(t, w)

				wtx, err := db.BeginWrite()
				require.NoError(t, err)
				if c.spill {
					restartPutTx(t, wtx, 0, 50, 2)
					restartSpillAll(t, db)
					require.Equal(t, gen+1, w.logGen.Load(), "setup: the spill started the log over")
					require.NotZero(t, w.nFrame.Load(), "setup: spilled frames in the new log")
				}
				restartPutTx(t, wtx, 0, chunkRows, 2)
				var start uint32 // the commit's first frame follows the restart, or the spilled frames
				if c.spill {
					start = w.nFrame.Load()
				}
				ow.arm(c.fail)
				require.ErrorIs(t, wtx.Commit(), errInjectedChunk)
				ev, failed := ow.disarm()
				require.True(t, failed, "setup: a chunk failed")
				require.Equal(t, gen+1, w.logGen.Load(), "setup: the log was started over")
				_, chunks := checkChunks(t, ev, start, 0, ow.frameSize, ow.shm)
				if c.fail == "commit" {
					require.GreaterOrEqual(t, chunks, 3, "setup: the commit chunk is not the first")
				}

				// walBase(): the start of the new log, its seed, and the size
				// the transaction began with.
				require.Zero(t, w.nFrame.Load(), "the unwind went to the start of the new log (old end %d)", old)
				require.Equal(t, w.genSeed1.Load(), w.cksum1)
				require.Equal(t, w.genSeed2.Load(), w.cksum2)
				f, n := w.index.mxCommitFrame.LoadPos()
				require.Zero(t, f)
				require.Equal(t, size, n)
				require.Zero(t, w.index.maxFrame.Load())
				if ow.shm {
					hdr, valid := w.index.readHeader()
					require.True(t, valid)
					require.Zero(t, hdr.mxFrame, "the failed commit was not published")
					require.Equal(t, size, hdr.nPage)
				}
				require.Equal(t, size, readerPageCount(t, db))
				restartCheck(t, db, 0, 200, 1)
				restartAbsent(t, db, 200, chunkRows)
				restartRecoverCopy(t, path, "after-fault", m, func(cdb *DB) {
					restartCheck(t, cdb, 0, 200, 1)
					restartAbsent(t, cdb, 200, chunkRows)
				})

				// The retry appends from frame 1 of the new log, in chunks.
				ow.arm("")
				restartPut(t, db, 0, chunkRows, 3)
				ev, _ = ow.disarm()
				frames, _ := checkChunks(t, ev, 0, 0, ow.frameSize, ow.shm)
				require.Equal(t, int(w.nFrame.Load()), frames)
				require.Equal(t, w.nFrame.Load(), w.index.mxCommitFrame.LoadLocal())
				restartCheck(t, db, 0, chunkRows, 3)
				restartRecoverCopy(t, path, "after-retry", m, func(cdb *DB) {
					restartCheck(t, cdb, 0, chunkRows, 3)
				})
				db = restartReopen(t, db, path, m)
				restartCheck(t, db, 0, chunkRows, 3)
				require.NoError(t, db.IntegrityCheck())
			})
		}
	}
}

// A schema format 6 commit that changes no header field writes no page 1
// (commitNeedsPage1), so its frames are leaves only and its size is the
// commit mark on its last frame. Spanning several chunks, appended to the log
// or after a restart: a failed commit chunk leaves the last commit and its
// size, the retry is all leaves with the size on the commit frame alone, and
// a crash copy recovers it with that size.
func TestWriteFrames_Format6NoPage1Chunks(t *testing.T) {
	useCurrentFormat(t)
	for _, m := range chunkModes()[:2] { // the modes with a WAL file
		for _, restart := range []bool{false, true} {
			name := m.name + "/append"
			if restart {
				name = m.name + "/after-restart"
			}
			t.Run(name, func(t *testing.T) {
				db, path := openRestartDB(t, m)
				require.GreaterOrEqual(t, db.SchemaFormat(), uint32(schemaFormatTableLeaf), "setup: schema format 6")
				restartPut(t, db, 0, chunkRows, 1)
				w := db.pager.wal
				if restart {
					require.NoError(t, db.Checkpoint(CheckpointPassive))
					require.Equal(t, w.index.mxCommitFrame.LoadLocal(), w.index.liveNBackfill(), "setup: the whole log is backfilled")
				}
				gen := w.logGen.Load()
				base, size := w.index.mxCommitFrame.LoadPos()
				if restart {
					base = 0
				}
				ow := wrapWAL(t, w)

				// The same rows, the same sizes: every leaf, no header field.
				ow.arm("commit")
				wtx, err := db.BeginWrite()
				require.NoError(t, err)
				restartPutTx(t, wtx, 0, chunkRows, 2)
				require.ErrorIs(t, wtx.Commit(), errInjectedChunk)
				ev, failed := ow.disarm()
				require.True(t, failed, "setup: the commit chunk failed")
				if restart {
					require.Equal(t, gen+1, w.logGen.Load(), "setup: the commit started the log over")
					iHdr := -1
					for i, e := range ev {
						if e.kind == "write" && e.off == 0 && e.n == walHeaderSize {
							iHdr = i
							break
						}
					}
					require.GreaterOrEqual(t, iHdr, 0, "the new log's header was written: %v", ev)
					require.Equal(t, "sync", ev[iHdr+1].kind, "and synced: %v", ev)
					for _, e := range ev[:iHdr] {
						require.NotEqual(t, "write", e.kind, "a frame before the new log's header: %v", ev)
					}
				}
				_, chunks := checkChunks(t, ev, base, base, ow.frameSize, ow.shm)
				require.GreaterOrEqual(t, chunks, 3, "setup: the commit spans several chunks")

				f, n := w.index.mxCommitFrame.LoadPos()
				require.Equal(t, base, f)
				require.Equal(t, size, n)
				require.Equal(t, base, w.nFrame.Load())
				if ow.shm {
					hdr, valid := w.index.readHeader()
					require.True(t, valid)
					require.Equal(t, base, hdr.mxFrame, "the failed commit was not published")
					require.Equal(t, size, hdr.nPage)
				}
				require.Equal(t, size, readerPageCount(t, db))
				restartCheck(t, db, 0, chunkRows, 1)
				restartRecoverCopy(t, path, "after-fault", m, func(cdb *DB) {
					restartCheck(t, cdb, 0, chunkRows, 1)
					require.Equal(t, size, readerPageCount(t, cdb))
				})

				ow.arm("")
				restartPut(t, db, 0, chunkRows, 3)
				ev, _ = ow.disarm()
				frames, chunks := checkChunks(t, ev, base, base, ow.frameSize, ow.shm)
				require.GreaterOrEqual(t, chunks, 3, "setup: the retry spans several chunks")
				mx := w.index.mxCommitFrame.LoadLocal()
				require.Equal(t, base+uint32(frames), mx)
				got := p1Frames(t, path, int(w.pageSize), mx)[base:]
				for i, fr := range got {
					require.NotEqual(t, uint32(1), fr.pgno, "frame %d: a commit that changes no header field writes no page 1", base+uint32(i)+1)
					if i < len(got)-1 {
						require.Zero(t, fr.nTrunc, "frame %d", base+uint32(i)+1)
					}
				}
				require.Equal(t, size, got[len(got)-1].nTrunc, "the commit frame carries the size")
				_, n = w.index.mxCommitFrame.LoadPos()
				require.Equal(t, size, n)
				restartCheck(t, db, 0, chunkRows, 3)
				restartRecoverCopy(t, path, "after-retry", m, func(cdb *DB) {
					f, n := cdb.pager.wal.index.mxCommitFrame.LoadPos()
					require.Equal(t, mx, f)
					require.Equal(t, size, n)
					require.Equal(t, size, readerPageCount(t, cdb))
					restartCheck(t, cdb, 0, chunkRows, 3)
				})
				db = restartReopen(t, db, path, m)
				restartCheck(t, db, 0, chunkRows, 3)
				require.NoError(t, db.IntegrityCheck())
			})
		}
	}
}
