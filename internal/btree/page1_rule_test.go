package btree

// Which commits write page 1, and what its change counter holds.
//
// A schema format 6 commit writes page 1 when its header changed, when page 1
// is dirty, or as the frame carrying the commit mark when no other page is
// dirty (pager.commitNeedsPage1; SQLite pager.c:6500-6512). The change counter
// and VersionValidFor then get dbFileVers+1, dbFileVers being page 1's
// counter as last read or restored (pager_write_changecounter,
// pager.c:3084-3097). A schema format 5 file keeps page 1 in every commit and
// moves the counter by MarkDataChanged.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// p1Frame is one frame of the WAL: its page, the commit mark (the database
// size, 0 for a frame that is not the last of a commit), and, for page 1, the
// change counter and VersionValidFor it carries.
type p1Frame struct {
	pgno, nTrunc uint32
	fcc, vvf     uint32
}

// p1Frames lists the first n frames of the WAL at path. Page 1's first 100
// bytes are plaintext under every codec.
func p1Frames(t *testing.T, path string, pageSize int, n uint32) []p1Frame {
	t.Helper()
	data, err := os.ReadFile(path + "-wal")
	require.NoError(t, err)
	var out []p1Frame
	for i, off := uint32(0), walHeaderSize; i < n; i, off = i+1, off+walFrameSize+pageSize {
		require.LessOrEqual(t, off+walFrameSize+pageSize, len(data), "frame %d", i+1)
		h := data[off : off+walFrameSize]
		f := p1Frame{pgno: binary.BigEndian.Uint32(h[0:4]), nTrunc: binary.BigEndian.Uint32(h[4:8])}
		if f.pgno == 1 {
			page := data[off+walFrameSize:]
			f.fcc = binary.BigEndian.Uint32(page[24:28])
			f.vvf = binary.BigEndian.Uint32(page[92:96])
		}
		out = append(out, f)
	}
	return out
}

// p1Commit is what one commit wrote: its frames from the WAL, whether page 1
// was among them, and the counter page 1 carried.
type p1Commit struct {
	frames  int
	page1   bool
	fcc     uint32
	vvf     uint32
	nTrunc  uint32
	spilled int
}

// p1Recorder reads back, from the WAL file, what the last commit of db
// wrote: the frames after the previous commit mark, up to db's last frame.
// A commit is the frames since the previous mark, its spilled frames included.
type p1Recorder struct {
	t    *testing.T
	db   *DB
	path string
	last uint32 // db's commit frame at the previous call
}

func newP1Recorder(t *testing.T, db *DB, path string) *p1Recorder {
	return &p1Recorder{t: t, db: db, path: path, last: db.pager.wal.index.mxCommitFrame.LoadLocal()}
}

// next returns what the commit just made wrote.
func (r *p1Recorder) next() p1Commit {
	r.t.Helper()
	n := r.db.pager.wal.index.mxCommitFrame.LoadLocal()
	require.NotEqual(r.t, r.last, n, "a commit was made")
	r.last = n
	frames := p1Frames(r.t, r.path, int(r.db.pager.pageSize), n)
	require.NotZero(r.t, frames[len(frames)-1].nTrunc, "the last frame carries the commit mark")
	start := len(frames) - 1
	for start > 0 && frames[start-1].nTrunc == 0 {
		start--
	}
	var c p1Commit
	for _, f := range frames[start:] {
		c.frames++
		if f.pgno == 1 {
			require.False(r.t, c.page1, "page 1 written twice in one commit")
			c.page1, c.fcc, c.vvf = true, f.fcc, f.vvf
		}
	}
	c.nTrunc = frames[len(frames)-1].nTrunc
	return c
}

// none checks that nothing was committed since the last call.
func (r *p1Recorder) none() {
	r.t.Helper()
	require.Equal(r.t, r.last, r.db.pager.wal.index.mxCommitFrame.LoadLocal(), "nothing committed")
}

func p1Open(t *testing.T, opts Options) (*DB, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "p1.db")
	if opts.PageSize == 0 {
		opts.PageSize = 4096
	}
	opts.DisableAutoCheckpoint = true
	db, err := testOpen(t, path, opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, path
}

func p1Write(t *testing.T, db *DB, f func(tx *WriteTx, ns *Namespace)) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	if err != nil {
		ns = nil
	}
	f(tx, ns)
	require.NoError(t, tx.Commit())
}

// p1Big is a value that takes a leaf page of its own: every put of a new key
// allocates a page, which grows the database (the freelist is empty).
func p1Big(fill byte) []byte { return bytes.Repeat([]byte{fill}, 3000) }

func p1Key(i int) []byte { return binary.BigEndian.AppendUint32(nil, uint32(i)) }

// TestPage1Rule_FrameShape: a schema format 6 commit carries page 1 only when
// it has to.
func TestPage1Rule_FrameShape(t *testing.T) {
	skipInLegacyFormatSuite(t)
	db, path := p1Open(t, Options{})
	r := newP1Recorder(t, db, path)

	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		_, err := tx.CreateNamespace("t")
		require.NoError(t, err)
	})
	c := r.next()
	require.True(t, c.page1, "a catalog change writes page 1")

	p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Put(ns, p1Key(0), []byte("small"))) })
	c = r.next()
	require.False(t, c.page1, "a put that fits the root page leaves the header alone")
	require.Equal(t, 1, c.frames)

	p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Put(ns, p1Key(0), []byte("SMALL"))) })
	c = r.next()
	require.False(t, c.page1, "an in-place update")
	require.Equal(t, 1, c.frames)

	p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
		for i := 1; i <= 3; i++ {
			require.NoError(t, tx.Put(ns, p1Key(i), p1Big(1)))
		}
	})
	c = r.next()
	require.True(t, c.page1, "growth writes page 1")
	require.Equal(t, db.pager.dbSize.Load(), c.nTrunc)

	// An overflowing value rewritten at the same size frees its chain and
	// allocates it again: the freelist ends as it began.
	over := bytes.Repeat([]byte{7}, 3*4096)
	p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Put(ns, p1Key(10), over)) })
	require.True(t, r.next().page1, "setup: the chain grows the database")
	size := db.pager.dbSize.Load()
	p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
		require.NoError(t, tx.Put(ns, p1Key(10), bytes.Repeat([]byte{8}, len(over))))
	})
	c = r.next()
	require.Equal(t, size, db.pager.dbSize.Load())
	require.False(t, c.page1, "a net-zero free and allocate leaves the header alone")

	p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Delete(ns, p1Key(2))) })
	require.True(t, r.next().page1, "a freelist change writes page 1")

	// The schema cookie alone: an existing page updated in place, with the
	// cookie raised (a metadata-only schema change).
	cookie := db.pager.header.SchemaCookie
	p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
		require.NoError(t, tx.Put(ns, p1Key(0), []byte("small")))
		tx.MarkSchemaChanged()
	})
	c = r.next()
	require.True(t, c.page1, "a schema-cookie change writes page 1")
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	sc := rtx.SnapshotSchemaCookie()
	require.NoError(t, rtx.Rollback())
	require.Equal(t, cookie+1, sc, "a reader sees the committed cookie")
	require.Equal(t, []string{fmt.Sprintf("GET cookie %d", cookie+1)}, runP1Child(t, path, Options{}, "cookie"),
		"another process sees it")

	// Every dirty page spilled before the commit: page 1 carries the mark.
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, p1Key(1), p1Big(2)))
	restartSpillAll(t, db)
	require.Zero(t, db.pager.writerCache.nDirty)
	require.NoError(t, tx.Commit())
	c = r.next()
	require.True(t, c.page1, "the carrier")
	require.Equal(t, 2, c.frames, "the spilled page and the carrier")

	// An empty transaction writes nothing.
	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {})
	r.none()

	// A catalog change undone by a savepoint leaves page 1 re-dirtied, with
	// the freelist trunk the new root came from (restored pages are dirty,
	// as in SQLite's savepoint playback).
	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		sp, err := tx.Savepoint()
		require.NoError(t, err)
		_, err = tx.CreateNamespace("gone")
		require.NoError(t, err)
		require.NoError(t, tx.RollbackToSavepoint(sp))
	})
	require.True(t, r.next().page1)

	require.NoError(t, db.Close())
	db, err = testOpen(t, path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, cookie+1, db.pager.header.SchemaCookie, "the cookie survives a reopen")
	require.NoError(t, db.IntegrityCheck())
	rtx, err = db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err = rtx.GetNamespace("t")
	require.NoError(t, err)
	v, err := rtx.Get(ns, p1Key(10))
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{8}, len(over)), v)
	_, err = rtx.GetNamespace("gone")
	require.ErrorIs(t, err, ErrNamespaceNotFound)
}

// TestPage1Rule_Format5EveryCommit: a schema format 5 file keeps page 1 in
// every commit that writes anything, with the change counter moved by
// MarkDataChanged, and stays schema format 5.
func TestPage1Rule_Format5EveryCommit(t *testing.T) {
	useLegacyFormat(t)
	db, path := p1Open(t, Options{})
	r := newP1Recorder(t, db, path)
	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		_, err := tx.CreateNamespace("t")
		require.NoError(t, err)
	})
	first := r.next()
	require.True(t, first.page1)
	fcc := first.fcc
	for i, marked := range []bool{true, false, true, true, false} {
		p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
			require.NoError(t, tx.Put(ns, p1Key(0), []byte(fmt.Sprintf("v%d", i))))
			if marked {
				tx.MarkDataChanged()
			}
		})
		c := r.next()
		require.True(t, c.page1, "commit %d: page 1 in every commit", i)
		if marked {
			fcc++
		}
		require.Equal(t, fcc, c.fcc, "commit %d", i)
		require.Equal(t, fcc, c.vvf, "commit %d", i)
	}
	p1Write(t, db, func(tx *WriteTx, _ *Namespace) { tx.MarkDataChanged() })
	r.none()

	require.NoError(t, db.Close())
	SetCreateLegacyFormatForTest(false)
	db, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db.Close()
	require.Equal(t, uint32(5), db.SchemaFormat())
	require.False(t, db.pager.page1OnChange())
}

// p1CounterOf returns the change counter a commit wrote into page 1, failing
// if it did not write page 1.
func p1CounterOf(t *testing.T, r *p1Recorder, what string) uint32 {
	t.Helper()
	c := r.next()
	require.True(t, c.page1, "%s writes page 1", what)
	require.Equal(t, c.fcc, c.vvf, "%s: VersionValidFor follows the counter", what)
	return c.fcc
}

// TestPage1Rule_FCCLikeSQLite replays the events of the pinned SQLite
// oracle and expects its counter values. The oracle is the CLI of SQLite
// 3.52.0, source id 2026-03-06 16:01:44 557aeb43869d3585137b17690cb3b64f7de6
// 921774daae9e56403c3717dceab6, built from the version-3.52.0 tag, with the
// same sequence of transactions: create table (page 1: 2); a first insert
// that fits (no page 1); two growing inserts (2, 2); an in-place update (no
// page 1); a growing insert, create table, a growing insert (2, 2, 2); a
// second connection's two growing inserts (3, 3); two more of the first
// connection's (4, 4); a savepoint around a create table that is rolled back
// (one frame, page 1: 5); two growing inserts (5, 5).
func TestPage1Rule_FCCLikeSQLite(t *testing.T) {
	skipInLegacyFormatSuite(t)
	db, path := p1Open(t, Options{})
	r := newP1Recorder(t, db, path)
	var got []uint32
	put := func(k int, v []byte) {
		p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Put(ns, p1Key(k), v)) })
	}

	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		_, err := tx.CreateNamespace("t")
		require.NoError(t, err)
	})
	got = append(got, p1CounterOf(t, r, "create"))
	put(0, p1Big(1))
	require.False(t, r.next().page1)
	put(1, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 1"))
	put(2, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 2"))
	put(0, p1Big(9))
	require.False(t, r.next().page1)
	put(3, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 3"))
	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		_, err := tx.CreateNamespace("u")
		require.NoError(t, err)
	})
	got = append(got, p1CounterOf(t, r, "create u"))
	put(4, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 4"))

	// The other connection: another process.
	out := runP1Peer(t, path, "5,6")
	for _, f := range strings.Fields(out) {
		v, err := strconv.ParseUint(f, 10, 32)
		require.NoError(t, err)
		got = append(got, uint32(v))
	}
	put(7, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 7"))
	put(8, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 8"))

	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		sp, err := tx.Savepoint()
		require.NoError(t, err)
		_, err = tx.CreateNamespace("w")
		require.NoError(t, err)
		require.NoError(t, tx.RollbackToSavepoint(sp))
	})
	c := r.next()
	require.Equal(t, 1, c.frames)
	require.True(t, c.page1)
	got = append(got, c.fcc)
	put(9, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 9"))
	put(10, p1Big(1))
	got = append(got, p1CounterOf(t, r, "insert 10"))

	require.Equal(t, []uint32{2, 2, 2, 2, 2, 2, 3, 3, 4, 4, 5, 5, 5}, got)
}

const p1PeerEnv = "BTREE_TEST_P1_PEER"

// runP1Peer runs TestPage1Rule_Peer in a child process on path: one growing
// put per key in keys (comma-separated), each its own commit. It returns
// the change counters the child's commits wrote into page 1.
func runP1Peer(t *testing.T, path, keys string) string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPage1Rule_Peer$", "-test.timeout=60s")
	cmd.Env = append(os.Environ(), p1PeerEnv+"="+path+"|"+keys)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "peer: %s", out)
	for _, line := range strings.Split(string(out), "\n") {
		if rest, ok := strings.CutPrefix(line, "P1PEER "); ok {
			return rest
		}
	}
	t.Fatalf("peer printed no counters: %s", out)
	return ""
}

// TestPage1Rule_Peer is the child of runP1Peer.
func TestPage1Rule_Peer(t *testing.T) {
	arg := os.Getenv(p1PeerEnv)
	if arg == "" {
		t.Skip("child process only")
	}
	path, keys, _ := strings.Cut(arg, "|")
	db, err := Open(path, Options{PageSize: 4096, DisableAutoCheckpoint: true})
	require.NoError(t, err)
	defer db.Close()
	r := newP1Recorder(t, db, path)
	var vals []string
	for _, k := range strings.Split(keys, ",") {
		i, err := strconv.Atoi(k)
		require.NoError(t, err)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.GetNamespace("t")
		require.NoError(t, err)
		require.NoError(t, tx.Put(ns, p1Key(i), p1Big(3)))
		require.NoError(t, tx.Commit())
		vals = append(vals, strconv.FormatUint(uint64(p1CounterOf(t, r, "peer insert")), 10))
	}
	fmt.Println("P1PEER " + strings.Join(vals, " "))
}

// TestPage1Rule_FCCCycles: the counter follows page 1's reads and restores,
// repeated. SQLite's values for the same transactions are in the comments:
// its cache keeps a clean page 1 through a rollback (pagerRollbackWal
// touches only logged and dirty pages, pager.c:3147-3166), any-store's
// rollback clears the writer cache, so there the counter runs ahead.
func TestPage1Rule_FCCCycles(t *testing.T) {
	skipInLegacyFormatSuite(t)
	setup := func(t *testing.T) (*DB, *p1Recorder, uint32) {
		db, path := p1Open(t, Options{})
		r := newP1Recorder(t, db, path)
		p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
			_, err := tx.CreateNamespace("t")
			require.NoError(t, err)
		})
		c := p1CounterOf(t, r, "create")
		// The first big value fits the root; every one after it takes a page.
		p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Put(ns, p1Key(-1), p1Big(1))) })
		require.False(t, r.next().page1)
		return db, r, c
	}
	grow := func(t *testing.T, db *DB, k int) {
		p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Put(ns, p1Key(k), p1Big(1))) })
	}

	t.Run("warm", func(t *testing.T) {
		// SQLite: the same value every time.
		db, r, c := setup(t)
		for k := 0; k < 5; k++ {
			grow(t, db, k)
			require.Equal(t, c, p1CounterOf(t, r, "grow"), "cycle %d", k)
		}
	})
	t.Run("rollback", func(t *testing.T) {
		// SQLite: the same value every time; here one more per rolled-back
		// transaction followed by a page-1 commit.
		db, r, c := setup(t)
		for k := 0; k < 5; k++ {
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.GetNamespace("t")
			require.NoError(t, err)
			require.NoError(t, tx.Put(ns, p1Key(100), []byte("x")))
			require.NoError(t, tx.Rollback())
			grow(t, db, k)
			require.Equal(t, c+uint32(k)+1, p1CounterOf(t, r, "grow"), "cycle %d", k)
		}
	})
	t.Run("savepoint-growth", func(t *testing.T) {
		// SQLite: one more per cycle, as here: the growth journals page 1,
		// and the rollback to the savepoint restores dbFileVers from it
		// (pager.c:2481-2484). Here only the page count moved, and the
		// header the savepoint holds gives the value.
		db, r, c := setup(t)
		for k := 0; k < 5; k++ {
			p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
				sp, err := tx.Savepoint()
				require.NoError(t, err)
				require.NoError(t, tx.Put(ns, p1Key(1000+k), p1Big(2)))
				require.NoError(t, tx.RollbackToSavepoint(sp))
				require.NoError(t, tx.Put(ns, p1Key(k), p1Big(1)))
			})
			require.Equal(t, c+uint32(k)+1, p1CounterOf(t, r, "grow after a savepoint"), "cycle %d", k)
		}
	})
	t.Run("evicted", func(t *testing.T) {
		// Page 1 dropped from the writer cache between transactions, as
		// cache pressure does: the next begin reads it again.
		db, r, c := setup(t)
		for k := 0; k < 5; k++ {
			db.pager.writerCache.discard(1)
			grow(t, db, k)
			require.Equal(t, c+uint32(k)+1, p1CounterOf(t, r, "grow"), "cycle %d", k)
		}
	})
	t.Run("evicted-in-transaction", func(t *testing.T) {
		// Page 1 dropped during the transaction: the commit fetches it, and
		// the value comes from that read (fetch, then compute).
		db, r, c := setup(t)
		for k := 0; k < 5; k++ {
			p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
				require.NoError(t, tx.Put(ns, p1Key(k), p1Big(1)))
				db.pager.writerCache.discard(1)
			})
			require.Equal(t, c+uint32(k)+1, p1CounterOf(t, r, "grow"), "cycle %d", k)
		}
	})
	t.Run("savepoint-net-zero", func(t *testing.T) {
		// SQLite: one more, c+1. The page taken from the freelist and given
		// back journals page 1 at the savepoint, and the rollback restores
		// dbFileVers from that image (pager.c:2481-2484). Here the header is
		// back where the savepoint had it, so nothing tells the rollback
		// page 1 was touched (drift-2026-11-12-5).
		db, r, c := setup(t)
		grow(t, db, 0)
		require.Equal(t, c, p1CounterOf(t, r, "grow"))
		p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Delete(ns, p1Key(0))) })
		require.Equal(t, c, p1CounterOf(t, r, "a freed page"))
		require.NotZero(t, db.pager.header.TotalFreelistPgs, "setup: a free page")
		p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
			sp, err := tx.Savepoint()
			require.NoError(t, err)
			before := db.pager.header
			pg, err := db.pager.allocatePage() // takes a page from the freelist
			require.NoError(t, err)
			pgno := pg.pgno
			db.pager.releasePage(pg)
			require.NoError(t, db.pager.freePage(pgno)) // and gives it back
			require.Equal(t, before, db.pager.header, "setup: the header is back where it was")
			require.NoError(t, tx.RollbackToSavepoint(sp))
			_, err = tx.CreateNamespace("catalog")
			require.NoError(t, err)
		})
		require.Equal(t, c, p1CounterOf(t, r, "a catalog change after the savepoint"))
	})
	t.Run("carrier", func(t *testing.T) {
		db, r, c := setup(t)
		grow(t, db, 0)
		require.Equal(t, c, p1CounterOf(t, r, "grow"))
		for k := 0; k < 3; k++ {
			p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
				require.NoError(t, tx.Put(ns, p1Key(0), p1Big(byte(k))))
				restartSpillAll(t, db)
			})
			require.Equal(t, c, p1CounterOf(t, r, "carrier"), "cycle %d", k)
		}
	})
}

// TestPage1Rule_FCCWraps: the counter is a u32, and dbFileVers+1 wraps to 0
// as SQLite's sqlite3Get4byte()+1 does (pager.c:3089). A file whose counter
// and VersionValidFor are both 0 opens and is written to: the two match, and
// any-store takes a zero VersionValidFor as "size from the pager's count".
// Schema format 5 wraps MarkDataChanged's increment the same way.
func TestPage1Rule_FCCWraps(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprintf("format5=%v", legacy), func(t *testing.T) {
			if legacy {
				useLegacyFormat(t)
			} else {
				skipInLegacyFormatSuite(t)
			}
			db, path := p1Open(t, Options{})
			r := newP1Recorder(t, db, path)
			p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
				_, err := tx.CreateNamespace("t")
				require.NoError(t, err)
			})
			r.next()
			p1Write(t, db, func(tx *WriteTx, ns *Namespace) { require.NoError(t, tx.Put(ns, p1Key(-1), p1Big(1))) })
			r.next()
			for k, want := range []uint32{0xffffffff, 0} {
				if k == 0 && !legacy {
					db.pager.dbFileVers.Store(0xfffffffe)
				}
				p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
					require.NoError(t, tx.Put(ns, p1Key(k), p1Big(1)))
					tx.MarkDataChanged()
					if legacy && k == 0 {
						tx.pager.header.FileChangeCount = 0xfffffffe
					}
				})
				require.Equal(t, want, p1CounterOf(t, r, "grow"), "commit %d", k)
				if !legacy {
					// The next transaction reads page 1 again, as after an
					// eviction.
					db.pager.writerCache.discard(1)
				}
			}
			// Counter and VersionValidFor both 0: the size is the pager's.
			require.NoError(t, db.Close())
			db2, err := testOpen(t, path, Options{PageSize: 4096})
			require.NoError(t, err)
			defer db2.Close()
			require.Zero(t, db2.pager.header.VersionValidFor)
			require.NoError(t, db2.IntegrityCheck())
			restartPutNS(t, db2, "t")
		})
	}
}

// restartPutNS commits one more big value into namespace name of db.
func restartPutNS(t *testing.T, db *DB, name string) {
	t.Helper()
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.GetNamespace(name)
	require.NoError(t, err)
	require.NoError(t, tx.Put(ns, p1Key(1<<20), p1Big(5)))
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())
}

const p1ChildEnv = "BTREE_TEST_P1_CHILD"

// runP1Child runs TestPage1Rule_Child on path in another process, with ops
// separated by ";":
//
//	get:NS:K   print "GET NS K <first byte> <length>" of key K in NS
//	put:NS:K   commit one big value (fill 6) at key K in NS
//	check      IntegrityCheck
//	cookie     print "GET cookie <schema cookie>" of a read transaction
//
// and returns its "GET" lines.
func runP1Child(t *testing.T, path string, opts Options, ops string) []string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestPage1Rule_Child$", "-test.timeout=60s")
	mode := "mp"
	if opts.CacheSize != 0 {
		mode = strconv.Itoa(opts.CacheSize)
	}
	cmd.Env = append(os.Environ(), p1ChildEnv+"="+path+"|"+mode+"|"+ops)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "child: %s", out)
	var gets []string
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "GET ") {
			gets = append(gets, line)
		}
	}
	return gets
}

// TestPage1Rule_Child is the child of runP1Child.
func TestPage1Rule_Child(t *testing.T) {
	arg := os.Getenv(p1ChildEnv)
	if arg == "" {
		t.Skip("child process only")
	}
	parts := strings.SplitN(arg, "|", 3)
	opts := Options{PageSize: 4096, DisableAutoCheckpoint: true}
	if parts[1] != "mp" {
		n, err := strconv.Atoi(parts[1])
		require.NoError(t, err)
		opts.CacheSize = n
	}
	db, err := Open(parts[0], opts)
	require.NoError(t, err)
	defer db.Close()
	for _, op := range strings.Split(parts[2], ";") {
		f := strings.Split(op, ":")
		switch f[0] {
		case "get":
			k, err := strconv.Atoi(f[2])
			require.NoError(t, err)
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			ns, err := rtx.GetNamespace(f[1])
			require.NoError(t, err)
			v, err := rtx.Get(ns, p1Key(k))
			require.NoError(t, err)
			require.NotEmpty(t, v)
			fmt.Printf("GET %s %d %d %d\n", f[1], k, v[0], len(v))
			require.NoError(t, rtx.Rollback())
		case "put":
			k, err := strconv.Atoi(f[2])
			require.NoError(t, err)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			ns, err := tx.GetNamespace(f[1])
			require.NoError(t, err)
			require.NoError(t, tx.Put(ns, p1Key(k), p1Big(6)))
			require.NoError(t, tx.Commit())
		case "check":
			require.NoError(t, db.IntegrityCheck())
		case "cookie":
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			fmt.Printf("GET cookie %d\n", rtx.SnapshotSchemaCookie())
			require.NoError(t, rtx.Rollback())
		default:
			t.Fatalf("unknown op %q", op)
		}
	}
}

// TestPage1Rule_Page1NeverSpilled: page 1 dirtied by a catalog change, then
// enough writes to spill a 10-page cache many times over. Page 1 is never a
// spill victim (pcache.findSpillVictim, pagerStress; SQLite keeps it
// referenced for the transaction, btree.c:3447), so the one page-1 frame of
// the transaction is in its commit, after every spilled frame.
func TestPage1Rule_Page1NeverSpilled(t *testing.T) {
	db, path := p1Open(t, Options{CacheSize: 10})
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	for i := 0; i < 3000; i++ {
		require.NoError(t, tx.Put(ns, p1Key(i), bytes.Repeat([]byte{byte(i)}, 300)))
	}
	spilled := db.pager.wal.nFrame.Load()
	require.Greater(t, spilled, uint32(100), "setup: the transaction spilled")
	for i, f := range p1Frames(t, path, 4096, spilled) {
		require.NotEqual(t, uint32(1), f.pgno, "frame %d: page 1 spilled", i+1)
	}
	require.NoError(t, tx.Commit())
	frames := p1Frames(t, path, 4096, db.pager.wal.nFrame.Load())
	var page1 []int
	for i, f := range frames {
		if f.pgno == 1 {
			page1 = append(page1, i+1)
		}
	}
	require.Len(t, page1, 1, "page 1 written once")
	require.Greater(t, uint32(page1[0]), spilled, "in the commit, after the spilled frames")
	require.NotZero(t, frames[len(frames)-1].nTrunc, "the commit's last frame carries the mark")
	require.LessOrEqual(t, page1[0], len(frames))
	require.NoError(t, db.IntegrityCheck())
}

// TestPage1Rule_SpilledOnlyCommitCarriesPage1: every dirty page spilled
// before the commit and the header unchanged: the commit still needs a frame
// with the commit mark, and page 1 carries it (pager.c:6500-6512). Another
// process reads the commit, and the file reopens with it.
func TestPage1Rule_SpilledOnlyCommitCarriesPage1(t *testing.T) {
	db, path := p1Open(t, Options{CacheSize: 100})
	p1Write(t, db, func(tx *WriteTx, _ *Namespace) {
		ns, err := tx.CreateNamespace("t")
		require.NoError(t, err)
		for i := 0; i < 50; i++ {
			require.NoError(t, tx.Put(ns, p1Key(i), []byte("old-value")))
		}
	})
	r := newP1Recorder(t, db, path)
	p1Write(t, db, func(tx *WriteTx, ns *Namespace) {
		for i := 0; i < 50; i++ {
			require.NoError(t, tx.Put(ns, p1Key(i), []byte("new-value"))) // same size
		}
		restartSpillAll(t, db)
		require.Zero(t, db.pager.writerCache.nDirty, "everything spilled")
	})
	c := r.next()
	require.True(t, c.page1, "page 1 carries the commit mark")
	frames := p1Frames(t, path, 4096, db.pager.wal.nFrame.Load())
	require.Equal(t, uint32(1), frames[len(frames)-1].pgno, "as the last frame")

	gets := runP1Child(t, path, Options{}, "get:t:7;check")
	require.Equal(t, []string{fmt.Sprintf("GET t 7 %d %d", 'n', len("new-value"))}, gets)

	require.NoError(t, db.Close())
	db, err := testOpen(t, path, Options{PageSize: 4096})
	require.NoError(t, err)
	defer db.Close()
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer rtx.Rollback()
	ns, err := rtx.GetNamespace("t")
	require.NoError(t, err)
	v, err := rtx.Get(ns, p1Key(7))
	require.NoError(t, err)
	require.Equal(t, "new-value", string(v))
	require.NoError(t, db.IntegrityCheck())
}

// TestPage1Rule_BackupDestinationHeader: a backup shrinks a larger,
// checkpointed destination; its commit writes page 1 with the source's
// header and the smaller size. The destination stays open without a
// checkpoint, and another process opens it, reads, writes and checks it.
func TestPage1Rule_BackupDestinationHeader(t *testing.T) {
	dst, dstPath := p1Open(t, Options{})
	p1Write(t, dst, func(tx *WriteTx, _ *Namespace) {
		ns, err := tx.CreateNamespace("t")
		require.NoError(t, err)
		for i := 0; i < 100; i++ {
			require.NoError(t, tx.Put(ns, p1Key(i), p1Big(1)))
		}
	})
	require.NoError(t, dst.Checkpoint(CheckpointTruncate))
	large := dst.pager.dbSize.Load()

	src, _ := p1Open(t, Options{})
	p1Write(t, src, func(tx *WriteTx, _ *Namespace) {
		ns, err := tx.CreateNamespace("t")
		require.NoError(t, err)
		for i := 0; i < 5; i++ {
			require.NoError(t, tx.Put(ns, p1Key(i), p1Big(2)))
		}
		tx.MarkSchemaChanged()
	})
	srcSize := src.pager.dbSize.Load()
	require.Less(t, srcSize, large)

	r := newP1Recorder(t, dst, dstPath)
	b, err := dst.BackupInit(src)
	require.NoError(t, err)
	require.ErrorIs(t, b.Step(context.Background(), -1), ErrBackupDone)
	require.NoError(t, b.Finish())
	c := r.next()
	require.True(t, c.page1, "the backup's commit writes page 1")
	require.Equal(t, srcSize, c.nTrunc)
	require.Equal(t, srcSize, dst.pager.header.DatabaseSize)
	require.Equal(t, src.pager.header.FirstFreelistPg, dst.pager.header.FirstFreelistPg)
	require.Equal(t, src.pager.header.TotalFreelistPgs, dst.pager.header.TotalFreelistPgs)

	gets := runP1Child(t, dstPath, Options{}, "get:t:3;put:t:200;check;get:t:200")
	require.Equal(t, []string{"GET t 3 2 3000", "GET t 200 6 3000"}, gets)
	restartPutNS(t, dst, "t")
	require.NoError(t, dst.IntegrityCheck())
}
