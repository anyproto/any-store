package btree

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// checkPage1Invariant turns on two checks of the pager's header against the
// committed page 1, in the manner of SQLite's assert()s: off in release
// builds, on for the btree test suite (its TestMain), in both schema formats.
// They hold the contract a schema format 6 commit relies on when it leaves
// page 1 out (commitNeedsPage1): the header a write transaction works from is
// the committed page 1's, and every commit leaves page 1 serializing the
// header it committed.
//
//   - At write begin, after the refresh: the committed page 1, read past the
//     writer cache, decodes to p.header field for field; p.dbSize is what
//     lockBtreePageCount makes of it against the pager's count; and
//     VersionValidFor is 0 (a new database, or a file that never kept it) or
//     equal to FileChangeCount.
//   - Right after a commit is published: the committed page 1 decodes to
//     p.header, its DatabaseSize is the commit's page count, and if the
//     commit wrote page 1 its change counter and VersionValidFor are equal
//     (0 included: the counter is a u32 and dbFileVers+1 wraps, as SQLite's
//     does, pager.c:3089).
//
// A page 1 that cannot be read or decoded past the cache is left to the code
// that reads it; tests that damage page 1 on purpose rely on that. A broken
// check at begin fails the begin with errPage1Invariant; one after a commit,
// which is published by then, panics.
var checkPage1Invariant atomic.Bool

// SetCheckPage1ForTest turns the page-1 checks (checkPage1Invariant) on or
// off for every handle of the process. Test suites outside the package turn
// it on in their TestMain.
func SetCheckPage1ForTest(on bool) {
	checkPage1Invariant.Store(on)
}

// committedPage1 reads page 1 at the writer's position without the writer
// cache, from the WAL frame the wal-index resolves or the database file, and
// decodes its header.
func (p *pager) committedPage1() (dbHeader, bool) {
	pg, err := p.readTempPage(1, p.wal.nFrame.Load(), p.wal.index.liveMinFrame(), max(p.dbSize.Load(), 1), nil, nil)
	if err != nil {
		return dbHeader{}, false
	}
	defer p.releasePage(pg)
	var h dbHeader
	if err := h.deserialize(pg.data[:dbHeaderSize]); err != nil {
		return dbHeader{}, false
	}
	return h, true
}

// errPage1Invariant is the error of a broken check at write begin.
var errPage1Invariant = errors.New("btree: page-1 invariant broken")

func (p *pager) checkPage1AtBegin() error {
	h, ok := p.committedPage1()
	if !ok {
		return nil
	}
	if h != p.header {
		return fmt.Errorf("%w at write begin: committed %+v, header %+v", errPage1Invariant, h, p.header)
	}
	if h.VersionValidFor != 0 && h.VersionValidFor != h.FileChangeCount {
		return fmt.Errorf("%w at write begin: VersionValidFor %d, FileChangeCount %d", errPage1Invariant, h.VersionValidFor, h.FileChangeCount)
	}
	count, err := p.committedPageCount()
	if err != nil {
		return nil
	}
	if n, err := lockBtreePageCount(&h, count); err == nil && n != p.dbSize.Load() {
		return fmt.Errorf("%w at write begin: page count %d, the pager's %d", errPage1Invariant, n, p.dbSize.Load())
	}
	return nil
}

func (p *pager) assertPage1AfterCommit(wrotePage1 bool) {
	h, ok := p.committedPage1()
	if !ok {
		return
	}
	if h != p.header {
		panic(fmt.Sprintf("btree: page 1 after commit (written: %v): committed %+v, header %+v", wrotePage1, h, p.header))
	}
	if h.DatabaseSize != p.dbSize.Load() {
		panic(fmt.Sprintf("btree: page 1 after commit: DatabaseSize %d, the commit's page count %d", h.DatabaseSize, p.dbSize.Load()))
	}
	if wrotePage1 && h.VersionValidFor != h.FileChangeCount {
		panic(fmt.Sprintf("btree: page 1 after commit: VersionValidFor %d, FileChangeCount %d", h.VersionValidFor, h.FileChangeCount))
	}
}
