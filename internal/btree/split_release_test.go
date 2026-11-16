package btree

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errRebuildInjected = errors.New("injected rebuild failure")

// failRebuildOf makes every rebuild of page pgno of p's writer cache fail
// until the test ends. The hook is process-wide, so matching the cache keeps
// other pagers' pages out of it.
func failRebuildOf(t *testing.T, p *pager, pgno uint32) {
	t.Helper()
	setRebuildPageFaultHook(func(pg *page) error {
		if pg.cache == p.writerCache && pg.pgno == pgno {
			return errRebuildInjected
		}
		return nil
	})
	t.Cleanup(func() { setRebuildPageFaultHook(nil) })
}

// nextAllocPgno returns the page number the next allocatePage hands out while
// the freelist is empty.
func nextAllocPgno(t *testing.T, p *pager) uint32 {
	t.Helper()
	require.Zero(t, p.header.FirstFreelistPg, "the test assumes an empty freelist")
	return p.dbSize.Load() + 1
}

// requireUnpinned asserts that pgno sits in the writer cache with no pins.
func requireUnpinned(t *testing.T, p *pager, pgno uint32) {
	t.Helper()
	pg := p.writerCache.hashFind(pgno)
	require.NotNil(t, pg, "page %d is not in the writer cache", pgno)
	assert.Equal(t, 0, pg.pinCount, "page %d pin count", pgno)
}

// leafCellsFor returns n sorted leaf cells with values of valSize bytes.
func leafCellsFor(n, valSize int) []cellData {
	cells := make([]cellData, n)
	for i := range n {
		cells[i] = cellData{key: fmt.Appendf(nil, "key-%06d", i), value: bytes.Repeat([]byte{'v'}, valSize)}
	}
	return cells
}

// splitFault names which rebuild of a split fails: none, the page being split
// (left), or the page the split allocated (right).
type splitFault int

const (
	noFault splitFault = iota
	leftRebuildFails
	rightRebuildFails
)

func (f splitFault) String() string {
	switch f {
	case leftRebuildFails:
		return "left-rebuild-fails"
	case rightRebuildFails:
		return "right-rebuild-fails"
	}
	return "ok"
}

// splitFaultCases runs run under each fault with a full leaf root pinned for
// writing and checks that the page the split allocated ends unpinned.
func splitFaultCases(t *testing.T, faults []splitFault, run func(t *testing.T, bt *btree, pg *page) error) {
	for _, fault := range faults {
		t.Run(fault.String(), func(t *testing.T) {
			p := tempPager(t)
			bt := initLeafBtree(t, p)
			pg, err := p.getWritablePage(bt.rootPage)
			require.NoError(t, err)
			defer p.releasePage(pg)
			require.NoError(t, bt.rebuildLeafPage(pg, leafCellsFor(30, 100)))

			newPgno := nextAllocPgno(t, p)
			switch fault {
			case leftRebuildFails:
				failRebuildOf(t, p, pg.pgno)
			case rightRebuildFails:
				failRebuildOf(t, p, newPgno)
			}

			err = run(t, bt, pg)
			if fault == noFault {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errRebuildInjected)
			}
			requireUnpinned(t, p, newPgno)
			assert.Equal(t, 1, pg.pinCount, "the split page keeps the caller's pin")
		})
	}
}

// rootLeafFaultCases runs run on a full leaf root pinned for writing, failing
// the rebuild of one page at a time: the root itself, the child balance_deeper
// allocates, or the page balance_nonroot allocates after it. Every page the
// operation allocated must end unpinned and the root must keep only the
// caller's pin.
func rootLeafFaultCases(t *testing.T, run func(bt *btree, pg *page) error) {
	for _, tc := range []struct {
		name string
		// failed is the page whose rebuild fails, as an offset from the first
		// page the operation allocates; -1 is the root, -2 is no fault.
		failed int
	}{
		{name: "ok", failed: -2},
		{name: "root-rebuild-fails", failed: -1},
		{name: "child-rebuild-fails", failed: 0},
		{name: "sibling-rebuild-fails", failed: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// given
			p := tempPager(t)
			bt := initLeafBtree(t, p)
			pg, err := p.getWritablePage(bt.rootPage)
			require.NoError(t, err)
			defer p.releasePage(pg)
			require.NoError(t, bt.rebuildLeafPage(pg, leafCellsFor(30, 100)))
			firstNew := nextAllocPgno(t, p)
			switch {
			case tc.failed == -1:
				failRebuildOf(t, p, pg.pgno)
			case tc.failed >= 0:
				failRebuildOf(t, p, firstNew+uint32(tc.failed))
			}

			// when
			err = run(bt, pg)

			// then
			if tc.failed == -2 {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, errRebuildInjected)
			}
			for pgno := firstNew; pgno <= p.dbSize.Load(); pgno++ {
				requireUnpinned(t, p, pgno)
			}
			assert.Equal(t, 1, pg.pinCount, "the root keeps the caller's pin")
		})
	}
}

func TestUpdateLeafCell_ReleasesPagesOnRebuildError(t *testing.T) {
	rootLeafFaultCases(t, func(bt *btree, pg *page) error {
		// A value nine times the old one cannot be written in place and
		// pushes the page over its size: the old cell is dropped and the new
		// one is balanced in.
		return bt.updateLeafCell(pg, 15, []byte("key-000015"), bytes.Repeat([]byte{'w'}, 900), nil)
	})
}

func TestSplitRootLeafAndInsert_ReleasesPagesOnRebuildError(t *testing.T) {
	rootLeafFaultCases(t, func(bt *btree, pg *page) error {
		return bt.splitRootLeafAndInsert(pg, 16, []byte("key-000015x"), bytes.Repeat([]byte{'w'}, 900))
	})
}

func TestSplitRoot_ReleasesNewLeftPageOnRebuildError(t *testing.T) {
	t.Run("leaf-root", func(t *testing.T) {
		// The only rebuild of the new page is the first one; leftRebuildFails
		// would fail the final rebuild of the root instead and does not apply.
		splitFaultCases(t, []splitFault{noFault, rightRebuildFails},
			func(t *testing.T, bt *btree, pg *page) error {
				return bt.splitRoot(pg, []byte("key-000030"), 99)
			})
	})

	t.Run("interior-root", func(t *testing.T) {
		for _, fault := range []splitFault{noFault, rightRebuildFails} {
			t.Run(fault.String(), func(t *testing.T) {
				p := tempPager(t)
				bt := initLeafBtree(t, p)
				pg, err := p.getWritablePage(bt.rootPage)
				require.NoError(t, err)
				defer p.releasePage(pg)
				cells := []cellData{
					{leftChild: 2, key: []byte("key-000010")},
					{leftChild: 3, key: []byte("key-000020")},
				}
				require.NoError(t, bt.rebuildInteriorPage(pg, cells, 4))

				newPgno := nextAllocPgno(t, p)
				if fault == rightRebuildFails {
					failRebuildOf(t, p, newPgno)
				}
				err = bt.splitRoot(pg, []byte("key-000030"), 5)
				if fault == noFault {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, errRebuildInjected)
				}
				requireUnpinned(t, p, newPgno)
				assert.Equal(t, 1, pg.pinCount, "the root keeps the caller's pin")
			})
		}
	})
}
