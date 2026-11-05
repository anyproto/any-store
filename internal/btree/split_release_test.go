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

// failRebuildOf makes every rebuild of page pgno fail until the test ends.
func failRebuildOf(t *testing.T, pgno uint32) {
	t.Helper()
	setRebuildPageFaultHook(func(pg *page) error {
		if pg.pgno == pgno {
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
				failRebuildOf(t, pg.pgno)
			case rightRebuildFails:
				failRebuildOf(t, newPgno)
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

func TestSplitLeafAndInsert_ReleasesRightPageOnRebuildError(t *testing.T) {
	splitFaultCases(t, []splitFault{noFault, leftRebuildFails, rightRebuildFails},
		func(t *testing.T, bt *btree, pg *page) error {
			return bt.splitLeafAndInsert(pg, 15, []byte("key-000014x"), []byte("v"))
		})
}

func TestUpdateLeafCell_ReleasesRightPageOnRebuildError(t *testing.T) {
	splitFaultCases(t, []splitFault{noFault, leftRebuildFails, rightRebuildFails},
		func(t *testing.T, bt *btree, pg *page) error {
			// A value nine times the old one cannot be written in place and
			// pushes the page over its size, so the slow path splits it.
			return bt.updateLeafCell(pg, 15, []byte("key-000015"), bytes.Repeat([]byte{'w'}, 900), nil)
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
					failRebuildOf(t, newPgno)
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
