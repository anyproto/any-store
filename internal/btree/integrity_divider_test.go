package btree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIntegrityCheck_DividerRange_PositiveRandom verifies that valid trees
// built via random insert / delete / update across several page sizes pass the
// strengthened divider-range check. This is the positive side of the proof:
// the new bound check does not produce false positives on healthy trees.
func TestIntegrityCheck_DividerRange_PositiveRandom(t *testing.T) {
	for _, pageSize := range []uint32{512, 1024, 4096} {
		pageSize := pageSize
		t.Run(fmt.Sprintf("pageSize=%d", pageSize), func(t *testing.T) {
			db := tempDBWithPageSize(t, pageSize)
			tx, err := db.BeginWrite()
			require.NoError(t, err)
			_, err = tx.CreateNamespace("t1")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			rng := rand.New(rand.NewSource(int64(pageSize) * 7919))
			present := map[uint32]bool{}

			// Several rounds of mixed insert / delete / update with varying value
			// sizes (some forcing overflow), interleaved with integrity checks.
			for round := 0; round < 6; round++ {
				tx, err := db.BeginWrite()
				require.NoError(t, err)
				ns, err := db.getNamespaceLocked("t1")
				require.NoError(t, err)
				for op := 0; op < 400; op++ {
					k := uint32(rng.Intn(2000))
					key := binary.BigEndian.AppendUint32(nil, k)
					switch rng.Intn(3) {
					case 0, 1:
						valSize := rng.Intn(200)
						if rng.Intn(10) == 0 {
							valSize = pageSizeDependentBig(pageSize) // force overflow
						}
						require.NoError(t, tx.Put(ns, key, make([]byte, valSize)))
						present[k] = true
					case 2:
						if present[k] {
							require.NoError(t, tx.Delete(ns, key))
							delete(present, k)
						}
					}
				}
				require.NoError(t, tx.Commit())
				require.NoError(t, db.IntegrityCheck())
			}
		})
	}
}

func pageSizeDependentBig(pageSize uint32) int {
	// A value comfortably larger than maxLocalPayload to force overflow pages.
	return int(pageSize) * 3
}

// findInteriorPageWithCell walks the namespace tree and returns the page number
// of the first interior page that has >= 1 cell, along with that page's first
// divider key and its left child's largest key. Used to plant a precise
// divider-range violation.
func findFirstInterior(t *testing.T, bt *btree, pgno uint32) (interiorPgno uint32, ok bool) {
	t.Helper()
	pg, err := bt.getPage(pgno)
	require.NoError(t, err)
	if pg.header.isLeaf() {
		bt.pager.releasePage(pg)
		return 0, false
	}
	if pg.header.cellCount >= 1 {
		bt.pager.releasePage(pg)
		return pgno, true
	}
	rc := pg.header.rightChild
	bt.pager.releasePage(pg)
	return findFirstInterior(t, bt, rc)
}

// subtreeMaxKey returns the largest uint32 key in the subtree rooted at pgno.
func subtreeMaxKey(t *testing.T, bt *btree, pgno uint32) uint32 {
	t.Helper()
	pg, err := bt.getPage(pgno)
	require.NoError(t, err)
	usable := bt.usablePageSize()
	var maxKey uint32
	if pg.header.isLeaf() {
		n := int(pg.header.cellCount)
		for i := 0; i < n; i++ {
			off := int(pg.getCellOffset(i))
			k, _ := leafFullKey(pg.data, off, usable, bt.pager, bt.walMaxFrame, bt.cache)
			if len(k) == 4 {
				if v := binary.BigEndian.Uint32(k); v > maxKey {
					maxKey = v
				}
			}
		}
		bt.pager.releasePage(pg)
		return maxKey
	}
	n := int(pg.header.cellCount)
	children := make([]uint32, 0, n+1)
	for i := 0; i < n; i++ {
		off := int(pg.getCellOffset(i))
		_, lc, _ := bt.interiorCellFullKey(pg.data, off, usable)
		children = append(children, lc)
	}
	children = append(children, pg.header.rightChild)
	bt.pager.releasePage(pg)
	for _, c := range children {
		if v := subtreeMaxKey(t, bt, c); v > maxKey {
			maxKey = v
		}
	}
	return maxKey
}

// TestIntegrityCheck_DividerRange_CatchesPlantedViolation is the NEGATIVE test:
// it builds a valid multi-level tree, then clobbers a parent's divider key D_0
// to the largest key in its left child subtree. Because any-store's divider
// semantics route keys < D_0 to the left child, that child now holds a key
// (== new D_0) that violates its EXCLUSIVE upper bound. The strengthened check
// MUST report a divider-range violation. This proves the check actually detects
// the misrouted-divider corruption it is designed to catch.
//
// The corruption keeps the parent page's own cell ordering intact (new D_0 is
// still < D_1), so the failure is specifically the new bound check, not the
// pre-existing "key out of order" check.
func TestIntegrityCheck_DividerRange_CatchesPlantedViolation(t *testing.T) {
	db := tempDBWithPageSize(t, 1024)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Build a 2+ level tree.
	putN(t, db, "t1", 200, 30)

	// Baseline: a healthy tree must pass.
	require.NoError(t, db.IntegrityCheck())

	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)

	// Locate an interior page with a cell, read its first divider's left child,
	// and compute that child subtree's max key.
	var planted struct {
		interiorPgno uint32
		cellOff      int
		leftChild    uint32
		newDivider   uint32
	}
	{
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		btr := &btree{pager: db.pager, rootPage: ns.rootPage, walMaxFrame: rtx.walMaxFrame}
		ip, ok := findFirstInterior(t, btr, btr.rootPage)
		require.True(t, ok, "expected a multi-level tree with an interior page")
		pg, err := btr.getPage(ip)
		require.NoError(t, err)
		require.GreaterOrEqual(t, int(pg.header.cellCount), 1)
		off := int(pg.getCellOffset(0))
		k, lc, _ := btr.interiorCellFullKey(pg.data, off, btr.usablePageSize())
		require.Equal(t, 4, len(k), "divider key must be a 4-byte uint32")
		btr.pager.releasePage(pg)
		childMax := subtreeMaxKey(t, btr, lc)
		planted.interiorPgno = ip
		planted.cellOff = off
		planted.leftChild = lc
		// Set divider == child's max key. Keys < divider go left, so the child
		// now contains a key (childMax) that is NOT < divider => violation.
		planted.newDivider = childMax
		rtx.Rollback()
	}

	// Clobber the divider key in place.
	tx2, err := db.BeginWrite()
	require.NoError(t, err)
	wpg, err := db.pager.getWritablePage(planted.interiorPgno)
	require.NoError(t, err)
	// Interior cell layout: [4-byte leftChild][varint keyLen][key]. For a
	// 4-byte key, keyLen is a 1-byte varint (4), so the key bytes start at
	// cellOff+5.
	keyStart := planted.cellOff + 5
	binary.BigEndian.PutUint32(wpg.data[keyStart:keyStart+4], planted.newDivider)
	db.pager.releasePage(wpg)
	require.NoError(t, tx2.Commit())

	// The strengthened check MUST catch the planted divider-range violation.
	err = db.IntegrityCheck()
	require.Error(t, err, "expected divider-range violation to be detected")
	ie, ok := err.(*IntegrityError)
	require.True(t, ok)
	found := false
	for _, e := range ie.Errors {
		if strings.Contains(e, "divider") && (strings.Contains(e, "upper bound") || strings.Contains(e, "lower bound")) {
			found = true
			break
		}
	}
	require.True(t, found, "expected a divider bound violation, got: %v", ie.Errors)
}
