package btree

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBalanceNonroot_FillFactor is the spec success metric for the
// balance_nonroot 3-sibling redistribution port (btree.c:8248-9030). It reuses
// the balance_quick harness (walkLeavesForFill / reportFillStats /
// leafFillStats, btree_balance_quick_test.go) so before/after numbers are
// directly comparable to the measurement in
// docs/btree/plans/2026-05-23-balance-nonroot-3sibling.md.
//
// The "random" workload is the home turf of 3-sibling redistribution: every
// split that touches a low-fill neighbour re-packs that neighbour too, so the
// long low-fill tail of the old 2-way splitter (min 8.7%, 631 leaves) is
// eliminated and avg fill rises. Thresholds are conservative (the plan targets
// ~0.75 avg; we assert ≥0.72 to leave headroom for the omitted editPage/rekey
// micro-optimisations).
func TestBalanceNonroot_FillFactor(t *testing.T) {
	const (
		pageSize = 1024
		nRows    = 5000
		valSize  = 80
	)

	cases := []struct {
		name    string
		fillKey func(i int) []byte
		// minAvgFill / maxLeaves / minMinFill are asserted only when set (>0).
		minAvgFill float64
		maxLeaves  int
		minMinFill float64
		// requireNonroot asserts balanceNonroot actually fired on this workload.
		requireNonroot bool
	}{
		{
			name: "random",
			fillKey: func(i int) []byte {
				rng := rand.New(rand.NewSource(int64(i) * 2862933555777941757))
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, rng.Uint32())
				return k
			},
			minAvgFill:     0.72,
			maxLeaves:      560,
			minMinFill:     0.30,
			requireNonroot: true,
		},
		{
			// Descending keys = always leftmost insert: the adversarial case for
			// any left-biased B-tree. Even SQLite (WITHOUT ROWID index btree, same
			// params) packs this to only ~42.5% payload across 831 leaves — the
			// leftmost leaf repeatedly re-splits and its right neighbours never
			// refill. Our balance_nonroot port matches SQLite's 831 leaves exactly
			// and improves the 2-way baseline from avg 0.434 / 999 leaves to
			// 0.52 / 831. The threshold is set to "beats the 2-way baseline and
			// matches SQLite's leaf count", not the random-case 0.72 (unreachable
			// here for SQLite too). See plan §"Measurable success criteria".
			name: "sequential_reverse",
			fillKey: func(i int) []byte {
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, uint32(nRows-i))
				return k
			},
			minAvgFill:     0.50, // base 2-way: 0.434; SQLite: ~0.425 payload
			maxLeaves:      850,  // base 2-way: 999; SQLite: 831
			requireNonroot: true,
		},
		{
			// Strict low/high alternation forces a 3-page gather on nearly every
			// split. SQLite (same params) reaches ~59.5% payload across 629 leaves;
			// our port reaches ~0.65 / 666 leaves, vs the 2-way baseline 0.505 /
			// 857. Threshold = beats baseline, approaches SQLite.
			name: "interleaved",
			fillKey: func(i int) []byte {
				k := make([]byte, 4)
				var v uint32
				if i%2 == 0 {
					v = uint32(i / 2)
				} else {
					v = uint32(nRows - i/2)
				}
				binary.BigEndian.PutUint32(k, v)
				return k
			},
			minAvgFill:     0.62, // base 2-way: 0.505; SQLite: ~0.595 payload
			maxLeaves:      720,  // base 2-way: 857; SQLite: 629
			requireNonroot: true,
		},
		{
			name: "monotonic_append", // proves balance_quick still owns the append path
			fillKey: func(i int) []byte {
				k := make([]byte, 4)
				binary.BigEndian.PutUint32(k, uint32(i+1))
				return k
			},
			minAvgFill: 0.85,
		},
	}

	val := make([]byte, valSize)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resetPoolForTest(t)
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: pageSize})
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			tx, err := db.BeginWrite()
			require.NoError(t, err)
			_, err = tx.CreateNamespace("t1")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			db.pager.balanceNonrootDispatchCount.Store(0)

			tx, err = db.BeginWrite()
			require.NoError(t, err)
			ns, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			for i := 0; i < nRows; i++ {
				require.NoError(t, tx.Put(ns, tc.fillKey(i), val))
			}
			require.NoError(t, tx.Commit())

			nonrootDispatches := db.pager.balanceNonrootDispatchCount.Load()

			// Tree integrity is the make-or-break gate.
			require.NoError(t, db.IntegrityCheck(), "%s: tree integrity after %d inserts", tc.name, nRows)

			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			ns2, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)
			bt := &btree{pager: db.pager, rootPage: ns2.rootPage, walMaxFrame: rtx.walMaxFrame}
			stats := walkLeavesForFill(t, bt)
			usable := bt.usablePageSize()
			reportFillStats(t, tc.name, stats, usable, nRows)

			// Full-scan count verification (no lost / duplicated cells).
			require.Equal(t, nRows, stats.totalCells, "%s: leaf cell count", tc.name)

			const leafHeaderSize = 8
			leafCapacity := usable - leafHeaderSize
			used := stats.totalUsed()
			avgFill := float64(used) / float64(stats.leafCount*leafCapacity)

			sorted := make([]int, len(stats.perLeafUsed))
			copy(sorted, stats.perLeafUsed)
			sort.Ints(sorted)
			minFill := float64(sorted[0]) / float64(leafCapacity)

			t.Logf("%s: avgFill=%.4f leaves=%d minFill=%.4f nonrootDispatches=%d",
				tc.name, avgFill, stats.leafCount, minFill, nonrootDispatches)

			if tc.requireNonroot {
				require.Greater(t, nonrootDispatches, int64(0),
					"%s: balanceNonroot must have fired (guards against balance_quick swallowing everything)", tc.name)
			}
			if tc.minAvgFill > 0 {
				require.GreaterOrEqual(t, avgFill, tc.minAvgFill,
					"%s: avg leaf fill %.1f%% across %d leaves", tc.name, avgFill*100, stats.leafCount)
			}
			if tc.maxLeaves > 0 {
				require.LessOrEqual(t, stats.leafCount, tc.maxLeaves,
					"%s: leaf count %d", tc.name, stats.leafCount)
			}
			if tc.minMinFill > 0 {
				require.GreaterOrEqual(t, minFill, tc.minMinFill,
					"%s: min leaf fill %.1f%% (kills the low-fill tail)", tc.name, minFill*100)
			}
		})
	}
}

// TestBalanceNonroot_SiblingGather pins the NB=3 sibling gather and the
// k ∈ {nOld-1, nOld, nOld+1} invariant (btree.c:8328 nOld, the k pack loop at
// btree.c:8563-8605). It builds a depth-2 tree with several adjacent leaves at
// known high fill, then overflows a MIDDLE (non-rightmost) leaf so the general
// balance_nonroot path runs (not balance_quick), and asserts via the test-only
// lastBalanceNOld / lastBalanceNNew hooks that exactly 3 siblings were gathered
// and the output page count stayed within {nOld-1, nOld, nOld+1}.
func TestBalanceNonroot_SiblingGather(t *testing.T) {
	resetPoolForTest(t)
	dir := t.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 1024})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t1")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Build a depth-2 tree: insert a spread of keys so the root is interior with
	// several leaf children, each reasonably full. valSize 80, pageSize 1024 →
	// ~11 cells/leaf; 400 rows → a depth-2 tree with ~40 leaves under one root
	// (well past the depth-1 threshold but not yet depth-3).
	val := make([]byte, 80)
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	// Even keys only, leaving odd-key gaps to overflow a specific middle leaf.
	for i := 0; i < 400; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i*2))
		require.NoError(t, tx.Put(ns, key, val))
	}
	require.NoError(t, tx.Commit())
	require.NoError(t, db.IntegrityCheck())

	// Now hammer the MIDDLE of the key space with odd keys. Middle-of-tree
	// inserts cannot use balance_quick (idx != cellCount), so they exercise
	// balance_nonroot. Track that nOld==3 was gathered at least once and that
	// nNew always stayed in {nOld-1, nOld, nOld+1}.
	db.pager.balanceNonrootDispatchCount.Store(0)
	db.pager.lastBalanceNOld.Store(0)
	db.pager.lastBalanceNNew.Store(0)

	saw3 := false
	tx, err = db.BeginWrite()
	require.NoError(t, err)
	ns, err = db.getNamespaceLocked("t1")
	require.NoError(t, err)
	for i := 0; i < 400; i++ {
		// Odd keys in the middle third of the populated range.
		key := binary.BigEndian.AppendUint32(nil, uint32(266+i*2+1))
		before := db.pager.balanceNonrootDispatchCount.Load()
		require.NoError(t, tx.Put(ns, key, val))
		after := db.pager.balanceNonrootDispatchCount.Load()
		if after > before {
			nOld := db.pager.lastBalanceNOld.Load()
			nNew := db.pager.lastBalanceNNew.Load()
			require.GreaterOrEqual(t, nOld, int64(1), "nOld must be ≥1")
			require.LessOrEqual(t, nOld, int64(nbSiblings), "nOld must be ≤ NB=3")
			// k ∈ {nOld-1, nOld, nOld+1} (btree.c:8328 / k pack loop).
			require.GreaterOrEqual(t, nNew, nOld-1, "nNew ≥ nOld-1")
			require.LessOrEqual(t, nNew, nOld+1, "nNew ≤ nOld+1")
			if nOld == int64(nbSiblings) {
				saw3 = true
			}
		}
	}
	require.NoError(t, tx.Commit())

	require.Greater(t, db.pager.balanceNonrootDispatchCount.Load(), int64(0),
		"balance_nonroot must have fired for middle-of-tree inserts")
	require.True(t, saw3,
		"a full 3-sibling gather (nOld==3) must have occurred at least once")

	require.NoError(t, db.IntegrityCheck())

	// Every key must be readable and counts must match.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtx.Rollback() })
	ns2, err := db.getNamespaceLocked("t1")
	require.NoError(t, err)
	count := 0
	cur := rtx.NewCursor(ns2)
	var prev []byte
	for cerr := cur.First(); cerr == nil && cur.Valid(); cerr = cur.Next() {
		k, kerr := cur.Key()
		require.NoError(t, kerr)
		if prev != nil {
			require.Negative(t, bytes.Compare(prev, k), "scan order")
		}
		prev = bytes.Clone(k)
		count++
	}
	require.Equal(t, 800, count, "400 even + 400 odd keys")
}

// TestBalanceNonroot_RandomMutationIntegrity is the make-or-break correctness
// gate: heavy randomized insert/delete/update workloads followed by a full
// IntegrityCheck (key ordering, divider <,>= invariants, no lost/duplicated
// cells, page accounting, child-depth equality, overflow chain lengths, orphan
// pages) and a full-scan count + readback against a reference map. A balance
// bug that passes the unit tests but corrupts the tree is caught here.
func TestBalanceNonroot_RandomMutationIntegrity(t *testing.T) {
	for _, pageSize := range []uint32{512, 1024, 4096} {
		pageSize := pageSize
		t.Run("page"+itoaTest(int(pageSize)), func(t *testing.T) {
			resetPoolForTest(t)
			dir := t.TempDir()
			db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: pageSize})
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			tx, err := db.BeginWrite()
			require.NoError(t, err)
			_, err = tx.CreateNamespace("t1")
			require.NoError(t, err)
			require.NoError(t, tx.Commit())

			rng := rand.New(rand.NewSource(0x0ADC0FFEE0DDF00D))
			ref := map[uint32][]byte{}
			const ops = 12000

			// Mix of inserts, updates (same key, different value size) and deletes,
			// committed in randomly-sized batches with periodic integrity checks.
			i := 0
			for i < ops {
				batch := 1 + rng.Intn(64)
				tx, err = db.BeginWrite()
				require.NoError(t, err)
				ns, err := db.getNamespaceLocked("t1")
				require.NoError(t, err)
				for b := 0; b < batch && i < ops; b, i = b+1, i+1 {
					r := rng.Intn(100)
					switch {
					case r < 60 || len(ref) == 0:
						// insert/overwrite a random key with a random-length value
						k := rng.Uint32() % 4000
						vlen := rng.Intn(300)
						v := make([]byte, vlen)
						for j := range v {
							v[j] = byte(k + uint32(j))
						}
						key := binary.BigEndian.AppendUint32(nil, k)
						require.NoError(t, tx.Put(ns, key, v))
						ref[k] = bytes.Clone(v)
					case r < 80:
						// delete a random existing key
						var k uint32
						for kk := range ref {
							k = kk
							break
						}
						key := binary.BigEndian.AppendUint32(nil, k)
						require.NoError(t, tx.Delete(ns, key))
						delete(ref, k)
					default:
						// update an existing key with a larger value (forces growth/split)
						var k uint32
						for kk := range ref {
							k = kk
							break
						}
						vlen := 50 + rng.Intn(500)
						v := make([]byte, vlen)
						for j := range v {
							v[j] = byte(^k + uint32(j))
						}
						key := binary.BigEndian.AppendUint32(nil, k)
						require.NoError(t, tx.Put(ns, key, v))
						ref[k] = bytes.Clone(v)
					}
				}
				require.NoError(t, tx.Commit())

				if i%2000 < batch { // a few times across the run
					require.NoError(t, db.IntegrityCheck(), "integrity after %d ops (pageSize=%d)", i, pageSize)
				}
			}

			require.NoError(t, db.IntegrityCheck(), "final integrity (pageSize=%d)", pageSize)

			// Full-scan: ordered, no dups, exact count, and value match vs ref.
			rtx, err := db.BeginRead()
			require.NoError(t, err)
			defer func() { _ = rtx.Rollback() }()
			ns2, err := db.getNamespaceLocked("t1")
			require.NoError(t, err)

			count := 0
			var prev []byte
			cur := rtx.NewCursor(ns2)
			for cerr := cur.First(); cerr == nil && cur.Valid(); cerr = cur.Next() {
				k, kerr := cur.Key()
				require.NoError(t, kerr)
				if prev != nil {
					require.Negative(t, bytes.Compare(prev, k), "scan order (pageSize=%d)", pageSize)
				}
				prev = bytes.Clone(k)
				kk := binary.BigEndian.Uint32(k)
				want, ok := ref[kk]
				require.True(t, ok, "scanned key %d not in ref (pageSize=%d)", kk, pageSize)
				gotVal, verr := cur.Value()
				require.NoError(t, verr)
				// bytes.Equal treats nil and empty-slice as equal (a zero-length
				// value round-trips as nil); require.Equal would not.
				require.True(t, bytes.Equal(want, gotVal), "value mismatch key %d (pageSize=%d)", kk, pageSize)
				count++
			}
			require.Equal(t, len(ref), count, "scan count vs ref (pageSize=%d)", pageSize)

			// Point-lookup every reference key.
			for kk, want := range ref {
				got, gerr := rtx.Get(ns2, binary.BigEndian.AppendUint32(nil, kk))
				require.NoError(t, gerr, "get key %d (pageSize=%d)", kk, pageSize)
				require.True(t, bytes.Equal(want, got), "get value key %d (pageSize=%d)", kk, pageSize)
			}
		})
	}
}

func itoaTest(n int) string {
	return string([]byte{byte('0' + n/1000%10), byte('0' + n/100%10), byte('0' + n/10%10), byte('0' + n%10)})
}
