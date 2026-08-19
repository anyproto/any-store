package btree

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// k encodes n as a 4-byte big-endian key (byte order == numeric order).
func k(n uint32) []byte { return binary.BigEndian.AppendUint32(nil, n) }

// TestRangeFraction_Interpolation checks that B-tree page interpolation estimates
// range fractions within a usable tolerance for the planner. A small page size
// forces a multi-level tree so the interior-node interpolation path is exercised.
func TestRangeFraction_Interpolation(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	const N = 2000
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	for i := uint32(1); i <= N; i++ {
		require.NoError(t, tx.Put(ns, k(i), []byte{0}))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err = db.getNamespaceLocked("t")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	defer cur.Close()

	// Sanity: the tree is multi-level (otherwise interpolation is trivial).
	// (512-byte pages with 2000 keys gives at least 2 levels.)

	cases := []struct {
		name     string
		low, high []byte
		want     float64
		tol      float64
	}{
		{"full unbounded", nil, nil, 1.0, 0.0},
		{"lower half", nil, k(N / 2), 0.5, 0.08},
		{"upper half (one-sided $gt)", k(N / 2), nil, 0.5, 0.08},
		{"middle 50%", k(N / 4), k(3 * N / 4), 0.5, 0.10},
		{"first decile", nil, k(N / 10), 0.1, 0.06},
		{"last decile ($gt)", k(9 * N / 10), nil, 0.1, 0.06},
		{"narrow 1%", k(N/2 - 10), k(N/2 + 10), 0.01, 0.05},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := cur.RangeFraction(tc.low, tc.high)
			require.NoError(t, err)
			assert.InDelta(t, tc.want, f, tc.tol,
				"RangeFraction(%v..%v) = %v, want ~%v", tc.low, tc.high, f, tc.want)
		})
	}

	// Inverted / empty range clamps to 0 (never negative).
	f, err := cur.RangeFraction(k(3*N/4), k(N/4))
	require.NoError(t, err)
	assert.Equal(t, 0.0, f, "inverted range must clamp to 0")

	// Monotonic ranks: keyRank is non-decreasing in the key.
	prev := -1.0
	for i := uint32(0); i <= 10; i++ {
		r, err := cur.keyRank(k(i * (N / 10)))
		require.NoError(t, err)
		assert.GreaterOrEqual(t, r, prev-1e-9, "keyRank must be monotonic")
		prev = r
	}
}

// TestRangeFraction_Skew validates the central design claim: page interpolation
// measures the physical KEY space (where the balanced tree spends its pages), not
// the numeric VALUE domain, so a tight cluster of keys is reported by its share of
// entries — not its tiny share of the value range. A value-domain min/max
// interpolation would call the cluster ~0% here; interpolation must call it ~90%.
func TestRangeFraction_Skew(t *testing.T) {
	db := tempDBWithPageSize(t, 512)

	tx, err := db.BeginWrite()
	require.NoError(t, err)
	ns, err := tx.CreateNamespace("t")
	require.NoError(t, err)
	// 1800 keys densely clustered in [1_000_000, 1_001_800]; 200 keys sparsely
	// spread across the rest of the uint32 domain. The cluster is ~90% of entries
	// but a vanishing fraction of the value range.
	for i := uint32(0); i < 1800; i++ {
		require.NoError(t, tx.Put(ns, k(1_000_000+i), []byte{0}))
	}
	for i := uint32(0); i < 200; i++ {
		require.NoError(t, tx.Put(ns, k(i*20_000_000+1), []byte{0}))
	}
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err = db.getNamespaceLocked("t")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	defer cur.Close()

	f, err := cur.RangeFraction(k(1_000_000), k(1_001_800))
	require.NoError(t, err)
	assert.InDelta(t, 0.9, f, 0.1,
		"cluster holding ~90%% of entries must estimate ~0.9, got %v", f)
}

// TestRangeFraction_EmptyTree returns 0 for any bounded range and a clean 1.0 for
// the fully-unbounded range on an empty namespace.
func TestRangeFraction_EmptyTree(t *testing.T) {
	db := tempDB(t)
	tx, err := db.BeginWrite()
	require.NoError(t, err)
	_, err = tx.CreateNamespace("t")
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()
	ns, err := db.getNamespaceLocked("t")
	require.NoError(t, err)
	cur := rtx.NewCursor(ns)
	defer cur.Close()

	f, err := cur.RangeFraction(k(1), k(100))
	require.NoError(t, err)
	assert.Equal(t, 0.0, f)

	f, err = cur.RangeFraction(nil, nil)
	require.NoError(t, err)
	assert.Equal(t, 1.0, f)
}
