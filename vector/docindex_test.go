package vector

import (
	"encoding/binary"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// objectID-like 12-byte ids, the any-store default shape.
func docIDs(n int) [][]byte {
	ids := make([][]byte, n)
	for i := range ids {
		b := make([]byte, 12)
		binary.BigEndian.PutUint64(b[:8], uint64(i+1))
		binary.BigEndian.PutUint32(b[8:], 0xABCD0000^uint32(i))
		ids[i] = b
	}
	return ids
}

func TestIDDictRoundTrip(t *testing.T) {
	d := NewIDDict(8)
	ids := docIDs(100)
	for _, id := range ids {
		l, isNew := d.Intern(id)
		require.True(t, isNew)
		require.Equal(t, id, d.ID(l))
	}
	require.Equal(t, 100, d.Len())
	// re-intern is stable, not new
	l0, isNew := d.Intern(ids[0])
	require.False(t, isNew)
	require.Equal(t, uint32(0), l0)

	l, ok := d.Label(ids[42])
	require.True(t, ok)
	require.Equal(t, ids[42], d.ID(l))

	_, ok = d.Delete(ids[42])
	require.True(t, ok)
	require.Equal(t, 99, d.Len())
	_, ok = d.Label(ids[42])
	require.False(t, ok, "deleted id no longer resolves")
}

func TestDocFlatHNSWEndToEnd(t *testing.T) {
	const (
		dim = 32
		n   = 1000
		k   = 10
	)
	vecs, _ := randVectors(n, dim, 4)
	ids := docIDs(n)

	x := NewDocFlatHNSW(dim, L2, 1, n)
	x.g.EfSearch = 64
	for i := range vecs {
		x.Add(ids[i], vecs[i])
	}
	require.Equal(t, n, x.Len())

	// self-query returns the document id bytes
	res := x.Search(vecs[7], 1)
	require.Len(t, res, 1)
	require.Equal(t, ids[7], res[0].ID)

	// delete + ensure it disappears from results
	require.True(t, x.Delete(ids[7]))
	require.Equal(t, n-1, x.Len())
	for _, r := range x.Search(vecs[7], 10) {
		assert.NotEqual(t, ids[7], r.ID)
	}

	// update moves a doc to another location
	require.True(t, x.Update(ids[100], vecs[500]))
	var found bool
	for _, r := range x.Search(vecs[500], 5) {
		if string(r.ID) == string(ids[100]) {
			found = true
		}
	}
	assert.True(t, found)
}

// TestIDMapMemory quantifies the cost of linking graph nodes to document ids:
// the flat-arena IDDict vs the naive [][]byte ("store a pointer to each id").
func TestIDMapMemory(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const n = 500_000
	idLen := 12

	heapMiB := func() float64 {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return float64(ms.HeapAlloc) / (1 << 20)
	}

	measure := func(build func(ids [][]byte) func()) float64 {
		ids := docIDs(n)
		release := build(ids)
		ids = nil
		_ = ids
		with := heapMiB()
		release()
		return with - heapMiB()
	}

	t.Logf("%d document ids x %d bytes = %.1f MiB raw id bytes", n, idLen, float64(n*idLen)/(1<<20))

	// (a) flat-arena dictionary
	var dictMem float64
	flatHeap := measure(func(ids [][]byte) func() {
		d := NewIDDict(n)
		for _, id := range ids {
			d.Intern(id)
		}
		dictMem = float64(d.MemBytes()) / (1 << 20)
		return func() { runtime.KeepAlive(d); d = nil; _ = d }
	})
	t.Logf("flat-arena IDDict   heap=%6.1f MiB (%.1f B/id)  [MemBytes=%.1f MiB]",
		flatHeap, flatHeap*(1<<20)/float64(n), dictMem)

	// (b) naive [][]byte + map (the "store ptr to id" option)
	naiveHeap := measure(func(ids [][]byte) func() {
		fwd := make(map[string]uint32, n)
		rev := make([][]byte, 0, n)
		for _, id := range ids {
			cp := append([]byte(nil), id...)
			fwd[string(cp)] = uint32(len(rev))
			rev = append(rev, cp)
		}
		return func() { runtime.KeepAlive(fwd); runtime.KeepAlive(rev); fwd = nil; rev = nil; _, _ = fwd, rev }
	})
	t.Logf("naive [][]byte+map  heap=%6.1f MiB (%.1f B/id)", naiveHeap, naiveHeap*(1<<20)/float64(n))
	t.Logf("=> flat arena saves %.1f MiB (%.0f%%) of id-mapping RAM", naiveHeap-flatHeap, 100*(naiveHeap-flatHeap)/naiveHeap)
}

// TestDeleteCompactRAM tracks resident RAM across the delete -> compact
// lifecycle: tombstones do NOT free memory; Compact does.
func TestDeleteCompactRAM(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	const (
		n   = 50_000
		dim = 128
	)
	vecs, keys := randVectors(n, dim, 9)
	g := NewFlatHNSW(dim, L2, 1)
	for i := range vecs {
		g.Add(keys[i], vecs[i])
	}
	mib := func(b int) float64 { return float64(b) / (1 << 20) }
	report := func(stage string) {
		t.Logf("%-22s live=%6d physical=%6d  arena=%6.1f MiB", stage, g.Len(), g.PhysicalLen(), mib(g.MemBytes()))
	}
	report("after build")

	for i := 0; i < n/2; i++ { // delete 50%
		g.Delete(keys[i])
	}
	report("after 50% delete")
	require.Equal(t, n, g.PhysicalLen(), "tombstones keep physical size")

	g.Compact()
	report("after compact")
	require.Equal(t, n/2, g.PhysicalLen())
}
