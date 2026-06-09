package vindex

import (
	"sync"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// vecTier is a process-wide, append-only RAM cache of the :vec namespace for a
// hybrid index — the "vector tier". It removes the dominant search cost: reading
// each neighbour's vector from the btree (with its tree-descent overhead) to
// compute a distance. With both the adjacency mirror and this tier, a layer-0
// hop is pure RAM (array index + SIMD distance), like a fully in-memory HNSW.
//
// It is far simpler than the adjacency mirror because **vectors are immutable**:
// a label's vector never changes once written, and labels are dense and
// monotonic. So the tier is only ever GROWN to cover newly inserted labels —
// existing entries are never invalidated or rewritten. A reader whose snapshot
// references labels < N only needs the tier to cover [0, N); a tier that already
// covers more (because a newer reader extended it) is harmless, since the extra
// labels are immutable and simply unreferenced by the older adjacency. This
// makes it safe to share one growing tier across readers at different snapshots
// with no per-snapshot versioning.
//
// Records are stored at their natural btree format (raw float32 for QuantNone,
// scale+int8 for QuantInt8) in one contiguous slab, stride = vecRecordSize, so
// the cache is byte-identical to the btree and search results are unchanged.
type vecTier struct {
	mu     sync.Mutex
	stride int
	n      uint32 // labels [0, n) are populated
	data   []byte // contiguous slab, record i at [i*stride : (i+1)*stride]
}

func newVecTier(stride int) *vecTier { return &vecTier{stride: stride} }

// ensure makes the tier cover labels [0, upto), reading any missing records from
// the :vec namespace at rtx's snapshot, and returns the backing slab. The
// returned slice is safe to use without the lock for the duration of one search:
// the slab only grows (it may be reallocated by a later ensure, but this slice
// keeps pointing at a valid, immutable copy of every label it already covers).
//
// A Compact rebuilds the graph and reuses labels for DIFFERENT vectors, but it
// also moves the index's namespace root pages, so the collection reopens the
// Index with a fresh (empty) tier — this slab is never reused across a
// compaction, which is why no epoch/versioning is needed here.
func (t *vecTier) ensure(rtx *btree.ReadTx, ns *btree.Namespace, upto uint32) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if upto <= t.n {
		return t.data, nil
	}
	need := int(upto) * t.stride
	if cap(t.data) < need {
		grown := make([]byte, need, need+need/4) // modest headroom for interleaved growth
		copy(grown, t.data)
		t.data = grown
	} else {
		t.data = t.data[:need]
	}
	var rec, kbuf []byte
	var err error
	for lbl := t.n; lbl < upto; lbl++ {
		kbuf = labelKey(kbuf, lbl)
		rec, err = rtx.AppendValue(ns, kbuf, rec[:0])
		if err != nil {
			t.data = t.data[:int(t.n)*t.stride] // keep the consistent prefix
			return nil, err
		}
		copy(t.data[int(lbl)*t.stride:], rec)
	}
	t.n = upto
	return t.data, nil
}
