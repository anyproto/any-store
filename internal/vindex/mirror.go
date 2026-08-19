package vindex

import (
	"encoding/binary"
	"errors"
	"slices"
	"sync"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// The hybrid-mode layer-0 mirror is a RAM snapshot of HNSW layer 0 — each node's
// layer-0 neighbour list plus the tombstone set — that read searches consult so a
// layer-0 hop avoids btree reads (the bulk of search I/O). The btree stays the
// source of truth; the mirror is derived and snapshot-aligned, never ahead of the
// reader's snapshot (which would dereference labels whose vector isn't visible
// yet) and never silently behind.
//
// It is split into an immutable CSR BASE plus a small per-generation OVERLAY:
//
//   - l0Base is a pointer-free CSR (offsets/edges/deleted-bitset) direct-indexed
//     by the dense [0,nextLabel) label space. No Go maps, no per-node slices: the
//     common (unpatched) lookup is one array index / one bit test, and GC never
//     scans it. The base is immutable and shared across readers; it is rebuilt
//     only on a fold or when no base covers the reader's snapshot.
//
//   - l0Overlay holds just the labels whose layer-0 adjacency or deleted-state
//     changed since the base's generation (a write touches the new node plus its
//     ~m0 back-links, or flips one tombstone bit — a tiny set). Adjacency lives in
//     a u32slab (the vecCache arena pattern: label -> []uint32 run, open-addressed,
//     pointer-free); deletes in a u32set. Built by replaying the in-memory dirty
//     ring and re-reading only the changed labels' :adj at the reader's snapshot,
//     so a search after a write costs O(changed), not O(N).
//
// meta.l0Gen is the generation: every layer-0 write (insert/replace/delete) bumps
// it, and a mirror is valid only for a snapshot whose l0Gen it matches exactly.

// ---------------------------------------------------------------------------
// CSR base
// ---------------------------------------------------------------------------

// l0Base is an immutable CSR snapshot of layer 0 at gen, covering labels [0,n).
// All three slices are pointer-free, so the GC never scans them.
type l0Base struct {
	gen     uint64
	n       uint32   // labels [0,n)
	offsets []uint32 // len n+1; label L's edges are edges[offsets[L]:offsets[L+1]]
	edges   []uint32 // flat layer-0 neighbour lists, concatenated in label order
	deleted []uint64 // tombstone bitset, n/64+1 words
}

func (b *l0Base) neighbors(label uint32) []uint32 {
	if label >= b.n {
		return nil
	}
	return b.edges[b.offsets[label]:b.offsets[label+1]]
}

func (b *l0Base) isDeleted(label uint32) bool {
	if label >= b.n {
		return false
	}
	return b.deleted[label>>6]&(1<<(label&63)) != 0
}

// buildL0Base scans the adjacency namespace at rtx's snapshot and materialises the
// layer-0 CSR + tombstone set, tagged with gen. Records are keyed by big-endian
// label so the cursor yields labels in ascending order; the label space is dense
// over [0,nextLabel) (every inserted label keeps its :adj record, tombstoned or
// not), but any gap is filled as an empty neighbour run for safety.
func buildL0Base(rtx *btree.ReadTx, ix *Index, gen uint64) (*l0Base, error) {
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return nil, err
	}
	n := mt.nextLabel
	b := &l0Base{
		gen:     gen,
		n:       n,
		offsets: make([]uint32, n+1),
		edges:   make([]uint32, 0, int(n)*ix.m0), // m0 is the layer-0 degree cap
		deleted: make([]uint64, n/64+1),
	}
	cur := rtx.NewCursor(ix.vadj)
	defer cur.Close()
	if err = cur.First(); err != nil {
		return nil, err
	}
	var nbrs []uint32
	var expected uint32 // next label we expect, to fill any gaps as empty
	for cur.Valid() {
		kb, kerr := cur.Key()
		if kerr != nil {
			return nil, kerr
		}
		label := binary.BigEndian.Uint32(kb)
		if label >= n {
			break // beyond the snapshot's high-water mark
		}
		// Fill gaps (missing labels) with empty runs at the current edge offset.
		for ; expected < label; expected++ {
			b.offsets[expected] = uint32(len(b.edges))
		}
		val, verr := cur.Value()
		if verr != nil {
			return nil, verr
		}
		_, deleted, herr := adjHeader(val)
		if herr != nil {
			return nil, herr
		}
		nbrs, err = adjNeighbors(val, 0, nbrs[:0])
		if err != nil {
			return nil, err
		}
		b.offsets[label] = uint32(len(b.edges))
		b.edges = append(b.edges, nbrs...)
		if deleted {
			b.deleted[label>>6] |= 1 << (label & 63)
		}
		expected = label + 1
		if err = cur.Next(); err != nil {
			return nil, err
		}
	}
	for ; expected <= n; expected++ { // trailing offsets (incl. the n+1 sentinel)
		b.offsets[expected] = uint32(len(b.edges))
	}
	return b, nil
}

// ---------------------------------------------------------------------------
// u32slab: label -> []uint32 run (the vecCache arena pattern, for adjacency)
// ---------------------------------------------------------------------------

// u32slab maps a sparse set of uint32 labels to variable-length []uint32 runs held
// in one contiguous arena, via an open-addressing probe table (lowbias32, like
// u32set / vecCache). It is built once per overlay and then read-only; all fields
// are pointer-free so it adds no GC scan cost.
type u32slab struct {
	key  []uint32 // probe table: label at this position
	off  []uint32 // run start offset into data
	ln   []uint32 // run length
	occ  []bool   // position occupied (labels include 0, so no zero sentinel)
	mask uint32
	data []uint32 // arena of concatenated runs
}

// newU32slab sizes the probe table for expected entries under a ~0.67 load factor.
func newU32slab(expected int) *u32slab {
	n := 16
	for n < expected+expected/2+1 {
		n <<= 1
	}
	return &u32slab{
		key:  make([]uint32, n),
		off:  make([]uint32, n),
		ln:   make([]uint32, n),
		occ:  make([]bool, n),
		mask: uint32(n - 1),
		data: make([]uint32, 0, expected*16),
	}
}

// put stores label -> a copy of nbrs. Idempotent on label: a repeated put updates
// the entry to the new run (the old run's bytes become dead in the arena, which is
// fine — the overlay is small and short-lived).
func (s *u32slab) put(label uint32, nbrs []uint32) {
	pos := hashU32(label) & s.mask
	for s.occ[pos] {
		if s.key[pos] == label {
			break // overwrite existing
		}
		pos = (pos + 1) & s.mask
	}
	s.off[pos] = uint32(len(s.data))
	s.ln[pos] = uint32(len(nbrs))
	s.data = append(s.data, nbrs...)
	s.key[pos] = label
	s.occ[pos] = true
}

// get returns label's run (a subslice of the arena, valid for the slab's lifetime)
// and whether it was present.
func (s *u32slab) get(label uint32) ([]uint32, bool) {
	pos := hashU32(label) & s.mask
	for s.occ[pos] {
		if s.key[pos] == label {
			off := s.off[pos]
			return s.data[off : off+s.ln[pos]], true
		}
		pos = (pos + 1) & s.mask
	}
	return nil, false
}

// ---------------------------------------------------------------------------
// Overlay
// ---------------------------------------------------------------------------

// l0Overlay holds the layer-0 changes since a base generation: rewritten/new
// adjacency in adj, newly-tombstoned labels in del (nil when no deletes). Built
// once and then immutable.
type l0Overlay struct {
	adj *u32slab
	del *u32set // nil if no deletes in this overlay
}

// buildOverlay re-reads each changed label's :adj at rtx's snapshot and records it
// in a fresh overlay. labels must be deduplicated. Reading at the reader's own
// snapshot is what keeps the overlay snapshot-aligned and abort-safe: the dirty
// ring only says WHICH labels to refresh, never their contents, so a label an
// aborted write happened to list simply re-reads to its true current adjacency.
func buildOverlay(rtx *btree.ReadTx, ix *Index, labels []uint32) (*l0Overlay, error) {
	ov := &l0Overlay{adj: newU32slab(len(labels))}
	var nbrs []uint32
	var kbuf []byte
	for _, label := range labels {
		kbuf = labelKey(kbuf, label)
		val, err := rtx.Get(ix.vadj, kbuf)
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				continue // not visible at this snapshot — leave it to the base
			}
			return nil, err
		}
		_, deleted, herr := adjHeader(val)
		if herr != nil {
			return nil, herr
		}
		nbrs, err = adjNeighbors(val, 0, nbrs[:0])
		if err != nil {
			return nil, err
		}
		ov.adj.put(label, nbrs)
		if deleted {
			if ov.del == nil {
				ov.del = &u32set{}
				ov.del.reset()
			}
			ov.del.visit(label)
		}
	}
	return ov, nil
}

// ---------------------------------------------------------------------------
// Mirror = base + overlay
// ---------------------------------------------------------------------------

// l0Mirror is the snapshot-aligned layer-0 view a search uses: an immutable CSR
// base plus an optional overlay of changes since the base's generation. Its gen
// matches the read snapshot's meta.l0Gen exactly.
type l0Mirror struct {
	gen  uint64
	base *l0Base
	ovl  *l0Overlay // nil when gen == base.gen (no changes since the base)
}

func (m *l0Mirror) neighbors(label uint32) []uint32 {
	if m.ovl != nil {
		if nb, ok := m.ovl.adj.get(label); ok {
			return nb
		}
	}
	return m.base.neighbors(label)
}

func (m *l0Mirror) isDeleted(label uint32) bool {
	if m.ovl != nil && m.ovl.del != nil && m.ovl.del.contains(label) {
		return true
	}
	return m.base.isDeleted(label)
}

// ---------------------------------------------------------------------------
// Dirty ring (in-memory changelog for incremental rebuild)
// ---------------------------------------------------------------------------

const (
	// overlayFoldCap bounds the overlay: past this many changed labels we fold by
	// rebuilding the base, so the overlay stays small (cheap lookups, cheap build).
	overlayFoldCap = 4096
	// dirtyBudget bounds the ring in total recorded labels; oldest entries drop
	// when exceeded (those generations then force a full rebuild, which is fine).
	dirtyBudget = 16384
	// thrashThreshold / thrashRetry: once full rebuilds happen this many times in a
	// row (e.g. cross-process churn the in-memory ring can't cover), bypass the
	// mirror and read layer 0 from the btree, retrying a rebuild every thrashRetry
	// calls so the mirror re-enables once the generation stabilises.
	thrashThreshold = 16
	thrashRetry     = 64
)

// dirtyEntry records the layer-0 labels a single write (one l0Gen step) changed.
type dirtyEntry struct {
	gen    uint64
	labels []uint32
}

// dirtyRing is an in-memory, per-Index changelog of layer-0 label changes, written
// by the single writer and read by searches building an overlay. Bounded by
// dirtyBudget labels; trimmed from the front.
type dirtyRing struct {
	mu        sync.Mutex
	entries   []dirtyEntry
	curLabels int
}

// record appends one write's changed labels (copied; the caller's slice is reused).
func (r *dirtyRing) record(gen uint64, labels []uint32) {
	if len(labels) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, dirtyEntry{gen: gen, labels: append([]uint32(nil), labels...)})
	r.curLabels += len(labels)
	drop := 0
	for r.curLabels > dirtyBudget && drop < len(r.entries)-1 {
		r.curLabels -= len(r.entries[drop].labels)
		drop++
	}
	if drop > 0 {
		r.entries = append(r.entries[:0], r.entries[drop:]...)
	}
}

// trimTo drops entries at or below gen (their changes are folded into a new base).
func (r *dirtyRing) trimTo(gen uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	drop := 0
	for drop < len(r.entries) && r.entries[drop].gen <= gen {
		r.curLabels -= len(r.entries[drop].labels)
		drop++
	}
	if drop > 0 {
		r.entries = append(r.entries[:0], r.entries[drop:]...)
	}
}

// coverage returns the deduplicated union of labels changed in (fromGen, toGen]
// and whether the ring fully covers that contiguous generation range. l0Gen steps
// by exactly one per write, so full coverage means every integer generation in the
// range is present (duplicates from aborted-then-retried writes are harmless — the
// overlay re-reads each label regardless).
func (r *dirtyRing) coverage(fromGen, toGen uint64) ([]uint32, bool) {
	if toGen <= fromGen {
		return nil, true
	}
	need := toGen - fromGen
	r.mu.Lock()
	defer r.mu.Unlock()
	if uint64(len(r.entries)) < need {
		return nil, false // can't possibly cover (one entry per generation at most)
	}
	present := make([]bool, need)
	var got uint64
	var labels []uint32
	for i := range r.entries {
		g := r.entries[i].gen
		if g <= fromGen || g > toGen {
			continue
		}
		if idx := g - fromGen - 1; !present[idx] {
			present[idx] = true
			got++
		}
		labels = append(labels, r.entries[i].labels...)
	}
	if got != need {
		return nil, false
	}
	dedupU32(&labels)
	return labels, true
}

// dedupU32 sorts s in place and removes duplicates. slices.Sort on uint32 is
// generic (no reflection); the changed-label sets are small.
func dedupU32(s *[]uint32) {
	a := *s
	if len(a) < 2 {
		return
	}
	slices.Sort(a)
	w := 1
	for i := 1; i < len(a); i++ {
		if a[i] != a[w-1] {
			a[w] = a[i]
			w++
		}
	}
	*s = a[:w]
}

// ---------------------------------------------------------------------------
// Index integration
// ---------------------------------------------------------------------------

// layer0Mirror returns a mirror whose gen matches mt.l0Gen, for the caller to use
// on this search. It tries, in order: (1) the published mirror if its gen already
// matches (read-after-read); (2) an incremental overlay over the shared base when
// the base is no newer than the snapshot and the dirty ring covers the gap; (3) a
// full base rebuild (cold path, also the thrash-bypass point). A returned nil
// mirror (with nil error) means "read layer 0 from the btree this call" — the
// thrash valve. The publish is best-effort CAS-to-newest; correctness comes from
// the returned, gen-matched value, never the shared pointer.
func (ix *Index) layer0Mirror(rtx *btree.ReadTx, mt *meta) (*l0Mirror, error) {
	g := mt.l0Gen
	if m := ix.l0pub.Load(); m != nil && m.gen == g {
		ix.fullStreak.Store(0)
		return m, nil
	}

	base := ix.l0base.Load()
	if base != nil && base.gen <= g {
		if labels, ok := ix.dirtyCoverage(base.gen, g); ok && len(labels) <= overlayFoldCap {
			var ovl *l0Overlay
			if len(labels) > 0 {
				var err error
				if ovl, err = buildOverlay(rtx, ix, labels); err != nil {
					return nil, err
				}
			}
			m := &l0Mirror{gen: g, base: base, ovl: ovl}
			ix.publishMirror(m)
			ix.fullStreak.Store(0)
			return m, nil
		}
	}

	// Cold path: full rebuild. Bypass under sustained thrash (retry periodically).
	if s := ix.fullStreak.Add(1); s > thrashThreshold && s%thrashRetry != 0 {
		return nil, nil
	}
	nb, err := buildL0Base(rtx, ix, g)
	if err != nil {
		return nil, err
	}
	ix.publishBase(nb)
	if ix.dirty != nil {
		ix.dirty.trimTo(g)
	}
	m := &l0Mirror{gen: g, base: nb}
	ix.publishMirror(m)
	return m, nil
}

// dirtyCoverage consults the ring (nil ring → never covers, forcing a rebuild).
func (ix *Index) dirtyCoverage(fromGen, toGen uint64) ([]uint32, bool) {
	if ix.dirty == nil {
		return nil, toGen <= fromGen
	}
	return ix.dirty.coverage(fromGen, toGen)
}

func (ix *Index) publishMirror(m *l0Mirror) {
	for {
		cur := ix.l0pub.Load()
		if cur != nil && cur.gen >= m.gen {
			return
		}
		if ix.l0pub.CompareAndSwap(cur, m) {
			return
		}
	}
}

func (ix *Index) publishBase(b *l0Base) {
	for {
		cur := ix.l0base.Load()
		if cur != nil && cur.gen >= b.gen {
			return
		}
		if ix.l0base.CompareAndSwap(cur, b) {
			return
		}
	}
}
