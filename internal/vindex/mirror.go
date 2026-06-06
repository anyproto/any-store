package vindex

import (
	"encoding/binary"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// l0Mirror is an immutable RAM snapshot of HNSW layer 0 — each node's layer-0
// neighbour list plus the set of tombstoned nodes — captured at a specific
// meta.l0Gen. Hybrid-mode read searches consult it to serve layer-0 neighbour
// and deleted lookups without btree reads (the bulk of search I/O).
//
// It is rebuilt by scanning the :adj namespace whenever a reader's snapshot
// l0Gen no longer matches (an in-process write or a peer commit). Because the
// scan runs on the reader's own snapshot, the rebuilt mirror is exactly aligned
// to that snapshot — never ahead of it (which would dereference labels whose
// vector isn't visible yet) and never silently behind. The btree remains the
// source of truth; deleting the mirror and rebuilding always reproduces it.
//
// Adjacency-only by design (the int8 vector tier is a separate opt-in): vectors
// are still read from the btree, so distances are computed exactly as in pure
// btree mode and recall is identical.
type l0Mirror struct {
	gen     uint64
	adj     map[uint32][]uint32 // label -> layer-0 neighbours
	deleted map[uint32]struct{} // tombstoned labels (sparse: only deletions)
}

func (m *l0Mirror) neighbors(label uint32) []uint32 {
	return m.adj[label]
}

func (m *l0Mirror) isDeleted(label uint32) bool {
	_, ok := m.deleted[label]
	return ok
}

// buildL0Mirror scans the adjacency namespace at rtx's snapshot and materialises
// the layer-0 graph + tombstone set, tagged with gen.
func buildL0Mirror(rtx *btree.ReadTx, ix *Index, gen uint64) (*l0Mirror, error) {
	m := &l0Mirror{gen: gen, adj: make(map[uint32][]uint32), deleted: make(map[uint32]struct{})}
	cur := rtx.NewCursor(ix.vadj)
	defer cur.Close()
	if err := cur.First(); err != nil {
		return nil, err
	}
	var nbrs []uint32
	for cur.Valid() {
		kb, err := cur.Key()
		if err != nil {
			return nil, err
		}
		label := binary.BigEndian.Uint32(kb)
		val, err := cur.Value()
		if err != nil {
			return nil, err
		}
		_, deleted, err := adjHeader(val)
		if err != nil {
			return nil, err
		}
		nbrs, err = adjNeighbors(val, 0, nbrs[:0])
		if err != nil {
			return nil, err
		}
		m.adj[label] = append([]uint32(nil), nbrs...)
		if deleted {
			m.deleted[label] = struct{}{}
		}
		if err := cur.Next(); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// layer0Mirror returns a mirror whose gen matches mt.l0Gen, building one if the
// cached mirror is absent or stale. The returned mirror is the one the caller
// must use for this search (its gen == the read snapshot's l0Gen). The cache
// publish is best-effort (CAS to the newest gen); a losing CAS never affects
// correctness because the caller uses the returned, gen-matched value — not the
// shared pointer.
func (ix *Index) layer0Mirror(rtx *btree.ReadTx, mt *meta) (*l0Mirror, error) {
	if m := ix.l0pub.Load(); m != nil && m.gen == mt.l0Gen {
		return m, nil
	}
	nm, err := buildL0Mirror(rtx, ix, mt.l0Gen)
	if err != nil {
		return nil, err
	}
	for {
		cur := ix.l0pub.Load()
		if cur != nil && cur.gen >= nm.gen {
			break // a concurrent build already published an equal/newer mirror
		}
		if ix.l0pub.CompareAndSwap(cur, nm) {
			break
		}
	}
	return nm, nil
}
