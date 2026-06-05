package vector

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// paged_persist.go gives PagedHNSW a durable, reopenable on-disk form using the
// vector/adjacency SPLIT recommended in DELETE_UPDATE.md §4:
//
//	<name>:topo   one small record per node: key + level + per-layer neighbours
//	<name>:vec    one record per node: the raw vector (paged during search)
//	<name>:pmeta  index parameters + entry point
//
// OpenPaged loads ONLY the :topo records into RAM (the navigable graph) and
// leaves the vectors in :vec, so a reopened index keeps Option B's small
// resident footprint — the topology survives a restart, the vectors stay on
// disk. This is what lets vectorbench build once and then reopen-and-search a
// cold on-disk index without rebuilding.

// newPagedHNSW builds the PagedHNSW wrapper (shared by BuildPagedFromFlat and
// OpenPaged): graph topology in g, vectors read from vecNS.
func newPagedHNSW(g *FlatHNSW, db *btree.DB, vecNS *btree.Namespace, metric Metric) *PagedHNSW {
	sf := make([]float32, g.dim)
	return &PagedHNSW{
		g:        g,
		db:       db,
		vecNS:    vecNS,
		dim:      g.dim,
		dist:     metric.DistanceFor(),
		scratchF: sf,
		scratchB: unsafe.Slice((*byte)(unsafe.Pointer(&sf[0])), g.dim*4),
	}
}

// AttachPaged wraps an in-RAM FlatHNSW with an existing on-disk :vec namespace
// (already populated, e.g. by PersistPaged) without writing anything. The
// returned index can do pure paged Search AND the route-in-RAM SearchHybrid,
// because its topology source g still holds the full vectors in RAM.
func AttachPaged(g *FlatHNSW, db *btree.DB, name string, metric Metric) (*PagedHNSW, error) {
	vecNS, err := db.GetNamespace(name + ":vec")
	if err != nil {
		return nil, err
	}
	return newPagedHNSW(g, db, vecNS, metric), nil
}

func ensureNS(wtx *btree.WriteTx, name string) (*btree.Namespace, error) {
	ns, err := wtx.GetNamespace(name)
	if err == nil {
		return ns, nil
	}
	if !errors.Is(err, btree.ErrNamespaceNotFound) {
		return nil, err
	}
	return wtx.CreateNamespace(name)
}

// PersistPaged writes g to db under name in the split on-disk form (:topo, :vec,
// :pmeta) in a single write transaction.
func PersistPaged(g *FlatHNSW, db *btree.DB, name string, metric Metric) error {
	wtx, err := db.BeginWrite()
	if err != nil {
		return err
	}
	topoNS, err := ensureNS(wtx, name+":topo")
	if err != nil {
		_ = wtx.Rollback()
		return err
	}
	vecNS, err := ensureNS(wtx, name+":vec")
	if err != nil {
		_ = wtx.Rollback()
		return err
	}
	pmetaNS, err := ensureNS(wtx, name+":pmeta")
	if err != nil {
		_ = wtx.Rollback()
		return err
	}

	var keyBuf [8]byte
	var topoBuf []byte
	nbrs := make([][]uint32, 0, 8)
	for id := uint32(0); id < uint32(len(g.keys)); id++ {
		binary.BigEndian.PutUint64(keyBuf[:], uint64(id))
		// vector record
		if err := wtx.Put(vecNS, keyBuf[:], f32bytes(g.vectorAt(id))); err != nil {
			_ = wtx.Rollback()
			return err
		}
		// topology record
		lvl := g.level[id]
		nbrs = nbrs[:0]
		for lc := int32(0); lc <= lvl; lc++ {
			nbrs = append(nbrs, g.neighbors(id, lc))
		}
		topoBuf = encodeTopo(topoBuf[:0], g.keys[id], lvl, nbrs)
		if err := wtx.Put(topoNS, keyBuf[:], topoBuf); err != nil {
			_ = wtx.Rollback()
			return err
		}
	}

	meta := indexMeta{
		dim: g.dim, metric: metric,
		M: g.M, M0: g.M0, Ml: g.Ml,
		EfSearch: g.EfSearch, EfConstruction: g.EfConstruction,
		entryID: g.entryID, topLayer: g.topLayer,
		count: uint32(len(g.keys)),
	}
	if err := wtx.Put(pmetaNS, []byte(btreeMetaKey), encodeMeta(nil, meta)); err != nil {
		_ = wtx.Rollback()
		return err
	}
	return wtx.Commit()
}

// PagedExists reports whether a persisted paged index named name is present.
func PagedExists(db *btree.DB, name string) bool {
	ns, err := db.GetNamespace(name + ":pmeta")
	if err != nil {
		return false
	}
	rtx, err := db.BeginRead()
	if err != nil {
		return false
	}
	defer rtx.Rollback()
	_, err = rtx.Get(ns, []byte(btreeMetaKey))
	return err == nil
}

// OpenPaged reopens a persisted paged index, loading only its topology into RAM.
func OpenPaged(db *btree.DB, name string) (*PagedHNSW, error) {
	pmetaNS, err := db.GetNamespace(name + ":pmeta")
	if err != nil {
		return nil, err
	}
	topoNS, err := db.GetNamespace(name + ":topo")
	if err != nil {
		return nil, err
	}
	vecNS, err := db.GetNamespace(name + ":vec")
	if err != nil {
		return nil, err
	}

	rtx, err := db.BeginRead()
	if err != nil {
		return nil, err
	}
	defer rtx.Rollback()

	mb, err := rtx.Get(pmetaNS, []byte(btreeMetaKey))
	if err != nil {
		return nil, err
	}
	meta, err := decodeMeta(mb)
	if err != nil {
		return nil, err
	}

	g := NewFlatHNSW(meta.dim, meta.metric, 0)
	g.M, g.M0, g.Ml = meta.M, meta.M0, meta.Ml
	g.EfSearch, g.EfConstruction = meta.EfSearch, meta.EfConstruction

	cur := rtx.NewCursor(topoNS)
	defer cur.Close()
	if err := cur.First(); err != nil {
		return nil, err
	}
	for cur.Valid() {
		v, err := cur.Value()
		if err != nil {
			return nil, err
		}
		key, level, nbrs, err := decodeTopo(v)
		if err != nil {
			return nil, err
		}
		g.appendTopo(key, level, nbrs)
		if err := cur.Next(); err != nil {
			return nil, err
		}
	}
	if meta.count > 0 {
		g.setEntry(meta.entryID, meta.topLayer)
	}
	return newPagedHNSW(g, db, vecNS, meta.metric), nil
}

// PhysLen returns the number of nodes (live + tombstoned) in the loaded graph.
func (p *PagedHNSW) PhysLen() int { return len(p.g.keys) }

// encodeTopo: key(8) | level(4) | per layer [count(2) ids(count*4)].
func encodeTopo(buf []byte, key uint64, level int32, nbrs [][]uint32) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], key)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(level))
	buf = append(buf, tmp[:4]...)
	for lc := int32(0); lc <= level; lc++ {
		ids := nbrs[lc]
		binary.LittleEndian.PutUint16(tmp[:2], uint16(len(ids)))
		buf = append(buf, tmp[:2]...)
		for _, id := range ids {
			binary.LittleEndian.PutUint32(tmp[:4], id)
			buf = append(buf, tmp[:4]...)
		}
	}
	return buf
}

func decodeTopo(b []byte) (key uint64, level int32, nbrs [][]uint32, err error) {
	if len(b) < 12 {
		return 0, 0, nil, errors.New("vector: topo record too short")
	}
	key = binary.LittleEndian.Uint64(b)
	level = int32(binary.LittleEndian.Uint32(b[8:]))
	off := 12
	nbrs = make([][]uint32, level+1)
	for lc := int32(0); lc <= level; lc++ {
		if off+2 > len(b) {
			return 0, 0, nil, errors.New("vector: truncated topo neighbours")
		}
		cnt := int(binary.LittleEndian.Uint16(b[off:]))
		off += 2
		ids := make([]uint32, cnt)
		for i := 0; i < cnt; i++ {
			ids[i] = binary.LittleEndian.Uint32(b[off:])
			off += 4
		}
		nbrs[lc] = ids
	}
	return key, level, nbrs, nil
}
