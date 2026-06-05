package vector

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// BtreeHNSW lands the arena HNSW on top of any-store's embedded btree engine.
// It keeps the working graph in memory as a FlatHNSW (so search runs at
// in-memory speed) while durably persisting every node — vector + per-layer
// adjacency — into two btree namespaces:
//
//	<name>:meta   one record with the index parameters + entry point
//	<name>:nodes  one record per node, keyed by the dense id (8-byte big-endian)
//
// Because ids are dense and big-endian-keyed, a forward cursor over the nodes
// namespace yields them in id order, which is exactly the order FlatHNSW needs
// to rebuild its arenas on reload (no re-construction, no re-randomised levels).
//
// Writes are buffered: Add mutates the in-memory graph and records which nodes
// changed; Flush persists the dirty set + meta in a single write transaction.
// This batches the (relatively expensive) durable writes the same way a bulk
// import would.
//
// BtreeHNSW is not safe for concurrent Add/Flush; serialise those externally.
// Concurrent Search is safe (it only reads the in-memory FlatHNSW).
type BtreeHNSW struct {
	db      *btree.DB
	nodesNS *btree.Namespace
	metaNS  *btree.Namespace
	flat    *FlatHNSW
	metric  Metric
	dim     int

	mu    sync.Mutex
	dirty map[uint32]struct{}
}

const (
	btreeMetaVersion = 1
	btreeMetaKey     = "meta"
)

// OpenBtreeHNSW opens (or creates) a persistent index named name inside db. If
// the namespaces already hold an index its parameters and dim are taken from
// the stored meta and the dim/metric arguments are only used when creating a
// fresh index.
func OpenBtreeHNSW(db *btree.DB, name string, dim int, m Metric) (*BtreeHNSW, error) {
	b := &BtreeHNSW{db: db, metric: m, dim: dim, dirty: make(map[uint32]struct{})}

	// Ensure both namespaces exist.
	wtx, err := db.BeginWrite()
	if err != nil {
		return nil, err
	}
	for _, ns := range []string{name + ":meta", name + ":nodes"} {
		if _, err := wtx.GetNamespace(ns); err != nil {
			if !errors.Is(err, btree.ErrNamespaceNotFound) {
				_ = wtx.Rollback()
				return nil, err
			}
			if _, err := wtx.CreateNamespace(ns); err != nil {
				_ = wtx.Rollback()
				return nil, err
			}
		}
	}
	if err := wtx.Commit(); err != nil {
		return nil, err
	}

	if b.metaNS, err = db.GetNamespace(name + ":meta"); err != nil {
		return nil, err
	}
	if b.nodesNS, err = db.GetNamespace(name + ":nodes"); err != nil {
		return nil, err
	}

	if err := b.load(); err != nil {
		return nil, err
	}
	b.flat.notifyDirty = func(id uint32) { b.dirty[id] = struct{}{} }
	return b, nil
}

// load reconstructs the in-memory FlatHNSW from the persisted records.
func (b *BtreeHNSW) load() error {
	rtx, err := b.db.BeginRead()
	if err != nil {
		return err
	}
	defer rtx.Rollback()

	metaBytes, err := rtx.Get(b.metaNS, []byte(btreeMetaKey))
	if errors.Is(err, btree.ErrKeyNotFound) {
		// fresh index
		b.flat = NewFlatHNSW(b.dim, b.metric, 0)
		return nil
	}
	if err != nil {
		return err
	}
	meta, err := decodeMeta(metaBytes)
	if err != nil {
		return err
	}
	b.dim, b.metric = meta.dim, meta.metric

	flat := NewFlatHNSW(meta.dim, meta.metric, 0)
	flat.M, flat.M0 = meta.M, meta.M0
	flat.Ml = meta.Ml
	flat.EfSearch, flat.EfConstruction = meta.EfSearch, meta.EfConstruction

	// Iterate nodes in id order (big-endian keys => ascending == id order).
	cur := rtx.NewCursor(b.nodesNS)
	defer cur.Close()
	if err := cur.First(); err != nil {
		return err
	}
	var loaded uint32
	for cur.Valid() {
		val, err := cur.Value()
		if err != nil {
			return err
		}
		key, level, vec, nbrs, err := decodeNode(val, meta.dim)
		if err != nil {
			return err
		}
		flat.appendRaw(key, level, vec, nbrs)
		loaded++
		if err := cur.Next(); err != nil {
			return err
		}
	}
	if loaded != meta.count {
		return fmt.Errorf("vector: persisted node count mismatch: meta=%d loaded=%d", meta.count, loaded)
	}
	if meta.count > 0 {
		flat.setEntry(meta.entryID, meta.topLayer)
	}
	b.flat = flat
	return nil
}

// Add inserts a vector into the in-memory graph and records the touched nodes
// for the next Flush. It does not write to disk.
func (b *BtreeHNSW) Add(key uint64, vec []float32) {
	b.flat.Add(key, vec)
}

// Flush durably writes all nodes touched since the last Flush, plus the index
// meta, in a single btree write transaction.
func (b *BtreeHNSW) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.dirty) == 0 {
		return nil
	}

	wtx, err := b.db.BeginWrite()
	if err != nil {
		return err
	}

	var keyBuf [8]byte
	var valBuf []byte
	for id := range b.dirty {
		key, level, vec, nbrs := b.flat.snapshot(id)
		binary.BigEndian.PutUint64(keyBuf[:], uint64(id))
		valBuf = encodeNode(valBuf[:0], key, level, vec, nbrs)
		if err := wtx.Put(b.nodesNS, keyBuf[:], valBuf); err != nil {
			_ = wtx.Rollback()
			return err
		}
	}

	meta := indexMeta{
		dim: b.dim, metric: b.metric,
		M: b.flat.M, M0: b.flat.M0, Ml: b.flat.Ml,
		EfSearch: b.flat.EfSearch, EfConstruction: b.flat.EfConstruction,
		entryID: b.flat.entryID, topLayer: b.flat.topLayer,
		count: uint32(len(b.flat.keys)),
	}
	if err := wtx.Put(b.metaNS, []byte(btreeMetaKey), encodeMeta(nil, meta)); err != nil {
		_ = wtx.Rollback()
		return err
	}
	if err := wtx.Commit(); err != nil {
		return err
	}
	b.dirty = make(map[uint32]struct{})
	return nil
}

// Search runs an in-memory ANN search over the working graph.
func (b *BtreeHNSW) Search(query []float32, k int) []SearchResult {
	return b.flat.Search(query, k)
}

// Len returns the number of indexed vectors.
func (b *BtreeHNSW) Len() int { return b.flat.Len() }

// SetEf adjusts the query-time candidate list size.
func (b *BtreeHNSW) SetEf(ef int) { b.flat.EfSearch = ef }

// ---------------------------------------------------------------------------
// Serialisation
// ---------------------------------------------------------------------------

type indexMeta struct {
	dim            int
	metric         Metric
	M, M0          int
	Ml             float64
	EfSearch       int
	EfConstruction int
	entryID        uint32
	topLayer       int32
	count          uint32
}

func encodeMeta(buf []byte, m indexMeta) []byte {
	var tmp [4]byte
	put32 := func(v uint32) { binary.LittleEndian.PutUint32(tmp[:], v); buf = append(buf, tmp[:]...) }
	put32(btreeMetaVersion)
	put32(uint32(m.dim))
	buf = append(buf, byte(m.metric))
	put32(uint32(m.M))
	put32(uint32(m.M0))
	var f8 [8]byte
	binary.LittleEndian.PutUint64(f8[:], math.Float64bits(m.Ml))
	buf = append(buf, f8[:]...)
	put32(uint32(m.EfSearch))
	put32(uint32(m.EfConstruction))
	put32(m.entryID)
	put32(uint32(m.topLayer))
	put32(m.count)
	return buf
}

func decodeMeta(b []byte) (indexMeta, error) {
	var m indexMeta
	if len(b) < 4 {
		return m, errors.New("vector: meta too short")
	}
	off := 0
	get32 := func() uint32 { v := binary.LittleEndian.Uint32(b[off:]); off += 4; return v }
	if v := get32(); v != btreeMetaVersion {
		return m, fmt.Errorf("vector: unsupported meta version %d", v)
	}
	m.dim = int(get32())
	m.metric = Metric(b[off])
	off++
	m.M = int(get32())
	m.M0 = int(get32())
	m.Ml = math.Float64frombits(binary.LittleEndian.Uint64(b[off:]))
	off += 8
	m.EfSearch = int(get32())
	m.EfConstruction = int(get32())
	m.entryID = get32()
	m.topLayer = int32(get32())
	m.count = get32()
	return m, nil
}

// encodeNode lays out: key(8) | level(4) | vector(dim*4) | per-layer [count(2) ids(count*4)].
func encodeNode(buf []byte, key uint64, level int32, vec []float32, nbrs [][]uint32) []byte {
	var tmp [8]byte
	binary.LittleEndian.PutUint64(tmp[:], key)
	buf = append(buf, tmp[:]...)
	binary.LittleEndian.PutUint32(tmp[:4], uint32(level))
	buf = append(buf, tmp[:4]...)
	for _, f := range vec {
		binary.LittleEndian.PutUint32(tmp[:4], math.Float32bits(f))
		buf = append(buf, tmp[:4]...)
	}
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

func decodeNode(b []byte, dim int) (key uint64, level int32, vec []float32, nbrs [][]uint32, err error) {
	if len(b) < 12+dim*4 {
		return 0, 0, nil, nil, errors.New("vector: node record too short")
	}
	off := 0
	key = binary.LittleEndian.Uint64(b[off:])
	off += 8
	level = int32(binary.LittleEndian.Uint32(b[off:]))
	off += 4
	vec = make([]float32, dim)
	for i := range vec {
		vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(b[off:]))
		off += 4
	}
	nbrs = make([][]uint32, level+1)
	for lc := int32(0); lc <= level; lc++ {
		if off+2 > len(b) {
			return 0, 0, nil, nil, errors.New("vector: truncated node neighbours")
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
	return key, level, vec, nbrs, nil
}
