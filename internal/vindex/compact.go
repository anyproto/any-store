package vindex

import (
	"encoding/binary"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// Compact rebuilds the index from its live vectors, dropping every tombstone and
// re-densifying labels to [0, live). It runs entirely inside wtx: the index's
// namespaces are deleted and recreated, then the surviving vectors are
// re-inserted into a fresh HNSW graph. Because it is one transaction it is
// atomic — on commit the clean graph replaces the old one; on any error the tx
// rolls back with the original graph intact.
//
// It reclaims the storage that Delete/replace only tombstoned: dead :adj nodes,
// superseded :vec records, and stale :lbl entries. The rebuilt graph is also of
// higher quality than a tombstone-laden one (deleted nodes no longer route or
// dilute neighbour lists), so recall improves or holds.
//
// Returns a NEW *Index bound to the rebuilt namespaces; the caller must publish
// it in place of the old one and re-apply SetHybrid/SetVectorCache (the returned
// index defaults to plain btree mode). When there is nothing to reclaim (no
// tombstones and labels already dense) it is a no-op and returns an index over
// the existing graph unchanged.
//
// Relabeling safety: live nodes get fresh labels, so any label-keyed RAM cache
// (the layer-0 mirror, the vector tier) built before the compaction would be
// invalid. Recreating the namespaces moves their btree root pages, so the
// collection's reconcile reopens the Index with fresh handles and empty caches
// (the same root-moved path that handles a range index's drop+recreate). The
// old Index object is only ever read at the pre-compaction MVCC snapshot, where
// its caches are still correct, so no in-place cache versioning is needed.
func Compact(wtx *btree.WriteTx, prefix string, seed int64) (*Index, error) {
	rtx := &wtx.ReadTx
	old, err := OpenTx(rtx, prefix, seed)
	if err != nil {
		return nil, err
	}
	mt, err := old.readMeta(rtx)
	if err != nil {
		return nil, err
	}
	// Nothing to reclaim: no tombstones and the label space is already dense.
	if mt.deletedCount == 0 && int64(mt.nextLabel) == mt.count {
		return old, nil
	}

	// Snapshot the live (docId, vector) set. :doc maps every live document to its
	// current label and excludes tombstones by construction, so iterating it is
	// exactly the survivor set. Vectors are dequantized to float32 and copied out
	// (the cursor value and decode scratch are both reused per step).
	type liveVec struct {
		docID []byte
		vec   []float32
	}
	lives := make([]liveVec, 0, mt.count)
	cur := rtx.NewCursor(old.vdoc)
	defer cur.Close()
	if err = cur.First(); err != nil {
		return nil, err
	}
	var kbuf []byte
	tmp := make([]float32, old.dim)
	for cur.Valid() {
		docID, kerr := cur.Key()
		if kerr != nil {
			return nil, kerr
		}
		lb, verr := cur.Value()
		if verr != nil {
			return nil, verr
		}
		label := binary.LittleEndian.Uint32(lb)
		kbuf = labelKey(kbuf, label)
		vbytes, gerr := rtx.Get(old.vvec, kbuf)
		if gerr != nil {
			return nil, gerr
		}
		var vec []float32
		if old.quant == QuantNone {
			vec = append([]float32(nil), bytesAsF32(vbytes, old.dim)...)
		} else {
			if v, ok := decodeVecInto(vbytes, old.dim, old.quant, tmp); ok {
				vec = append([]float32(nil), v...)
			}
		}
		lives = append(lives, liveVec{docID: append([]byte(nil), docID...), vec: vec})
		if err = cur.Next(); err != nil {
			return nil, err
		}
	}

	// Drop and recreate the namespaces. DeleteNamespace removes the master-table
	// entry (dirty-visible in this tx), so the subsequent Create of the same name
	// succeeds and lands on fresh root pages.
	for _, name := range nsNames(prefix) {
		if derr := wtx.DeleteNamespace(name); derr != nil {
			return nil, derr
		}
	}
	ix, err := Create(wtx, prefix, Params{
		Dim:            mt.dim,
		Metric:         mt.metric,
		M:              mt.m,
		EfConstruction: mt.efC,
		EfSearch:       mt.efS,
		Ml:             mt.ml,
		Quantization:   mt.quant,
	}, seed)
	if err != nil {
		return nil, err
	}
	for _, lv := range lives {
		if err = ix.Insert(wtx, lv.docID, lv.vec); err != nil {
			return nil, err
		}
	}
	return ix, nil
}

// MetaRoot returns the btree root page of the index's meta namespace. The
// collection compares it against the on-disk namespace to detect that a peer
// compaction recreated the index (root moved) and the Index must be reopened.
func (ix *Index) MetaRoot() uint32 { return ix.vmeta.RootPage() }
