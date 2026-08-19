package anystore

import (
	"context"
	"errors"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
)

// IndexSketchInspector exposes decoded sketch state for diagnostic and test
// use. It intentionally lives outside the DB interface — consumers opt in via
// type assertion so the main public API stays focused.
type IndexSketchInspector interface {
	InspectIndexSketch(ctx context.Context, collName, indexName string) (IndexSketchInfo, error)
}

// IndexSketchInfo is a read-only snapshot of an index's persisted sketch.
type IndexSketchInfo struct {
	Size     int      // buckets per level; normally qplanner.DefaultSketchSize
	Levels   int      // number of prefix levels (= number of index fields)
	DocCount uint64   // total documents tracked by this index
	Buckets  []uint64 // per-bucket frequency counts, len == Size*Levels; level L is Buckets[L*Size:(L+1)*Size]
}

// InspectIndexSketch returns the decoded on-disk sketch for the named index.
// Returns ErrIndexNotFound if no sketch bytes exist for that (collName, indexName).
// This reads the persisted sketch, not any in-memory state held by an open
// collection handle — callers that want a live view should close and reopen
// the DB, or ensure no writes are in flight.
func (db *db) InspectIndexSketch(ctx context.Context, collName, indexName string) (IndexSketchInfo, error) {
	var info IndexSketchInfo
	err := db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		data, err := tx.AppendValue(db.systemNS, sketchKey(collName, indexName), nil)
		if err != nil {
			if errors.Is(err, btree.ErrKeyNotFound) {
				return ErrIndexNotFound
			}
			return err
		}
		// Constructed at 1 level; UnmarshalBinary adopts the blob's real shape.
		sk := qplanner.NewIndexSketch(qplanner.DefaultSketchSize, 1)
		sk.UnmarshalBinary(data)
		info.Size = sk.Size
		info.Levels = sk.NumLevels()
		info.DocCount = sk.GetDocCount()
		info.Buckets = make([]uint64, len(sk.Buckets))
		copy(info.Buckets, sk.Buckets)
		return nil
	})
	if err != nil {
		return IndexSketchInfo{}, err
	}
	return info, nil
}
