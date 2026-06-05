package vindex

import "github.com/anyproto/any-store/v2/internal/btree"

// Stats summarises a vector index: graph parameters, node counts (live vs
// tombstoned), and the physical size of each backing namespace.
type Stats struct {
	Dim          int
	Metric       Metric
	Quantization Quantization
	M            int
	M0           int
	EfSearch     int
	NodeCount    int // physical nodes ever allocated (incl. tombstones)
	LiveCount    int // non-deleted nodes
	DeletedCount int // tombstones (compaction reclaims these)
	TopLayer     int

	Vec  btree.NamespaceSize // vectors (immutable)
	Adj  btree.NamespaceSize // graph adjacency
	Doc  btree.NamespaceSize // docId -> label
	Lbl  btree.NamespaceSize // label -> docId
	Meta btree.NamespaceSize // params + entry point
}

// Stats reads the index meta and walks each namespace to produce a storage and
// occupancy summary for the index as seen by rtx's snapshot.
func (ix *Index) Stats(rtx *btree.ReadTx) (Stats, error) {
	mt, err := ix.readMeta(rtx)
	if err != nil {
		return Stats{}, err
	}
	s := Stats{
		Dim:          mt.dim,
		Metric:       mt.metric,
		Quantization: mt.quant,
		M:            mt.m,
		M0:           mt.m0,
		EfSearch:     mt.efS,
		NodeCount:    int(mt.nextLabel),
		LiveCount:    int(mt.count),
		DeletedCount: int(mt.deletedCount),
		TopLayer:     int(mt.topLayer),
	}
	for _, p := range []struct {
		ns  *btree.Namespace
		dst *btree.NamespaceSize
	}{
		{ix.vvec, &s.Vec},
		{ix.vadj, &s.Adj},
		{ix.vdoc, &s.Doc},
		{ix.vlbl, &s.Lbl},
		{ix.vmeta, &s.Meta},
	} {
		sz, err := rtx.NamespaceSize(p.ns)
		if err != nil {
			return Stats{}, err
		}
		*p.dst = sz
	}
	return s, nil
}
