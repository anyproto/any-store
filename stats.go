package anystore

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/klauspost/compress/s2"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/qplanner"
)

// IndexStats describes the storage footprint and sketch state of a single index.
type IndexStats struct {
	// Name is the index name.
	Name string

	// Fields are the indexed fields, with leading '-' marking descending order.
	Fields []string

	// Unique reports whether the index enforces a unique constraint.
	Unique bool

	// Sparse reports whether the index skips documents missing the fields.
	Sparse bool

	// EntryCount is the number of entries (key/value pairs) in the index B-tree.
	EntryCount int

	// PayloadBytes is the sum of key+value byte lengths across all entries.
	PayloadBytes int

	// SizeBytes is the physical on-disk size of the index B-tree, computed as
	// its page count (including overflow pages) times the database page size.
	SizeBytes int

	// SketchDocCount is the document count tracked by the index sketch.
	SketchDocCount uint64

	// SketchSize is the number of buckets in the index sketch.
	SketchSize int

	// SketchDistribution summarizes the sketch's bucket frequency distribution.
	SketchDistribution qplanner.SketchDistribution
}

// VectorIndexStats describes a vector (HNSW) index: its parameters, node
// occupancy, and the physical size of its backing namespaces.
type VectorIndexStats struct {
	// Name is the index name.
	Name string

	// Field is the indexed embedding field path.
	Field string

	// Dim is the embedding dimension; Metric is the distance metric.
	Dim    int
	Metric string
	// Mode is the index strategy ("btree" | "hybrid" | "brute").
	Mode string
	// Quantization is the stored vector format ("none" | "int8").
	Quantization string

	// M / EfSearch are the HNSW graph parameters.
	M        int
	EfSearch int

	// NodeCount is the number of nodes ever allocated (including tombstones);
	// LiveCount excludes tombstones; DeletedCount is the tombstone count. A high
	// DeletedCount/NodeCount ratio signals that a compaction/rebuild is due.
	NodeCount    int
	LiveCount    int
	DeletedCount int

	// VectorBytes / GraphBytes / MappingBytes / MetaBytes are the on-disk sizes
	// of the vector, adjacency, docId<->label, and meta namespaces respectively.
	VectorBytes  int
	GraphBytes   int
	MappingBytes int
	MetaBytes    int

	// SizeBytes is the total physical size of the index (sum of the above).
	SizeBytes int
}

// CollectionStats describes the storage footprint of a collection: its
// documents, compression effectiveness and per-index sizes.
type CollectionStats struct {
	// Name is the collection name.
	Name string

	// DocCount is the number of documents in the collection.
	DocCount int

	// StoredDocsBytes is the sum of stored document value bytes. When
	// compression is enabled this counts the compressed form.
	StoredDocsBytes int

	// UncompressedDocsBytes is the sum of document value bytes after
	// decompression. It equals StoredDocsBytes when nothing is compressed.
	UncompressedDocsBytes int

	// CompressionEnabled reports whether compression is active for this
	// collection. Individual documents below anyenc.CompressMinSize are stored
	// uncompressed even when it is enabled.
	CompressionEnabled bool

	// CompressionRatio is UncompressedDocsBytes/StoredDocsBytes. It is 1.0 when
	// nothing is compressed and grows above 1.0 as compression saves space.
	CompressionRatio float64

	// DocsSizeBytes is the physical on-disk size of the collection's document
	// B-tree (page count including overflow pages times the page size).
	DocsSizeBytes int

	// IndexesSizeBytes is the sum of SizeBytes across all range (B-tree) indexes.
	IndexesSizeBytes int

	// VectorIndexesSizeBytes is the sum of SizeBytes across all vector indexes.
	VectorIndexesSizeBytes int

	// TotalSizeBytes is DocsSizeBytes + IndexesSizeBytes + VectorIndexesSizeBytes.
	TotalSizeBytes int

	// Indexes holds per-(range-)index statistics.
	Indexes []IndexStats

	// VectorIndexes holds per-vector-index statistics.
	VectorIndexes []VectorIndexStats
}

// Stats walks the collection's document and index B-trees and reports their
// storage footprint, compression effectiveness and per-index sketch state.
//
// It performs a full scan of the collection and is therefore O(documents +
// index entries); it is intended for diagnostics, not hot paths. All figures
// are read within a single read transaction and are mutually consistent.
func (c *collection) Stats(ctx context.Context) (stats CollectionStats, err error) {
	c.mu.Lock()
	name := c.name
	indexes := append([]*index(nil), c.loadIndexes()...)
	vindexes := append([]*vectorIndex(nil), c.loadVectorIndexes()...)
	c.mu.Unlock()

	stats.Name = name
	stats.CompressionEnabled = !c.compressionDisabled()
	pageSize := int(c.db.btreeDB.PageSize())

	err = c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		// Documents: scan the collection B-tree summing stored and
		// uncompressed value sizes.
		cursor := tx.NewCursor(c.ns)
		defer cursor.Close()
		if cErr := cursor.First(); cErr != nil {
			return cErr
		}
		for cursor.Valid() {
			val, vErr := cursor.Value()
			if vErr != nil {
				return vErr
			}
			stats.DocCount++
			stats.StoredDocsBytes += len(val)
			uncompressed, uErr := uncompressedDocLen(val)
			if uErr != nil {
				return uErr
			}
			stats.UncompressedDocsBytes += uncompressed
			if nErr := cursor.Next(); nErr != nil {
				return nErr
			}
		}

		// Physical size of the document B-tree.
		docSize, dErr := tx.NamespaceSize(c.ns)
		if dErr != nil {
			return dErr
		}
		stats.DocsSizeBytes = docSize.TotalPages() * pageSize

		// Per-index statistics.
		for _, idx := range indexes {
			idxSize, iErr := tx.NamespaceSize(idx.ns)
			if iErr != nil {
				return iErr
			}
			is := IndexStats{
				Name:         idx.info.Name,
				Fields:       append([]string(nil), idx.info.Fields...),
				Unique:       idx.info.Unique,
				Sparse:       idx.info.Sparse,
				EntryCount:   idxSize.Entries,
				PayloadBytes: idxSize.PayloadBytes,
				SizeBytes:    idxSize.TotalPages() * pageSize,
			}
			if s := idx.loadPubSketch(); s != nil {
				is.SketchDocCount = s.GetDocCount()
				is.SketchSize = s.Size
				is.SketchDistribution = s.Distribution()
			}
			stats.IndexesSizeBytes += is.SizeBytes
			stats.Indexes = append(stats.Indexes, is)
		}

		// Per-vector-index statistics.
		for _, vi := range vindexes {
			// IVF-PQ backend: sum its btree namespaces (codes + vectors + maps).
			if vi.ivf != nil {
				vs, vErr := vi.ivf.Stats(tx)
				if vErr != nil {
					return vErr
				}
				pages := func(ns btree.NamespaceSize) int { return ns.TotalPages() * pageSize }
				vstat := VectorIndexStats{
					Name:         vi.info.Name,
					Field:        vi.info.Vector.Field,
					Dim:          vs.Dim,
					Metric:       vi.info.Vector.Metric.toVindex().String(),
					Mode:         vi.mode.String(),
					Quantization: vi.info.Vector.Quantization.toVindex().String(),
					M:            vs.M,
					NodeCount:    int(vs.Count),
					LiveCount:    int(vs.Count),
					VectorBytes:  pages(vs.Vec),
					GraphBytes:   pages(vs.Cell) + pages(vs.CB), // codes + codebooks
					MappingBytes: pages(vs.Doc) + pages(vs.Lbl),
					MetaBytes:    pages(vs.Meta),
				}
				vstat.SizeBytes = vstat.VectorBytes + vstat.GraphBytes + vstat.MappingBytes + vstat.MetaBytes
				stats.VectorIndexesSizeBytes += vstat.SizeBytes
				stats.VectorIndexes = append(stats.VectorIndexes, vstat)
				continue
			}
			// Brute-force mode keeps no index data: report metadata only, zero bytes.
			if vi.ix == nil {
				stats.VectorIndexes = append(stats.VectorIndexes, VectorIndexStats{
					Name:         vi.info.Name,
					Field:        vi.info.Vector.Field,
					Dim:          vi.dim,
					Metric:       vi.info.Vector.Metric.toVindex().String(),
					Mode:         vi.mode.String(),
					Quantization: vi.info.Vector.Quantization.toVindex().String(),
				})
				continue
			}
			vs, vErr := vi.ix.Stats(tx)
			if vErr != nil {
				return vErr
			}
			pages := func(ns btree.NamespaceSize) int { return ns.TotalPages() * pageSize }
			vstat := VectorIndexStats{
				Name:         vi.info.Name,
				Field:        vi.info.Vector.Field,
				Dim:          vs.Dim,
				Metric:       vs.Metric.String(),
				Mode:         vi.mode.String(),
				Quantization: vs.Quantization.String(),
				M:            vs.M,
				EfSearch:     vs.EfSearch,
				NodeCount:    vs.NodeCount,
				LiveCount:    vs.LiveCount,
				DeletedCount: vs.DeletedCount,
				VectorBytes:  pages(vs.Vec),
				GraphBytes:   pages(vs.Adj),
				MappingBytes: pages(vs.Doc) + pages(vs.Lbl),
				MetaBytes:    pages(vs.Meta),
			}
			vstat.SizeBytes = vstat.VectorBytes + vstat.GraphBytes + vstat.MappingBytes + vstat.MetaBytes
			stats.VectorIndexesSizeBytes += vstat.SizeBytes
			stats.VectorIndexes = append(stats.VectorIndexes, vstat)
		}
		return nil
	})
	if err != nil {
		return CollectionStats{}, err
	}

	stats.TotalSizeBytes = stats.DocsSizeBytes + stats.IndexesSizeBytes + stats.VectorIndexesSizeBytes
	if stats.StoredDocsBytes > 0 {
		stats.CompressionRatio = float64(stats.UncompressedDocsBytes) / float64(stats.StoredDocsBytes)
	} else {
		stats.CompressionRatio = 1.0
	}
	return stats, nil
}

// uncompressedDocLen returns the logical (decompressed) length of a stored
// document value. Values that are not S2-compressed objects are returned at
// their stored length.
func uncompressedDocLen(val []byte) (int, error) {
	if len(val) < 5 || val[0] != byte(anyenc.TypeCompressedObjectS2) {
		return len(val), nil
	}
	compLen := binary.BigEndian.Uint32(val[1:5])
	if int(compLen) > len(val)-5 {
		return 0, fmt.Errorf("anystore: corrupted compressed document: declared %d bytes, have %d", compLen, len(val)-5)
	}
	n, err := s2.DecodedLen(val[5 : 5+compLen])
	if err != nil {
		return 0, err
	}
	return n, nil
}
