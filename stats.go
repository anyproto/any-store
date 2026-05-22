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

	// IndexesSizeBytes is the sum of SizeBytes across all indexes.
	IndexesSizeBytes int

	// TotalSizeBytes is DocsSizeBytes plus IndexesSizeBytes.
	TotalSizeBytes int

	// Indexes holds per-index statistics.
	Indexes []IndexStats
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
	indexes := append([]*index(nil), c.indexes...)
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
			if idx.sketch != nil {
				is.SketchDocCount = idx.sketch.GetDocCount()
				is.SketchSize = idx.sketch.Size
				is.SketchDistribution = idx.sketch.Distribution()
			}
			stats.IndexesSizeBytes += is.SizeBytes
			stats.Indexes = append(stats.Indexes, is)
		}
		return nil
	})
	if err != nil {
		return CollectionStats{}, err
	}

	stats.TotalSizeBytes = stats.DocsSizeBytes + stats.IndexesSizeBytes
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
