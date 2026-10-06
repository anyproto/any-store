// Package vivf is the btree-resident IVF-SQ vector index: a k-means coarse
// quantizer partitions the vectors into cells, each cell is a contiguous key
// range of int8 scalar-quantized records, and a search scans a few cells with
// exact int8 distance. See store.go for the store and codec.go for the layout.
package vivf
