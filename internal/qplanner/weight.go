package qplanner

import (
	"bytes"
	"slices"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/bitmap"
	"github.com/anyproto/any-store/query"
)

// WeightedIndex holds the computed weight and metadata for an index candidate.
type WeightedIndex struct {
	Info               *IndexInfo
	Weight             int
	QueryFieldsBits    bitmap.Bitmap256
	SortFieldsBits     bitmap.Bitmap256
	Bounds             query.Bounds
	ExactSort          bool
	PartialSort        bool // index covers leading sort fields but not all
	PartialSortFields  int  // number of leading sort fields covered by index
	Used               bool
	FilterFullyCovered bool
}

// QueryField tracks a query field and its bounds.
type QueryField struct {
	Field  string
	Bounds query.Bounds
}

// WeightParams holds parameters for computing index weights.
type WeightParams struct {
	Cond       query.Filter
	SortFields []query.SortField
	IndexHints []IndexHintParam
	// MaxIndexes is the maximum number of indexes to use in a query.
	MaxIndexes int
}

// IndexHintParam mirrors the public IndexHint type.
type IndexHintParam struct {
	IndexName string
	Boost     int
}

// ComputeWeights computes weights for all index candidates and selects which to use.
// It returns the weighted indexes and the collected query fields.
func ComputeWeights(indexes []*IndexInfo, params *WeightParams) ([]WeightedIndex, []QueryField) {
	if params.MaxIndexes <= 0 {
		params.MaxIndexes = 2
	}

	var queryFields []QueryField
	getQueryField := func(field string) (QueryField, int) {
		for i, f := range queryFields {
			if f.Field == field {
				return f, i
			}
		}
		bounds := params.Cond.IndexBounds(field, nil)
		f := QueryField{Field: field, Bounds: bounds}
		queryFields = append(queryFields, f)
		return f, len(queryFields) - 1
	}

	weighted := make([]WeightedIndex, len(indexes))
	for i, idx := range indexes {
		weighted[i].Info = idx

		// Query weight
		weighted[i].Weight, weighted[i].QueryFieldsBits = indexQueryWeight(idx, params.Cond, getQueryField)

		// Sort weight
		if sw, sf, partial, partialCount := indexSortWeight(idx, params.SortFields); sw > 0 {
			weighted[i].Weight += sw
			weighted[i].SortFieldsBits = sf
			weighted[i].ExactSort = sf.CountLeadingOnes() == len(params.SortFields)
			weighted[i].PartialSort = partial
			weighted[i].PartialSortFields = partialCount
		}

		// Apply hints
		if weighted[i].Weight > 0 {
			for _, hint := range params.IndexHints {
				if hint.IndexName == idx.Name {
					weighted[i].Weight += hint.Boost
				}
			}
		}
	}

	// Sort by weight descending
	slices.SortFunc(weighted, func(a, b WeightedIndex) int {
		if a.Weight > b.Weight {
			return -1
		}
		if a.Weight < b.Weight {
			return 1
		}
		return 0
	})

	// Build bitmap of all query fields that have bounds
	var allQueryFieldsBits bitmap.Bitmap256
	for i, f := range queryFields {
		if len(f.Bounds) != 0 && i < 256 {
			allQueryFieldsBits = allQueryFieldsBits.Set(uint8(i))
		}
	}

	// Select which indexes to use
	var (
		usedFieldsBits bitmap.Bitmap256
		usedSortBits   bitmap.Bitmap256
		exactSortFound bool
		usedCount      int
	)
	for i, idx := range weighted {
		if idx.Weight < 1 {
			continue
		}
		if usedCount >= params.MaxIndexes {
			break
		}
		if usedFieldsBits.Subtract(idx.QueryFieldsBits).Count() != 0 ||
			usedSortBits.Subtract(idx.SortFieldsBits).Count() != 0 ||
			(!exactSortFound && (idx.ExactSort || idx.PartialSort)) {
			usedFieldsBits = usedFieldsBits.Or(idx.QueryFieldsBits)
			usedSortBits = usedSortBits.Or(idx.SortFieldsBits)
			if idx.ExactSort {
				exactSortFound = true
			}
			weighted[i].Used = true

			// Compute bounds
			bounds, chainLen := computeIndexBounds(idx.Info, params.Cond, getQueryField)
			weighted[i].Bounds = bounds

			// Check if the index chain covers ALL query fields with bounds
			if chainLen > 0 && allQueryFieldsBits.Count() > 0 {
				var chainFieldsBits bitmap.Bitmap256
				for ci := range chainLen {
					if ci < len(idx.Info.FieldNames) {
						_, fi := getQueryField(idx.Info.FieldNames[ci])
						if fi < 256 {
							chainFieldsBits = chainFieldsBits.Set(uint8(fi))
						}
					}
				}
				if allQueryFieldsBits.Subtract(chainFieldsBits).Count() == 0 {
					weighted[i].FilterFullyCovered = true
				}
			}
			usedCount++
		}
	}

	return weighted, queryFields
}

// indexQueryWeight computes the query weight for an index based on field bounds.
func indexQueryWeight(idx *IndexInfo, cond query.Filter, getQueryField func(string) (QueryField, int)) (weight int, fieldBits bitmap.Bitmap256) {
	var isChain = true
	for i, field := range idx.FieldNames {
		qField, fi := getQueryField(field)
		if len(qField.Bounds) != 0 {
			if isChain {
				if i == 0 {
					weight = 10
				} else {
					weight *= 2
				}
			} else {
				weight += 2
			}
			if i < 256 {
				fieldBits = fieldBits.Set(uint8(fi))
			}
		} else {
			if isChain {
				isChain = false
				weight -= 1
			}
		}
	}
	if weight > 0 && idx.Unique {
		weight++
	}
	return
}

// indexSortWeight computes the sort weight for an index.
// Returns weight, field bits, whether it's a partial sort, and how many leading fields match.
func indexSortWeight(idx *IndexInfo, sortFields []query.SortField) (weight int, fieldBits bitmap.Bitmap256, partial bool, partialCount int) {
	if len(sortFields) > 256 {
		sortFields = sortFields[:256]
	}
	var isChain = true
	for i, sf := range sortFields {
		if isChain && i < len(idx.FieldNames) {
			if idx.FieldNames[i] == sf.Field {
				if i == 0 {
					weight = 11
				} else {
					weight *= 2
					if idx.Reverse[i] == sf.Reverse {
						weight += 2
					}
				}
				fieldBits = fieldBits.Set(uint8(i))
				partialCount = i + 1
				continue
			}
		}
		isChain = false
		if slices.Contains(idx.FieldNames, sf.Field) {
			weight += 5
			fieldBits = fieldBits.Set(uint8(i))
		} else {
			break
		}
	}
	// Partial sort: index covers some leading sort fields but not all
	if partialCount > 0 && partialCount < len(sortFields) {
		partial = true
	}
	return
}

// computeIndexBounds computes combined tuple bounds for an index.
func computeIndexBounds(idx *IndexInfo, cond query.Filter, getQueryField func(string) (QueryField, int)) (query.Bounds, int) {
	type fieldBound struct {
		bounds query.Bounds
		fixed  bool
	}

	var chain []fieldBound
	for _, field := range idx.FieldNames {
		fb := cond.IndexBounds(field, nil)
		if len(fb) == 0 {
			break
		}
		allFixed := true
		for _, b := range fb {
			if len(b.Start) == 0 || !bytes.Equal(b.Start, b.End) {
				allFixed = false
				break
			}
		}
		chain = append(chain, fieldBound{bounds: fb, fixed: allFixed})
		if !allFixed {
			break
		}
	}

	if len(chain) == 0 {
		return nil, 0
	}

	chainLen := len(chain)

	var result query.Bounds
	for _, b := range chain[0].bounds {
		result = append(result, b)
	}

	for i := 1; i < len(chain); i++ {
		if !chain[i-1].fixed {
			break
		}
		var extended query.Bounds
		for _, prev := range result {
			for _, cur := range chain[i].bounds {
				eb := query.Bound{
					StartInclude: cur.StartInclude,
					EndInclude:   cur.EndInclude,
				}
				if len(cur.Start) > 0 {
					eb.Start = append(append(anyenc.Tuple(nil), prev.Start...), cur.Start...)
				} else {
					eb.Start = append(anyenc.Tuple(nil), prev.Start...)
					eb.StartInclude = true
				}
				if len(cur.End) > 0 {
					eb.End = append(append(anyenc.Tuple(nil), prev.End...), cur.End...)
				} else {
					eb.End = append(append(anyenc.Tuple(nil), prev.End...), 0xff)
					eb.EndInclude = true
				}
				extended = append(extended, eb)
			}
		}
		result = extended
	}

	return result, chainLen
}

// AdjustBoundsForNonUnique adjusts End bounds for non-unique indexes by appending 0xff
// to capture all docId suffixes.
func AdjustBoundsForNonUnique(bounds query.Bounds) query.Bounds {
	adjusted := make(query.Bounds, len(bounds))
	for i, b := range bounds {
		adjusted[i] = b
		if len(b.End) > 0 && b.EndInclude {
			adjusted[i].End = append(append(anyenc.Tuple(nil), b.End...), 0xff)
			adjusted[i].EndInclude = true
		}
	}
	return adjusted
}
