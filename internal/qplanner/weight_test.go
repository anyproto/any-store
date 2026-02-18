package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/query"
)

func makeIndex(name string, fields []string, reverse []bool, unique, sparse bool) *IndexInfo {
	fieldPaths := make([][]string, len(fields))
	for i, f := range fields {
		fieldPaths[i] = []string{f}
	}
	if reverse == nil {
		reverse = make([]bool, len(fields))
	}
	return &IndexInfo{
		Name:       name,
		FieldNames: fields,
		FieldPaths: fieldPaths,
		Reverse:    reverse,
		Unique:     unique,
		Sparse:     sparse,
	}
}

func TestComputeWeights_SingleFieldIndex(t *testing.T) {
	idx := makeIndex("idx_a", []string{"a"}, nil, false, false)

	t.Run("matching filter", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		require.Len(t, weighted, 1)
		assert.Equal(t, 10, weighted[0].Weight)
		assert.True(t, weighted[0].Used)
	})

	t.Run("non-matching filter", func(t *testing.T) {
		cond := query.MustParseCondition(`{"b": 1}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		require.Len(t, weighted, 1)
		// chain break on "a" (no bounds): weight = 0 - 1 = -1
		assert.Equal(t, -1, weighted[0].Weight)
		assert.False(t, weighted[0].Used)
	})

	t.Run("range filter", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": {"$gt": 5}}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		require.Len(t, weighted, 1)
		assert.Equal(t, 10, weighted[0].Weight)
		assert.True(t, weighted[0].Used)
	})

	t.Run("nil filter", func(t *testing.T) {
		cond := query.MustParseCondition(nil)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		require.Len(t, weighted, 1)
		// All filter provides no bounds → chain break: -1
		assert.Equal(t, -1, weighted[0].Weight)
		assert.False(t, weighted[0].Used)
	})
}

func TestComputeWeights_UniqueBonus(t *testing.T) {
	idxNonUnique := makeIndex("idx_a", []string{"a"}, nil, false, false)
	idxUnique := makeIndex("idx_a_uniq", []string{"a"}, nil, true, false)

	cond := query.MustParseCondition(`{"a": 1}`)

	wNon, _ := ComputeWeights([]*IndexInfo{idxNonUnique}, &WeightParams{Cond: cond})
	wUniq, _ := ComputeWeights([]*IndexInfo{idxUnique}, &WeightParams{Cond: cond})

	assert.Equal(t, 10, wNon[0].Weight)
	assert.Equal(t, 11, wUniq[0].Weight) // +1 for unique
}

func TestComputeWeights_CompoundIndex_Chain(t *testing.T) {
	idx := makeIndex("idx_ab", []string{"a", "b"}, nil, false, false)

	t.Run("both fields matched", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": 2}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		// chain: first field = 10, second field = 10*2 = 20
		assert.Equal(t, 20, weighted[0].Weight)
		assert.True(t, weighted[0].Used)
	})

	t.Run("only first field matched", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		// chain: first = 10, second breaks chain: 10 - 1 = 9
		assert.Equal(t, 9, weighted[0].Weight)
		assert.True(t, weighted[0].Used)
	})

	t.Run("only second field matched - no chain", func(t *testing.T) {
		cond := query.MustParseCondition(`{"b": 2}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		// first field breaks chain: -1, second field non-chain: +2 → total = 1
		assert.Equal(t, 1, weighted[0].Weight)
		assert.True(t, weighted[0].Used)
	})
}

func TestComputeWeights_CompoundIndex_ThreeFields(t *testing.T) {
	idx := makeIndex("idx_abc", []string{"a", "b", "c"}, nil, false, false)

	t.Run("all three matched", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": 2, "c": 3}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		// chain: 10 → 20 → 40
		assert.Equal(t, 40, weighted[0].Weight)
	})

	t.Run("first two matched", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": 2}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		// chain: 10 → 20, then break: 20 - 1 = 19
		assert.Equal(t, 19, weighted[0].Weight)
	})

	t.Run("first and third matched - chain breaks at second", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "c": 3}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		// chain: first = 10, second breaks chain: 10 - 1 = 9, third non-chain: 9 + 2 = 11
		assert.Equal(t, 11, weighted[0].Weight)
	})
}

func TestComputeWeights_SortWeight(t *testing.T) {
	idx := makeIndex("idx_a", []string{"a"}, nil, false, false)

	t.Run("sort matches index field", func(t *testing.T) {
		cond := query.MustParseCondition(nil)
		sortFields := query.MustParseSort("a").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: cond, SortFields: sortFields,
		})
		// query weight = -1 (All filter, chain break), sort weight = 11, total = 10
		assert.Equal(t, 10, weighted[0].Weight)
		assert.True(t, weighted[0].ExactSort)
		assert.False(t, weighted[0].PartialSort)
		assert.True(t, weighted[0].Used)
	})

	t.Run("sort doesn't match", func(t *testing.T) {
		cond := query.MustParseCondition(nil)
		sortFields := query.MustParseSort("b").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: cond, SortFields: sortFields,
		})
		// query weight = -1, sort weight = 0, total = -1
		assert.Equal(t, -1, weighted[0].Weight)
		assert.False(t, weighted[0].ExactSort)
		assert.False(t, weighted[0].Used)
	})

	t.Run("reverse sort matches", func(t *testing.T) {
		cond := query.MustParseCondition(nil)
		sortFields := query.MustParseSort("-a").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: cond, SortFields: sortFields,
		})
		// query weight = -1, sort weight = 11, total = 10
		assert.Equal(t, 10, weighted[0].Weight)
		assert.True(t, weighted[0].ExactSort)
		assert.True(t, weighted[0].Used)
	})
}

func TestComputeWeights_CompoundSortWeight(t *testing.T) {
	idx := makeIndex("idx_ab", []string{"a", "b"}, nil, false, false)

	t.Run("sort on both fields - exact sort", func(t *testing.T) {
		sortFields := query.MustParseSort("a", "b").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: query.MustParseCondition(nil), SortFields: sortFields,
		})
		// query weight = -1 (first field breaks chain), sort: 11*2+2 = 24, total = 23
		assert.Equal(t, 23, weighted[0].Weight)
		assert.True(t, weighted[0].ExactSort)
		assert.False(t, weighted[0].PartialSort)
		assert.True(t, weighted[0].Used)
	})

	t.Run("sort on first field only - partial sort", func(t *testing.T) {
		sortFields := query.MustParseSort("a", "c").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: query.MustParseCondition(nil), SortFields: sortFields,
		})
		assert.True(t, weighted[0].PartialSort)
		assert.False(t, weighted[0].ExactSort)
		assert.Equal(t, 1, weighted[0].PartialSortFields)
	})

	t.Run("sort on both fields reverse second - exact sort with direction penalty", func(t *testing.T) {
		sortFields := query.MustParseSort("a", "-b").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: query.MustParseCondition(nil), SortFields: sortFields,
		})
		// query weight = -1, sort: 11*2 = 22 (no direction bonus), total = 21
		assert.Equal(t, 21, weighted[0].Weight)
		assert.True(t, weighted[0].ExactSort)
	})
}

func TestComputeWeights_SortAndFilter(t *testing.T) {
	idx := makeIndex("idx_a", []string{"a"}, nil, false, false)

	cond := query.MustParseCondition(`{"a": {"$gt": 5}}`)
	sortFields := query.MustParseSort("a").Fields()

	weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
		Cond: cond, SortFields: sortFields,
	})
	// query weight 10 + sort weight 11 = 21
	assert.Equal(t, 21, weighted[0].Weight)
	assert.True(t, weighted[0].ExactSort)
	assert.True(t, weighted[0].Used)
}

func TestComputeWeights_MultipleIndexes_Selection(t *testing.T) {
	idxA := makeIndex("idx_a", []string{"a"}, nil, false, false)
	idxB := makeIndex("idx_b", []string{"b"}, nil, false, false)
	idxC := makeIndex("idx_c", []string{"c"}, nil, false, false)

	t.Run("two indexes selected with MaxIndexes=2", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": 2, "c": 3}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idxA, idxB, idxC}, &WeightParams{
			Cond: cond, MaxIndexes: 2,
		})
		usedCount := 0
		for _, w := range weighted {
			if w.Used {
				usedCount++
			}
		}
		assert.Equal(t, 2, usedCount)
	})

	t.Run("MaxIndexes=1", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": 2}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idxA, idxB}, &WeightParams{
			Cond: cond, MaxIndexes: 1,
		})
		usedCount := 0
		for _, w := range weighted {
			if w.Used {
				usedCount++
			}
		}
		assert.Equal(t, 1, usedCount)
	})
}

func TestComputeWeights_IndexHints(t *testing.T) {
	idxA := makeIndex("idx_a", []string{"a"}, nil, false, false)
	idxB := makeIndex("idx_b", []string{"b"}, nil, false, false)

	cond := query.MustParseCondition(`{"a": 1, "b": 1}`)
	weighted, _ := ComputeWeights([]*IndexInfo{idxA, idxB}, &WeightParams{
		Cond: cond,
		IndexHints: []IndexHintParam{
			{IndexName: "idx_b", Boost: 100},
		},
	})

	// idx_b should have higher weight due to hint
	for _, w := range weighted {
		if w.Info.Name == "idx_b" {
			assert.True(t, w.Weight > 10)
			assert.True(t, w.Used)
		}
	}
	// idx_b should be first (highest weight)
	assert.Equal(t, "idx_b", weighted[0].Info.Name)
}

func TestComputeWeights_FilterFullyCovered(t *testing.T) {
	idx := makeIndex("idx_ab", []string{"a", "b"}, nil, false, false)

	t.Run("filter fully covered by compound index", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": 2}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		assert.True(t, weighted[0].FilterFullyCovered)
	})

	t.Run("filter on field not in any index - still considered covered", func(t *testing.T) {
		// Field "c" is not present in any index, so it's invisible to the
		// FilterFullyCovered check which only tracks fields queried through indexes.
		cond := query.MustParseCondition(`{"a": 1, "c": 3}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		assert.True(t, weighted[0].FilterFullyCovered)
	})

	t.Run("filter on both index fields but second not in chain", func(t *testing.T) {
		// With a range on first field, only first field is in the chain.
		// Both fields are tracked as query fields via the index.
		// Chain only covers "a", but "b" also has bounds → not fully covered.
		cond := query.MustParseCondition(`{"a": {"$gt": 1}, "b": 2}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		assert.False(t, weighted[0].FilterFullyCovered)
	})
}

func TestComputeWeights_CompoundReverseIndex(t *testing.T) {
	// Index with a ascending and b descending
	idx := makeIndex("idx_a_negb", []string{"a", "b"}, []bool{false, true}, false, false)

	t.Run("sort matches compound with correct directions", func(t *testing.T) {
		sortFields := query.MustParseSort("a", "-b").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: query.MustParseCondition(nil), SortFields: sortFields,
		})
		// query weight = -1 (All filter, chain break on first field)
		// sort: first=11, second: 11*2+2=24 (direction matches: both reverse)
		// total = -1 + 24 = 23
		assert.Equal(t, 23, weighted[0].Weight)
		assert.True(t, weighted[0].ExactSort)
	})

	t.Run("sort opposite direction on second field", func(t *testing.T) {
		sortFields := query.MustParseSort("a", "b").Fields()
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{
			Cond: query.MustParseCondition(nil), SortFields: sortFields,
		})
		// query weight = -1, sort: 11*2 = 22 (no direction bonus), total = 21
		assert.Equal(t, 21, weighted[0].Weight)
		assert.True(t, weighted[0].ExactSort)
	})
}

func TestComputeWeights_InFilter(t *testing.T) {
	idx := makeIndex("idx_a", []string{"a"}, nil, false, false)

	cond := query.MustParseCondition(`{"a": {"$in": [1, 2, 3]}}`)
	weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
	assert.Equal(t, 10, weighted[0].Weight)
	assert.True(t, weighted[0].Used)
	assert.True(t, len(weighted[0].Bounds) > 0)
}

func TestComputeWeights_SortedByWeightDesc(t *testing.T) {
	idxA := makeIndex("idx_a", []string{"a"}, nil, false, false)
	idxAB := makeIndex("idx_ab", []string{"a", "b"}, nil, false, false)

	cond := query.MustParseCondition(`{"a": 1, "b": 2}`)
	weighted, _ := ComputeWeights([]*IndexInfo{idxA, idxAB}, &WeightParams{Cond: cond})

	// idx_ab should have higher weight (compound chain)
	assert.Equal(t, "idx_ab", weighted[0].Info.Name)
	assert.True(t, weighted[0].Weight > weighted[1].Weight)
}

func TestComputeWeights_NoIndexes(t *testing.T) {
	cond := query.MustParseCondition(`{"a": 1}`)
	weighted, fields := ComputeWeights(nil, &WeightParams{Cond: cond})
	assert.Len(t, weighted, 0)
	assert.Len(t, fields, 0)
}

func TestComputeWeights_RangeCompound(t *testing.T) {
	idx := makeIndex("idx_ab", []string{"a", "b"}, nil, false, false)

	t.Run("first field range breaks chain for bounds", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": {"$gt": 5}, "b": 2}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		assert.True(t, weighted[0].Weight > 0)
		assert.True(t, weighted[0].Used)
		assert.True(t, len(weighted[0].Bounds) > 0)
	})

	t.Run("first field fixed, second field range - both in bounds", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": {"$gt": 5}}`)
		weighted, _ := ComputeWeights([]*IndexInfo{idx}, &WeightParams{Cond: cond})
		assert.True(t, weighted[0].Weight > 0)
		assert.True(t, len(weighted[0].Bounds) > 0)
	})
}

func TestComputeIndexBounds(t *testing.T) {
	idx := makeIndex("idx_a", []string{"a"}, nil, false, false)

	t.Run("equality bound", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1}`)
		getQF := func(field string) (QueryField, int) {
			return QueryField{Field: field, Bounds: cond.IndexBounds(field, nil)}, 0
		}
		bounds, chainLen := computeIndexBounds(idx, cond, getQF)
		assert.Equal(t, 1, chainLen)
		assert.True(t, len(bounds) > 0)
	})

	t.Run("no matching field", func(t *testing.T) {
		cond := query.MustParseCondition(`{"b": 1}`)
		getQF := func(field string) (QueryField, int) {
			return QueryField{Field: field, Bounds: cond.IndexBounds(field, nil)}, 0
		}
		bounds, chainLen := computeIndexBounds(idx, cond, getQF)
		assert.Equal(t, 0, chainLen)
		assert.Nil(t, bounds)
	})
}

func TestComputeIndexBounds_Compound(t *testing.T) {
	idx := makeIndex("idx_ab", []string{"a", "b"}, nil, false, false)

	t.Run("both fixed", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": 2}`)
		getQF := func(field string) (QueryField, int) {
			b := cond.IndexBounds(field, nil)
			fi := 0
			if field == "b" {
				fi = 1
			}
			return QueryField{Field: field, Bounds: b}, fi
		}
		bounds, chainLen := computeIndexBounds(idx, cond, getQF)
		assert.Equal(t, 2, chainLen)
		assert.Equal(t, 1, len(bounds)) // single combined bound
	})

	t.Run("first fixed, second range", func(t *testing.T) {
		cond := query.MustParseCondition(`{"a": 1, "b": {"$gt": 5}}`)
		getQF := func(field string) (QueryField, int) {
			b := cond.IndexBounds(field, nil)
			fi := 0
			if field == "b" {
				fi = 1
			}
			return QueryField{Field: field, Bounds: b}, fi
		}
		bounds, chainLen := computeIndexBounds(idx, cond, getQF)
		assert.Equal(t, 2, chainLen)
		assert.True(t, len(bounds) > 0)
	})
}

func TestAdjustBoundsForNonUnique(t *testing.T) {
	t.Run("appends 0xff to End with EndInclude", func(t *testing.T) {
		bounds := query.Bounds{
			{Start: []byte{1}, End: []byte{2}, StartInclude: true, EndInclude: true},
		}
		adjusted := AdjustBoundsForNonUnique(bounds)
		assert.Equal(t, 1, len(adjusted))
		assert.True(t, len(adjusted[0].End) > len(bounds[0].End))
		assert.Equal(t, byte(0xff), adjusted[0].End[len(adjusted[0].End)-1])
	})

	t.Run("no change for EndInclude=false", func(t *testing.T) {
		bounds := query.Bounds{
			{Start: []byte{1}, End: []byte{2}, StartInclude: true, EndInclude: false},
		}
		adjusted := AdjustBoundsForNonUnique(bounds)
		assert.Equal(t, bounds[0].End, adjusted[0].End)
	})

	t.Run("empty bounds", func(t *testing.T) {
		adjusted := AdjustBoundsForNonUnique(nil)
		assert.Len(t, adjusted, 0)
	})
}

func TestIndexSortWeight(t *testing.T) {
	t.Run("single field exact match", func(t *testing.T) {
		idx := makeIndex("idx_a", []string{"a"}, nil, false, false)
		sortFields := query.MustParseSort("a").Fields()
		w, _, partial, count := indexSortWeight(idx, sortFields)
		assert.Equal(t, 11, w)
		assert.False(t, partial)
		assert.Equal(t, 1, count)
	})

	t.Run("two fields exact match same direction", func(t *testing.T) {
		idx := makeIndex("idx_ab", []string{"a", "b"}, []bool{false, false}, false, false)
		sortFields := query.MustParseSort("a", "b").Fields()
		w, _, partial, count := indexSortWeight(idx, sortFields)
		assert.Equal(t, 24, w) // 11 * 2 + 2
		assert.False(t, partial)
		assert.Equal(t, 2, count)
	})

	t.Run("two fields exact match diff direction", func(t *testing.T) {
		idx := makeIndex("idx_ab", []string{"a", "b"}, []bool{false, false}, false, false)
		sortFields := query.MustParseSort("a", "-b").Fields()
		w, _, partial, count := indexSortWeight(idx, sortFields)
		assert.Equal(t, 22, w) // 11 * 2 (no direction bonus)
		assert.False(t, partial)
		assert.Equal(t, 2, count)
	})

	t.Run("partial sort - first field matches", func(t *testing.T) {
		idx := makeIndex("idx_a", []string{"a"}, nil, false, false)
		sortFields := query.MustParseSort("a", "b").Fields()
		w, _, partial, count := indexSortWeight(idx, sortFields)
		assert.True(t, w > 0)
		assert.True(t, partial)
		assert.Equal(t, 1, count)
	})

	t.Run("no match at all", func(t *testing.T) {
		idx := makeIndex("idx_a", []string{"a"}, nil, false, false)
		sortFields := query.MustParseSort("x").Fields()
		w, _, partial, count := indexSortWeight(idx, sortFields)
		assert.Equal(t, 0, w)
		assert.False(t, partial)
		assert.Equal(t, 0, count)
	})
}

func TestIndexQueryWeight(t *testing.T) {
	t.Run("single field eq", func(t *testing.T) {
		idx := makeIndex("idx_a", []string{"a"}, nil, false, false)
		cond := query.MustParseCondition(`{"a": 1}`)
		getQF := func(field string) (QueryField, int) {
			return QueryField{Field: field, Bounds: cond.IndexBounds(field, nil)}, 0
		}
		w, _ := indexQueryWeight(idx, cond, getQF)
		assert.Equal(t, 10, w)
	})

	t.Run("compound chain", func(t *testing.T) {
		idx := makeIndex("idx_abc", []string{"a", "b", "c"}, nil, false, false)
		cond := query.MustParseCondition(`{"a": 1, "b": 2, "c": 3}`)
		fi := 0
		getQF := func(field string) (QueryField, int) {
			i := fi
			fi++
			return QueryField{Field: field, Bounds: cond.IndexBounds(field, nil)}, i
		}
		w, _ := indexQueryWeight(idx, cond, getQF)
		assert.Equal(t, 40, w) // 10 → 20 → 40
	})

	t.Run("unique bonus", func(t *testing.T) {
		idx := makeIndex("idx_a", []string{"a"}, nil, true, false)
		cond := query.MustParseCondition(`{"a": 1}`)
		getQF := func(field string) (QueryField, int) {
			return QueryField{Field: field, Bounds: cond.IndexBounds(field, nil)}, 0
		}
		w, _ := indexQueryWeight(idx, cond, getQF)
		assert.Equal(t, 11, w) // 10 + 1 unique
	})
}

func TestComputeWeights_BestIndexSelected(t *testing.T) {
	idxA := makeIndex("idx_a", []string{"a"}, nil, false, false)
	idxB := makeIndex("idx_b", []string{"b"}, nil, false, false)
	idxAB := makeIndex("idx_ab", []string{"a", "b"}, nil, false, false)

	cond := query.MustParseCondition(`{"a": 1, "b": 2}`)
	weighted, _ := ComputeWeights([]*IndexInfo{idxA, idxB, idxAB}, &WeightParams{
		Cond: cond, MaxIndexes: 2,
	})

	// idx_ab with weight 20 should be first
	assert.Equal(t, "idx_ab", weighted[0].Info.Name)
	assert.True(t, weighted[0].Used)
}

func TestComputeWeights_SortOnlyIndex(t *testing.T) {
	idxA := makeIndex("idx_a", []string{"a"}, nil, false, false)

	sortFields := query.MustParseSort("a").Fields()
	cond := query.MustParseCondition(`{"b": 1}`) // filter on different field

	weighted, _ := ComputeWeights([]*IndexInfo{idxA}, &WeightParams{
		Cond: cond, SortFields: sortFields,
	})
	// query weight = -1 (chain break), sort weight = 11, total = 10
	assert.Equal(t, 10, weighted[0].Weight)
	assert.True(t, weighted[0].ExactSort)
	assert.True(t, weighted[0].Used)
}

func TestComputeWeights_FilterAndSort_DifferentIndexes(t *testing.T) {
	idxA := makeIndex("idx_a", []string{"a"}, nil, false, false)
	idxB := makeIndex("idx_b", []string{"b"}, nil, false, false)

	cond := query.MustParseCondition(`{"a": 1}`)
	sortFields := query.MustParseSort("b").Fields()

	weighted, _ := ComputeWeights([]*IndexInfo{idxA, idxB}, &WeightParams{
		Cond: cond, SortFields: sortFields, MaxIndexes: 2,
	})

	// Both should be used - one for filter, one for sort
	usedCount := 0
	for _, w := range weighted {
		if w.Used {
			usedCount++
		}
	}
	assert.Equal(t, 2, usedCount)
}

func TestComputeWeights_DefaultMaxIndexes(t *testing.T) {
	idxA := makeIndex("idx_a", []string{"a"}, nil, false, false)
	idxB := makeIndex("idx_b", []string{"b"}, nil, false, false)
	idxC := makeIndex("idx_c", []string{"c"}, nil, false, false)

	cond := query.MustParseCondition(`{"a": 1, "b": 2, "c": 3}`)
	weighted, _ := ComputeWeights([]*IndexInfo{idxA, idxB, idxC}, &WeightParams{
		Cond: cond,
		// MaxIndexes defaults to 2
	})
	usedCount := 0
	for _, w := range weighted {
		if w.Used {
			usedCount++
		}
	}
	assert.Equal(t, 2, usedCount)
}
