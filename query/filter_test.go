package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

func TestComp(t *testing.T) {
	a := &anyenc.Arena{}
	t.Run("eq", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, EqValue: anyenc.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(1), nil))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(2), nil))
			assert.False(t, cmp.Ok(a.NewNumberInt(0), nil))
			assert.False(t, cmp.Ok(a.NewNumberInt(-1), nil))
			assert.False(t, cmp.Ok(a.NewString("1"), nil))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bound{
				Start:        anyenc.AppendAnyValue(nil, 1),
				End:          anyenc.AppendAnyValue(nil, 1),
				StartInclude: true,
				EndInclude:   true,
			}, bs[0])
		})
	})
	t.Run("eq_array", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, EqValue: anyenc.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(anyenc.MustParseJson(`[3,2,1]`), nil))
			assert.True(t, cmp.Ok(anyenc.MustParseJson(`[1]`), nil))
			assert.True(t, cmp.Ok(anyenc.MustParseJson(`[1,2]`), nil))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(anyenc.MustParseJson(`[]`), nil))
			assert.False(t, cmp.Ok(anyenc.MustParseJson(`[0,2,3]`), nil))
			assert.False(t, cmp.Ok(a.NewNumberInt(-1), nil))
			assert.False(t, cmp.Ok(anyenc.MustParseJson(`["1",2]`), nil))
		})
		t.Run("array-array", func(t *testing.T) {
			aCmp := Comp{CompOp: CompOpEq, EqValue: anyenc.MustParseJson(`[1,2,3]`).MarshalTo(nil)}
			assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[1,2,3]`), nil))
			assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[[1,2,3], 1]`), nil))
		})
		t.Run("empty array", func(t *testing.T) {
			aCmp := Comp{CompOp: CompOpEq, EqValue: anyenc.MustParseJson(`[]`).MarshalTo(nil)}
			assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[]`), nil))
		})
	})
	t.Run("ne", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpNe, EqValue: anyenc.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2), nil))
			assert.True(t, cmp.Ok(a.NewNumberInt(0), nil))
			assert.True(t, cmp.Ok(a.NewNumberInt(-1), nil))
			assert.True(t, cmp.Ok(anyenc.MustParseJson(`[0,2,3]`), nil))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1), nil))
			assert.False(t, cmp.Ok(anyenc.MustParseJson(`[0,1,3]`), nil))
		})
		t.Run("array-array", func(t *testing.T) {
			aCmp := Comp{CompOp: CompOpNe, EqValue: anyenc.MustParseJson(`[1,2,3]`).MarshalTo(nil)}
			assert.False(t, aCmp.Ok(anyenc.MustParseJson(`[1,2,3]`), nil))
			assert.False(t, aCmp.Ok(anyenc.MustParseJson(`[[1,2,3], 1]`), nil))
			assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[1,2]`), nil))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			require.Len(t, bs, 2)
			assert.Equal(t, Bounds{
				{
					End: anyenc.AppendAnyValue(nil, 1),
				},
				{
					Start: anyenc.AppendAnyValue(nil, 1),
				},
			}, bs)
		})
	})
	t.Run("gt", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpGt, EqValue: anyenc.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2), nil))
			assert.True(t, cmp.Ok(a.NewNumberInt(3), nil))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(1.1), nil))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1), nil))
			assert.False(t, cmp.Ok(a.NewNumberInt(0), nil))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					Start: anyenc.AppendAnyValue(nil, 1),
				},
			}, bs)
		})
	})
	t.Run("gte", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpGte, EqValue: anyenc.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2), nil))
			assert.True(t, cmp.Ok(a.NewNumberInt(3), nil))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(1), nil))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(0), nil))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					Start:        anyenc.AppendAnyValue(nil, 1),
					StartInclude: true,
				},
			}, bs)
		})
	})
	t.Run("lt", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpLt, EqValue: anyenc.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(0), nil))
			assert.True(t, cmp.Ok(a.NewNumberInt(-1), nil))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(0.9), nil))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1), nil))
			assert.False(t, cmp.Ok(a.NewNumberInt(2), nil))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					End: anyenc.AppendAnyValue(nil, 1),
				},
			}, bs)
		})
	})
	t.Run("lte", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpLte, EqValue: anyenc.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(1), nil))
			assert.True(t, cmp.Ok(a.NewNumberInt(0), nil))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(0.9), nil))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(2), nil))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					End:        anyenc.AppendAnyValue(nil, 1),
					EndInclude: true,
				},
			}, bs)
		})
	})
	t.Run("with buf", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, EqValue: anyenc.AppendAnyValue(nil, 1)}
		docBuf := &syncpool.DocBuffer{}
		assert.True(t, cmp.Ok(a.NewNumberInt(1), docBuf))
		assert.False(t, cmp.Ok(a.NewNumberInt(-1), docBuf))
		assert.False(t, cmp.Ok(a.NewString("1"), docBuf))
		assert.True(t, cmp.Ok(anyenc.MustParseJson(`[3,2,1]`), docBuf))
		assert.True(t, cmp.Ok(anyenc.MustParseJson(`[1]`), docBuf))
		assert.False(t, cmp.Ok(anyenc.MustParseJson(`[]`), docBuf))
		assert.False(t, cmp.Ok(anyenc.MustParseJson(`[0,2,3]`), docBuf))

		aCmp := Comp{CompOp: CompOpEq, EqValue: anyenc.MustParseJson(`[1,2,3]`).MarshalTo(nil)}
		assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[1,2,3]`), docBuf))
		assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[[1,2,3], 1]`), docBuf))

		aCmp = Comp{CompOp: CompOpEq, EqValue: anyenc.MustParseJson(`[]`).MarshalTo(nil)}
		assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[]`), docBuf))
		assert.False(t, aCmp.Ok(anyenc.MustParseJson(`[1]`), docBuf))

		cmp = Comp{CompOp: CompOpNe, EqValue: anyenc.AppendAnyValue(nil, 1)}
		assert.True(t, cmp.Ok(a.NewNumberInt(0), docBuf))
		assert.True(t, cmp.Ok(a.NewNumberInt(-1), docBuf))
		assert.True(t, cmp.Ok(anyenc.MustParseJson(`[0,2,3]`), docBuf))
		assert.False(t, cmp.Ok(anyenc.MustParseJson(`[0,1,3]`), docBuf))

		aCmp = Comp{CompOp: CompOpNe, EqValue: anyenc.MustParseJson(`[1,2,3]`).MarshalTo(nil)}
		assert.False(t, aCmp.Ok(anyenc.MustParseJson(`[1,2,3]`), docBuf))
		assert.False(t, aCmp.Ok(anyenc.MustParseJson(`[[1,2,3], 1]`), docBuf))
		assert.True(t, aCmp.Ok(anyenc.MustParseJson(`[1,2]`), docBuf))
	})
}

func TestAnd(t *testing.T) {
	f, err := ParseCondition(`{"a":1, "b":"2"}`)
	require.NoError(t, err)
	t.Run("ok", func(t *testing.T) {
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"2","c":4}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":2,"b":"2","c":4}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":2,"c":4}`), nil))
	})
	t.Run("bounds", func(t *testing.T) {
		bs := f.IndexBounds("a", nil)
		require.Len(t, bs, 1)

		bs = f.IndexBounds("z", nil)
		assert.Nil(t, bs)
	})
}

// TestAndIndexBounds_TwoConjunctsSameField_Range pins that two range
// conjuncts on the same field intersect into a single bounded range
// (I-04: pre-fix And.IndexBounds returned only the first conjunct's bounds).
func TestAndIndexBounds_TwoConjunctsSameField_Range(t *testing.T) {
	f, err := ParseCondition(`{"$and":[{"a":{"$gte":5}},{"a":{"$lte":10}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	require.Len(t, bs, 1)
	assert.Equal(t, []byte(newBoundKey(5)), []byte(bs[0].Start))
	assert.Equal(t, []byte(newBoundKey(10)), []byte(bs[0].End))
	assert.True(t, bs[0].StartInclude)
	assert.True(t, bs[0].EndInclude)
}

// TestAndIndexBounds_InAndRange pins that an $in set intersected with a
// range conjunct keeps only the in-values inside the range (I-04).
func TestAndIndexBounds_InAndRange(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$in":[1,2,5,10]},"$and":[{"a":{"$gte":5}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	require.Len(t, bs, 2)
	// SortAndMerge orders by Start: {5} then {10}.
	assert.Equal(t, []byte(newBoundKey(5)), []byte(bs[0].Start))
	assert.Equal(t, []byte(newBoundKey(5)), []byte(bs[0].End))
	assert.Equal(t, []byte(newBoundKey(10)), []byte(bs[1].Start))
	assert.Equal(t, []byte(newBoundKey(10)), []byte(bs[1].End))
}

// TestAndIndexBounds_DisjointConjuncts pins that conjuncts with no common
// value intersect to an empty bound set (I-04: the wrong-answer trigger —
// pre-fix this returned the $in bounds and CountOnly over-counted).
func TestAndIndexBounds_DisjointConjuncts(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$in":[1,2]},"$and":[{"a":{"$gte":5}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	assert.Len(t, bs, 0)
}

// TestAndIndexBounds_ThreeConjuncts_EmptyStaysEmpty pins that once the
// running intersection is empty, a later contributing conjunct cannot
// resurrect it (a ∧ ∅ = ∅).
func TestAndIndexBounds_ThreeConjuncts_EmptyStaysEmpty(t *testing.T) {
	f, err := ParseCondition(`{"$and":[{"a":{"$in":[1,2]}},{"a":{"$gte":5}},{"a":{"$lte":8}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	assert.Len(t, bs, 0)
}

func TestOr(t *testing.T) {
	f, err := ParseCondition(`{"$or":[{"a":1},{"b":"2"}]}`)
	require.NoError(t, err)

	t.Run("ok", func(t *testing.T) {
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"2","c":4}`), nil))
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"3","c":4}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":12,"b":2,"c":4}`), nil))
	})
	t.Run("bounds", func(t *testing.T) {
		t.Run("no filter", func(t *testing.T) {
			bs := f.IndexBounds("a", nil)
			assert.Nil(t, bs)
		})
		t.Run("filter", func(t *testing.T) {
			f2, err := ParseCondition(`{"$or":[{"a":1},{"a":"2"}]}`)
			require.NoError(t, err)
			bs := f2.IndexBounds("a", nil)
			assert.Len(t, bs, 2)
		})
	})

}

func TestNor(t *testing.T) {
	f, err := ParseCondition(`{"$nor":[{"a":1},{"b":"2"}]}`)
	require.NoError(t, err)
	t.Run("ok", func(t *testing.T) {
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"2","c":4}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"3","c":4}`), nil))
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":12,"b":2,"c":4}`), nil))
	})
	t.Run("bounds", func(t *testing.T) {
		t.Run("no filter", func(t *testing.T) {
			bs := f.IndexBounds("a", nil)
			assert.Nil(t, bs)
		})
		t.Run("filter", func(t *testing.T) {
			f2, err := ParseCondition(`{"$nor":[{"a":1},{"a":"2"}]}`)
			require.NoError(t, err)
			bs := f2.IndexBounds("a", nil)
			assert.Len(t, bs, 0)
		})
	})
	t.Run("with eq", func(t *testing.T) {
		f, err := ParseCondition(`{"$nor":[{"a":{"$eq": 1}}]}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"2","c":4}`), nil))
	})
}

func TestNot(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$not":{"$eq":2}}}`)
	require.NoError(t, err)
	t.Run("ok", func(t *testing.T) {
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"2","c":4}`), nil))
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":"3","c":4}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":2,"b":2,"c":4}`), nil))
	})
	t.Run("bounds", func(t *testing.T) {
		bs := f.IndexBounds("a", nil)
		assert.Len(t, bs, 0)
	})
}

func TestComplex(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$in":[1,2,3]}, "b":{"$all":[1,2]}, "c": "test"}`)
	require.NoError(t, err)
	t.Run("ok", func(t *testing.T) {
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":2,"b":[3,2,1],"c":"test"}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":[3,2],"c":"test"}`), nil))
	})
	t.Run("ok with docBuf", func(t *testing.T) {
		docBuf := &syncpool.DocBuffer{}
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":2,"b":[3,2,1],"c":"test"}`), docBuf))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":1,"b":[3,2],"c":"test"}`), docBuf))
	})

	t.Run("bounds", func(t *testing.T) {
		bs := f.IndexBounds("a", nil)
		assert.Len(t, bs, 3)
	})

}

func TestExists(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		t.Run("true", func(t *testing.T) {
			f, err := ParseCondition(`{"a":{"$exists":true}}`)
			require.NoError(t, err)
			assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":1}`), nil))
			assert.False(t, f.Ok(anyenc.MustParseJson(`{"b":1}`), nil))
		})
		t.Run("false", func(t *testing.T) {
			f, err := ParseCondition(`{"a":{"$exists":false}}`)
			require.NoError(t, err)
			assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":1}`), nil))
			assert.True(t, f.Ok(anyenc.MustParseJson(`{"b":1}`), nil))
		})
	})
	t.Run("bounds", func(t *testing.T) {
		f, err := ParseCondition(`{"a":{"$exists":true}}`)
		require.NoError(t, err)
		bs := f.IndexBounds("a", nil)
		assert.Len(t, bs, 0)
	})
}

func TestTypeFilter(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$type":"number"}}`)
	require.NoError(t, err)
	t.Run("ok", func(t *testing.T) {
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"a":1}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":"1"}`), nil))
	})
	t.Run("bounds", func(t *testing.T) {
		bs := f.IndexBounds("a", nil)
		require.Len(t, bs, 1)
	})
}

func TestRegexp(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "a"}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "a"}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": "A"}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name":"b"}`), nil))
	})
	t.Run("ok - complex expression", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^(?i)a"}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": "baaa"}`), nil))
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "A"}`), nil))
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "a"}`), nil))
	})
	t.Run("ok - array", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^(?i)a"}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": ["A", "B", "C"]}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": ["baaa"]}`), nil))
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": ["baaa", "a"]}`), nil))
	})
	t.Run("ok - number", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^a(?i)"}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name":1}`), nil))
	})
	t.Run("ok - nil value", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^a(?i)"}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(nil, nil))
	})

	t.Run("index: no prefix", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "prefix"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 0)
	})
	t.Run("index: ^(?i)prefix - no prefix", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^(?i)prefix"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 0)
	})
	t.Run("index: ^prefix\\.test - return prefix.test", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^prefix\.test"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
		assert.Equal(t, `"prefix.test"`, append(bounds[0].Start, 0).String())
	})
	t.Run("index: ^prefix\\.test{1}* - return prefix.test", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^prefix\.test{a-zA-z}*"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
		assert.Equal(t, `"prefix.test"`, append(bounds[0].Start, 0).String())
	})
	t.Run("index: ^prefix+ - return prefix", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^prefix+"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
		assert.Equal(t, `"prefix"`, append(bounds[0].Start, 0).String())
	})
	t.Run("index: ^\\.a* - return prefix", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^\.a*"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
		assert.Equal(t, `".a"`, append(bounds[0].Start, 0).String())
	})
}

func TestSize(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$size": 2}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": [1,2]}`), nil))
	})
	t.Run("value nil", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$size": 2}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"arr": [1,2]}`), nil))
	})
	t.Run("not ok", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$size": 2}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": "a"}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": []}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": [1]}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": [1,2,3]}`), nil))
	})
	t.Run("error parsing expression - expected number", func(t *testing.T) {
		_, err := ParseCondition(`{"name":{"$size": "2"}}`)
		require.Error(t, err)
	})
	t.Run("to string then parse", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$size": 2}}`)
		require.NoError(t, err)

		_, err = ParseCondition(f.String())
		require.NoError(t, err)
	})
}

func TestIn(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a":2,"b":[3,2,1],"c":"test"}`)
	docBuf := &syncpool.DocBuffer{}

	t.Run("ok", func(t *testing.T) {
		f, err := ParseCondition(`{"c": {"$in": ["test"]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(doc, docBuf))
	})
	t.Run("not ok", func(t *testing.T) {
		f, err := ParseCondition(`{"a": {"$in": ["42"]}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(doc, docBuf))
	})

	t.Run("ok array", func(t *testing.T) {
		f, err := ParseCondition(`{"b": {"$in": [1,2]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(doc, docBuf))
	})
	t.Run("ok just one array", func(t *testing.T) {
		f, err := ParseCondition(`{"b": {"$in": [1,4]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(doc, docBuf))
	})
	t.Run("not ok any array", func(t *testing.T) {
		f, err := ParseCondition(`{"b": {"$in": [8,4]}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(doc, docBuf))
	})

}

func TestIn_IndexBounds(t *testing.T) {
	t.Run("point lookups", func(t *testing.T) {
		in := NewInValue(
			anyenc.MustParseJson(`1`),
			anyenc.MustParseJson(`2`),
			anyenc.MustParseJson(`3`),
		)
		bs := in.IndexBounds("a", nil)
		require.Len(t, bs, 3)
		// bounds should be sorted
		for i := 1; i < len(bs); i++ {
			assert.True(t, bs.Less(i-1, i), "bounds should be sorted")
		}
		// each bound is a point lookup
		for _, b := range bs {
			assert.True(t, b.StartInclude)
			assert.True(t, b.EndInclude)
			assert.Equal(t, b.Start, b.End)
		}
	})
	t.Run("over limit returns input", func(t *testing.T) {
		values := make(map[string]struct{}, orExpressionLimit+1)
		for i := 0; i < orExpressionLimit+1; i++ {
			values[string(anyenc.AppendAnyValue(nil, i))] = struct{}{}
		}
		in := In{Values: values}
		input := Bounds{{Start: []byte{1}, End: []byte{2}, StartInclude: true, EndInclude: true}}
		bs := in.IndexBounds("a", input)
		assert.Equal(t, input, bs)
	})
	t.Run("duplicates are merged", func(t *testing.T) {
		in := NewInValue(
			anyenc.MustParseJson(`1`),
			anyenc.MustParseJson(`1`),
			anyenc.MustParseJson(`2`),
		)
		bs := in.IndexBounds("a", nil)
		assert.Len(t, bs, 2)
	})
	t.Run("appends to existing bounds", func(t *testing.T) {
		in := NewInValue(
			anyenc.MustParseJson(`5`),
		)
		existing := Bounds{{
			Start: anyenc.AppendAnyValue(nil, 1), End: anyenc.AppendAnyValue(nil, 1),
			StartInclude: true, EndInclude: true,
		}}
		bs := in.IndexBounds("a", existing)
		assert.Len(t, bs, 2)
	})
}

func TestKey_IndexBounds(t *testing.T) {
	t.Run("field match delegates to inner", func(t *testing.T) {
		k := Key{
			Path:   []string{"a"},
			Filter: NewComp(CompOpEq, 1),
		}
		bs := k.IndexBounds("a", nil)
		require.Len(t, bs, 1)
		assert.Equal(t, anyenc.Tuple(anyenc.AppendAnyValue(nil, 1)), bs[0].Start)
	})
	t.Run("field mismatch returns input", func(t *testing.T) {
		k := Key{
			Path:   []string{"a"},
			Filter: NewComp(CompOpEq, 1),
		}
		bs := k.IndexBounds("b", nil)
		assert.Nil(t, bs)
	})
}

func TestAll_IndexBounds(t *testing.T) {
	a := All{}
	input := Bounds{{Start: []byte{1}, End: []byte{2}}}
	bs := a.IndexBounds("a", input)
	assert.Equal(t, input, bs)

	bs = a.IndexBounds("a", nil)
	assert.Nil(t, bs)
}

func TestExists_IndexBounds(t *testing.T) {
	e := Exists{}
	input := Bounds{{Start: []byte{1}, End: []byte{2}}}
	bs := e.IndexBounds("a", input)
	assert.Equal(t, input, bs)

	bs = e.IndexBounds("a", nil)
	assert.Nil(t, bs)
}

func TestCompNe_IndexBounds(t *testing.T) {
	cmp := NewComp(CompOpNe, 5)
	bs := cmp.IndexBounds("a", nil)
	require.Len(t, bs, 2)
	val5 := anyenc.Tuple(anyenc.AppendAnyValue(nil, 5))
	// first bound: (-inf, 5)
	assert.Nil(t, bs[0].Start)
	assert.Equal(t, val5, bs[0].End)
	assert.False(t, bs[0].EndInclude)
	// second bound: (5, inf)
	assert.Equal(t, val5, bs[1].Start)
	assert.Nil(t, bs[1].End)
	assert.False(t, bs[1].StartInclude)
}

func TestOr_IndexBounds_NonOverlapping(t *testing.T) {
	// Non-overlapping ranges from different branches stay separate
	f, err := ParseCondition(`{"$or":[{"a":{"$lte":5}},{"a":{"$gte":10}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	require.Len(t, bs, 2)
	// first: (-inf, 5]
	assert.Nil(t, bs[0].Start)
	assert.Equal(t, anyenc.Tuple(anyenc.AppendAnyValue(nil, 5)), bs[0].End)
	assert.True(t, bs[0].EndInclude)
	// second: [10, inf)
	assert.Equal(t, anyenc.Tuple(anyenc.AppendAnyValue(nil, 10)), bs[1].Start)
	assert.True(t, bs[1].StartInclude)
	assert.Nil(t, bs[1].End)
}

func BenchmarkIndexBounds(b *testing.B) {
	b.Run("Comp/Eq", func(b *testing.B) {
		cmp := NewComp(CompOpEq, 42)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			cmp.IndexBounds("a", nil)
		}
	})
	b.Run("In/10", func(b *testing.B) {
		benchInIndexBounds(b, 10)
	})
	b.Run("In/100", func(b *testing.B) {
		benchInIndexBounds(b, 100)
	})
	b.Run("In/500", func(b *testing.B) {
		benchInIndexBounds(b, 500)
	})
	b.Run("And/2fields", func(b *testing.B) {
		f, _ := ParseCondition(`{"a":1, "b":2}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.IndexBounds("a", nil)
		}
	})
	b.Run("Or/3branches", func(b *testing.B) {
		f, _ := ParseCondition(`{"$or":[{"a":1},{"a":2},{"a":3}]}`)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.IndexBounds("a", nil)
		}
	})
}

func benchInIndexBounds(b *testing.B, n int) {
	values := make([]*anyenc.Value, n)
	a := &anyenc.Arena{}
	for i := range values {
		values[i] = a.NewNumberInt(i)
	}
	in := NewInValue(values...)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		in.IndexBounds("a", nil)
	}
}

func BenchmarkFilter_Ok(b *testing.B) {
	doc := anyenc.MustParseJson(`{"a":2,"b":[3,2,1],"c":"test"}`)
	docBuf := &syncpool.DocBuffer{}
	bench := func(b *testing.B, query string) {
		f, err := ParseCondition(query)
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.Ok(doc, docBuf)
		}
	}

	b.Run("simple eq", func(b *testing.B) {
		bench(b, `{"a":2}`)
	})
	b.Run("eq array", func(b *testing.B) {
		bench(b, `{"b":3}`)
	})
	b.Run("double eq", func(b *testing.B) {
		bench(b, `{"a":2, "c":"test"}`)
	})
	b.Run("in", func(b *testing.B) {
		bench(b, `{"a":{"$in":[1,2]}}`)
	})
	b.Run("all", func(b *testing.B) {
		bench(b, `{"b":{"$all":[1,3]}}`)
	})
}

// --- Coverage tests from filter_size_coverage_test.go ---

// TestSize_Coverage_IndexBoundsComposition verifies that $size contributes
// no bounds of its own when composed with another indexed-field predicate
// in an $and. The other field (id) must still drive IndexBounds as if $size
// were absent, and the filter's Ok() must agree with a naive evaluation.
// Covers query/filter.go:553-554 (Size.IndexBounds identity return).
func TestSize_Coverage_IndexBoundsComposition(t *testing.T) {
	// {$and: [{arr: {$size: 2}}, {id: {$gt: 10}}]}
	f, err := ParseCondition(`{"$and": [{"arr": {"$size": 2}}, {"id": {"$gt": 10}}]}`)
	require.NoError(t, err)

	t.Run("bounds on id include the id range", func(t *testing.T) {
		bs := f.IndexBounds("id", nil)
		// id > 10 → one open-ended Bound with Start set, no End.
		require.Len(t, bs, 1, "id > 10 must contribute exactly one Bound")
		assert.NotEmpty(t, bs[0].Start, "id > 10 Bound must have a Start value")
		assert.False(t, bs[0].StartInclude, "$gt is exclusive — StartInclude must be false")
		assert.Empty(t, bs[0].End, "id > 10 Bound must be open-ended (no End)")
	})

	t.Run("bounds on arr do not include the $size term", func(t *testing.T) {
		// $size.IndexBounds returns the input bounds unchanged; the AND
		// evaluation therefore yields nil for field "arr".
		bs := f.IndexBounds("arr", nil)
		assert.Empty(t, bs,
			"$size must contribute no bounds — IndexBounds('arr', nil) must stay empty")
	})

	t.Run("Ok matches naive filter over arr + id", func(t *testing.T) {
		// Docs: naive filter = len(arr) == 2 AND id > 10.
		cases := []struct {
			json string
			want bool
		}{
			{`{"id": 15, "arr": [1, 2]}`, true},      // size ok, id>10 ok
			{`{"id": 5, "arr": [1, 2]}`, false},      // size ok, id fails
			{`{"id": 15, "arr": [1]}`, false},        // size fails
			{`{"id": 15, "arr": [1, 2, 3]}`, false},  // size fails
			{`{"id": 15}`, false},                    // arr missing
			{`{"id": 15, "arr": []}`, false},         // size == 0
			{`{"id": 11, "arr": ["a", "b"]}`, true},  // exactly id>10
			{`{"id": 10, "arr": ["a", "b"]}`, false}, // id == 10 not > 10
			{`{"id": 100, "arr": ["x", "y"]}`, true}, // big id, size 2
			{`{"id": 100, "arr": ["x"]}`, false},     // big id, size 1
		}
		for _, c := range cases {
			doc := anyenc.MustParseJson(c.json)
			got := f.Ok(doc, nil)
			assert.Equal(t, c.want, got, "doc=%s", c.json)
		}
	})
}
