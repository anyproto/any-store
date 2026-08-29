package query

import (
	"bytes"
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
					End:   []byte{byte(anyenc.TypeNumber) + 1},
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
					End:          []byte{byte(anyenc.TypeNumber) + 1},
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
					Start:        []byte{byte(anyenc.TypeNumber)},
					StartInclude: true,
					End:          anyenc.AppendAnyValue(nil, 1),
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
					Start:        []byte{byte(anyenc.TypeNumber)},
					StartInclude: true,
					End:          anyenc.AppendAnyValue(nil, 1),
					EndInclude:   true,
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

// TestAndIndexBounds_SameFieldOverApprox pins the contract: And.IndexBounds
// returns a SOUND OVER-APPROXIMATION (the first contributing conjunct's bounds),
// NOT the intersection. Intersecting would be unsound for array/multi-key fields
// (see And.IndexBounds and TestQueryCount_ArrayTwoSidedRange). Here two range
// conjuncts on "a" yield just the first, [5, <number-bracket-end>), a superset a
// FilterIter trims.
func TestAndIndexBounds_SameFieldOverApprox(t *testing.T) {
	f, err := ParseCondition(`{"$and":[{"a":{"$gte":5}},{"a":{"$lte":10}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	require.Len(t, bs, 1)
	assert.Equal(t, []byte(newBoundKey(5)), []byte(bs[0].Start))
	assert.True(t, bs[0].StartInclude)
	assert.Equal(t, []byte{byte(anyenc.TypeNumber) + 1}, []byte(bs[0].End),
		"over-approx keeps the first conjunct's bracket-open upper bound, not the $lte")
	assert.False(t, bs[0].EndInclude)
}

// TestAndIndexBounds_InAndRange_OverApprox pins that an $in conjoined with a
// range yields the full $in set (the first conjunct), NOT the in-values trimmed
// to the range. The dropped $gte is re-applied by a FilterIter; for Count the
// CountOnly fast path is gated off (indexCoversFilter rejects the 2-predicate
// field) so the over-approx is never miscounted.
func TestAndIndexBounds_InAndRange_OverApprox(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$in":[1,2,5,10]},"$and":[{"a":{"$gte":5}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	require.Len(t, bs, 4, "all four $in values, not just those >= 5")
	assert.Equal(t, []byte(newBoundKey(1)), []byte(bs[0].Start))
	assert.Equal(t, []byte(newBoundKey(10)), []byte(bs[3].Start))
}

// TestAndIndexBounds_DisjointConjuncts_OverApproxNotEmpty pins that disjoint
// conjuncts (no common value) do NOT collapse to empty bounds — they
// over-approximate to the first conjunct's $in set. The CountOnly over-count
// wrong answer is prevented instead at the planner gate
// (indexCoversFilter rejects a >1-predicate field) and re-checked end-to-end by
// TestQueryCount_AndConjunctionLostInCount.
func TestAndIndexBounds_DisjointConjuncts_OverApproxNotEmpty(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$in":[1,2]},"$and":[{"a":{"$gte":5}}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	require.Len(t, bs, 2, "over-approx returns the $in bounds, never empty (would drop matches for array fields)")
	assert.True(t, bs.Contains(newBoundKey(1)))
	assert.True(t, bs.Contains(newBoundKey(2)))
}

// TestOrIndexBounds_DisjointAndBranch_SafeOverApprox pins that an And nested in
// an Or stays an over-approximation: an unsatisfiable And branch must NOT
// collapse the Or bounds to empty and drop the satisfiable a==1 disjunct. Index
// bounds must always be a superset of the match set (a FilterIter re-checks them).
func TestOrIndexBounds_DisjointAndBranch_SafeOverApprox(t *testing.T) {
	f, err := ParseCondition(`{"$or":[{"a":1},{"$and":[{"a":{"$in":[1,2,3]}},{"a":{"$gte":5}}]}]}`)
	require.NoError(t, err)
	bs := f.IndexBounds("a", nil)
	require.NotEmpty(t, bs, "Or bounds must not collapse to empty for a satisfiable query")
	assert.True(t, bs.Contains(newBoundKey(1)), "bounds must include the matching value a=1")
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

func TestTypeFilter_ObjectID(t *testing.T) {
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("a", a.NewObjectID(anyenc.NewObjectID()))

	for _, spec := range []string{`{"a":{"$type":"objectId"}}`, `{"a":{"$type":11}}`} {
		f, err := ParseCondition(spec)
		require.NoError(t, err, spec)
		assert.True(t, f.Ok(doc, nil), spec)
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"a":"x"}`), nil), spec)
	}
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
	t.Run("ok - $options: i", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "newsletter", "$options": "i"}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "Newsletter weekly"}`), nil))
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "my newsletter digest"}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": "other"}`), nil))
	})
	t.Run("ok - $options before $regex", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$options": "i", "$regex": "^a"}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "Abc"}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": "bA"}`), nil))
	})
	t.Run("ok - empty $options is a plain $regex", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "a", "$options": ""}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "a"}`), nil))
		assert.False(t, f.Ok(anyenc.MustParseJson(`{"name": "A"}`), nil))
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
	t.Run("index: ^prefix with $options i - no prefix", func(t *testing.T) {
		// Case-insensitive matching must not narrow the scan to the
		// literal-case prefix range.
		f, err := ParseCondition(`{"name":{"$regex": "^prefix", "$options": "i"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 0)
	})
	t.Run("index: ^prefix with $options m - no prefix", func(t *testing.T) {
		// Under m the anchor matches at any line start, so "x\nprefix"
		// matches while sorting outside the literal prefix range.
		f, err := ParseCondition(`{"name":{"$regex": "^prefix", "$options": "m"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 0)
	})
	t.Run("index: ^prefix with $options s - keeps prefix", func(t *testing.T) {
		// s only changes what '.' matches; it cannot affect a literal
		// anchored prefix, so the narrow scan stays sound.
		f, err := ParseCondition(`{"name":{"$regex": "^prefix", "$options": "s"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
	})
	t.Run("ok - duplicate $options keys are last-wins", func(t *testing.T) {
		// Duplicate keys collapse last-wins in the JSON parser (standard JSON
		// behavior, before operator validation); the surviving occurrence is
		// then validated like any other.
		f, err := ParseCondition(`{"name":{"$regex":"^a","$options":"!","$options":"i"}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"name": "Abc"}`), nil))
		_, err = ParseCondition(`{"name":{"$regex":"^a","$options":"i","$options":"!"}}`)
		require.Error(t, err)
	})
	t.Run("ok - $options String round-trip", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex":"^a","$options":"i"}}`)
		require.NoError(t, err)
		assert.Equal(t, `{"name": {"$regex": "^a", "$options": "i"}}`, f.String())
		f2, err := ParseCondition(f.String())
		require.NoError(t, err)
		assert.True(t, f2.Ok(anyenc.MustParseJson(`{"name": "Abc"}`), nil))
		assert.False(t, f2.Ok(anyenc.MustParseJson(`{"name": "bc"}`), nil))
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

	// $in is an OR of equalities: an array member of the set must match the
	// WHOLE field array, mirroring Comp.Ok — an empty array can only match
	// this way, and the index/Count path already includes these matches.
	t.Run("ok whole array member", func(t *testing.T) {
		f, err := ParseCondition(`{"b": {"$in": [[3,2,1], 9]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(doc, docBuf))
	})
	t.Run("ok empty array member", func(t *testing.T) {
		f, err := ParseCondition(`{"e": {"$in": [[], 9]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"e":[]}`), docBuf))
	})
	t.Run("not ok different whole array", func(t *testing.T) {
		f, err := ParseCondition(`{"b": {"$in": [[1,2,3]]}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(doc, docBuf))
	})

	// A null member matches a MISSING field, keeping $in consistent with
	// {"$eq":null} (Comp.Ok probes encodedNull for a nil value) and with the
	// index/Count path (a missing field is indexed under TypeNull, and
	// In.IndexBounds emits a point bound for the null member).
	t.Run("null member matches missing field", func(t *testing.T) {
		f, err := ParseCondition(`{"x": {"$in": [null]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(doc, docBuf))
	})
	t.Run("no null member: missing field not matched", func(t *testing.T) {
		f, err := ParseCondition(`{"x": {"$in": [1, "y"]}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(doc, docBuf))
	})
	t.Run("null member matches explicit null", func(t *testing.T) {
		f, err := ParseCondition(`{"x": {"$in": [null]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"x":null}`), docBuf))
	})
	t.Run("null member matches null array element", func(t *testing.T) {
		f, err := ParseCondition(`{"x": {"$in": [null]}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(anyenc.MustParseJson(`{"x":[null,1]}`), docBuf))
	})
	t.Run("hand-built In: nil probe by null membership", func(t *testing.T) {
		withNull := In{Values: map[string]struct{}{string(encodedNull): {}}}
		assert.True(t, withNull.Ok(nil, docBuf))
		withoutNull := In{Values: map[string]struct{}{"x": {}}}
		assert.False(t, withoutNull.Ok(nil, docBuf))
	})
	// Complements keep excluding missing fields: $nin parses to Nor-of-$eq
	// (Comp already matches missing via encodedNull), and Not inverts Ok.
	t.Run("nin null excludes missing field", func(t *testing.T) {
		f, err := ParseCondition(`{"x": {"$nin": [null]}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(doc, docBuf))
	})
	t.Run("not-in null excludes missing field", func(t *testing.T) {
		f, err := ParseCondition(`{"x": {"$not": {"$in": [null]}}}`)
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
			assert.True(t, bytes.Compare(bs[i-1].Start, bs[i].Start) < 0, "bounds should be sorted")
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
	// first: [number, 5]
	assert.Equal(t, anyenc.Tuple{byte(anyenc.TypeNumber)}, bs[0].Start)
	assert.True(t, bs[0].StartInclude)
	assert.Equal(t, anyenc.Tuple(anyenc.AppendAnyValue(nil, 5)), bs[0].End)
	assert.True(t, bs[0].EndInclude)
	// second: [10, number-bracket-end)
	assert.Equal(t, anyenc.Tuple(anyenc.AppendAnyValue(nil, 10)), bs[1].Start)
	assert.True(t, bs[1].StartInclude)
	assert.Equal(t, anyenc.Tuple{byte(anyenc.TypeNumber) + 1}, bs[1].End)
	assert.False(t, bs[1].EndInclude)
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
	b.Run("string eq", func(b *testing.B) {
		bench(b, `{"c":"test"}`)
	})
	b.Run("string eq miss", func(b *testing.B) {
		bench(b, `{"c":"other"}`)
	})
	b.Run("gt", func(b *testing.B) {
		bench(b, `{"a":{"$gt":1}}`)
	})
	b.Run("ne", func(b *testing.B) {
		bench(b, `{"a":{"$ne":5}}`)
	})
	b.Run("type miss", func(b *testing.B) {
		bench(b, `{"c":2}`) // number filter against a string field
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
		// id > 10 → one Bound from 10 (exclusive) to the number bracket's end.
		require.Len(t, bs, 1, "id > 10 must contribute exactly one Bound")
		assert.NotEmpty(t, bs[0].Start, "id > 10 Bound must have a Start value")
		assert.False(t, bs[0].StartInclude, "$gt is exclusive — StartInclude must be false")
		assert.Equal(t, anyenc.Tuple{byte(anyenc.TypeNumber) + 1}, bs[0].End, "id > 10 Bound ends at the number bracket's edge")
		assert.False(t, bs[0].EndInclude)
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

// TestComp_TypeBracketing pins MongoDB type bracketing for the ordering ops:
// a probe outside the operand's bracket is never less or greater, false/true
// share a bracket, a missing field is null (its own bracket), and the index
// bounds are the exact key image of each predicate.
func TestComp_TypeBracketing(t *testing.T) {
	a := &anyenc.Arena{}
	buf := &syncpool.DocBuffer{}
	date := a.NewDateTimeMillis(1756058400000)
	// cond is a field-level condition object ({"$gt":5}); parseCompObj
	// yields the filter a Key would wrap.
	ok := func(cond string, v *anyenc.Value) bool {
		f, err := parseCompObj(anyenc.MustParseJson(cond))
		require.NoError(t, err, cond)
		return f.Ok(v, buf)
	}
	docOk := func(cond, doc string) bool {
		return ok(cond, anyenc.MustParseJson(doc))
	}

	t.Run("dateTime field vs number literal", func(t *testing.T) {
		for _, cond := range []string{`{"$gt":1756058400}`, `{"$gte":1756058400}`, `{"$lt":1756058400}`, `{"$lte":1756058400}`, `{"$eq":1756058400}`} {
			assert.False(t, ok(cond, date), cond)
			assert.False(t, ok(cond, nil), cond+" absent field")
		}
		assert.True(t, ok(`{"$ne":1756058400}`, date))
		assert.True(t, ok(`{"$gte":{"$date":"2025-08-24T18:00:00Z"}}`, date))
		assert.False(t, ok(`{"$lt":{"$date":"2025-08-24T18:00:00Z"}}`, date))
	})
	t.Run("number vs string vs bool", func(t *testing.T) {
		assert.False(t, ok(`{"$gt":0}`, a.NewString("5")))
		assert.False(t, ok(`{"$gte":0}`, a.NewTrue()))
		assert.False(t, ok(`{"$lt":"a"}`, a.NewNumberInt(1)))
		assert.False(t, ok(`{"$lte":"a"}`, a.NewNull()))
		assert.True(t, ok(`{"$lt":"b"}`, a.NewString("a")))
		assert.True(t, ok(`{"$gt":0}`, a.NewNumberFloat64(0.5)))
		assert.False(t, ok(`{"$gt":0}`, a.NewObjectID(anyenc.ObjectID{})))
		assert.False(t, ok(`{"$gt":0}`, a.NewBinary([]byte{9})))
	})
	t.Run("bool is one bracket", func(t *testing.T) {
		assert.True(t, ok(`{"$gt":false}`, a.NewTrue()))
		assert.False(t, ok(`{"$gt":false}`, a.NewFalse()))
		assert.True(t, ok(`{"$gte":false}`, a.NewFalse()))
		assert.True(t, ok(`{"$lt":true}`, a.NewFalse()))
		assert.False(t, ok(`{"$lt":true}`, a.NewTrue()))
		assert.True(t, ok(`{"$lte":true}`, a.NewTrue()))
		assert.False(t, ok(`{"$lt":false}`, a.NewFalse()))
		assert.False(t, ok(`{"$gt":true}`, a.NewTrue()))
		assert.False(t, ok(`{"$gt":false}`, a.NewNumberInt(1)))
	})
	t.Run("null operand", func(t *testing.T) {
		for _, cond := range []string{`{"$gte":null}`, `{"$lte":null}`} {
			assert.True(t, ok(cond, nil), cond+" missing")
			assert.True(t, ok(cond, a.NewNull()), cond+" null")
			assert.False(t, ok(cond, a.NewNumberInt(0)), cond+" number")
			assert.False(t, ok(cond, a.NewFalse()), cond+" false")
		}
		for _, cond := range []string{`{"$gt":null}`, `{"$lt":null}`} {
			assert.False(t, ok(cond, nil), cond+" missing")
			assert.False(t, ok(cond, a.NewNull()), cond+" null")
			assert.False(t, ok(cond, a.NewNumberInt(0)), cond+" number")
		}
	})
	t.Run("arrays bracket per element", func(t *testing.T) {
		assert.True(t, docOk(`{"$lt":"y"}`, `[1,"x"]`))
		assert.False(t, docOk(`{"$lt":"y"}`, `[1,"z"]`))
		assert.False(t, docOk(`{"$gt":5}`, `["6",[6]]`))
		assert.True(t, docOk(`{"$gt":[1]}`, `[2]`), "array operand compares the whole array")
		assert.True(t, docOk(`{"$gt":[1]}`, `[0,[2]]`), "array operand compares array elements")
		assert.False(t, docOk(`{"$lt":5}`, `[]`))
		assert.False(t, docOk(`{"$gte":null}`, `[]`))
		assert.True(t, docOk(`{"$gte":null}`, `[null,1]`))
	})
	t.Run("not of an unsatisfiable comparison is true", func(t *testing.T) {
		assert.True(t, ok(`{"$not":{"$gt":5}}`, a.NewString("x")))
		assert.True(t, ok(`{"$not":{"$gt":5}}`, nil))
		assert.False(t, ok(`{"$not":{"$gt":5}}`, a.NewNumberInt(6)))
	})
	t.Run("degenerate operand", func(t *testing.T) {
		for _, op := range []CompOp{CompOpGt, CompOpGte, CompOpLt, CompOpLte} {
			c := &Comp{CompOp: op}
			assert.False(t, c.Ok(a.NewNumberInt(1), buf))
			assert.False(t, c.Ok(nil, buf))
			assert.Empty(t, c.IndexBounds("a", nil))
		}
	})
	t.Run("bounds are the bracket-clamped image", func(t *testing.T) {
		null, num, str, fals, tru := byte(anyenc.TypeNull), byte(anyenc.TypeNumber), byte(anyenc.TypeString), byte(anyenc.TypeFalse), byte(anyenc.TypeTrue)
		bounds := func(cond string) Bounds { return MustParseCondition(cond).IndexBounds("a", nil) }
		assert.Equal(t, Bounds{{Start: []byte{null}, End: []byte{num}, StartInclude: true}}, bounds(`{"a":{"$gte":null}}`))
		assert.Equal(t, Bounds{{Start: []byte{null}, End: []byte{null}, StartInclude: true, EndInclude: true}}, bounds(`{"a":{"$lte":null}}`))
		assert.Equal(t, Bounds{{Start: []byte{null}, End: []byte{num}}}, bounds(`{"a":{"$gt":null}}`))
		assert.Empty(t, bounds(`{"a":{"$lt":null}}`), "[null, null) is empty: no bounds, filter decides")
		assert.Empty(t, bounds(`{"a":{"$lt":false}}`))
		assert.Equal(t, Bounds{{Start: []byte{fals}, End: []byte{tru + 1}}}, bounds(`{"a":{"$gt":false}}`))
		assert.Equal(t, Bounds{{Start: []byte{fals}, End: []byte{tru}, StartInclude: true}}, bounds(`{"a":{"$lt":true}}`))
		assert.Equal(t, Bounds{{Start: []byte{tru}, End: []byte{tru + 1}}}, bounds(`{"a":{"$gt":true}}`))
		assert.Equal(t, Bounds{{Start: []byte{str}, End: anyenc.AppendAnyValue(nil, "m"), StartInclude: true}}, bounds(`{"a":{"$lt":"m"}}`))
		dt := date.MarshalTo(nil)
		assert.Equal(t, Bounds{{Start: dt, End: []byte{byte(anyenc.TypeDateTime) + 1}, StartInclude: true}}, bounds(`{"a":{"$gte":{"$date":"2025-08-24T18:00:00Z"}}}`))
		// Cross-bracket conjunctions intersect to nothing in the tight channel.
		_, empty := TightIndexBounds(MustParseCondition(`{"a":{"$gt":5,"$lt":"a"}}`), "a")
		assert.True(t, empty)
		// $ne stays unbracketed: it matches every other type.
		assert.Equal(t, 2, len(bounds(`{"a":{"$ne":5}}`)))
		assert.Empty(t, bounds(`{"a":{"$ne":5}}`)[0].Start)
		assert.Empty(t, bounds(`{"a":{"$ne":5}}`)[1].End)
	})
	t.Run("explain renders bracket edges by type name", func(t *testing.T) {
		assert.Equal(t, "Bounds{('5','<string>')}", MustParseCondition(`{"a":{"$gt":5}}`).IndexBounds("a", nil).String())
		assert.Equal(t, "Bounds{['<number>','5')}", MustParseCondition(`{"a":{"$lt":5}}`).IndexBounds("a", nil).String())
	})
}
