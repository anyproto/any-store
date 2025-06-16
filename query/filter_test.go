package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/syncpool"
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
