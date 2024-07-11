package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/encoding"
)

func TestComp(t *testing.T) {
	a := &fastjson.Arena{}
	t.Run("eq", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, EqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(1)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(2)))
			assert.False(t, cmp.Ok(a.NewNumberInt(0)))
			assert.False(t, cmp.Ok(a.NewNumberInt(-1)))
			assert.False(t, cmp.Ok(a.NewString("1")))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bound{
				Start:        encoding.AppendAnyValue(nil, 1),
				End:          encoding.AppendAnyValue(nil, 1),
				StartInclude: true,
				EndInclude:   true,
			}, bs[0])
		})
	})
	t.Run("eq_array", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, EqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(fastjson.MustParse(`[3,2,1]`)))
			assert.True(t, cmp.Ok(fastjson.MustParse(`[1]`)))
			assert.True(t, cmp.Ok(fastjson.MustParse(`[1,2]`)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(fastjson.MustParse(`[]`)))
			assert.False(t, cmp.Ok(fastjson.MustParse(`[0,2,3]`)))
			assert.False(t, cmp.Ok(a.NewNumberInt(-1)))
			assert.False(t, cmp.Ok(fastjson.MustParse(`["1",2]`)))
		})
		t.Run("array-array", func(t *testing.T) {
			aCmp := Comp{CompOp: CompOpEq, EqValue: encoding.AppendJSONValue(nil, fastjson.MustParse(`[1,2,3]`))}
			assert.True(t, aCmp.Ok(fastjson.MustParse(`[1,2,3]`)))
			assert.True(t, aCmp.Ok(fastjson.MustParse(`[[1,2,3], 1]`)))
		})
		t.Run("empty array", func(t *testing.T) {
			aCmp := Comp{CompOp: CompOpEq, EqValue: encoding.AppendJSONValue(nil, fastjson.MustParse(`[]`))}
			assert.True(t, aCmp.Ok(fastjson.MustParse(`[]`)))
		})
	})
	t.Run("ne", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpNe, EqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2)))
			assert.True(t, cmp.Ok(a.NewNumberInt(0)))
			assert.True(t, cmp.Ok(a.NewNumberInt(-1)))
			assert.True(t, cmp.Ok(fastjson.MustParse(`[0,2,3]`)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1)))
			assert.False(t, cmp.Ok(fastjson.MustParse(`[0,1,3]`)))
		})
		t.Run("array-array", func(t *testing.T) {
			aCmp := Comp{CompOp: CompOpNe, EqValue: encoding.AppendJSONValue(nil, fastjson.MustParse(`[1,2,3]`))}
			assert.False(t, aCmp.Ok(fastjson.MustParse(`[1,2,3]`)))
			assert.False(t, aCmp.Ok(fastjson.MustParse(`[[1,2,3], 1]`)))
			assert.True(t, aCmp.Ok(fastjson.MustParse(`[1,2]`)))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			require.Len(t, bs, 2)
			assert.Equal(t, Bounds{
				{
					End: encoding.AppendAnyValue(nil, 1),
				},
				{
					Start: encoding.AppendAnyValue(nil, 1),
				},
			}, bs)
		})
	})
	t.Run("gt", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpGt, EqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2)))
			assert.True(t, cmp.Ok(a.NewNumberInt(3)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(1.1)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1)))
			assert.False(t, cmp.Ok(a.NewNumberInt(0)))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					Start: encoding.AppendAnyValue(nil, 1),
				},
			}, bs)
		})
	})
	t.Run("gte", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpGte, EqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2)))
			assert.True(t, cmp.Ok(a.NewNumberInt(3)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(1)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(0)))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					Start:        encoding.AppendAnyValue(nil, 1),
					StartInclude: true,
				},
			}, bs)
		})
	})
	t.Run("lt", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpLt, EqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(0)))
			assert.True(t, cmp.Ok(a.NewNumberInt(-1)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(0.9)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1)))
			assert.False(t, cmp.Ok(a.NewNumberInt(2)))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					End: encoding.AppendAnyValue(nil, 1),
				},
			}, bs)
		})
	})
	t.Run("lte", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpLte, EqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(1)))
			assert.True(t, cmp.Ok(a.NewNumberInt(0)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(0.9)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(2)))
		})
		t.Run("bounds", func(t *testing.T) {
			bs := cmp.IndexBounds("", nil)
			assert.Equal(t, Bounds{
				{
					End:        encoding.AppendAnyValue(nil, 1),
					EndInclude: true,
				},
			}, bs)
		})
	})
	t.Run("okBytes", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, EqValue: encoding.AppendAnyValue(nil, 1)}
		assert.True(t, cmp.OkBytes(encoding.AppendAnyValue(nil, 1)))
		assert.False(t, cmp.OkBytes(encoding.AppendAnyValue(nil, 2)))
	})
}

func TestAnd(t *testing.T) {
	f, err := ParseCondition(`{"a":1, "b":"2"}`)
	require.NoError(t, err)
	t.Run("ok", func(t *testing.T) {
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":2,"b":"2","c":4}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":2,"c":4}`)))
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
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"3","c":4}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":12,"b":2,"c":4}`)))
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
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"3","c":4}`)))
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":12,"b":2,"c":4}`)))
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
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
	})
}

func TestNot(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$not":{"$eq":2}}}`)
	require.NoError(t, err)
	t.Run("ok", func(t *testing.T) {
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"3","c":4}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":2,"b":2,"c":4}`)))
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
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":2,"b":[3,2,1],"c":"test"}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":[3,2],"c":"test"}`)))
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
			assert.True(t, f.Ok(fastjson.MustParse(`{"a":1}`)))
			assert.False(t, f.Ok(fastjson.MustParse(`{"b":1}`)))
		})
		t.Run("false", func(t *testing.T) {
			f, err := ParseCondition(`{"a":{"$exists":false}}`)
			require.NoError(t, err)
			assert.False(t, f.Ok(fastjson.MustParse(`{"a":1}`)))
			assert.True(t, f.Ok(fastjson.MustParse(`{"b":1}`)))
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
		assert.True(t, f.Ok(fastjson.MustParse(`{"a":1}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"a":"1"}`)))
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
		assert.True(t, f.Ok(fastjson.MustParse(`{"name": "a"}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"name": "A"}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"name":"b"}`)))
	})
	t.Run("ok - complex expression", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^(?i)a"}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(fastjson.MustParse(`{"name": "baaa"}`)))
		assert.True(t, f.Ok(fastjson.MustParse(`{"name": "A"}`)))
		assert.True(t, f.Ok(fastjson.MustParse(`{"name": "a"}`)))
	})
	t.Run("ok - array", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^(?i)a"}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(fastjson.MustParse(`{"name": ["A", "B", "C"]}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"name": ["baaa"]}`)))
		assert.True(t, f.Ok(fastjson.MustParse(`{"name": ["baaa", "a"]}`)))
	})
	t.Run("ok - number", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^a(?i)"}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(fastjson.MustParse(`{"name":1}`)))
	})
	t.Run("ok - nil value", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^a(?i)"}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(nil))
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
		assert.Equal(t, "prefix.test", append(bounds[0].Start, 0).String())
	})
	t.Run("index: ^prefix\\.test{1}* - return prefix.test", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^prefix\.test{a-zA-z}*"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
		assert.Equal(t, "prefix.test", append(bounds[0].Start, 0).String())
	})
	t.Run("index: ^prefix+ - return prefix", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^prefix+"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
		assert.Equal(t, "prefix", append(bounds[0].Start, 0).String())
	})
	t.Run("index: ^\\.a* - return prefix", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$regex": "^\.a*"}}`)
		require.NoError(t, err)
		bounds := f.IndexBounds("name", Bounds{})
		assert.Len(t, bounds, 1)
		assert.Equal(t, ".a", append(bounds[0].Start, 0).String())
	})
}

func TestSize(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$size": 2}}`)
		require.NoError(t, err)
		assert.True(t, f.Ok(fastjson.MustParse(`{"name": [1,2]}`)))
	})
	t.Run("value nil", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$size": 2}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(fastjson.MustParse(`{"arr": [1,2]}`)))
	})
	t.Run("not ok", func(t *testing.T) {
		f, err := ParseCondition(`{"name":{"$size": 2}}`)
		require.NoError(t, err)
		assert.False(t, f.Ok(fastjson.MustParse(`{"name": "a"}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"name": []}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"name": [1]}`)))
		assert.False(t, f.Ok(fastjson.MustParse(`{"name": [1,2,3]}`)))
	})
	t.Run("error parsing expression - expected number", func(t *testing.T) {
		_, err := ParseCondition(`{"name":{"$size": "2"}}`)
		require.Error(t, err)
	})
}

func BenchmarkFilter_Ok(b *testing.B) {
	doc := fastjson.MustParse(`{"a":2,"b":[3,2,1],"c":"test"}`)
	bench := func(b *testing.B, query string) {
		f, err := ParseCondition(query)
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.Ok(doc)
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
