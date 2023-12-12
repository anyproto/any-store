package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
)

func TestComp_Ok(t *testing.T) {
	a := &fastjson.Arena{}
	t.Run("eq", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, eqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(1)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(2)))
			assert.False(t, cmp.Ok(a.NewNumberInt(0)))
			assert.False(t, cmp.Ok(a.NewNumberInt(-1)))
			assert.False(t, cmp.Ok(a.NewString("1")))
		})
	})
	t.Run("eq_array", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpEq, eqValue: encoding.AppendAnyValue(nil, 1)}
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
	})
	t.Run("ne", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpNe, eqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2)))
			assert.True(t, cmp.Ok(a.NewNumberInt(0)))
			assert.True(t, cmp.Ok(a.NewNumberInt(-1)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1)))
		})
	})
	t.Run("gt", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpGt, eqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2)))
			assert.True(t, cmp.Ok(a.NewNumberInt(3)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(1.1)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1)))
			assert.False(t, cmp.Ok(a.NewNumberInt(0)))
		})
	})
	t.Run("gte", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpGte, eqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(2)))
			assert.True(t, cmp.Ok(a.NewNumberInt(3)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(1)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(0)))
		})
	})
	t.Run("lt", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpLt, eqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(0)))
			assert.True(t, cmp.Ok(a.NewNumberInt(-1)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(0.9)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(1)))
			assert.False(t, cmp.Ok(a.NewNumberInt(2)))
		})
	})
	t.Run("lte", func(t *testing.T) {
		cmp := Comp{CompOp: CompOpLte, eqValue: encoding.AppendAnyValue(nil, 1)}
		t.Run("true", func(t *testing.T) {
			assert.True(t, cmp.Ok(a.NewNumberInt(1)))
			assert.True(t, cmp.Ok(a.NewNumberInt(0)))
			assert.True(t, cmp.Ok(a.NewNumberFloat64(0.9)))
		})
		t.Run("false", func(t *testing.T) {
			assert.False(t, cmp.Ok(a.NewNumberInt(2)))
		})
	})
}

func TestAnd_Ok(t *testing.T) {
	f, err := ParseCondition(`{"a":1, "b":"2"}`)
	require.NoError(t, err)
	assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
	assert.False(t, f.Ok(fastjson.MustParse(`{"a":2,"b":"2","c":4}`)))
	assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":2,"c":4}`)))
}

func TestOr_Ok(t *testing.T) {
	f, err := ParseCondition(`{"$or":[{"a":1},{"b":"2"}]}`)
	require.NoError(t, err)
	assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
	assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"3","c":4}`)))
	assert.False(t, f.Ok(fastjson.MustParse(`{"a":12,"b":2,"c":4}`)))
}

func TestNor_Ok(t *testing.T) {
	f, err := ParseCondition(`{"$nor":[{"a":1},{"b":"2"}]}`)
	require.NoError(t, err)
	assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
	assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"3","c":4}`)))
	assert.True(t, f.Ok(fastjson.MustParse(`{"a":12,"b":2,"c":4}`)))
}

func TestNot_Ok(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$not":{"$eq":2}}}`)
	require.NoError(t, err)
	assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"2","c":4}`)))
	assert.True(t, f.Ok(fastjson.MustParse(`{"a":1,"b":"3","c":4}`)))
	assert.False(t, f.Ok(fastjson.MustParse(`{"a":2,"b":2,"c":4}`)))
}

func TestComplex(t *testing.T) {
	f, err := ParseCondition(`{"a":{"$in":[1,2,3]}, "b":{"$all":[1,2]}, "c": "test"}`)
	require.NoError(t, err)
	assert.True(t, f.Ok(fastjson.MustParse(`{"a":2,"b":[3,2,1],"c":"test"}`)))
	assert.False(t, f.Ok(fastjson.MustParse(`{"a":1,"b":[3,2],"c":"test"}`)))
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
