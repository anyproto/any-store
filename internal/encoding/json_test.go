package encoding

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestAppendJSONValue(t *testing.T) {
	var (
		a = &fastjson.Arena{}
		p = &fastjson.Parser{}
	)

	var values = []*fastjson.Value{
		a.NewString("string"),
		a.NewNull(),
		a.NewFalse(),
		a.NewTrue(),
		a.NewNumberFloat64(42.2),
		fastjson.MustParse(`["a","b","c"]`),
		fastjson.MustParse(`{"a":1}`),
	}

	for _, v := range values {
		b := AppendJSONValue(nil, v)
		res, n, err := DecodeToJSON(p, a, b)
		assert.True(t, n > 0)
		t.Log(v.String(), n)
		require.NoError(t, err)
		assert.Equal(t, res.String(), v.String())
	}

	_, _, err := DecodeToJSON(p, a, nil)
	require.Error(t, err)
}

func TestAppendInvertedJSON(t *testing.T) {
	var (
		a = &fastjson.Arena{}
	)

	var values = [][2]*fastjson.Value{
		{a.NewString("astring"), a.NewString("bstring")},
		{a.NewNull(), a.NewFalse()},
		{a.NewNumberFloat64(42.2), a.NewNumberFloat64(43.3)},
		{fastjson.MustParse(`["a","b","c"]`), fastjson.MustParse(`["d","b","c"]`)},
		{fastjson.MustParse(`{"a":1}`), fastjson.MustParse(`{"b":1}`)},
	}

	for _, v := range values {
		b1 := AppendInvertedJSON(nil, v[0])
		b2 := AppendInvertedJSON(nil, v[1])
		assert.Equal(t, 1, bytes.Compare(b1, b2), v[0].String())
	}

}
