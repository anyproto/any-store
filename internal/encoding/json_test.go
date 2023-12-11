package encoding

import (
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
		res, err := DecodeToJSON(p, a, b)
		require.NoError(t, err)
		assert.Equal(t, res.String(), v.String())
	}

	_, err := DecodeToJSON(p, a, nil)
	require.Error(t, err)
}
