package key

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
)

func TestKey_AppendJSON(t *testing.T) {
	k1 := New().AppendJSON(fastjson.MustParse(`3.33`)).AppendJSON(fastjson.MustParse(`"string"`))
	k2 := New().AppendJSON(fastjson.MustParse(`12.33`)).AppendJSON(fastjson.MustParse(`"string2"`))
	assert.Equal(t, -1, bytes.Compare(k1, k2))
	assert.Equal(t, "3.33/string", k1.String())
	assert.Equal(t, "12.33/string2", k2.String())
}

func TestKey_ReadJSONValue(t *testing.T) {
	var jsons = []string{
		`true`, `false`, `null`, `"string"`, `3.14`, `[1,2,3]`, `{"a":"b"}`,
	}

	k := New()
	for _, j := range jsons {
		k = k.AppendJSON(fastjson.MustParse(j))
	}
	var result []string
	require.NoError(t, k.ReadJSONValue(&fastjson.Parser{}, &fastjson.Arena{}, func(v *fastjson.Value) error {
		result = append(result, v.String())
		return nil
	}))
	assert.Equal(t, jsons, result)
}

func TestKey_ReadByteValues(t *testing.T) {
	var jsons = []string{
		`true`, `false`, `null`, `"string"`, `3.14`, `[1,2,3]`, `{"a":"b"}`,
	}
	var expected = make([][]byte, 0, len(jsons))

	k := New()
	for _, j := range jsons {
		jv := fastjson.MustParse(j)
		k = k.AppendJSON(jv)
		expected = append(expected, encoding.AppendJSONValue(nil, jv))
	}

	var result [][]byte
	require.NoError(t, k.ReadByteValues(func(b []byte) error {
		result = append(result, bytes.Clone(b))
		return nil
	}))
	assert.Equal(t, expected, result)
}
