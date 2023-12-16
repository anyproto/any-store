package key

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestKey_AppendJSON(t *testing.T) {
	ns := NewNS("/test/prefix")
	k1 := ns.GetKey().AppendJSON(fastjson.MustParse(`3.33`)).AppendJSON(fastjson.MustParse(`"string"`))
	k2 := ns.GetKey().AppendJSON(fastjson.MustParse(`12.33`)).AppendJSON(fastjson.MustParse(`"string2"`))
	assert.Equal(t, -1, bytes.Compare(k1, k2))
	assert.Equal(t, "/test/prefix/3.33/string", k1.String())
	assert.Equal(t, "/test/prefix/12.33/string2", k2.String())
}

func TestKey_ReadJSONValue(t *testing.T) {
	var jsons = []string{
		`true`, `false`, `null`, `"string"`, `3.14`, `[1,2,3]`, `{"a":"b"}`,
	}

	ns := NewNS("/test/prefix")
	k := ns.GetKey()
	for _, j := range jsons {
		k = k.AppendJSON(fastjson.MustParse(j))
	}
	var result []string
	require.NoError(t, k.ReadJSONValue(ns, &fastjson.Parser{}, &fastjson.Arena{}, func(v *fastjson.Value) error {
		result = append(result, v.String())
		return nil
	}))
	assert.Equal(t, jsons, result)
}
