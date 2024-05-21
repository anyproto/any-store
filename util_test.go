package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func Test_stringArrayToJson(t *testing.T) {
	arr := []string{"a", "b", "c"}
	jRes := stringArrayToJson(&fastjson.Arena{}, arr)
	assert.Equal(t, `["a","b","c"]`, jRes)

	resArr, err := jsonToStringArray(&fastjson.Parser{}, jRes)
	require.NoError(t, err)
	assert.Equal(t, arr, resArr)
}
