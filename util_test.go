package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_stringArrayToJson(t *testing.T) {
	arr := []string{"a", "b", "c"}
	jRes := stringArrayToJson(arr)
	assert.Equal(t, `["a","b","c"]`, jRes)
}
