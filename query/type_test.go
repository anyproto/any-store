package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestType_String(t *testing.T) {
	assert.Equal(t, "null", TypeNull.String())
	assert.Equal(t, "object", TypeObject.String())
	assert.Equal(t, "", Type(8).String())
	assert.Equal(t, "", Type(0).String())

}
