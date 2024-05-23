package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/parser"
)

func TestModifierSet_Modify(t *testing.T) {
	mod := MustParseModifier(`{"$set":{"key":"value"}}`)
	a := &fastjson.Arena{}
	p := &fastjson.Parser{}
	d, _ := parser.AnyToJSON(p, `{}`)
	res, modified, err := mod.Modify(a, d)
	require.NoError(t, err)
	assert.True(t, modified)
	assert.Equal(t, `{"key":"value"}`, res.String())
}
