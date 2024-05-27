package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/parser"
)

type modifierCase struct {
	mod     string
	doc     string
	exp     string
	changed bool
}

type modifierCaseError struct {
	mod string
	doc string
	err string
}

func testModCases(t *testing.T, cases ...modifierCase) {
	a := &fastjson.Arena{}
	p := &fastjson.Parser{}
	for _, c := range cases {
		mod := MustParseModifier(c.mod)
		d, err := parser.AnyToJSON(p, c.doc)
		require.NoError(t, err)
		res, modified, err := mod.Modify(a, d)
		require.NoError(t, err)
		assert.Equal(t, c.changed, modified, c.mod)
		assert.Equal(t, c.exp, res.String())
	}
}

func testModCasesErr(t *testing.T, cases ...modifierCaseError) {
	a := &fastjson.Arena{}
	p := &fastjson.Parser{}
	for _, c := range cases {
		mod, err := ParseModifier(c.mod)
		if err != nil {
			assert.EqualError(t, err, c.err, err)
			continue
		}
		d, err := parser.AnyToJSON(p, c.doc)
		require.NoError(t, err)
		md, modified, err := mod.Modify(a, d)
		if !assert.Error(t, err) {
			t.Log(md)
		} else {
			assert.EqualError(t, err, c.err, err)
			assert.False(t, modified)
		}
	}
}

func TestParseModifier(t *testing.T) {
	testModCasesErr(t, []modifierCaseError{
		{
			mod: `{}`,
			err: `empty modifier`,
		},
		{
			mod: `{"a":"b"}`,
			err: `unknown modifier 'a'`,
		},
		{
			mod: `[]`,
			err: `value doesn't contain object; it contains array`,
		},
		{
			mod: `{"$set":{"$a":1}}`,
			err: `unexpect identifier '$a'`,
		},
		{
			mod: `{"$unset":{"$a":1}}`,
			err: `unexpect identifier '$a'`,
		},
		{
			mod: `{"$inc":{"a":"not a num"}}`,
			err: `not numeric value for $inc in field 'a'`,
		},
	}...)

}

func TestModifierSet_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$set":{"key":"value"}}`,
				`{}`,
				`{"key":"value"}`,
				true,
			},
			{
				`{"$set":{"key":"value"}}`,
				`{"key":"value"}`,
				`{"key":"value"}`,
				false,
			},
			{
				`{"$set":{"k1":"v1", "k2":"v2"}}`,
				`{}`,
				`{"k1":"v1","k2":"v2"}`,
				true,
			},
			{
				`{"$set":{"k1":"v1", "k2":"v2"}}`,
				`{"k1":"v1","k2":"v3"}`,
				`{"k1":"v1","k2":"v2"}`,
				true,
			},
			{
				`{"$set":{"key.sKey":"value"}}`,
				`{}`,
				`{"key":{"sKey":"value"}}`,
				true,
			},
			{
				`{"$set":{"key.sKey":"value"}}`,
				`{"key":{"a":"b"}}`,
				`{"key":{"a":"b","sKey":"value"}}`,
				true,
			},
			{
				`{"$set":{"key.0":"value"}}`,
				`{"key":["prev"]}`,
				`{"key":["value"]}`,
				true,
			},
			{
				`{"$set":{"key.1":"value"}}`,
				`{"key":["prev"]}`,
				`{"key":["prev","value"]}`,
				true,
			},
			{
				`{"$set":{"key.1":"value"}}`,
				`{"key":["prev"]}`,
				`{"key":["prev","value"]}`,
				true,
			},
			{
				`{"$set":{"key.1.sKey":"value"}}`,
				`{"key":[1,{"a":"b"}]}`,
				`{"key":[1,{"a":"b","sKey":"value"}]}`,
				true,
			},
		}...)
	})

	t.Run("error", func(t *testing.T) {
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$set":{"key.sKey":"value"}}`,
				`{"key":[1,2,3]}`,
				"cannot create field 'sKey' in element [1,2,3]",
			},
			{
				`{"$set":{"key.sKey.key":"value"}}`,
				`{"key":[1,2,3]}`,
				"cannot create field 'sKey' in element [1,2,3]",
			},
			{
				`{"$set":{"key.sKey.key":"value"}}`,
				`{"key":1}`,
				"cannot create field 'sKey' in element 1",
			},
		}...)
	})
}

func TestModifierUnset_Modify(t *testing.T) {
	testModCases(t, []modifierCase{
		{
			`{"$unset":{"key":""}}`,
			`{"a":"b"}`,
			`{"a":"b"}`,
			false,
		},
		{
			`{"$unset":{"key":""}}`,
			`{"a":"b", "key":"value"}`,
			`{"a":"b"}`,
			true,
		},
		{
			`{"$unset":{"key.sKey":""}}`,
			`{"key":{"sKey":"value","a":"b"}}`,
			`{"key":{"a":"b"}}`,
			true,
		},
		{
			`{"$unset":{"key.1":""}}`,
			`{"key":[1,2,3]}`,
			`{"key":[1,3]}`,
			true,
		},
		{
			`{"$unset":{"key.sKey":""}}`,
			`{}`,
			`{}`,
			false,
		},
	}...)
}

func TestModifierInc_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$inc":{"key":2}}`,
				`{}`,
				`{"key":2}`,
				true,
			},
			{
				`{"$inc":{"key.sKey":2}}`,
				`{}`,
				`{"key":{"sKey":2}}`,
				true,
			},
			{
				`{"$inc":{"key":-2}}`,
				`{"key":42}`,
				`{"key":40}`,
				true,
			},
			{
				`{"$inc":{"a":1, "b":2}}`,
				`{"a":2}`,
				`{"a":3,"b":2}`,
				true,
			},
		}...)
	})

	t.Run("error", func(t *testing.T) {
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$inc":{"a":1}}`,
				`{"a":"2"}`,
				`not numeric value '"2"'`,
			},
		}...)
	})

}
