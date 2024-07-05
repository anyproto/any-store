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

func TestModifierRename_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$rename":{"old":"new"}}`,
				`{}`,
				`{}`,
				false,
			},
			{
				`{"$rename":{"old":"old"}}`,
				`{"old":"value"}`,
				`{"old":"value"}`,
				false,
			},
			{
				`{"$rename":{"old":"new"}}`,
				`{"old":"value"}`,
				`{"new":"value"}`,
				true,
			},
			{
				`{"$rename":{"old":"new"}}`,
				`{"old":"value", "new":"value1"}`,
				`{"new":"value"}`,
				true,
			},
		}...)
	})
}

func TestModifierPop_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$pop":{"arr": -1}}`,
				`{}`,
				`{}`,
				false,
			},
			{
				`{"$pop":{"arr": -1}}`,
				`{"arr": [1]}`,
				`{"arr":[]}`,
				true,
			},
			{
				`{"$pop":{"arr": 1}}`,
				`{"arr": [1]}`,
				`{"arr":[]}`,
				true,
			},
			{
				`{"$pop":{"arr": -1}}`,
				`{"arr": [1,2,3]}`,
				`{"arr":[2,3]}`,
				true,
			},
			{
				`{"$pop":{"arr": 1}}`,
				`{"arr": [1,2,3]}`,
				`{"arr":[1,2]}`,
				true,
			},
			{
				`{"$pop":{"arr": 1}}`,
				`{"arr": []}`,
				`{"arr":[]}`,
				false,
			},
		}...)
	})
	t.Run("error", func(t *testing.T) {
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$pop":{"arr":1}}`,
				`{"arr":"2"}`,
				`failed to pop item, value doesn't contain array; it contains string`,
			},
			{
				`{"$pop":{"arr":""}}`,
				`{"arr":[1,2]}`,
				`failed to pop item, value doesn't contain number; it contains string`,
			},
			{
				`{"$pop":{"arr":2}}`,
				`{"arr":[1,2]}`,
				`failed to pop item: wrong argument`,
			},
		}...)
	})
}

func TestModifierPush_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$push":{"arr": -1}}`,
				`{}`,
				`{}`,
				false,
			},
			{
				`{"$push":{"arr": 1}}`,
				`{"arr": [1]}`,
				`{"arr":[1,1]}`,
				true,
			},
		}...)
	})
	t.Run("error", func(t *testing.T) {
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$push":{"arr":1}}`,
				`{"arr":"2"}`,
				`failed to pop item, value doesn't contain array; it contains string`,
			},
		}...)
	})
}

func TestModifierPull_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$pull":{"arr": 1}}`,
				`{}`,
				`{}`,
				false,
			},
			{
				`{"$pull":{"arr": 1}}`,
				`{"arr": []}`,
				`{"arr":[]}`,
				false,
			},
			{
				`{"$pull":{"arr": 1}}`,
				`{"arr": [2, 3, 1, 4]}`,
				`{"arr":[2,3,4]}`,
				true,
			},
			{
				`{"$pull":{"arr": 1}}`,
				`{"arr": [2, 3, 4]}`,
				`{"arr":[2,3,4]}`,
				false,
			},
			{
				`{"$pull":{"arr": {"id":"123","name":"one"}}}`,
				`{"arr": [{"id":"123","name":"two"},{"id":"321","name":"one"},{"id":"123","name":"one"}]}`,
				`{"arr":[{"id":"123","name":"two"},{"id":"321","name":"one"}]}`,
				true,
			},
			{
				`{"$pull":{"arr": {"$in":[2,6]}}}`,
				`{"arr": [1,2,3,4,5,6,7]}`,
				`{"arr":[1,3,4,5,7]}`,
				true,
			},
			{
				`{"$pull":{"arr": {"$gte":6}}}`,
				`{"arr": [1,2,3,4,5,6,7]}`,
				`{"arr":[1,2,3,4,5]}`,
				true,
			},
			{
				`{"$pull":{"arr": {"$gt":6}}}`,
				`{"arr": [1,2,3,4,5,6,7]}`,
				`{"arr":[1,2,3,4,5,6]}`,
				true,
			},
			{
				`{"$pull":{"arr": {"$lte":6}}}`,
				`{"arr": [1,2,3,4,5,6,7]}`,
				`{"arr":[7]}`,
				true,
			},
			{
				`{"$pull":{"arr": {"$lt":6}}}`,
				`{"arr": [1,2,3,4,5,6,7]}`,
				`{"arr":[6,7]}`,
				true,
			},
		}...)
	})
	t.Run("error", func(t *testing.T) {
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$pull":{"arr":1}}`,
				`{"arr":"2"}`,
				`failed to pop item, value doesn't contain array; it contains string`,
			},
		}...)
	})
}

func TestModifierPullAll_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$pullAll":{"arr": [1]}}`,
				`{}`,
				`{}`,
				false,
			},
			{
				`{"$pullAll":{"arr": [1]}}`,
				`{"arr": []}`,
				`{"arr":[]}`,
				false,
			},
			{
				`{"$pullAll":{"arr": [1,2,3,4]}}`,
				`{"arr": [1,2,3,4,5]}`,
				`{"arr":[5]}`,
				true,
			},
		}...)
	})
	t.Run("error", func(t *testing.T) {
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$pullAll":{"arr":1}}`,
				`{"arr":"2"}`,
				`failed to pop item, value doesn't contain array; it contains number`,
			},
			{
				`{"$pullAll":{"arr":1}}`,
				`{"arr":[1,2,3,4,5]}`,
				`failed to pop item, value doesn't contain array; it contains number`,
			},
		}...)
	})
}

func TestModifierAddToSet_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$addToSet":{"arr": [1]}}`,
				`{}`,
				`{}`,
				false,
			},
			{
				`{"$addToSet":{"arr": [1]}}`,
				`{"arr": []}`,
				`{"arr":[[1]]}`,
				true,
			},
			{
				`{"$addToSet":{"arr": "test"}}`,
				`{"arr": [1,2,3,4,5]}`,
				`{"arr":[1,2,3,4,5,"test"]}`,
				true,
			},
			{
				`{"$addToSet":{"arr": 5}}`,
				`{"arr": [1,2,3,4,5]}`,
				`{"arr":[1,2,3,4,5]}`,
				false,
			},
		}...)
	})
	t.Run("error", func(t *testing.T) {
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$addToSet":{"arr":1}}`,
				`{"arr":"2"}`,
				`failed to pop item, value doesn't contain array; it contains string`,
			},
		}...)
	})
}

func BenchmarkModifier(b *testing.B) {
	bench := func(b *testing.B, query string) {
		doc := fastjson.MustParse(`{"a":2,"b":[3,2,1],"c":"test"}`)
		a := &fastjson.Arena{}
		p := &fastjson.Parser{}
		d, err := parser.AnyToJSON(p, doc)
		modifier, err := ParseModifier(query)
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _, err := modifier.Modify(a, d)
			require.NoError(b, err)
		}
	}

	b.Run("set", func(b *testing.B) {
		bench(b, `{"$set":{"a":3}}`)
	})
	b.Run("unset", func(b *testing.B) {
		bench(b, `{"$unset":{"a":3}}`)
	})
	b.Run("inc", func(b *testing.B) {
		bench(b, `{"$inc":{"a":2}}`)
	})
	b.Run("rename", func(b *testing.B) {
		bench(b, `{"$rename":{"a":"b"}}`)
	})
	b.Run("pull", func(b *testing.B) {
		bench(b, `{"$pull":{"b":3}}`)
	})
	b.Run("pull query", func(b *testing.B) {
		bench(b, `{"$pull":{"b": {"$in":[3,1]}}}`)
	})
	b.Run("pop", func(b *testing.B) {
		bench(b, `{"$pop":{"b":1}}`)
	})
	b.Run("push", func(b *testing.B) {
		bench(b, `{"$push":{"b":6}}`)
	})
	b.Run("pull all", func(b *testing.B) {
		bench(b, `{"$pullAll":{"b":[1,2]}}`)
	})
	b.Run("add to set", func(b *testing.B) {
		bench(b, `{"$addToSet":{"b":[1,2,5]}}`)
	})
}
