package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/internal/parser"
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
	a := &anyenc.Arena{}
	for _, c := range cases {
		mod := MustParseModifier(c.mod)
		d, err := parser.Parse(c.doc)
		require.NoError(t, err)
		res, modified, err := mod.Modify(a, d)
		require.NoError(t, err)
		assert.Equal(t, c.changed, modified, c.mod)
		assert.Equal(t, c.exp, res.String())
	}
}

func testModCasesErr(t *testing.T, cases ...modifierCaseError) {
	a := &anyenc.Arena{}
	for _, c := range cases {
		mod, err := ParseModifier(c.mod)
		if err != nil {
			assert.EqualError(t, err, c.err, err)
			continue
		}
		d, err := parser.Parse(c.doc)
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
			err: `parse modifier: empty modifier`,
		},
		{
			mod: `{"a":"b"}`,
			err: `parse modifier: unknown modifier: a (at a)`,
		},
		{
			mod: `[]`,
			err: `parse modifier: modifier must be an object`,
		},
		{
			mod: `{"$set":{"$a":1}}`,
			err: `parse modifier: unexpected operator $a, expected a field path (at $set.$a)`,
		},
		{
			mod: `{"$unset":{"$a":1}}`,
			err: `parse modifier: unexpected operator $a, expected a field path (at $unset.$a)`,
		},
		{
			mod: `{"$inc":{"a":"not a num"}}`,
			err: `parse modifier: $inc requires a numeric value, got "not a num" (at $inc.a)`,
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
				true,
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
			{
				`{"$rename":{"old":"new.new1"}}`,
				`{"old":"value"}`,
				`{"new":{"new1":"value"}}`,
				true,
			},
			{
				`{"$rename":{"old":"new.new1"}}`,
				`{"old":"value", "new":{"new1":"value"}}`,
				`{"new":{"new1":"value"}}`,
				true,
			},
			{
				`{"$rename":{"old.old1":"new.new1"}}`,
				`{"old": {"old1": "value"}}`,
				`{"old":{},"new":{"new1":"value"}}`,
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
				`parse modifier: $pop must be 1 (last) or -1 (first), got "" (at $pop.arr)`,
			},
			{
				`{"$pop":{"arr":2}}`,
				`{"arr":[1,2]}`,
				`parse modifier: $pop must be 1 (last) or -1 (first), got 2 (at $pop.arr)`,
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
			{
				`{"$pull":{"arr": {"$regex":"^it","$options":"i"}}}`,
				`{"arr": ["item","ITEM-2","x"]}`,
				`{"arr":["x"]}`,
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
				`parse modifier: $pullAll must be an array, got 1 (at $pullAll.arr)`,
			},
			{
				`{"$pullAll":{"arr":1}}`,
				`{"arr":[1,2,3,4,5]}`,
				`parse modifier: $pullAll must be an array, got 1 (at $pullAll.arr)`,
			},
		}...)
	})
}

func TestModifierAddToSet_Modify(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		testModCases(t, []modifierCase{
			{
				`{"$addToSet":{"arr": 1}}`,
				`{}`,
				`{"arr":[1]}`,
				true,
			},
			{
				`{"$addToSet":{"arr": [1]}}`,
				`{}`,
				`{"arr":[[1]]}`,
				true,
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
		a := &anyenc.Arena{}
		d, err := parser.Parse(doc)
		modifier, err := ParseModifier(query)
		require.NoError(b, err)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			a.Reset()
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
	b.Run("rename - object", func(b *testing.B) {
		bench(b, `{"$rename":{"a":"d.c"}}`)
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
	b.Run("set past the padding limit", func(b *testing.B) {
		// An object key past the limit: the modifier is staged.
		bench(b, `{"$set":{"m.1500001":1}}`)
	})
}

// $pull with a condition uses query semantics: ordering ops are type-bracketed,
// so {"$gt":5} removes only numbers above 5, never a string element.
func TestPullConditionIsTypeBracketed(t *testing.T) {
	m, err := ParseModifier(`{"$pull":{"tags":{"$gt":5}}}`)
	require.NoError(t, err)
	a := &anyenc.Arena{}
	v := anyenc.MustParseJson(`{"tags":["a",3,7,true,null]}`)
	res, modified, err := m.Modify(a, v)
	require.NoError(t, err)
	assert.True(t, modified)
	assert.Equal(t, `{"tags":["a",3,true,null]}`, res.String())
}

// A modifier whose path indexes past the end of an array pads it with nulls,
// up to MongoDB's limit of 1,500,000 padding elements; past it the modifier
// returns an error and the document is not changed.
func TestModifier_ArrayPaddingIsBounded(t *testing.T) {
	const limit = 1_500_000
	a := &anyenc.Arena{}
	for _, tc := range []struct {
		name, modFmt string
	}{
		{"$set", `{"$set":{"arr.%d":1}}`},
		{"$inc", `{"$inc":{"arr.%d":1}}`},
		{"$set through an intermediate segment", `{"$set":{"arr.%d.x":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// arr has two elements: index 2+limit needs exactly limit nulls.
			doc := anyenc.MustParseJson(`{"arr":[1,2]}`)
			res, modified, err := MustParseModifier(fmt.Sprintf(tc.modFmt, 2+limit)).Modify(a, doc)
			require.NoError(t, err)
			assert.True(t, modified)
			assert.Len(t, res.GetArray("arr"), 2+limit+1)

			doc = anyenc.MustParseJson(`{"arr":[1,2]}`)
			_, modified, err = MustParseModifier(fmt.Sprintf(tc.modFmt, 2+limit+1)).Modify(a, doc)
			require.ErrorContains(t, err, "can't backfill more than 1500000 elements")
			assert.False(t, modified)
			assert.Equal(t, `{"arr":[1,2]}`, doc.String())
		})
	}
}

// A modifier refused by the padding limit leaves the document it was given
// as it was, whichever of its operations is refused: a $rename whose
// destination is refused keeps its source, and the operations before the
// refused one in a chain are not applied either.
func TestModifier_PaddingRefusalLeavesDocumentUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name, doc, mod string
	}{
		{"$rename destination", `{"x":7,"arr":[]}`, `{"$rename":{"x":"arr.1500001"}}`},
		{"$rename destination, nested", `{"x":7,"o":{"arr":[]}}`, `{"$rename":{"x":"o.arr.1500001.y"}}`},
		{"$set fields after a change", `{"arr":[1,2]}`, `{"$set":{"a":1,"arr.1500003":1}}`},
		{"operators after a change", `{"n":1,"arr":[1,2]}`, `{"$inc":{"n":1},"$unset":{"x":""},"$set":{"arr.1500003":1}}`},
		{"$unset before a refusal", `{"x":1,"arr":[1,2]}`, `{"$unset":{"x":""},"$addToSet":{"arr.1500003":1}}`},
		{"an index too large for an int", `{"a":0,"arr":[1,2]}`, `{"$set":{"a":1,"arr.99999999999999999999":1}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc := anyenc.MustParseJson(tc.doc)
			res, modified, err := MustParseModifier(tc.mod).Modify(&anyenc.Arena{}, doc)
			require.Error(t, err)
			assert.Nil(t, res)
			assert.False(t, modified)
			assert.Equal(t, anyenc.MustParseJson(tc.doc).String(), doc.String())
		})
	}
}

// A modifier that could be refused but is not runs once, on a copy of the
// document it was given: the copy is the result, the document is unchanged,
// and an operation that changes a value an earlier one stored sees it once.
func TestModifier_PaddingStagedAppliesOnce(t *testing.T) {
	const src = `{"x":7,"arr":[1,2]}`
	doc := anyenc.MustParseJson(src)
	mod := MustParseModifier(`{"$set":{"o":{"n":1},"arr.1500002":true},"$inc":{"o.n":1},"$rename":{"x":"y"}}`)
	res, modified, err := mod.Modify(&anyenc.Arena{}, doc)
	require.NoError(t, err)
	assert.True(t, modified)
	assert.NotSame(t, doc, res, "the result is a copy")
	assert.Equal(t, anyenc.MustParseJson(src).String(), doc.String(), "the document given is unchanged")
	assert.Equal(t, 2, res.GetInt("o", "n"))
	assert.Equal(t, 7, res.GetInt("y"))
	assert.Nil(t, res.Get("x"))
	arr := res.GetArray("arr")
	require.Len(t, arr, 1_500_003)
	assert.Equal(t, anyenc.TypeTrue, arr[len(arr)-1].Type())
}

// The copy a staged modifier runs on is the document as it is in memory,
// not an encode and parse of it: a branch nested deeper than the parser's 512
// levels does not fail the modifier, and a key "\x1f", which anyenc's
// encoding reads back as "", keeps its value, so a refusal on a path through
// it is seen and leaves the document unchanged.
func TestModifier_PaddingStagedCopiesTheDocumentAsItIs(t *testing.T) {
	t.Run("a branch deeper than 512 levels", func(t *testing.T) {
		a := &anyenc.Arena{}
		deep := a.NewArray()
		for cur, i := deep, 0; i < 600; i++ {
			next := a.NewArray()
			cur.SetArrayItem(0, next)
			cur = next
		}
		doc := a.NewObject()
		doc.Set("deep", deep)
		doc.Set("labels", a.NewObject())
		res, modified, err := MustParseModifier(`{"$set":{"labels.1500001":1}}`).Modify(a, doc)
		require.NoError(t, err)
		assert.True(t, modified)
		assert.Equal(t, 1, res.GetInt("labels", "1500001"))
		assert.Equal(t, anyenc.TypeArray, res.Get("deep").Type())
	})
	t.Run(`a key "\x1f"`, func(t *testing.T) {
		const src = `{"x":7,"\u001f":[]}`
		doc := anyenc.MustParseJson(src)
		require.NotNil(t, doc.Get("\x1f"))
		res, modified, err := MustParseModifier(`{"$rename":{"x":"\u001f.1500001"}}`).Modify(&anyenc.Arena{}, doc)
		require.ErrorContains(t, err, "can't backfill more than 1500000 elements")
		assert.Nil(t, res)
		assert.False(t, modified)
		assert.Equal(t, anyenc.MustParseJson(src).String(), doc.String())
		assert.Equal(t, 7, doc.GetInt("x"))
	})
}

// A parsed modifier can change its own operands: here the stored $set
// operand's "a" becomes an empty array once the modifier has run. A second
// application is then refused by the padding limit after its $set has
// already run, and the document it was given is unchanged.
func TestModifier_PaddingStagedRefusalAfterOperandChange(t *testing.T) {
	mod := MustParseModifier(`{"$set":{"o":{"a":{},"c":[]}},"$inc":{"o.a.1500001":1},"$rename":{"o.c":"o.a"}}`)
	// The first application meets "a" as an object; $rename then puts the
	// operand's empty "c" array in its place.
	res, _, err := mod.Modify(&anyenc.Arena{}, anyenc.MustParseJson(`{"x":1}`))
	require.NoError(t, err)
	require.NotNil(t, res.Get("o", "a"))
	assert.Equal(t, anyenc.TypeArray, res.Get("o", "a").Type())

	doc := anyenc.MustParseJson(`{"x":1}`)
	res, modified, err := mod.Modify(&anyenc.Arena{}, doc)
	require.ErrorContains(t, err, "can't backfill more than 1500000 elements")
	assert.Nil(t, res)
	assert.False(t, modified)
	assert.Equal(t, `{"x":1}`, doc.String())
}

// Only a modifier with a segment past the padding limit, or too large for an
// int, takes the staged path; every other modifier runs as the plain chain.
func TestParseModifier_StagesOnlyPathsPastThePaddingLimit(t *testing.T) {
	for mod, staged := range map[string]bool{
		`{"$set":{"a":1,"b.c":2}}`:                                false,
		`{"$set":{"arr.1500000":1}}`:                              false,
		`{"$set":{"arr.+5":1,"arr.-1":1,"arr.x":1}}`:              false,
		`{"$unset":{"arr.1500000.x":""}}`:                         false,
		`{"$set":{"arr.1500001":1}}`:                              true,
		`{"$inc":{"o.arr.2147483647.n":1}}`:                       true,
		`{"$rename":{"x":"arr.1500001"}}`:                         true,
		`{"$rename":{"arr.1500001":"x"}}`:                         true,
		`{"$set":{"a":1},"$push":{"arr.99999999999999999999":1}}`: true,
	} {
		_, isStaged := MustParseModifier(mod).(paddingStaged)
		assert.Equal(t, staged, isStaged, mod)
	}
}
