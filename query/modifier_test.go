package query

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
				`$pop: field 'arr' must be an array: value doesn't contain array; it contains string`,
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
			// A missing field becomes an array holding the value, as in
			// MongoDB, at any depth.
			{
				`{"$push":{"arr": -1}}`,
				`{}`,
				`{"arr":[-1]}`,
				true,
			},
			{
				`{"$push":{"a.b.arr": {"x":1}}}`,
				`{"a":{}}`,
				`{"a":{"b":{"arr":[{"x":1}]}}}`,
				true,
			},
			{
				`{"$push":{"arr": 1}}`,
				`{"arr": [1]}`,
				`{"arr":[1,1]}`,
				true,
			},
			{
				`{"$push":{"arr": [2]}}`,
				`{"arr": []}`,
				`{"arr":[[2]]}`,
				true,
			},
		}...)
	})
	t.Run("error", func(t *testing.T) {
		// A field that holds something other than an array is refused, as
		// MongoDB refuses it, null included.
		testModCasesErr(t, []modifierCaseError{
			{
				`{"$push":{"arr":1}}`,
				`{"arr":"2"}`,
				`$push: field 'arr' must be an array: value doesn't contain array; it contains string`,
			},
			{
				`{"$push":{"a.arr":1}}`,
				`{"a":{"arr":null}}`,
				`$push: field 'a.arr' must be an array: value doesn't contain array; it contains null`,
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
				`$pull: field 'arr' must be an array: value doesn't contain array; it contains string`,
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
			{
				`{"$pullAll":{"arr":[1]}}`,
				`{"arr":"2"}`,
				`$pullAll: field 'arr' must be an array: value doesn't contain array; it contains string`,
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
				`$addToSet: field 'arr' must be an array: value doesn't contain array; it contains string`,
			},
		}...)
	})
}

func BenchmarkModifier(b *testing.B) {
	bench := func(b *testing.B, query string) {
		modifier, err := ParseModifier(query)
		require.NoError(b, err)
		// Every iteration modifies a document of its own, parsed outside the
		// timer a batch at a time: a modifier that changes the document it is
		// given would otherwise give each later iteration a different one.
		const src = `{"a":2,"b":[3,2,1],"c":"test"}`
		docs := make([]*anyenc.Value, 1024)
		a := &anyenc.Arena{}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if i%len(docs) == 0 {
				b.StopTimer()
				for j := range docs {
					docs[j] = anyenc.MustParseJson(src)
				}
				b.StartTimer()
			}
			a.Reset()
			_, _, err := modifier.Modify(a, docs[i%len(docs)])
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

// A $set of the value the field holds, to the order of its objects' fields,
// keeps the document's own value and reports no change. A value that is
// equal but orders an object's fields otherwise is stored, so the field
// order a later write keeps is the operand's, and still reports no change.
func TestModifierSet_IdenticalKeepsTheValue(t *testing.T) {
	for _, tc := range []struct {
		mod, doc string
		kept     bool
	}{
		{`{"$set":{"a":[1,2,{"x":1,"y":[3]}]}}`, `{"a":[1,2,{"x":1,"y":[3]}]}`, true},
		{`{"$set":{"a":{"x":1,"y":"s"}}}`, `{"a":{"x":1,"y":"s"}}`, true},
		{`{"$set":{"a":5}}`, `{"a":5}`, true},
		{`{"$set":{"a":{"y":"s","x":1}}}`, `{"a":{"x":1,"y":"s"}}`, false},
		{`{"$set":{"a":[{"y":1,"x":1}]}}`, `{"a":[{"x":1,"y":1}]}`, false},
	} {
		mod := MustParseModifier(tc.mod)
		doc := anyenc.MustParseJson(tc.doc)
		before := doc.Get("a")
		res, modified, err := mod.Modify(&anyenc.Arena{}, doc)
		require.NoError(t, err)
		assert.False(t, modified, tc.mod)
		assert.Equal(t, tc.kept, res.Get("a") == before, tc.mod)
		assert.Equal(t, anyenc.MustParseJson(tc.mod).Get("$set", "a").String(), res.Get("a").String(), tc.mod)
	}
}

// A filtered $set that matches documents already holding its value: a
// 10,000-number array set to itself. The document is given its own array
// back before each run, so every run starts from the same document.
func BenchmarkModifier_SetIdenticalArray(b *testing.B) {
	nums := make([]string, 10_000)
	for i := range nums {
		nums[i] = strconv.Itoa(i)
	}
	arr := "[" + strings.Join(nums, ",") + "]"
	mod := MustParseModifier(`{"$set":{"arr":` + arr + `}}`)
	doc := anyenc.MustParseJson(`{"id":1,"arr":` + arr + `}`)
	orig := doc.Get("arr")
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		doc.Set("arr", orig)
		a.Reset()
		if _, modified, err := mod.Modify(a, doc); err != nil || modified {
			b.Fatal(modified, err)
		}
	}
}

// Parsing a $push and an $addToSet of a scalar, which refuseEachForm looks
// at for a '$' key only when it is an object.
func BenchmarkParseModifier_ScalarPush(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		if _, err := ParseModifier(`{"$push":{"tags":"a"},"$addToSet":{"seen":3}}`); err != nil {
			b.Fatal(err)
		}
	}
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
// document it was given: the copy is the result and the document is
// unchanged. A counting operation added to the staged chain is called once.
func TestModifier_PaddingStagedAppliesOnce(t *testing.T) {
	const src = `{"x":7,"arr":[1,2]}`
	doc := anyenc.MustParseJson(src)
	staged, ok := MustParseModifier(`{"$set":{"o":{"n":1},"arr.1500002":true},"$inc":{"o.n":1},"$rename":{"x":"y"}}`).(paddingStaged)
	require.True(t, ok)
	calls := 0
	staged.ModifierChain = append(slices.Clone(staged.ModifierChain), ModifyFunc(func(_ *anyenc.Arena, v *anyenc.Value) (*anyenc.Value, bool, error) {
		calls++
		return v, false, nil
	}))
	res, modified, err := staged.Modify(&anyenc.Arena{}, doc)
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "the chain is applied once")
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

// A staged modifier stores copies of its operands. Its own earlier result,
// passed back in, is left as it was when the modifier is refused: the first
// application below stores {"n":1} at "o", increments it there, and turns
// "arr" into an array; on the second, the copy's "o" is a new copy of the
// operand, so the increment does not reach the earlier result's "o" before
// the padding refusal. Applying the modifier does not change its operands,
// so every application to the same document gives the same result.
func TestModifier_PaddingStagedStoresCopiesOfItsOperands(t *testing.T) {
	const src = `{"arr":{},"next":[]}`
	mod := MustParseModifier(`{"$set":{"o":{"n":1}},"$inc":{"o.n":1,"arr.1500001":1},"$rename":{"next":"arr"}}`)
	first, _, err := mod.Modify(&anyenc.Arena{}, anyenc.MustParseJson(src))
	require.NoError(t, err)
	require.Equal(t, 2, first.GetInt("o", "n"))
	require.Equal(t, anyenc.TypeArray, first.Get("arr").Type())
	want := first.String()

	res, modified, err := mod.Modify(&anyenc.Arena{}, first)
	require.ErrorContains(t, err, "can't backfill more than 1500000 elements")
	assert.Nil(t, res)
	assert.False(t, modified)
	assert.Equal(t, want, first.String(), "the earlier result passed back in")

	again, _, err := mod.Modify(&anyenc.Arena{}, anyenc.MustParseJson(src))
	require.NoError(t, err)
	assert.Equal(t, want, again.String(), "a later application")

	// $push and $addToSet store copies too.
	mod = MustParseModifier(`{"$push":{"p":{"n":1}},"$addToSet":{"s":{"n":1}},"$inc":{"p.0.n":1,"s.0.n":1,"m.1500001":1}}`)
	for range 2 {
		res, _, err = mod.Modify(&anyenc.Arena{}, anyenc.MustParseJson(`{"p":[],"s":[]}`))
		require.NoError(t, err)
		assert.Equal(t, 2, res.GetInt("p", "0", "n"))
		assert.Equal(t, 2, res.GetInt("s", "0", "n"))
	}
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

// A modifier gives every document it is applied to a value of its own: $set,
// $push and $addToSet store a copy of an object or array operand, so a later
// operation of the same modifier that changes it in place changes that
// document's copy, not the operand, and the next document starts from the
// operand as parsed. MongoDB copies an operand into each document. None of
// these modifiers takes the staged path, and the first document has no array
// to pad.
func TestModifier_ObjectOperandsAreCopied(t *testing.T) {
	for _, tc := range []struct{ mod, doc, want string }{
		{`{"$push":{"p":{"n":1}},"$inc":{"p.0.n":1}}`, `{}`, `{"p":[{"n":2}]}`},
		{`{"$push":{"p":{"n":1}},"$inc":{"p.0.n":1}}`, `{"p":[]}`, `{"p":[{"n":2}]}`},
		{`{"$addToSet":{"s":{"n":1}},"$inc":{"s.0.n":1}}`, `{}`, `{"s":[{"n":2}]}`},
		{`{"$set":{"o":{"n":1}},"$inc":{"o.n":1}}`, `{}`, `{"o":{"n":2}}`},
		{`{"$set":{"arr":[1]},"$push":{"arr":2}}`, `{}`, `{"arr":[1,2]}`},
	} {
		mod := MustParseModifier(tc.mod)
		_, staged := mod.(paddingStaged)
		require.False(t, staged, tc.mod)
		for i := range 3 {
			res, modified, err := mod.Modify(&anyenc.Arena{}, anyenc.MustParseJson(tc.doc))
			require.NoError(t, err)
			assert.True(t, modified)
			assert.Equal(t, tc.want, res.String(), "%s, application %d", tc.mod, i+1)
		}
	}
}

// $pull, $pullAll, $pop and the source of $rename leave a document that does
// not have their path as it was, as MongoDB does: they create no missing
// parent and pad no array, which another operation of the same modifier
// would then have stored ({"meta":{}}).
func TestModifier_RemovalsCreateNoPath(t *testing.T) {
	testModCases(t, []modifierCase{
		{`{"$pull":{"meta.old":"x"}}`, `{"id":1}`, `{"id":1}`, false},
		{`{"$pull":{"meta.old":"x"},"$push":{"tags":"a"}}`, `{"id":1}`, `{"id":1,"tags":["a"]}`, true},
		{`{"$pullAll":{"meta.old":["x"]},"$set":{"seen":true}}`, `{"id":1}`, `{"id":1,"seen":true}`, true},
		{`{"$pop":{"meta.old":1},"$set":{"seen":true}}`, `{"id":1}`, `{"id":1,"seen":true}`, true},
		{`{"$rename":{"meta.old":"new"},"$set":{"seen":true}}`, `{"id":1}`, `{"id":1,"seen":true}`, true},
		{`{"$pull":{"a.b.c":1}}`, `{"a":{}}`, `{"a":{}}`, false},
		{`{"$pull":{"items.3.tags":"x"}}`, `{"items":[{}]}`, `{"items":[{}]}`, false},
		{`{"$pop":{"items.0.tags":-1}}`, `{"items":[{"tags":[1,2]}]}`, `{"items":[{"tags":[2]}]}`, true},
	}...)
}

// $push and $addToSet take one value. MongoDB's modifiers ($each, and for
// $push $position, $slice and $sort) are not implemented, so an operand
// object with a key that starts with '$' is refused instead of stored as it
// is; such a key deeper in the operand is a value like any other.
func TestParseModifier_PushModifiersRefused(t *testing.T) {
	testModCasesErr(t, []modifierCaseError{
		{
			mod: `{"$push":{"tags":{"$each":["a","b"]}}}`,
			err: `parse modifier: $push takes one value: its modifiers ($each, $position, $slice, $sort) are not supported, and an object operand cannot have a key that starts with '$', got $each (at $push.tags)`,
		},
		{
			mod: `{"$push":{"tags":{"n":1,"$slice":2}}}`,
			err: `parse modifier: $push takes one value: its modifiers ($each, $position, $slice, $sort) are not supported, and an object operand cannot have a key that starts with '$', got $slice (at $push.tags)`,
		},
		{
			mod: `{"$addToSet":{"tags":{"$each":["a","b"]}}}`,
			err: `parse modifier: $addToSet takes one value: its modifiers ($each) are not supported, and an object operand cannot have a key that starts with '$', got $each (at $addToSet.tags)`,
		},
	}...)
	testModCases(t, []modifierCase{
		{`{"$push":{"tags":{"n":{"$each":1}}}}`, `{}`, `{"tags":[{"n":{"$each":1}}]}`, true},
		{`{"$push":{"tags":["$each"]}}`, `{}`, `{"tags":[["$each"]]}`, true},
	}...)
}

// Where a removal's path stops matters as in MongoDB. A path the document
// could hold but does not (a missing field of an object, an index past an
// array's end) is a no-op for $unset, $pop, $pull, $pullAll and the source of
// $rename. A path it cannot hold — a field of a scalar, a name in an array —
// is a no-op for $unset (UnsetNode::allowNonViablePath) and an error for the
// others (PathNotViable), and the error fails the whole modifier, a sibling
// $set included.
func TestModifier_RemovalPathViability(t *testing.T) {
	testModCases(t, []modifierCase{
		{`{"$pull":{"items.3.x":1},"$set":{"seen":true}}`, `{"items":[]}`, `{"items":[],"seen":true}`, true},
		{`{"$pop":{"items.3.x":1},"$set":{"seen":true}}`, `{"items":[]}`, `{"items":[],"seen":true}`, true},
		{`{"$pullAll":{"items.3.x":[1]},"$set":{"seen":true}}`, `{"items":[]}`, `{"items":[],"seen":true}`, true},
		{`{"$rename":{"items.3.x":"y"},"$set":{"seen":true}}`, `{"items":[]}`, `{"items":[],"seen":true}`, true},
		{`{"$pull":{"items.3":1}}`, `{"items":[]}`, `{"items":[]}`, false},
		{`{"$unset":{"a.b":""}}`, `{"a":1}`, `{"a":1}`, false},
		{`{"$unset":{"a.b.c":""}}`, `{"a":{"b":null}}`, `{"a":{"b":null}}`, false},
		{`{"$unset":{"items.bad.x":""},"$set":{"seen":true}}`, `{"items":[]}`, `{"items":[],"seen":true}`, true},
		{`{"$unset":{"items.bad":""}}`, `{"items":[1]}`, `{"items":[1]}`, false},
	}...)
	testModCasesErr(t, []modifierCaseError{
		{`{"$pull":{"items.bad.x":1},"$set":{"seen":true}}`, `{"items":[]}`,
			`$pull: cannot use the part 'bad' of 'items.bad.x' to traverse the element []`},
		{`{"$pop":{"items.bad.x":1},"$set":{"seen":true}}`, `{"items":[]}`,
			`$pop: cannot use the part 'bad' of 'items.bad.x' to traverse the element []`},
		{`{"$pullAll":{"items.bad.x":[1]},"$set":{"seen":true}}`, `{"items":[]}`,
			`$pullAll: cannot use the part 'bad' of 'items.bad.x' to traverse the element []`},
		{`{"$rename":{"items.bad.x":"y"},"$set":{"seen":true}}`, `{"items":[]}`,
			`$rename: cannot use the part 'bad' of 'items.bad.x' to traverse the element []`},
		{`{"$pull":{"items.bad":1}}`, `{"items":[1]}`,
			`$pull: cannot use the part 'bad' of 'items.bad' to traverse the element [1]`},
		{`{"$pull":{"a.b":1}}`, `{"a":1}`,
			`$pull: cannot use the part 'b' of 'a.b' to traverse the element 1`},
		{`{"$pop":{"a.b.c":1}}`, `{"a":{"b":null}}`,
			`$pop: cannot use the part 'c' of 'a.b.c' to traverse the element null`},
		{`{"$rename":{"a.b":"c"}}`, `{"a":"s"}`,
			`$rename: cannot use the part 'b' of 'a.b' to traverse the element "s"`},
	}...)
}

// Strings, binary values and packed vectors hand out their bytes
// (GetStringBytes, GetBytes, GetVectorF32), so $set, $push and $addToSet
// store a copy of them too: a change made through one document's value
// reaches neither the modifier nor the next document.
func TestModifier_ByteOperandsAreCopied(t *testing.T) {
	a := &anyenc.Arena{}
	set := a.NewObject()
	set.Set("s", a.NewString("abc"))
	set.Set("b", a.NewBinary([]byte{1, 2, 3}))
	set.Set("v", a.NewVectorF32([]float32{1, 2}))
	push := a.NewObject()
	push.Set("ps", a.NewString("abc"))
	add := a.NewObject()
	add.Set("as", a.NewBinary([]byte{1, 2, 3}))
	src := a.NewObject()
	src.Set("$set", set)
	src.Set("$push", push)
	src.Set("$addToSet", add)
	mod := MustParseModifier(src)
	_, staged := mod.(paddingStaged)
	require.False(t, staged)

	apply := func() *anyenc.Value {
		res, modified, err := mod.Modify(&anyenc.Arena{}, anyenc.MustParseJson(`{}`))
		require.NoError(t, err)
		require.True(t, modified)
		return res
	}
	first := apply()
	want := first.String()
	first.GetStringBytes("s")[0] = 'x'
	first.GetBytes("b")[0] = 0xff
	first.GetVectorF32("v")[0] = 42
	first.GetStringBytes("ps", "0")[0] = 'x'
	first.GetBytes("as", "0")[0] = 0xff
	require.NotEqual(t, want, first.String())
	assert.Equal(t, want, apply().String(), "the next document")
}

// A modifier given as an *anyenc.Value is copied, so changing the caller's
// value afterwards changes nothing the modifier writes. Built in Go, the
// value has no parser bound: one nested past 512 levels, or one that
// contains itself, is refused when it is parsed, before anything copies it.
func TestParseModifier_GoValueBoundAndCopied(t *testing.T) {
	a := &anyenc.Arena{}
	operand := a.NewObject()
	operand.Set("n", a.NewNumberInt(1))
	push := a.NewObject()
	push.Set("p", operand)
	src := a.NewObject()
	src.Set("$push", push)
	mod, err := ParseModifier(src)
	require.NoError(t, err)
	operand.Set("n", a.NewNumberInt(2))
	res, _, err := mod.Modify(&anyenc.Arena{}, anyenc.MustParseJson(`{}`))
	require.NoError(t, err)
	assert.Equal(t, `{"p":[{"n":1}]}`, res.String())

	// nested is a modifier {"$push":{"p":[[...]]}} of the given depth, the
	// modifier object being the first level.
	nested := func(depth int) *anyenc.Value {
		inner := a.NewArray()
		for cur, i := inner, 3; i < depth; i++ {
			next := a.NewArray()
			cur.SetArrayItem(0, next)
			cur = next
		}
		push := a.NewObject()
		push.Set("p", inner)
		mod := a.NewObject()
		mod.Set("$push", push)
		return mod
	}
	_, err = ParseModifier(nested(512))
	require.NoError(t, err)
	_, err = ParseModifier(nested(513))
	var pe *ParseError
	require.ErrorAs(t, err, &pe)
	require.EqualError(t, err, "parse modifier: modifier is nested deeper than 512 levels")

	self := a.NewArray()
	self.SetArrayItem(0, self)
	push = a.NewObject()
	push.Set("p", self)
	src = a.NewObject()
	src.Set("$push", push)
	_, err = ParseModifier(src)
	require.ErrorAs(t, err, &pe)
}
