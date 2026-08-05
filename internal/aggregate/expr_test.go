package aggregate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func evalExprOn(t *testing.T, exprJson string, doc *anyenc.Value) *anyenc.Value {
	t.Helper()
	e, err := ParseExpr(anyenc.MustParseJson(exprJson))
	require.NoError(t, err)
	v, err := e.Eval(&anyenc.Arena{}, doc)
	require.NoError(t, err)
	return v
}

func TestArithExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 10, "b": 3, "s": "str", "z": 0, "nul": null}`)
	eval := func(t *testing.T, exprJson string) *anyenc.Value {
		return evalExprOn(t, exprJson, doc)
	}
	num := func(t *testing.T, exprJson string) float64 {
		v := eval(t, exprJson)
		require.Equal(t, anyenc.TypeNumber, v.Type())
		return v.GetFloat64()
	}
	null := func(t *testing.T, exprJson string) {
		v := eval(t, exprJson)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	}

	t.Run("nested composition", func(t *testing.T) {
		assert.Equal(t, float64(17), num(t, `{"$add": ["$a", {"$multiply": ["$b", 2]}, 1]}`))
	})
	t.Run("variadic and shorthand", func(t *testing.T) {
		assert.Equal(t, float64(24), num(t, `{"$multiply": [2, 3, 4]}`))
		assert.Equal(t, float64(10), num(t, `{"$add": "$a"}`))
		assert.Equal(t, float64(5), num(t, `{"$multiply": [5]}`))
	})
	t.Run("empty operand list yields the identity", func(t *testing.T) {
		assert.Equal(t, float64(0), num(t, `{"$add": []}`))
		assert.Equal(t, float64(1), num(t, `{"$multiply": []}`))
	})
	t.Run("subtract and divide", func(t *testing.T) {
		assert.Equal(t, float64(7), num(t, `{"$subtract": ["$a", "$b"]}`))
		assert.Equal(t, float64(2.5), num(t, `{"$divide": ["$a", 4]}`))
	})
	t.Run("divide by zero", func(t *testing.T) {
		null(t, `{"$divide": ["$a", 0]}`)
		null(t, `{"$divide": ["$a", "$z"]}`)
	})
	t.Run("null and missing propagate", func(t *testing.T) {
		null(t, `{"$add": ["$nope", 1]}`)
		null(t, `{"$add": ["$nul", 1]}`)
		null(t, `{"$subtract": [{"$literal": null}, 1]}`)
	})
	t.Run("non-finite result", func(t *testing.T) {
		null(t, `{"$multiply": [1e308, 10]}`)
		null(t, `{"$divide": [1e308, 1e-308]}`)
		// The overflowing nested expr is already null; it propagates.
		null(t, `{"$subtract": [{"$multiply": [1e308, 10]}, 1]}`)
	})
	t.Run("non-numeric operand", func(t *testing.T) {
		null(t, `{"$add": ["$s", 1]}`)
		null(t, `{"$multiply": [true, 2]}`)
	})
	t.Run("datetime operand is non-numeric for now", func(t *testing.T) {
		a := &anyenc.Arena{}
		d := a.NewObject()
		d.Set("d", a.NewDateTimeMillis(1000))
		v := evalExprOn(t, `{"$add": ["$d", 1]}`, d)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	})
}

func TestRoundExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"x": 3.7}`)
	num := func(t *testing.T, exprJson string) float64 {
		v := evalExprOn(t, exprJson, doc)
		require.Equal(t, anyenc.TypeNumber, v.Type())
		return v.GetFloat64()
	}
	null := func(t *testing.T, exprJson string) {
		v := evalExprOn(t, exprJson, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	}

	t.Run("half to even", func(t *testing.T) {
		assert.Equal(t, float64(2), num(t, `{"$round": [1.5]}`))
		assert.Equal(t, float64(2), num(t, `{"$round": [2.5]}`))
		assert.Equal(t, float64(-2), num(t, `{"$round": [-1.5]}`))
		assert.Equal(t, float64(0), num(t, `{"$round": [0.5]}`))
	})
	t.Run("positive place", func(t *testing.T) {
		// 2.345's double (2.3450000000000002) sits above the midpoint: rounds up.
		assert.Equal(t, 2.35, num(t, `{"$round": [2.345, 2]}`))
		// 1.25 scales to exactly 12.5: a true tie, half to even.
		assert.Equal(t, 1.2, num(t, `{"$round": [1.25, 1]}`))
	})
	t.Run("negative place", func(t *testing.T) {
		assert.Equal(t, float64(1230), num(t, `{"$round": [1234.5, -1]}`))
		assert.Equal(t, float64(1200), num(t, `{"$round": [1234.5, -2]}`))
	})
	t.Run("place beyond float64 resolution is identity", func(t *testing.T) {
		assert.Equal(t, 1.5, num(t, `{"$round": [1.5, 100]}`))
	})
	t.Run("single non-array operand", func(t *testing.T) {
		assert.Equal(t, float64(4), num(t, `{"$round": "$x"}`))
	})
	t.Run("invalid place", func(t *testing.T) {
		null(t, `{"$round": [1.5, 1.5]}`)
		null(t, `{"$round": [1.5, 101]}`)
		null(t, `{"$round": [1.5, -21]}`)
		null(t, `{"$round": [1.5, "two"]}`)
	})
	t.Run("null and missing propagate", func(t *testing.T) {
		null(t, `{"$round": ["$nope"]}`)
		null(t, `{"$round": [{"$literal": null}, 2]}`)
	})
}

func TestAbsExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"n": -5, "s": "str"}`)
	t.Run("numeric", func(t *testing.T) {
		assert.Equal(t, float64(5), evalExprOn(t, `{"$abs": "$n"}`, doc).GetFloat64())
		assert.Equal(t, 1.5, evalExprOn(t, `{"$abs": [1.5]}`, doc).GetFloat64())
	})
	t.Run("null, missing, non-numeric", func(t *testing.T) {
		for _, j := range []string{`{"$abs": "$nope"}`, `{"$abs": [{"$literal": null}]}`, `{"$abs": "$s"}`} {
			v := evalExprOn(t, j, doc)
			require.NotNil(t, v, j)
			assert.Equal(t, anyenc.TypeNull, v.Type(), j)
		}
	})
}

func TestConcatExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"s1": "héllo", "s2": "wörld", "n": 5, "nul": null}`)
	str := func(t *testing.T, exprJson string) string {
		v := evalExprOn(t, exprJson, doc)
		require.Equal(t, anyenc.TypeString, v.Type())
		return string(v.GetStringBytes())
	}
	null := func(t *testing.T, exprJson string) {
		v := evalExprOn(t, exprJson, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	}

	t.Run("multibyte and empty strings", func(t *testing.T) {
		assert.Equal(t, "héllo → wörld", str(t, `{"$concat": ["$s1", " → ", "$s2"]}`))
		assert.Equal(t, "ab", str(t, `{"$concat": ["a", "", "b"]}`))
		assert.Equal(t, "", str(t, `{"$concat": [""]}`))
		assert.Equal(t, "", str(t, `{"$concat": []}`))
	})
	t.Run("single non-array operand", func(t *testing.T) {
		assert.Equal(t, "héllo", str(t, `{"$concat": "$s1"}`))
	})
	t.Run("null, missing, non-string", func(t *testing.T) {
		null(t, `{"$concat": ["$s1", "$nul"]}`)
		null(t, `{"$concat": ["$s1", "$nope"]}`)
		null(t, `{"$concat": ["$s1", "$n"]}`)
	})
}

// countingExpr wraps an Expr and counts Eval calls — the structural proof of
// branch laziness for $cond/$switch/$ifNull.
type countingExpr struct {
	inner Expr
	n     int
}

func (c *countingExpr) Eval(a *anyenc.Arena, doc *anyenc.Value) (*anyenc.Value, error) {
	c.n++
	return c.inner.Eval(a, doc)
}

func (c *countingExpr) String() string { return c.inner.String() }

func mustExpr(t *testing.T, exprJson string) Expr {
	t.Helper()
	e, err := ParseExpr(anyenc.MustParseJson(exprJson))
	require.NoError(t, err)
	return e
}

func TestCondExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"z": 0, "f": false, "nul": null, "es": "", "s": "x", "ea": [], "eo": {}, "n": 0.0}`)
	branch := func(t *testing.T, condExpr string) string {
		v := evalExprOn(t, `{"$cond": [`+condExpr+`, "then", "else"]}`, doc)
		require.Equal(t, anyenc.TypeString, v.Type())
		return string(v.GetStringBytes())
	}

	t.Run("truthiness", func(t *testing.T) {
		// Mongo coercion: false, 0, null, missing → false; everything else
		// (including "", [], {}) → true.
		for cond, want := range map[string]string{
			`0`:       "else",
			`"$z"`:    "else",
			`"$n"`:    "else", // 0.0
			`false`:   "else",
			`"$f"`:    "else",
			`"$nul"`:  "else",
			`"$nope"`: "else", // missing
			`""`:      "then",
			`"$es"`:   "then",
			`"x"`:     "then",
			`"$s"`:    "then",
			`"$ea"`:   "then", // []
			`"$eo"`:   "then", // {}
			`[]`:      "then",
			`{}`:      "then",
			`-1`:      "then",
			`true`:    "then",
		} {
			assert.Equal(t, want, branch(t, cond), "cond=%s", cond)
		}
	})
	t.Run("object form", func(t *testing.T) {
		v := evalExprOn(t, `{"$cond": {"if": "$s", "then": 1, "else": 2}}`, doc)
		assert.Equal(t, float64(1), v.GetFloat64())
	})
	t.Run("untaken branch does not affect the result", func(t *testing.T) {
		v := evalExprOn(t, `{"$cond": [true, "ok", {"$divide": [1, 0]}]}`, doc)
		assert.Equal(t, "ok", string(v.GetStringBytes()))
	})
	t.Run("laziness is structural", func(t *testing.T) {
		then := &countingExpr{inner: mustExpr(t, `"t"`)}
		els := &countingExpr{inner: mustExpr(t, `"e"`)}
		e := &CondExpr{If: mustExpr(t, `"$s"`), Then: then, Else: els}
		v, err := e.Eval(&anyenc.Arena{}, doc)
		require.NoError(t, err)
		assert.Equal(t, "t", string(v.GetStringBytes()))
		assert.Equal(t, 1, then.n)
		assert.Zero(t, els.n, "untaken branch must not be evaluated")
	})
	t.Run("nests with other operators", func(t *testing.T) {
		v := evalExprOn(t, `{"$add": [10, {"$cond": [{"$eq": ["$z", 0]}, 1, 2]}]}`, doc)
		assert.Equal(t, float64(11), v.GetFloat64())
	})
}

func TestSwitchExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 5}`)
	t.Run("first truthy case wins", func(t *testing.T) {
		v := evalExprOn(t, `{"$switch": {"branches": [
			{"case": {"$lt": ["$a", 3]}, "then": "low"},
			{"case": {"$lt": ["$a", 10]}, "then": "mid"},
			{"case": true, "then": "any"}
		]}}`, doc)
		assert.Equal(t, "mid", string(v.GetStringBytes()))
	})
	t.Run("default", func(t *testing.T) {
		v := evalExprOn(t, `{"$switch": {"branches": [{"case": false, "then": 1}], "default": "$a"}}`, doc)
		assert.Equal(t, float64(5), v.GetFloat64())
	})
	t.Run("no match and no default yields null", func(t *testing.T) {
		// Mongo raises here; streaming eval has no per-document error channel.
		v := evalExprOn(t, `{"$switch": {"branches": [{"case": false, "then": 1}]}}`, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	})
	t.Run("laziness is structural", func(t *testing.T) {
		case2 := &countingExpr{inner: mustExpr(t, `true`)}
		then2 := &countingExpr{inner: mustExpr(t, `2`)}
		def := &countingExpr{inner: mustExpr(t, `0`)}
		e := &SwitchExpr{
			Cases:   []Expr{mustExpr(t, `true`), case2},
			Thens:   []Expr{mustExpr(t, `1`), then2},
			Default: def,
		}
		v, err := e.Eval(&anyenc.Arena{}, doc)
		require.NoError(t, err)
		assert.Equal(t, float64(1), v.GetFloat64())
		assert.Zero(t, case2.n, "later cases must not be evaluated")
		assert.Zero(t, then2.n)
		assert.Zero(t, def.n)
	})
}

func TestIfNullExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 1, "nul": null, "z": 0, "f": false}`)
	t.Run("two operands", func(t *testing.T) {
		assert.Equal(t, float64(1), evalExprOn(t, `{"$ifNull": ["$a", 9]}`, doc).GetFloat64())
		assert.Equal(t, float64(9), evalExprOn(t, `{"$ifNull": ["$nul", 9]}`, doc).GetFloat64())
		assert.Equal(t, float64(9), evalExprOn(t, `{"$ifNull": ["$nope", 9]}`, doc).GetFloat64())
	})
	t.Run("falsy but present values pass through", func(t *testing.T) {
		assert.Equal(t, float64(0), evalExprOn(t, `{"$ifNull": ["$z", 9]}`, doc).GetFloat64())
		assert.Equal(t, anyenc.TypeFalse, evalExprOn(t, `{"$ifNull": ["$f", 9]}`, doc).Type())
	})
	t.Run("four operands take the first non-null", func(t *testing.T) {
		v := evalExprOn(t, `{"$ifNull": ["$nope", "$nul", "$a", 9]}`, doc)
		assert.Equal(t, float64(1), v.GetFloat64())
	})
	t.Run("all null yields the last operand", func(t *testing.T) {
		v := evalExprOn(t, `{"$ifNull": ["$nul", "$nope", {"$literal": null}]}`, doc)
		require.NotNil(t, v)
		assert.Equal(t, anyenc.TypeNull, v.Type())
	})
	t.Run("laziness is structural", func(t *testing.T) {
		rest := &countingExpr{inner: mustExpr(t, `9`)}
		e := &IfNullExpr{Args: []Expr{mustExpr(t, `"$a"`), rest}}
		v, err := e.Eval(&anyenc.Arena{}, doc)
		require.NoError(t, err)
		assert.Equal(t, float64(1), v.GetFloat64())
		assert.Zero(t, rest.n, "replacement must not be evaluated when unused")
	})
}

func TestCompareExprEval(t *testing.T) {
	doc := anyenc.MustParseJson(`{"a": 1, "b": 2, "nul": null}`)
	boolOf := func(t *testing.T, exprJson string, d *anyenc.Value) bool {
		v := evalExprOn(t, exprJson, d)
		require.NotNil(t, v)
		switch v.Type() {
		case anyenc.TypeTrue:
			return true
		case anyenc.TypeFalse:
			return false
		}
		t.Fatalf("not a bool: %s -> %s", exprJson, v.Type())
		return false
	}
	cmpOf := func(t *testing.T, exprJson string, d *anyenc.Value) int {
		v := evalExprOn(t, exprJson, d)
		require.Equal(t, anyenc.TypeNumber, v.Type())
		return int(v.GetFloat64())
	}

	t.Run("same-type", func(t *testing.T) {
		for _, tc := range []struct {
			lo, hi string
		}{
			{`1`, `2`},
			{`-2`, `-1`},
			{`"a"`, `"b"`},
			{`"a"`, `"ab"`}, // prefix orders first
			{`false`, `true`},
			{`[1]`, `[1,2]`}, // elementwise: prefix orders first
			{`[1,2]`, `[1,3]`},
			{`{"x":1}`, `{"x":2}`},
		} {
			pair := tc.lo + "," + tc.hi
			assert.True(t, boolOf(t, `{"$lt": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$lte": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$gt": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$gte": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$eq": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$ne": [`+pair+`]}`, doc), pair)
			assert.Equal(t, -1, cmpOf(t, `{"$cmp": [`+pair+`]}`, doc), pair)
			assert.Equal(t, 1, cmpOf(t, `{"$cmp": [`+tc.hi+`,`+tc.lo+`]}`, doc), pair)
		}
		for _, v := range []string{`1`, `"a"`, `true`, `false`, `null`, `[1,2]`, `{"x":1}`} {
			pair := v + "," + v
			assert.True(t, boolOf(t, `{"$eq": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$lte": [`+pair+`]}`, doc), pair)
			assert.True(t, boolOf(t, `{"$gte": [`+pair+`]}`, doc), pair)
			assert.False(t, boolOf(t, `{"$ne": [`+pair+`]}`, doc), pair)
			assert.Zero(t, cmpOf(t, `{"$cmp": [`+pair+`]}`, doc), pair)
		}
	})
	t.Run("negative zero equals zero", func(t *testing.T) {
		assert.True(t, boolOf(t, `{"$eq": [-0.0, 0]}`, doc))
		assert.Zero(t, cmpOf(t, `{"$cmp": [-0.0, 0]}`, doc))
		assert.False(t, boolOf(t, `{"$lt": [-0.0, 0]}`, doc))
	})
	t.Run("missing equals null", func(t *testing.T) {
		assert.True(t, boolOf(t, `{"$eq": ["$nope", null]}`, doc))
		assert.True(t, boolOf(t, `{"$eq": ["$nope", "$nul"]}`, doc))
		assert.True(t, boolOf(t, `{"$eq": ["$nope", "$also.missing"]}`, doc))
		assert.Equal(t, -1, cmpOf(t, `{"$cmp": ["$nope", 0]}`, doc), "null sorts before numbers")
	})
	t.Run("cross-type order is anyenc tag order and $lt agrees with $cmp", func(t *testing.T) {
		// null < number < string < false < true < array < object.
		ordered := []string{`null`, `-1`, `0`, `1.5`, `"a"`, `"b"`, `false`, `true`, `[1]`, `[2]`, `{"x":1}`}
		for i, lo := range ordered {
			for _, hi := range ordered[i+1:] {
				pair := lo + "," + hi
				assert.True(t, boolOf(t, `{"$lt": [`+pair+`]}`, doc), pair)
				assert.Equal(t, -1, cmpOf(t, `{"$cmp": [`+pair+`]}`, doc), pair)
				assert.Equal(t, 1, cmpOf(t, `{"$cmp": [`+hi+`,`+lo+`]}`, doc), pair)
				assert.False(t, boolOf(t, `{"$eq": [`+pair+`]}`, doc), pair)
			}
		}
	})
	t.Run("object equality is field-order-sensitive", func(t *testing.T) {
		// Marshaled-bytes order, consistent with $group key equality
		// (divergence from Mongo's order-insensitive document comparison).
		assert.False(t, boolOf(t, `{"$eq": [{"a": 1, "b": 2}, {"b": 2, "a": 1}]}`, doc))
	})
	t.Run("dateTime values", func(t *testing.T) {
		a := &anyenc.Arena{}
		d := a.NewObject()
		d.Set("d1", a.NewDateTimeMillis(1000))
		d.Set("d2", a.NewDateTimeMillis(2000))
		d.Set("d1b", a.NewDateTimeMillis(1000))
		assert.True(t, boolOf(t, `{"$lt": ["$d1", "$d2"]}`, d))
		assert.True(t, boolOf(t, `{"$eq": ["$d1", "$d1b"]}`, d))
		assert.Equal(t, -1, cmpOf(t, `{"$cmp": ["$d1", "$d2"]}`, d))
		// dateTime tag sorts after every JSON-expressible type.
		assert.True(t, boolOf(t, `{"$lt": [{"x": 1}, "$d1"]}`, d))
	})
	t.Run("comparison feeds $cond", func(t *testing.T) {
		v := evalExprOn(t, `{"$cond": [{"$gte": ["$b", "$a"]}, "yes", "no"]}`, doc)
		assert.Equal(t, "yes", string(v.GetStringBytes()))
	})
}

// TestExprEvalAllocFree pins the hot-path contract: operator evaluation
// allocates nothing per document once the arena cache is warm.
func TestExprEvalAllocFree(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark-backed test")
	}
	doc := anyenc.MustParseJson(`{"a": 3, "b": 4, "s1": "héllo → w", "s2": "0123456789"}`)
	for _, tc := range []struct{ name, json string }{
		{"arith", `{"$add": ["$a", {"$multiply": ["$b", 2]}, 1]}`},
		{"concat", `{"$concat": ["$s1", "$s2"]}`},
		{"cond over comparison", `{"$cond": [{"$lt": ["$a", "$b"]}, {"$add": ["$a", 1]}, "$b"]}`},
		{"switch", `{"$switch": {"branches": [
			{"case": {"$eq": ["$s1", "$s2"]}, "then": 1},
			{"case": {"$gt": ["$a", "$b"]}, "then": 2},
			{"case": true, "then": "$a"}
		], "default": 0}}`},
		{"compare containers", `{"$cmp": [["$s1", "$a"], ["$s2", "$b"]]}`},
		{"ifNull", `{"$ifNull": ["$nope", "$a"]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := ParseExpr(anyenc.MustParseJson(tc.json))
			require.NoError(t, err)
			a := &anyenc.Arena{}
			// Warm up the arena value cache.
			for i := 0; i < 1000; i++ {
				a.Reset()
				if _, err := e.Eval(a, doc); err != nil {
					t.Fatal(err)
				}
			}
			res := testing.Benchmark(func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					a.Reset()
					v, err := e.Eval(a, doc)
					if err != nil || v == nil {
						b.Fatal(v, err)
					}
				}
			})
			assert.Zero(t, res.AllocsPerOp(), "expression eval must be alloc-free in steady state")
		})
	}
}

func BenchmarkArithExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$add": ["$a", {"$multiply": ["$b", 2]}, 1]}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"a": 3, "b": 4}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCondExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$cond": [{"$lt": ["$a", "$b"]}, {"$add": ["$a", 1]}, "$b"]}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"a": 3, "b": 4}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSwitchExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$switch": {"branches": [
		{"case": {"$lt": ["$a", 3]}, "then": "low"},
		{"case": {"$lt": ["$a", 10]}, "then": "mid"},
		{"case": true, "then": "high"}
	], "default": "none"}}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"a": 5}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcatExprEval(b *testing.B) {
	e, err := ParseExpr(anyenc.MustParseJson(`{"$concat": ["$s1", "$s2"]}`))
	if err != nil {
		b.Fatal(err)
	}
	doc := anyenc.MustParseJson(`{"s1": "héllo → w", "s2": "0123456789"}`)
	a := &anyenc.Arena{}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Reset()
		if _, err := e.Eval(a, doc); err != nil {
			b.Fatal(err)
		}
	}
}
