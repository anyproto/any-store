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
