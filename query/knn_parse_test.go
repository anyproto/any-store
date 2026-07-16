package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/syncpool"
)

// TestKnnParse pins the $knn grammar: exactly one accepted form, and a
// normative error string for every rejected shape. The error strings are part
// of the contract — programmatic consumers see the same rules re-checked at
// detection, and both layers must speak identically.
func TestKnnParse(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			json string
			want Knn
		}{
			{
				name: "plain array query",
				json: `{"v":{"$knn":{"$query":[0.5,1,2],"$k":10}}}`,
				want: Knn{Query: []float32{0.5, 1, 2}, K: 10},
			},
			{
				name: "packed $vector query",
				json: `{"v":{"$knn":{"$query":{"$vector":[0.5,1,2]},"$k":10}}}`,
				want: Knn{Query: []float32{0.5, 1, 2}, K: 10},
			},
			{
				name: "all options",
				json: `{"v":{"$knn":{"$query":[1],"$k":5,"$ef":400,"$index":"emb"}}}`,
				want: Knn{Query: []float32{1}, K: 5, Ef: 400, Index: "emb"},
			},
			{
				name: "ef == k",
				json: `{"v":{"$knn":{"$query":[1],"$k":7,"$ef":7}}}`,
				want: Knn{Query: []float32{1}, K: 7, Ef: 7},
			},
			{
				name: "dotted path",
				json: `{"a.b":{"$knn":{"$query":[1],"$k":1}}}`,
				want: Knn{Query: []float32{1}, K: 1},
			},
			{
				name: "under $and",
				json: `{"$and":[{"v":{"$knn":{"$query":[1],"$k":3}}},{"x":1}]}`,
				want: Knn{Query: []float32{1}, K: 3},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f, err := ParseCondition(tc.json)
				require.NoError(t, err)
				require.True(t, ContainsKnn(f), "parsed filter must contain the Knn")
				var got *Knn
				var walk func(Filter)
				walk = func(f Filter) {
					switch ft := f.(type) {
					case Knn:
						got = &ft
					case Key:
						walk(ft.Filter)
					case And:
						for _, s := range ft {
							walk(s)
						}
					case *And:
						for _, s := range *ft {
							walk(s)
						}
					}
				}
				walk(f)
				require.NotNil(t, got)
				assert.Equal(t, tc.want, *got)
			})
		}
	})

	t.Run("rejected", func(t *testing.T) {
		for _, tc := range []struct {
			name    string
			json    string
			wantErr string
		}{
			{
				name:    "not an object: bare array",
				json:    `{"v":{"$knn":[1,2,3]}}`,
				wantErr: `$knn must be an object, e.g. {"$knn":{"$query":[...],"$k":10}}`,
			},
			{
				name: "not an object: sole-key $vector (eaten by extjson into a vector VALUE)",
				json: `{"v":{"$knn":{"$vector":[1,2]}}}`,
				// anyenc decodes the single-key {"$vector":[…]} object into a
				// TypeVectorF32 value before the parser sees it, so this fails
				// the object check — not the unknown-field check.
				wantErr: `$knn must be an object`,
			},
			{
				name:    "missing $query",
				json:    `{"v":{"$knn":{"$k":10}}}`,
				wantErr: `$knn requires $query`,
			},
			{
				name:    "$query not numeric",
				json:    `{"v":{"$knn":{"$query":"abc","$k":10}}}`,
				wantErr: `$knn: $query must be an array of numbers or {"$vector":[...]}`,
			},
			{
				name:    "$query mixed array",
				json:    `{"v":{"$knn":{"$query":[1,"x"],"$k":10}}}`,
				wantErr: `$knn: $query must be an array of numbers or {"$vector":[...]}`,
			},
			{
				name:    "$query empty",
				json:    `{"v":{"$knn":{"$query":[],"$k":10}}}`,
				wantErr: `$knn: $query must be non-empty`,
			},
			{
				name:    "missing $k",
				json:    `{"v":{"$knn":{"$query":[1,2]}}}`,
				wantErr: `$knn requires $k (the number of neighbours to select)`,
			},
			{
				name:    "$k zero",
				json:    `{"v":{"$knn":{"$query":[1],"$k":0}}}`,
				wantErr: `$knn: $k must be an integer in [1, 10000]`,
			},
			{
				name:    "$k negative",
				json:    `{"v":{"$knn":{"$query":[1],"$k":-3}}}`,
				wantErr: `$knn: $k must be an integer in [1, 10000]`,
			},
			{
				name:    "$k too big",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10001}}}`,
				wantErr: `$knn: $k must be an integer in [1, 10000]`,
			},
			{
				name:    "$k fractional",
				json:    `{"v":{"$knn":{"$query":[1],"$k":2.5}}}`,
				wantErr: `$knn: $k must be an integer in [1, 10000]`,
			},
			{
				name:    "$k not a number",
				json:    `{"v":{"$knn":{"$query":[1],"$k":"ten"}}}`,
				wantErr: `$knn: $k must be an integer in [1, 10000]`,
			},
			{
				name:    "$ef below $k",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$ef":5}}}`,
				wantErr: `$knn: $ef must be an integer in [$k, 65536]`,
			},
			{
				name:    "$ef too big",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$ef":65537}}}`,
				wantErr: `$knn: $ef must be an integer in [$k, 65536]`,
			},
			{
				name:    "$ef fractional",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$ef":10.5}}}`,
				wantErr: `$knn: $ef must be an integer in [$k, 65536]`,
			},
			{
				name:    "$index not a string",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$index":7}}}`,
				wantErr: `$knn: $index must be a string`,
			},
			{
				name:    "unknown field",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$bogus":1}}}`,
				wantErr: `unknown $knn field: $bogus`,
			},
			{
				name:    "$vector as an option key (with other keys, so not eaten)",
				json:    `{"v":{"$knn":{"$vector":[1,2],"$k":10}}}`,
				wantErr: `unknown $knn field: $vector (did you mean "$query"?`,
			},
			{
				name:    "reserved $maxDistance",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$maxDistance":0.5}}}`,
				wantErr: `$knn: $maxDistance is reserved and not supported`,
			},
			{
				name:    "reserved $minScore",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$minScore":0.5}}}`,
				wantErr: `$knn: $minScore is reserved and not supported`,
			},
			{
				name:    "reserved $prefilter",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$prefilter":true}}}`,
				wantErr: `$knn: $prefilter is reserved and not supported`,
			},
			{
				name:    "reserved $nprobe",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10,"$nprobe":8}}}`,
				wantErr: `$knn: $nprobe is reserved and not supported`,
			},
			{
				name:    "mixed with another operator on the field",
				json:    `{"v":{"$knn":{"$query":[1],"$k":10},"$exists":true}}`,
				wantErr: `$knn must be the only operator on its field`,
			},
			{
				name:    "under $not",
				json:    `{"v":{"$not":{"$knn":{"$query":[1],"$k":10}}}}`,
				wantErr: `$knn is not allowed under $not`,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := ParseCondition(tc.json)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			})
		}
	})

	t.Run("NaN and Inf rejected", func(t *testing.T) {
		// JSON cannot spell NaN/Inf, so exercise the guard through the packed
		// $vector encoding, which carries raw float32 bits.
		a := &anyenc.Arena{}
		obj := a.NewObject()
		obj.Set("$query", a.NewVectorF32([]float32{1, float32(nan())}))
		obj.Set("$k", a.NewNumberInt(3))
		_, err := parseKnn(obj)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "$knn: $query must contain finite numbers")
	})
}

func nan() float64 {
	var zero float64
	return zero / zero
}

// TestKnnString_RoundTrip pins the lossless String() contract:
// MustParseCondition(f.String()) reproduces the filter exactly. Contrast
// Text.String(), which is lossy — do not repeat that here.
func TestKnnString_RoundTrip(t *testing.T) {
	for _, kn := range []Knn{
		{Query: []float32{0.1, 0.25, -3e-7}, K: 10},
		{Query: []float32{1}, K: 1, Ef: 64},
		{Query: []float32{1.5, 2.5}, K: 3, Ef: 4096, Index: "emb"},
		NewKnn([]float32{9.75}, 2, KnnEf(8), KnnIndex("x")),
	} {
		f := MustParseCondition(`{"v":` + kn.String() + `}`)
		key, ok := f.(Key)
		require.True(t, ok, "round-trip of %s", kn.String())
		require.Equal(t, kn, key.Filter, "String() must round-trip losslessly: %s", kn.String())
	}
}

// TestKnn_OkIsFalse pins fail-closed Ok — the opposite of Text.Ok's fail-open
// (which is a bug when reached: a fail-open source filter on Query.Delete
// costs the collection; a fail-closed one costs zero rows and is loud).
func TestKnn_OkIsFalse(t *testing.T) {
	kn := NewKnn([]float32{1, 2}, 3)
	buf := &syncpool.DocBuffer{}
	assert.False(t, kn.Ok(nil, buf), "nil probe")
	assert.False(t, kn.Ok(anyenc.MustParseJson(`5`), buf), "scalar probe")
	assert.False(t, kn.Ok(anyenc.MustParseJson(`[1,2]`), buf), "array probe")
	a := &anyenc.Arena{}
	assert.False(t, kn.Ok(a.NewVectorF32([]float32{1, 2}), buf), "vector probe — even the exact query vector")

	// IndexBounds returns bs VERBATIM: Or.IndexBounds detects a
	// non-contributing branch by comparing lengths, so a shorter return would
	// silently discard accumulated sibling bounds.
	bs := Bounds{{}}
	assert.Equal(t, bs, kn.IndexBounds("v", bs))
}

// TestGuaranteesPresence_SourceFilters pins the source-filter carve-out:
// GuaranteesPresence probes the inner filter's Ok directly, so fail-closed
// Knn.Ok would read as "guarantees presence" — the aggressive answer, feeding
// sparse-index selection. Both source filters must answer false.
func TestGuaranteesPresence_SourceFilters(t *testing.T) {
	assert.False(t, GuaranteesPresence(Key{Path: []string{"v"}, Filter: NewKnn([]float32{1}, 3)}, "v"))
	assert.False(t, GuaranteesPresence(Key{Path: []string{"v"}, Filter: Text{Search: "x"}}, "v"))
	// Sanity: a real predicate that rejects nil and null still answers true.
	assert.True(t, GuaranteesPresence(MustParseCondition(`{"v":{"$gt":1}}`), "v"))
}

// TestContainsKnn_WalksEveryNode pins the full-tree walk: a Knn under Not/Nor
// is exactly the match-all reflection the walk exists to catch.
func TestContainsKnn_WalksEveryNode(t *testing.T) {
	kn := Key{Path: []string{"v"}, Filter: NewKnn([]float32{1}, 3)}
	and := And{kn, MustParseCondition(`{"x":1}`)}
	assert.True(t, ContainsKnn(kn))
	assert.True(t, ContainsKnn(and))
	assert.True(t, ContainsKnn(&and))
	assert.True(t, ContainsKnn(Or{kn}))
	assert.True(t, ContainsKnn(Nor{kn}))
	assert.True(t, ContainsKnn(Not{Filter: kn}))
	assert.True(t, ContainsKnn(Key{Path: []string{"w"}, Filter: Not{Filter: NewKnn([]float32{1}, 1)}}))
	assert.True(t, ContainsKnn(NewKnn([]float32{1}, 1)), "a bare unwrapped Knn")
	assert.False(t, ContainsKnn(MustParseCondition(`{"x":{"$gt":1},"$text":{"$search":"q"}}`)))

	// Pointer-built composites satisfy Filter via value-receiver method sets
	// and MUST be walked too — a skipped &Not{Knn} is a match-all reflection.
	knnP := NewKnn([]float32{1}, 3)
	assert.True(t, ContainsKnn(&kn), "*Key")
	assert.True(t, ContainsKnn(&knnP), "*Knn")
	assert.True(t, ContainsKnn(Key{Path: []string{"v"}, Filter: &knnP}), "Key{*Knn}")
	notP := Not{Filter: kn}
	assert.True(t, ContainsKnn(&notP), "*Not")
	orP := Or{kn}
	assert.True(t, ContainsKnn(&orP), "*Or")
	norP := Nor{kn}
	assert.True(t, ContainsKnn(&norP), "*Nor")
	textP := Text{Search: "x"}
	assert.True(t, ContainsSourceFilter(&textP), "*Text")
	assert.False(t, GuaranteesPresence(Key{Path: []string{"v"}, Filter: &knnP}, "v"), "*Knn presence carve-out")

	assert.True(t, ContainsSourceFilter(and))
	assert.True(t, ContainsSourceFilter(MustParseCondition(`{"$text":{"$search":"q"},"x":1}`)))
	assert.False(t, ContainsSourceFilter(MustParseCondition(`{"x":{"$gt":1}}`)))
}
