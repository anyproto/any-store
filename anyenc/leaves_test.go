package anyenc

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// leavesString renders leaves as " | "-joined JSON, "missing" for nil, with
// a "@" suffix on positional leaves.
func leavesString(leaves []Leaf) string {
	parts := make([]string, len(leaves))
	for i, l := range leaves {
		if l.Value == nil {
			parts[i] = "missing"
		} else {
			parts[i] = l.Value.String()
		}
		if l.Positional {
			parts[i] += "@"
		}
	}
	return strings.Join(parts, " | ")
}

func TestAppendLeaves(t *testing.T) {
	cases := []struct {
		doc, path, want string
	}{
		// no array on the path: exactly Get's answer
		{`{"a":{"b":1}}`, "a.b", `1`},
		{`{"a":{"b":[1,2]}}`, "a.b", `[1,2]`},
		{`{"a":5}`, "a.b", `missing`},
		{`{}`, "a.b", `missing`},
		{`{"a":null}`, "a.b", `missing`},
		{`{"a":{"c":1}}`, "a.b", `missing`},
		{`{"a":[1,2]}`, "a", `[1,2]`},
		// array of objects: one leaf per element, in order
		{`{"a":[{"b":1},{"b":2}]}`, "a.b", `1 | 2`},
		{`{"a":[{"b":[1,2]}]}`, "a.b", `[1,2]`},
		{`{"a":[{"b":1},{"c":2}]}`, "a.b", `1 | missing`},
		{`{"a":[{"b":1},5,"s",null,[{"b":1}]]}`, "a.b", `1 | missing | missing | missing | missing`},
		{`{"a":[]}`, "a.b", `missing`},
		{`{"a":[1,2]}`, "a.b", `missing | missing`},
		// traversal repeats at every array level
		{`{"a":[{"b":[{"c":1},{"c":2}]}]}`, "a.b.c", `1 | 2`},
		{`{"a":[{"b":[{"c":[1,2]}]}]}`, "a.b.c", `[1,2]`},
		{`{"a":[{"b":{"c":[1,2]}},{"b":{"c":3}}]}`, "a.b.c", `[1,2] | 3`},
		{`{"a":[{"b":1},{"b":2}]}`, "a.b.c", `missing | missing`},
		{`{"a":[{"b":[1,2]}]}`, "a.b.c", `missing | missing`},
		{`{"a":[{"b":[]}]}`, "a.b.c", `missing`},
		// numeric segments index arrays and are keys on objects
		{`{"a":[{"b":1},{"b":2}]}`, "a.0.b", `1`},
		{`{"a":[{"b":1},{"b":2}]}`, "a.1.b", `2`},
		{`{"a":[{"b":1},{"b":2}]}`, "a.2.b", `missing`},
		{`{"a":[{"b":1},{"b":2}]}`, "a.-1.b", `missing`},
		{`{"a":[{"b":1},{"b":2}]}`, "a.+1.b", `2`},
		{`{"a":[{"b":1},{"b":2}]}`, "a.01.b", `2`},
		{`{"a":{"0":{"b":7}}}`, "a.0.b", `7`},
		{`{"a":[{"0":{"b":7}}]}`, "a.0.b", `missing`},
		{`{"a":[[1,2],[3]]}`, "a.0", `[1,2]@`},
		{`{"a":[1,2]}`, "a.1", `2@`},
		{`{"a":{"0":[1,2]}}`, "a.0", `[1,2]`},
		{`{"a":[{"b":[[1,2]]},{"b":{"0":[3]}}]}`, "a.b.0", `[1,2]@ | [3]`},
		{`{"a":[[{"b":1}]]}`, "a.0.b", `1`},
		{`{"a":[]}`, "a.0", `missing`},
		{`{"a":[[{"b":1}]]}`, "a.b", `missing`},
	}
	var buf []Leaf
	for _, c := range cases {
		v := MustParseJson(c.doc)
		buf = v.AppendLeaves(buf[:0], strings.Split(c.path, ".")...)
		assert.Equal(t, c.want, leavesString(buf), "%s %s", c.doc, c.path)
	}
}

// With no array crossed by a non-numeric segment the single leaf is Get's answer.
func TestAppendLeaves_GetParity(t *testing.T) {
	for _, c := range []struct{ doc, path string }{
		{`{"a":{"b":1}}`, "a.b"}, {`{"a":{"b":[1,2]}}`, "a.b"}, {`{"a":5}`, "a.b"},
		{`{}`, "a.b"}, {`{"a":null}`, "a.b"}, {`{"a":[1,2]}`, "a"},
		{`{"a":[{"b":1},{"b":2}]}`, "a.1.b"}, {`{"a":[{"b":1}]}`, "a.2.b"},
		{`{"a":[[1,2],[3]]}`, "a.0"}, {`{"a":[]}`, "a.0"}, {`{"a":{"0":{"b":7}}}`, "a.0.b"},
	} {
		v := MustParseJson(c.doc)
		path := strings.Split(c.path, ".")
		leaves := v.AppendLeaves(nil, path...)
		require.Len(t, leaves, 1, "%s %s", c.doc, c.path)
		got := v.Get(path...)
		if got == nil {
			assert.Nil(t, leaves[0].Value, "%s %s", c.doc, c.path)
		} else {
			require.NotNil(t, leaves[0].Value, "%s %s", c.doc, c.path)
			assert.Equal(t, got.String(), leaves[0].Value.String(), "%s %s", c.doc, c.path)
		}
	}
}

// GetLeaf answers exactly when AppendLeaves yields one non-positional leaf,
// and then agrees with it.
func TestGetLeaf_AgreesWithAppendLeaves(t *testing.T) {
	docs := []string{
		`{"a":{"b":1}}`, `{"a":5}`, `{}`, `{"a":[]}`, `{"a":[1,2]}`, `{"a":[[1,2]]}`,
		`{"a":[{"b":1},{"b":2}]}`, `{"a":[{"b":[1]}]}`, `{"a":{"0":{"b":7}}}`, `{"a":{"b":[{"c":1}]}}`,
	}
	paths := [][]string{{"a"}, {"a", "b"}, {"a", "0"}, {"a", "0", "b"}, {"a", "1", "b"}, {"a", "b", "c"}, {"a", "b", "0"}}
	for _, d := range docs {
		v := MustParseJson(d)
		for _, p := range paths {
			leaves := v.AppendLeaves(nil, p...)
			got, single := v.GetLeaf(p...)
			if single {
				require.Len(t, leaves, 1, "%s %v", d, p)
				assert.False(t, leaves[0].Positional, "%s %v", d, p)
				assert.Equal(t, leavesString(leaves), leavesString([]Leaf{{Value: got}}), "%s %v", d, p)
			} else {
				assert.True(t, len(leaves) > 1 || leaves[0].Positional || strings.Contains(d, "["), "%s %v", d, p)
			}
		}
	}
}

func TestAppendLeaves_NilAndEmptyPath(t *testing.T) {
	var nilV *Value
	require.Equal(t, "missing", leavesString(nilV.AppendLeaves(nil, "a")))
	v := MustParseJson(`{"a":1}`)
	require.Equal(t, `{"a":1}`, leavesString(v.AppendLeaves(nil)))
}

func TestAppendLeaves_AllocFree(t *testing.T) {
	v := MustParseJson(`{"a":[{"b":[{"c":1},{"c":2}]},{"b":[{"c":3}]},{"x":1},5]}`)
	path := []string{"a", "b", "c"}
	buf := v.AppendLeaves(nil, path...)
	allocs := testing.AllocsPerRun(100, func() {
		buf = v.AppendLeaves(buf[:0], path...)
	})
	assert.Zero(t, allocs)
	assert.Equal(t, "1 | 2 | 3 | missing | missing", leavesString(buf))
}

func valuesString(vals []*Value) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		if v == nil {
			parts[i] = "null"
		} else {
			parts[i] = v.String()
		}
	}
	return strings.Join(parts, " | ")
}

func TestAppendIndexAndElementValues(t *testing.T) {
	cases := []struct{ doc, path, index, elements string }{
		{`{"a":1}`, "a", `1`, `1`},
		{`{}`, "a", `null`, `null`},
		{`{"a":[]}`, "a", `[]`, `[]`},
		{`{"a":[1,2]}`, "a", `1 | 2 | [1,2]`, `1 | 2`},
		{`{"a":[[1],2]}`, "a", `[1] | 2 | [[1],2]`, `[1] | 2`},
		{`{"a":[[1,2],[3]]}`, "a.0", `[1,2]`, `[1,2]`}, // positional: whole only
		{`{"a":[{"b":1},{"b":[2,3]},{"c":1},{"b":[]}]}`, "a.b", `1 | 2 | 3 | [2,3] | null | []`, `1 | 2 | 3 | null | []`},
	}
	var leaves []Leaf
	var vals []*Value
	for _, c := range cases {
		v := MustParseJson(c.doc)
		leaves = v.AppendLeaves(leaves[:0], strings.Split(c.path, ".")...)
		vals = AppendIndexValues(vals[:0], leaves)
		assert.Equal(t, c.index, valuesString(vals), "index %s %s", c.doc, c.path)
		vals = AppendElementValues(vals[:0], leaves)
		assert.Equal(t, c.elements, valuesString(vals), "elements %s %s", c.doc, c.path)
	}
}

func TestWalkLeaves_MatchesAppendLeaves_AllocFree(t *testing.T) {
	v := MustParseJson(`{"a":[{"b":[{"c":1},{"c":2}]},{"b":[{"c":3}]},{"x":1},5,{"b":{"c":[4]}}]}`)
	path := []string{"a", "b", "c"}
	want := v.AppendLeaves(nil, path...)
	var got []Leaf
	v.WalkLeaves(path, func(l Leaf) { got = append(got, l) })
	assert.Equal(t, leavesString(want), leavesString(got))
	n := 0
	allocs := testing.AllocsPerRun(100, func() {
		v.WalkLeaves(path, func(l Leaf) { n++ })
	})
	assert.Zero(t, allocs)
}

func TestParseIndexSegment(t *testing.T) {
	cases := []struct {
		seg string
		n   int
		ok  bool
	}{
		{"0", 0, true}, {"7", 7, true}, {"+3", 3, true}, {"01", 1, true},
		{"-1", -1, true}, {"-0", 0, true}, {"99999999999999999999", -1, true},
		{"", 0, false}, {"+", 0, false}, {"-", 0, false}, {"a", 0, false},
		{"1a", 0, false}, {" 1", 0, false}, {"1 ", 0, false}, {"1.0", 0, false},
	}
	for _, c := range cases {
		n, ok := ParseIndexSegment(c.seg)
		assert.Equal(t, c.ok, ok, c.seg)
		if ok {
			assert.Equal(t, c.n, n, c.seg)
		}
	}
}
