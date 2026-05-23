package anyenc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildDocLike mirrors the benchmark's buildDoc shape (id, scalars, string +
// int arrays) so the projected-parse correctness guarantee is exercised on the
// exact document layout the optimization targets (~920 B, 80-element nums).
func buildDocLike(a *Arena, id int) *Value {
	doc := a.NewObject()
	doc.Set("id", a.NewNumberInt(id))
	doc.Set("a", a.NewNumberInt(id%100))
	doc.Set("b", a.NewNumberInt((id/100)%50))
	doc.Set("c", a.NewNumberInt((id/5000)%10))
	doc.Set("val", a.NewNumberInt(id*7%1000))
	doc.Set("email", a.NewString(fmt.Sprintf("user%d@test.com", id)))
	doc.Set("score", a.NewNumberFloat64(float64(id%1000)/10.0))
	tags := a.NewArray()
	tags.SetArrayItem(0, a.NewString(fmt.Sprintf("tag-%d", id%20)))
	tags.SetArrayItem(1, a.NewString(fmt.Sprintf("cat-%d", id%10)))
	tags.SetArrayItem(2, a.NewString(fmt.Sprintf("grp-%d", id%5)))
	doc.Set("tags", tags)
	nums := a.NewArray()
	for k := 0; k < 80; k++ {
		nums.SetArrayItem(k, a.NewNumberInt((id+k*7)%100))
	}
	doc.Set("nums", nums)
	return doc
}

// buildNestedDoc has a nested object and an array-of-objects so projecting a
// ROOT key whose value is a nested structure can be verified against deep paths.
func buildNestedDoc(a *Arena, id int) *Value {
	doc := a.NewObject()
	doc.Set("id", a.NewNumberInt(id))
	meta := a.NewObject()
	meta.Set("labels", func() *Value {
		arr := a.NewArray()
		arr.SetArrayItem(0, a.NewString("x"))
		arr.SetArrayItem(1, a.NewString("y"))
		return arr
	}())
	meta.Set("nested", func() *Value {
		o := a.NewObject()
		o.Set("deep", a.NewNumberInt(id))
		o.Set("flag", a.NewBool(id%2 == 0))
		return o
	}())
	doc.Set("meta", meta)
	doc.Set("trailing", a.NewString("z"))
	return doc
}

// buildBigDocLike mirrors the benchmark's buildBigDoc (large payload string)
// so projected parse is verified for documents that overflow btree pages.
func buildBigDocLike(a *Arena, id int) *Value {
	doc := a.NewObject()
	doc.Set("id", a.NewNumberInt(id))
	doc.Set("a", a.NewNumberInt(id%100))
	doc.Set("b", a.NewNumberInt((id/100)%50))
	doc.Set("payload", a.NewString(strings.Repeat("X", 5000)))
	return doc
}

// allTopKeys returns every top-level key in the encoded object, in order.
func allTopKeys(t *testing.T, encoded []byte) []string {
	t.Helper()
	full, err := Parse(encoded)
	require.NoError(t, err)
	require.Equal(t, TypeObject, full.Type())
	obj, err := full.Object()
	require.NoError(t, err)
	var keys []string
	obj.Visit(func(k []byte, _ *Value) {
		keys = append(keys, string(k))
	})
	return keys
}

// assertProjectedMatchesOwned checks the central invariant: for the projected
// set `want`, every wanted top-level key marshals byte-identically to a full
// parse, and every non-wanted top-level key is absent (Get == nil).
func assertProjectedMatchesOwned(t *testing.T, encoded []byte, want []string) {
	t.Helper()

	var pFull, pProj Parser
	full, err := pFull.ParseOwned(encoded)
	require.NoError(t, err)
	proj, err := pProj.ParseProjected(encoded, want)
	require.NoError(t, err)
	require.Equal(t, TypeObject, proj.Type(), "projected value must be an object")

	for _, k := range allTopKeys(t, encoded) {
		fv := full.Get(k)
		pv := proj.Get(k)
		if wantKey(want, k) {
			require.NotNil(t, pv, "wanted key %q must be present", k)
			assert.Equal(t,
				fv.MarshalTo(nil), pv.MarshalTo(nil),
				"wanted key %q must marshal identically to full parse", k)
		} else {
			assert.Nil(t, pv, "non-wanted key %q must be skipped (Get==nil)", k)
		}
	}
}

func TestParseProjected_MatchesOwned_BuildDoc(t *testing.T) {
	a := &Arena{}
	// A spread of ids to exercise different value contents (string lengths,
	// number magnitudes, array contents).
	ids := []int{0, 1, 50, 99, 12345, 999999}
	wantSets := [][]string{
		{"a"},
		{"a", "b", "c"},
		{"val"},
		{"id", "a", "val", "email"},
		{"tags"},
		{"nums"},
		{"a", "nums", "tags"},
		{"id"},
		{"score"},
		{"email"},
		// Keys that don't exist in the doc — must yield nil, not error.
		{"a", "doesnotexist"},
		{"missing1", "missing2"},
		// Empty set — projects nothing.
		{},
		nil,
		// Every key — must be byte-identical to a full parse.
		{"id", "a", "b", "c", "val", "email", "score", "tags", "nums"},
	}
	for _, id := range ids {
		a.Reset()
		encoded := buildDocLike(a, id).MarshalTo(nil)
		for _, want := range wantSets {
			t.Run(fmt.Sprintf("id=%d/want=%v", id, want), func(t *testing.T) {
				assertProjectedMatchesOwned(t, encoded, want)
			})
		}
	}
}

func TestParseProjected_NestedPaths(t *testing.T) {
	a := &Arena{}
	encoded := buildNestedDoc(a, 42).MarshalTo(nil)

	var pFull, pProj Parser
	full, err := pFull.ParseOwned(encoded)
	require.NoError(t, err)
	// Project only the root "meta" — nested deep paths must still resolve
	// identically because the whole "meta" subtree is decoded.
	proj, err := pProj.ParseProjected(encoded, []string{"meta"})
	require.NoError(t, err)

	for _, path := range [][]string{
		{"meta"},
		{"meta", "labels"},
		{"meta", "labels", "0"},
		{"meta", "labels", "1"},
		{"meta", "nested"},
		{"meta", "nested", "deep"},
		{"meta", "nested", "flag"},
	} {
		fv := full.Get(path...)
		pv := proj.Get(path...)
		require.NotNil(t, pv, "nested path %v under projected root must resolve", path)
		assert.Equal(t, fv.MarshalTo(nil), pv.MarshalTo(nil),
			"nested path %v must marshal identically", path)
	}
	// A sibling root NOT projected must be absent.
	assert.Nil(t, proj.Get("trailing"))
	assert.Nil(t, proj.Get("id"))
}

func TestParseProjected_Overflow_BigDoc(t *testing.T) {
	a := &Arena{}
	encoded := buildBigDocLike(a, 7).MarshalTo(nil)
	for _, want := range [][]string{
		{"a"},
		{"id", "a", "b"},
		{"payload"},
		{"a", "payload"},
		{"id", "a", "b", "payload"},
	} {
		assertProjectedMatchesOwned(t, encoded, want)
	}
}

func TestParseProjected_CompressedObject(t *testing.T) {
	a := &Arena{}
	// Build a doc large enough to actually compress (> CompressMinSize).
	doc := buildDocLike(a, 314159)
	var scratch []byte
	compressed, _ := doc.MarshalCompressed(nil, scratch)
	// Sanity: it really is the compressed wrapper, not a plain object.
	require.Equal(t, TypeCompressedObjectS2, Type(compressed[0]),
		"doc must be large enough to compress for this test to be meaningful")

	for _, want := range [][]string{
		{"a"},
		{"val"},
		{"a", "nums"},
		{"id", "a", "b", "c", "val", "email", "score", "tags", "nums"},
	} {
		assertProjectedMatchesOwned(t, compressed, want)
	}
}

func TestParseProjected_EmptyObject(t *testing.T) {
	a := &Arena{}
	encoded := a.NewObject().MarshalTo(nil)
	var p Parser
	v, err := p.ParseProjected(encoded, []string{"a"})
	require.NoError(t, err)
	require.Equal(t, TypeObject, v.Type())
	obj, err := v.Object()
	require.NoError(t, err)
	assert.Equal(t, 0, obj.Len())
	assert.Nil(t, v.Get("a"))
}

func TestParseProjected_EmptyKey(t *testing.T) {
	// Object with the empty-string key must round-trip through projection
	// (the emptyKey sentinel path in parseObjectProjected).
	a := &Arena{}
	doc := a.NewObject()
	doc.Set("", a.NewNumberInt(7))
	doc.Set("a", a.NewNumberInt(9))
	encoded := doc.MarshalTo(nil)

	var pFull, pProj Parser
	full, err := pFull.ParseOwned(encoded)
	require.NoError(t, err)

	// Project the empty key.
	proj, err := pProj.ParseProjected(encoded, []string{""})
	require.NoError(t, err)
	assert.Equal(t, full.Get("").MarshalTo(nil), proj.Get("").MarshalTo(nil))
	assert.Nil(t, proj.Get("a"))

	// Project "a" only — empty key must be skipped without disturbing "a".
	proj2, err := pProj.ParseProjected(encoded, []string{"a"})
	require.NoError(t, err)
	assert.Equal(t, full.Get("a").MarshalTo(nil), proj2.Get("a").MarshalTo(nil))
	assert.Nil(t, proj2.Get(""))
}

func TestParseProjected_NonObjectFallback(t *testing.T) {
	// A non-object top-level value can't be projected; ParseProjected must
	// fall back to a full parse and return the value unchanged.
	a := &Arena{}
	for _, v := range []*Value{
		a.NewNumberInt(42),
		a.NewString("hello"),
		func() *Value {
			arr := a.NewArray()
			arr.SetArrayItem(0, a.NewNumberInt(1))
			return arr
		}(),
		a.NewBool(true),
		a.NewNull(),
	} {
		encoded := v.MarshalTo(nil)
		var pProj Parser
		proj, err := pProj.ParseProjected(encoded, []string{"a"})
		require.NoError(t, err)
		assert.Equal(t, encoded, proj.MarshalTo(nil),
			"non-object value must round-trip via full-parse fallback")
	}
}

func TestParseProjected_Errors(t *testing.T) {
	var p Parser
	// Empty input.
	_, err := p.ParseProjected(nil, []string{"a"})
	require.Error(t, err)
	// Unknown type byte.
	_, err = p.ParseProjected([]byte{0xff}, []string{"a"})
	require.Error(t, err)
	// Object with a malformed (truncated) skipped value must still error —
	// the structural skip validates the bytes it walks.
	a := &Arena{}
	doc := a.NewObject()
	doc.Set("a", a.NewNumberInt(1))
	doc.Set("b", a.NewNumberInt(2))
	encoded := doc.MarshalTo(nil)
	// Truncate within b's number payload (after a was fully encoded).
	truncated := encoded[:len(encoded)-3]
	_, err = p.ParseProjected(truncated, []string{"a"})
	require.Error(t, err, "truncated trailing value must be detected even when skipped")
}

// TestParseProjected_ReusableParser verifies the parser cache is correctly
// reset between projected parses (no stale Values bleed across calls), which
// matters because the hot path reuses one Parser per goroutine for every row.
func TestParseProjected_ReusableParser(t *testing.T) {
	a := &Arena{}
	enc1 := buildDocLike(a, 11).MarshalTo(nil)
	a.Reset()
	enc2 := buildDocLike(a, 22).MarshalTo(nil)

	var p Parser
	v1, err := p.ParseProjected(enc1, []string{"a", "val"})
	require.NoError(t, err)
	got1a := v1.Get("a").MarshalTo(nil)

	v2, err := p.ParseProjected(enc2, []string{"a", "val"})
	require.NoError(t, err)
	// v2 must reflect enc2, not stale enc1 content.
	var ref Parser
	full2, err := ref.ParseOwned(enc2)
	require.NoError(t, err)
	assert.Equal(t, full2.Get("a").MarshalTo(nil), v2.Get("a").MarshalTo(nil))
	assert.Equal(t, full2.Get("val").MarshalTo(nil), v2.Get("val").MarshalTo(nil))

	// Sanity: enc1 and enc2 differ on "a" (11%100 vs 22%100), so the reuse
	// check is meaningful.
	_ = got1a
	assert.NotEqual(t, 11%100, 22%100)
}
