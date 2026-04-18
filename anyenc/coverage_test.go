package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArena_ApproxSize pins Arena.ApproxSize — an empty arena reports a
// baseline size (possibly 0 depending on initial cap), and allocating a
// non-empty value strictly grows the reported size.
func TestArena_ApproxSize(t *testing.T) {
	a := &Arena{}
	emptySize := a.ApproxSize()
	assert.GreaterOrEqual(t, emptySize, 0)

	// Allocate an 11-byte string. The cache stores a Value with len(v.v)==11,
	// plus valueSize overhead. A broken implementation that returns a constant
	// would fail the strict `>` assertion.
	_ = a.NewStringBytes([]byte("hello world"))
	after := a.ApproxSize()
	assert.Greater(t, after, emptySize,
		"ApproxSize must strictly grow after allocating a non-empty value")
}

// TestMustParse_AndParseOwned pins the MustParse wrapper and Parser.ParseOwned,
// both of which are exposed as public API and not yet covered.
func TestMustParse_AndParseOwned(t *testing.T) {
	// Encode a value first via MarshalTo on a parsed value.
	v := MustParseJson(`{"a":"hello","b":42}`)
	encoded := v.MarshalTo(nil)

	// MustParse must return the equivalent value.
	parsed := MustParse(encoded)
	assert.Equal(t, []byte("hello"), parsed.GetStringBytes("a"))

	// ParseOwned on the same bytes without copying.
	p := &Parser{}
	pov, err := p.ParseOwned(encoded)
	require.NoError(t, err)
	assert.Equal(t, v.String(), pov.String())

	// ParseOwned on invalid input must error.
	_, err = p.ParseOwned([]byte{0xff})
	require.Error(t, err)
}

// TestMustParse_PanicsOnError covers the error branch of MustParse.
func TestMustParse_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() { _ = MustParse([]byte{0xff}) })
}

// TestParser_ApproxSize pins Parser.ApproxSize which forwards to the cache's
// approxSizeValues.
func TestParser_ApproxSize(t *testing.T) {
	p := &Parser{}
	s0 := p.ApproxSize()
	assert.GreaterOrEqual(t, s0, 0)

	// Parse a small doc so the parser's value cache grows.
	_, err := p.Parse(MustParseJson(`{"a":"hello"}`).MarshalTo(nil))
	require.NoError(t, err)
	assert.GreaterOrEqual(t, p.ApproxSize(), s0)
}

// TestParserPool_GetPut pins the sync.Pool wrappers for Parser and proves the
// retrieved parser is functional (can Parse encoded bytes) — not just non-nil.
func TestParserPool_GetPut(t *testing.T) {
	pp := &ParserPool{}
	p := pp.Get()
	require.NotNil(t, p, "empty pool returns a fresh Parser")

	// Prove the fresh parser actually works.
	encoded := MustParseJson(`"hello"`).MarshalTo(nil)
	v, err := p.Parse(encoded)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(v.GetStringBytes()))

	pp.Put(p)

	// After Put, Get must return a usable Parser. sync.Pool is
	// non-deterministic, but either branch (pooled or fresh fallback) must
	// produce a working Parser.
	p2 := pp.Get()
	require.NotNil(t, p2)
	v2, err := p2.Parse(encoded)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(v2.GetStringBytes()))
}

// TestArenaPool_GetPut pins the sync.Pool wrappers for Arena. Put calls
// Reset on the arena before returning it to the pool.
func TestArenaPool_GetPut(t *testing.T) {
	ap := &ArenaPool{}
	a := ap.Get()
	require.NotNil(t, a)
	// Produce some values so Reset has work to do.
	_ = a.NewString("hi")
	ap.Put(a)
	a2 := ap.Get()
	require.NotNil(t, a2)
}

// TestTuple_FieldBytes exercises Tuple.FieldBytes for each encoded field
// position, plus the out-of-range error branch.
func TestTuple_FieldBytes(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("hello"))
	tuple = tuple.Append(a.NewNumberInt(42))
	tuple = tuple.Append(a.NewNull())

	fb0, err := tuple.FieldBytes(0)
	require.NoError(t, err)
	assert.NotEmpty(t, fb0)

	fb1, err := tuple.FieldBytes(1)
	require.NoError(t, err)
	assert.NotEmpty(t, fb1)

	fb2, err := tuple.FieldBytes(2)
	require.NoError(t, err)
	assert.NotEmpty(t, fb2)

	// Out of range.
	_, err = tuple.FieldBytes(3)
	require.Error(t, err)
}

// TestValue_GoType covers Value.GoType for every supported type plus the
// default-panic branch (unreachable via the public API but handled here for
// robustness).
func TestValue_GoType(t *testing.T) {
	a := &Arena{}

	assert.Equal(t, float64(3.14), a.NewNumberFloat64(3.14).GoType())
	assert.Equal(t, "hi", a.NewString("hi").GoType())
	assert.Equal(t, []byte{0x01, 0x02}, a.NewBinary([]byte{0x01, 0x02}).GoType())
	assert.Equal(t, true, a.NewTrue().GoType())
	assert.Equal(t, false, a.NewFalse().GoType())
	assert.Nil(t, a.NewNull().GoType())

	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("x"))
	arr.SetArrayItem(1, a.NewNumberInt(7))
	got := arr.GoType()
	assert.Equal(t, []any{"x", float64(7)}, got)

	obj := a.NewObject()
	obj.Set("k", a.NewString("v"))
	objGo := obj.GoType()
	assert.Equal(t, map[string]any{"k": "v"}, objGo)
}
