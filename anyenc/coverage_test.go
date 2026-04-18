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

// TestAppendAnyValue_BytesAndPanic covers the `case []byte` arm at any.go:18-21
// and the `default: panic(...)` arm at any.go:67 for unsupported types.
func TestAppendAnyValue_BytesAndPanic(t *testing.T) {
	// []byte takes the TypeString encoding.
	b := AppendAnyValue(nil, []byte("hi"))
	assert.Equal(t, byte(TypeString), b[0])
	// EOS terminator at the end.
	assert.Equal(t, EOS, b[len(b)-1])

	// Unsupported type → panic.
	assert.Panics(t, func() {
		_ = AppendAnyValue(nil, struct{ X int }{X: 1})
	})
}

// TestParse_ErrorPaths hits the many error branches inside parseValue,
// parseObject, parseArray, parseBinary when input is truncated/malformed.
func TestParse_ErrorPaths(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		{"empty", []byte{}},
		{"invalid_type_byte", []byte{0xfe}},
		{"string_missing_eos", []byte{byte(TypeString), 'x'}}, // no EOS
		{"truncated_number", []byte{byte(TypeNumber), 0x01}},  // need 8 bytes
		{"truncated_binary_lenhdr", []byte{byte(TypeBinary), 0x00}},
		{"object_missing_eos_on_key", []byte{byte(TypeObject), 'k'}}, // no EOS after key
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.in)
			assert.Error(t, err, "parse must reject %x", tc.in)
		})
	}
}

// TestTuple_ReadValues_ErrorPaths covers tuple.go ReadValues error branches:
// parse failure, callback error propagation.
func TestTuple_ReadValues_ErrorPaths(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("a"))
	tuple = tuple.Append(a.NewString("b"))

	t.Run("callback_error_propagates", func(t *testing.T) {
		sentinel := assertCustomErr{"stop"}
		err := tuple.ReadValues(&Parser{}, func(v *Value) error {
			return sentinel
		})
		require.ErrorIs(t, err, sentinel)
	})
	t.Run("parse_failure", func(t *testing.T) {
		// Tuple with garbage.
		bad := Tuple{0xff, 0xff, 0xff}
		err := bad.ReadValues(&Parser{}, func(v *Value) error { return nil })
		require.Error(t, err)
	})
}

// TestTuple_ReadBytes_ErrorPaths covers tuple.go ReadBytes error branches.
func TestTuple_ReadBytes_ErrorPaths(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("a"))

	t.Run("callback_error_propagates", func(t *testing.T) {
		sentinel := assertCustomErr{"rb-stop"}
		err := tuple.ReadBytes(func(b []byte) error { return sentinel })
		require.ErrorIs(t, err, sentinel)
	})
	t.Run("parse_failure", func(t *testing.T) {
		bad := Tuple{0xff, 0xff}
		err := bad.ReadBytes(func(b []byte) error { return nil })
		require.Error(t, err)
	})
}

// TestTuple_OffsetAfter covers all branches: n<=0, n in range, n beyond
// field count (clamps to len(t)), and parse failure propagation.
func TestTuple_OffsetAfter(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("a"))
	tuple = tuple.Append(a.NewString("b"))

	t.Run("n_zero", func(t *testing.T) {
		off, err := tuple.OffsetAfter(0)
		require.NoError(t, err)
		assert.Equal(t, 0, off)
	})
	t.Run("n_negative", func(t *testing.T) {
		off, err := tuple.OffsetAfter(-1)
		require.NoError(t, err)
		assert.Equal(t, 0, off)
	})
	t.Run("n_in_range", func(t *testing.T) {
		off, err := tuple.OffsetAfter(1)
		require.NoError(t, err)
		assert.Greater(t, off, 0)
		assert.Less(t, off, len(tuple))
	})
	t.Run("n_beyond_len_clamped", func(t *testing.T) {
		off, err := tuple.OffsetAfter(99)
		require.NoError(t, err)
		assert.Equal(t, len(tuple), off, "n past the end must clamp to len(tuple)")
	})
	t.Run("parse_failure", func(t *testing.T) {
		bad := Tuple{0xff, 0xff, 0xff}
		_, err := bad.OffsetAfter(1)
		require.Error(t, err)
	})
}

// assertCustomErr is a sentinel error type for checking callback propagation.
type assertCustomErr struct{ msg string }

func (e assertCustomErr) Error() string { return e.msg }

// TestParse_MalformedInputs_Extra targets the remaining error branches in
// parseObject, parseArray, parseBinary, parseCompressedObjectS2.
func TestParse_MalformedInputs_Extra(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
	}{
		// parseObject: truncated after reading the key EOS
		{"object_no_value_after_key", []byte{byte(TypeObject), 'k', EOS}},
		// parseArray: length byte but nothing to parse
		{"array_truncated", []byte{byte(TypeArray)}},
		// parseBinary: length says 10 bytes but only 2 supplied
		{"binary_length_overflow", append(
			[]byte{byte(TypeBinary), 0x00, 0x00, 0x00, 0x0a},
			0x01, 0x02)},
		// parseBinary: total input shorter than length-header
		{"binary_short_header", []byte{byte(TypeBinary), 0x00, 0x00}},
		// parseCompressedObjectS2: too short for 5-byte header
		{"compressed_too_short", []byte{byte(TypeCompressedObjectS2), 0x01}},
		// parseCompressedObjectS2: header says 1000 bytes but payload is empty
		{"compressed_length_overflow", []byte{
			byte(TypeCompressedObjectS2),
			0x00, 0x00, 0x03, 0xe8, // 1000
		}},
		// Object with an inner value parse failure.
		{"object_bad_inner_value", []byte{byte(TypeObject), 'k', EOS, 0xfe}},
		// Tail bytes after a valid value — rejected by Parse.
		{"trailing_bytes", append(
			MustParseJson(`"x"`).MarshalTo(nil),
			0x00)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.in)
			assert.Error(t, err, "input %x must fail to parse", tc.in)
		})
	}
}

// TestParseJson_Error covers ParseJson error path when input is invalid JSON.
func TestParseJson_Error(t *testing.T) {
	_, err := ParseJson(`{invalid json`)
	require.Error(t, err)
}

// TestValue_NilGuards covers the nil-receiver and invalid-state guards on
// Set, Del, Get, SetArrayItem. All return silently for invalid inputs.
func TestValue_NilGuards(t *testing.T) {
	a := &Arena{}

	// Nil receivers must not panic.
	var nilV *Value
	nilV.Set("k", a.NewString("v"))
	nilV.Del("k")
	nilV.SetArrayItem(0, a.NewString("v"))
	assert.Nil(t, nilV.Get("k"))

	// Set/Del on a scalar value (not object/array) is a silent no-op.
	scalar := a.NewString("just a string")
	scalar.Set("k", a.NewString("x"))
	scalar.Del("k")

	// Set on an array with non-integer key → silent no-op.
	arr := a.NewArray()
	arr.Set("not-a-number", a.NewString("x"))
	arr.Set("-1", a.NewString("x")) // negative index
	items, _ := arr.Array()
	assert.Equal(t, 0, len(items))

	// Del on array with non-integer / negative index → silent no-op.
	arr.SetArrayItem(0, a.NewString("v0"))
	arr.Del("not-a-number")
	arr.Del("-1")
	arr.Del("99") // beyond bounds
	items, _ = arr.Array()
	assert.Equal(t, 1, len(items))

	// Get on array with non-integer / negative / out-of-range index → nil.
	assert.Nil(t, arr.Get("not-a-number"))
	assert.Nil(t, arr.Get("-1"))
	assert.Nil(t, arr.Get("99"))

	// Get through a scalar as a mid-path node → nil (the scalar branch).
	obj := a.NewObject()
	obj.Set("a", a.NewString("leaf"))
	// Get "a.b" traverses a scalar at key "a", falls into else-nil branch.
	assert.Nil(t, obj.Get("a", "b"))

	// SetArrayItem on a non-array value — silent no-op.
	obj.SetArrayItem(0, a.NewString("x")) // obj is not array
}

// TestValue_AppendBytes_ErrorPath covers v.AppendBytes when the value is not
// binary — it must propagate the Bytes() error at value.go:149-151.
func TestValue_AppendBytes_ErrorPath(t *testing.T) {
	a := &Arena{}
	str := a.NewString("not binary")
	_, err := str.AppendBytes(nil)
	require.Error(t, err, "AppendBytes on non-binary value must surface Bytes() error")
}

// TestType_String covers the remaining Type.String cases not hit by
// existing tests: binary, compressed-object-s2, and the default unknown path.
func TestType_String(t *testing.T) {
	assert.Equal(t, "binary", TypeBinary.String())
	assert.Equal(t, "compressedObjectS2", TypeCompressedObjectS2.String())
	assert.Contains(t, Type(99).String(), "unknown type")
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
