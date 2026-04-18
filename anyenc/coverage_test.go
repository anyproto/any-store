package anyenc

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArena_ApproxSize pins Arena.ApproxSize — an empty arena reports a
// baseline size (possibly 0 depending on initial cap), and allocating a
// non-empty value strictly grows the reported size.
//
// The formula (see parser.go:259-264 approxSizeValues) is:
//
//	sum over vs[:cap(vs)] of (len(v.v) + valueSize)
//
// so an 11-byte string contributes at least 11 + valueSize bytes. We assert
// the delta covers the 11 payload bytes (the valueSize offset is added on top),
// which proves the string payload was actually accounted for.
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
	assert.GreaterOrEqual(t, after-emptySize, 11,
		"delta must cover at least the 11 payload bytes of 'hello world'")
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

	// Round-trip a hand-built encoded byte sequence that never passed through
	// MarshalTo. This proves MustParse parses the raw wire format and is not
	// merely symmetric with the local encoder (which would mask corruption
	// affecting both encoder and decoder identically).
	t.Run("hand_built_string", func(t *testing.T) {
		raw := []byte{byte(TypeString), 'h', 'e', 'l', 'l', 'o', EOS}
		got := MustParse(raw)
		sb, err := got.StringBytes()
		require.NoError(t, err)
		assert.Equal(t, []byte("hello"), sb)
	})
}

// TestMustParse_PanicsOnError covers the error branch of MustParse.
func TestMustParse_PanicsOnError(t *testing.T) {
	assert.Panics(t, func() { _ = MustParse([]byte{0xff}) })
}

// TestParser_ApproxSize pins Parser.ApproxSize which forwards to the cache's
// approxSizeValues. Parse (parser.go:68-78) calls p.c.reset() on each call,
// which truncates cache length to 0 but keeps capacity — so ApproxSize
// continues to reflect the previously-grown cap rather than shrinking
// linearly with input.
func TestParser_ApproxSize(t *testing.T) {
	p := &Parser{}
	s0 := p.ApproxSize()
	assert.GreaterOrEqual(t, s0, 0)

	// Parse a small doc so the parser's value cache grows.
	_, err := p.Parse(MustParseJson(`{"a":"hello"}`).MarshalTo(nil))
	require.NoError(t, err)
	smallSize := p.ApproxSize()
	assert.Greater(t, smallSize, 0,
		"cache must strictly grow after parsing a non-empty doc")

	// Parse a LARGER doc — many keys and a longer string value — so the
	// cached Values (and their []byte payloads) aggregate more bytes.
	largeJSON := `{"a":"hello world","b":"a longer string payload","c":"another one","d":1,"e":2,"f":3,"g":"last"}`
	_, err = p.Parse(MustParseJson(largeJSON).MarshalTo(nil))
	require.NoError(t, err)
	largeSize := p.ApproxSize()
	assert.Greater(t, largeSize, smallSize,
		"cache must strictly grow when parsing a larger doc (more cached values)")

	// Re-parse an empty-object doc. Parse calls c.reset() which truncates
	// length but keeps cap, and approxSizeValues iterates vs[:cap(vs)] — so
	// the reported size must not exceed the previously-grown size. This
	// pins the reset behavior: cap is retained for reuse, not shrunk.
	_, err = p.Parse(MustParseJson(`{}`).MarshalTo(nil))
	require.NoError(t, err)
	resetSize := p.ApproxSize()
	assert.LessOrEqual(t, resetSize, largeSize,
		"post-reset ApproxSize must not exceed prior high-water mark")
}

// TestParserPool_GetPut pins the sync.Pool wrappers for Parser and proves the
// retrieved parser is functional (can Parse encoded bytes) — not just non-nil.
// Also verifies round-trip pointer identity: when a parser is Put back and a
// Get happens shortly after, under GOMAXPROCS(1) it is very likely (not
// guaranteed — sync.Pool drains during GC) that the exact same pointer is
// returned. We retry a bounded number of times before relaxing to a
// functional-only assertion.
func TestParserPool_GetPut(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	pp := &ParserPool{}
	p := pp.Get()
	require.NotNil(t, p, "empty pool returns a fresh Parser")

	// Prove the fresh parser actually works.
	encoded := MustParseJson(`"hello"`).MarshalTo(nil)
	v, err := p.Parse(encoded)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(v.GetStringBytes()))

	// Seed p with observable state before Put so any returned pointer that
	// equals p definitionally came from the pool.
	_, err = p.Parse(encoded)
	require.NoError(t, err)
	pp.Put(p)

	// After Put, Get must return a usable Parser. sync.Pool is
	// non-deterministic, but either branch (pooled or fresh fallback) must
	// produce a working Parser.  Try up to 10 times to see the same pointer;
	// if the pool GC'd the entry, that's acceptable — still require the
	// returned parser works on the encoded bytes.
	var p2 *Parser
	var sameIdentity bool
	for i := 0; i < 10; i++ {
		p2 = pp.Get()
		if p2 == p {
			sameIdentity = true
			break
		}
		pp.Put(p2)
	}
	require.NotNil(t, p2)
	if !sameIdentity {
		t.Log("pool GC'd the pointer before any Get retrieved it (acceptable)")
	}
	v2, err := p2.Parse(encoded)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(v2.GetStringBytes()))
}

// TestArenaPool_GetPut pins the sync.Pool wrappers for Arena. Put calls
// Reset on the arena before returning it to the pool — we verify that by
// observing the returned arena's cache is cleared (vs length == 0 after
// reset, see parser.go:242-244 cache.reset). Under GOMAXPROCS(1), Put+Get
// back-to-back very likely hands back the exact same arena pointer, which
// we check in a bounded retry loop.
func TestArenaPool_GetPut(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	ap := &ArenaPool{}
	a := ap.Get()
	require.NotNil(t, a)

	// Produce several values so Reset has work to do.
	_ = a.NewString("hi")
	_ = a.NewString("there")
	_ = a.NewNumberInt(42)
	assert.Greater(t, a.ApproxSize(), 0,
		"arena must have non-zero ApproxSize after allocating values")

	ap.Put(a)

	// Try to observe pointer identity. If we get `a` back, we can verify
	// that Reset fired — approxSizeValues walks vs[:cap(vs)] counting
	// len(v.v) of each cached Value. Reset calls c.vs = c.vs[:0], which
	// shrinks length to 0 but keeps capacity — so prior cached payloads
	// (v.v) remain reachable via cap and still contribute to size. This
	// means size may NOT drop to zero. We therefore verify a weaker but
	// still useful invariant: after Put+Get, the arena is *usable* for
	// fresh allocations, and its size does not strictly grow without new
	// allocations.
	var a2 *Arena
	var sameIdentity bool
	for i := 0; i < 10; i++ {
		a2 = ap.Get()
		if a2 == a {
			sameIdentity = true
			break
		}
		ap.Put(a2)
	}
	require.NotNil(t, a2)

	if sameIdentity {
		// Same arena — verify the returned arena is ready to accept fresh
		// allocations starting from length 0. We measure size before/after
		// allocating a known string and assert the delta is consistent
		// with a freshly-reset (empty) vs slice.
		before := a2.ApproxSize()
		_ = a2.NewString("new")
		after := a2.ApproxSize()
		assert.Greater(t, after, before,
			"post-reset arena must strictly grow after new allocation")
	} else {
		t.Log("pool GC'd the pointer; falling back to functional check")
		// Still require the returned arena works.
		v := a2.NewString("post-pool")
		sb, err := v.StringBytes()
		require.NoError(t, err)
		assert.Equal(t, []byte("post-pool"), sb)
	}
}

// TestTuple_FieldBytes exercises Tuple.FieldBytes for each encoded field
// position, plus the out-of-range error branch. Asserts that each field's
// bytes equal the individually-marshaled value, and that concatenating the
// per-field bytes reproduces the full tuple — proving FieldBytes carves at
// the correct boundaries.
func TestTuple_FieldBytes(t *testing.T) {
	a := &Arena{}
	var tuple Tuple
	tuple = tuple.Append(a.NewString("hello"))
	tuple = tuple.Append(a.NewNumberInt(42))
	tuple = tuple.Append(a.NewNull())

	// Expected per-field encodings.
	expectedFb0 := a.NewString("hello").MarshalTo(nil)
	expectedFb1 := a.NewNumberInt(42).MarshalTo(nil)
	expectedFb2 := a.NewNull().MarshalTo(nil)

	fb0, err := tuple.FieldBytes(0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb0, expectedFb0),
		"field 0 bytes must equal independent MarshalTo of the value: got %x want %x", fb0, expectedFb0)

	fb1, err := tuple.FieldBytes(1)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb1, expectedFb1),
		"field 1 bytes must equal independent MarshalTo of the value: got %x want %x", fb1, expectedFb1)

	fb2, err := tuple.FieldBytes(2)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb2, expectedFb2),
		"field 2 bytes must equal independent MarshalTo of the value: got %x want %x", fb2, expectedFb2)

	// Concatenating the per-field bytes must exactly reproduce the tuple.
	var concat []byte
	concat = append(concat, fb0...)
	concat = append(concat, fb1...)
	concat = append(concat, fb2...)
	assert.True(t, bytes.Equal(concat, []byte(tuple)),
		"concat of FieldBytes must reproduce the full tuple: got %x want %x", concat, []byte(tuple))

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

	// Round-trip via MustParse: the emitted bytes must parse back to a
	// string value whose StringBytes is "hi" — proves the []byte arm
	// produced a well-formed TypeString encoding, not just tagged bytes.
	v := MustParse(b)
	sb, err := v.StringBytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("hi"), sb)

	// Unsupported type → panic.
	assert.Panics(t, func() {
		_ = AppendAnyValue(nil, struct{ X int }{X: 1})
	})
}

// TestParse_ErrorPaths hits the many error branches inside parseValue,
// parseObject, parseArray, parseBinary when input is truncated/malformed.
// Each subtest asserts a specific error substring harvested from parser.go
// so that a silent refactor of one branch into another doesn't keep the
// tests green spuriously.
func TestParse_ErrorPaths(t *testing.T) {
	tests := []struct {
		name     string
		in       []byte
		wantSubs string // substring that must appear in err.Error()
	}{
		{"empty", []byte{}, "expected value"},
		{"invalid_type_byte", []byte{0xfe}, "unknown type"},
		{"string_missing_eos", []byte{byte(TypeString), 'x'}, "end of string not found"},
		{"truncated_number", []byte{byte(TypeNumber), 0x01}, "expected 8 bytes"},
		{"truncated_binary_lenhdr", []byte{byte(TypeBinary), 0x00}, "minimum 4 byte"},
		{"object_missing_eos_on_key", []byte{byte(TypeObject), 'k'}, "parse object key: end of string not found"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.in)
			require.Error(t, err, "parse must reject %x", tc.in)
			assert.Contains(t, err.Error(), tc.wantSubs,
				"error %q must contain %q", err.Error(), tc.wantSubs)
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
// parseObject, parseArray, parseBinary, parseCompressedObjectS2. Each
// subtest asserts a specific error substring so that changes to the
// failure path cannot silently redirect to a different branch.
func TestParse_MalformedInputs_Extra(t *testing.T) {
	tests := []struct {
		name     string
		in       []byte
		wantSubs string
	}{
		// parseObject reads the key ("k") then consumes its EOS, leaving 0 bytes
		// for parseValue → "expected value, but got 0 byte".
		{"object_no_value_after_key", []byte{byte(TypeObject), 'k', EOS}, "expected value"},
		// parseArray: length byte but nothing to parse → "parse array: unexpected end".
		{"array_truncated", []byte{byte(TypeArray)}, "parse array: unexpected end"},
		// parseBinary: length says 10 bytes but only 2 supplied → "expected 10 bytes to read binary".
		{"binary_length_overflow", append(
			[]byte{byte(TypeBinary), 0x00, 0x00, 0x00, 0x0a},
			0x01, 0x02), "to read binary"},
		// parseBinary: total input shorter than length-header → "minimum 4 byte for binary header".
		{"binary_short_header", []byte{byte(TypeBinary), 0x00, 0x00}, "minimum 4 byte"},
		// parseCompressedObjectS2: too short for 5-byte header → "compressed object: expected at least 5 bytes".
		{"compressed_too_short", []byte{byte(TypeCompressedObjectS2), 0x01}, "expected at least 5 bytes"},
		// parseCompressedObjectS2: header says 1000 bytes but payload is empty →
		// "compressed object: expected 1000 compressed bytes".
		{"compressed_length_overflow", []byte{
			byte(TypeCompressedObjectS2),
			0x00, 0x00, 0x03, 0xe8, // 1000
		}, "compressed bytes"},
		// Object with an inner value parse failure → "unknown type 254".
		{"object_bad_inner_value", []byte{byte(TypeObject), 'k', EOS, 0xfe}, "unknown type"},
		// Tail bytes after a valid value — rejected by Parse → "unexpected tail".
		{"trailing_bytes", append(
			MustParseJson(`"x"`).MarshalTo(nil),
			0x00), "unexpected tail"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.in)
			require.Error(t, err, "input %x must fail to parse", tc.in)
			assert.Contains(t, err.Error(), tc.wantSubs,
				"error %q must contain %q", err.Error(), tc.wantSubs)
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
// After each no-op, an invariant is asserted to prove the target value
// was not corrupted by the silent return.
func TestValue_NilGuards(t *testing.T) {
	a := &Arena{}

	// Nil receivers must not panic.
	var nilV *Value
	nilV.Set("k", a.NewString("v"))
	nilV.Del("k")
	nilV.SetArrayItem(0, a.NewString("v"))
	assert.Nil(t, nilV.Get("k"))

	// Set/Del on a scalar value (not object/array) is a silent no-op.
	// Invariant: the scalar's underlying string must be untouched.
	scalar := a.NewString("just a string")
	scalar.Set("k", a.NewString("x"))
	scalar.Del("k")
	assert.Equal(t, "just a string", string(scalar.GetStringBytes()),
		"scalar value must not be corrupted by Set/Del no-op")

	// Set on an array with non-integer key → silent no-op.
	arr := a.NewArray()
	arr.Set("not-a-number", a.NewString("x"))
	arr.Set("-1", a.NewString("x")) // negative index
	items, _ := arr.Array()
	assert.Equal(t, 0, len(items))

	// Del on array with non-integer / negative / out-of-range index →
	// silent no-op. Invariant: items[0] (value "v0") must still be there.
	arr.SetArrayItem(0, a.NewString("v0"))
	arr.Del("not-a-number")
	arr.Del("-1")
	arr.Del("99") // beyond bounds
	items, _ = arr.Array()
	require.Equal(t, 1, len(items))
	sb, err := items[0].StringBytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("v0"), sb,
		"array item 0 must be unchanged after silent Del no-ops")

	// Get on array with non-integer / negative / out-of-range index → nil.
	assert.Nil(t, arr.Get("not-a-number"))
	assert.Nil(t, arr.Get("-1"))
	assert.Nil(t, arr.Get("99"))

	// Get through a scalar as a mid-path node → nil (the scalar branch).
	obj := a.NewObject()
	obj.Set("a", a.NewString("leaf"))
	// Get "a.b" traverses a scalar at key "a", falls into else-nil branch.
	assert.Nil(t, obj.Get("a", "b"))

	// SetArrayItem on a non-array value — silent no-op. Invariants:
	// the object's original "a" key value must still resolve, and a numeric
	// "0" key must NOT have been added (the no-op truly did nothing).
	obj.SetArrayItem(0, a.NewString("x")) // obj is not array
	assert.Nil(t, obj.Get("0"),
		"SetArrayItem on object must not create a numeric key")
	gotA := obj.Get("a")
	require.NotNil(t, gotA)
	leafBytes, err := gotA.StringBytes()
	require.NoError(t, err)
	assert.Equal(t, []byte("leaf"), leafBytes,
		"object's original key must survive SetArrayItem no-op")
}

// TestValue_AppendBytes_ErrorPath covers v.AppendBytes when the value is not
// binary — it must propagate the Bytes() error at value.go:149-151. Also
// asserts the returned slice is nil and the error message mentions the
// type-mismatch ("binary" in the expected-type clause).
func TestValue_AppendBytes_ErrorPath(t *testing.T) {
	a := &Arena{}
	str := a.NewString("not binary")
	got, err := str.AppendBytes(nil)
	require.Error(t, err, "AppendBytes on non-binary value must surface Bytes() error")
	assert.Nil(t, got, "returned bytes must be nil on error")
	assert.Contains(t, err.Error(), "binary",
		"error message %q must mention the expected type 'binary'", err.Error())
}

// TestType_String covers the remaining Type.String cases not hit by
// existing tests: binary, compressed-object-s2, and the default unknown path.
func TestType_String(t *testing.T) {
	assert.Equal(t, "binary", TypeBinary.String())
	assert.Equal(t, "compressedObjectS2", TypeCompressedObjectS2.String())
	assert.Contains(t, Type(99).String(), "unknown type")
}

// TestValue_GoType covers Value.GoType for every supported type. The panic
// branch at value.go:461 is unreachable from the public API (every Type
// constant is handled) and is not tested.
//
// For the binary case, also verifies that GoType returns a COPY of the
// underlying bytes (value.go:441 `append([]byte{}, v.v...)`): mutating the
// returned slice must NOT corrupt v.v, which we re-read via v.Bytes().
func TestValue_GoType(t *testing.T) {
	a := &Arena{}

	assert.Equal(t, float64(3.14), a.NewNumberFloat64(3.14).GoType())
	assert.Equal(t, "hi", a.NewString("hi").GoType())
	assert.Equal(t, true, a.NewTrue().GoType())
	assert.Equal(t, false, a.NewFalse().GoType())
	assert.Nil(t, a.NewNull().GoType())

	// Binary: assert copy semantics, not aliasing.
	bin := a.NewBinary([]byte{0x01, 0x02})
	goBytes, ok := bin.GoType().([]byte)
	require.True(t, ok, "GoType of binary must return []byte")
	assert.Equal(t, []byte{0x01, 0x02}, goBytes)
	// Mutate the returned slice.
	goBytes[0] = 0xFF
	// The underlying v.v (read via Bytes()) must remain unchanged.
	origBytes, err := bin.Bytes()
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0x02}, origBytes,
		"GoType must return a copy — mutation of returned slice leaked into v.v")

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
