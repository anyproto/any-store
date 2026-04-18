package anyenc

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

var encDecData = []string{
	`"a"`,
	`22.32`,
	`true`,
	`false`,
	`null`,
	`["1","2","3"]`,
	`{"key":"value"}`,
	`{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`,
	`[{},{"":[{"":1},{}]},{"a":"b"}]`,
}

func TestEncodeDecode(t *testing.T) {
	var a = &Arena{}
	var fa = &fastjson.Arena{}
	var p = &Parser{}
	var buf []byte
	for _, js := range encDecData {
		jv := fastjson.MustParse(js)
		anyVal := a.NewFromFastJson(jv)
		buf = anyVal.MarshalTo(buf[:0])
		assert.NotEmpty(t, buf, js)
		decVal, err := p.Parse(buf)
		require.NoError(t, err, js)
		require.NotNil(t, decVal, js)
		res := decVal.FastJson(fa).String()
		assert.Equal(t, js, res)
		t.Logf("type: %s; json len: %d; anyenc len: %d", decVal.Type(), len(js), len(buf))
	}
}

func TestBinary(t *testing.T) {
	a := &Arena{}
	p := &Parser{}
	payload := []byte{1, 2, 3}
	bin := a.NewBinary(payload)
	buf := bin.MarshalTo(nil)
	val, err := p.Parse(buf)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload, val.GetBytes()))
}

func BenchmarkParser_Parse(b *testing.B) {
	a := &Arena{}
	js := `{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`
	data := a.NewFromFastJson(fastjson.MustParse(js)).MarshalTo(nil)
	p := &Parser{}
	fp := &fastjson.Parser{}
	b.Run("anyenc", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = p.Parse(data)
		}
	})
	b.Run("fastjson", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			_, _ = fp.Parse(js)
		}
	})

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
