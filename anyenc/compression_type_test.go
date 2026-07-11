package anyenc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func genLargeTestDoc() string {
	var sb strings.Builder
	sb.WriteString(`{"id":"doc-12345","items":[`)
	for i := 0; i < 50; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, `{"idx":%d,"name":"item-%d","val":%d}`, i, i, i*17)
	}
	sb.WriteString(`],"meta":{"created":"2025-01-15","version":3}}`)
	return sb.String()
}

var compressedLargeDoc = genLargeTestDoc()

func TestMarshalCompressedRoundtrip(t *testing.T) {
	a := &Arena{}
	fa := &fastjson.Arena{}
	p := &Parser{}

	jv := fastjson.MustParse(compressedLargeDoc)
	val := a.NewFromFastJson(jv)

	// Verify it's above threshold
	require.True(t, val.IsSizeBigger(CompressMinSize), "test doc must exceed CompressMinSize")

	var dst, scratch []byte
	dst, scratch = val.MarshalCompressed(nil, scratch)
	require.NotEmpty(t, dst)
	_ = scratch

	// First byte should be TypeCompressedObjectS2
	assert.Equal(t, byte(TypeCompressedObjectS2), dst[0])

	// Parse it back
	decoded, err := p.Parse(dst)
	require.NoError(t, err)
	require.NotNil(t, decoded)

	got := decoded.FastJson(fa).String()
	assert.Equal(t, compressedLargeDoc, got)
}

func TestMarshalCompressedSmallFallback(t *testing.T) {
	a := &Arena{}
	p := &Parser{}
	fa := &fastjson.Arena{}

	// Small object below threshold — should fall back to plain MarshalTo
	js := `{"id":"abc","x":1}`
	jv := fastjson.MustParse(js)
	val := a.NewFromFastJson(jv)

	require.False(t, val.IsSizeBigger(CompressMinSize), "small doc should be below threshold")

	var dst, scratch []byte
	dst, scratch = val.MarshalCompressed(nil, scratch)
	_ = scratch
	expected := val.MarshalTo(nil)
	assert.Equal(t, expected, dst)

	// Should still parse correctly (it's a normal TypeObject)
	decoded, err := p.Parse(dst)
	require.NoError(t, err)
	got := decoded.FastJson(fa).String()
	assert.Equal(t, js, got)
}

// genIncompressibleTestDoc builds an object whose payload is deterministic
// high-entropy bytes (a keyed xorshift stream) — the shape of encrypted CRDT
// change bodies, which S2 cannot shrink.
func genIncompressibleTestDoc(a *Arena, n int) *Value {
	data := make([]byte, n)
	state := uint64(0x9e3779b97f4a7c15)
	for i := range data {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		data[i] = byte(state)
	}
	obj := a.NewObject()
	obj.Set("id", a.NewString("enc-doc"))
	obj.Set("payload", a.NewBinary(data))
	return obj
}

// TestMarshalCompressedIncompressibleKeepsPlain: when S2 output would not be
// smaller than the plain encoding, the plain TypeObject bytes must be stored
// instead — same format old readers already handle, and no decode+copy cost on
// every future parse.
func TestMarshalCompressedIncompressibleKeepsPlain(t *testing.T) {
	a := &Arena{}
	p := &Parser{}

	val := genIncompressibleTestDoc(a, 4096)
	require.True(t, val.IsSizeBigger(CompressMinSize))

	plain := val.MarshalTo(nil)
	dst, _ := val.MarshalCompressed(nil, nil)

	assert.Equal(t, byte(TypeObject), dst[0], "incompressible object must be stored plain")
	assert.Equal(t, plain, dst)

	decoded, err := p.Parse(dst)
	require.NoError(t, err)
	assert.Equal(t, "enc-doc", string(decoded.GetStringBytes("id")))
	assert.Equal(t, val.GetBytes("payload"), decoded.GetBytes("payload"))

	// The fallback truncates dst back to the header start — a non-empty dst
	// prefix must survive.
	prefix := []byte("prefix")
	dst2, _ := val.MarshalCompressed(append([]byte{}, prefix...), nil)
	assert.Equal(t, prefix, dst2[:len(prefix)])
	assert.Equal(t, plain, dst2[len(prefix):])
}

func TestParserTrimScratch(t *testing.T) {
	a := &Arena{}
	p := &Parser{}

	// A compressible large doc so the parse path actually inflates decompBuf.
	val := a.NewFromFastJson(fastjson.MustParse(compressedLargeDoc))
	dst, _ := val.MarshalCompressed(nil, nil)
	require.Equal(t, byte(TypeCompressedObjectS2), dst[0])
	_, err := p.Parse(dst)
	require.NoError(t, err)
	require.Greater(t, cap(p.c.decompBuf), 0)
	require.Greater(t, cap(p.b), 0)

	p.TrimScratch(1 << 30) // above the combined caps: keeps everything
	assert.Greater(t, cap(p.c.decompBuf), 0)
	assert.Greater(t, cap(p.b), 0)

	p.TrimScratch(0) // no limit: keeps everything
	assert.Greater(t, cap(p.c.decompBuf), 0)

	// Sum over limit but input copy alone within it: only decompBuf is freed —
	// the bound is on the combined retention, sacrificing decompBuf first.
	p.TrimScratch(cap(p.b) + cap(p.c.decompBuf) - 1)
	assert.Equal(t, 0, cap(p.c.decompBuf))
	assert.Greater(t, cap(p.b), 0)

	_, err = p.Parse(dst) // regrow both
	require.NoError(t, err)
	require.Greater(t, cap(p.c.decompBuf), 0)

	p.TrimScratch(16) // both over the limit: frees both
	assert.Equal(t, 0, cap(p.c.decompBuf))
	assert.Equal(t, 0, cap(p.b))

	// Parser must still work after trimming.
	_, err = p.Parse(dst)
	require.NoError(t, err)
}

func TestMarshalCompressedNonObject(t *testing.T) {
	a := &Arena{}

	cases := []struct {
		name string
		val  *Value
	}{
		{"string", a.NewString("hello")},
		{"number", a.NewNumberFloat64(42)},
		{"true", a.NewTrue()},
		{"false", a.NewFalse()},
		{"null", a.NewNull()},
		{"nil", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var dst, scratch []byte
			dst, scratch = tc.val.MarshalCompressed(nil, scratch)
			expected := tc.val.MarshalTo(nil)
			assert.Equal(t, expected, dst)
			_ = scratch
		})
	}
}

func TestIsSizeBigger(t *testing.T) {
	a := &Arena{}

	// Small object
	small := a.NewFromFastJson(fastjson.MustParse(`{"id":"a"}`))
	assert.False(t, small.IsSizeBigger(50))
	assert.True(t, small.IsSizeBigger(2))

	// Large object — estimateSize may slightly overcount (EOS bytes in strings)
	// but should be close to actual marshaled size and always >= it
	large := a.NewFromFastJson(fastjson.MustParse(compressedLargeDoc))
	marshaledSize := len(large.MarshalTo(nil))
	assert.True(t, large.IsSizeBigger(marshaledSize-1))
	assert.True(t, large.IsSizeBigger(100)) // well above 100 bytes

	// Nil — marshals to 1 byte (TypeNull)
	assert.False(t, (*Value)(nil).IsSizeBigger(1))
	assert.True(t, (*Value)(nil).IsSizeBigger(0))
}

func BenchmarkIsSizeBigger(b *testing.B) {
	a := &Arena{}
	val := a.NewFromFastJson(fastjson.MustParse(compressedLargeDoc))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = val.IsSizeBigger(CompressMinSize)
	}
}

func BenchmarkMarshalCompressed(b *testing.B) {
	a := &Arena{}
	val := a.NewFromFastJson(fastjson.MustParse(compressedLargeDoc))

	dst := make([]byte, 0, 2048)
	scratch := make([]byte, 0, 2048)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		dst, scratch = val.MarshalCompressed(dst[:0], scratch)
	}
}

func BenchmarkParseCompressed(b *testing.B) {
	a := &Arena{}
	val := a.NewFromFastJson(fastjson.MustParse(compressedLargeDoc))

	var dst, scratch []byte
	dst, scratch = val.MarshalCompressed(nil, scratch)
	_ = scratch

	p := &Parser{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = p.Parse(dst)
	}
}
