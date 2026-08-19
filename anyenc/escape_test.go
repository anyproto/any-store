package anyenc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEscapeStringRoundTrip(t *testing.T) {
	cases := []string{
		"",
		"plain",
		"\x00",
		"\x00\x00\x00",
		"a\x00b",
		"\x00leading",
		"trailing\x00",
		"a\x00\xffb", // data that looks like an escape pair
		"\xff",
		"\x1f",
		strings.Repeat("x\x00", 100),
	}
	for _, s := range cases {
		a := &Arena{}
		enc := a.NewString(s).MarshalTo(nil)
		v, err := Parse(enc)
		require.NoError(t, err, "%q", s)
		assert.Equal(t, s, v.GetString(), "%q", s)
	}
}

func TestEscapeKeyRoundTrip(t *testing.T) {
	cases := []string{
		"",
		"plain",
		"\x00",
		"a\x00b",
		"\x1f\x1f", // leading 0x1F keys keep their old-format literal encoding
		"\x1fkey",
		"\xff", // reserved first byte, emptyKey-prefixed
		"\xffkey",
		"\x00key",
		"key\x1f", // reserved bytes are only special at position 0
		"key\xff",
	}
	for _, key := range cases {
		a := &Arena{}
		obj := a.NewObject()
		obj.Set(key, a.NewNumberInt(42))
		enc := obj.MarshalTo(nil)
		v, err := Parse(enc)
		require.NoError(t, err, "%q", key)
		o, err := v.Object()
		require.NoError(t, err, "%q", key)
		require.Equal(t, 1, o.Len(), "%q", key)
		o.Visit(func(k []byte, vv *Value) {
			assert.Equal(t, key, string(k), "%q", key)
			n, _ := vv.Int()
			assert.Equal(t, 42, n)
		})
	}
}

// Encoded strings must compare (bytes.Compare) exactly like the source strings,
// including strings with embedded zero bytes — that is the invariant index keys
// rely on. Inverted (descending) fields must compare in exactly reversed order
// once a following field is appended — index keys always continue with the
// docId suffix, and a bare inverted field that is an encoding-prefix of another
// (only possible between "a" and "a\x00...") has no defined bare order.
func TestEscapeOrderPreserved(t *testing.T) {
	ss := []string{
		"", "\x00", "\x00\x00", "\x00\x01", "\x01", "a", "a\x00", "a\x00b",
		"a\x01", "ab", "a\xff", "b", "\xff", "\xff\x00",
	}
	a := &Arena{}
	docId := a.NewString("docid")
	for _, s1 := range ss {
		for _, s2s := range ss {
			e1 := a.NewString(s1).MarshalTo(nil)
			e2 := a.NewString(s2s).MarshalTo(nil)
			assert.Equal(t, sign(strings.Compare(s1, s2s)), sign(bytes.Compare(e1, e2)),
				"%q vs %q (% x vs % x)", s1, s2s, e1, e2)

			// the same must hold with a following tuple field appended
			t1 := Tuple(e1).Append(a.NewNumberInt(7))
			t2 := Tuple(e2).Append(a.NewNumberInt(7))
			assert.Equal(t, sign(strings.Compare(s1, s2s)), sign(bytes.Compare(t1, t2)),
				"tuple %q vs %q", s1, s2s)

			// inverted fields in index-key shape (docId appended, not inverted)
			i1 := Tuple(nil).AppendInverted(a.NewString(s1)).Append(docId)
			i2 := Tuple(nil).AppendInverted(a.NewString(s2s)).Append(docId)
			assert.Equal(t, -sign(strings.Compare(s1, s2s)), sign(bytes.Compare(i1, i2)),
				"inverted %q vs %q", s1, s2s)
		}
	}
}

// The encoding of "a" is a byte-prefix of the encoding of "a\x00b" (kept for
// back-compatibility with the old single-byte terminator). Range bounds remain
// exact because the escape continuation byte (0xFF ascending, 0x00 inverted)
// sorts outside the padding qplanner appends for docId suffixes: ascending
// End = enc(v)+0xFF still excludes enc(v)+0xFF+more (longer sorts after).
func TestEscapePrefixOverlapBounds(t *testing.T) {
	a := &Arena{}
	encA := a.NewString("a").MarshalTo(nil)
	encANul := a.NewString("a\x00b").MarshalTo(nil)
	require.True(t, bytes.HasPrefix(encANul, encA))

	// ascending: stored key enc("a")+docIdTag is within [enc("a"), enc("a")+0xFF],
	// stored key for "a\x00b" is not.
	end := append(append([]byte{}, encA...), 0xff)
	keyA := Tuple(encA).Append(a.NewString("docid"))
	assert.True(t, bytes.Compare(keyA, end) <= 0)
	keyANul := Tuple(encANul).Append(a.NewString("docid"))
	assert.True(t, bytes.Compare(keyANul, end) > 0)

	// inverted: stored key inv("a")+docIdTag is above inv("a")+0x00..., so a
	// Start of inv(enc("a"))+0x01 separates them.
	invA := Tuple(nil).AppendInverted(a.NewString("a"))
	invANul := Tuple(nil).AppendInverted(a.NewString("a\x00b"))
	start := append(append([]byte{}, invA...), 0x01)
	ikeyA := Tuple(invA.Copy()).Append(a.NewString("docid"))
	ikeyANul := Tuple(invANul).Append(a.NewString("docid"))
	assert.True(t, bytes.Compare(ikeyA, start) >= 0)
	assert.True(t, bytes.Compare(ikeyANul, start) < 0)
}

// NUL-free data must encode bit-identically to the historical format (the old
// encoder stripped NULs, so no existing stored data contains escape pairs):
// hand-built old-format bytes equal the new encoder's output and parse back.
func TestBackCompatOldFormat(t *testing.T) {
	old := []byte{byte(TypeObject)}
	old = append(old, 'k', 'e', 'y', EOS, byte(TypeString), 'v', 'a', 'l', EOS)
	old = append(old, 'n', EOS)
	old = AppendAnyValue(old, 42)
	old = append(old, emptyKey, EOS, byte(TypeTrue))  // empty key
	old = append(old, 0x1f, 'k', EOS, byte(TypeNull)) // old-format literal key "\x1fk"
	old = append(old, EOS)

	v, err := Parse(old)
	require.NoError(t, err)
	assert.Equal(t, "val", v.GetString("key"))
	assert.Equal(t, 42, v.GetInt("n"))
	assert.True(t, v.GetBool(""))
	assert.Equal(t, TypeNull, v.Get("\x1fk").Type())

	reencoded := v.MarshalTo(nil)
	assert.Equal(t, old, reencoded)
}

// Known wart kept for back-compatibility: the single-byte key "\x1f" encodes
// to the empty-key marker and decodes as "".
func TestEmptyKeyWart(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	obj.Set("\x1f", a.NewNumberInt(1))
	v, err := Parse(obj.MarshalTo(nil))
	require.NoError(t, err)
	assert.Equal(t, 1, v.GetInt(""))
}

func sign(n int) int {
	switch {
	case n > 0:
		return 1
	case n < 0:
		return -1
	}
	return 0
}

// Tuple skipping must step over escaped strings and objects with escaped keys,
// in both normal and inverted form.
func TestEscapeTupleSkip(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	obj.Set("k\x00ey", a.NewString("va\x00lue"))

	for _, inverted := range []bool{false, true} {
		var tup Tuple
		if inverted {
			tup = tup.AppendInverted(a.NewString("a\x00b"))
			tup = tup.AppendInverted(obj)
		} else {
			tup = tup.Append(a.NewString("a\x00b"))
			tup = tup.Append(obj)
		}
		tup = tup.Append(a.NewString("last"))

		off, err := tup.OffsetAfter(2)
		require.NoError(t, err, "inverted=%v", inverted)
		fb, err := tup.FieldBytes(2)
		require.NoError(t, err, "inverted=%v", inverted)
		v, err := Parse(fb)
		require.NoError(t, err)
		assert.Equal(t, "last", v.GetString(), "inverted=%v", inverted)
		assert.Equal(t, len(tup)-len(fb), off)
	}
}

// AppendAnyValue must produce byte-identical encoding to Value.MarshalTo for
// the same string: FindId builds lookup keys with the former, inserts build
// stored keys with the latter.
func TestEscapeAppendAnyValueConsistent(t *testing.T) {
	a := &Arena{}
	for _, s := range []string{"plain", "a\x00b", "\x00", "x\xffy"} {
		fromAny := AppendAnyValue(nil, s)
		fromVal := a.NewString(s).MarshalTo(nil)
		assert.Equal(t, fromVal, fromAny, "%q", s)
		fromBytes := AppendAnyValue(nil, []byte(s))
		assert.Equal(t, fromVal, fromBytes, "%q", s)
	}
}

func TestParseDepthLimit(t *testing.T) {
	// a long run of array tags must return an error, not overflow the stack
	b := make([]byte, 1<<20)
	for i := range b {
		b[i] = byte(TypeArray)
	}
	_, err := Parse(b)
	require.ErrorContains(t, err, "max parse depth")

	// nesting within the limit still parses
	depth := 100
	ok := make([]byte, 0, depth*2)
	for i := 0; i < depth; i++ {
		ok = append(ok, byte(TypeArray))
	}
	for i := 0; i < depth; i++ {
		ok = append(ok, EOS)
	}
	_, err = Parse(ok)
	require.NoError(t, err)
}

func TestParseNestedCompressedRejected(t *testing.T) {
	a := &Arena{}
	inner := a.NewObject()
	for i := 0; i < 20; i++ {
		inner.Set(fmt.Sprintf("key%d", i), a.NewString("inner-value-inner-value"))
	}
	innerComp, _ := inner.MarshalCompressed(nil, nil)
	require.Equal(t, byte(TypeCompressedObjectS2), innerComp[0])

	outerPlain := []byte{byte(TypeObject)}
	outerPlain = appendEscapedKey(outerPlain, "k")
	outerPlain = append(outerPlain, innerComp...)
	outerPlain = append(outerPlain, EOS)

	comp := s2.Encode(nil, outerPlain)
	enc := []byte{byte(TypeCompressedObjectS2)}
	enc = binary.BigEndian.AppendUint32(enc, uint32(len(comp)))
	enc = append(enc, comp...)

	_, err := Parse(enc)
	require.Error(t, err)
}

func TestParseCompressedSizeLimit(t *testing.T) {
	// hand-craft an s2 block whose header claims a huge decoded size
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(maxDecompressedSize)+1)
	comp := append(hdr[:n], 0x00) // header + one (bogus) literal byte
	enc := []byte{byte(TypeCompressedObjectS2)}
	enc = binary.BigEndian.AppendUint32(enc, uint32(len(comp)))
	enc = append(enc, comp...)

	_, err := Parse(enc)
	require.ErrorContains(t, err, "exceeds limit")
}

func TestParseCompressedInnerTail(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	obj.Set("k", a.NewString("v"))
	plain := obj.MarshalTo(nil)
	plain = append(plain, 0xAB) // trailing garbage inside the compressed payload

	comp := s2.Encode(nil, plain)
	enc := []byte{byte(TypeCompressedObjectS2)}
	enc = binary.BigEndian.AppendUint32(enc, uint32(len(comp)))
	enc = append(enc, comp...)

	_, err := Parse(enc)
	require.ErrorContains(t, err, "unexpected tail")
}

func TestParseVectorLengthValidated(t *testing.T) {
	enc := []byte{byte(TypeVectorF32)}
	enc = binary.BigEndian.AppendUint32(enc, 6) // not a multiple of 4
	enc = append(enc, 1, 2, 3, 4, 5, 6)
	_, err := Parse(enc)
	require.ErrorContains(t, err, "multiple of 4")
}

func TestNegativeZeroNormalized(t *testing.T) {
	pos := AppendFloat64(nil, 0)
	neg := AppendFloat64(nil, math.Copysign(0, -1))
	assert.Equal(t, pos, neg)
	assert.Equal(t, float64(0), BytesToFloat64(neg))
}

// Marshal must be a fixed point through parse: parse(b) -> marshal -> parse ->
// marshal yields identical bytes, and corrupt input never panics.
func FuzzParse(f *testing.F) {
	a := &Arena{}
	obj := a.NewObject()
	obj.Set("key", a.NewString("value"))
	obj.Set("a\x00b", a.NewNumberInt(42))
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewString("s\x00"))
	arr.SetArrayItem(1, a.NewBinary([]byte{0, 1, 2}))
	obj.Set("arr", arr)
	oid, _ := ObjectIDFromHex("0123456789abcdef01234567")
	obj.Set("oid", a.NewObjectID(oid))
	f.Add(a.NewObjectID(oid).MarshalTo(nil))
	f.Add(obj.MarshalTo(nil))
	comp, _ := obj.MarshalCompressed(nil, nil)
	f.Add(comp)
	f.Add([]byte{byte(TypeArray), byte(TypeArray), EOS, EOS})
	f.Add([]byte{byte(TypeString), 'a', EOS})

	f.Fuzz(func(t *testing.T, data []byte) {
		p := &Parser{}
		v, err := p.Parse(data)
		if err != nil {
			return
		}
		m1 := v.MarshalTo(nil)
		v2, err := Parse(m1)
		if err != nil {
			t.Fatalf("remarshal of valid value failed to parse: %v (input % x, marshaled % x)", err, data, m1)
		}
		m2 := v2.MarshalTo(nil)
		if !bytes.Equal(m1, m2) {
			t.Fatalf("marshal not a fixed point: % x != % x (input % x)", m1, m2, data)
		}
	})
}
