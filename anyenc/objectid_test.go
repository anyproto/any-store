package anyenc

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deterministic ids for order/round-trip assertions.
func mustOID(t *testing.T, h string) ObjectID {
	t.Helper()
	id, err := ObjectIDFromHex(h)
	require.NoError(t, err)
	return id
}

func TestObjectID_ValueRoundTrip(t *testing.T) {
	a := &Arena{}
	id := mustOID(t, "0123456789abcdef01234567")

	enc := a.NewObjectID(id).MarshalTo(nil)
	// Fixed 13 bytes: tag + 12 raw bytes, no length prefix.
	require.Len(t, enc, 1+objectIDLen)
	assert.Equal(t, byte(TypeObjectID), enc[0])
	assert.True(t, bytes.Equal(enc[1:], id[:]))

	v, err := Parse(enc)
	require.NoError(t, err)
	assert.Equal(t, TypeObjectID, v.Type())

	got, err := v.ObjectID()
	require.NoError(t, err)
	assert.Equal(t, id, got)
}

func TestObjectID_InObject(t *testing.T) {
	a := &Arena{}
	id := NewObjectID()
	// any-store's primary key field is "id".
	doc := a.NewObject()
	doc.Set("id", a.NewObjectID(id))
	doc.Set("n", a.NewNumberInt(7))

	v, err := Parse(doc.MarshalTo(nil))
	require.NoError(t, err)

	assert.Equal(t, id, v.GetObjectID("id"))
	got, err := v.Get("id").ObjectID()
	require.NoError(t, err)
	assert.Equal(t, id, got)
	assert.Equal(t, 7, v.GetInt("n"))
}

func TestObjectID_Accessors_WrongType(t *testing.T) {
	a := &Arena{}
	v := a.NewString("not an id")

	_, err := v.ObjectID()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "objectID")

	// GetObjectID returns NilObjectID on absent / wrong type.
	assert.Equal(t, NilObjectID, v.GetObjectID())
	assert.True(t, v.GetObjectID().IsZero())

	obj := a.NewObject()
	obj.Set("id", a.NewNumberInt(1))
	assert.Equal(t, NilObjectID, obj.GetObjectID("id"))
	assert.Equal(t, NilObjectID, obj.GetObjectID("missing"))
}

func TestObjectID_FastJsonAndGoType(t *testing.T) {
	a := &Arena{}
	id := mustOID(t, "0123456789abcdef01234567")
	v := a.NewObjectID(id)

	// FastJson/String render the hex form (lossy display, like Binary->base64).
	assert.Equal(t, `"0123456789abcdef01234567"`, v.String())
	assert.Equal(t, id.Hex(), v.GoType())
}

func TestObjectID_MemcmpOrderable(t *testing.T) {
	id1 := mustOID(t, "0123456789abcdef01234567")
	id2 := mustOID(t, "0123456789abcdef01234568") // one greater in the last byte
	a := &Arena{}

	e1 := a.NewObjectID(id1).MarshalTo(nil)
	e2 := a.NewObjectID(id2).MarshalTo(nil)
	assert.Equal(t, -1, bytes.Compare(e1, e2), "objectID encoding must be byte-orderable")

	// Inversion reverses order (reverse index keys).
	inv1 := Tuple{}.AppendInverted(a.NewObjectID(id1))
	inv2 := Tuple{}.AppendInverted(a.NewObjectID(id2))
	assert.Equal(t, 1, bytes.Compare(inv1, inv2))

	// Tag 0x0B sorts after every existing scalar type.
	str := a.NewString("zzzz").MarshalTo(nil)
	num := a.NewNumberInt(1 << 30).MarshalTo(nil)
	assert.Equal(t, 1, bytes.Compare(e1, str))
	assert.Equal(t, 1, bytes.Compare(e1, num))
}

// The inverted objectID field must be length-skippable by tuple readers even
// though its tag (0xF4) sits below the contiguous inverted-scalar run.
func TestObjectID_InvertedSkip(t *testing.T) {
	a := &Arena{}
	id := NewObjectID()
	marker := MustParseJson(`42`)

	inv := Tuple{}.AppendInverted(a.NewObjectID(id))
	full := append(append(Tuple{}, inv...), Tuple{}.Append(marker)...)

	off, err := inv.OffsetAfter(1)
	require.NoError(t, err)
	assert.Equal(t, len(inv), off)

	fb, err := full.FieldBytes(0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb, inv))
	fb1, err := full.FieldBytes(1)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb1, Tuple{}.Append(marker)))
}

func TestObjectID_ParseTruncated(t *testing.T) {
	// tag + only 3 of 12 payload bytes.
	enc := []byte{byte(TypeObjectID), 1, 2, 3}
	_, err := Parse(enc)
	require.ErrorContains(t, err, "objectID")
}

// A stray inverted-vector tag (0xF5) must still be rejected — the non-contiguous
// inverted detection must not swallow it when it added 0xF4.
func TestObjectID_StrayInvertedVectorStillErrors(t *testing.T) {
	// length-only skip path (c == nil) via a tuple reader.
	_, err := Tuple([]byte{byte(iTypeObjectID) + 1 /* 0xF5 */, 0, 0, 0, 0}).OffsetAfter(1)
	require.Error(t, err)
}

func TestObjectID_ZeroAllocations(t *testing.T) {
	a := &Arena{}
	id := NewObjectID()
	v := a.NewObjectID(id)
	enc := v.MarshalTo(nil)
	p := &Parser{}
	dst := make([]byte, 0, 1+objectIDLen)

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		sinkOID, _ = v.ObjectID()
	}), "ObjectID() accessor must not allocate")

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		sinkBytes = v.MarshalTo(dst[:0])
	}), "MarshalTo must not allocate on a warm buffer")

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		a.Reset()
		sinkVal = a.NewObjectID(id)
	}), "NewObjectID must not allocate on a warm arena")

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		sinkVal, _ = p.ParseOwned(enc)
	}), "parse must not allocate on a warm parser")
}

// --- generator / type behavior (ported from the former internal/objectid) ---

func TestObjectID_String(t *testing.T) {
	id := NewObjectID()
	require.Contains(t, id.String(), id.Hex())
}

func TestObjectID_FromHex_RoundTrip(t *testing.T) {
	before := NewObjectID()
	after, err := ObjectIDFromHex(before.Hex())
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestObjectID_FromHex_Invalid(t *testing.T) {
	_, err := ObjectIDFromHex("this is not a valid hex string!")
	require.Error(t, err)

	_, err = ObjectIDFromHex("deadbeef")
	require.Equal(t, ErrInvalidHex, err)
}

func TestObjectID_Timestamp(t *testing.T) {
	cases := []struct{ hex, want string }{
		{"000000001111111111111111", "1970-01-01 00:00:00 +0000 UTC"},
		{"7FFFFFFF1111111111111111", "2038-01-19 03:14:07 +0000 UTC"},
		{"800000001111111111111111", "2038-01-19 03:14:08 +0000 UTC"},
		{"FFFFFFFF1111111111111111", "2106-02-07 06:28:15 +0000 UTC"},
	}
	for _, c := range cases {
		id, err := ObjectIDFromHex(c.hex)
		require.NoError(t, err)
		assert.Equal(t, c.want, id.Timestamp().String())
	}
}

func TestObjectID_FromTimestamp(t *testing.T) {
	layout := "2006-01-02T15:04:05.000Z"
	cases := []struct{ in, want string }{
		{"1970-01-01T00:00:00.000Z", "00000000"},
		{"2038-01-19T03:14:07.000Z", "7fffffff"},
		{"2106-02-07T06:28:15.000Z", "ffffffff"},
	}
	for _, c := range cases {
		ts, err := time.Parse(layout, c.in)
		require.NoError(t, err)
		id := NewObjectIDFromTimestamp(ts)
		assert.Equal(t, c.want, hex.EncodeToString(id[0:4]))
	}
}

func TestObjectID_CounterOverflow(t *testing.T) {
	objectIDCounter = 0xFFFFFFFF
	NewObjectID()
	require.Equal(t, uint32(0), objectIDCounter)
}

// package-level sinks keep the zero-alloc benchmark/test bodies from being
// optimized away and prevent the results from escaping to the heap.
var (
	sinkOID   ObjectID
	sinkBytes []byte
	sinkVal   *Value
)
