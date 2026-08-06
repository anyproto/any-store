package anyenc

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// invertedSkipCases enumerates every value kind that can appear (possibly
// nested) in an index-key field, so the length-only inverted-skip path
// (parseValue with c==nil) is exercised across all of them. These all FAILED
// before the inverted-tag length handling was added to parseValue.
func invertedSkipCases(t *testing.T) []*Value {
	t.Helper()
	// objectID cannot be produced from JSON, so build it via an arena. The arena
	// is not reset, so the returned value stays valid for the test's lifetime.
	a := &Arena{}
	return []*Value{
		a.NewObjectID(NewObjectID()),
		a.NewDateTimeMillis(1754413200123),
		a.NewDateTimeMillis(-62135596800000),
		MustParseJson(`123`),
		MustParseJson(`-456.75`),
		MustParseJson(`0`),
		MustParseJson(`"hello"`),
		MustParseJson(`""`),
		MustParseJson(`true`),
		MustParseJson(`false`),
		MustParseJson(`null`),
		MustParseJson(`[]`),
		MustParseJson(`[1,2,3]`),
		MustParseJson(`[1,"x",true,null]`),
		MustParseJson(`[[1,2],[3]]`),
		MustParseJson(`{"k":1}`),
		MustParseJson(`{"":4}`),
		MustParseJson(`{"a":{"b":2},"c":[1,2]}`),
	}
}

// TestInverted_OffsetAfter_FieldBytes_RoundTrip is the foundation gate (Group
// A1): an inverted field must be skippable by Tuple.OffsetAfter/FieldBytes,
// and FieldBytes must round-trip the inverted bytes exactly.
func TestInverted_OffsetAfter_FieldBytes_RoundTrip(t *testing.T) {
	marker := MustParseJson(`42`) // a trailing field so OffsetAfter(1) is interior
	for _, v := range invertedSkipCases(t) {
		v := v
		t.Run(v.String(), func(t *testing.T) {
			inv := Tuple{}.AppendInverted(v)
			full := append(Tuple{}.AppendInverted(v), Tuple{}.Append(marker)...)

			// OffsetAfter(1) on the single inverted field == its length.
			off, err := inv.OffsetAfter(1)
			require.NoError(t, err)
			assert.Equal(t, len(inv), off, "OffsetAfter(1) must equal the inverted field length")

			// OffsetAfter(1) on (inverted, marker) lands exactly at the marker.
			off2, err := full.OffsetAfter(1)
			require.NoError(t, err)
			assert.Equal(t, len(inv), off2, "OffsetAfter(1) on (inv,marker) must land at marker start")

			// FieldBytes(0) round-trips the inverted bytes.
			fb, err := full.FieldBytes(0)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(fb, inv), "FieldBytes(0) must equal inverted field bytes: got %x want %x", fb, inv)

			// FieldBytes(1) returns the marker bytes.
			fb1, err := full.FieldBytes(1)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(fb1, Tuple{}.Append(marker)), "FieldBytes(1) must equal marker bytes")
		})
	}
}

// TestInverted_OffsetAfter_MixedTuple (Group A2) covers mixed inverted/normal
// fields in both orders, ensuring length accounting stays correct across a tag
// boundary transition.
func TestInverted_OffsetAfter_MixedTuple(t *testing.T) {
	num := MustParseJson(`123`)
	normNum := Tuple{}.Append(num)
	invNum := Tuple{}.AppendInverted(num)

	t.Run("inverted_then_normal", func(t *testing.T) {
		tup := append(append(Tuple{}, invNum...), normNum...)
		off1, err := tup.OffsetAfter(1)
		require.NoError(t, err)
		assert.Equal(t, len(invNum), off1)
		off2, err := tup.OffsetAfter(2)
		require.NoError(t, err)
		assert.Equal(t, len(tup), off2)
	})
	t.Run("normal_then_inverted", func(t *testing.T) {
		tup := append(append(Tuple{}, normNum...), invNum...)
		off1, err := tup.OffsetAfter(1)
		require.NoError(t, err)
		assert.Equal(t, len(normNum), off1)
		off2, err := tup.OffsetAfter(2)
		require.NoError(t, err)
		assert.Equal(t, len(tup), off2)
	})
}

// TestInverted_BinaryLengthSkip (Group A3): an inverted binary field's 4-byte
// header is itself inverted; the skip path must un-invert it to recover the
// payload length. Build a binary value directly (no JSON binary literal).
func TestInverted_BinaryLengthSkip(t *testing.T) {
	a := &Arena{}
	payload := []byte{0x00, 0x01, 0xff, 0x7f, 0x80, 0x00, 0x00}
	bin := a.NewBinary(payload)
	marker := MustParseJson(`7`)

	invBin := Tuple{}.AppendInverted(bin)
	full := append(append(Tuple{}, invBin...), Tuple{}.Append(marker)...)

	// Sanity: the inverted header decodes back to len(payload).
	require.GreaterOrEqual(t, len(invBin), 5)
	var hdr [4]byte
	for i := 0; i < 4; i++ {
		hdr[i] = ^invBin[1+i]
	}
	assert.Equal(t, uint32(len(payload)), binary.BigEndian.Uint32(hdr[:]))

	off, err := invBin.OffsetAfter(1)
	require.NoError(t, err)
	assert.Equal(t, len(invBin), off)

	fb, err := full.FieldBytes(0)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb, invBin))
	fb1, err := full.FieldBytes(1)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(fb1, Tuple{}.Append(marker)))
}

// TestInverted_OrderPreserving (Group A4) verifies the central invariant that
// inversion reverses comparison order for every scalar kind, which is what
// makes a reverse field's inverted storage sort descending in value space.
func TestInverted_OrderPreserving(t *testing.T) {
	pairs := [][2]*Value{
		{MustParseJson(`1`), MustParseJson(`2`)},
		{MustParseJson(`-5`), MustParseJson(`5`)},
		{MustParseJson(`-100.5`), MustParseJson(`-100.25`)},
		{MustParseJson(`"a"`), MustParseJson(`"b"`)},
		{MustParseJson(`"ab"`), MustParseJson(`"abc"`)},
		{MustParseJson(`false`), MustParseJson(`true`)},
		{MustParseJson(`null`), MustParseJson(`1`)},
	}
	for _, p := range pairs {
		ascX := Tuple{}.Append(p[0])
		ascY := Tuple{}.Append(p[1])
		invX := Tuple{}.AppendInverted(p[0])
		invY := Tuple{}.AppendInverted(p[1])
		ascCmp := bytes.Compare(ascX, ascY)
		invCmp := bytes.Compare(invX, invY)
		assert.Equal(t, -ascCmp, invCmp,
			"inversion must reverse order for %s vs %s", p[0].String(), p[1].String())
	}
}
