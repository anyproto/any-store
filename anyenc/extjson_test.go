package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testHex = "0123456789abcdef01234567"

func TestExtJSON_ObjectID_RoundTrip(t *testing.T) {
	id := mustOID(t, testHex)

	// decode a wrapper into a typed value
	v, err := ParseJson(`{"id":{"$oid":"` + testHex + `"}}`)
	require.NoError(t, err)
	assert.Equal(t, TypeObjectID, v.Get("id").Type())
	assert.Equal(t, id, v.GetObjectID("id"))

	// render back to the same wrapper, and re-parse to an equal value
	assert.Equal(t, `{"id":{"$oid":"`+testHex+`"}}`, v.String())
	v2, err := ParseJson(v.String())
	require.NoError(t, err)
	assert.Equal(t, id, v2.GetObjectID("id"))

	// top-level wrapper decodes to a bare objectID
	top, err := ParseJson(`{"$oid":"` + testHex + `"}`)
	require.NoError(t, err)
	assert.Equal(t, TypeObjectID, top.Type())
}

func TestExtJSON_Binary_RoundTrip(t *testing.T) {
	v, err := ParseJson(`{"b":{"$binary":"AQID"}}`) // base64 of {1,2,3}
	require.NoError(t, err)
	assert.Equal(t, TypeBinary, v.Get("b").Type())
	assert.Equal(t, []byte{1, 2, 3}, v.GetBytes("b"))

	assert.Equal(t, `{"b":{"$binary":"AQID"}}`, v.String())
	v2, err := ParseJson(v.String())
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, v2.GetBytes("b"))
}

func TestExtJSON_Vector_RoundTrip(t *testing.T) {
	v, err := ParseJson(`{"e":{"$vector":[1,2,3]}}`)
	require.NoError(t, err)
	assert.Equal(t, TypeVectorF32, v.Get("e").Type())
	assert.Equal(t, []float32{1, 2, 3}, v.GetVectorF32("e"))

	assert.Equal(t, `{"e":{"$vector":[1,2,3]}}`, v.String())
	v2, err := ParseJson(v.String())
	require.NoError(t, err)
	assert.Equal(t, []float32{1, 2, 3}, v2.GetVectorF32("e"))
}

// Malformed or ambiguous wrappers must stay ordinary objects, never error.
func TestExtJSON_LenientFallthrough(t *testing.T) {
	cases := []string{
		`{"$oid":"nothex"}`,                // invalid hex
		`{"$oid":123}`,                     // not a string
		`{"$binary":"not base64 !!!"}`,     // invalid base64
		`{"$vector":["a","b"]}`,            // not numbers
		`{"$vector":"x"}`,                  // not an array
		`{"$oid":"` + testHex + `","x":1}`, // more than one key
		`{"$unknown":"x"}`,                 // unknown tag
	}
	for _, c := range cases {
		v, err := ParseJson(c)
		require.NoError(t, err, c)
		assert.Equal(t, TypeObject, v.Type(), c)
	}
}

// A stored objectID inside a nested/array structure must also round-trip.
func TestExtJSON_Nested(t *testing.T) {
	a := &Arena{}
	id := NewObjectID()
	doc := a.NewObject()
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewObjectID(id))
	arr.SetArrayItem(1, a.NewBinary([]byte{9, 9}))
	doc.Set("items", arr)

	v, err := ParseJson(doc.String())
	require.NoError(t, err)
	got := v.GetArray("items")
	require.Len(t, got, 2)
	assert.Equal(t, TypeObjectID, got[0].Type())
	oid, _ := got[0].ObjectID()
	assert.Equal(t, id, oid)
	assert.Equal(t, TypeBinary, got[1].Type())
}
