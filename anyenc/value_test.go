package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fastjson"
)

func TestValue_Getters(t *testing.T) {
	a := &Arena{}
	js := `{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`
	val := MustParseJson(js)
	val.Set("bin", a.NewBinary([]byte{1, 2, 3}))

	t.Run("null", func(t *testing.T) {
		assert.Equal(t, TypeNull, val.Get("object", "null").Type())
		assert.Equal(t, TypeNull, val.Get("object", "non exists").Type())
	})
	t.Run("string", func(t *testing.T) {
		assert.Equal(t, TypeString, val.Get("key").Type())
		assert.Equal(t, "value", val.GetString("key"))
		assert.Equal(t, []byte("value"), val.GetStringBytes("key"))

		assert.Equal(t, "", val.GetString("int"))
		assert.Nil(t, val.GetStringBytes("int"))

		v, err := val.Get("key").StringBytes()
		require.NoError(t, err)
		assert.Equal(t, []byte("value"), v)

		_, err = val.Get("int").StringBytes()
		assert.Error(t, err)
	})
	t.Run("number", func(t *testing.T) {
		assert.Equal(t, TypeNumber, val.Get("array", "1").Type())
		assert.Equal(t, 33.2, val.GetFloat64("object", "float"))
		assert.Equal(t, 42, val.GetInt("int"))

		assert.Equal(t, float64(0), val.GetFloat64("key"))
		assert.Equal(t, 0, val.GetInt("key"))

		v, err := val.Get("int").Int()
		require.NoError(t, err)
		assert.Equal(t, 42, v)
		_, err = val.Get("key").Int()
		assert.Error(t, err)

		fv, err := val.Get("object", "float").Float64()
		require.NoError(t, err)
		assert.Equal(t, 33.2, fv)

		_, err = val.Get("key").Float64()
		assert.Error(t, err)
	})
	t.Run("bool", func(t *testing.T) {
		assert.True(t, val.GetBool("object", "true"))
		assert.False(t, val.GetBool("object", "false"))
		assert.False(t, val.GetBool("key"))
		assert.False(t, val.GetBool("1"))

		v, err := val.Get("object", "true").Bool()
		require.NoError(t, err)
		assert.True(t, v)

		v, err = val.Get("object", "false").Bool()
		require.NoError(t, err)
		assert.False(t, v)

		_, err = val.Get("id").Bool()
		require.Error(t, err)
	})
	t.Run("binary", func(t *testing.T) {
		assert.Equal(t, TypeBinary, val.Get("bin").Type())
		assert.Equal(t, []byte{1, 2, 3}, val.GetBytes("bin"))

		assert.Nil(t, val.GetBytes("int"))

		v, err := val.Get("bin").Bytes()
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, v)

		v, err = val.Get("bin").AppendBytes(nil)
		require.NoError(t, err)
		assert.Equal(t, []byte{1, 2, 3}, v)

		_, err = val.Get("int").Bytes()
		assert.Error(t, err)
	})
	t.Run("array", func(t *testing.T) {
		assert.Equal(t, TypeArray, val.Get("array").Type())
		assert.Len(t, val.GetArray("array"), 3)

		assert.Nil(t, val.GetArray("int"))

		v, err := val.Get("array").Array()
		require.NoError(t, err)
		assert.Len(t, v, 3)

		_, err = val.Get("int").Array()
		assert.Error(t, err)
	})
	t.Run("object", func(t *testing.T) {
		assert.Equal(t, TypeObject, val.Get("object").Type())
		assert.Equal(t, 4, val.GetObject("object").Len())

		assert.Nil(t, val.GetObject("int"))

		v, err := val.Get("object").Object()
		require.NoError(t, err)
		assert.NotNil(t, v)
		_, err = val.Get("int").Object()
		assert.Error(t, err)
	})
}

func BenchmarkValue_MarshalTo(b *testing.B) {
	js := `{"id":12345,"key":"value","int":42,"array":[1,2,3],"object":{"float":33.2,"null":null,"true":true,"false":false}}`
	a := &Arena{}
	fv := fastjson.MustParse(js)
	v := a.NewFromFastJson(fv)
	b.Run("anyenc", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		var buf []byte
		for range b.N {
			buf = v.MarshalTo(buf[:0])
		}
	})
	b.Run("fastjson", func(b *testing.B) {
		b.ResetTimer()
		b.ReportAllocs()
		var buf []byte
		for range b.N {
			buf = fv.MarshalTo(buf[:0])
		}
	})
}

func TestValue_Set(t *testing.T) {
	a := &Arena{}
	val := MustParseJson(`{"a":{"b":[1,2,3],"c":3}}`)

	val.Get("c").Set("44", a.NewFalse())

	val.Set("d", a.NewTrue())
	assert.True(t, val.GetBool("d"))

	val.Set("c", a.NewNumberInt(5))
	assert.Equal(t, 5, val.GetInt("c"))

	valB := val.Get("a", "b")
	valB.Set("0", a.NewNumberInt(-1))
	assert.Equal(t, -1, val.GetInt("a", "b", "0"))

	valB.Set("4", a.NewNumberInt(33))
	assert.Equal(t, `[-1,2,3,null,33]`, val.Get("a", "b").String())
}

func TestValue_Del(t *testing.T) {
	val := MustParseJson(`{"a":{"b":[1,2,3],"c":3}}`)
	val.Get("a", "b").Del("1")
	val.Get("a").Del("c")
	assert.Equal(t, `{"a":{"b":[1,3]}}`, val.String())
}

// EOS bytes inside strings and keys are escaped on marshal and restored on
// parse — values round-trip unchanged (historically they were silently
// stripped, which mutated data).
func TestValue_StringsWithEOS(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	key := "a" + string(EOS) + "b"
	value := "c" + string(EOS) + "d"
	obj.Set(key, a.NewString(value))
	data := obj.MarshalTo(nil)
	dec, err := Parse(data)
	require.NoError(t, err)
	require.Equal(t, value, dec.GetString(key))
	o, err := dec.Object()
	require.NoError(t, err)
	require.Equal(t, 1, o.Len())
	o.Visit(func(k []byte, _ *Value) {
		assert.Equal(t, key, string(k))
	})
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
