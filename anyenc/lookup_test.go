package anyenc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawByPath(t *testing.T) {
	var a Arena
	var p Parser

	doc := MustParseJson(`{"id": 7, "name": "x", "nested": {"v": [1.5, -2, 3], "s": "deep"}, "tail": true}`)
	enc := doc.MarshalTo(nil)

	t.Run("top-level", func(t *testing.T) {
		raw, err := p.RawByPath(enc, "name")
		require.NoError(t, err)
		v, _, err := parseValue(raw, &p.c, 0)
		require.NoError(t, err)
		assert.Equal(t, "x", string(v.GetStringBytes()))
	})

	t.Run("nested array", func(t *testing.T) {
		raw, err := p.RawByPath(enc, "nested", "v")
		require.NoError(t, err)
		fs, ok := AppendFloat32s(raw, nil)
		require.True(t, ok)
		assert.Equal(t, []float32{1.5, -2, 3}, fs)
	})

	t.Run("absent key and non-object path", func(t *testing.T) {
		for _, path := range [][]string{{"nope"}, {"name", "deeper"}, {"nested", "nope"}} {
			raw, err := p.RawByPath(enc, path...)
			require.NoError(t, err)
			assert.Nil(t, raw, "path %v", path)
		}
	})

	t.Run("compressed document", func(t *testing.T) {
		big := a.NewObject()
		for i := range 50 {
			big.Set(fmt.Sprintf("pad%d", i), a.NewString("padding padding padding padding"))
		}
		big.Set("v", MustParseJson(`[0.25, 4]`))
		comp, _ := big.MarshalCompressed(nil, nil)
		require.Equal(t, TypeCompressedObjectS2, Type(comp[0]))
		raw, err := p.RawByPath(comp, "v")
		require.NoError(t, err)
		fs, ok := AppendFloat32s(raw, nil)
		require.True(t, ok)
		assert.Equal(t, []float32{0.25, 4}, fs)
	})

	t.Run("packed vector field", func(t *testing.T) {
		o := a.NewObject()
		o.Set("emb", a.NewVectorF32([]float32{9, -8.5, 7}))
		o.Set("z", a.NewNumberInt(1))
		enc := o.MarshalTo(nil)
		raw, err := p.RawByPath(enc, "emb")
		require.NoError(t, err)
		fs, ok := AppendFloat32s(raw, nil)
		require.True(t, ok)
		assert.Equal(t, []float32{9, -8.5, 7}, fs)
	})

	t.Run("not a vector", func(t *testing.T) {
		raw, err := p.RawByPath(enc, "tail")
		require.NoError(t, err)
		_, ok := AppendFloat32s(raw, nil)
		assert.False(t, ok)
	})
}

// TestValidateRaw pins the raw-filter reject path's error surface: a scan that
// skips the full parse must still fail on corrupt bytes anywhere in the
// document, exactly like ParseOwned would.
func TestValidateRaw(t *testing.T) {
	v := MustParseJson(`{"a":50,"b":{"c":[1,2,3]},"s":"str"}`)
	raw := v.MarshalTo(nil)
	if err := ValidateRaw(raw); err != nil {
		t.Fatalf("valid doc: %v", err)
	}
	// Truncated mid-document: field "a" (first) is still readable, the rest is not.
	if err := ValidateRaw(raw[:len(raw)-3]); err == nil {
		t.Fatal("truncated doc: expected error")
	}
	// Trailing junk after the root value.
	if err := ValidateRaw(append(append([]byte{}, raw...), 0x01)); err == nil {
		t.Fatal("trailing junk: expected error")
	}
	// Unknown type tag on the last field's value ("s": key bytes + EOS, then tag).
	bad := append([]byte{}, raw...)
	i := bytes.Index(bad, []byte{'s', EOS})
	if i < 0 {
		t.Fatal("key not found in encoding")
	}
	bad[i+2] = 0xEE
	if err := ValidateRaw(bad); err == nil {
		t.Fatal("corrupt tag: expected error")
	}
}
