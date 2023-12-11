package key

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKey_AppendPart(t *testing.T) {
	var k Key
	k = k.AppendPart([]byte("one")).AppendString("two")
	assert.Equal(t, "/one/two", k.String())
}

func TestKey_LastPart(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		var k Key
		k = k.AppendPart([]byte("root")).AppendPart([]byte("two")).AppendPart([]byte(`last`))
		assert.Equal(t, `last`, k.LastPart().String())
	})
	t.Run("slash", func(t *testing.T) {
		var k Key
		k = k.AppendPart([]byte("root")).AppendPart([]byte("two")).AppendPart([]byte(`with/slash`))
		assert.Equal(t, `with/slash`, k.LastPart().String())
	})
	t.Run("null", func(t *testing.T) {
		var k Key
		assert.Equal(t, "", k.LastPart().String())
	})
}

func TestKey_Equal(t *testing.T) {
	var k1, k2 Key

	k1 = k1.AppendPart([]byte("one")).AppendPart([]byte("two"))
	k2 = k2.AppendPart([]byte("one")).AppendPart([]byte("two"))
	assert.True(t, k1.Equal(k2))
	k2 = k2.AppendPart([]byte("3"))
	assert.False(t, k1.Equal(k2))
}

func TestKeyFromString(t *testing.T) {
	ss := []string{
		"/some/string",
		"some/string",
		"some/string/",
	}
	for _, s := range ss {
		k := KeyFromString(s)
		assert.Equal(t, "/some/string", k.String())
		assert.Equal(t, "string", k.LastPart().String())
	}
}

func TestNS_Peek(t *testing.T) {
	ns := NewNS(KeyFromString("my/namespace"))
	k := ns.Peek()
	k = k.AppendPart([]byte("el1"))
	assert.Equal(t, "/my/namespace/el1", k.String())
	k = ns.Peek()
	k = k.AppendPart([]byte("el2"))
	assert.Equal(t, "/my/namespace/el2", k.String())
}
