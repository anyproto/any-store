package qplanner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/anyenc"
)

// TestCursorSource_Get_And_AppendSeekKey covers the helper methods on
// CursorSource (iterator.go:42-56) that weren't exercised by direct unit
// tests before.
func TestCursorSource_Get_And_AppendSeekKey(t *testing.T) {
	db, ns := coverageBtree(t, "cs_methods", []string{"x", "y"})
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	cs := &CursorSource{Tx: rtx, Ns: ns}

	t.Run("get_hit", func(t *testing.T) {
		xkey := anyenc.AppendAnyValue(nil, "x")
		val, err := cs.Get(xkey)
		require.NoError(t, err)
		require.NotNil(t, val)
		// Decode the raw value bytes and assert id=="x".
		decoded, perr := anyenc.Parse(val)
		require.NoError(t, perr)
		assert.Equal(t, "x", string(decoded.Get("id").GetStringBytes()),
			"Get must return the {id:x} doc we inserted at key x")
	})
	t.Run("append_seek_key", func(t *testing.T) {
		xkey := anyenc.AppendAnyValue(nil, "x")
		seek, err := cs.AppendSeekKey(xkey, nil)
		require.NoError(t, err)
		// Seek(prefix="x") must land exactly on key "x" — that's the first
		// key >= prefix. Assert the bytes match the exact key.
		assert.Equal(t, xkey, seek,
			"AppendSeekKey(prefix=x) must land exactly on key x")
	})
	t.Run("new_cursor", func(t *testing.T) {
		c := cs.NewCursor()
		require.NotNil(t, c)
		c.Close()
	})
}

// TestIndexInfo_AppendIndexKey covers AppendIndexKey's reverse and forward
// branches (iterator.go:71-77).
func TestIndexInfo_AppendIndexKey(t *testing.T) {
	a := &anyenc.Arena{}
	doc := a.NewObject()
	doc.Set("a", a.NewString("hello"))
	doc.Set("b", a.NewString("world"))

	ii := &IndexInfo{
		FieldNames: []string{"a", "b"},
		FieldPaths: [][]string{{"a"}, {"b"}},
		Reverse:    []bool{false, true},
	}
	var fwd, rev anyenc.Tuple
	fwd = ii.AppendIndexKey(fwd, doc, 0) // forward on field "a"
	rev = ii.AppendIndexKey(rev, doc, 1) // reverse on field "b"

	// Forward emits raw type-byte + bytes; reverse emits each byte bitwise-inverted.
	// For a string value, forward starts with TypeString (3); reverse starts
	// with ^TypeString (252). A regression that routed reverse through Append
	// instead of AppendInverted would fail these strict checks.
	require.NotEmpty(t, fwd)
	require.NotEmpty(t, rev)
	assert.Equal(t, byte(anyenc.TypeString), fwd[0],
		"forward string field must begin with TypeString byte")
	assert.Equal(t, ^byte(anyenc.TypeString), rev[0],
		"reverse string field must begin with bitwise-inverted TypeString byte")
}
