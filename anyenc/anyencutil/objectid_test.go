package anyencutil

import (
	"testing"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEqual_ObjectID(t *testing.T) {
	a := &anyenc.Arena{}
	id := anyenc.NewObjectID()

	assert.True(t, Equal(a.NewObjectID(id), a.NewObjectID(id)))
	assert.False(t, Equal(a.NewObjectID(id), a.NewObjectID(anyenc.NewObjectID())),
		"distinct objectIDs must not compare equal")
}

func TestCopy_ObjectID(t *testing.T) {
	src := &anyenc.Arena{}
	dst := &anyenc.Arena{}
	id := anyenc.NewObjectID()

	cp := Copy(dst, src.NewObjectID(id))
	require.Equal(t, anyenc.TypeObjectID, cp.Type())

	src.Reset() // the copy must be independent of the source arena
	got, err := cp.ObjectID()
	require.NoError(t, err)
	assert.Equal(t, id, got)
}
