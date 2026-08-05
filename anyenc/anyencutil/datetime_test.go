package anyencutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestEqual_DateTime(t *testing.T) {
	a := &anyenc.Arena{}
	assert.True(t, Equal(a.NewDateTimeMillis(123), a.NewDateTimeMillis(123)))
	assert.False(t, Equal(a.NewDateTimeMillis(123), a.NewDateTimeMillis(124)))
	assert.False(t, Equal(a.NewDateTimeMillis(123), a.NewNumberInt(123)),
		"dateTime must not compare equal to a number")
}

func TestCopy_DateTime(t *testing.T) {
	src := &anyenc.Arena{}
	dst := &anyenc.Arena{}

	cp := Copy(dst, src.NewDateTimeMillis(1754413200123))
	require.Equal(t, anyenc.TypeDateTime, cp.Type())
	src.Reset()

	ms, err := cp.DateTimeMillis()
	require.NoError(t, err)
	assert.Equal(t, int64(1754413200123), ms)
}
