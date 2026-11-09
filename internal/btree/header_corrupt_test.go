package btree

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErrHeaderCorruptIsCorrupt(t *testing.T) {
	// a caller matching the broad ErrCorrupt still sees a header failure
	assert.True(t, errors.Is(ErrHeaderCorrupt, ErrCorrupt))
	assert.False(t, errors.Is(ErrCorrupt, ErrHeaderCorrupt))
	assert.False(t, errors.Is(ErrSQLiteFormat, ErrHeaderCorrupt), "a v1 file is intact, not corrupt")
}

func TestDeserializeBadHeaderIsHeaderCorrupt(t *testing.T) {
	var h dbHeader
	assert.ErrorIs(t, h.deserialize(make([]byte, 10)), ErrHeaderCorrupt)
	assert.ErrorIs(t, h.deserialize(make([]byte, dbHeaderSize)), ErrHeaderCorrupt)
}
