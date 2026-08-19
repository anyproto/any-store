package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestType_String covers the remaining Type.String cases not hit by
// existing tests: binary, compressed-object-s2, and the default unknown path.
func TestType_String(t *testing.T) {
	assert.Equal(t, "binary", TypeBinary.String())
	assert.Equal(t, "compressedObjectS2", TypeCompressedObjectS2.String())
	assert.Equal(t, "objectID", TypeObjectID.String())
	assert.Contains(t, Type(99).String(), "unknown type")
}
