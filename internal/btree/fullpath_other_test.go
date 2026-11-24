//go:build !unix && !windows

package btree

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// On js/wasm and wasip1 the name is the one given: a relative name is not
// joined to a working directory, and nothing is cleaned.
func TestFullPathname_AsGiven(t *testing.T) {
	for _, path := range []string{"db", "notes.db", "x/../db", "./db", "/db", "/x//db", ""} {
		got, err := FullPathname(path)
		require.NoError(t, err)
		require.Equal(t, path, got)
	}
}
