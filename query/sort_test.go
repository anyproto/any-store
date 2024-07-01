package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseSort(t *testing.T) {
	t.Run("asc string", func(t *testing.T) {
		s, err := ParseSort("a.c", "-b")
		require.NoError(t, err)
		require.NotNil(t, s)
		ss := s.(Sorts)
		require.Len(t, ss, 2)
		assert.False(t, ss[0].(*SortField).Reverse)
		assert.Equal(t, []string{"a", "c"}, ss[0].(*SortField).Path)
		assert.True(t, ss[1].(*SortField).Reverse)
		assert.Equal(t, []string{"b"}, ss[1].(*SortField).Path)
	})
	t.Run("custom", func(t *testing.T) {
		s, err := ParseSort(&SortField{Path: []string{"a"}})
		require.NoError(t, err)
		require.NotNil(t, s)
	})
}
