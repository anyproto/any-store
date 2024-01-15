package query

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/internal/key"
)

func newBoundKey(v any) (k key.Key) {
	return k.AppendAny(v)
}

type boundTestCase struct {
	unmerged Bounds
	expected string
}

var boundTestData = []boundTestCase{
	{
		unmerged: Bounds{
			{Start: nil, End: newBoundKey(10)},
			{Start: newBoundKey(5), End: newBoundKey(15)},
		},
		expected: `Bounds{[-inf,'15')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(2)},
			{Start: newBoundKey(2), End: newBoundKey(3)},
		},
		expected: `Bounds{('1','2'),('2','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(2), End: newBoundKey(3)},
			{Start: newBoundKey(1), End: newBoundKey(2)},
		},
		expected: `Bounds{('1','2'),('2','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(2), EndInclude: true},
			{Start: newBoundKey(2), End: newBoundKey(3)},
		},
		expected: `Bounds{('1','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(2), End: newBoundKey(3), StartInclude: true},
			{Start: newBoundKey(1), End: newBoundKey(2)},
		},
		expected: `Bounds{('1','3')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10)},
			{Start: newBoundKey(1), End: newBoundKey(2)},
		},
		expected: `Bounds{('1','10')}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10)},
			{Start: newBoundKey(8), End: nil},
		},
		expected: `Bounds{('1',inf]}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10), StartInclude: true},
			{Start: newBoundKey(8), End: nil},
		},
		expected: `Bounds{['1',inf]}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(10), StartInclude: true},
			{Start: newBoundKey(8), End: newBoundKey(11), EndInclude: true},
		},
		expected: `Bounds{['1','11']}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: newBoundKey(2), StartInclude: true},
			{Start: newBoundKey(4), End: newBoundKey(5), EndInclude: true},
		},
		expected: `Bounds{['1','2'),('4','5']}`,
	},
	{
		unmerged: Bounds{
			{Start: newBoundKey(1), End: nil},
			{Start: nil, End: newBoundKey(2)},
		},
		expected: `Bounds{[-inf,inf]}`,
	},
}

func TestBounds_Append(t *testing.T) {
	for _, tc := range boundTestData {
		var bs Bounds
		for _, b := range tc.unmerged {
			bs = bs.Append(b)
		}
		assert.Equal(t, tc.expected, bs.String())
	}
}
