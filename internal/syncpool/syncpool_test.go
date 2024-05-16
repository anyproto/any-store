package syncpool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncPools_GetDocBuf(t *testing.T) {
	sp := NewSyncPool()
	buf := sp.GetDocBuf()
	assert.NotNil(t, buf.Arena)
	assert.NotNil(t, buf.Parser)
	buf.SmallBuf = append(buf.SmallBuf, 1, 2, 3)
	buf.DocBuf = append(buf.DocBuf, 1, 2, 3, 4, 5)
	sp.ReleaseDocBuf(buf)

	buf = sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	assert.NotNil(t, buf.Arena)
	assert.NotNil(t, buf.Parser)
	assert.Len(t, buf.SmallBuf, 3)
	assert.Len(t, buf.DocBuf, 5)
}
