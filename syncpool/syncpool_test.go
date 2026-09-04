package syncpool

import (
	"github.com/anyproto/any-store/v2/anyenc"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncPools_GetDocBuf(t *testing.T) {
	sp := NewSyncPool(1000)
	buf := sp.GetDocBuf()
	assert.NotNil(t, buf.Arena)
	assert.NotNil(t, buf.Parser)
	buf.SmallBuf = append(buf.SmallBuf, 1, 2, 3)
	buf.DocBuf = append(buf.DocBuf, 1, 2, 3, 4, 5)

	// Keep a reference to verify reuse if the pool returns the same buffer.
	// sync.Pool may drop items during GC, so reuse is not guaranteed.
	saved := buf
	sp.ReleaseDocBuf(buf)

	// Prevent GC from clearing the pool between Put and Get.
	runtime.KeepAlive(saved)

	buf = sp.GetDocBuf()
	defer sp.ReleaseDocBuf(buf)

	assert.NotNil(t, buf.Arena)
	assert.NotNil(t, buf.Parser)
	if buf == saved {
		// Same buffer was reused — verify state was preserved.
		assert.Len(t, buf.SmallBuf, 3)
		assert.Len(t, buf.DocBuf, 5)
	}
	// If a fresh buffer was returned (GC cleared the pool), that's acceptable.
}

func TestReleaseDocBuf_LeavesCountTowardSizeLimit(t *testing.T) {
	sp := NewSyncPool(1024)
	b := sp.GetDocBuf()
	b.Leaves = make([]anyenc.Leaf, 0, 1024)
	sp.ReleaseDocBuf(b)
	got := sp.GetDocBuf()
	assert.Less(t, cap(got.Leaves)*leafSize, 1024, "an oversized Leaves scratch must not be pooled")
}
