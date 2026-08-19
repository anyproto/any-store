package qplanner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// TestVerifyIter_AllocFree_OnHotCache pins the alloc-free guarantee of the
// VerifyIter → tx.Has path on a hot page cache. With tx.Has skipping the
// cell-value copy and keyBuf preallocated to capacity, draining the iter
// must not allocate per-probe.
//
// The historical implementation used tx.Get, which copied cell.value into
// a fresh slice on every successful match — the source of the +500
// allocs/op regression on Cbo/TwoIdx (see
// any-store-tests/results/7180d3c_vs_v2.0.0-alpha.2/cbo-twoidx-investigation.md).
func TestVerifyIter_AllocFree_OnHotCache(t *testing.T) {
	// Populate the verify namespace with all probed docIds so every probe
	// hits — this is the path that previously allocated in tx.Get.
	_, rtx, verifyNs := openBtreeForVerify(t, "verify_alloc", func(tx *btree.WriteTx, ns *btree.Namespace) {
		for i := 0; i < 50; i++ {
			require.NoError(t, tx.Put(ns, []byte(fmt.Sprintf("doc%02d", i)), []byte{}))
		}
	})

	// Pre-build candidate hits so the AllocsPerRun closure doesn't allocate
	// the slice repeatedly.
	hits := make([]fakeHit, 50)
	for i := 0; i < 50; i++ {
		hits[i] = fakeHit{
			key:   []byte(fmt.Sprintf("k%02d", i)),
			docId: []byte(fmt.Sprintf("doc%02d", i)),
		}
	}

	upstream := &fakeIter{hits: hits}
	it := &VerifyIter{
		Source:   upstream,
		Tx:       rtx,
		VerifyNs: verifyNs,
		Prefix:   nil, // verify key == docId for this test
	}

	// Warm: drain once to grow keyBuf and load pages into pcache.
	for {
		_, docId, _, err := it.Next()
		require.NoError(t, err)
		if docId == nil {
			break
		}
	}

	// Measure: per-iteration allocs across a full drain. Reset the
	// upstream cursor in-place (no allocation) so each run does the
	// same work.
	allocs := testing.AllocsPerRun(20, func() {
		upstream.i = 0
		for {
			_, docId, _, err := it.Next()
			if err != nil || docId == nil {
				return
			}
		}
	})
	assert.Zero(t, allocs,
		"VerifyIter must be alloc-free on hot cache (was tx.Get → cell.value copy per match)")
}
