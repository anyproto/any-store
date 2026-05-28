package anystore

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/anyproto/any-store/v2/internal/qplanner"
)

// TestSetKWayMergeMax_TopLevelWrapperDelegates confirms the public
// wrapper at the anystore package level actually drives the
// internal/qplanner setting. Without this test, a downstream regression
// (wrapper deleted or pointing at the wrong package) would only surface
// once a customer hit a production issue with no kill switch available.
func TestSetKWayMergeMax_TopLevelWrapperDelegates(t *testing.T) {
	// Save and restore at both layers so a misaligned wrapper doesn't
	// corrupt subsequent test runs.
	origInner := qplanner.SetKWayMergeMax(64) // capture & restore baseline
	qplanner.SetKWayMergeMax(origInner)
	t.Cleanup(func() { qplanner.SetKWayMergeMax(origInner) })

	prev := SetKWayMergeMax(7)
	assert.Equal(t, origInner, prev, "wrapper must return previous value")
	// Confirm the inner package sees the change.
	got := qplanner.SetKWayMergeMax(prev) // also restores
	assert.Equal(t, 7, got, "qplanner must see the value the wrapper set")
}

func TestSetKWayMergeMinEntries_TopLevelWrapperDelegates(t *testing.T) {
	orig := qplanner.SetKWayMergeMinEntries(200)
	qplanner.SetKWayMergeMinEntries(orig)
	t.Cleanup(func() { qplanner.SetKWayMergeMinEntries(orig) })

	prev := SetKWayMergeMinEntries(9)
	assert.Equal(t, orig, prev)
	got := qplanner.SetKWayMergeMinEntries(prev)
	assert.Equal(t, 9, got)
}
