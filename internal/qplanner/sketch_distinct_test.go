package qplanner

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIndexSketch_DistinctEstimate covers the distinct-value estimator that powers
// the per-column indexed-equality selectivity fallback: empty -> 1, low fill ~=
// distinct count, saturated/high fill capped at docCount (never diverges).
func TestIndexSketch_DistinctEstimate(t *testing.T) {
	t.Run("empty sketch returns 1", func(t *testing.T) {
		s := NewIndexSketch(DefaultSketchSize)
		assert.Equal(t, float64(1), s.DistinctEstimate())
	})

	t.Run("docCount but no buckets returns 1", func(t *testing.T) {
		s := NewIndexSketch(DefaultSketchSize)
		s.IncrementDocCount()
		assert.Equal(t, float64(1), s.DistinctEstimate())
	})

	t.Run("low fill approximates distinct count", func(t *testing.T) {
		s := NewIndexSketch(1024)
		const n = 50
		for i := 0; i < n; i++ {
			s.Increment([]byte(fmt.Sprintf("val-%d", i)))
			s.IncrementDocCount()
		}
		// 50 distinct values over 1024 buckets: collisions are rare and the
		// occupancy estimator should land close to 50.
		assert.InDelta(t, float64(n), s.DistinctEstimate(), float64(n)*0.25)
	})

	t.Run("saturated sketch capped at docCount, never diverges", func(t *testing.T) {
		s := NewIndexSketch(64) // small so a modest corpus saturates every bucket
		const docs = 100000
		for i := 0; i < docs; i++ {
			s.Increment([]byte(fmt.Sprintf("v%d", i)))
			s.IncrementDocCount()
		}
		// nonEmpty == Size -> the ln term would diverge; must return docCount.
		assert.Equal(t, float64(docs), s.DistinctEstimate())
	})

	t.Run("high fill corrected and capped at docCount", func(t *testing.T) {
		s := NewIndexSketch(1024)
		for i := 0; i < 500; i++ {
			s.Increment([]byte(fmt.Sprintf("k%d", i)))
		}
		// Fewer docs than the occupancy estimate -> the cap binds.
		s.docCount.Store(300)
		assert.LessOrEqual(t, s.DistinctEstimate(), float64(300))
	})
}
