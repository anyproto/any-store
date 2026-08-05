package aggregate

import (
	"errors"

	"github.com/anyproto/any-store/v2/query"
)

// atPath prefixes seg onto a *query.ParseError's Path as the pipeline
// parser's recursive descent unwinds — same contract as the query package's
// unexported twin: root-to-leaf path, in-place mutation safe because every
// ParseError is freshly allocated at its failure site, other error types pass
// through unchanged.
func atPath(err error, seg string) error {
	var pe *query.ParseError
	if errors.As(err, &pe) {
		if pe.Path == "" {
			pe.Path = seg
		} else {
			pe.Path = seg + "." + pe.Path
		}
	}
	return err
}

// withSource stamps Source "pipeline" onto the *ParseError at the
// ParsePipeline boundary — including errors that originated in the filter
// grammar under $match, whose Path is pipeline-relative by then.
func withSource(err error, source string) error {
	var pe *query.ParseError
	if errors.As(err, &pe) {
		pe.Source = source
	}
	return err
}

var (
	// ErrGroupLimitExceeded is returned when $group exceeds the configured
	// maximum number of unique group keys.
	ErrGroupLimitExceeded = errors.New("any-store/aggregate: group limit exceeded")

	// ErrAccumArrayLimitExceeded is returned when a $push or $addToSet
	// accumulator exceeds the configured maximum array length.
	ErrAccumArrayLimitExceeded = errors.New("any-store/aggregate: accumulator array limit exceeded")

	// ErrMemoryLimitExceeded is returned when the blocking stages of a
	// pipeline retain more than the configured memory budget.
	ErrMemoryLimitExceeded = errors.New("any-store/aggregate: memory limit exceeded")
)
