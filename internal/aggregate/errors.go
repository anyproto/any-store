package aggregate

import "errors"

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
