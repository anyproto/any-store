package durability

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/internal/btree"
)

// FlushMode represents how to flush data during idle periods
type FlushMode string

const (
	FlushModeFsync              FlushMode = "FSYNC"
	FlushModeCheckpointPassive  FlushMode = "CHECKPOINT_PASSIVE"
	FlushModeCheckpointFull     FlushMode = "CHECKPOINT_FULL"
	FlushModeCheckpointRestart  FlushMode = "CHECKPOINT_RESTART"
	FlushModeCheckpointTruncate FlushMode = "CHECKPOINT_TRUNCATE"
)

// NewFlushFunc creates a flush function based on the given FlushMode.
// Returns an error if the mode is invalid.
func NewFlushFunc(mode FlushMode) (func(ctx context.Context, db *btree.DB) error, error) {
	if mode == "" {
		mode = FlushModeCheckpointPassive
	}

	switch mode {
	case FlushModeFsync,
		FlushModeCheckpointPassive,
		FlushModeCheckpointFull,
		FlushModeCheckpointRestart,
		FlushModeCheckpointTruncate:
		return func(ctx context.Context, db *btree.DB) error {
			return db.Checkpoint()
		}, nil
	default:
		return nil, fmt.Errorf("invalid flush mode: %s", mode)
	}
}
