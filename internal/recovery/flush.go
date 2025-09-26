package recovery

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/internal/driver"
)

// FlushMode represents how to flush data during idle periods
type FlushMode string

const (
	FlushModeFsync             FlushMode = "FSYNC"
	FlushModeCheckpointPassive FlushMode = "CHECKPOINT_PASSIVE"
	FlushModeCheckpointFull    FlushMode = "CHECKPOINT_FULL"
	FlushModeCheckpointRestart FlushMode = "CHECKPOINT_RESTART"
)

// NewFlushFunc creates a flush function based on the given FlushMode.
// Returns an error if the mode is invalid.
func NewFlushFunc(mode FlushMode) (func(ctx context.Context, conn *driver.Conn) error, error) {
	switch mode {
	case FlushModeFsync, FlushModeCheckpointPassive, FlushModeCheckpointFull, FlushModeCheckpointRestart:
		// Valid mode
	case "":
		// Default to passive
		mode = FlushModeCheckpointPassive
	default:
		return nil, fmt.Errorf("invalid flush mode: %s", mode)
	}

	return func(ctx context.Context, conn *driver.Conn) error {
		switch mode {
		case FlushModeFsync:
			return conn.Fsync(ctx)
		case FlushModeCheckpointFull:
			return conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(FULL)")
		case FlushModeCheckpointRestart:
			return conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(RESTART)")
		default: // FlushModeCheckpointPassive
			return conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
		}
	}, nil
}