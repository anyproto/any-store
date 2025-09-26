package recovery

import (
	"context"
	"fmt"
	"time"

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
func NewFlushFunc(mode FlushMode) (func(ctx context.Context, conn *driver.Conn) (Stats, error), error) {
	switch mode {
	case FlushModeFsync, FlushModeCheckpointPassive, FlushModeCheckpointFull, FlushModeCheckpointRestart:
		// Valid mode
	case "":
		// Default to passive
		mode = FlushModeCheckpointPassive
	default:
		return nil, fmt.Errorf("invalid flush mode: %s", mode)
	}

	return func(ctx context.Context, conn *driver.Conn) (Stats, error) {
		start := time.Now()
		stats := Stats{
			LastFlushTime: start,
		}

		var err error
		switch mode {
		case FlushModeFsync:
			stats.CheckpointMode = "FSYNC_ONLY"
			err = conn.Fsync(ctx)
		case FlushModeCheckpointFull:
			stats.CheckpointMode = "FULL"
			err = conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(FULL)")
		case FlushModeCheckpointRestart:
			stats.CheckpointMode = "RESTART"
			err = conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(RESTART)")
		default: // FlushModeCheckpointPassive
			stats.CheckpointMode = "PASSIVE"
			err = conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
		}

		if err != nil {
			return stats, err
		}

		stats.FlushDuration = time.Since(start)
		stats.Success = true
		return stats, nil
	}, nil
}