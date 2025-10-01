package durability

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/internal/driver"
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
func NewFlushFunc(mode FlushMode) (func(ctx context.Context, conn *driver.Conn) error, error) {
	if mode == "" {
		mode = FlushModeCheckpointPassive
	}

	switch mode {
	case FlushModeFsync:
		return func(ctx context.Context, conn *driver.Conn) error {
			return conn.Fsync(ctx)
		}, nil
	case FlushModeCheckpointPassive:
		return func(ctx context.Context, conn *driver.Conn) error {
			return conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
		}, nil
	case FlushModeCheckpointFull:
		return func(ctx context.Context, conn *driver.Conn) error {
			return conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(FULL)")
		}, nil
	case FlushModeCheckpointRestart:
		return func(ctx context.Context, conn *driver.Conn) error {
			return conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(RESTART)")
		}, nil
	case FlushModeCheckpointTruncate:
		return func(ctx context.Context, conn *driver.Conn) error {
			return conn.ExecNoResult(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
		}, nil
	default:
		return nil, fmt.Errorf("invalid flush mode: %s", mode)
	}
}
