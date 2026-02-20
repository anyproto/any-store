package anystore

import (
	"time"

	"github.com/anyproto/any-store/internal/durability"
)

// Config provides the configuration options for the database.
type Config struct {
	// SyncPoolElementMaxSize defines maximum size of buffer that can be returned to the syncpool
	// default value is 2MiB
	SyncPoolElementMaxSize int

	// NoCommitSync skips fsync on WAL commit (like SQLite synchronous=NORMAL in WAL mode).
	// Data is still durable at checkpoint time. Reduces write latency at the cost
	// of losing the last committed transaction(s) on power loss.
	NoCommitSync bool

	// DurabilityConfig provides configuration for crash recovery and idle auto-flush
	Durability DurabilityConfig
}

type FlushMode string

const (
	FlushModeFsync             FlushMode = "FSYNC"              // Only fsync, no checkpoint
	FlushModeCheckpointPassive FlushMode = "CHECKPOINT_PASSIVE" // Checkpoint with PASSIVE mode
	FlushModeCheckpointFull    FlushMode = "CHECKPOINT_FULL"    // Checkpoint with FULL mode
	FlushModeCheckpointRestart FlushMode = "CHECKPOINT_RESTART" // Checkpoint with RESTART mode
)

type DurabilityConfig struct {
	// Enable auto-flush according to IdleAfter and FlushMode
	AutoFlush bool

	// IdleAfter is the duration to wait after the last write before performing autoflush
	// Default: 20s
	IdleAfter time.Duration

	// FlushMode specifies how to autoflush data during idle periods
	// Default: FlushModeCheckpointPassive
	FlushMode FlushMode

	// Sentinel enables the sentinel file (.lock) that tracks database dirty state
	// When true (default is false), the sentinel file is used to detect unclean shutdowns and perform QuickCheck on open
	Sentinel bool
}

func (c *Config) setDefaults() {
	if c.SyncPoolElementMaxSize <= 0 {
		c.SyncPoolElementMaxSize = 2 << 20
	}

	if c.Durability.AutoFlush {
		if c.Durability.IdleAfter <= 0 {
			c.Durability.IdleAfter = 20 * time.Second
		}
		if c.Durability.FlushMode == "" {
			c.Durability.FlushMode = FlushModeCheckpointPassive
		}
	}
}

// toRecoveryFlushMode converts config.FlushMode to recovery.FlushMode
func (m FlushMode) toRecoveryFlushMode() durability.FlushMode {
	return durability.FlushMode(m)
}
