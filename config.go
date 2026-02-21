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

	// CommitSync forces fsync on every WAL commit (like SQLite synchronous=FULL in WAL mode).
	// When false (default), fsync is deferred to checkpoint time, which reduces write latency
	// at the cost of losing the last committed transaction(s) on power loss.
	CommitSync bool

	// InMemory keeps the entire database in memory with no files on disk.
	// The database does not survive process crashes. When true, InProcess
	// and CommitSync=false are forced on automatically.
	// The path argument to Open is ignored and can be any string (e.g. ":memory:").
	InMemory bool

	// DurabilityConfig provides configuration for crash recovery and idle auto-flush
	Durability DurabilityConfig
}

// FlushMode controls checkpoint behavior during flush, matching SQLite's
// SQLITE_CHECKPOINT_* modes.
type FlushMode string

const (
	// FlushModeCheckpointPassive checkpoints as many WAL frames as possible
	// without waiting for any readers or writers to finish. Might leave the
	// checkpoint unfinished if there are concurrent readers or writers.
	FlushModeCheckpointPassive FlushMode = "CHECKPOINT_PASSIVE"

	// FlushModeCheckpointFull waits until there is no writer and all readers
	// are reading from the most recent snapshot, then checkpoints all frames.
	// Blocks new writers while pending, but new readers continue unimpeded.
	// The WAL is preserved (not reset).
	FlushModeCheckpointFull FlushMode = "CHECKPOINT_FULL"

	// FlushModeCheckpointRestart is like Full but after checkpointing also
	// waits until all readers are reading from the database file only, then
	// resets the WAL so new writes start from the beginning. Blocks new
	// writers while pending, but does not impede readers.
	FlushModeCheckpointRestart FlushMode = "CHECKPOINT_RESTART"

	// FlushModeCheckpointTruncate is like Restart but also truncates the WAL
	// file to zero bytes.
	FlushModeCheckpointTruncate FlushMode = "CHECKPOINT_TRUNCATE"
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
