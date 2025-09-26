package anystore

import (
	"runtime"
	"time"

	"github.com/anyproto/any-store/internal/recovery"
)

var defaultSQLiteOptions = map[string]string{
	"cache_size": "-2000", // negative value for kilobytes instead of pages
}

// Config provides the configuration options for the database.
type Config struct {
	// Namespace is a prefix for all created tables and indexes by any-store,
	// helping to isolate tables and indexes within the same database file.
	Namespace string

	// ReadConnections specifies the number of read connections to the database,
	// optimizing read operations by allowing multiple concurrent read connections.
	ReadConnections int

	// SQLiteConnectionOptions provides additional options for SQLite connections,
	// corresponding to SQLite pragmas or other connection settings.
	SQLiteConnectionOptions map[string]string

	// SQLiteGlobalPageCachePreallocateSizeBytes is the size of the global page cache to preallocate.
	// Initialised on the first call to NewConnManager and shared by all connections.
	// default value is 10M
	// set negative to disable preallocation
	SQLiteGlobalPageCachePreallocateSizeBytes int

	// SyncPoolElementMaxSize defines maximum size of buffer that can be returned to the syncpool
	// default value id 2MiB
	SyncPoolElementMaxSize int

	// StalledConnectionsDetectorEnabled enables the collection of stack traces and duration of acquired connections
	// You can then use StalledConnections method of the ConnManager
	StalledConnectionsDetectorEnabled bool
	// StalledConnectionsPanicOnClose enables panic on Close in case of any connection is not released after this timeout
	StalledConnectionsPanicOnClose time.Duration

	// RecoveryConfig provides configuration for crash recovery and idle durability
	Recovery RecoveryConfig
}

type FlushMode string

const (
	FlushModeFsync             FlushMode = "FSYNC"              // Only fsync, no checkpoint
	FlushModeCheckpointPassive FlushMode = "CHECKPOINT_PASSIVE" // Checkpoint with PASSIVE mode + fsync
	FlushModeCheckpointFull    FlushMode = "CHECKPOINT_FULL"    // Checkpoint with FULL mode + fsync
	FlushModeCheckpointRestart FlushMode = "CHECKPOINT_RESTART" // Checkpoint with RESTART mode + fsync
)

type RecoveryConfig struct {
	// Enabled enables the recovery controller
	Enabled bool

	// IdleAfter is the duration to wait after the last write before performing an idle flush
	// Default: 20s
	IdleAfter time.Duration

	// FlushMode specifies how to flush data during idle periods
	// Default: FlushModeCheckpointPassive
	FlushMode FlushMode

	// DisableSentinel disables the sentinel file (.lock) that tracks database dirty state
	// When false (default), the sentinel file is used to detect unclean shutdowns and run QuickCheck
	DisableSentinel bool
}

func (c *Config) setDefaults() {
	if c.ReadConnections <= 0 {
		c.ReadConnections = runtime.NumCPU()
	}
	if c.SQLiteConnectionOptions == nil {
		c.SQLiteConnectionOptions = defaultSQLiteOptions
	}
	for k, v := range defaultSQLiteOptions {
		c.SQLiteConnectionOptions[k] = v
	}
	if c.SyncPoolElementMaxSize <= 0 {
		c.SyncPoolElementMaxSize = 2 << 20
	}

	if c.SQLiteGlobalPageCachePreallocateSizeBytes == 0 {
		c.SQLiteGlobalPageCachePreallocateSizeBytes = 10 << 20
	}

	if c.Recovery.Enabled {
		if c.Recovery.IdleAfter <= 0 {
			c.Recovery.IdleAfter = 20 * time.Second
		}
		if c.Recovery.FlushMode == "" {
			c.Recovery.FlushMode = FlushModeCheckpointPassive
		}
	}
}

func (c *Config) pragma() map[string]string {
	pragma := make(map[string]string)
	for k, v := range defaultSQLiteOptions {
		pragma[k] = v
	}
	if c.SQLiteConnectionOptions != nil {
		for k, v := range c.SQLiteConnectionOptions {
			pragma[k] = v
		}
	}
	return pragma
}

// toRecoveryFlushMode converts config.FlushMode to recovery.FlushMode
func (m FlushMode) toRecoveryFlushMode() recovery.FlushMode {
	return recovery.FlushMode(m)
}
