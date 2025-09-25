package anystore

import (
	"context"
	"runtime"
	"time"

	"github.com/anyproto/any-store/internal/driver"
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

type CheckpointMode string

const (
	CheckpointPassive  CheckpointMode = "PASSIVE"
	CheckpointFull     CheckpointMode = "FULL"
	CheckpointTruncate CheckpointMode = "TRUNCATE"
)

type RecoveryConfig struct {
	// Enabled enables the recovery controller
	Enabled bool

	// IdleAfter is the duration to wait after the last write before performing an idle flush
	// Default: 20s
	IdleAfter time.Duration

	// ForceFlushIdleAfter is the idle threshold used for ForceFlush
	// This is shorter than IdleAfter to ensure quick flush on app suspension
	// Default: 100ms
	ForceFlushIdleAfter time.Duration

	// CheckpointMode specifies the WAL checkpoint mode to use during idle flush
	// Default: CheckpointPassive
	CheckpointMode CheckpointMode

	// Flush is an optional custom flush function
	// If nil, uses default flush (fsync + WAL checkpoint)
	Flush func(ctx context.Context, conn *driver.Conn) (recovery.Stats, error)

	// Trackers are recovery trackers to register with the controller
	Trackers []recovery.Tracker

	// OnIdleSafe are callbacks to invoke after successful idle flush
	OnIdleSafe []recovery.OnIdleSafeCallback

	// QuickCheckTimeout is the timeout for running QuickCheck on dirty database open
	// Default: 5 minutes
	QuickCheckTimeout time.Duration

	// UseSentinel enables the default sentinel file tracker
	// When true, creates a .lock file alongside the database to detect unclean shutdown
	UseSentinel bool
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
		if c.Recovery.ForceFlushIdleAfter <= 0 {
			c.Recovery.ForceFlushIdleAfter = 100 * time.Millisecond
		}
		if c.Recovery.CheckpointMode == "" {
			c.Recovery.CheckpointMode = CheckpointPassive
		}
		if c.Recovery.QuickCheckTimeout <= 0 {
			c.Recovery.QuickCheckTimeout = 5 * time.Minute
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
