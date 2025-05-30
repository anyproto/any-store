package anystore

import (
	"runtime"
	"time"
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
