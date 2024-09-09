package anystore

import (
	"runtime"
)

var defaultSQLiteOptions = map[string]string{
	"cache_size":           "100000",
	"temp_store_directory": "'/data/data/io.anytype.app.debug/files'",
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
