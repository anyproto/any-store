package anystore

import (
	"net/url"
	"runtime"
)

var defaultSQLiteOptions = map[string]string{
	"_journal_mode": "WAL",
	"_busy_timeout": "5000",
	"_synchronous":  "NORMAL",
	"_cache_size":   "10000000",
	"_foreign_keys": "true",
}

type Config struct {
	Namespace               string
	ReadConnections         int
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

func (c *Config) dsn(path string) string {
	connUrl := url.Values{}
	for k, v := range c.SQLiteConnectionOptions {
		connUrl.Add(k, v)
	}
	return path + "?" + connUrl.Encode()
}
