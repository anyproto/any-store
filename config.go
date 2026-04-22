package anystore

import (
	"time"

	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/durability"
)

// InitPageBuffer pre-allocates a global pool of nPages page-sized buffers.
// Must be called before opening any databases that use UseGlobalPageBuffer.
// Mirrors sqlite3_config(SQLITE_CONFIG_PAGECACHE). Call once at process startup.
//
// Example: InitPageBuffer(4096, 5000) pre-allocates ~20MB of page buffers.
func InitPageBuffer(pageSize, nPages int) {
	btree.ConfigPageCache(pageSize, nPages)
}

// Config provides the configuration options for the database.
type Config struct {
	// SyncPoolElementMaxSize defines maximum size of buffer that can be returned to the syncpool
	// default value is 2MiB
	SyncPoolElementMaxSize int

	// CommitSync forces fsync on every WAL commit (like SQLite synchronous=FULL in WAL mode).
	// When false (default), fsync is deferred to checkpoint time, which reduces write latency
	// at the cost of losing the last committed transaction(s) on power loss.
	CommitSync bool

	// DisableAutoCheckpoint disables WAL auto-checkpoint entirely.
	// When true, checkpoint must be triggered manually or via durability auto-flush.
	DisableAutoCheckpoint bool

	// AutoCheckpointAfter overrides the default WAL auto-checkpoint threshold (10000 frames).
	// Only used when DisableAutoCheckpoint is false. 0 means use default.
	AutoCheckpointAfter int

	// CacheSize overrides the default per-DB page cache size (in pages,
	// default 5000). Primarily for tests that need to force pagerStress /
	// cache-spill behavior at low data volumes. Zero means use the default.
	CacheSize int

	// InMemory keeps the entire database in memory with no files on disk.
	// The database does not survive process crashes. When true, InProcess
	// and CommitSync=false are forced on automatically.
	// The path argument to Open is ignored and can be any string (e.g. ":memory:").
	InMemory bool

	// DisableCompression disables S2 compression for document values.
	// By default, objects larger than 256 bytes are compressed with S2.
	DisableCompression bool

	// UseGlobalPageBuffer opts this DB into the global pre-allocated page
	// buffer pool. The pool must be initialized beforehand via InitPageBuffer.
	// When false (default), page buffers use sync.Pool (GC-managed, like
	// SQLite's default malloc mode).
	UseGlobalPageBuffer bool

	// MmapSize enables mmap-backed reads of the database file up to the
	// given byte limit. Zero disables mmap (reads use pread via ReadAt).
	// Matches SQLite's PRAGMA mmap_size. Recommended 64-512 MiB for
	// large-blob read workloads. Linux/darwin + amd64/arm64 only; no-op
	// on other platforms.
	MmapSize int64

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

// Compression specifies the compression algorithm for document values.
type Compression int

const (
	// S2 enables S2 compression for objects larger than 256 bytes (default).
	S2 Compression = 1
	// NoCompression disables compression entirely.
	NoCompression Compression = 2
)

// CollectionOptions configures per-collection settings at creation time.
type CollectionOptions struct {
	// Compression overrides the database-wide compression setting for this collection.
	// Zero value inherits the database default.
	Compression Compression
}
