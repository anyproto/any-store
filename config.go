package anystore

import (
	"runtime"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/durability"
	"github.com/anyproto/any-store/v2/internal/vindex"
)

// InitPageBuffer pre-allocates a global pool of nPages page-sized buffers.
// Must be called before opening any databases that use UseGlobalPageBuffer.
// Mirrors sqlite3_config(SQLITE_CONFIG_PAGECACHE). Call once at process startup.
//
// Example: InitPageBuffer(4096, 5000) pre-allocates ~20MB of page buffers.
func InitPageBuffer(pageSize, nPages int) {
	btree.ConfigPageCache(pageSize, nPages)
}

// SetVectorBuildConcurrency bounds the worker goroutines used to BUILD and
// COMPACT vector (HNSW) indexes — the only place any-store uses internal
// parallelism. It is a single process-wide budget shared by every vector index
// in the process, so many simultaneous builds/compactions cannot oversubscribe
// the CPU. n <= 0 (the default) uses GOMAXPROCS; set it low (e.g. 1–2) on
// constrained or many-database deployments. It does NOT affect search,
// insert/update/delete, or any non-vector operation.
//
// Like InitPageBuffer this is a process-global setting, not per-database: call
// it once at process startup, before opening databases.
func SetVectorBuildConcurrency(n int) {
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}
	vindex.SetBuildConcurrency(n)
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

	// ReadConcurrency caps the number of concurrent read transactions. Reads
	// beyond this many serialize on a semaphore, so on a multi-core host a low cap
	// throttles concurrent Find/$text throughput. Each live reader holds its own
	// page cache (≈ CacheSize × PageSize), created lazily, so peak reader RAM is
	// ReadConcurrency × that — only realized under actual concurrent load. Zero
	// means the default, max(NumCPU-1, 4), which scales with the host while staying
	// modest on small devices.
	ReadConcurrency int

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
	// given byte limit. Zero (default) disables mmap — reads use pread
	// via ReadAt, same behavior as before this feature existed.
	// Matches SQLite's PRAGMA mmap_size. Linux/darwin + amd64/arm64
	// only; no-op on other platforms.
	//
	// SAFETY WARNINGS — read before enabling:
	//
	//   - SIGBUS on unreachable storage: if the DB file becomes
	//     truncated, unlinked, or the storage device is removed while
	//     a mapping is live (USB unplug, iCloud eviction of a
	//     "dataless" file, NFS server timeout, external process
	//     truncate), accessing the mapped bytes generates SIGBUS. Go's
	//     runtime translates SIGBUS to an uncatchable "unexpected
	//     fault address" crash — the whole process dies. The pread
	//     path (MmapSize=0) returns a normal error for all of these
	//     scenarios. Only enable on paths you control: local disk,
	//     stable filesystem, single-device. Do NOT enable for DBs
	//     stored in iCloud Drive, OneDrive, Dropbox, or any network
	//     mount.
	//
	//   - Mobile / iOS: mmap'd bytes count against the per-process
	//     memory limit and can trip iOS's "jetsam" reaper on older
	//     devices. iOS may also silently purge mmap pages under
	//     memory pressure; re-faulting succeeds for local files but
	//     fails (SIGBUS) if the backing file has become unavailable
	//     (permission revocation, iCloud eviction). Recommend leaving
	//     at zero on iOS/iPadOS unless you have a measured need.
	//
	//   - Network filesystems: mmap coherence over NFS/SMB is weaker
	//     than over local disks; concurrent writes from peers may be
	//     invisible to the mapping, and reads may observe torn state.
	//     pread does not have this issue.
	//
	// Recommended when it IS safe: 64-512 MiB on workloads with
	// frequent large-blob or repeat-page reads, on stable local
	// storage, desktop/server deployments.
	MmapSize int64

	// DurabilityConfig provides configuration for crash recovery and idle auto-flush
	Durability DurabilityConfig

	// Encryption, when non-empty, enables page-level AES-256-GCM encryption
	// of the on-disk database file. Zero value means no encryption.
	Encryption EncryptionConfig

	// OnIntegrityError, when non-nil, is invoked from the read path on
	// every per-page integrity failure (XXH3 trailer mismatch in cksum
	// mode, AEAD auth-tag failure in encryption mode). Plain databases
	// never fire it.
	//
	// The callback runs synchronously on the I/O goroutine and must not
	// block. Push to a buffered channel + drain elsewhere if you need
	// retention or cross-thread delivery.
	//
	// Set this at Open time so failures discovered during the first
	// page-1 read (which happens inside Open) are observable. There is
	// no post-Open setter — the codebase favors config-at-Open over
	// runtime mutation.
	OnIntegrityError func(IntegrityError)

	// ContinueOnIntegrityError, when true, lets reads of corrupt pages
	// return their (potentially garbage) bytes instead of erroring with
	// ErrPageIntegrity. The OnIntegrityError callback still fires —
	// only the error-return is suppressed. Mirror of cksumvfs's
	// `PRAGMA checksum_verification = OFF`.
	//
	// Default (false) is the safe choice: corrupt pages cause reads to
	// fail, callers see the error, app halts or recovers. Enable only
	// for forensic dumps where you'd rather read garbage than not be
	// able to read at all.
	//
	// Honored only in checksum mode. AEAD-encrypted databases ignore
	// this flag (disabling AEAD verification would return attacker-
	// controlled plaintext, defeating the cipher).
	ContinueOnIntegrityError bool
}

// EncryptionConfig enables page-level encryption of the database file.
//
// Three ways to enable encryption, in increasing order of control:
//
//  1. Passphrase (common case):
//     cfg.Encryption.Passphrase = []byte("my-pass")
//     Default cipher is AES-256-GCM. Set CipherType to pick a different
//     built-in (ChaCha20-Poly1305 / XChaCha20-Poly1305).
//
//  2. Passphrase + CipherType (pick cipher, keep passphrase KDF):
//     cfg.Encryption.Passphrase = []byte("my-pass")
//     cfg.Encryption.CipherType = anystore.CipherChaCha20Poly1305
//
//  3. Bring-your-own Codec (HSM, external KMS, custom AEAD):
//     cfg.Encryption.Codec = myCodec  // must implement anystore.Codec
//     Passphrase / CipherType / KDFIterations are ignored in this mode.
//
// Passphrase treatment (modes 1 and 2):
//   - Exactly 32 bytes → raw key, no KDF.
//   - Any other length → PBKDF2-HMAC-SHA256 stretches it to 32 bytes
//     against the 16-byte salt stored in the database header.
//
// An empty (length 0) non-nil Passphrase is rejected with an error; use
// nil Passphrase to disable encryption.
//
// Once a database is created with encryption, it must always be opened
// with matching Encryption config. Reopening without the passphrase (or
// with the wrong one) returns an error. There is no in-place rekey in
// v1; changing a passphrase requires exporting to a freshly-opened
// database.
//
// InMemory databases ignore Encryption (there is no at-rest artifact
// to protect).
type EncryptionConfig struct {
	// Passphrase is the user-supplied secret. Nil = no encryption.
	Passphrase []byte

	// KDFIterations overrides the PBKDF2 cost. Zero means 256,000 (the
	// SQLCipher v4 default). Ignored when Passphrase is exactly 32 bytes
	// (raw-key path). Use lower values only in tests.
	KDFIterations int

	// CipherType selects the built-in AEAD when Passphrase is set.
	// Zero value (CipherAES256GCM) is the default.
	CipherType CipherType

	// Codec, when non-nil, bypasses Passphrase / KDFIterations / CipherType
	// and is used verbatim. Use this for HSM-backed or other externally-
	// keyed codecs.
	Codec Codec
}

// Enabled reports whether this config will install a codec on Open.
func (e EncryptionConfig) Enabled() bool {
	return e.Passphrase != nil || e.Codec != nil
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

	// PrimaryKey is the document field whose value is the collection's primary
	// key. Empty defaults to "id". Honored only at CreateCollection; the primary
	// key is immutable once the collection exists.
	PrimaryKey string
}
