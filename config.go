package anystore

import (
	"fmt"
	"math"
	"runtime"
	"time"

	"github.com/anyproto/any-store/v2/internal/btree"
	"github.com/anyproto/any-store/v2/internal/buildpool"
	"github.com/anyproto/any-store/v2/internal/durability"
)

// InitPageBuffer pre-allocates a global pool of nPages page-sized buffers.
// Must be called before opening any databases that use UseGlobalPageBuffer.
// Mirrors sqlite3_config(SQLITE_CONFIG_PAGECACHE). Call once at process
// startup: the first call configures the pool for the life of the process and
// a later call, with the same or other arguments, does nothing.
//
// pageSize must be the database page size, 4096. It does not select a page
// size. A pool of another size from 512 to 65536 is allocated as asked and
// never used: a database with UseGlobalPageBuffer takes buffers from the pool
// only when they are exactly its page size, and otherwise opens and runs on
// the default allocator, without the pool's memory bound. (SQLite's page cache
// uses any slot large enough for a page and its header; the buffers of this
// pool are the pages themselves.) A pageSize below 512 or an nPages below 1
// allocates nothing and leaves every database on the default allocator.
// SQLite clears such a configuration too (sqlite3MallocInit) and allocates
// pages from its default allocator, though when each of its caches is
// separate (any build without SQLITE_ENABLE_MEMORY_MANAGEMENT) each still
// sizes its first bulk allocation from nPages; here every cache keeps its
// default one.
// A pageSize above 65536, the largest
// page size, or an nPages*pageSize too large for an int also allocates
// nothing: these two bounds are this package's own, since SQLite is handed its
// buffer and allocates none.
//
// The pool is a single allocation of nPages*pageSize bytes, made by the first
// call, so nPages must fit the memory the process can spare.
//
// Example: InitPageBuffer(4096, 5000) pre-allocates ~20MB of page buffers.
func InitPageBuffer(pageSize, nPages int) {
	btree.ConfigPageCache(pageSize, nPages)
}

// SetVectorBuildConcurrency caps how many vector index build tasks execute at
// once across the process — the only place any-store uses internal
// parallelism: the HNSW graph construction of a build or compaction, and the
// IVF k-means training and bulk placement that creating, compacting or
// automatically rebuilding an IVF index runs. It is a single process-wide
// budget shared by every vector index, so many simultaneous builds and
// compactions cannot run more build tasks at once than it allows. The serial
// work between those tasks (btree writes, sorts, centroid updates, sampling)
// runs on each build's own goroutine outside the cap. n <= 0 (the default)
// uses GOMAXPROCS; set it low (e.g. 1–2) on constrained or many-database
// deployments. An IVF index comes out the same whatever the budget; only its
// build time changes.
//
// It does not affect search or any non-vector operation, nor a vector write
// itself; but a self-contained write that takes an index past its
// CompactRatio compacts (or, for IVF, rebuilds) that index before it returns,
// and that work runs under the budget.
//
// A build or compaction runs inside its database's write transaction and
// waits for budget tokens there, with no timeout. The budget is shared across
// databases, so while builds in other databases hold every token, a build
// here waits and this database's other writers wait behind its write
// transaction.
//
// Like InitPageBuffer this is a process-global setting, not per-database: call
// it once at process startup, before opening databases. A build takes the
// budget per phase — an IVF build for each k-means pass and placement step, an
// HNSW build for its construction and again for its repair pass — so a call
// while a build runs applies from that build's next phase on.
func SetVectorBuildConcurrency(n int) {
	if n <= 0 {
		n = runtime.GOMAXPROCS(0)
	}
	buildpool.Set(n)
}

// Config provides the configuration options for the database.
type Config struct {
	// SyncPoolElementMaxSize defines maximum size of buffer that can be returned to the syncpool
	// default value is 2MiB
	SyncPoolElementMaxSize int

	// AutoCheckpointAfter overrides the default WAL auto-checkpoint threshold (10000 frames).
	// Only used when DisableAutoCheckpoint is false. 0 means use default.
	AutoCheckpointAfter int

	// CacheSize overrides the default per-DB page cache size (in pages,
	// default 5000). Primarily for tests that need to force pagerStress /
	// cache-spill behavior at low data volumes. Zero or a negative value
	// means the default: a negative value is not a size in KiB, as it is for
	// SQLite's cache_size. A value above 2,147,418,112 pages (on a 32-bit
	// target, above 524,288) is taken as that bound, as SQLite clamps its
	// cache_size.
	CacheSize int

	// ReadConcurrency caps the number of concurrent read transactions. Reads
	// beyond this many serialize on a semaphore, so on a multi-core host a low cap
	// throttles concurrent Find/$text throughput. Each live reader holds its own
	// page cache (≈ CacheSize × PageSize), created lazily, so peak reader RAM is
	// ReadConcurrency × that — only realized under actual concurrent load. Zero
	// or a negative value means the default, max(NumCPU-1, 4), which scales with
	// the host while staying modest on small devices. A value above 65,536 is
	// taken as 65,536.
	ReadConcurrency int

	// CommitSync forces fsync on every WAL commit (like SQLite synchronous=FULL in WAL mode).
	// When false (default), fsync is deferred to checkpoint time, which reduces write latency
	// at the cost of losing the last committed transaction(s) on power loss.
	CommitSync bool

	// DisableAutoCheckpoint disables WAL auto-checkpoint entirely.
	// When true, checkpoint must be triggered manually or via durability auto-flush.
	DisableAutoCheckpoint bool

	// InMemory keeps the entire database in memory with no files on disk.
	// The database does not survive process crashes. When true, InProcess
	// and CommitSync=false are forced on automatically.
	// The path argument to Open is ignored and can be any string (e.g. ":memory:").
	// Without InMemory the path is an ordinary file path: ":memory:" has no
	// special meaning, unlike in SQLite, and the filesystem's naming rules
	// apply to it (Windows refuses ':' in a file name).
	InMemory bool

	// DisableCompression disables S2 compression for document values.
	// By default, objects larger than 256 bytes are compressed with S2.
	DisableCompression bool

	// UseGlobalPageBuffer opts this DB into the global pre-allocated page
	// buffer pool. The pool must be initialized beforehand via InitPageBuffer.
	// When false (default), page buffers use sync.Pool (GC-managed, like
	// SQLite's default malloc mode).
	UseGlobalPageBuffer bool

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

	// Encryption configures page-level encryption of the database file. A
	// Passphrase or a raw 32-byte Key enables it; CipherType only selects
	// the cipher, AES-256-GCM by default, and without a Passphrase or a Key
	// the file is plain whatever CipherType says. The zero value means no
	// encryption.
	Encryption EncryptionConfig

	// OnIntegrityError, when non-nil, is invoked from the read path on
	// every per-page integrity failure (XXH3 trailer mismatch in cksum
	// mode, AEAD auth-tag failure in encryption mode). Plain databases
	// never fire it. It receives the failures of the database it was
	// opened with and no other. The destination of a Backup is another
	// database: its failures come back as Backup's error.
	//
	// The callback runs synchronously on the I/O goroutine and must not
	// block. Push to a buffered channel + drain elsewhere if you need
	// retention or cross-thread delivery.
	//
	// Set this at Open time so failures discovered during the first
	// page-1 read (which happens inside Open) are observable. There is
	// no post-Open setter — the codebase favors config-at-Open over
	// runtime mutation.
	//
	// Under encryption, Open's authentication of page 1 is not reported
	// as it happens: a page 1 that fails it may be a wrong key, and Open
	// tells the two apart first (see ErrWrongKey). A wrong key calls the
	// callback for no page, neither page 1 nor the other pages Open
	// tried. A damaged page 1 calls it once, for page 1, before Open
	// returns ErrPageIntegrity, when another page Open tried authenticates
	// under the key and so proves it. When none does, Open calls the
	// callback for no page, also when the key is right: it returns
	// ErrWrongKey when every page it tried was read and failed its
	// authentication, as a wrong key makes them all fail, and the error of
	// the read when a page could not be read (an I/O error), since that page
	// might have proved the key.
	OnIntegrityError func(IntegrityError)
}

// EncryptionConfig enables page-level encryption of the database file.
//
// Encryption is enabled by a passphrase or by a raw key, and uses one of the
// built-in ciphers:
//
//	cfg.Encryption.Passphrase = []byte("my-pass")
//	cfg.Encryption.CipherType = anystore.CipherChaCha20Poly1305 // optional
//
// or, with a 32-byte key the application holds:
//
//	cfg.Encryption.Key = key
//
// The default cipher is AES-256-GCM; CipherType picks another built-in
// (ChaCha20-Poly1305 / XChaCha20-Poly1305).
//
// The field that carries the secret says what it is, never its length:
//   - Passphrase is stretched with PBKDF2-HMAC-SHA256 to a 32-byte key
//     against the 16-byte salt stored in the database header, at
//     KDFIterations rounds, whatever its length, 32 bytes included.
//   - Key is the 32-byte cipher key itself, used with no key derivation.
//   - Set one of them. Both nil means no encryption. Open fails with
//     ErrInvalidConfig for both set, and for a Key that is not nil and not
//     32 bytes long, empty included. An empty Passphrase that is not nil
//     fails Open too.
//
// SQLCipher has the same rule: a raw key must be spelled as a blob literal,
// x'<64 hex>', so a passphrase never becomes a raw key by its length.
//
// Once a database is created with encryption, it must always be opened
// with matching Encryption config. Open returns ErrEncryptionKeyRequired
// for an encrypted file opened with an empty EncryptionConfig (Enabled
// reports false), ErrNotEncrypted for a file that is not encrypted opened
// with one that is not empty, and ErrWrongKey for an EncryptionConfig that
// does not open the file: another passphrase or key than the file's, or
// another cipher. A Passphrase and a Key of the same bytes are two keys:
// a file created with one is ErrWrongKey with the other. None of the three
// errors matches ErrCorrupt, and each leaves the file unchanged. There is
// no in-place rekey; changing a passphrase or key requires exporting to a
// freshly-opened database.
//
// An InMemory database keeps its pages unencrypted in memory (there is
// no at-rest artifact to protect). Its Backup is encrypted only when
// Enabled reports true: it is then an encrypted file that opens with the
// same EncryptionConfig. With the zero EncryptionConfig, the default, its
// Backup is a plain file.
type EncryptionConfig struct {
	// Passphrase is a secret such as a password. PBKDF2-HMAC-SHA256
	// stretches it to the cipher key against the salt in the database
	// header, at KDFIterations rounds, whatever its length. Nil = no
	// passphrase; an empty one that is not nil fails Open. Open keeps a
	// copy of it, which Backup uses, so the caller may clear its slice once
	// Open returns.
	Passphrase []byte

	// Key is a raw cipher key, used as it is, with no key derivation. It
	// must be exactly 32 bytes: Open fails with ErrInvalidConfig for a Key
	// that is not nil and of another length, empty included, and for a Key
	// set together with Passphrase. KDFIterations does not apply to it.
	// Nil = no key.
	//
	// Key is for key material the application already has: a key from a
	// key store, or one it derived itself, for example with HKDF from a
	// master key, one per database. A password belongs in Passphrase,
	// which stretches it. Every database encrypted with one Key, the copies
	// Backup makes of it included, draws its random nonces under that key,
	// so they all share one budget of random nonces: about 2^32 page
	// writes per key with AES-GCM and ChaCha20-Poly1305, whose nonces are
	// 12 bytes. A key per database is the safer use.
	//
	// A file that a release before v2.6.1 created with a Passphrase of
	// exactly 32 bytes was encrypted with those bytes as the raw key: it
	// opens with Key set to them, and with that Passphrase it is
	// ErrWrongKey.
	//
	// Open keeps a copy of it, which Backup uses, so the caller may clear
	// its slice once Open returns.
	Key []byte

	// KDFIterations overrides the PBKDF2 cost of Passphrase. Zero means
	// 256,000 (the SQLCipher v4 default). Key does not use it. Use lower
	// values only in tests. With a Passphrase, a count above 2,147,483,647
	// (2^31-1; 2,147,483,646 on a 32-bit target) fails Open with
	// ErrInvalidConfig. The count is part of the key: a database created
	// with a Passphrase opens only with the effective count it was created
	// with (0 and 256,000 are one).
	KDFIterations int

	// CipherType selects the built-in AEAD when Passphrase or Key is set.
	// Zero value (CipherAES256GCM) is the default. A value other than the
	// Cipher constants fails Open with ErrInvalidConfig when Passphrase or
	// Key is set; without either it is ignored.
	CipherType CipherType
}

// Enabled reports whether this config encrypts the database: whether
// Passphrase or Key is set.
func (e EncryptionConfig) Enabled() bool {
	return e.Passphrase != nil || e.Key != nil
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
	// waits until all readers are reading from the database file only, so
	// that the next write transaction starts the WAL over from its
	// beginning. The checkpoint itself leaves the WAL as it is: its size
	// does not change, and the file is reused, not shrunk. Blocks new
	// writers while pending, but does not impede readers. Returns a busy
	// error if a reader still uses the WAL when the busy timeout ends.
	FlushModeCheckpointRestart FlushMode = "CHECKPOINT_RESTART"

	// FlushModeCheckpointTruncate is like Restart but also resets the WAL
	// and truncates its file to zero bytes. It is the only mode that gives
	// the WAL's disk space back.
	FlushModeCheckpointTruncate FlushMode = "CHECKPOINT_TRUNCATE"
)

type DurabilityConfig struct {
	// Sentinel enables the sentinel file (.lock) that tracks database dirty state
	// When true (default is false), the sentinel file is used to detect unclean shutdowns and perform QuickCheck on open.
	// A write creates the file. A flush removes it, and so does a Close that checkpointed as the last
	// handle on the database file. A Close while another process still has the database open leaves it:
	// that close runs no checkpoint, so if the remaining processes are killed the next open runs the check.
	// An InMemory database has no sentinel file.
	Sentinel bool

	// Enable auto-flush according to IdleAfter and FlushMode
	AutoFlush bool

	// IdleAfter is the duration to wait after the last write before performing autoflush
	// Default: 20s
	IdleAfter time.Duration

	// FlushMode specifies how to autoflush data during idle periods
	// Default: FlushModeCheckpointPassive. A value other than the
	// FlushModeCheckpoint constants fails Open with ErrInvalidConfig.
	FlushMode FlushMode
}

// maxCacheSize is the largest CacheSize Open uses; a larger one is taken as
// it. SQLite clamps cache_size rather than refusing it: pcache1Cachesize holds
// the page count of a cache's group at 0x7fff0000 (pcache1.c:820-822), the
// cache's own count when each cache has a group of its own, as here. A 32-bit
// target is held lower, at the pages its address space can hold, 2 GiB of
// 4 KiB pages: (math.MaxInt+1)/4096, 524,288. That also keeps the cache's int
// arithmetic (90% of the page count) in range there. Nothing is allocated in
// proportion to the bound: a cache allocates pages as it fills, and its hash
// table grows with them past a fixed initial size (btree newPcache).
const maxCacheSize = min(0x7fff0000, math.MaxInt/4096+1)

// maxReadConcurrency is the largest ReadConcurrency Open uses; a larger one is
// taken as it. Open allocates the pool of idle reader caches at the full
// number of readers, 8 bytes a slot, whether or not they are ever used, so the
// number has to be one every Open can allocate: at this bound it is 512 KiB.
// The readers themselves cost far more: with the default cache size each one
// that runs takes 20 pages (80 KiB) at its first read, so 65,536 running at
// once would hold more than 5 GiB of page cache, and a process with that many
// is bound by its memory before it is by this cap.
const maxReadConcurrency = 1 << 16

// maxKDFIterations is the largest Encryption.KDFIterations Open derives a key
// with: 2^31-1, the largest kdf_iter SQLCipher takes (a C int). The count is
// part of the key, so a database opens only with the count it was created
// with: a larger one is refused, not clamped to another key. Go's PBKDF2 loop
// never ends at the largest int, so on a 32-bit target, where that is 2^31-1,
// the bound is one less. One PBKDF2-HMAC-SHA256 iteration takes about 0.1 µs
// on an AMD Ryzen 9 9950X core and 0.11 µs on an Apple M2 core, so the
// default 256,000 costs Open about 30 ms and the bound about 4 minutes.
const maxKDFIterations = min(math.MaxInt32, math.MaxInt-1)

// validate reports the first field of c that Open cannot run with, wrapped in
// ErrInvalidConfig. It runs before Open resolves the path or touches a file,
// so a refused Config leaves nothing open. Sizes past what the code can
// allocate are clamped instead (cacheSizes); a value with no nearest valid
// one, as an unknown FlushMode or CipherType or a Key that is not 32 bytes
// long, two that exclude each other, as a Key and a Passphrase, or one that
// would derive another key, as a KDFIterations past maxKDFIterations, is an
// error.
func (c *Config) validate() error {
	switch c.Durability.FlushMode {
	case "", FlushModeCheckpointPassive, FlushModeCheckpointFull, FlushModeCheckpointRestart, FlushModeCheckpointTruncate:
	default:
		return fmt.Errorf("%w: Durability.FlushMode %q is not one of the FlushModeCheckpoint modes", ErrInvalidConfig, c.Durability.FlushMode)
	}
	if e := c.Encryption; e.Enabled() {
		if e.Key != nil && e.Passphrase != nil {
			return fmt.Errorf("%w: Encryption.Key and Encryption.Passphrase are both set", ErrInvalidConfig)
		}
		if e.Key != nil && len(e.Key) != btree.KeyLen {
			return fmt.Errorf("%w: Encryption.Key is %d bytes, not %d", ErrInvalidConfig, len(e.Key), btree.KeyLen)
		}
		switch e.CipherType {
		case CipherAES256GCM, CipherChaCha20Poly1305, CipherXChaCha20Poly1305:
		default:
			return fmt.Errorf("%w: Encryption.CipherType %q is not one of the Cipher constants", ErrInvalidConfig, e.CipherType)
		}
		// Only a passphrase goes through the KDF: a Key ignores the count.
		if e.Passphrase != nil && e.KDFIterations > maxKDFIterations {
			return fmt.Errorf("%w: Encryption.KDFIterations is %d, more than %d", ErrInvalidConfig, e.KDFIterations, maxKDFIterations)
		}
	}
	return nil
}

// cacheSizes returns the page cache size and the reader limit Open uses: the
// configured ones, their defaults for zero or a negative value, and the
// bounds for a value past them.
func (c *Config) cacheSizes() (cacheSize, readConcurrency int) {
	cacheSize = c.CacheSize
	if cacheSize <= 0 {
		cacheSize = 5000
	}
	// Concurrent read transactions are bounded by a semaphore (each live
	// reader holds its own page cache). The default scales with the host —
	// max(NumCPU-1, 4) — so concurrent $text/Find reads aren't artificially
	// serialized on a multi-core machine, while staying modest on small
	// devices. Reader caches are created lazily up to this cap, so RAM follows
	// actual concurrency, not the cap.
	readConcurrency = c.ReadConcurrency
	if readConcurrency <= 0 {
		readConcurrency = max(runtime.NumCPU()-1, 4)
	}
	return min(cacheSize, maxCacheSize), min(readConcurrency, maxReadConcurrency)
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
