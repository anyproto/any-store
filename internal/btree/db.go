package btree

// DB is the public API for the embedded key-value database.
// It manages namespaces, transactions, and the underlying pager/WAL.

import (
	// BEGIN ENCRYPTION
	"crypto/rand"
	// END ENCRYPTION
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
)

// openDBs tracks database files currently open in this process. It is the sole
// guard against same-process double-open corruption (two writers each believing
// they hold the exclusive WAL write lock): in-process SHM locks are
// instance-local, and a same-process second SHARED flock on the DB file also
// succeeds, so neither backstops this. SQLite coordinates same-process opens
// through a process-global unixInodeInfo keyed by (dev,ino) (os_unix.c:1282-1349);
// any-store forbids them instead, but must key on the same FILE IDENTITY rather
// than a lexical path so that the same inode reached via a symlink, hardlink,
// bind-mount, or distinct mount path is recognised as already-open. See
// openRegistryKey for how the key is derived. Prevents double-open corruption.
var openDBs sync.Map

// openRegistryKey returns the key under which a database file is tracked in
// openDBs. It prefers FILE IDENTITY (device, inode) over the lexical path so
// that the same physical file opened under two different spellings (symlink,
// hardlink, bind-mount, …) collides in the registry, matching SQLite's
// (dev,ino) unixInodeInfo keying.
//
// It falls back to the lexical canonical path (filepath.Abs) when no inode is
// available — a brand-new file that os.Stat cannot yet see, a platform with no
// portable (dev,ino) (Windows/wasm), or a VFS-injected FileInfo. Those cases
// keep the previous lexical behaviour and never break in-memory/VFS opens.
func openRegistryKey(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("btree: cannot resolve path: %w", err)
	}
	if fi, statErr := os.Stat(path); statErr == nil {
		if key, ok := fileIdentityKey(fi); ok {
			return key, nil
		}
	}
	return abs, nil
}

// Options configures the database.
type Options struct {
	PageSize  uint32 // Page size in bytes (default: 4096)
	CacheSize int    // Maximum number of cached pages (default: 2000)

	// DisableAutoCheckpoint disables auto-checkpoint entirely (manual Checkpoint() only).
	DisableAutoCheckpoint bool

	// AutoCheckpointAfter is the number of WAL frames after which an
	// automatic passive checkpoint is triggered. 0 means use default (10000).
	// Ignored when DisableAutoCheckpoint is true.
	AutoCheckpointAfter int

	// InProcess uses heap-backed shared memory for the WAL index instead of
	// mmap'd files with POSIX fcntl locks. Faster, but restricts access to a
	// single OS process. Equivalent to SQLite's PRAGMA locking_mode=EXCLUSIVE
	// (WAL_HEAPMEMORY_MODE): SHM locks become no-ops, memory barriers are
	// skipped, and no .db-wal-shm file is created.
	//
	// Forced to true automatically on platforms without mmap SHM support
	// (e.g. Windows) and when InMemory is true.
	InProcess bool

	// NoCommitSync skips fdatasync on WAL commit. WAL frames are still written
	// to the WAL file on disk, but durability is deferred until checkpoint.
	// Equivalent to SQLite's PRAGMA synchronous=NORMAL in WAL mode:
	//   - false (default) = synchronous=FULL  — fsync every WAL commit
	//   - true            = synchronous=NORMAL — fsync only on checkpoint
	NoCommitSync bool

	// InMemory keeps the entire database in memory with no files on disk.
	// WAL frames are stored in a heap-backed slice (memFrames) instead of
	// being written to a WAL file. Checkpoint moves frames into the in-memory
	// page cache (which is made non-purgeable so pages are never evicted).
	// The database does not survive process crashes.
	//
	// When InMemory is true, InProcess and NoCommitSync are forced to true
	// automatically (heap SHM, no fsync — both meaningless for in-memory).
	// The path argument to Open is ignored and can be any string.
	InMemory bool

	// MaxReaders is the maximum number of concurrent read transactions per DB.
	// Each reader holds a private pcache with up to CacheSize pages that
	// persist across transactions (matching SQLite's per-connection cache).
	// Total reader cache memory per DB is bounded by
	// MaxReaders * CacheSize * PageSize.
	// Default: 4.
	MaxReaders int

	// UsePageSlab opts this DB into the global pre-allocated page buffer slab.
	// The slab must be initialized beforehand via ConfigPageCache (btree) or
	// InitPageBuffer (anystore). When false (default), page buffers use
	// sync.Pool, matching SQLite's default malloc-based allocation.
	UsePageSlab bool

	// BEGIN ENCRYPTION
	// Key enables page-level AES-256-GCM encryption when non-nil.
	// Accepted forms:
	//   - len 32: raw AES-256 key, used directly (no KDF).
	//   - any other len: treated as a passphrase; PBKDF2-HMAC-SHA256 derives
	//     a 32-byte key using the file's salt and KDFIterations rounds.
	// An empty slice (len 0) is rejected with an error; use nil for "no
	// encryption".
	Key []byte

	// KDFIterations controls PBKDF2 cost. Ignored when Key is a raw
	// 32-byte key. Zero means DefaultKDFIterations (256,000). Use lower
	// values only in tests.
	KDFIterations int

	// CipherType selects the built-in cipher when Key is set. Zero value
	// (CipherAES256GCM) is the default. See CipherType constants for the
	// available options.
	CipherType CipherType

	// Codec, when non-nil, replaces the built-in cipher with a caller-
	// provided implementation (e.g. HSM-backed). When both Key and Codec
	// are set, Codec wins and Key/CipherType are ignored. Callers own the
	// codec's lifetime and must supply an implementation safe for
	// concurrent use.
	Codec Codec

	// Checksum, when true, installs a no-encryption page-checksum codec
	// (XXH3-128 trailer, 16 bytes per page). Mutually exclusive with
	// Key and Codec — combining them returns an error from Open.
	// Conceptually mirrors SQLite's cksumvfs extension. See
	// docs/plans/2026-04-27-cksumvfs-port.md.
	Checksum bool

	// OnIntegrityError, if non-nil, is fired by the installed codec
	// (cksum or AEAD) on every per-page integrity failure. Same hook
	// for both modes. Runs on the I/O goroutine; must not block.
	OnIntegrityError func(pgno uint32, inner error)
	// END ENCRYPTION

	// MmapSize enables mmap-backed reads of the database file up to the
	// given byte limit. Zero disables mmap (reads use pread via ReadAt).
	// Values > 0 allocate a shared mapping of min(DBsize, MmapSize)
	// bytes; reads falling within the mapping memcpy from it instead of
	// issuing pread. Writes still go through WriteAt and are coherent
	// via the OS unified page cache. Matches SQLite's PRAGMA mmap_size
	// (sqlitec/src/os_unix.c:4240 SQLITE_FCNTL_MMAP_SIZE).
	//
	// Linux/darwin + amd64/arm64 only; no-op on other platforms.
	//
	// SAFETY: enabling trades a small perf win for SIGBUS-crash tail
	// risk on file-shrink / device-removal events (USB unplug, iCloud
	// eviction, NFS timeout). Go cannot recover from SIGBUS; the whole
	// process dies. Only enable on stable local storage. Do NOT enable
	// on mobile / networked filesystems / cloud sync paths. See
	// anystore.Config.MmapSize for the full rationale.
	MmapSize int64
}

// BEGIN ENCRYPTION

// CipherType selects a built-in AEAD for page encryption.
type CipherType string

const (
	// CipherAES256GCM is AES-256 in Galois/Counter Mode. Hardware-accelerated
	// on modern amd64/arm64 via AES-NI (~5 GB/s). Per-page overhead: 32 bytes
	// (12-byte nonce + 16-byte tag, rounded to 32 for block alignment).
	// Default choice for disk-backed databases.
	CipherAES256GCM CipherType = ""

	// CipherChaCha20Poly1305 is ChaCha20 with Poly1305 authentication.
	// Constant-time in pure software without requiring hardware support.
	// Per-page overhead: 32 bytes. Competitive with AES-GCM on non-AES-NI
	// hardware; slightly slower on AES-NI hardware.
	CipherChaCha20Poly1305 CipherType = "chacha20-poly1305"

	// CipherXChaCha20Poly1305 is the extended-nonce variant of ChaCha20-
	// Poly1305. Uses a 24-byte nonce which makes random-nonce collision
	// vanishingly improbable even at astronomical write rates. Per-page
	// overhead: 48 bytes. Recommended only for workloads exceeding ~2^32
	// page writes per key.
	CipherXChaCha20Poly1305 CipherType = "xchacha20-poly1305"
)

// newCodecFromType constructs a codec of the selected type from a 32-byte key.
// CipherAES256GCM is the zero value (""), so no-CipherType-set == AES-GCM.
func newCodecFromType(ct CipherType, key []byte) (Codec, error) {
	switch ct {
	case CipherAES256GCM:
		return NewAESCodec(key)
	case CipherChaCha20Poly1305:
		return NewChaCha20Poly1305Codec(key)
	case CipherXChaCha20Poly1305:
		return NewXChaCha20Poly1305Codec(key)
	default:
		return nil, fmt.Errorf("btree: unknown CipherType %q", ct)
	}
}

// END ENCRYPTION

// DefaultOptions returns default database options.
func DefaultOptions() Options {
	return Options{
		PageSize:            DefaultPageSize,
		CacheSize:           defaultCacheSize,
		AutoCheckpointAfter: AutoCheckpointThreshold,
	}
}

// BEGIN ENCRYPTION

// buildCodec constructs the Codec from Options. Returns (nil, nil) when no
// encryption is configured. Input modes:
//   - Options.Codec set: used as-is.
//   - Options.Key set with len == KeyLen: raw AES-256 key, used directly.
//   - Options.Key set with any other non-zero length: treated as a passphrase,
//     derived via PBKDF2 using the supplied salt and KDFIterations.
//   - Options.Key set with length 0: rejected.
//
// buildCodec constructs the Codec for the resolved mode. `useCksum` is the
// caller's effective checksum decision (file-state-authoritative on reopen,
// opts.Checksum on new DBs). buildCodec rejects combining checksum mode with
// explicit AEAD configuration, but is otherwise mechanical.
func buildCodec(opts Options, salt []byte, useCksum bool) (Codec, error) {
	if opts.Codec != nil {
		if useCksum {
			return nil, fmt.Errorf("btree: Options.Checksum cannot be combined with Options.Codec")
		}
		return opts.Codec, nil
	}
	if opts.Key != nil {
		if useCksum {
			return nil, fmt.Errorf("btree: Options.Checksum cannot be combined with Options.Key")
		}
		if len(opts.Key) == 0 {
			return nil, fmt.Errorf("btree: Options.Key is empty (use nil for no encryption)")
		}
		var raw []byte
		if len(opts.Key) == KeyLen {
			raw = opts.Key
		} else {
			raw = DeriveKey(opts.Key, salt, opts.KDFIterations)
		}
		return newCodecFromType(opts.CipherType, raw)
	}
	if useCksum {
		return newCksumCodec(), nil
	}
	return nil, nil
}

// readInitialHeader opens the file at path and reads the database header.
// Returns ErrNotExist if the file doesn't exist (distinguished by callers).
// Returns ErrCorrupt if the file exists but is shorter than dbHeaderSize.
func readInitialHeader(path string) (dbHeader, error) {
	var h dbHeader
	f, err := osOpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return h, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return h, err
	}
	if info.Size() == 0 {
		return h, errEmptyFile
	}
	buf := make([]byte, dbHeaderSize)
	if _, err := f.ReadAt(buf, 0); err != nil {
		return h, err
	}
	if err := h.deserialize(buf); err != nil {
		return h, err
	}
	return h, nil
}

// errEmptyFile signals that the target path exists but is a freshly-created
// (zero-byte) file. Not exported — used only as a control-flow signal inside
// Open.
var errEmptyFile = errors.New("btree: empty file")

// END ENCRYPTION

// DB represents an open database.
type DB struct {
	mu              sync.RWMutex
	writeMu         sync.Mutex // serializes write transactions
	pager           *pager
	path            string
	registryKey     string // key under which this DB is tracked in openDBs (file identity or lexical path)
	opts            Options
	closing         atomic.Bool // set to reject new transactions
	closed          atomic.Bool // set when Close() is actually called
	writerLocksDone atomic.Bool // CAS guard: writer lock cleanup (endRead+RUnlock+Unlock) runs exactly once

	// Namespace root pages are stored in a master table on page 1.
	// Format: each cell in the master B-tree maps namespace name -> root page number (4 bytes).
	masterBT *btree

	// Local counter cache for multi-process staleness detection.
	// Accessed atomically: written by Commit (under writeMu + mu.RLock)
	// and read by BeginRead/BeginWrite (under mu.RLock). Since both
	// paths hold mu.RLock (not exclusive), atomics are required.
	localFileChangeCounter atomic.Uint32
	localSchemaCookie      atomic.Uint32

	// dataVersion is a monotonically increasing counter incremented on every
	// write commit. Used by persistent reader caches to detect staleness:
	// when a cache's dataVersion differs from the current DB value, the cache
	// is cleared. Unlike walMaxFrame, this counter never wraps around after
	// checkpoint restart. Matches SQLite's pPager->iDataVersion (pager.c:1776).
	dataVersion atomic.Uint64

	readTxPool  sync.Pool
	writeTxPool sync.Pool

	// readerCaches is a GC-stable pool of per-reader pcache instances.
	// Unlike sync.Pool, this channel survives GC cycles, so reader caches
	// (including their pFree buffers) persist across transactions. This
	// matches SQLite's model where PCache persists for the pager's lifetime.
	// Capacity is MaxReaders (same as readerSem).
	readerCaches    chan *pcache
	readerCacheSize int // max(CacheSize/10, 50)

	// readerSem limits the number of concurrent read transactions.
	// Buffered channel with capacity MaxReaders; BeginRead sends, Rollback receives.
	readerSem chan struct{}
	// closeCh is closed when the DB is shutting down, unblocking any
	// goroutines waiting on readerSem in BeginRead.
	closeCh   chan struct{}
	closeOnce sync.Once // guards closing closeCh

	// lastAutoCheckpointErr stores the most recent non-nil error from
	// tx.pager.tryCheckpoint() during auto-checkpoint. Allows monitoring
	// code to detect silent failures (disk full, permissions, etc.) that
	// the auto-checkpoint path otherwise swallows. Reads/writes are
	// atomic via atomic.Value. Nil means "no error seen since open".
	lastAutoCheckpointErr atomic.Value
}

// autoCheckpointResult wraps the result of an auto-checkpoint attempt
// so atomic.Value can hold a nil error without tripping its typed-nil
// panic.
type autoCheckpointResult struct{ err error }

// LastAutoCheckpointError returns the error from the most recent
// auto-checkpoint attempt, or nil if the last attempt succeeded (or
// auto-checkpoint has never run since this DB was opened). Use this
// for monitoring / health checks — the auto-checkpoint itself is
// best-effort and its errors are otherwise not surfaced to callers.
func (db *DB) LastAutoCheckpointError() error {
	v := db.lastAutoCheckpointErr.Load()
	if v == nil {
		return nil
	}
	r, _ := v.(autoCheckpointResult)
	return r.err
}

// Open opens or creates a database at the given path.
func Open(path string, opts Options) (*DB, error) {
	if opts.PageSize == 0 {
		opts.PageSize = DefaultPageSize
	}
	if opts.PageSize < MinPageSize || opts.PageSize > MaxPageSize {
		return nil, errors.New("btree: invalid page size")
	}
	// Page size must be a power of 2
	if opts.PageSize&(opts.PageSize-1) != 0 {
		return nil, errors.New("btree: page size must be a power of 2")
	}
	if opts.CacheSize <= 0 {
		opts.CacheSize = defaultCacheSize
	}
	if opts.AutoCheckpointAfter == 0 && !opts.DisableAutoCheckpoint {
		opts.AutoCheckpointAfter = AutoCheckpointThreshold
	}

	if opts.InMemory {
		opts.InProcess = true
		opts.NoCommitSync = true
	}
	if !hasMmapShm {
		// Platform lacks mmap SHM (e.g. Windows); force heap SHM.
		opts.InProcess = true
	}

	if opts.UsePageSlab && !globalPageSlab.initialized.Load() {
		return nil, ErrPageSlabNotInitialized
	}

	if err := initPageBufferPool(opts.PageSize); err != nil {
		return nil, err
	}

	// canonicalPath is the lexical absolute path, used for Path()/db.path. The
	// double-open guard itself keys on FILE IDENTITY (dev,ino) and is registered
	// after p.open() below, once the file exists and can be stat'd — see the
	// openDBs registration following p.open().
	var canonicalPath, registryKey string
	if !opts.InMemory {
		var err error
		canonicalPath, err = filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("btree: cannot resolve path: %w", err)
		}
	}
	openSuccess := false
	defer func() {
		if !openSuccess && registryKey != "" {
			openDBs.Delete(registryKey)
		}
	}()

	p := newPager(path, opts.PageSize, opts.CacheSize, !opts.InMemory)
	p.useSlab = opts.UsePageSlab
	p.writerCache.useSlab = opts.UsePageSlab
	p.inProcess = opts.InProcess
	p.noCommitSync = opts.NoCommitSync
	p.inMemory = opts.InMemory
	p.mmapSize = opts.MmapSize

	// BEGIN ENCRYPTION
	// Build and install codec before p.open(). For an existing file we read
	// the header first to discover the salt; for a new file we generate a
	// fresh salt. InMemory databases skip encryption entirely (encryption.md §7).
	//
	// The encryption marker on disk is a non-zero Salt field (bytes 72-87
	// of page 1). ReservedSpace alone is NOT the marker, because the same
	// byte is used by SQLite-compat tests that exercise reserve-related
	// code paths on plaintext databases.
	//
	// Note: there is a TOCTOU window between readInitialHeader (which
	// opens/reads/closes the file) and p.open() (which reopens and locks).
	// An external process swapping the file in that window would surface
	// as a decrypt error on the first page read — not silent corruption.
	// openDBs above blocks same-process double-open. A future hardening
	// could hold a shared lock across readInitialHeader and p.open().
	wantEncryption := opts.Key != nil || opts.Codec != nil
	var zeroSalt [SaltLen]byte
	// Resolve effective codec mode. Encryption is opt-in (errors if it
	// doesn't match the file). Checksum is auto-detected from the file's
	// ReservedSpace marker on existing DBs and honored from Options.Checksum
	// on new DBs — file state is authoritative on reopen, so users can't
	// accidentally upgrade or downgrade a live database via config drift.
	useCksum := false
	if !opts.InMemory {
		existingHeader, herr := readInitialHeader(path)
		fileExists := herr == nil
		if herr != nil && !errors.Is(herr, os.ErrNotExist) && !errors.Is(herr, errEmptyFile) {
			return nil, fmt.Errorf("btree: read header: %w", herr)
		}
		if fileExists {
			fileEncrypted := existingHeader.Salt != zeroSalt
			// Checksum-mode marker: zero salt + ReservedSpace == cksumOverhead (16).
			// Any other non-zero ReservedSpace is left to higher-level
			// integrity validation (e.g. tests synthesizing off-page cells).
			fileCksum := !fileEncrypted && existingHeader.ReservedSpace == cksumOverhead
			switch {
			case fileEncrypted && !wantEncryption:
				return nil, fmt.Errorf("btree: database file is encrypted, Options.Key or Options.Codec required")
			case !fileEncrypted && wantEncryption:
				return nil, fmt.Errorf("btree: database file is not encrypted, remove Options.Key/Codec")
			}
			useCksum = fileCksum
		} else {
			useCksum = opts.Checksum
		}
		wantCodec := wantEncryption || useCksum
		if wantCodec {
			var salt [SaltLen]byte
			if fileExists {
				salt = existingHeader.Salt
			} else if wantEncryption {
				if _, err := rand.Read(salt[:]); err != nil {
					return nil, fmt.Errorf("btree: generate salt: %w", err)
				}
			}
			codec, cerr := buildCodec(opts, salt[:], useCksum)
			if cerr != nil {
				return nil, cerr
			}
			if sink, ok := codec.(OnErrorSink); ok && opts.OnIntegrityError != nil {
				sink.SetOnError(opts.OnIntegrityError)
			}
			p.header.Salt = salt
			p.installCodec(codec)
		}
	} else if opts.InMemory && (wantEncryption || opts.Checksum) {
		// InMemory: build codec from zero salt; no on-disk marker.
		var salt [SaltLen]byte
		codec, cerr := buildCodec(opts, salt[:], opts.Checksum)
		if cerr != nil {
			return nil, cerr
		}
		if sink, ok := codec.(OnErrorSink); ok && opts.OnIntegrityError != nil {
			sink.SetOnError(opts.OnIntegrityError)
		}
		p.installCodec(codec)
	}
	// END ENCRYPTION

	if err := p.open(); err != nil {
		return nil, err
	}

	// Register in the process-global open-DB guard, keyed on FILE IDENTITY
	// (dev,ino) rather than the lexical path, so that the same inode reached via
	// a symlink/hardlink/bind-mount/distinct-mount-path is recognised as already
	// open. We do this AFTER p.open() because the file now definitely exists (it
	// was created for a new DB), so its inode is stable — keying before open
	// would mis-key brand-new files (no inode yet -> lexical fallback) against a
	// later open of the same now-existing file (inode). This mirrors SQLite,
	// which opens the fd and then consults its (dev,ino) unixInodeInfo registry
	// (os_unix.c:1282-1349). Falls back to the lexical canonical path when no
	// inode is available (Windows/wasm, or a VFS-injected FileInfo).
	if !opts.InMemory {
		registryKey = canonicalPath
		if fi, statErr := p.file.Stat(); statErr == nil {
			if key, ok := fileIdentityKey(fi); ok {
				registryKey = key
			}
		}
		if _, loaded := openDBs.LoadOrStore(registryKey, true); loaded {
			_ = p.close()
			// Clear so the deferred cleanup does not delete the entry that the
			// already-open handle owns.
			registryKey = ""
			return nil, ErrDatabaseOpen
		}
	}

	// Reject databases created with an older schema format.
	// Schema format 5 introduced unified leaf cell overflow (key+value as a
	// single payload blob). Older formats stored key fully on-page and only
	// allowed value overflow, which panics on large keys.
	if p.header.SchemaFormat != 0 && p.header.SchemaFormat < 5 {
		_ = p.close()
		return nil, ErrOldFormat
	}

	readerCacheSize := opts.CacheSize

	maxReaders := opts.MaxReaders
	if maxReaders <= 0 {
		maxReaders = defaultMaxReaders
	}

	db := &DB{
		pager:       p,
		path:        path,
		registryKey: registryKey,
		opts:        opts,
		masterBT: &btree{
			pager:    p,
			rootPage: 1,
		},
		readerCacheSize: readerCacheSize,
		readerCaches:    make(chan *pcache, maxReaders),
		readerSem:       make(chan struct{}, maxReaders),
		closeCh:         make(chan struct{}),
	}

	// Initialize local counters from the on-disk state (reading through WAL).
	maxFrame, slot, err := p.beginRead()
	if err != nil {
		p.close()
		return nil, err
	}
	fcc, sc, err := p.readHeaderCounters(maxFrame)
	p.endRead(slot)
	if err != nil {
		p.close()
		return nil, err
	}
	db.localFileChangeCounter.Store(fcc)
	db.localSchemaCookie.Store(sc)

	if canonicalPath != "" {
		db.path = canonicalPath
	}
	openSuccess = true
	return db, nil
}

// Close closes the database.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return ErrClosed
	}
	db.closing.Store(true)
	db.closeOnce.Do(func() { close(db.closeCh) })

	if !db.writeMu.TryLock() {
		// Writer holds writeMu. Force-rollback the abandoned/in-flight tx.
		// writerOpMu serializes with commit/rollback so we don't interleave
		// with a concurrent Commit or Rollback call.
		// Use rollbackForClose which skips writerCache operations to avoid
		// racing with the writer's B-tree ops (pcache has no mutex).
		db.pager.writerOpMu.Lock()
		if pagerState(db.pager.state.Load()) == pagerWriter {
			db.pager.rollbackForClose()
		}
		db.pager.writerOpMu.Unlock()

		// Use CAS to ensure the writer lock cleanup (endRead + RUnlock +
		// Unlock) happens exactly once. Without this, the writer goroutine
		// completing concurrently could race with Close and double-release.
		if db.writerLocksDone.CompareAndSwap(false, true) {
			db.pager.endRead(db.pager.writerWalSlot)
			db.mu.RUnlock()
			db.writeMu.Unlock()
		}
		db.writeMu.Lock()
	}
	db.writeMu.Unlock()
	// Wait for all active readers to finish
	db.mu.Lock()
	db.mu.Unlock()
	// Drain reader cache pool — return buffers to slab.
	for {
		select {
		case c := <-db.readerCaches:
			c.destroy()
		default:
			goto drained
		}
	}
drained:
	err := db.pager.close()
	// Remove from open registry after full cleanup
	if !db.opts.InMemory {
		openDBs.Delete(db.registryKey)
	}
	return err
}

// SetClosing marks the database as closing, causing new transactions to fail.
// Also unblocks any goroutines waiting on the reader semaphore in BeginRead.
func (db *DB) SetClosing() {
	db.closing.Store(true)
	db.closeOnce.Do(func() { close(db.closeCh) })
}

// Path returns the database file path.
// For in-memory databases it returns the empty string, matching SQLite's
// sqlite3BtreeGetFilename (btree.c:11302), which calls
// sqlite3PagerFilename(pPager, /*nullIfMemDb=*/1) and returns "" (&zFake[4])
// for memDb/TEMP databases (pager.c:7088).
func (db *DB) Path() string {
	if db.opts.InMemory {
		return ""
	}
	return db.path
}

// PageSize returns the fixed page size in bytes for this database.
// ~ sqlite3BtreeGetPageSize (btree.c). Unlike SQLite, our page size is
// immutable after Open — set from Options.PageSize or read from the header.
func (db *DB) PageSize() uint32 {
	return db.pager.pageSize
}

// DatabaseSize returns the current number of pages in the database,
// including page 1 (the header page). It reads the live, process-global
// writer counter (atomic pager.dbSize) and provides NO read-snapshot
// guarantee: unlike SQLite's sqlite3BtreeLastPage / sqlite3PagerPagecount
// (btree.c:2371, pager.c:3925), which return the caller's per-connection
// snapshot count, this value can be advanced by a concurrent writer at any
// time. It is meaningful only when called inside the writer (where p.dbSize
// is the writer's own live count) or when there are no concurrent writers.
// Callers that need a count consistent with a read snapshot must use
// ReadTx.DatabaseSize() instead, which returns the snapshot-faithful bound.
// DB has no tx handle to derive a snapshot from, so this method intentionally
// keeps the global-counter semantics.
func (db *DB) DatabaseSize() uint32 {
	return db.pager.dbSize.Load()
}

// HasOpenTransaction returns true if the DB has an active read or write
// transaction. Write state is tracked on the pager; reader state lives
// on the DB's readerSem (pager.state only transitions for writers).
// ~ sqlite3BtreeTxnState != SQLITE_TXN_NONE (backup.c:125).
func (db *DB) HasOpenTransaction() bool {
	if pagerState(db.pager.state.Load()) != pagerOpen {
		return true
	}
	return len(db.readerSem) > 0
}

// Options returns a copy of the options this DB was opened with. Used
// by high-level callers (e.g. anystore.Backup) to open a destination
// DB with matching page size. No direct SQLite counterpart — SQLite
// uses attached-db paths to imply options.
func (db *DB) Options() Options {
	return db.opts
}

// beginRead starts a read-only transaction.
// When readCounters is false, disk counters are initialized from local counters
// without reading page-1 metadata, which is useful for hot point-lookups.
func (db *DB) beginRead(readCounters bool) (*ReadTx, error) {
	if db.closing.Load() {
		return nil, ErrClosed
	}

	// Acquire reader semaphore — limits concurrent read transactions.
	// Uses closeCh to unblock if the DB is closing while we wait.
	select {
	case db.readerSem <- struct{}{}:
	case <-db.closeCh:
		return nil, ErrClosed
	}

	db.mu.RLock()
	if db.closing.Load() {
		db.mu.RUnlock()
		<-db.readerSem
		return nil, ErrClosed
	}

	hdr, maxFrame, slot, err := db.pager.beginReadHdr()
	if err != nil {
		db.mu.RUnlock()
		<-db.readerSem
		return nil, err
	}

	localFCC := db.localFileChangeCounter.Load()
	localSC := db.localSchemaCookie.Load()
	fcc := localFCC
	sc := localSC
	if readCounters {
		// Read on-disk counters for staleness detection.
		fcc, sc, err = db.pager.readHeaderCounters(maxFrame)
		if err != nil {
			db.pager.endRead(slot)
			db.mu.RUnlock()
			<-db.readerSem
			return nil, err
		}
	}

	// Allocate reader cache from pool for per-connection page caching.
	// Persistent cache: keep cached pages only if the DB snapshot hasn't
	// changed. We check dataVersion (monotonic, never wraps) to detect
	// writes. walMaxFrame alone suffers ABA after checkpoint restart.
	// Matches SQLite pager.c:3246-3267 (pagerBeginReadTransaction —
	// pager_reset only if change-counter changed).
	curDV := db.dataVersion.Load()
	// Snapshot DB page count for this reader (hdr.nPage, fallback p.dbSize).
	// Carried on the cache so reader bound checks accept pages a peer
	// allocated after open. Set in lockstep with walMaxFrame.
	snapDbSize := db.pager.effectiveReaderDbSize(hdr)
	var cache *pcache
	select {
	case cache = <-db.readerCaches:
		if cache.dataVersion != curDV || cache.walMaxFrame != maxFrame {
			cache.clear()
			cache.dataVersion = curDV
			cache.walMaxFrame = maxFrame
		}
		cache.dbSize = snapDbSize
	default:
		cache = newPcache(int(db.pager.pageSize), db.readerCacheSize, true)
		cache.useSlab = db.pager.useSlab
		cache.dataVersion = curDV
		cache.walMaxFrame = maxFrame
		cache.dbSize = snapDbSize
	}

	tx := db.getReadTx()
	tx.db = db
	tx.pager = db.pager
	tx.cache = cache
	tx.closed = false
	tx.writable = false
	// Dual-write during per-connection-hdr migration. Step 5 will remove
	// walMaxFrame; until then, tests still read it directly.
	tx.walMaxFrame = maxFrame
	if hdr.isInit != 0 {
		tx.walHdr = hdr
	} else {
		tx.walHdr = WalIndexHdr{isInit: 1, mxFrame: maxFrame}
	}
	tx.walSlot = slot
	tx.diskFileChangeCounter = fcc
	tx.diskSchemaCookie = sc
	tx.localFileChangeCounter = localFCC
	tx.localSchemaCookie = localSC
	return tx, nil
}

// BeginRead starts a read-only transaction.
// DRIFT: BeginReadFast skips page-1 counter read; staleness APIs return stale/false See docs/btree/NOTES.md#drift-45-beginreadfast-skips-page-1-staleness-counter-reads
func (db *DB) BeginRead() (*ReadTx, error) {
	return db.beginRead(true)
}

// BeginReadFast starts a read-only transaction without reading page-1 counters.
// It preserves snapshot isolation for data access, but skips staleness metadata.
// DRIFT: BeginReadFast skips page-1 counter read; staleness APIs return stale/false See docs/btree/NOTES.md#drift-45-beginreadfast-skips-page-1-staleness-counter-reads
func (db *DB) BeginReadFast() (*ReadTx, error) {
	return db.beginRead(false)
}

// BeginWrite starts a read-write transaction. Only one write transaction
// can be active at a time (single-writer semantics). Blocks until any
// existing write transaction completes.
//
// busySnapshotInnerRetries is the tight-loop budget inside BeginWrite
// for the BUSY_SNAPSHOT race (wal.c:3714 SQLITE_BUSY_SNAPSHOT). Most
// races resolve in 1-2 attempts (another writer just committed, we
// re-read and proceed). If we exceed this budget, we hand off to the
// wal busyHandler for the standard delays[]/totals[] backoff
// (DefaultBusyTimeout, matching SQLite's sqliteDefaultBusyCallback in
// main.c:1717). After busyHandler exhausts its budget, ErrBusySnapshot
// surfaces to the caller — matching SQLite's caller contract.
const busySnapshotInnerRetries = 3

func (db *DB) BeginWrite() (*WriteTx, error) {
	if db.closing.Load() {
		return nil, ErrClosed
	}
	db.writeMu.Lock()
	if db.closing.Load() {
		db.writeMu.Unlock()
		return nil, ErrClosed
	}
	db.mu.RLock()
	if db.closing.Load() {
		db.mu.RUnlock()
		db.writeMu.Unlock()
		return nil, ErrClosed
	}

	var maxFrame uint32
	var slot int
	var readSnap WalIndexHdr

	for attempt := 0; ; attempt++ {
		var err error
		readSnap, maxFrame, slot, err = db.pager.beginReadHdr()
		if err != nil {
			if errors.Is(err, ErrBusyRecovery) {
				xBusy := db.pager.wal.busyHandler
				if xBusy != nil && xBusy(attempt) {
					continue
				}
			}
			db.mu.RUnlock()
			db.writeMu.Unlock()
			return nil, err
		}

		// Store WAL slot on pager BEFORE beginWrite() so that Close can
		// read it after observing pagerWriter via the atomic state store
		// inside beginWrite() (happens-before guarantee).
		db.pager.writerWalSlot = slot

		err = db.pager.beginWrite(readSnap)
		if err == nil {
			break
		}
		db.pager.endRead(slot)
		if !errors.Is(err, ErrBusySnapshot) {
			db.mu.RUnlock()
			db.writeMu.Unlock()
			return nil, err
		}
		// Clear writerCache: another process committed, our cached pages are stale
		db.pager.writerCache.clear()
		// After a small tight-retry budget, route through the busy handler so
		// we back off instead of spinning. When busyHandler returns false
		// (budget exhausted) or is nil, surface ErrBusySnapshot to the caller.
		if attempt >= busySnapshotInnerRetries {
			xBusy := db.pager.wal.busyHandler
			if xBusy == nil || !xBusy(attempt-busySnapshotInnerRetries) {
				db.mu.RUnlock()
				db.writeMu.Unlock()
				return nil, ErrBusySnapshot
			}
		}
	}

	// Read the committed page-1 counters for staleness detection from the
	// in-memory header instead of re-reading page 1 from the WAL on every
	// write tx. After beginWrite, p.header is authoritative: beginWrite
	// invokes refreshHeaderFromPage1 on the exact stateChanged edge
	// (wal.beginWriteWithSnapshot: live SHM hdr != writerHdr) that signals a
	// peer process committed or checkpointed — the same WAL-index-change edge
	// SQLite uses to pager_reset and re-read page 1 (pager.c:3246-3267
	// pagerBeginReadTransaction). When stateChanged is false the SHM header
	// equals what this process last wrote, so the on-disk counters are
	// unchanged and p.header still matches. SQLite likewise keeps the header
	// in the in-memory page-1 PgHdr and does not re-read it from the WAL per
	// statement. The staleness signal (diskFileChangeCounter vs
	// localFileChangeCounter, consumed by checkStale → IsDataStale) is
	// therefore preserved on the identical invalidation edge.
	fcc, sc := db.pager.committedCounters()

	// Reset the cleanup guard for this write transaction.
	db.writerLocksDone.Store(false)

	tx := db.getWriteTx()
	tx.ReadTx.db = db
	tx.ReadTx.pager = db.pager
	tx.ReadTx.cache = nil // writer uses shared pcache, not a reader cache
	tx.ReadTx.closed = false
	tx.ReadTx.walMaxFrame = maxFrame
	// Reuse the readSnap hdr captured above for BUSY_SNAPSHOT. In-process
	// mode has readSnap=zero; synthesize minimal hdr so read paths consuming
	// walHdr.mxFrame see the correct frame ceiling.
	if readSnap.isInit != 0 {
		tx.ReadTx.walHdr = readSnap
	} else {
		tx.ReadTx.walHdr = WalIndexHdr{isInit: 1, mxFrame: maxFrame}
	}
	tx.ReadTx.walSlot = slot
	tx.ReadTx.writable = true
	tx.ReadTx.diskFileChangeCounter = fcc
	tx.ReadTx.diskSchemaCookie = sc
	tx.ReadTx.localFileChangeCounter = db.localFileChangeCounter.Load()
	tx.ReadTx.localSchemaCookie = db.localSchemaCookie.Load()
	tx.dataChanged = false   // pool reuse safety
	tx.schemaChanged = false // pool reuse safety
	return tx, nil
}

func (db *DB) getReadTx() *ReadTx {
	if tx, ok := db.readTxPool.Get().(*ReadTx); ok {
		return tx
	}
	return &ReadTx{}
}

func (db *DB) putReadTx(tx *ReadTx) {
	db.readTxPool.Put(tx)
}

func (db *DB) getWriteTx() *WriteTx {
	if tx, ok := db.writeTxPool.Get().(*WriteTx); ok {
		return tx
	}
	return &WriteTx{}
}

func (db *DB) putWriteTx(tx *WriteTx) {
	db.writeTxPool.Put(tx)
}

// WriterCacheLen returns the number of pages in the writer cache (test helper).
func (db *DB) WriterCacheLen() int {
	return db.pager.writerCache.nPage
}

// Checkpoint triggers a WAL checkpoint with the specified mode, writing
// committed WAL frames back to the database file.
// DRIFT: DB.Checkpoint omits SQLite's same-handle open-tx SQLITE_LOCKED guard See docs/btree/NOTES.md#drift-47-checkpoint-omits-open-transaction-guard
// DRIFT: DB.Checkpoint drops pnLog/pnCkpt out-parameters See docs/btree/NOTES.md#drift-48-checkpoint-drops-frame-count-out-parameters
func (db *DB) Checkpoint(mode CheckpointMode) error {
	if db.closing.Load() {
		return ErrClosed
	}
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closing.Load() {
		return ErrClosed
	}
	return db.pager.checkpointWithMode(mode)
}

// UpdateLocalCounters manually sets the local counter cache. This is used by
// a process that has detected staleness and rebuilt its in-memory state: after
// rebuilding, call this to record the new baseline so subsequent transactions
// no longer report as stale. Uses atomic stores, safe to call concurrently.
func (db *DB) UpdateLocalCounters(fileChangeCounter, schemaCookie uint32) {
	db.localFileChangeCounter.Store(fileChangeCounter)
	db.localSchemaCookie.Store(schemaCookie)
}

// CreateNamespace creates a new namespace. Must be called within a write transaction.
func (db *DB) CreateNamespace(tx *WriteTx, name string) error {
	if tx.closed {
		return ErrTxClosed
	}

	// Check if namespace already exists using proper tree traversal
	// (page 1 may be an interior node after splits).
	// Uses getNamespaceLocked which sees dirty pages from the current write tx.
	_, err := db.getNamespaceLocked(name)
	if err == nil {
		return ErrNamespaceExists
	}
	if !errors.Is(err, ErrNamespaceNotFound) {
		return err
	}

	// Allocate a new page for the namespace's B-tree root
	rootPg, err := db.pager.allocatePage()
	if err != nil {
		return err
	}

	// Initialize as empty leaf page
	hdrOff := 0
	rootPg.header.pageType = pageTypeLeafIdx
	rootPg.header.cellCount = 0
	rootPg.header.cellContentOff = uint16(db.pager.usableSize())
	rootPg.header.serialize(rootPg.data[hdrOff:])
	db.pager.releasePage(rootPg)

	// Store namespace -> root page mapping in master table using proper
	// btree Put which handles multi-level trees and splits correctly.
	var rootPgBuf [4]byte
	binary.BigEndian.PutUint32(rootPgBuf[:], rootPg.pgno)

	bt := &btree{pager: db.pager, rootPage: 1, walMaxFrame: tx.walHdr.mxFrame, writable: true}
	return bt.Put([]byte(name), rootPgBuf[:])
}

// DeleteNamespace deletes a namespace. Must be called within a write transaction.
// All pages belonging to the namespace's B-tree are freed to the freelist.
func (db *DB) DeleteNamespace(tx *WriteTx, name string) error {
	if tx.closed {
		return ErrTxClosed
	}

	// Look up the namespace's root page using proper tree traversal
	// (page 1 may be an interior node after splits).
	// Uses getNamespaceLocked which sees dirty pages from the current write tx.
	ns, err := db.getNamespaceLocked(name)
	if err != nil {
		return err
	}
	rootPage := ns.rootPage

	// Delete namespace entry from master table
	bt := &btree{pager: db.pager, rootPage: 1, walMaxFrame: tx.walHdr.mxFrame, writable: true}
	if err := bt.Delete([]byte(name)); err != nil {
		return err
	}

	// Free all pages in the namespace's B-tree
	if rootPage != 0 {
		return db.freeTreePages(rootPage)
	}
	return nil
}

// maxTreeDepth bounds freeTreePages recursion. It is a conservative cap far
// above any real B-tree height for this page size; exceeding it implies a
// cyclic/self-referencing child pointer and is treated as corruption. This is
// the practical stand-in for clearDatabasePage's pager-refcount guard
// (btree.c:10233-10238), which any-store does not port verbatim (its single
// writer goroutine has a different ref/pin model than SQLite's shared cache).
const maxTreeDepth = 64

// freeTreePages recursively frees all pages in a B-tree, including the root
// and any overflow page chains attached to leaf cells. Its only caller is
// DeleteNamespace, which also removes the namespace's master-table entry, so
// this implements drop-table semantics (root reclaimed) matching SQLite's
// sqlite3BtreeDropTable (btree.c:10331), not clear-table (root retained).
func (db *DB) freeTreePages(pgno uint32) error {
	return db.freeTreePagesDepth(pgno, 0)
}

// freeTreePagesDepth is the recursive worker for freeTreePages. The depth
// parameter bounds recursion against a cyclic child pointer.
func (db *DB) freeTreePagesDepth(pgno uint32, depth int) error {
	// Up-front page-count guard, mirroring clearDatabasePage's
	// `if(pgno>btreePagecount(pBt)) return SQLITE_CORRUPT_PGNO` (btree.c:10228).
	// Catching an out-of-range child up front matches C and avoids touching a
	// zero-filled buffer (getPageWriter zero-fills pages within dbSize).
	if pgno == 0 || pgno > db.pager.dbSize.Load() {
		return ErrCorrupt
	}
	// Bound recursion to prevent infinite recursion / stack overflow on a
	// cyclic child pointer (stand-in for the C refcount guard at
	// btree.c:10233-10238).
	if depth > maxTreeDepth {
		return ErrCorrupt
	}

	pg, err := db.pager.getPage(pgno)
	if err != nil {
		return err
	}

	if pg.header.isInterior() {
		// Collect child page numbers before freeing
		usableSize := db.pager.usableSize()
		n := int(pg.header.cellCount)
		cpOff := pg.cellPointerOffset()
		children := make([]uint32, 0, n+1)
		var overflowHeads []uint32
		for i := range n {
			// Bounds-check the interior child-pointer read: a corrupt 16-bit
			// cell offset near page end must yield ErrCorrupt, not a panic in
			// the writer goroutine (C equivalent: btreeInitPage validation via
			// getAndInitPage, btree.c:10231).
			off, oerr := pg.getCellOffsetSafe(i)
			if oerr != nil {
				db.pager.releasePage(pg)
				return oerr
			}
			if int(off) < cpOff+(n*2) || int(off)+4 > len(pg.data) {
				db.pager.releasePage(pg)
				return ErrCorrupt
			}
			childPgno := binary.BigEndian.Uint32(pg.data[off : off+4])
			children = append(children, childPgno)
			// Free the overflow chain hanging off this interior divider cell.
			// clearDatabasePage runs BTREE_CLEAR_CELL on EVERY cell including
			// interior dividers (btree.c:10240-10248); the macro frees the
			// cell's overflow chain whenever the payload spills off-page
			// (btree.c:7056-7062). Collect the chain heads while the page is
			// held; free them after releasing the page.
			c, _, perr := parseInteriorCell(pg.data, int(off), usableSize)
			if perr != nil {
				db.pager.releasePage(pg)
				return perr
			}
			if c.overflowPg != 0 {
				overflowHeads = append(overflowHeads, c.overflowPg)
			}
		}
		children = append(children, pg.header.rightChild)
		db.pager.releasePage(pg)

		// Free the divider-cell overflow chains collected above.
		for _, ovfl := range overflowHeads {
			if err := db.pager.freeOverflowChain(ovfl); err != nil {
				return err
			}
		}

		// Recurse into children first (free leaves before interior)
		for _, child := range children {
			if err := db.freeTreePagesDepth(child, depth+1); err != nil {
				return err
			}
		}
	} else {
		// Leaf page: free any overflow chains
		usableSize := db.pager.usableSize()
		n := int(pg.header.cellCount)
		for i := range n {
			off := pg.getCellOffset(i)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				db.pager.releasePage(pg)
				return cerr
			}
			if cell.overflowPg != 0 {
				db.pager.releasePage(pg)
				if err := db.pager.freeOverflowChain(cell.overflowPg); err != nil {
					return err
				}
				// Re-get page since we released it
				pg, err = db.pager.getPage(pgno)
				if err != nil {
					return err
				}
			}
		}
		db.pager.releasePage(pg)
	}

	// Free this page
	return db.pager.freePage(pgno)
}

// GetNamespace returns a Namespace handle for the given name.
// Always uses the reader path with snapshot isolation. Safe to call from any
// goroutine. To resolve namespaces from the writer goroutine (seeing dirty
// pages), use getNamespaceLocked directly.
func (db *DB) GetNamespace(name string) (*Namespace, error) {
	hdr, maxFrame, slot, err := db.pager.beginReadHdr()
	if err != nil {
		return nil, err
	}
	defer db.pager.endRead(slot)

	cache := newPcache(int(db.pager.pageSize), 200, true)
	cache.useSlab = db.pager.useSlab
	cache.dbSize = db.pager.effectiveReaderDbSize(hdr)
	defer cache.destroy()

	return db.getNamespaceAt(name, maxFrame, cache)
}

// getNamespaceLocked returns a Namespace handle (caller must hold read lock).
// Uses pager.getPage which reads from the writer cache — safe only when called
// from the writer goroutine (e.g. WriteTx.CreateNamespace after modifying page 1).
func (db *DB) getNamespaceLocked(name string) (*Namespace, error) {
	bt := &btree{pager: db.pager, rootPage: 1, writable: true}
	return db.resolveNamespace(name, bt)
}

// getNamespaceAt returns a Namespace handle using snapshot isolation.
// When cache is non-nil, pages are cached in the reader's private cache.
// When cache is nil, falls back to uncached reads.
// Safe to call from any goroutine (readers or writer).
func (db *DB) getNamespaceAt(name string, walMaxFrame uint32, cache *pcache) (*Namespace, error) {
	bt := &btree{pager: db.pager, cache: cache, rootPage: 1, walMaxFrame: walMaxFrame, writable: false}
	return db.resolveNamespace(name, bt)
}

// resolveNamespace searches the master table btree for the given namespace
// and returns a Namespace handle. Uses proper tree traversal that works
// whether page 1 is a leaf or interior node.
func (db *DB) resolveNamespace(name string, bt *btree) (*Namespace, error) {
	nameKey := []byte(name)

	// Search through the master btree (handles multi-level trees).
	pg, err := bt.getPage(bt.rootPage)
	if err != nil {
		return nil, err
	}

	usableSize := bt.usablePageSize()
	for {
		if pg.header.isLeaf() {
			idx, found, serr := searchLeafWithOverflow(pg, nameKey, usableSize, bt.pager, bt.walMaxFrame, bt.cache)
			if serr != nil {
				bt.pager.releasePage(pg)
				return nil, serr
			}
			if !found {
				bt.pager.releasePage(pg)
				return nil, ErrNamespaceNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				bt.pager.releasePage(pg)
				return nil, cerr
			}
			if len(cell.value) < 4 {
				bt.pager.releasePage(pg)
				return nil, ErrCorrupt
			}
			rootPage := binary.BigEndian.Uint32(cell.value)
			bt.pager.releasePage(pg)
			return &Namespace{
				name:     name,
				rootPage: rootPage,
				db:       db,
			}, nil
		}

		// Interior page — descend to the correct child
		childPgno, _, serr := searchInteriorWithOverflow(pg, nameKey, usableSize, bt.pager, bt.walMaxFrame, bt.cache)
		if serr != nil {
			bt.pager.releasePage(pg)
			return nil, serr
		}
		bt.pager.releasePage(pg)
		pg, err = bt.descendChild(childPgno)
		if err != nil {
			return nil, err
		}
	}
}

// ListNamespaces returns the names of all namespaces.
func (db *DB) ListNamespaces() ([]string, error) {
	hdr, maxFrame, slot, err := db.pager.beginReadHdr()
	if err != nil {
		return nil, err
	}
	defer db.pager.endRead(slot)

	cache := newPcache(int(db.pager.pageSize), 200, true)
	cache.useSlab = db.pager.useSlab
	cache.dbSize = db.pager.effectiveReaderDbSize(hdr)
	defer cache.destroy()

	bt := &btree{pager: db.pager, cache: cache, rootPage: 1, walMaxFrame: maxFrame, writable: false}
	cursor := bt.NewCursor()
	defer cursor.Close()

	if err := cursor.First(); err != nil {
		return nil, nil // empty master table
	}

	var names []string
	for cursor.Valid() {
		key, err := cursor.Key()
		if err != nil {
			return nil, err
		}
		names = append(names, string(key))
		if err := cursor.Next(); err != nil {
			return nil, err
		}
	}
	return names, nil
}

// Namespace represents a named key-value namespace (separate B-tree).
type Namespace struct {
	db       *DB
	name     string
	rootPage uint32
}

// Name returns the namespace name.
func (ns *Namespace) Name() string {
	return ns.name
}

// RootPage returns the root page number (for internal use).
func (ns *Namespace) RootPage() uint32 {
	return ns.rootPage
}

// ReadTx is a read-only transaction.
type ReadTx struct {
	db          *DB
	pager       *pager
	cache       *pcache // per-reader private page cache (nil for write transactions)
	walSlot     int     // reader slot number (for endRead)
	walMaxFrame uint32  // WAL snapshot for this transaction (TODO: migrate to walHdr.mxFrame)
	// walHdr is the full SHM header snapshot captured at BeginRead time.
	// Populated in parallel with walMaxFrame during the per-connection-hdr
	// migration (see docs/superpowers/specs/2026-04-18-per-connection-hdr-design.md).
	// Not yet consumed by production code.
	walHdr WalIndexHdr

	// Disk counters from page 1 at transaction start (for staleness detection).
	diskFileChangeCounter  uint32
	diskSchemaCookie       uint32
	localFileChangeCounter uint32 // snapshot of DB's local value
	localSchemaCookie      uint32 // snapshot of DB's local value
	closed                 bool
	writable               bool // true when embedded in a WriteTx (MVCC: allows seeing dirty pages)
}

// WalMaxFrame returns the reader's snapshot mxFrame. Prefer this over
// direct walMaxFrame field access — the field is being migrated to
// walHdr.mxFrame per the per-connection-hdr spec.
func (tx *ReadTx) WalMaxFrame() uint32 { return tx.walHdr.mxFrame }

// txGetPage fetches a page respecting MVCC snapshot isolation.
// For write transactions, pages are fetched from the writer cache or WAL.
// For read transactions, getPageReader uses a private cache for snapshot isolation.
func (tx *ReadTx) txGetPage(pgno uint32) (*page, error) {
	if tx.writable {
		// Use pager.getPage which uses wal.nFrame (not the frozen
		// tx.walHdr.mxFrame) so the writer sees its own spilled pages.
		return tx.pager.getPage(pgno)
	}
	return tx.pager.getPageReader(pgno, tx.walHdr.mxFrame, tx.cache)
}

// txDescendChild fetches a child page reached from an interior cell during a
// cursor-free descent, after rejecting an out-of-range pgno as corruption.
// ~ C's getAndInitPage upfront guard `if( pgno>btreePagecount(pBt) ) return
// SQLITE_CORRUPT_BKPT` (btree.c:2396-2399). The pager getters keep zero-filling
// above-bound pages (drift-4); this guard lives at the descent call site so a
// wild or freed child pointer fails fast with ErrCorrupt instead of descending
// into a fabricated zero page. The bound is the snapshot page count: the
// writer's live p.dbSize (cache==nil) or the reader cache's frozen snapshot
// bound, exactly as readerDbSizeBound resolves.
func (tx *ReadTx) txDescendChild(childPgno uint32) (*page, error) {
	if childPgno == 0 || childPgno > tx.pager.readerDbSizeBound(tx.cache) {
		return nil, ErrCorrupt
	}
	return tx.txGetPage(childPgno)
}

// readOverflow reads overflow chain data using the correct isolation level.
// Writers use the shared cache (to see their own dirty pages).
// Readers bypass the cache to avoid polluting it with stale snapshot data
// that the writer could later read, causing on-disk corruption.
func (tx *ReadTx) readOverflow(firstPgno uint32, buf []byte) error {
	if tx.writable {
		return tx.pager.readOverflowChainAt(firstPgno, buf, tx.walHdr.mxFrame)
	}
	return tx.pager.readOverflowChainReader(firstPgno, buf, tx.walHdr.mxFrame, tx.cache)
}

// AppendValue retrieves a value by key from the given namespace, appending it to buf.
// Pass nil for buf to allocate a new slice (equivalent to Get).
func (tx *ReadTx) AppendValue(ns *Namespace, key []byte, buf []byte) ([]byte, error) {
	if tx.closed {
		return buf, ErrTxClosed
	}
	pg, err := tx.txGetPage(ns.rootPage)
	if err != nil {
		return buf, err
	}

	// Search without starting a new read tx (we're already in one)
	usableSize := tx.pager.usableSize()
	for {
		if pg.header.isLeaf() {
			idx, found, serr := searchLeafWithOverflow(pg, key, usableSize, tx.pager, tx.walHdr.mxFrame, tx.cache)
			if serr != nil {
				tx.pager.releasePage(pg)
				return buf, serr
			}
			if !found {
				tx.pager.releasePage(pg)
				return buf, ErrKeyNotFound
			}
			off := pg.getCellOffset(idx)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				tx.pager.releasePage(pg)
				return buf, cerr
			}
			if cell.overflowPg != 0 {
				// Unified payload format: read keyLen, valLen, compute nLocal
				pos := int(off)
				keyLen, kn, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					tx.pager.releasePage(pg)
					return buf, ErrCorrupt
				}
				pos += kn
				valLen, _, verr := getVarintSafe(pg.data[pos:])
				if verr != nil {
					tx.pager.releasePage(pg)
					return buf, ErrCorrupt
				}

				totalPayload := int(keyLen) + int(valLen)
				nLocal := localPayloadSize(totalPayload, usableSize)
				localKeyBytes := min(nLocal, int(keyLen))
				localValBytes := nLocal - localKeyBytes
				keyOverflow := int(keyLen) - localKeyBytes
				valOverflow := int(valLen) - localValBytes

				// Grow buf to hold the full value — no intermediate allocations.
				start := len(buf)
				buf = slices.Grow(buf, int(valLen))[:start+int(valLen)]
				fullVal := buf[start:]

				// Copy local value portion from the page
				if localValBytes > 0 {
					copy(fullVal, cell.value)
				}

				// Read overflow value bytes directly into destination,
				// skipping key overflow. Uses readOverflowAt to avoid
				// allocating an intermediate overflow buffer.
				if valOverflow > 0 {
					if err := tx.pager.readOverflowAt(
						cell.overflowPg, keyOverflow, valOverflow,
						fullVal[localValBytes:], tx.walHdr.mxFrame, tx.cache,
					); err != nil {
						tx.pager.releasePage(pg)
						return buf[:start], err
					}
				}
				tx.pager.releasePage(pg)
				return buf, nil
			}
			buf = append(buf, cell.value...)
			tx.pager.releasePage(pg)
			return buf, nil
		}
		childPgno, _, serr := searchInteriorWithOverflow(pg, key, usableSize, tx.pager, tx.walHdr.mxFrame, tx.cache)
		if serr != nil {
			tx.pager.releasePage(pg)
			return buf, serr
		}
		tx.pager.releasePage(pg)
		pg, err = tx.txDescendChild(childPgno)
		if err != nil {
			return buf, err
		}
	}
}

// Get retrieves a value by key from the given namespace.
// The returned slice is a copy and is safe to retain after pages are released.
func (tx *ReadTx) Get(ns *Namespace, key []byte) ([]byte, error) {
	return tx.AppendValue(ns, key, nil)
}

// Has reports whether key exists in ns. Equivalent to tx.Get(ns, key)
// returning a non-ErrKeyNotFound result, but never reads the cell value
// — no parseLeafCellWithSize, no value-buffer allocation. Mirrors
// SQLite's sqlite3BtreeIndexMoveto + (*pRes==0) idiom (btree.c:6024),
// minus the cursor: we use the same cursor-free per-call descent
// established by AppendValue and AppendSeekKey.
func (tx *ReadTx) Has(ns *Namespace, key []byte) (bool, error) {
	if tx.closed {
		return false, ErrTxClosed
	}
	pg, err := tx.txGetPage(ns.rootPage)
	if err != nil {
		return false, err
	}

	usableSize := tx.pager.usableSize()
	for {
		if pg.header.isLeaf() {
			_, found, serr := searchLeafWithOverflow(
				pg, key, usableSize, tx.pager, tx.walHdr.mxFrame, tx.cache,
			)
			tx.pager.releasePage(pg)
			if serr != nil {
				return false, serr
			}
			return found, nil
		}
		childPgno, _, serr := searchInteriorWithOverflow(
			pg, key, usableSize, tx.pager, tx.walHdr.mxFrame, tx.cache,
		)
		if serr != nil {
			tx.pager.releasePage(pg)
			return false, serr
		}
		tx.pager.releasePage(pg)
		pg, err = tx.txDescendChild(childPgno)
		if err != nil {
			return false, err
		}
	}
}

// SeekKey finds the first key >= prefix in ns and returns it.
// Convenience wrapper around AppendSeekKey with a nil buffer.
// Returns ErrKeyNotFound if no key >= prefix exists.
func (tx *ReadTx) SeekKey(ns *Namespace, prefix []byte) ([]byte, error) {
	return tx.AppendSeekKey(ns, prefix, nil)
}

// AppendSeekKey finds the first key >= prefix in ns and appends it to buf.
// This is a single-shot traversal without cursor allocation, modeled after
// AppendValue but returning the key at the seek position instead of the value.
// Returns ErrKeyNotFound if no key >= prefix exists.
func (tx *ReadTx) AppendSeekKey(ns *Namespace, prefix []byte, buf []byte) ([]byte, error) {
	if tx.closed {
		return buf, ErrTxClosed
	}
	pg, err := tx.txGetPage(ns.rootPage)
	if err != nil {
		return buf, err
	}

	usableSize := tx.pager.usableSize()
	cache := tx.cache // nil for writers, per-connection pcache for readers
	rightmost := true // tracks if we're on the rightmost path through the tree

	// When we take a non-rightChild branch at an interior page, we save that
	// page number and cell index. If the leaf search overshoots (idx >= n),
	// we re-read this interior page and descend into the next sibling subtree
	// to find its leftmost key — no cursor allocation needed.
	var fallbackPgno uint32
	var fallbackIdx int

	for {
		if pg.header.isLeaf() {
			idx, _, serr := searchLeafWithOverflow(pg, prefix, usableSize, tx.pager, tx.walHdr.mxFrame, cache)
			if serr != nil {
				tx.pager.releasePage(pg)
				return buf, serr
			}
			n := int(pg.header.cellCount)
			if idx >= n {
				tx.pager.releasePage(pg)
				if rightmost {
					// On the rightmost leaf with prefix > all keys:
					// no key >= prefix exists anywhere in the tree.
					return buf, ErrKeyNotFound
				}
				// Non-rightmost leaf: navigate to the leftmost key of
				// the next sibling subtree (saved during descent).
				return tx.leftmostKeyAfter(fallbackPgno, fallbackIdx, buf)
			}
			off := pg.getCellOffset(idx)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				tx.pager.releasePage(pg)
				return buf, cerr
			}
			if cell.overflowPg != 0 {
				fullKey, kerr := leafFullKey(pg.data, int(off), usableSize, tx.pager, tx.walHdr.mxFrame, cache)
				tx.pager.releasePage(pg)
				if kerr != nil {
					return buf, kerr
				}
				return append(buf, fullKey...), nil
			}
			buf = append(buf, cell.key...)
			tx.pager.releasePage(pg)
			return buf, nil
		}
		childPgno, cellIdx, serr := searchInteriorWithOverflow(pg, prefix, usableSize, tx.pager, tx.walHdr.mxFrame, cache)
		if serr != nil {
			tx.pager.releasePage(pg)
			return buf, serr
		}
		// Track the deepest non-rightChild branch for cross-page navigation.
		if childPgno != pg.header.rightChild {
			rightmost = false
			fallbackPgno = pg.pgno
			fallbackIdx = cellIdx
		}
		tx.pager.releasePage(pg)
		pg, err = tx.txDescendChild(childPgno)
		if err != nil {
			return buf, err
		}
	}
}

// leftmostKeyAfter navigates to the first key in the subtree immediately
// following cell[cellIdx].leftChild on interior page interiorPgno.
// Used when a leaf search overshoots and needs the next sibling's first key.
// DRIFT: descent omits moveToChild's per-child nCell>=1 corruption guard See docs/btree/NOTES.md#drift-11-movetochild-child-page-ncell-greater-than-equal-one-descent-
func (tx *ReadTx) leftmostKeyAfter(interiorPgno uint32, cellIdx int, buf []byte) ([]byte, error) {
	pg, err := tx.txGetPage(interiorPgno)
	if err != nil {
		return buf, err
	}

	// Find the next child pointer after cell[cellIdx].leftChild.
	n := int(pg.header.cellCount)
	var nextPgno uint32
	if cellIdx+1 < n {
		off, oerr := pg.getCellOffsetSafe(cellIdx + 1)
		if oerr != nil {
			tx.pager.releasePage(pg)
			return buf, oerr
		}
		if int(off)+4 > len(pg.data) {
			tx.pager.releasePage(pg)
			return buf, ErrCorrupt
		}
		nextPgno = binary.BigEndian.Uint32(pg.data[off : off+4])
	} else {
		nextPgno = pg.header.rightChild
	}
	tx.pager.releasePage(pg)

	// Descend to the leftmost key of this subtree.
	usableSize := tx.pager.usableSize()
	cache := tx.cache // nil for writers, per-connection pcache for readers
	pg, err = tx.txDescendChild(nextPgno)
	if err != nil {
		return buf, err
	}
	for {
		if pg.header.isLeaf() {
			if pg.header.cellCount == 0 {
				tx.pager.releasePage(pg)
				return buf, ErrKeyNotFound
			}
			off := pg.getCellOffset(0)
			cell, _, cerr := parseLeafCellWithSize(pg.data, int(off), usableSize)
			if cerr != nil {
				tx.pager.releasePage(pg)
				return buf, cerr
			}
			if cell.overflowPg != 0 {
				fullKey, kerr := leafFullKey(pg.data, int(off), usableSize, tx.pager, tx.walHdr.mxFrame, cache)
				tx.pager.releasePage(pg)
				if kerr != nil {
					return buf, kerr
				}
				return append(buf, fullKey...), nil
			}
			buf = append(buf, cell.key...)
			tx.pager.releasePage(pg)
			return buf, nil
		}
		// Interior page: descend to cell[0].leftChild.
		off, oerr := pg.getCellOffsetSafe(0)
		if oerr != nil {
			tx.pager.releasePage(pg)
			return buf, oerr
		}
		if int(off)+4 > len(pg.data) {
			tx.pager.releasePage(pg)
			return buf, ErrCorrupt
		}
		childPgno := binary.BigEndian.Uint32(pg.data[off : off+4])
		tx.pager.releasePage(pg)
		pg, err = tx.txDescendChild(childPgno)
		if err != nil {
			return buf, err
		}
	}
}

// NewCursor creates a cursor for iterating over the namespace.
func (tx *ReadTx) NewCursor(ns *Namespace) *Cursor {
	c := &Cursor{}
	c.btData = btree{pager: tx.pager, cache: tx.cache, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: tx.writable}
	c.bt = &c.btData
	return c
}

// Count returns the total number of key-value pairs in the namespace.
// This is a lightweight operation that only reads page headers without parsing cell data.
func (tx *ReadTx) Count(ns *Namespace) (int, error) {
	if tx.closed {
		return 0, ErrTxClosed
	}
	bt := &btree{pager: tx.pager, cache: tx.cache, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: tx.writable}
	return bt.Count()
}

// DatabaseSize returns the number of pages in the database, including page 1
// (the header page), as of this transaction's snapshot.
// ~ sqlite3BtreeLastPage / sqlite3PagerPagecount (btree.c:2371, pager.c:3925):
// SQLite returns the per-connection pPager->dbSize, which is set from the
// reader's captured WAL-index header (sqlite3WalDbsize == pWal->hdr.nPage,
// pager.c:5448). Here it is the per-reader snapshot bound carried on the
// reader cache (tx.cache.dbSize, set at BeginRead from effectiveReaderDbSize),
// so the count is consistent with the same snapshot used for page reads —
// not the live global writer counter that DB.DatabaseSize exposes.
//
// For a write transaction (embedded ReadTx, cache==nil), readerDbSizeBound
// falls back to p.dbSize, which is the writer's live page count and matches
// sqlite3BtreeLastPage's pBt->nPage for the writing connection.
func (tx *ReadTx) DatabaseSize() uint32 {
	return tx.pager.readerDbSizeBound(tx.cache)
}

// GetNamespace returns a Namespace handle for the given name.
// Uses the transaction's WAL snapshot via getPageReader with the reader's
// private cache. Safe to call concurrently with the writer goroutine.
func (tx *ReadTx) GetNamespace(name string) (*Namespace, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	return tx.db.getNamespaceAt(name, tx.walHdr.mxFrame, tx.cache)
}

// IsDataStale returns true if the on-disk FileChangeCount differs from the
// locally cached value, indicating another process has committed data changes
// since this DB last committed or synced counters.
func (tx *ReadTx) IsDataStale() bool {
	return tx.diskFileChangeCounter != tx.localFileChangeCounter
}

// IsSchemaStale returns true if the on-disk SchemaCookie differs from the
// locally cached value, indicating another process has committed schema changes.
func (tx *ReadTx) IsSchemaStale() bool {
	return tx.diskSchemaCookie != tx.localSchemaCookie
}

// DiskFileChangeCounter returns the FileChangeCount read from page 1 at
// the start of this transaction.
func (tx *ReadTx) DiskFileChangeCounter() uint32 {
	return tx.diskFileChangeCounter
}

// DiskSchemaCookie returns the SchemaCookie read from page 1 at the start
// of this transaction.
func (tx *ReadTx) DiskSchemaCookie() uint32 {
	return tx.diskSchemaCookie
}

// Rollback ends the read transaction (for ReadTx, this is the same as commit).
func (tx *ReadTx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	// Return the reader cache to the channel pool for reuse. Pages are kept
	// intact (persistent cache): the next BeginRead() will check dataVersion
	// and clear the cache only if a write committed since this transaction.
	// Uses a channel instead of sync.Pool so caches survive GC cycles.
	if tx.cache != nil {
		select {
		case tx.db.readerCaches <- tx.cache:
		default:
			// Pool full (shouldn't happen — capacity matches MaxReaders).
			tx.cache.destroy()
		}
		tx.cache = nil
	}
	tx.pager.endRead(tx.walSlot)
	db := tx.db
	db.mu.RUnlock()
	// Release reader semaphore after all other cleanup so that a new
	// BeginRead can proceed immediately.
	<-db.readerSem
	db.putReadTx(tx)
	return nil
}

// WriteTx is a read-write transaction.
type WriteTx struct {
	ReadTx
	dataChanged   bool // set by MarkDataChanged; causes FileChangeCount++ on commit
	schemaChanged bool // set by MarkSchemaChanged; causes SchemaCookie++ on commit
}

// GetNamespace returns a Namespace handle for the given name.
// Uses pager.getPage which sees dirty pages from the current write transaction.
func (tx *WriteTx) GetNamespace(name string) (*Namespace, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	return tx.db.getNamespaceLocked(name)
}

// MarkDataChanged signals that this transaction modifies data, causing
// FileChangeCount to be incremented on commit.
func (tx *WriteTx) MarkDataChanged() {
	tx.dataChanged = true
}

// MarkSchemaChanged signals that this transaction modifies schema, causing
// SchemaCookie to be incremented on commit.
func (tx *WriteTx) MarkSchemaChanged() {
	tx.schemaChanged = true
}

// Put inserts or updates a key-value pair in the given namespace.
func (tx *WriteTx) Put(ns *Namespace, key, value []byte) error {
	if tx.closed {
		return ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: true}
	return bt.Put(key, value)
}

// Delete removes a key from the given namespace.
func (tx *WriteTx) Delete(ns *Namespace, key []byte) error {
	if tx.closed {
		return ErrTxClosed
	}
	bt := &btree{pager: tx.pager, rootPage: ns.rootPage, walMaxFrame: tx.walHdr.mxFrame, writable: true}
	return bt.Delete(key)
}

// AutoCheckpointThreshold is the default number of WAL frames after which
// an automatic passive checkpoint is triggered.
var AutoCheckpointThreshold = 10000

// Commit commits the transaction, writing all changes to the WAL.
func (tx *WriteTx) Commit() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	nFrame, newFCC, newSC, err := tx.pager.commit(tx.dataChanged, tx.schemaChanged)
	if err == nil {
		tx.db.localFileChangeCounter.Store(newFCC)
		tx.db.localSchemaCookie.Store(newSC)
		// Increment dataVersion so persistent reader caches detect staleness.
		// Unlike walMaxFrame, this counter never wraps after checkpoint restart.
		tx.db.dataVersion.Add(1)
	}
	threshold := tx.db.opts.AutoCheckpointAfter
	needCheckpoint := threshold > 0 && int(nFrame) >= threshold
	// Use CAS to ensure writer lock cleanup happens exactly once.
	// Close() may race with Commit, so both use writerLocksDone to coordinate.
	db := tx.db
	if db.writerLocksDone.CompareAndSwap(false, true) {
		tx.pager.endRead(tx.walSlot)

		// Auto-checkpoint before releasing db.mu.RLock to avoid deadlock with Close().
		// Checkpoint does NOT block readers — it only blocks new writers.
		// Errors are stored on db.lastAutoCheckpointErr instead of silently
		// discarded so monitoring code can observe them via
		// LastAutoCheckpointError().
		if err == nil && needCheckpoint {
			cpErr := tx.pager.tryCheckpoint()
			db.lastAutoCheckpointErr.Store(autoCheckpointResult{err: cpErr})
		}
		db.mu.RUnlock()
		db.writeMu.Unlock()
	}
	db.putWriteTx(tx)
	return err
}

// Rollback discards all changes in the transaction.
func (tx *WriteTx) Rollback() error {
	if tx.closed {
		return ErrTxClosed
	}
	tx.closed = true
	err := tx.pager.rollback()
	// Use CAS to ensure writer lock cleanup happens exactly once.
	// Close() may race with Rollback, so both use writerLocksDone to coordinate.
	db := tx.db
	if db.writerLocksDone.CompareAndSwap(false, true) {
		tx.pager.endRead(tx.walSlot)
		db.mu.RUnlock()
		db.writeMu.Unlock()
	}
	db.putWriteTx(tx)
	return err
}

// Savepoint creates a savepoint within the transaction.
// Returns a savepoint ID that can be used with RollbackToSavepoint or ReleaseSavepoint.
func (tx *WriteTx) Savepoint() (int, error) {
	if tx.closed {
		return 0, ErrTxClosed
	}
	return tx.pager.savepoint()
}

// RollbackToSavepoint rolls back all changes made since the given savepoint.
func (tx *WriteTx) RollbackToSavepoint(id int) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.pager.rollbackToSavepoint(id)
}

// ReleaseSavepoint releases a savepoint, merging its changes into the parent.
func (tx *WriteTx) ReleaseSavepoint(id int) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.pager.releaseSavepoint(id)
}

// CreateNamespace creates a new namespace within this transaction.
// DRIFT: CreateNamespace adds ErrNamespaceExists uniqueness pre-check not in C See docs/btree/NOTES.md#drift-59-createnamespace-uniqueness-pre-check-not-in-btreecreatetable
func (tx *WriteTx) CreateNamespace(name string) (*Namespace, error) {
	if tx.closed {
		return nil, ErrTxClosed
	}
	if err := tx.db.CreateNamespace(tx, name); err != nil {
		return nil, err
	}
	return tx.db.getNamespaceLocked(name)
}

// DeleteNamespace deletes a namespace within this transaction.
func (tx *WriteTx) DeleteNamespace(name string) error {
	if tx.closed {
		return ErrTxClosed
	}
	return tx.db.DeleteNamespace(tx, name)
}
