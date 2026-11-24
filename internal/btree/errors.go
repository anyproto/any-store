package btree

import (
	"errors"
	"fmt"
)

var (
	// ErrCorrupt indicates the database file is corrupted: damage that is in
	// the files and will be there at the next open. Callers discard and
	// rebuild a database on it, so no transient state may return it or an
	// error that wraps it: not an I/O error, a full disk, a cancelled
	// context, a busy lock, a wrong key or a closed handle.
	ErrCorrupt = errors.New("btree: database is corrupt")

	// ErrHeaderCorrupt indicates the file header is unusable: a file cut inside it or never a
	// database, bad field values, or a size that disagrees with the file. It wraps ErrCorrupt.
	ErrHeaderCorrupt = fmt.Errorf("%w: header", ErrCorrupt)

	// ErrWrongKey is returned (wrapped) by Open when the key does not open
	// the file: page 1 failed its authentication, and so did every other
	// page the open then tried under the same key (pager.proveKeyAtOpen).
	// Deliberately NOT wrapping ErrCorrupt or ErrPageIntegrity: nothing
	// shows the file to be damaged, and a caller that discards a corrupt
	// database must not discard one it only failed to unlock.
	ErrWrongKey = errors.New("btree: wrong encryption key")

	// ErrKeyRequired is returned by Open for a file that is encrypted when
	// the options carry neither Key nor Codec. It is decided from the
	// file's header alone, and the file is left unchanged. It does not wrap
	// ErrCorrupt.
	ErrKeyRequired = errors.New("btree: database file is encrypted, Options.Key or Options.Codec required")

	// ErrNotEncrypted is returned by Open for a file that is not encrypted
	// when the options carry Key or Codec. The file is left unchanged. It
	// does not wrap ErrCorrupt.
	ErrNotEncrypted = errors.New("btree: database file is not encrypted, remove Options.Key/Codec")

	// ErrSQLiteFormat reports a file carrying SQLite's header magic — an
	// any-store v1 database. Deliberately NOT wrapping ErrCorrupt: the file is
	// intact, only older, and a caller that responds to corruption by discarding
	// and rebuilding the database must not be told to throw it away.
	ErrSQLiteFormat = errors.New("btree: SQLite-format file (any-store v1 database)")

	// ErrReadOnly indicates a write was attempted on a read-only transaction.
	ErrReadOnly = errors.New("btree: read-only transaction")

	// ErrTxClosed indicates the transaction has already been closed.
	ErrTxClosed = errors.New("btree: transaction closed")

	// ErrKeyNotFound indicates the requested key was not found.
	ErrKeyNotFound = errors.New("btree: key not found")

	// ErrKeyExists indicates the key already exists (for unique insert).
	ErrKeyExists = errors.New("btree: key already exists")

	// ErrNamespaceNotFound indicates the namespace does not exist.
	ErrNamespaceNotFound = errors.New("btree: namespace not found")

	// ErrNamespaceExists indicates the namespace already exists.
	ErrNamespaceExists = errors.New("btree: namespace already exists")

	// ErrInvalidSavepoint indicates an invalid savepoint ID was used.
	ErrInvalidSavepoint = errors.New("btree: invalid savepoint")

	// ErrBusy indicates the database is locked by another writer.
	ErrBusy = errors.New("btree: database is busy")

	// ErrClosed indicates the database has been closed.
	ErrClosed = errors.New("btree: database is closed")

	// ErrReadersBusy: every reader slot is held (TryBeginRead).
	ErrReadersBusy = errors.New("btree: all reader slots are busy")

	// ErrFull indicates the database or page is full.
	ErrFull = errors.New("btree: database is full")

	// ErrInvalidPage indicates a page number is out of range or invalid.
	ErrInvalidPage = errors.New("btree: invalid page number")

	// ErrKeyTooLarge is returned by Put for a key longer than the record
	// size limit (1 GiB). Nothing is written.
	ErrKeyTooLarge = errors.New("btree: key too large")

	// ErrValueTooLarge is returned by Put when the key fits the record size
	// limit (1 GiB) and the key and the value together do not. Nothing is
	// written.
	ErrValueTooLarge = errors.New("btree: value too large")

	// ErrWALShortRead is the error of a read of a WAL frame that comes up
	// short: the wal-index points at a frame past the end of the log file,
	// as SQLite's read of it gives SQLITE_IOERR_SHORT_READ
	// (sqlite3WalReadFrame, wal.c:3649-3664; unixRead). A read returns it
	// wrapped with the read's own error (io.EOF): frameReadErr. Deliberately NOT
	// wrapping ErrCorrupt: the database file is not damaged, and the
	// wal-index is rebuilt from the log when every handle has closed it.
	ErrWALShortRead = errors.New("btree: WAL frame read came up short: the wal-index points past the end of the log")

	// ErrWALVersion indicates the WAL was written by an incompatible (newer)
	// any-store version. Unlike a WAL header that fails its checks
	// (errWalHeaderInvalid) it must never trigger recovery's start-fresh
	// truncation: the WAL holds committed transactions this binary cannot
	// read, and destroying it would lose them. Open fails instead; upgrade
	// the binary to proceed.
	ErrWALVersion = errors.New("btree: incompatible WAL version")

	// ErrOldFormat indicates the database was created with an older schema format
	// that is no longer supported. The database must be recreated.
	ErrOldFormat = errors.New("btree: unsupported old schema format (requires version 5+)")

	// ErrNewerFormat indicates the file was written in a format this binary does
	// not know: a header read version, write version or schema format above its
	// maximum. Deliberately NOT wrapping ErrCorrupt: the file is intact, and a
	// caller that discards a corrupt database must not discard this one. Open
	// fails without changing the file; a newer binary opens it.
	ErrNewerFormat = errors.New("btree: database format is newer than this version supports")

	// ErrBusySnapshot indicates another connection committed since this
	// transaction's snapshot was taken, so proceeding would violate
	// snapshot isolation. Equivalent to SQLite's SQLITE_BUSY_SNAPSHOT
	// (wal.c:3714). Surfaced to the caller after a short bounded in-line
	// retry (see db.go busySnapshotInnerRetries) and a BusyHandler backoff
	// pass. Callers should retry the whole transaction (BeginWrite → ... →
	// Commit), optionally with their own backoff.
	ErrBusySnapshot = errors.New("btree: busy snapshot")

	// ErrBusyRecovery indicates another connection is currently executing
	// WAL recovery (walIndexRecover). Equivalent to SQLite's
	// SQLITE_BUSY_RECOVERY (wal.c:3084-3108). Callers should back off
	// before retrying — recovery can take seconds on a large WAL and
	// tight-spinning just burns CPU and amplifies lock contention.
	ErrBusyRecovery = errors.New("btree: busy recovery")

	// ErrProtocol indicates the WAL retry protocol was exhausted.
	ErrProtocol = errors.New("btree: WAL protocol retry limit exhausted")

	// ErrDatabaseOpen indicates the database file is already open in this process.
	ErrDatabaseOpen = errors.New("btree: database already open in this process")

	// ErrInProcessLocked indicates the database file is held by a DIFFERENT
	// process. In-process mode uses heap-backed (process-local) WAL-index shared
	// memory, so it is single-process only; on open it takes an exclusive
	// whole-file lock and reports this error if another process already holds it.
	ErrInProcessLocked = errors.New("btree: database is open by another process (in-process mode is single-process only)")

	// ErrPageSlabNotInitialized indicates UsePageSlab was set but the global
	// page slab was not initialized via ConfigPageCache.
	ErrPageSlabNotInitialized = errors.New("btree: page slab not initialized (call ConfigPageCache first)")

	// ErrPageBufferPoolSizeMismatch indicates a database was opened with a page
	// size that differs from the page buffer pool's initialized size. All
	// databases in a process must use the same page size.
	ErrPageBufferPoolSizeMismatch = errors.New("btree: page buffer pool already initialized with a different page size")
)

// errLeafKeySpilled reports a leaf cell whose key continues in an overflow
// chain, so leafKeyAt cannot return it from the page alone; callers take the
// full-key path (leafFullKey) instead.
var errLeafKeySpilled = errors.New("btree: leaf key spills to an overflow chain")

// nestedError is an error whose text is msg and which matches parent under errors.Is, so a
// specific failure keeps its own message and still belongs to the broader kind.
func nestedError(msg string, parent error) error {
	return &nested{msg: msg, parent: parent}
}

type nested struct {
	msg    string
	parent error
}

func (e *nested) Error() string { return e.msg }
func (e *nested) Unwrap() error { return e.parent }
