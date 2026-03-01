package btree

import "errors"

var (
	// ErrCorrupt indicates the database file is corrupted.
	ErrCorrupt = errors.New("btree: database is corrupt")

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

	// ErrFull indicates the database or page is full.
	ErrFull = errors.New("btree: database is full")

	// ErrInvalidPage indicates a page number is out of range or invalid.
	ErrInvalidPage = errors.New("btree: invalid page number")

	// ErrKeyTooLarge indicates the key exceeds the maximum allowed size.
	ErrKeyTooLarge = errors.New("btree: key too large")

	// ErrValueTooLarge indicates the value exceeds maximum allowed size.
	ErrValueTooLarge = errors.New("btree: value too large")

	// ErrWALCorrupt indicates the WAL file is corrupted.
	ErrWALCorrupt = errors.New("btree: WAL is corrupt")

	// ErrOldFormat indicates the database was created with an older schema format
	// that is no longer supported. The database must be recreated.
	ErrOldFormat = errors.New("btree: unsupported old schema format (requires version 5+)")

	// ErrProtocol indicates the WAL retry protocol was exhausted.
	ErrProtocol = errors.New("btree: WAL protocol retry limit exhausted")

	// ErrDatabaseOpen indicates the database file is already open in this process.
	ErrDatabaseOpen = errors.New("btree: database already open in this process")
)
