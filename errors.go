package anystore

import (
	"errors"
	"fmt"

	"github.com/anyproto/any-store/v2/internal/aggregate"
	"github.com/anyproto/any-store/v2/internal/btree"
)

var (
	// ErrGroupLimitExceeded is returned when an aggregation $group stage
	// exceeds the configured maximum number of unique group keys
	// (AggQuery.GroupLimit).
	ErrGroupLimitExceeded = aggregate.ErrGroupLimitExceeded

	// ErrAccumArrayLimitExceeded is returned when a $push or $addToSet
	// accumulator exceeds the configured maximum array length
	// (AggQuery.AccumArrayLimit).
	ErrAccumArrayLimitExceeded = aggregate.ErrAccumArrayLimitExceeded

	// ErrAggMemoryLimitExceeded is returned when the blocking stages of an
	// aggregation pipeline retain more than the configured memory budget
	// (AggQuery.MemoryLimit).
	ErrAggMemoryLimitExceeded = aggregate.ErrMemoryLimitExceeded
)

var (
	// ErrDocExists is returned when attempting to insert a document that already exists.
	ErrDocExists = errors.New("any-store: document already exists")

	// ErrDocNotFound is returned when a document cannot be found by its ID.
	ErrDocNotFound = errors.New("any-store: document not found")

	// ErrDocWithoutId is returned when a document is provided without a required ID.
	ErrDocWithoutId = errors.New("any-store: document missing ID")

	// ErrArrayPrimaryKey is returned when a document's primary-key value is an
	// array. Array pks are rejected because filter semantics on arrays are
	// element-wise while the storage key is the whole-array encoding, so a doc
	// could match a pk filter yet sit outside the scanned key range. Files
	// written before this ban that contain array pks must be recreated.
	ErrArrayPrimaryKey = errors.New("any-store: array values are not allowed as primary key")

	// ErrPrimaryKeyMismatch is returned by Collection when the requested
	// PrimaryKey option conflicts with an existing collection's stored primary
	// key. The primary key is immutable after creation.
	ErrPrimaryKeyMismatch = errors.New("any-store: primary key mismatch")

	// ErrPrimaryKeyModification is returned when an update would change a
	// document's primary-key value. The primary key is immutable (Mongo
	// _id semantics): a $set on the id field is rejected rather than
	// leaving a ghost data record under the document's old key.
	ErrPrimaryKeyModification = errors.New("any-store: primary key modification is not allowed")

	// ErrCollectionExists is returned when attempting to create a collection that already exists.
	ErrCollectionExists = errors.New("any-store: collection already exists")

	// ErrInvalidCollectionName is returned when a collection name is empty or
	// collides with a reserved namespace family (see validateCollectionName).
	ErrInvalidCollectionName = errors.New("any-store: invalid collection name")

	// ErrCollectionNotFound is returned when a collection cannot be found.
	ErrCollectionNotFound = errors.New("any-store: collection not found")

	// ErrCollectionClosed is returned by operations on a collection handle
	// that is no longer live: explicitly closed, dropped, or invalidated
	// because another process renamed or dropped the collection (SQLite
	// re-prepare style — re-open the collection to continue). It wraps
	// ErrCollectionNotFound so errors.Is matches what a fresh OpenCollection
	// under the stale name returns.
	ErrCollectionClosed = fmt.Errorf("any-store: collection handle is closed (dropped, renamed, or closed): %w", ErrCollectionNotFound)

	// ErrCollectionClosedDuringOpen is returned by OpenCollection and
	// Collection when the collection was there at each of their attempts,
	// but the handle each attempt found was closed before it could be
	// returned, as by a Close() of it in another goroutine: an open returns
	// the handle the collection has, the same one to every caller. It does
	// not match ErrCollectionNotFound or ErrCollectionClosed, and Collection
	// has created nothing. Open again: the open finds the handle the
	// collection has then, or returns ErrCollectionNotFound if it is gone.
	ErrCollectionClosedDuringOpen = errors.New("any-store: collection handle closed during the open; open again")

	// ErrInvalidIndexName is returned when an index name (given or derived from
	// the field list) exceeds the maximum length (see validateIndexName).
	ErrInvalidIndexName = errors.New("any-store: invalid index name")

	// ErrIndexExists is returned when attempting to create an index that already exists.
	ErrIndexExists = errors.New("any-store: index already exists")

	// ErrIndexNotFound is returned when an index cannot be found.
	ErrIndexNotFound = errors.New("any-store: index not found")

	// ErrIndexMismatch is returned when an index with the requested name already
	// exists but with a different definition (different fields, unique, or sparse
	// flags). A redefinition is never applied silently: drop the existing index
	// first, then create it with the new definition.
	ErrIndexMismatch = errors.New("any-store: index exists with a different definition")

	// ErrNamespaceExists is returned when a collection or index name derives
	// a storage name that another collection's index already has. Collection
	// names may contain ":", so collection "A"'s index "b:c" and collection
	// "A:b"'s index "c" derive the same catalog key: creating the second of
	// the two, with CreateIndex or EnsureIndex and whatever its definition,
	// returns it (wrapped), and nothing is created.
	ErrNamespaceExists = btree.ErrNamespaceExists

	// ErrTxIsReadOnly is returned when a write operation is attempted in a read-only transaction.
	ErrTxIsReadOnly = errors.New("any-store: transaction is read-only")

	// ErrTxIsUsed is returned when an operation is attempted on a transaction that has already been committed or rolled back.
	ErrTxIsUsed = errors.New("any-store: transaction has already been used")

	// ErrTxOtherInstance is returned when an operation is attempted using a transaction from a different database instance.
	ErrTxOtherInstance = errors.New("any-store: transaction belongs to another database instance")
	// ErrTxConcurrentCalls is the panic value of a call made on a transaction
	// while a call from another goroutine is in progress on it; the check is
	// best effort.
	ErrTxConcurrentCalls = errors.New("any-store: concurrent calls on one transaction")
	// ErrTxEndInModifier refuses a Commit or Rollback of a transaction or
	// a savepoint made from inside a modifier (txHandle.enterEnd).
	ErrTxEndInModifier = errors.New("any-store: transaction ended from inside a modifier")
	// ErrSavepointInModifier refuses a savepoint opened from inside a
	// modifier (db.WriteTx with the transaction's context).
	ErrSavepointInModifier = errors.New("any-store: savepoint opened from inside a modifier")
	// ErrWriteInModifier refuses a write to, or a schema change of, a
	// collection from inside one of its own modifiers (collection.writable).
	ErrWriteInModifier = errors.New("any-store: collection written from inside its modifier")

	// ErrUniqueConstraint is returned when a unique constraint violation occurs.
	ErrUniqueConstraint = errors.New("any-store: unique constraint violation")

	// ErrIterClosed is returned when operations are attempted on a closed iterator.
	ErrIterClosed = errors.New("any-store: iterator is closed")

	// ErrIterRolledBack is the error of an iterator that was opened or moved
	// inside a savepoint's scope once that savepoint is rolled back: Next
	// returns false, Err and Doc report it. The pages it stood on went with
	// the rollback; the transaction goes on.
	ErrIterRolledBack = errors.New("any-store: iterator ended by a savepoint rollback")

	// ErrIterOpen refuses a Drop, DropIndex or CompactVectorIndex of a
	// collection while an iterator of the transaction is open on it
	// (collection.droppable): they free the trees the iterator reads.
	// Close the iterator first.
	ErrIterOpen = errors.New("any-store: an iterator of the transaction is open on the collection")

	// ErrQuickCheckFailed is returned when we did a quick check (e.g. when opening db in a dirty state with sentinel config on) and it failed, indicating possible database corruption.
	ErrQuickCheckFailed error = &nestedError{msg: "any-store: quick check failed on db open", parent: ErrCorrupt}

	ErrDBIsClosed = btree.ErrClosed

	// ErrBusy is returned when a lock stayed held past the busy timeout:
	// another process's writer or checkpoint, or, for Backup, a write
	// transaction of this process. The operation can be tried again. Match
	// it with errors.Is: the busy errors of the storage layer that are not
	// this value wrap it by the time they leave Backup. What a Backup that
	// returned it has done is described at DB.Backup.
	//
	// CopyToLatestFormat returns it (wrapped) at once, without waiting, when
	// another call holds a copy lock it needs, as a call copying to the same
	// destination does, or to a name one of the two keeps beside the other
	// (on js/wasm and wasip1, any call in the process), and when a database
	// is open at the destination's staging name. No file but the copy's lock
	// files has been created, written or removed: a lock the call took
	// before it failed it released, removing the lock file, which may be one
	// that a killed call left there unlocked.
	ErrBusy = btree.ErrBusy

	ErrDBIsNotOpened = errors.New("any-store: database is not opened")

	// ErrDBAlreadyOpen is returned by Open, and (wrapped) by Backup for its destination, when
	// this process already has the file open, or is opening it: one process holds one handle
	// on a database file. The file is recognised by its identity, not by its name, so a second
	// path to it (a hard link or a symbolic link) is refused too; on Windows, js/wasm and
	// wasip1 it is recognised by its full path. Nothing was locked or changed. Use the handle
	// that is open, or close it first.
	ErrDBAlreadyOpen = btree.ErrDatabaseOpen

	// ErrDBOpenInOtherProcess is returned by Open, and (wrapped) by Backup for its destination,
	// on a platform without multi-process access, when another process has the file open.
	// Multi-process access needs linux or darwin on amd64 or arm64; everywhere else a database
	// file belongs to one process at a time, and a second process is refused at once, without
	// waiting. Nothing was changed. On Windows a second Open in the same process that names
	// the file by another path (a link) is refused with this error too.
	//
	// It is not returned on linux or darwin on amd64 or arm64, where a second process opens
	// the file, nor on js/wasm and wasip1, which take no lock on the file: there a second
	// process is not detected.
	ErrDBOpenInOtherProcess = btree.ErrInProcessLocked

	// ErrUnsupportedDB is returned by a package function that takes a DB,
	// such as SchemaFormat, when that DB is not one this package opened: a
	// type of the caller's own that implements the interface.
	ErrUnsupportedDB = errors.New("any-store: database was not opened by this package")

	// ErrTxInContext is returned by CopyToLatestFormat when its ctx carries a
	// transaction, read or write: the copy reads its source in a transaction
	// of its own, and SQLite refuses VACUUM inside a transaction the same
	// way. Pass a ctx without one.
	ErrTxInContext = errors.New("any-store: cannot run inside a transaction")

	// ErrDestinationExists is returned (possibly wrapped) by
	// CopyToLatestFormat when a file is at the destination path, or at one
	// of the paths a database keeps beside it: path+"-wal", path+"-wal-shm",
	// the sentinel path+".lock" and the creation journal path+"-journal";
	// or when the destination is one of those paths of a file that exists.
	// The call replaces nothing: a file that was there is left as it was.
	// One found before the call takes its copy locks refuses it before it
	// creates anything.
	// The check is made again under the locks and right before the copy is
	// published, and a file that appeared by then refuses the call after it
	// created its lock files, or built the copy under its staging name, and
	// removed them again. Apart from the read bookkeeping in the source's
	// wal-index (its -wal-shm), which the copy's read of the source changes
	// as any reader's does, no file but the copy's own lock and staging
	// files has been created, written or removed; the source's files are
	// never renamed, removed or otherwise written.
	ErrDestinationExists = errors.New("any-store: destination exists")

	// ErrCopyPublished is returned by CopyToLatestFormat, always wrapped
	// together with the error of the step that failed, when the copy is
	// complete, verified and published at the destination path, and a step
	// after the publication failed: the sync of the destination's directory
	// (on Unix), without which the destination's name may not survive a
	// power loss, or the removal of a file left under the staging name. The
	// CopyReport returned with it is filled. The destination is the copy and
	// can be used; do not remove it or run the copy again because of this
	// error. To make its name durable, sync its directory.
	ErrCopyPublished = errors.New("any-store: the copy is published, but a step after its publication failed")

	// ErrIncompatibleVersion is returned (wrapped) by Open when the file was written in a format
	// newer than this release reads. The file is intact and is left unchanged; open it with a
	// release that knows the format. It does not match ErrCorrupt.
	ErrIncompatibleVersion = errors.New("any-store: incompatible version")

	// ErrV1Database is returned by Open when the file was created by any-store
	// v1, which is SQLite-backed and cannot be read by v2. See
	// docs/migration-v1-to-v2.md for moving the data across.
	ErrV1Database = errors.New("any-store: file is an any-store v1 database, not readable by v2; use github.com/anyproto/any-store or migrate (docs/migration-v1-to-v2.md)")

	// ErrWrongKey is returned (wrapped) by Open when Config.Encryption holds a passphrase, key
	// or codec that does not open the file. The file is left unchanged, and Open with the right
	// key opens it. It does not match ErrCorrupt or ErrPageIntegrity, and Config.OnIntegrityError
	// is not called.
	//
	// Open authenticates page 1 with the key. When that fails, it tries up to 16 other pages,
	// spread over the database, with the same key:
	//
	//   - if one of them authenticates, the key is right and page 1 is damaged: Open returns an
	//     error that matches ErrPageIntegrity and ErrCorrupt, and Config.OnIntegrityError is
	//     called for page 1;
	//   - if none does, Open returns ErrWrongKey.
	//
	// A wrong key is never reported as corruption: it would have to pass a page's
	// authentication tag, a chance of about 2^-128. The reverse can happen: a database that is
	// damaged on page 1 and on every other page tried, or that has no other page (a new, empty
	// database is one page), is reported as ErrWrongKey although the key is right. So does a
	// file whose key salt in the header (bytes 72-87) was overwritten, when the key is derived
	// from a passphrase. ErrWrongKey therefore means "this key does not open this file", and
	// never "discard the file".
	//
	// Once Open has returned a database the key is proven, and a page that fails its
	// authentication later is damaged: ErrPageIntegrity.
	ErrWrongKey = btree.ErrWrongKey

	// ErrEncryptionKeyRequired is returned by Open, and (wrapped) by Backup for its destination,
	// when the file is encrypted and Config.Encryption is empty: no Passphrase and no Codec.
	// Nothing past the file's header was read, and the file is left unchanged. Open it with the
	// Config.Encryption it was created with. It does not match ErrCorrupt.
	ErrEncryptionKeyRequired = errors.New("any-store: database file is encrypted: set Config.Encryption")

	// ErrNotEncrypted is returned by Open, and (wrapped) by Backup for its destination, when
	// the file is not encrypted and Config.Encryption holds a Passphrase or a Codec. The file
	// is left unchanged; it is not encrypted in place. Open it with an empty Config.Encryption.
	// It does not match ErrCorrupt.
	ErrNotEncrypted = errors.New("any-store: database file is not encrypted: clear Config.Encryption")

	// ErrCorrupt is the umbrella for damaged database files: every error below that names a kind
	// of damage matches it, as does a broken structure found while reading. Match it when the kind
	// does not matter, for instance to discard and rebuild the database:
	//
	//	if errors.Is(err, anystore.ErrCorrupt) { ... }
	//
	// It is not returned for a v1 file (ErrV1Database), a file from a newer release
	// (ErrIncompatibleVersion) or a closed database.
	//
	// It is not returned for a wrong encryption key either: that is ErrWrongKey, which Open
	// tells from a damaged page 1 (see there).
	//
	// A damaged page that nothing has read yet is not found by Open: it surfaces on the read that
	// meets it, as ErrPageIntegrity.
	ErrCorrupt = btree.ErrCorrupt

	// ErrHeaderCorrupt is returned (wrapped) by Open when the file header is unusable: a file cut
	// inside it or never a database, bad field values, or a size that disagrees with the file.
	ErrHeaderCorrupt = btree.ErrHeaderCorrupt

	// ErrWALCorrupt is returned (wrapped) when the write-ahead log index points at a frame the
	// file does not hold. Recovery itself repairs a bad WAL header and drops torn frames silently.
	ErrWALCorrupt = btree.ErrWALCorrupt

	// ErrValueTooLarge is returned (wrapped) by a write of a record over the record size limit:
	// a stored key and its value together may take 1 GiB (1<<30 bytes). The limit applies to each
	// key/value pair a write stores, not to the document as a whole:
	//
	//   - the document itself: its encoded id as the key and its body as the value, the body
	//     compressed unless compression is off or does not shrink it. A body whose plain
	//     encoding is over 1 GiB is kept plain, so it is over the limit and refused;
	//   - each range-index entry: the encoded indexed values followed by the id as the key, and
	//     a one-byte value. A range-index key of exactly 1 GiB is therefore over the limit by
	//     that byte and reported with this error, not with ErrKeyTooLarge;
	//   - each record a full-text or vector index stores, its key and its value.
	//
	// Nothing of the record is written, and the operation is rolled back like after any other
	// failed write. Without the limit the record would be stored and every later read of it
	// would report ErrCorrupt.
	//
	// ErrKeyTooLarge is a separate error, not a kind of this one: a caller that handles records
	// over the limit checks for both.
	ErrValueTooLarge = btree.ErrValueTooLarge

	// ErrKeyTooLarge is returned (wrapped) by a write of a record whose key alone is over 1 GiB:
	// a document's encoded id, or an index entry's key. Which of the two errors an oversized
	// document gets depends on the pair that fails first: an update that grows an indexed field
	// past the limit writes the index entry before the document and returns this error even when
	// the id is short. It does not match ErrValueTooLarge; see there.
	ErrKeyTooLarge = btree.ErrKeyTooLarge

	// ErrPageBufferNotInitialized is returned when UseGlobalPageBuffer is set
	// but InitPageBuffer was not called before opening the database.
	ErrPageBufferNotInitialized = errors.New("any-store: global page buffer not initialized (call InitPageBuffer first)")
)

// nestedError keeps its own text and matches parent under errors.Is.
type nestedError struct {
	msg    string
	parent error
}

func (e *nestedError) Error() string { return e.msg }
func (e *nestedError) Unwrap() error { return e.parent }

// openErr gives an error of btree.Open the identity this package exports
// for it, where the btree's own names its options and not Config's.
func openErr(err error) error {
	switch {
	case errors.Is(err, btree.ErrPageSlabNotInitialized):
		return ErrPageBufferNotInitialized
	case errors.Is(err, btree.ErrSQLiteFormat):
		return ErrV1Database
	case errors.Is(err, btree.ErrNewerFormat):
		return fmt.Errorf("%w: %w", ErrIncompatibleVersion, err)
	case errors.Is(err, btree.ErrKeyRequired):
		return ErrEncryptionKeyRequired
	case errors.Is(err, btree.ErrNotEncrypted):
		return ErrNotEncrypted
	}
	return err
}

// isBusyErr reports one of the storage layer's "not now" errors: a lock that
// stayed busy, a snapshot another process's commit overtook, a wal-index
// being rebuilt, or no free reader slot.
func isBusyErr(err error) bool {
	return errors.Is(err, btree.ErrBusy) || errors.Is(err, btree.ErrBusySnapshot) ||
		errors.Is(err, btree.ErrBusyRecovery) || errors.Is(err, btree.ErrReadersBusy)
}

// busyErr makes every busy error match ErrBusy, keeping the original.
func busyErr(err error) error {
	if err == nil || errors.Is(err, ErrBusy) || !isBusyErr(err) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrBusy, err)
}
