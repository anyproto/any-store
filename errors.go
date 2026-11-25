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

	// ErrQuickCheckFailed is returned (wrapped, with the damage the check met) by Open when the
	// database was left dirty (Config.Durability.Sentinel is on and its marker file was found)
	// and the quick check Open then runs found damage, or Open's read of the catalog's root
	// before it did. It matches ErrCorrupt.
	//
	// A quick check that did not finish is not this error and does not match ErrCorrupt: Open
	// then returns, wrapped, what stopped it, to match as itself: the ctx's error
	// (context.Canceled, context.DeadlineExceeded, the latter also for the check's own limit of
	// five minutes), ErrBusy, or an I/O error. The marker file stays, and the next Open runs
	// the check again.
	ErrQuickCheckFailed error = &nestedError{msg: "any-store: quick check failed on db open", parent: ErrCorrupt}

	ErrDBIsClosed = btree.ErrClosed

	// ErrBusy is returned when a lock stayed held past the busy timeout:
	// another process's writer or checkpoint, or, for Backup, a write
	// transaction of this process. The operation can be tried again. Match
	// it with errors.Is. Open, Backup and IntegrityCheck return every busy
	// error of the storage layer as one that matches it: a lock that stayed
	// held, a snapshot that another process's commit overtook, a recovery of
	// the write-ahead log that another process is running, no free reader
	// slot. The begin of a transaction does not (ReadTx, WriteTx, and the
	// transaction an operation begins for itself): it returns this error for
	// a lock that stayed held, and the storage layer's other busy errors as
	// they are. What a Backup that returned it has done is described at
	// DB.Backup.
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
	// path to it (a hard link or a symbolic link) is refused too; on Windows, js/wasm, wasip1
	// and plan9 it is recognised by its full path. Nothing was locked or changed. Use the handle
	// that is open, or close it first.
	ErrDBAlreadyOpen = btree.ErrDatabaseOpen

	// ErrDBOpenInOtherProcess is returned by Open, and (wrapped) by Backup for its destination,
	// on a platform without multi-process access, when another process has the file open.
	// Multi-process access needs a target Go builds with the linux or darwin tag (linux,
	// android, darwin, ios) on amd64 or arm64; everywhere else a database file belongs to one
	// process at a time, and a second process is refused at once, without waiting. Nothing was
	// changed. On Windows a second Open in the same process that names the file by another path
	// (a link) is refused with this error too.
	//
	// It is not returned on linux, android, darwin or ios on amd64 or arm64, where a second
	// process opens the file, nor on js/wasm, wasip1 and plan9, which take no lock on the file:
	// there a second process is not detected.
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

	// ErrCopyVerifyFailed is returned (wrapped) by CopyToLatestFormat when the copy it built
	// fails its verification: a collection's options, document count or index definitions
	// differ from the source's snapshot, a record of its catalog does not decode, a
	// collection's data is missing, or the copy's integrity check found damage in it. It is
	// returned too for damage the copy meets in itself while it is built: a page of the copy
	// that fails its checksum when the copy reads it back, to add the next documents or to
	// build an index. The fault is the copy's, not the source's: nothing is published at the
	// destination, the staging files are removed, and the source is as it was. It does not
	// match ErrCorrupt, even when what was found in the copy would: do not discard the source
	// on it. Damage met reading the source is returned as the source's, and matches
	// ErrCorrupt. Run the copy again; one that keeps failing points at the storage the copy is
	// written to.
	ErrCopyVerifyFailed = errors.New("any-store: copy to latest format: the copy failed its verification")

	// ErrBackupFormatMismatch is returned (wrapped) by Backup when the file at its path is a
	// database of another schema format than this one's (SchemaFormat). A backup copies the
	// pages as they are, so it would turn the file into one of this database's format under
	// whatever has it open. No page of this database is copied, and the file keeps its
	// contents; closing it may checkpoint its write-ahead log into it, as closing any database
	// may. Back up to a new path, or to a database of this one's format.
	ErrBackupFormatMismatch = btree.ErrBackupFormatMismatch

	// ErrBackupCodecMismatch is returned (wrapped) by Backup when the file at its path is a
	// database whose pages are laid out another way than this one's: plain, with checksums,
	// or encrypted (an in-memory database's pages are plain). No page of this database is
	// copied, and the file keeps its contents; closing it may checkpoint its write-ahead log
	// into it, as closing any database may. Back up to a new path, or to a database of this
	// one's kind.
	ErrBackupCodecMismatch = btree.ErrBackupCodecMismatch

	// ErrIncompatibleVersion is returned (wrapped) by Open, and by Backup for its destination,
	// when the file was written in a format newer than this release reads, or its write-ahead log
	// has a version this release does not read, or a process that has the file open uses an
	// index of the log (path+"-wal-shm") of a version this release does not use. The files are
	// left unchanged; open them with the release that wrote them. It does not match ErrCorrupt.
	ErrIncompatibleVersion = errors.New("any-store: incompatible version")

	// ErrV1Database is returned by Open when the file was created by any-store
	// v1, which is SQLite-backed and cannot be read by v2. See
	// docs/migration-v1-to-v2.md for moving the data across.
	ErrV1Database = errors.New("any-store: file is an any-store v1 database, not readable by v2; use github.com/anyproto/any-store or migrate (docs/migration-v1-to-v2.md)")

	// ErrWrongKey is returned (wrapped) by Open when Config.Encryption does not open the file:
	// its passphrase or key, or its cipher, is not the one the file was encrypted with. The file
	// is left unchanged, and Open with the Config.Encryption the file was created with opens it.
	// It does not match ErrCorrupt or ErrPageIntegrity, and Config.OnIntegrityError is not
	// called.
	//
	// Open authenticates page 1 with the key once the file header and the database size it
	// records have passed their checks, which do not depend on the key: a header that is
	// malformed, or does not fit the file, is ErrHeaderCorrupt, and one of a newer format
	// ErrIncompatibleVersion, whatever the key. When the authentication fails, Open tries up to
	// 16 other pages, spread over the database, with the same key:
	//
	//   - if one of them authenticates, the key is right and page 1 is damaged: Open returns an
	//     error that matches ErrPageIntegrity and ErrCorrupt, and Config.OnIntegrityError is
	//     called for page 1;
	//   - if none does, and every one of them was read and failed its authentication, Open
	//     returns ErrWrongKey;
	//   - if one of them could not be read (an I/O error) and none authenticates, it might
	//     have proved the key: Open returns that read's error, which matches neither, and
	//     Config.OnIntegrityError is not called.
	//
	// A wrong key is never reported as corruption: it would have to pass a page's
	// authentication tag, a chance of about 2^-128. The reverse can happen: a database that is
	// damaged on page 1 and on every other page tried, or that has no page but page 1, is
	// reported as ErrWrongKey although the key is right. So does a
	// file whose key salt in the header (bytes 72-87) was overwritten with another value that is
	// not all zeros, when the key is derived from a passphrase; a salt overwritten with zeros
	// makes the file one that is not encrypted, ErrNotEncrypted. ErrWrongKey therefore means
	// "this key does not open this file", and never "discard the file".
	//
	// Once Open has returned a database the key is proven, and a page that fails its
	// authentication later is damaged: ErrPageIntegrity.
	ErrWrongKey = btree.ErrWrongKey

	// ErrEncryptionKeyRequired is returned by Open, and (wrapped) by Backup for its destination,
	// when the file is encrypted and Config.Encryption is empty (its Enabled reports false).
	// Open decides it from the file's header alone, and the file is left unchanged. Open it with
	// the Config.Encryption it was created with. It does not match ErrCorrupt.
	ErrEncryptionKeyRequired = errors.New("any-store: database file is encrypted: set Config.Encryption")

	// ErrNotEncrypted is returned by Open, and (wrapped) by Backup for its destination, when
	// the file is not encrypted and Config.Encryption is not empty (its Enabled reports true).
	// The file is left unchanged; it is not encrypted in place. Open it with an empty
	// Config.Encryption. It does not match ErrCorrupt.
	ErrNotEncrypted = errors.New("any-store: database file is not encrypted: clear Config.Encryption")

	// ErrCorrupt is the umbrella for damaged database files: every error below that names a kind
	// of damage matches it (ErrHeaderCorrupt, ErrPageIntegrity, ErrQuickCheckFailed), as do a
	// broken structure found while reading, a page of the database that the file ends inside
	// (a file cut short), and the report of IntegrityCheck. Match it when the kind does not
	// matter, for instance to discard and rebuild the database:
	//
	//	if errors.Is(err, anystore.ErrCorrupt) { ... }
	//
	// It means damage that is in the files and will be there at the next Open: of the database
	// the call read. Backup reads two, and can return it, or ErrHeaderCorrupt, for a damaged
	// file at its path while the database it copies is sound (see DB.Backup): rule that out
	// before discarding the source. It is not returned for a state that passes or that the
	// caller can change:
	//
	//   - a wrong encryption key (ErrWrongKey), or a Config.Encryption that does not fit the
	//     file (ErrEncryptionKeyRequired, ErrNotEncrypted);
	//   - a v1 file (ErrV1Database), or a file, a write-ahead log or its index of a version this
	//     release does not read (ErrIncompatibleVersion);
	//   - a file that is open already (ErrDBAlreadyOpen, ErrDBOpenInOtherProcess), a busy lock
	//     (ErrBusy), a closed database (ErrDBIsClosed);
	//   - a write-ahead log index that points past the end of the log (ErrWALShortRead);
	//   - a cancelled context, an I/O error, a full disk: these are returned as themselves,
	//     also when they end a commit, the quick check of a dirty Open, or an IntegrityCheck.
	//
	// A damaged index of the write-ahead log (path+"-wal-shm", shared memory) matches it too, as
	// in SQLite; closing every handle on the file, in every process, rebuilds the index.
	//
	// A damaged page that nothing has read yet is not found by Open: it surfaces on the read that
	// meets it, as ErrPageIntegrity.
	ErrCorrupt = btree.ErrCorrupt

	// ErrHeaderCorrupt is returned (wrapped) by Open when the file header is unusable: a file cut
	// inside it or never a database, bad field values, or a size that disagrees with the file.
	// It matches ErrCorrupt.
	ErrHeaderCorrupt = btree.ErrHeaderCorrupt

	// ErrWALShortRead is returned (wrapped) by a read, a commit or a checkpoint (Flush), when the
	// index of the write-ahead log (path+"-wal-shm") points at a frame past the end of the log
	// file (path+"-wal"): the read of the frame comes up short, as SQLite's does with
	// SQLITE_IOERR_SHORT_READ. The error keeps the short read's own error: io.EOF, or
	// io.ErrUnexpectedEOF, which does not match io.EOF, for a read that returned fewer bytes
	// than it asked for and no error. An I/O error while reading a frame is not this error.
	// Recovery itself repairs a bad WAL header and drops torn frames silently.
	//
	// The database is not damaged, and the error does not match ErrCorrupt. Close every handle on
	// the file, in every process, and open it again: the index is rebuilt from the log.
	ErrWALShortRead = btree.ErrWALShortRead

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
// for it, where the btree's own names its options and not Config's: a
// newer format of the file, of its WAL or of a live wal-index is
// ErrIncompatibleVersion, as SQLite's version checks of the WAL and its
// index give SQLITE_CANTOPEN (wal.c:1467, 2741), and every busy error
// matches ErrBusy.
func openErr(err error) error {
	switch {
	case errors.Is(err, btree.ErrPageSlabNotInitialized):
		return ErrPageBufferNotInitialized
	case errors.Is(err, btree.ErrSQLiteFormat):
		return ErrV1Database
	case errors.Is(err, btree.ErrKeyRequired):
		return ErrEncryptionKeyRequired
	case errors.Is(err, btree.ErrNotEncrypted):
		return ErrNotEncrypted
	}
	return busyErr(versionErr(err))
}

// versionErr makes a version refusal of the btree match
// ErrIncompatibleVersion, keeping the btree's error: a newer format of the
// file, or a WAL or a live wal-index of a version this release does not
// read or use, newer or older. Open applies it to every error it returns,
// as busyErr: a peer can publish a wal-index of another version after
// btree.Open has returned, and the write begin of the initialization then
// refuses it.
func versionErr(err error) error {
	if err == nil || errors.Is(err, ErrIncompatibleVersion) {
		return err
	}
	if errors.Is(err, btree.ErrNewerFormat) || errors.Is(err, btree.ErrWALVersion) {
		return fmt.Errorf("%w: %w", ErrIncompatibleVersion, err)
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
