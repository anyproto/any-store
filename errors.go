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

	// ErrTxIsReadOnly is returned when a write operation is attempted in a read-only transaction.
	ErrTxIsReadOnly = errors.New("any-store: transaction is read-only")

	// ErrTxIsUsed is returned when an operation is attempted on a transaction that has already been committed or rolled back.
	ErrTxIsUsed = errors.New("any-store: transaction has already been used")

	// ErrTxOtherInstance is returned when an operation is attempted using a transaction from a different database instance.
	ErrTxOtherInstance = errors.New("any-store: transaction belongs to another database instance")

	// ErrUniqueConstraint is returned when a unique constraint violation occurs.
	ErrUniqueConstraint = errors.New("any-store: unique constraint violation")

	// ErrIterClosed is returned when operations are attempted on a closed iterator.
	ErrIterClosed = errors.New("any-store: iterator is closed")

	// ErrQuickCheckFailed is returned when we did a quick check (e.g. when opening db in a dirty state with sentinel config on) and it failed, indicating possible database corruption.
	ErrQuickCheckFailed = errors.New("any-store: quick check failed on db open")

	ErrDBIsClosed = btree.ErrClosed

	ErrDBIsNotOpened       = errors.New("any-store: database is not opened")
	ErrIncompatibleVersion = errors.New("any-store: incompatible version")

	// ErrPageBufferNotInitialized is returned when UseGlobalPageBuffer is set
	// but InitPageBuffer was not called before opening the database.
	ErrPageBufferNotInitialized = errors.New("any-store: global page buffer not initialized (call InitPageBuffer first)")
)
