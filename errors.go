package anystore

import (
	"errors"

	"github.com/anyproto/any-store/v2/internal/btree"
)

var (
	// ErrDocExists is returned when attempting to insert a document that already exists.
	ErrDocExists = errors.New("any-store: document already exists")

	// ErrDocNotFound is returned when a document cannot be found by its ID.
	ErrDocNotFound = errors.New("any-store: document not found")

	// ErrDocWithoutId is returned when a document is provided without a required ID.
	ErrDocWithoutId = errors.New("any-store: document missing ID")

	// ErrPrimaryKeyMismatch is returned by Collection when the requested
	// PrimaryKey option conflicts with an existing collection's stored primary
	// key. The primary key is immutable after creation.
	ErrPrimaryKeyMismatch = errors.New("any-store: primary key mismatch")

	// ErrCollectionExists is returned when attempting to create a collection that already exists.
	ErrCollectionExists = errors.New("any-store: collection already exists")

	// ErrCollectionNotFound is returned when a collection cannot be found.
	ErrCollectionNotFound = errors.New("any-store: collection not found")

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
