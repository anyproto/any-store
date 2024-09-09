package anystore

import (
	"errors"

	"zombiezen.com/go/sqlite"

	"github.com/anyproto/any-store/internal/driver"
)

var (
	// ErrDocExists is returned when attempting to insert a document that already exists.
	ErrDocExists = errors.New("any-store: document already exists")

	// ErrDocNotFound is returned when a document cannot be found by its ID.
	ErrDocNotFound = errors.New("any-store: document not found")

	// ErrDocWithoutId is returned when a document is provided without a required ID.
	ErrDocWithoutId = errors.New("any-store: document missing ID")

	// ErrCollectionExists is returned when attempting to create a collection that already exists.
	ErrCollectionExists = errors.New("any-store: collection already exists")

	// ErrCollectionNotFound is returned when a collection cannot be found.
	ErrCollectionNotFound = errors.New("any-store: collection not found")

	// ErrIndexExists is returned when attempting to create an index that already exists.
	ErrIndexExists = errors.New("any-store: index already exists")

	// ErrIndexNotFound is returned when an index cannot be found.
	ErrIndexNotFound = errors.New("any-store: index not found")

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

	ErrDBIsClosed    = driver.ErrDBIsClosed
	ErrDBIsNotOpened = driver.ErrDBIsNotOpened
)

func replaceUniqErr(err, replaceTo error) error {
	if err == nil {
		return nil
	}
	switch sqlite.ErrCode(err) {
	case sqlite.ResultConstraintPrimaryKey,
		sqlite.ResultConstraintUnique:
		return replaceTo
	}
	return err
}

func replaceInterruptErr(err error) error {
	if err == nil {
		return nil
	}
	switch sqlite.ErrCode(err) {
	case sqlite.ResultInterrupt:
		return ErrDBIsClosed
	}
	return err
}
