package anystore

import (
	"errors"

	"github.com/mattn/go-sqlite3"
)

var (
	ErrDocExists    = errors.New("any-store: doc exists")
	ErrDocNotFound  = errors.New("any-store: doc not found")
	ErrDocWithoutId = errors.New("any-store: doc without id")

	ErrCollectionExists   = errors.New("any-store: collection exists")
	ErrCollectionNotFound = errors.New("any-store: collection not found")

	ErrIndexExists   = errors.New("any-store: index exists")
	ErrIndexNotFound = errors.New("any-store: index does not exist")

	ErrTxIsReadOnly    = errors.New("any-store: tx is read-only")
	ErrTxIsUsed        = errors.New("any-store: tx is been used")
	ErrTxOtherInstance = errors.New("any-store: tx is from an other db instance")

	ErrUniqueConstraint = errors.New("any-store: unique constraint")

	ErrIterClosed = errors.New("any-store: iter is closed")
)

func replaceUniqErr(err, replaceTo error) error {
	if err == nil {
		return nil
	}
	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) {
		if errors.Is(sqliteErr.Code, sqlite3.ErrConstraint) {
			return replaceTo
		}
	}
	return err
}
