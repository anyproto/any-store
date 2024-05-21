package anystore

import (
	"database/sql"
	"errors"

	"github.com/mattn/go-sqlite3"
)

var (
	ErrDocExists   = errors.New("any-store: doc exists")
	ErrDocNotFound = errors.New("any-store: doc not found")

	ErrCollectionExists   = errors.New("any-store: collection exists")
	ErrCollectionNotFound = errors.New("any-store: collection not found")

	ErrTxIsReadOnly    = errors.New("tx is read-only")
	ErrTxOtherInstance = errors.New("tx is from an other db instance")
)

func replaceNoRowsErr(err, replaceTo error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return replaceTo
	}
	return err
}

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
