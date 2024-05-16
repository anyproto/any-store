package anystore

import "errors"

var (
	ErrDocExists   = errors.New("any-store: doc exists")
	ErrDocNotFound = errors.New("any-store: doc not found")

	ErrCollectionExists   = errors.New("any-store: collection exists")
	ErrCollectionNotFound = errors.New("any-store: collection not found")
)
