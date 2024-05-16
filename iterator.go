package anystore

import "context"

type Iterator interface {
	Next(ctx context.Context) bool
	Doc() Doc
	Error() error
	Close() error
}
