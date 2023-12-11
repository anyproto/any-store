package anystore

import "context"

type Iterator interface {
	Next(ctx context.Context) (ok bool)
	Item() Item
	Close() error
}
