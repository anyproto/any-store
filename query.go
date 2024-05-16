package anystore

import "context"

type Query interface {
	Cond(filter any) Query
	Limit(limit uint) Query
	Offset(offset uint) Query
	Sort(sort ...any) Query
	Iter() Iterator
	Update(ctx context.Context, modifier any) error
	Delete(ctx context.Context) (err error)
}
