package anystore

import (
	"context"
	"github.com/anyproto/any-store/internal/conn"
)

type IndexInfo struct {
	Name   string
	Fields []string
	Unique bool
	Sparse bool
}

type Index interface {
	Info() IndexInfo
}

func newIndex(ctx context.Context, c *collection, info IndexInfo) (idx *index, err error) {
	return &index{}, nil
}

type index struct {
}

func (idx *index) drop(ctx context.Context, cn conn.Conn) (err error) {
	return nil
}

func (idx *index) renameColl(ctx context.Context, cn conn.Conn, name string) error {
	return nil
}
