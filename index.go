package anystore

import (
	"context"
	"github.com/anyproto/any-store/internal/conn"
	"strings"
)

type IndexInfo struct {
	Name   string
	Fields []string
	Unique bool
	Sparse bool
}

func (i IndexInfo) createName() string {
	return strings.Join(i.Fields, ",")
}

type Index interface {
	Info() IndexInfo
	Len(ctx context.Context) (int64, error)
}

func newIndex(ctx context.Context, c *collection, info IndexInfo) (idx *index, err error) {
	return &index{info: info}, nil
}

type index struct {
	info IndexInfo
}

func (idx *index) Info() IndexInfo {
	return idx.info
}

func (idx *index) Len(ctx context.Context) (int64, error) {
	return 0, nil
}

func (idx *index) Drop(ctx context.Context, cn conn.Conn) (err error) {
	return nil
}

func (idx *index) RenameColl(ctx context.Context, cn conn.Conn, name string) error {
	return nil
}

func (idx *index) Insert(ctx context.Context, cn conn.Conn, it item) error {
	return nil
}
