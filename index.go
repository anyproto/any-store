package anystore

import "context"

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
