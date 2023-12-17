package query

type Query struct {
	Filter        Filter
	Project       Project
	Sort          Sort
	Hint          []string
	Limit, Offset uint64
}
