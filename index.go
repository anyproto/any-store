package anystore

type IndexInfo struct {
	Name   string
	Fields []string
	Unique bool
	Sparse bool
}
