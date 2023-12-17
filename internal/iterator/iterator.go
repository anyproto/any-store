package iterator

type IdIterator interface {
	Next() bool
	Valid() bool
	Values() [][]byte
	Close() error
}
