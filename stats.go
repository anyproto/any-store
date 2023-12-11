package anystore

type CollectionStats struct {
	Len     int
	Size    int
	Indexes []IndexStats
}

type IndexStats struct {
	Len int
}
