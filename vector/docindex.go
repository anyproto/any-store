package vector

// DocResult is a search hit carrying the document id bytes (as any-store keys
// documents) instead of an internal label.
type DocResult struct {
	ID       []byte
	Distance float32
}

// DocFlatHNSW shows how a real any-store vector index composes the two pieces:
// an IDDict that turns document ids ([]byte) into dense uint32 labels, and a
// FlatHNSW arena graph keyed by those labels. It is the shape the collection
// layer would drive from its write transactions.
//
// Labels are stable for the life of a document id (they survive graph Compact,
// because Compact only reshuffles the graph's *internal* arena ids, never the
// labels it is keyed by). That stability is what lets the on-disk node records
// key by label without rewriting them on every compaction.
type DocFlatHNSW struct {
	dict *IDDict
	g    *FlatHNSW
}

// NewDocFlatHNSW creates an empty document-keyed index.
func NewDocFlatHNSW(dim int, m Metric, seed int64, capHint int) *DocFlatHNSW {
	return &DocFlatHNSW{
		dict: NewIDDict(capHint),
		g:    NewFlatHNSW(dim, m, seed),
	}
}

// Add inserts a vector for docID. Re-adding an existing id is a no-op (use
// Update to change its vector).
func (x *DocFlatHNSW) Add(docID []byte, vec []float32) {
	label, isNew := x.dict.Intern(docID)
	if !isNew {
		return
	}
	x.g.Add(uint64(label), vec)
}

// Update changes docID's vector (delete-old + reinsert-new in the graph; the
// label, and therefore the document's identity, is unchanged).
func (x *DocFlatHNSW) Update(docID []byte, vec []float32) bool {
	label, ok := x.dict.Label(docID)
	if !ok {
		return false
	}
	return x.g.Update(uint64(label), vec)
}

// Delete tombstones docID in both the dictionary and the graph.
func (x *DocFlatHNSW) Delete(docID []byte) bool {
	label, ok := x.dict.Label(docID)
	if !ok {
		return false
	}
	x.g.Delete(uint64(label))
	x.dict.Delete(docID)
	return true
}

// Search returns the k nearest document ids to query.
func (x *DocFlatHNSW) Search(query []float32, k int) []DocResult {
	hits := x.g.Search(query, k)
	out := make([]DocResult, len(hits))
	for i, h := range hits {
		out[i] = DocResult{ID: x.dict.ID(uint32(h.Key)), Distance: h.Distance}
	}
	return out
}

// Len returns the number of live documents.
func (x *DocFlatHNSW) Len() int { return x.dict.Len() }

// Compact reclaims the graph's tombstoned arena space. Dictionary tombstones
// (just the orphaned id bytes) are left in place — they are tiny next to the
// vector/adjacency arenas — and labels stay stable across the call.
func (x *DocFlatHNSW) Compact() { x.g.Compact() }

// MemBytes returns the combined footprint of the graph arenas and the id
// dictionary.
func (x *DocFlatHNSW) MemBytes() int { return x.g.MemBytes() + x.dict.MemBytes() }

// DictMemBytes isolates the id-dictionary footprint (for the RAM breakdown).
func (x *DocFlatHNSW) DictMemBytes() int { return x.dict.MemBytes() }
