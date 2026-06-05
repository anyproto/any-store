package vector

// idmap.go answers the "how do we link graph nodes to real any-store documents"
// question.
//
// any-store keys every document by the marshaled bytes of its `id` field
// (`idVal.MarshalTo`) — an arbitrary, variable-length []byte (a string, int,
// objectid, …). There is no numeric rowid. An HNSW arena, on the other hand,
// wants a dense, fixed-width uint32 to index its vector/adjacency slabs.
//
// IDDict bridges the two with **dictionary encoding**: each distinct document id
// is assigned a dense uint32 *label* (a monotonic counter). The label is what
// the graph stores; the document id is recovered from the label only when
// results are returned.
//
// Two ways to keep the label -> id-bytes reverse mapping:
//
//	[][]byte            one slice header (24 B on 64-bit) + one backing array
//	                    PER id  ->  24 B/id of pure overhead, N heap objects for
//	                    the GC to scan, ids scattered across the heap.
//
//	flat arena (this)   all id bytes concatenated in one []byte, plus one
//	                    []uint32 of end-offsets  ->  4 B/id of overhead, exactly
//	                    two heap objects total, ids contiguous.
//
// The arena form is the one any-store should use; the benchmark
// (BenchmarkIDMapMem) quantifies the gap.
type IDDict struct {
	forward map[string]uint32 // id bytes -> label
	data    []byte            // all id bytes concatenated
	ends    []uint32          // label -> end offset in data (start = ends[label-1], or 0)
	live    []bool            // label -> not deleted
	liveN   int
}

// NewIDDict returns an empty dictionary. capHint pre-sizes the arenas.
func NewIDDict(capHint int) *IDDict {
	return &IDDict{
		forward: make(map[string]uint32, capHint),
		ends:    make([]uint32, 0, capHint),
		live:    make([]bool, 0, capHint),
	}
}

// Intern returns the label for id, assigning a fresh one if unseen. isNew
// reports whether a new label was allocated. The id bytes are copied into the
// arena, so the caller may reuse its buffer.
func (d *IDDict) Intern(id []byte) (label uint32, isNew bool) {
	if l, ok := d.forward[string(id)]; ok { // string(id) does not allocate as a map key
		if !d.live[l] {
			d.live[l] = true
			d.liveN++
		}
		return l, false
	}
	label = uint32(len(d.ends))
	d.data = append(d.data, id...)
	d.ends = append(d.ends, uint32(len(d.data)))
	d.live = append(d.live, true)
	d.liveN++
	d.forward[string(id)] = label // one allocation: the map key string
	return label, true
}

// Label returns the label for id if present and live.
func (d *IDDict) Label(id []byte) (uint32, bool) {
	l, ok := d.forward[string(id)]
	if !ok || !d.live[l] {
		return 0, false
	}
	return l, true
}

// ID returns the document-id bytes for a label. The returned slice aliases the
// arena; copy it if you need to retain it past the next mutation.
func (d *IDDict) ID(label uint32) []byte {
	if int(label) >= len(d.ends) {
		return nil
	}
	start := uint32(0)
	if label > 0 {
		start = d.ends[label-1]
	}
	return d.data[start:d.ends[label]]
}

// Delete tombstones the label for id (the arena bytes stay until Compact). The
// forward entry is removed so the id can be re-Interned to a fresh label.
func (d *IDDict) Delete(id []byte) (label uint32, ok bool) {
	l, ok := d.forward[string(id)]
	if !ok || !d.live[l] {
		return 0, false
	}
	d.live[l] = false
	d.liveN--
	delete(d.forward, string(id))
	return l, true
}

// Len returns the number of live ids.
func (d *IDDict) Len() int { return d.liveN }

// MemBytes approximates the dictionary's resident size. The forward map is
// estimated (Go does not expose its true footprint); the arenas are exact.
func (d *IDDict) MemBytes() int {
	// Rough Go map cost: ~ (len(key)+ ptr + uint32 + bucket overhead). We use a
	// conservative per-entry constant plus the key bytes.
	const mapPerEntry = 48
	var keyBytes int
	for k := range d.forward {
		keyBytes += len(k) + mapPerEntry
	}
	return cap(d.data) + cap(d.ends)*4 + cap(d.live) + keyBytes
}
