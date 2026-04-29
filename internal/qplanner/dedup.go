package qplanner

// DocDedup is a zero-cost dedup helper for iterator-chain consumers.
// Stack-allocate one per query loop; the seen-set map is created lazily
// on the first multiKey=true entry.
//
// Usage:
//
//	var dedup DocDedup
//	for {
//	    _, docId, multiKey, err := iter.Next()
//	    if err != nil { return err }
//	    if docId == nil { break }
//	    if !dedup.Accept(docId, multiKey) { continue }
//	    // ... count or yield docId
//	}
//
// Properties:
//   - Zero allocation when every entry has multiKey=false (a single-bound
//     scalar or unique-index stream). The seen field stays nil.
//   - One map allocation (initial cap 64) on the first multiKey=true entry.
//   - O(distinct multi-key results) memory thereafter.
//
// Not safe for concurrent use; callers own the lifetime.
type DocDedup struct {
	seen map[string]struct{}
}

// Accept reports whether docId should be counted/yielded. multiKey=false
// is a guarantee from the iterator that this docId won't reappear, so
// Accept always returns true and never touches the map. multiKey=true
// triggers lazy map allocation and a dedup check; true is returned for
// the first sighting of each docId, false for repeats.
func (d *DocDedup) Accept(docId []byte, multiKey bool) bool {
	if !multiKey {
		return true
	}
	if d.seen == nil {
		d.seen = make(map[string]struct{}, 64)
	}
	if _, dup := d.seen[string(docId)]; dup {
		return false
	}
	d.seen[string(docId)] = struct{}{}
	return true
}

// Reset clears the internal seen-set without releasing the underlying
// map, allowing the same DocDedup to be reused across repeated query
// loops. Cheaper than reallocating a new map for each pass when the
// caller knows the steady-state size.
func (d *DocDedup) Reset() {
	for k := range d.seen {
		delete(d.seen, k)
	}
}
