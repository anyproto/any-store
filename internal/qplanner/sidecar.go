package qplanner

// FloatSidecar is an insert-only open-addressing map from a docId byte key to
// a float64, used for the per-document distance/score sidecars of vector and
// full-text plans. It replaces a map[string]float64 whose every insert
// allocated a string key: here keys are appended to one grow-only arena and
// slots carry (hash, offset, length), so a plan's whole sidecar costs a
// handful of amortized allocations. Same flat-table pattern as
// internal/vivf's u32fmap and the FTS read path's ftsScoreAcc, adapted to
// variable-length byte keys.
type FloatSidecar struct {
	hash  []uint64 // cached FNV-1a of the slot's key
	koff  []uint32 // key start in arena
	klen  []uint32 // key length; 0 = empty slot (docIds are never empty)
	val   []float64
	arena []byte
	mask  uint32
	n     int
}

// fnv1a64 hashes key with FNV-1a (inlined to keep the hot path
// dependency-free and allocation-free).
func fnv1a64(key []byte) uint64 {
	const offset64, prime64 = 14695981039346656037, 1099511628211
	h := uint64(offset64)
	for _, b := range key {
		h ^= uint64(b)
		h *= prime64
	}
	return h
}

func (s *FloatSidecar) init(size int) {
	s.hash = make([]uint64, size)
	s.koff = make([]uint32, size)
	s.klen = make([]uint32, size)
	s.val = make([]float64, size)
	s.mask = uint32(size - 1)
}

// Set inserts or updates key's value. The key bytes are copied.
func (s *FloatSidecar) Set(key []byte, v float64) {
	if s.klen == nil {
		s.init(64)
	}
	h := fnv1a64(key)
	pos := uint32(h) & s.mask
	for s.klen[pos] != 0 {
		if s.hash[pos] == h && s.keyAt(pos, key) {
			s.val[pos] = v
			return
		}
		pos = (pos + 1) & s.mask
	}
	if s.n >= int(s.mask-s.mask/4) { // keep load < 0.75
		s.grow()
		pos = uint32(h) & s.mask
		for s.klen[pos] != 0 {
			pos = (pos + 1) & s.mask
		}
	}
	s.hash[pos] = h
	s.koff[pos] = uint32(len(s.arena))
	s.klen[pos] = uint32(len(key))
	s.arena = append(s.arena, key...)
	s.val[pos] = v
	s.n++
}

// Get returns key's value and whether it is present.
func (s *FloatSidecar) Get(key []byte) (float64, bool) {
	if s.n == 0 {
		return 0, false
	}
	h := fnv1a64(key)
	pos := uint32(h) & s.mask
	for s.klen[pos] != 0 {
		if s.hash[pos] == h && s.keyAt(pos, key) {
			return s.val[pos], true
		}
		pos = (pos + 1) & s.mask
	}
	return 0, false
}

// keyAt reports whether the key stored in slot pos equals key.
func (s *FloatSidecar) keyAt(pos uint32, key []byte) bool {
	if int(s.klen[pos]) != len(key) {
		return false
	}
	return string(s.arena[s.koff[pos]:s.koff[pos]+s.klen[pos]]) == string(key)
}

func (s *FloatSidecar) grow() {
	size := len(s.klen) * 2
	ohash, okoff, oklen, oval := s.hash, s.koff, s.klen, s.val
	s.init(size)
	for i := range oklen {
		if oklen[i] == 0 {
			continue
		}
		pos := uint32(ohash[i]) & s.mask
		for s.klen[pos] != 0 {
			pos = (pos + 1) & s.mask
		}
		s.hash[pos] = ohash[i]
		s.koff[pos] = okoff[i]
		s.klen[pos] = oklen[i]
		s.val[pos] = oval[i]
	}
}
