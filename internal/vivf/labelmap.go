package vivf

// u32fmap is a flat open-addressing map from a dense uint32 label to a float32
// value, keeping the minimum on collision, with O(1) generation reset. It is the
// value-carrying sibling of internal/vindex's u32set (same rationale: labels are
// dense, so linear probing over a power-of-two table with the lowbias32 mix beats
// the Go map's hashing, and reset bumps a tag instead of clearing). Owned by a
// pooled searcher and reused across queries; pointer-free (no GC scan cost).
//
// It replaces the per-query map[uint32]float32 used to dedup closure candidates by
// keeping each label's best ADC distance.
type u32fmap struct {
	key  []uint32
	val  []float32
	seen []uint32 // slot occupied iff seen[i] == gen
	gen  uint32
	mask uint32
	n    int
}

// hashU32 is the lowbias32 finalizer (same mix internal/vindex uses).
func hashU32(x uint32) uint32 {
	x ^= x >> 16
	x *= 0x7feb352d
	x ^= x >> 15
	x *= 0x846ca68b
	x ^= x >> 16
	return x
}

// reset clears the map in O(1) after the first call (bumps the generation).
func (m *u32fmap) reset() {
	if m.key == nil {
		const init = 1024 // power of two; grows on demand
		m.key = make([]uint32, init)
		m.val = make([]float32, init)
		m.seen = make([]uint32, init)
		m.mask = uint32(init - 1)
		m.gen = 1
		m.n = 0
		return
	}
	m.n = 0
	m.gen++
	if m.gen == 0 { // wrapped: stale tags could false-hit, so clear once
		for i := range m.seen {
			m.seen[i] = 0
		}
		m.gen = 1
	}
}

// putMin inserts label->d, or lowers an existing label's value to d if smaller.
func (m *u32fmap) putMin(label uint32, d float32) {
	pos := hashU32(label) & m.mask
	for m.seen[pos] == m.gen {
		if m.key[pos] == label {
			if d < m.val[pos] {
				m.val[pos] = d
			}
			return
		}
		pos = (pos + 1) & m.mask
	}
	if m.n >= int(m.mask-m.mask/4) { // keep load < 0.75
		m.grow()
		pos = hashU32(label) & m.mask
		for m.seen[pos] == m.gen {
			pos = (pos + 1) & m.mask
		}
	}
	m.seen[pos] = m.gen
	m.key[pos] = label
	m.val[pos] = d
	m.n++
}

// collect appends every live (label, value) pair to dst (reset to length 0).
func (m *u32fmap) collect(dst []cand) []cand {
	dst = dst[:0]
	if m.key == nil {
		return dst
	}
	for i := range m.key {
		if m.seen[i] == m.gen {
			dst = append(dst, cand{m.key[i], m.val[i]})
		}
	}
	return dst
}

func (m *u32fmap) grow() {
	size := len(m.key) * 2
	nkey := make([]uint32, size)
	nval := make([]float32, size)
	nseen := make([]uint32, size)
	nmask := uint32(size - 1)
	g := m.gen
	for i := range m.key {
		if m.seen[i] != g {
			continue
		}
		p := hashU32(m.key[i]) & nmask
		for nseen[p] == g {
			p = (p + 1) & nmask
		}
		nseen[p] = g
		nkey[p] = m.key[i]
		nval[p] = m.val[i]
	}
	m.key, m.val, m.seen, m.mask = nkey, nval, nseen, nmask
}
