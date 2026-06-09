package vindex

// candidate is a (distance, label) pair used by the search frontier.
type candidate struct {
	dist  float32
	label uint32
}

// cheap is a binary heap of candidates; min-heap (closest at root) when max is
// false, max-heap (farthest at root) when true. Backing slice is reused across
// the search via the searcher.
type cheap struct {
	s   []candidate
	max bool
}

func (h *cheap) reset(max bool) {
	h.s = h.s[:0]
	h.max = max
}

func (h *cheap) len() int { return len(h.s) }

func (h *cheap) less(i, j int) bool {
	if h.max {
		return h.s[i].dist > h.s[j].dist
	}
	return h.s[i].dist < h.s[j].dist
}

func (h *cheap) push(c candidate) {
	h.s = append(h.s, c)
	i := len(h.s) - 1
	for i > 0 {
		p := (i - 1) / 2
		if !h.less(i, p) {
			break
		}
		h.s[i], h.s[p] = h.s[p], h.s[i]
		i = p
	}
}

func (h *cheap) peek() candidate { return h.s[0] }

func (h *cheap) pop() candidate {
	n := len(h.s) - 1
	top := h.s[0]
	h.s[0] = h.s[n]
	h.s = h.s[:n]
	i := 0
	for {
		l, r := 2*i+1, 2*i+2
		small := i
		if l < n && h.less(l, small) {
			small = l
		}
		if r < n && h.less(r, small) {
			small = r
		}
		if small == i {
			break
		}
		h.s[i], h.s[small] = h.s[small], h.s[i]
		i = small
	}
	return top
}
