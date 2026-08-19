package vector

// candidate is a (distance, internal-id) pair used by the flat HNSW search
// frontier. Kept tiny (8 bytes) so the heaps stay cache-friendly.
type candidate struct {
	dist float32
	id   uint32
}

// cheap is a binary heap of candidates. When max is true it is a max-heap
// (root = farthest), otherwise a min-heap (root = closest). The flat HNSW
// search keeps a min-heap of nodes still to expand and a max-heap of the best
// results found so far — the classic two-heap HNSW layer search.
//
// The backing slice is reused across searches via heapPool to keep the hot
// search path allocation-free.
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
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h.s[i], h.s[parent] = h.s[parent], h.s[i]
		i = parent
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
		smallest := i
		if l < n && h.less(l, smallest) {
			smallest = l
		}
		if r < n && h.less(r, smallest) {
			smallest = r
		}
		if smallest == i {
			break
		}
		h.s[i], h.s[smallest] = h.s[smallest], h.s[i]
		i = smallest
	}
	return top
}

// visitedList is an epoch-stamped "set" of node ids. Instead of clearing N
// bytes per search, we bump an epoch counter and treat mark[id]==epoch as
// "visited". Clearing only happens on epoch wraparound. This is a common arena
// trick that turns the per-query visited set from an allocation + map into a
// reusable flat array.
type visitedList struct {
	mark  []uint32
	epoch uint32
}

func (v *visitedList) prepare(n int) {
	if cap(v.mark) < n {
		v.mark = make([]uint32, n)
		v.epoch = 0
	}
	v.mark = v.mark[:n]
	v.epoch++
	if v.epoch == 0 { // wrapped: clear and restart
		for i := range v.mark {
			v.mark[i] = 0
		}
		v.epoch = 1
	}
}

// visit marks id and reports whether it was newly visited.
func (v *visitedList) visit(id uint32) bool {
	if v.mark[id] == v.epoch {
		return false
	}
	v.mark[id] = v.epoch
	return true
}
