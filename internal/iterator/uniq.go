package iterator

type UniqIterator struct {
	uniq map[string]struct{}
	IdIterator
}

func (u UniqIterator) Next() bool {
	for u.IdIterator.Next() {
		id := string(u.IdIterator.CurrentId())
		_, ok := u.uniq[id]
		if ok {
			continue
		}
		u.uniq[id] = struct{}{}
		return true
	}
	return false
}
