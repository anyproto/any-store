package iterator

func NewUniqIdIterator(source IdIterator) IdIterator {
	return &uniqIterator{
		uniq:       make(map[string]struct{}),
		IdIterator: source,
	}
}

type uniqIterator struct {
	uniq map[string]struct{}
	IdIterator
}

func (u uniqIterator) Next() bool {
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
