package iterator

type UniqIterator struct {
	uniq map[string]struct{}
	IdIterator
}

func (u UniqIterator) Next() bool {
	for u.IdIterator.Next() {
		vals := u.Values()
		val := string(vals[len(vals)-1])
		_, ok := u.uniq[val]
		if ok {
			continue
		}
		u.uniq[val] = struct{}{}
		return true
	}
	return false
}
