package iterator

import (
	"bytes"
	"slices"
	"strings"

	"github.com/dgraph-io/badger/v4"

	"github.com/anyproto/any-store/internal/key"
	"github.com/anyproto/any-store/internal/qcontext"
	"github.com/anyproto/any-store/query"
)

// NewIndexIterator creates IdIterator over the index. Requires uniq filtering.
func NewIndexIterator(qCtx *qcontext.QueryContext, indexNs *key.NS, bounds query.Bounds, isReverse bool) IdIterator {
	idxIt := &indexIterator{
		qCtx:      qCtx,
		indexNS:   indexNs,
		isReverse: isReverse,
		bounds:    bounds,
	}
	return NewUniqIdIterator(idxIt)
}

type indexIterator struct {
	qCtx *qcontext.QueryContext

	indexNS   *key.NS
	isReverse bool

	bounds       query.Bounds
	currentBound int
	currentId    []byte
	currentItem  *badger.Item

	bIter *badger.Iterator
	valid bool

	shouldCallNext bool
	err            error
}

func (i *indexIterator) Next() bool {
	if i.err != nil {
		return false
	}
	if i.bIter == nil {
		i.prepare()
	}
	if !i.valid {
		return false
	}
	if i.shouldCallNext {
		i.bIter.Next()
		i.shouldCallNext = false
	}
	for i.bIter.Valid() {
		it := i.bIter.Item()
		boundCheck, seek := i.checkNextBound(it)
		//fmt.Println("check bound", key.Key(it.Key()).String(), boundCheck, key.Key(seek).String(), i.bounds.String())
		if boundCheck {
			if i.err = i.setCurrentItem(it); i.err != nil {
				return false
			}
			i.shouldCallNext = true
			return true
		} else {
			if !i.valid {
				return false
			}
			if seek != nil {
				i.bIter.Seek(seek)
			} else {
				i.bIter.Next()
			}
		}
	}
	return false
}

func (i *indexIterator) setCurrentItem(it *badger.Item) (err error) {
	i.currentItem = it
	return key.Key(it.Key()).ReadByteValues(i.indexNS, func(b []byte) error {
		i.currentId = b
		return nil
	})
}

func (i *indexIterator) Valid() bool {
	if !i.valid {
		return false
	}
	return i.bIter.Valid()
}

func (i *indexIterator) prepare() {
	i.bIter = i.qCtx.Txn.NewIterator(badger.IteratorOptions{
		Reverse: i.isReverse,
		Prefix:  i.indexNS.Bytes(),
	})

	i.indexNS.ReuseKey(func(k key.Key) key.Key {
		i.bounds.SetPrefix(k)
		return k
	})

	// reverse bounds
	if i.isReverse {
		i.bounds.Reverse()
	}

	var start []byte

	// seek to start of the first bound if needed
	if i.bounds.Len() > 0 {
		start = i.bounds[0].Start
	} else {
		if i.isReverse {
			start = append(i.indexNS.Bytes(), 255)
		}
	}
	i.bIter.Seek(start)
	//fmt.Println("prepare seek", key.Key(start).String(), i.boundsToString().String())
	i.valid = true
}

func (i *indexIterator) checkNextBound(it *badger.Item) (pass bool, seek []byte) {
	if i.bounds.Len() == 0 {
		return true, nil
	}
	var (
		k = key.Key(it.Key())
	)

	// select right bound
	for {
		// all bounds iterated - exit
		if len(i.bounds)-1 < i.currentBound {
			i.valid = false
			return false, nil
		}

		bound := i.bounds[i.currentBound]

		// seek to the start of new bound
		if i.less(k, bound.Start, bound.StartInclude) {
			if !bound.StartInclude {
				if i.isReverse {
					return false, decr(slices.Clone(bound.Start))
				} else {
					return false, append(bound.Start, 255)
				}
			}
			return false, bound.Start
		}

		// if k > bound.End - check the next bound
		if i.less(bound.End, k, bound.EndInclude) {
			i.currentBound++
			continue
		} else {
			return true, nil
		}
	}

}

func (i *indexIterator) Close() error {
	if i.bIter != nil {
		i.bIter.Close()
	}
	i.valid = false
	return i.err
}

func (i *indexIterator) CurrentId() []byte {
	return i.currentId
}

func (i *indexIterator) less(a, b []byte, orEqual bool) bool {
	m := min(len(a), len(b))
	switch bytes.Compare(a[:m], b[:m]) {
	case 0:
		return !orEqual
	case -1:
		return !i.isReverse
	}
	return i.isReverse
}

func (i *indexIterator) String() string {
	indexName := i.indexNS.String()
	if lastSlash := strings.LastIndex(indexName, "/"); lastSlash != -1 {
		indexName = indexName[lastSlash+1:]
	}

	var result = "INDEX(" + indexName
	boundsToString := i.bounds.String()
	if len(boundsToString) > 0 {
		result += ", " + boundsToString
	}
	if i.isReverse {
		result += ", rev"
	}
	return result + ")"
}

func decr(b []byte) []byte {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != 0 {
			b[i]--
			return b
		}
	}
	return b
}
