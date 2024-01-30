package iterator

import (
	"bytes"
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

	if len(i.bounds) > 0 {
		if len(i.bounds[0].Start) == 0 {
			i.bounds[0].Start = i.indexNS.Bytes()
		}
		if len(i.bounds[len(i.bounds)-1].End) == 0 {
			i.bounds[len(i.bounds)-1].End = append(i.indexNS.Bytes(), 255)
		}
	}

	// reverse bounds
	if i.isReverse {
		i.bounds.Reverse()
	}

	var start []byte

	// seek to start of the first bound if needed
	if i.bounds.Len() > 0 && i.bounds[0].Start != nil {
		start = i.bounds[0].Start
	} else {
		if i.isReverse {
			start = i.indexNS.Bytes()
		}
	}
	if i.isReverse {
		start = append(start, 255)
	}
	i.bIter.Seek(start)
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
			if !bound.StartInclude && !i.isReverse {
				return false, append(bound.Start, 255)
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

func (i *indexIterator) boundsToString() (boundsToString query.Bounds) {
	var nilOrStripNS = func(b []byte) []byte {
		if len(b) == 0 {
			return nil
		}
		return b[i.indexNS.Len():]
	}
	for _, bnd := range i.bounds {
		boundsToString = append(boundsToString, query.Bound{
			Start:        nilOrStripNS(bnd.Start),
			End:          nilOrStripNS(bnd.End),
			StartInclude: bnd.StartInclude,
			EndInclude:   bnd.EndInclude,
		})
	}
	return boundsToString
}

func (i *indexIterator) String() string {
	indexName := i.indexNS.String()
	if lastSlash := strings.LastIndex(indexName, "/"); lastSlash != -1 {
		indexName = indexName[lastSlash+1:]
	}

	var result = "INDEX(" + indexName
	boundsToString := i.boundsToString()
	if len(boundsToString) > 0 {
		result += ", " + boundsToString.String()
	}
	if i.isReverse {
		result += ", rev"
	}
	return result + ")"
}
