package index

import (
	"bytes"
	"slices"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
)

type Info struct {
	CollectionNS key.NS
	Fields       []string
	Sparse       bool
}

func (i Info) IndexName() string {
	return strings.Join(i.Fields, "_")
}

func OpenIndex(txn *badger.Txn, info Info) (*Index, error) {
	var k key.Key
	k = info.CollectionNS.CopyTo(k).AppendString("index").AppendString(info.IndexName())

	fieldsPath := make([][]string, 0, len(info.Fields))
	for _, fn := range info.Fields {
		fieldsPath = append(fieldsPath, strings.Split(fn, "."))
	}

	stats, err := openStats(txn, k.Copy().AppendString("stats"))
	if err != nil {
		return nil, err
	}

	return &Index{
		dataNS:     key.NewNS(k),
		sparse:     info.Sparse,
		fieldPaths: fieldsPath,
		uniqBuf:    make([][]key.Key, len(info.Fields)),
		stats:      stats,
	}, nil
}

type Index struct {
	dataNS     key.NS
	sparse     bool
	fieldPaths [][]string

	keyBuf   key.Key
	keysBuf  []key.Key
	uniqBuf  [][]key.Key
	jvalsBuf []*fastjson.Value

	stats *indexStats
}

func (idx *Index) Insert(txn *badger.Txn, id []byte, d *fastjson.Value) (err error) {
	idx.fillKeysBuf(d)
	return idx.insertBuf(txn, id)
}

func (idx *Index) Update(txn *badger.Txn, id []byte, prev, new *fastjson.Value) (err error) {
	idx.fillKeysBuf(prev)
	if err = idx.deleteBuf(txn, id); err != nil {
		return err
	}
	idx.fillKeysBuf(new)
	return idx.insertBuf(txn, id)
}

func (idx *Index) Delete(txn *badger.Txn, id []byte, d *fastjson.Value) (err error) {
	idx.fillKeysBuf(d)
	return idx.deleteBuf(txn, id)
}

func (idx *Index) FlushStats(txn *badger.Txn) (err error) {
	return
}

func (idx *Index) writeKey() {
	nl := len(idx.keysBuf) + 1
	idx.keysBuf = slices.Grow(idx.keysBuf, nl)[:nl]
	idx.keysBuf[nl-1] = idx.keyBuf.CopyTo(idx.keysBuf[nl-1][:0])
}

func (idx *Index) writeValues(d *fastjson.Value, i int) bool {
	if i == len(idx.fieldPaths) {
		idx.writeKey()
		return true
	}
	v := d.Get(idx.fieldPaths[i]...)
	if v == nil && idx.sparse {
		return false
	}

	k := idx.keyBuf
	if v != nil && v.Type() == fastjson.TypeArray {
		arr, _ := v.Array()
		if len(arr) != 0 {
			idx.uniqBuf[i] = idx.uniqBuf[i][:0]
			for _, av := range arr {
				idx.keyBuf = encoding.AppendJSONValue(k.AppendPart(nil), av)
				if idx.isUnique(i, idx.keyBuf[len(k):]) {
					if !idx.writeValues(d, i+1) {
						return false
					}
				}
			}
			return true
		}
	}
	idx.keyBuf = encoding.AppendJSONValue(k.AppendPart(nil), v)
	return idx.writeValues(d, i+1)
}
func (idx *Index) fillKeysBuf(d *fastjson.Value) {
	idx.keysBuf = idx.keysBuf[:0]
	idx.keyBuf = idx.dataNS.CopyTo(idx.keyBuf[:0])
	idx.resetUnique()
	if !idx.writeValues(d, 0) {
		// we got false in case sparse index and nil value - reset the buffer
		idx.keysBuf = idx.keysBuf[:0]
	}
}

func (idx *Index) resetUnique() {
	for i := range idx.uniqBuf {
		idx.uniqBuf[i] = idx.uniqBuf[i][:0]
	}
}

func (idx *Index) isUnique(i int, k key.Key) bool {
	for _, ek := range idx.uniqBuf[i] {
		if bytes.Equal(k, ek) {
			return false
		}
	}
	nl := len(idx.uniqBuf[i]) + 1
	idx.uniqBuf[i] = slices.Grow(idx.uniqBuf[i], nl)[:nl]
	idx.uniqBuf[i][nl-1] = k.CopyTo(idx.uniqBuf[i][nl-1][:0])
	return true
}

func (idx *Index) insertBuf(txn *badger.Txn, id []byte) (err error) {
	for _, k := range idx.keysBuf {
		idx.stats.addKey(k)
		k = k.AppendPart(id)
		if err = txn.Set(k, nil); err != nil {
			return
		}
	}
	return
}

func (idx *Index) deleteBuf(txn *badger.Txn, id []byte) (err error) {
	for _, k := range idx.keysBuf {
		idx.stats.remove()
		if err = txn.Delete(k.AppendPart(id)); err != nil {
			return
		}
	}
	return
}
