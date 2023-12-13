package anystore

import (
	"hash"
	"slices"

	"github.com/RoaringBitmap/roaring"
	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
)

type Index struct {
	Fields []string
	Sparse bool
}

type indexInfo struct {
	id             string
	collectionName string
	bitmapBase64   string
	sparse         bool
	count          int
}

type index struct {
	ns         key.NS
	fieldPaths [][]string
	count      int
	bitmap     *roaring.Bitmap
	keyBuf     key.Key
	keysBuf    []key.Key
	jvalsBuf   []*fastjson.Value
	sparse     bool
	hash       hash.Hash32
}

func (idx *index) insert(txn *badger.Txn, id []byte, d *fastjson.Value) (err error) {
	idx.fillKeysBuf(d)
	return idx.insertBuf(txn, id)
}

func (idx *index) update(txn *badger.Txn, id []byte, prev, new *fastjson.Value) (err error) {
	idx.fillKeysBuf(prev)
	if err = idx.deleteBuf(txn, id); err != nil {
		return err
	}
	idx.fillKeysBuf(new)
	return idx.insertBuf(txn, id)
}

func (idx *index) delete(txn *badger.Txn, id []byte, d *fastjson.Value) (err error) {
	idx.fillKeysBuf(d)
	return idx.deleteBuf(txn, id)
}

func (idx *index) fillKeysBuf(d *fastjson.Value) {
	idx.keysBuf = idx.keysBuf[:0]
	idx.keyBuf = idx.ns.CopyTo(idx.keyBuf[:0])
	writeKey := func() {
		nl := len(idx.keysBuf) + 1
		idx.keysBuf = slices.Grow(idx.keysBuf, nl)[:nl]
		idx.keysBuf[nl-1] = idx.keyBuf.CopyTo(idx.keysBuf[nl-1][:0])
	}

	keyCount := len(idx.fieldPaths)

	var writeValues func(i int) bool
	writeValues = func(i int) bool {
		if i == keyCount {
			writeKey()
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
				for _, av := range arr {
					idx.keyBuf = encoding.AppendJSONValue(k.AppendPart(nil), av)
					if !writeValues(i + 1) {
						return false
					}
				}
				return true
			}
		}
		idx.keyBuf = encoding.AppendJSONValue(k.AppendPart(nil), v)
		return writeValues(i + 1)
	}

	if !writeValues(0) {
		idx.keysBuf = idx.keysBuf[:0]
	}
}

func (idx *index) insertBuf(txn *badger.Txn, id []byte) (err error) {
	for _, k := range idx.keysBuf {
		idx.hash.Reset()
		_, _ = idx.hash.Write(k)
		idx.bitmap.Add(idx.hash.Sum32())
		k = k.AppendPart(id)
		if err = txn.Set(k, nil); err != nil {
			return
		}
	}
	idx.count += len(idx.keysBuf)
	return
}

func (idx *index) deleteBuf(txn *badger.Txn, id []byte) (err error) {
	for _, k := range idx.keysBuf {
		idx.hash.Reset()
		_, _ = idx.hash.Write(k)
		idx.bitmap.Remove(idx.hash.Sum32())
		k = k.AppendPart(id)
		if err = txn.Delete(k); err != nil {
			return
		}
	}
	idx.count -= len(idx.keysBuf)
	return
}
