package index

import (
	"bytes"
	"slices"
	"strings"

	"github.com/dgraph-io/badger/v4"
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
)

type Info struct {
	IndexNS *key.NS
	Fields  []string
	Sparse  bool
}

func (i Info) IndexName() string {
	return strings.Join(i.Fields, "_")
}

func OpenIndex(txn *badger.Txn, info Info) (*Index, error) {
	prefix := info.IndexNS.String()
	ns := key.NewNS(prefix)

	fieldsPath := make([][]string, 0, len(info.Fields))
	for _, fn := range info.Fields {
		fieldsPath = append(fieldsPath, strings.Split(fn, "."))
	}

	stats, err := openStats(txn, key.NewNS(prefix+"/stats").GetKey())
	if err != nil {
		return nil, err
	}

	return &Index{
		Name:       info.IndexName(),
		dataNS:     ns,
		sparse:     info.Sparse,
		fieldPaths: fieldsPath,
		uniqBuf:    make([][]key.Key, len(info.Fields)),
		stats:      stats,
	}, nil
}

type Index struct {
	Name       string
	dataNS     *key.NS
	sparse     bool
	fieldPaths [][]string

	keyBuf      key.Key
	keysBuf     []key.Key
	keysBufPrev []key.Key
	uniqBuf     [][]key.Key
	jvalsBuf    []*fastjson.Value

	stats *indexStats
}

func (idx *Index) Insert(txn *badger.Txn, id []byte, d *fastjson.Value) (err error) {
	idx.fillKeysBuf(d)
	return idx.insertBuf(txn, id)
}

func (idx *Index) Update(txn *badger.Txn, id []byte, prev, new *fastjson.Value) (err error) {
	// calc previous index keys
	idx.fillKeysBuf(prev)

	// copy prev keys to second buffer
	idx.keysBufPrev = slices.Grow(idx.keysBufPrev, len(idx.keysBuf))[:len(idx.keysBuf)]
	for i, k := range idx.keysBuf {
		idx.keysBufPrev[i] = k.CopyTo(idx.keysBufPrev[i][:0])
	}

	// calc new index keys
	idx.fillKeysBuf(new)

	// delete equal keys from both bufs
	idx.keysBuf = slices.DeleteFunc(idx.keysBuf, func(k key.Key) bool {
		for i, pk := range idx.keysBufPrev {
			if bytes.Equal(k, pk) {
				idx.keysBufPrev = slices.Delete(idx.keysBufPrev, i, i+1)
				return true
			}
		}
		return false
	})
	if err = idx.deleteBuf(txn, id, idx.keysBufPrev); err != nil {
		return err
	}
	return idx.insertBuf(txn, id)
}

func (idx *Index) Delete(txn *badger.Txn, id []byte, d *fastjson.Value) (err error) {
	idx.fillKeysBuf(d)
	return idx.deleteBuf(txn, id, idx.keysBuf)
}

func (idx *Index) Drop(txn *badger.Txn) (err error) {
	it := txn.NewIterator(badger.IteratorOptions{
		Prefix: idx.dataNS.Bytes(),
	})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		_ = txn.Delete(it.Item().Key())
	}
	_ = txn.Delete(idx.stats.key)
	return
}

func (idx *Index) FlushStats(txn *badger.Txn) (err error) {
	return idx.stats.flush(txn)
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
				idx.keyBuf = k.AppendJSON(av)
				if idx.isUnique(i, idx.keyBuf[len(k):]) {
					if !idx.writeValues(d, i+1) {
						return false
					}
				}
			}
			return true
		}
	}
	idx.keyBuf = idx.keyBuf.AppendJSON(v)
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
		k = append(k, id...)
		if err = txn.Set(k, nil); err != nil {
			return
		}
	}
	return
}

func (idx *Index) deleteBuf(txn *badger.Txn, id []byte, keys []key.Key) (err error) {
	for _, k := range keys {
		idx.stats.remove()
		if err = txn.Delete(append(k, id...)); err != nil {
			return
		}
	}
	return
}

func (idx *Index) keys(txn *badger.Txn) (ks []key.Key, err error) {
	it := txn.NewIterator(badger.IteratorOptions{
		PrefetchSize:   100,
		PrefetchValues: false,
		Prefix:         idx.dataNS.Bytes(),
	})
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		ks = append(ks, bytes.Clone(it.Item().Key()))
	}
	return
}

type Stats struct {
	Bitmap uint64
	Count  uint64
}

func (idx *Index) Stats() Stats {
	return Stats{
		Bitmap: idx.stats.bitmap.GetCardinality(),
		Count:  idx.stats.count,
	}
}
