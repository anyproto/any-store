package index

import (
	"bytes"
	"encoding/binary"
	"hash"
	"hash/fnv"

	"github.com/RoaringBitmap/roaring"
	"github.com/dgraph-io/badger/v4"

	"github.com/anyproto/any-store/internal/key"
)

func openStats(txn *badger.Txn, k key.Key) (*indexStats, error) {
	is := &indexStats{
		key:    k,
		bitmap: roaring.New(),
		hash:   fnv.New32a(),
	}
	if txn != nil {
		it, err := txn.Get(k)
		if err != nil {
			if err != badger.ErrKeyNotFound {
				return nil, err
			}
		}

		if it != nil {
			if err = it.Value(func(val []byte) error {
				if len(val) < 8 {
					return nil
				}
				is.count = binary.LittleEndian.Uint64(val)
				return is.bitmap.UnmarshalBinary(val[8:])
			}); err != nil {
				return nil, err
			}
		}
	}
	return is, nil
}

type indexStats struct {
	key    key.Key
	count  uint64
	hash   hash.Hash32
	bitmap *roaring.Bitmap
}

func (is *indexStats) addKey(k key.Key) {
	is.hash.Reset()
	_, _ = is.hash.Write(k)
	h := is.hash.Sum32()
	is.bitmap.Add(h)
	is.count++
}

func (is *indexStats) remove() {
	is.count--
}

func (is *indexStats) flush(txn *badger.Txn) (err error) {
	bb := bytes.NewBuffer(make([]byte, 0, 8+is.bitmap.GetSerializedSizeInBytes()))
	bb.Write(binary.LittleEndian.AppendUint64(nil, is.count))
	if _, err = is.bitmap.WriteTo(bb); err != nil {
		return err
	}
	return txn.Set(is.key, bb.Bytes())
}
