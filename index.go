package anystore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/anyproto/any-store/anyenc"
	"github.com/anyproto/any-store/internal/btree"
	"github.com/anyproto/any-store/internal/qplanner"
)

// IndexInfo provides information about an index.
type IndexInfo struct {
	// Name is the name of the index. If empty, it will be generated
	// based on the fields (e.g., "name,-createdDate").
	Name string `json:"name"`

	// Fields are the fields included in the index. Each field can specify
	// ascending (e.g., "name") or descending (e.g., "-createdDate") order.
	Fields []string `json:"fields"`

	// Unique indicates whether the index enforces a unique constraint.
	Unique bool `json:"unique"`

	// Sparse indicates whether the index is sparse, indexing only documents
	// with the specified fields.
	Sparse bool `json:"sparse"`
}

func (i IndexInfo) createName() string {
	return strings.Join(i.Fields, ",")
}

// Index represents an index on a collection.
type Index interface {
	// Info returns the IndexInfo for this index.
	Info() IndexInfo

	// Len returns the length of the index.
	Len(ctx context.Context) (int, error)
}

func indexNsName(collName, indexName string) string {
	return "ix:" + collName + ":" + indexName
}

func newIndex(c *collection, info IndexInfo, ns *btree.Namespace) (idx *index, err error) {
	idx = &index{info: info, c: c, ns: ns}
	if err = idx.init(); err != nil {
		return nil, err
	}
	return
}

type index struct {
	c    *collection
	info IndexInfo
	ns   *btree.Namespace

	fieldNames []string
	fieldPaths [][]string
	reverse    []bool

	sketch         *qplanner.IndexSketch
	sketchModified bool

	keyBuf      anyenc.Tuple
	keysBuf     []anyenc.Tuple
	keysBufPrev []anyenc.Tuple
	uniqBuf     [][]anyenc.Tuple
}

func validateIndexField(s string) (err error) {
	if s == "" || s == "-" {
		return fmt.Errorf("index field is empty")
	}
	if strings.HasPrefix(s, "$") {
		return fmt.Errorf("invalid index field name: %s", s)
	}
	return nil
}

func parseIndexField(s string) (fields []string, reverse bool) {
	if strings.HasPrefix(s, "-") {
		return strings.Split(s[1:], "."), true
	}
	return strings.Split(s, "."), false
}

func (idx *index) init() (err error) {
	for _, field := range idx.info.Fields {
		fields, reverse := parseIndexField(field)
		for _, f := range fields {
			if f == "" {
				return fmt.Errorf("invalid index field: '%s'", field)
			}
		}
		idx.fieldNames = append(idx.fieldNames, strings.Join(fields, "."))
		idx.fieldPaths = append(idx.fieldPaths, fields)
		idx.reverse = append(idx.reverse, reverse)
	}
	idx.uniqBuf = make([][]anyenc.Tuple, len(idx.fieldPaths))
	return nil
}

func (idx *index) Info() IndexInfo {
	return idx.info
}

func (idx *index) Len(ctx context.Context) (count int, err error) {
	err = idx.c.db.doReadTx(ctx, func(tx *btree.ReadTx) error {
		var txErr error
		count, txErr = tx.Count(idx.ns)
		return txErr
	})
	return
}

// insertKeys inserts index entries for the given item into the index namespace.
func (idx *index) insertKeys(tx *btree.WriteTx, it item) error {
	idx.fillKeysBuf(it)
	idKey := it.appendId(nil)
	var valueBuf []byte
	for _, key := range idx.keysBuf {
		if idx.info.Unique {
			// For unique indexes: key = Tuple(v1, v2, ...), value = docId
			// Check if key already exists with a different docId
			var err error
			valueBuf, err = tx.AppendValue(idx.ns, key, valueBuf[:0])
			if err == nil {
				if !bytes.Equal(valueBuf, idKey) {
					return ErrUniqueConstraint
				}
				continue
			}
			if err := tx.Put(idx.ns, key, idKey); err != nil {
				return err
			}
		} else {
			// For non-unique indexes: key = Tuple(v1, v2, ..., docId), value = nil
			fullKey := append(anyenc.Tuple(nil), key...)
			fullKey = append(fullKey, idKey...)
			if err := tx.Put(idx.ns, fullKey, nil); err != nil {
				return err
			}
		}
		if idx.sketch != nil {
			idx.sketch.Increment(key)
			idx.sketchModified = true
		}
	}
	if idx.sketch != nil {
		idx.sketch.IncrementDocCount()
		idx.sketchModified = true
	}
	return nil
}

// deleteKeys deletes index entries for the given item from the index namespace.
func (idx *index) deleteKeys(tx *btree.WriteTx, it item) error {
	idx.fillKeysBuf(it)
	idKey := it.appendId(nil)
	for _, key := range idx.keysBuf {
		if idx.info.Unique {
			if err := tx.Delete(idx.ns, key); err != nil {
				if !errors.Is(err, btree.ErrKeyNotFound) {
					return err
				}
			}
		} else {
			fullKey := append(anyenc.Tuple(nil), key...)
			fullKey = append(fullKey, idKey...)
			if err := tx.Delete(idx.ns, fullKey); err != nil {
				if !errors.Is(err, btree.ErrKeyNotFound) {
					return err
				}
			}
		}
		if idx.sketch != nil {
			idx.sketch.Decrement(key)
			idx.sketchModified = true
		}
	}
	if idx.sketch != nil {
		idx.sketch.DecrementDocCount()
		idx.sketchModified = true
	}
	return nil
}

func (idx *index) writeKey() {
	nl := len(idx.keysBuf) + 1
	idx.keysBuf = slices.Grow(idx.keysBuf, nl)[:nl]
	idx.keysBuf[nl-1] = append(idx.keysBuf[nl-1][:0], idx.keyBuf...)
}

func (idx *index) writeValues(d *anyenc.Value, i int) bool {
	if i == len(idx.fieldPaths) {
		idx.writeKey()
		return true
	}
	v := d.Get(idx.fieldPaths[i]...)
	if idx.info.Sparse && (v == nil || v.Type() == anyenc.TypeNull) {
		return false
	}

	k := idx.keyBuf
	if v != nil && v.Type() == anyenc.TypeArray {
		arr, _ := v.Array()
		if len(arr) != 0 {
			idx.uniqBuf[i] = idx.uniqBuf[i][:0]
			for _, av := range arr {
				idx.keyBuf = av.MarshalTo(k)
				if idx.isUnique(i, idx.keyBuf) {
					if !idx.writeValues(d, i+1) {
						return false
					}
				}
			}
		}
	}

	idx.keyBuf = v.MarshalTo(k)
	return idx.writeValues(d, i+1)
}

func (idx *index) fillKeysBuf(it item) {
	idx.keysBuf = idx.keysBuf[:0]
	idx.keyBuf = idx.keyBuf[:0]
	idx.resetUnique()
	if !idx.writeValues(it.Value(), 0) {
		idx.keysBuf = idx.keysBuf[:0]
	}
}

func (idx *index) resetUnique() {
	for i := range idx.uniqBuf {
		idx.uniqBuf[i] = idx.uniqBuf[i][:0]
	}
}

func (idx *index) isUnique(i int, k anyenc.Tuple) bool {
	for _, ek := range idx.uniqBuf[i] {
		if bytes.Equal(k, ek) {
			return false
		}
	}
	nl := len(idx.uniqBuf[i]) + 1
	idx.uniqBuf[i] = slices.Grow(idx.uniqBuf[i], nl)[:nl]
	idx.uniqBuf[i][nl-1] = append(idx.uniqBuf[i][nl-1][:0], k...)
	return true
}

func (idx *index) Close() (err error) {
	return
}
