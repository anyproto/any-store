package anystore

import (
	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/key"
)

func newSystemCollection(db *DB) (*systemCollection, error) {
	sc := &systemCollection{Collection: &Collection{db: db, name: "_system"}}
	sc.dataNS = key.NewNS(nsPrefix.String() + "/_system")
	return sc, nil
}

type systemCollection struct {
	*Collection
}

func (sc *systemCollection) AddIndex(v *fastjson.Value) (err error) {
	_, err = sc.InsertOne(v)
	return
}

func (sc *systemCollection) Indexes(collName string) (indexes []Index, err error) {
	it, err := sc.Find().Cond(map[string]string{"collectionName": collName}).Iter()
	if err != nil {
		return nil, err
	}
	defer it.Close()
	for it.Next() {
		if err = it.Item().DecodeFastJSON(func(v *fastjson.Value) error {
			idx, err := indexFromJSON(v)
			if err != nil {
				return err
			}
			indexes = append(indexes, idx)
			return nil
		}); err != nil {
			return
		}
	}
	return
}
