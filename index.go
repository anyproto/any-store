package anystore

import (
	"strings"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/index"
	"github.com/anyproto/any-store/internal/key"
)

type Index struct {
	Fields []string
	Sparse bool
}

func (idx Index) Name() string {
	return strings.Join(idx.Fields, "_")
}

func (idx Index) internalInfo(ns *key.NS) index.Info {
	return index.Info{
		IndexNS: key.NewNS(ns.String() + "/" + idx.Name()),
		Fields:  idx.Fields,
		Sparse:  idx.Sparse,
	}
}

func (idx Index) ToJSON(collName string) *fastjson.Value {
	a := &fastjson.Arena{}
	obj := a.NewObject()
	id := a.NewString(collName + "/" + idx.Name())
	obj.Set("id", id)
	obj.Set("collectionName", a.NewString(collName))
	fields := a.NewArray()
	for i, f := range idx.Fields {
		fields.SetArrayItem(i, a.NewString(f))
	}
	obj.Set("fields", fields)
	if idx.Sparse {
		obj.Set("sparse", a.NewTrue())
	} else {
		obj.Set("sparse", a.NewFalse())
	}
	return obj
}

func indexFromJSON(v *fastjson.Value) (idx Index, err error) {
	for _, jf := range v.GetArray("fields") {
		fs, err := jf.StringBytes()
		if err != nil {
			return Index{}, err
		}
		idx.Fields = append(idx.Fields, string(fs))
	}
	idx.Sparse = v.GetBool("sparse")
	return idx, nil
}
