package sort

import (
	"bytes"
	"sort"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
)

func NewContainer(sorts Sort) *Container {
	return &Container{
		sort: sorts,
	}
}

type Container struct {
	NS   *key.NS
	sort Sort
	Data [][]byte
}

func (c *Container) Len() int {
	return len(c.Data)
}

func (c *Container) Less(i, j int) bool {
	return bytes.Compare(c.Data[i], c.Data[j]) == -1
}

func (c *Container) Swap(i, j int) {
	c.Data[i], c.Data[j] = c.Data[j], c.Data[i]
}

func (c *Container) Collect(v *fastjson.Value) {
	if c.NS == nil {
		c.NS = key.NewNS("")
	}
	var k = c.sort.AppendKey(c.NS.GetKey(), v)
	c.Data = append(c.Data, encoding.AppendJSONValue(k, v.Get("id")))
}

func (c *Container) Sort() {
	sort.Sort(c)
}
