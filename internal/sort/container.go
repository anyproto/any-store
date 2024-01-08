package sort

import (
	"bytes"

	"github.com/valyala/fastjson"

	"github.com/anyproto/any-store/internal/encoding"
	"github.com/anyproto/any-store/internal/key"
)

type Container struct {
	ns   *key.NS
	sort Sort
	data [][]byte
}

func (c *Container) Len() int {
	return len(c.data)
}

func (c *Container) Less(i, j int) bool {
	return bytes.Compare(c.data[i], c.data[j]) == -1
}

func (c *Container) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}

func (c *Container) Collect(v *fastjson.Value) {
	if c.ns == nil {
		c.ns = key.NewNS("")
	}
	var k = c.sort.AppendKey(c.ns.GetKey(), v)
	c.data = append(c.data, encoding.AppendJSONValue(k, v.Get("id")))
}
