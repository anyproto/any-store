package anyenc

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObject_Get(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	obj.Set("k", a.NewNumberInt(1))
	obj.Set("k", a.NewNumberInt(2))
	assert.Equal(t, 2, obj.GetInt("k"))
}

func TestObject_Len(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	assert.Equal(t, 0, obj.GetObject().Len())
	obj.Set("k", a.NewNumberInt(1))
	assert.Equal(t, 1, obj.GetObject().Len())
}

func TestObject_Del(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	obj.Set("k", a.NewNumberInt(1))
	obj.Set("k2", a.NewNumberInt(2))
	obj.Del("k")
	assert.Nil(t, obj.Get("k"))
}

func TestObject_Visit(t *testing.T) {
	a := &Arena{}
	obj := a.NewObject()
	obj.Set("k", a.NewNumberInt(1))
	obj.Set("k2", a.NewNumberInt(2))
	var keys []string
	var values []int
	obj.GetObject().Visit(func(k []byte, v *Value) {
		keys = append(keys, string(k))
		values = append(values, v.GetInt())
	})
	assert.Equal(t, []string{"k", "k2"}, keys)
	assert.Equal(t, []int{1, 2}, values)
}
