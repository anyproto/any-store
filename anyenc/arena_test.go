package anyenc

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testObject(a *Arena) *Value {
	arr := a.NewArray()
	arr.SetArrayItem(0, a.NewNumberFloat64(1))
	arr.SetArrayItem(1, a.NewNumberFloat64(2))
	obj := a.NewObject()
	obj.Set("s", a.NewString("string"))
	obj.Set("n", a.NewNumberFloat64(4.4))
	obj.Set("i", a.NewNumberInt(5))
	obj.Set("a", arr)
	obj.Set("b", a.NewBinary([]byte("binary")))
	return obj
}

func TestArena_Reset(t *testing.T) {
	ap := &ArenaPool{}
	var exp = `{"s":"string","n":4.4,"i":5,"a":[1,2],"b":"YmluYXJ5"}`
	var n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			for range 100 {
				a := ap.Get()
				a.Reset()
				res := testObject(a)
				assert.Equal(t, exp, res.String())
				ap.Put(a)
			}
		}()
	}
	wg.Wait()
}

func BenchmarkArena_Reset(b *testing.B) {
	a := &Arena{}
	b.ReportAllocs()
	for range b.N {
		a.Reset()
		_ = testObject(a)
	}
}
