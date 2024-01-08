package sort

import (
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/valyala/fastjson"
	"golang.org/x/exp/rand"

	"github.com/anyproto/any-store/internal/key"
)

func TestSortField_AppendKey(t *testing.T) {
	st := time.Now()
	c := &Container{
		sort: Sorts{
			&SortField{Path: []string{"a"}, Reverse: false},
			&SortField{Path: []string{"id"}, Reverse: true},
		},
	}
	for i := 0; i < 10; i++ {
		c.Collect(fastjson.MustParse(fmt.Sprintf(`{"id":%d,"a":%d}`, i, rand.Intn(3))))
	}

	srt := time.Now()
	sort.Sort(c)
	t.Log(time.Since(st), time.Since(srt))
	for _, d := range c.data {
		t.Log(key.Key(d).String())
	}
}
