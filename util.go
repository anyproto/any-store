package anystore

import (
	"context"
	"fmt"
	"strings"

	"github.com/anyproto/any-store/syncpool"
)

func copyItem(buf *syncpool.DocBuffer, it item) item {
	res, _ := buf.Parser.Parse(it.val.MarshalTo(buf.DocBuf[:0]))
	return item{val: res}
}

func stringArrayToJson(array []string) string {
	return fmt.Sprintf(`[%s]`, strings.Join(func() []string {
		result := make([]string, len(array))
		for i, s := range array {
			result[i] = fmt.Sprintf(`"%s"`, s)
		}
		return result
	}(), ","))
}

func assertCollCount(t interface {
	Helper()
	Errorf(format string, args ...any)
}, coll Collection, expected int) {
	t.Helper()
	count, err := coll.Count(context.Background())
	if err != nil {
		t.Errorf("count error: %v", err)
		return
	}
	if count != expected {
		t.Errorf("expected count %d, got %d", expected, count)
	}
}

func assertCollCountCtx(ctx context.Context, t interface {
	Helper()
	Errorf(format string, args ...any)
}, coll Collection, expected int) {
	t.Helper()
	count, err := coll.Find(nil).Count(ctx)
	if err != nil {
		t.Errorf("count error: %v", err)
		return
	}
	if count != expected {
		t.Errorf("expected count %d, got %d", expected, count)
	}
}
