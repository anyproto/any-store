package anystore

import (
	"context"
	"os"
	"testing"
)

// skipIfInMemory skips a test that the in-memory backend cannot run; reason
// says why, in the caller's terms.
func skipIfInMemory(t testing.TB, reason string) {
	t.Helper()
	if os.Getenv("ANYSTORE_TEST_INMEMORY") == "1" {
		t.Skip(reason)
	}
}

// assertCollCount counts through Collection.Count, which reads the namespace
// directly. For the planner's count see assertCollCountInTx or assertQueryCount.
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

// assertCollCountInTx counts through Find(nil), the planner path, in the
// transaction ctx carries — a different path from assertCollCount.
func assertCollCountInTx(ctx context.Context, t interface {
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
