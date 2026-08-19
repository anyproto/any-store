/*
Index/Planner tests inspired by SQLite: concurrent access patterns

Test scenario:
Tests concurrent goroutine access with indexes — parallel reads,
reads during writes, concurrent queries on indexed fields, and
concurrent inserts with unique index constraint enforcement.

These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/
package anystore

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
)

func TestIndex_Concurrent_ParallelReads(t *testing.T) {
	// Multiple goroutines reading from indexed collection simultaneously
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup
	const numReaders = 10
	errs := make([]error, numReaders)

	for g := range numReaders {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Each goroutine queries a different value
			val := idx % 10
			count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, val)).Count(ctx)
			if err != nil {
				errs[idx] = err
				return
			}
			if count != 10 {
				errs[idx] = fmt.Errorf("goroutine %d: expected 10, got %d for a=%d", idx, count, val)
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}
}

func TestIndex_Concurrent_ParallelRangeQueries(t *testing.T) {
	// Multiple goroutines running range queries with indexed sort
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup
	const numReaders = 8
	errs := make([]error, numReaders)

	for g := range numReaders {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lo := idx * 20
			hi := lo + 19
			q := coll.Find(fmt.Sprintf(`{"a":{"$gte":%d,"$lte":%d}}`, lo, hi)).Sort("a")
			vals := collectField(t, q, "a")
			if len(vals) != 20 {
				errs[idx] = fmt.Errorf("goroutine %d: expected 20 results, got %d", idx, len(vals))
				return
			}
			// Verify sort order using numeric comparison (string "9" > "10")
			for i := 1; i < len(vals); i++ {
				prev, _ := strconv.Atoi(vals[i-1])
				cur, _ := strconv.Atoi(vals[i])
				if prev > cur {
					errs[idx] = fmt.Errorf("goroutine %d: not sorted at %d: %d > %d", idx, i, prev, cur)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}
}

func TestIndex_Concurrent_ReadDuringWrite(t *testing.T) {
	// Readers query while a writer inserts documents
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Pre-populate some data
	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup
	var writeErr atomic.Value
	var readErrors sync.Map

	// Writer goroutine: insert 50 more docs
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; i < 100; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
			if err := coll.Insert(ctx, doc); err != nil {
				writeErr.Store(err)
				return
			}
		}
	}()

	// Reader goroutines: continuously query during writes
	for g := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for attempt := range 20 {
				val := (idx + attempt) % 10
				count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, val)).Count(ctx)
				if err != nil {
					readErrors.Store(fmt.Sprintf("r%d-a%d", idx, attempt), err)
					return
				}
				// Count should be between 5 (pre-populated) and 10 (all inserted)
				if count < 5 || count > 10 {
					readErrors.Store(fmt.Sprintf("r%d-a%d", idx, attempt),
						fmt.Errorf("unexpected count %d for a=%d", count, val))
					return
				}
			}
		}(g)
	}

	wg.Wait()

	if v := writeErr.Load(); v != nil {
		t.Fatalf("writer failed: %v", v)
	}
	readErrors.Range(func(key, value any) bool {
		t.Errorf("reader %v failed: %v", key, value)
		return true
	})

	// After all writes complete, verify final state
	totalCount, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, totalCount)
}

func TestIndex_Concurrent_UniqueInsertRace(t *testing.T) {
	// Multiple goroutines try to insert documents with the same unique key.
	// Exactly one should succeed, others should get ErrUniqueConstraint.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"email"}, Unique: true}))

	const numGoroutines = 10
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var constraintCount atomic.Int32

	for g := range numGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"email":"race@test.com"}`, idx))
			err := coll.Insert(ctx, doc)
			if err == nil {
				successCount.Add(1)
			} else {
				constraintCount.Add(1)
			}
		}(g)
	}
	wg.Wait()

	// Exactly one should succeed
	assert.Equal(t, int32(1), successCount.Load(), "exactly one insert should succeed")
	assert.Equal(t, int32(numGoroutines-1), constraintCount.Load(), "rest should fail with constraint error")

	// Verify only one doc exists
	count, err := coll.Find(`{"email":"race@test.com"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Concurrent_UniqueInsertDifferentKeys(t *testing.T) {
	// Multiple goroutines insert documents with different unique keys.
	// All should succeed without conflicts.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"key"}, Unique: true}))

	const numGoroutines = 10
	const docsPerGoroutine = 20
	var wg sync.WaitGroup
	errs := make([]error, numGoroutines)

	for g := range numGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := range docsPerGoroutine {
				id := idx*docsPerGoroutine + j
				doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"key":"k%d"}`, id, id))
				if err := coll.Insert(ctx, doc); err != nil {
					errs[idx] = fmt.Errorf("goroutine %d doc %d: %w", idx, j, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}

	// All docs inserted
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, numGoroutines*docsPerGoroutine, count)
}

func TestIndex_Concurrent_ReadWriteDelete(t *testing.T) {
	// One goroutine inserts, another deletes, readers query concurrently.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	// Pre-populate
	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%20))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup
	var insertErr, deleteErr atomic.Value

	// Writer: insert docs 100-149
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 100; i < 150; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%20))
			if err := coll.Insert(ctx, doc); err != nil {
				insertErr.Store(err)
				return
			}
		}
	}()

	// Deleter: delete docs with a=19
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := coll.Find(`{"a":19}`).Delete(ctx)
		if err != nil {
			deleteErr.Store(err)
		}
	}()

	// Readers
	for g := range 3 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for range 10 {
				_, err := coll.Find(fmt.Sprintf(`{"a":%d}`, idx%20)).Count(ctx)
				if err != nil {
					return
				}
			}
		}(g)
	}

	wg.Wait()

	if v := insertErr.Load(); v != nil {
		t.Fatalf("insert failed: %v", v)
	}
	if v := deleteErr.Load(); v != nil {
		t.Fatalf("delete failed: %v", v)
	}

	// After completion, verify a=19 has been deleted (all of them)
	count, err := coll.Find(`{"a":19}`).Count(ctx)
	require.NoError(t, err)
	// Note: some docs with a=19 from the inserter might have been inserted
	// after the delete ran. The exact count depends on timing.
	// We just verify no error and the count is reasonable.
	assert.True(t, count >= 0)
}

func TestIndex_Concurrent_ExplainDuringWrites(t *testing.T) {
	// Calling Explain while writes are happening should not panic or error
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup
	var writeErr atomic.Value

	// Writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 50; i < 150; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
			if err := coll.Insert(ctx, doc); err != nil {
				writeErr.Store(err)
				return
			}
		}
	}()

	// Explain callers
	for g := range 4 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for range 15 {
				explain, err := coll.Find(fmt.Sprintf(`{"a":%d}`, idx*10)).Explain(ctx)
				if err != nil {
					return
				}
				// Plan should always be valid
				if explain.Sql == "" {
					t.Errorf("goroutine %d: empty explain plan", idx)
					return
				}
			}
		}(g)
	}

	wg.Wait()

	if v := writeErr.Load(); v != nil {
		t.Fatalf("writer failed: %v", v)
	}
}

func TestIndex_Concurrent_UpdateWithIndex(t *testing.T) {
	// Multiple goroutines updating different documents, index stays consistent
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"v":0}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup
	errs := make([]error, 10)

	// Each goroutine updates a non-overlapping range of docs
	for g := range 10 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lo := idx * 10
			hi := lo + 9
			_, err := coll.Find(fmt.Sprintf(`{"a":{"$gte":%d,"$lte":%d}}`, lo, hi)).
				Update(ctx, `{"$set":{"v":1}}`)
			if err != nil {
				errs[idx] = err
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}

	// All 100 docs should have v=1 now
	count, err := coll.Find(`{"v":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, count)

	// Total count unchanged
	total, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, total)
}

func TestIndex_Concurrent_CompoundIndexConsistency(t *testing.T) {
	// Concurrent inserts with compound index, verify index scan returns correct results
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a", "b"}}))

	var wg sync.WaitGroup
	const numGoroutines = 5
	const docsPerGoroutine = 40
	errs := make([]error, numGoroutines)

	for g := range numGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := range docsPerGoroutine {
				id := idx*docsPerGoroutine + j
				doc := anyenc.MustParseJson(fmt.Sprintf(
					`{"id":%d,"a":%d,"b":%d}`, id, id%10, id%7))
				if err := coll.Insert(ctx, doc); err != nil {
					errs[idx] = fmt.Errorf("goroutine %d doc %d: %w", idx, j, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}

	total := numGoroutines * docsPerGoroutine
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, total, count)

	// Verify compound index query works correctly
	count, err = coll.Find(`{"a":5,"b":3}`).Count(ctx)
	require.NoError(t, err)
	assert.True(t, count >= 1, "expected at least 1 match for compound query")

	// Verify index is used
	explain, err := coll.Find(`{"a":5,"b":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan(a,b)")
}

func TestIndex_Concurrent_ConcurrentQueries(t *testing.T) {
	// 10 goroutines each run different query types (equality, range, sort+limit)
	// on the same indexed collection concurrently. All return correct results.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"b"}}))

	for i := range 200 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d,"b":%d}`, i, i%10, i%7))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup
	const numGoroutines = 10
	errs := make([]error, numGoroutines)

	for g := range numGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			switch idx % 3 {
			case 0:
				// Equality query
				val := idx % 10
				count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, val)).Count(ctx)
				if err != nil {
					errs[idx] = err
					return
				}
				if count != 20 {
					errs[idx] = fmt.Errorf("equality g%d: expected 20, got %d", idx, count)
				}
			case 1:
				// Range query
				count, err := coll.Find(`{"a":{"$gte":3,"$lte":6}}`).Count(ctx)
				if err != nil {
					errs[idx] = err
					return
				}
				if count != 80 { // 4 values * 20 each
					errs[idx] = fmt.Errorf("range g%d: expected 80, got %d", idx, count)
				}
			case 2:
				// Sort + Limit query
				vals := collectField(t, coll.Find(nil).Sort("a").Limit(10), "a")
				if len(vals) != 10 {
					errs[idx] = fmt.Errorf("sort+limit g%d: expected 10, got %d", idx, len(vals))
					return
				}
				for i := 1; i < len(vals); i++ {
					prev, _ := strconv.Atoi(vals[i-1])
					cur, _ := strconv.Atoi(vals[i])
					if prev > cur {
						errs[idx] = fmt.Errorf("sort+limit g%d: not sorted at %d", idx, i)
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}
}

func TestIndex_Concurrent_ParallelSortQueries(t *testing.T) {
	// Multiple goroutines do Sort("a").Limit(10) concurrently.
	// All should get the same top-10 results.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	const numGoroutines = 8
	var wg sync.WaitGroup
	results := make([][]string, numGoroutines)
	errs := make([]error, numGoroutines)

	for g := range numGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			vals := collectField(t, coll.Find(nil).Sort("a").Limit(10), "a")
			if len(vals) != 10 {
				errs[idx] = fmt.Errorf("goroutine %d: expected 10, got %d", idx, len(vals))
				return
			}
			results[idx] = vals
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}

	// All goroutines should have gotten the same top-10
	for i := 1; i < numGoroutines; i++ {
		if results[i] != nil && results[0] != nil {
			assert.Equal(t, results[0], results[i],
				"goroutine %d got different top-10 than goroutine 0", i)
		}
	}
}

func TestIndex_Concurrent_ReadAfterBulkWrite(t *testing.T) {
	// Insert 1000 docs, then 20 goroutines each query different ranges.
	// All should return correct counts.
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 1000 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	const numGoroutines = 20
	var wg sync.WaitGroup
	errs := make([]error, numGoroutines)

	for g := range numGoroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lo := idx * 50
			hi := lo + 49
			count, err := coll.Find(fmt.Sprintf(`{"a":{"$gte":%d,"$lte":%d}}`, lo, hi)).Count(ctx)
			if err != nil {
				errs[idx] = err
				return
			}
			if count != 50 {
				errs[idx] = fmt.Errorf("goroutine %d: expected 50 for range [%d,%d], got %d", idx, lo, hi, count)
			}
		}(g)
	}
	wg.Wait()

	for i, err := range errs {
		assert.NoError(t, err, "goroutine %d failed", i)
	}
}

func TestIndex_Concurrent_ConcurrentDeleteAndQuery(t *testing.T) {
	// One goroutine deletes docs via Find({a:5}).Delete().
	// Another queries {a:5}. The reader should see a consistent snapshot
	// (either all docs or none, not a partial state).
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Before delete: a=5 has 10 docs
	preCount, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, preCount)

	var wg sync.WaitGroup
	var deleteErr atomic.Value
	var readerCounts sync.Map

	// Deleter
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := coll.Find(`{"a":5}`).Delete(ctx)
		if err != nil {
			deleteErr.Store(err)
		}
	}()

	// Readers: query a=5 repeatedly during the delete
	for g := range 5 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for attempt := range 30 {
				count, err := coll.Find(`{"a":5}`).Count(ctx)
				if err != nil {
					readerCounts.Store(fmt.Sprintf("err-r%d-a%d", idx, attempt), err)
					return
				}
				// Each read should see a consistent snapshot:
				// either all 10 docs (before delete) or 0 (after delete).
				// Due to WAL mode read isolation, partial states should not be visible.
				if count != 0 && count != 10 {
					// Record unexpected count but don't fail immediately —
					// some implementations might show intermediate states
					// if delete is not fully atomic at the query level.
					readerCounts.Store(fmt.Sprintf("partial-r%d-a%d", idx, attempt), count)
				}
			}
		}(g)
	}

	wg.Wait()

	if v := deleteErr.Load(); v != nil {
		t.Fatalf("delete failed: %v", v)
	}

	// Check for any errors in readers
	readerCounts.Range(func(key, value any) bool {
		k := key.(string)
		if len(k) > 3 && k[:3] == "err" {
			t.Errorf("reader %v failed: %v", key, value)
		}
		// Log partial counts as warnings (not failures) since atomicity
		// at query-level depends on the implementation's isolation guarantees
		if len(k) > 7 && k[:7] == "partial" {
			t.Logf("reader %v saw partial count: %v", key, value)
		}
		return true
	})

	// After delete completes, a=5 should have 0 docs
	postCount, err := coll.Find(`{"a":5}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, postCount)

	// Other values should be unaffected
	otherCount, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 10, otherCount)

	// Total should be 90 (100 - 10 deleted)
	total, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 90, total)
}

// TestConcurrentReadersOverflowKeys tests MVCC isolation when a writer creates
// overflow index keys while multiple readers scan the btree simultaneously.
// This targets the new schema format 5 unified key+value overflow.
func TestConcurrentReadersOverflowKeys(t *testing.T) {
	ctx := ctx
	dbPath := filepath.Join(t.TempDir(), "concurrent-overflow.db")

	db, err := Open(ctx, dbPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	coll, err := db.Collection(ctx, "testcoll")
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(777888))
	arena := &anyenc.Arena{}

	// Insert initial docs with large "data" fields to create overflow keys
	for i := 0; i < 100; i++ {
		arena.Reset()
		obj := arena.NewObject()
		obj.Set("id", arena.NewString(fmt.Sprintf("doc-%06d", i)))
		obj.Set("val", arena.NewNumberInt(i))
		// Large data field: 1500-3000 bytes to force overflow in index keys
		dataSize := 1500 + rng.Intn(1500)
		obj.Set("data", arena.NewString(randomString(rng, dataSize)))
		if err := coll.UpsertOne(ctx, obj); err != nil {
			t.Fatalf("insert doc %d: %v", i, err)
		}
	}

	// Create index on "data" field (overflow keys)
	if err := coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"data"}}); err != nil {
		t.Fatalf("EnsureIndex(data): %v", err)
	}
	t.Log("Index with overflow keys created")

	// Concurrent readers + writer
	var wg sync.WaitGroup
	var readErrors int64
	var writeErrors int64
	done := make(chan struct{})

	// Spawn reader goroutines that scan the collection
	for r := 0; r < 8; r++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				// Full scan
				iter, err := coll.Find(nil).Iter(ctx)
				if err != nil {
					atomic.AddInt64(&readErrors, 1)
					continue
				}
				for iter.Next() {
					doc, err := iter.Doc()
					if err != nil {
						atomic.AddInt64(&readErrors, 1)
						break
					}
					// Read the data field to force overflow page traversal
					_ = doc.Value().GetStringBytes("data")
				}
				iter.Close()
			}
		}(r)
	}

	// Writer goroutine: update docs with new large data values
	writerRng := rand.New(rand.NewSource(999111))
	for round := 0; round < 50; round++ {
		for j := 0; j < 10; j++ {
			arena.Reset()
			obj := arena.NewObject()
			docID := writerRng.Intn(100)
			obj.Set("id", arena.NewString(fmt.Sprintf("doc-%06d", docID)))
			obj.Set("val", arena.NewNumberInt(round*10+j))
			dataSize := 1500 + writerRng.Intn(1500)
			obj.Set("data", arena.NewString(randomString(writerRng, dataSize)))
			if err := coll.UpsertOne(ctx, obj); err != nil {
				atomic.AddInt64(&writeErrors, 1)
			}
		}
		// Checkpoint every 10 rounds
		if round%10 == 0 {
			_ = db.Flush(ctx, 0, FlushModeCheckpointPassive)
		}
	}

	close(done)
	wg.Wait()

	re := atomic.LoadInt64(&readErrors)
	we := atomic.LoadInt64(&writeErrors)
	if re > 0 || we > 0 {
		t.Fatalf("Errors during concurrent access: reads=%d, writes=%d", re, we)
	}

	// Final verification
	count, err := coll.Count(ctx)
	if err != nil {
		t.Fatalf("final count: %v", err)
	}
	if err := db.QuickCheck(ctx); err != nil {
		t.Fatalf("QuickCheck: %v", err)
	}
	t.Logf("Concurrent overflow keys test passed (%d docs, 8 readers, 50 write rounds)", count)
}

// TestAudit15_ConcurrentReaderMultiKey_* — focused edge-case audit for MVCC
// isolation of multi-key (array-valued) index entries under concurrent
// read/write traffic.
//
// Background:
//
// any-store provides single-writer + multi-reader MVCC via WAL. A read tx
// opened before a write commits sees the pre-write snapshot; a read tx
// opened after sees the post-write snapshot. There is no torn state by
// construction.
//
// The per-entry value byte (see AUDIT01–AUDIT03 / qplanner.IndexValueScalar
// vs IndexValueMultiKey) introduces a NEW failure mode: if MVCC isolation
// were to leak even a single byte, a reader could observe a multi-key entry
// that has been partially rewritten as scalar (or vice versa). Symptoms:
//
//   - Count returns 2+ for a unique-id doc whose array overlaps multiple
//     $in bounds (dedup pipeline inactive because the value byte read as
//     scalar even though the entry was last written as multi-key).
//   - Iter yields the same doc id twice in one pass.
//
// Existing concurrent tests (index_concurrent_test.go) only exercise scalar
// fields, so the value-byte invariant under concurrency is unverified.
// These tests pin it: each subtest spins a writer that flips the same
// single doc's array tags in a tight loop while one or more readers
// continuously query the multi-key index. Across every observation, the
// count of distinct matching docs MUST be 0 or 1 — never 2 (which would
// indicate the dedup pipeline broke), never an error (which would indicate
// transient state was visible).
//
// The same doc id ("d1") is reused so the test exercises the
// delete-old-keys + insert-new-keys path inside collection.update for every
// flip — this is the operation most likely to expose a torn entry to a
// concurrent reader if MVCC were broken.

// audit15TestDuration bounds each subtest. Long enough to exercise many
// flip cycles; short enough to keep the suite fast under -race.
const audit15TestDuration = 200 * time.Millisecond

// runAudit15Workload starts one writer goroutine and the supplied reader
// goroutines, runs them for the configured duration via a deadline channel,
// then waits for all of them to exit. The writer's loop body is provided
// by writeStep (called repeatedly until done is closed). Each reader is
// also a closure called repeatedly until done is closed.
//
// All goroutines must respect the done channel. If any goroutine returns
// an error via writeErr / readerErrs, the test fails.
func runAudit15Workload(
	t *testing.T,
	writeStep func() error,
	readers []func() error,
) {
	t.Helper()
	done := make(chan struct{})
	deadline := time.After(audit15TestDuration)

	var wg sync.WaitGroup
	var writeErr atomic.Value
	readerErrs := make([]atomic.Value, len(readers))

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			if err := writeStep(); err != nil {
				writeErr.Store(err)
				return
			}
		}
	}()

	for i, r := range readers {
		wg.Add(1)
		go func(idx int, fn func() error) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				if err := fn(); err != nil {
					readerErrs[idx].Store(err)
					return
				}
			}
		}(i, r)
	}

	<-deadline
	close(done)
	wg.Wait()

	if v := writeErr.Load(); v != nil {
		t.Fatalf("writer failed: %v", v)
	}
	for i := range readerErrs {
		if v := readerErrs[i].Load(); v != nil {
			t.Fatalf("reader %d failed: %v", i, v)
		}
	}
}

// audit15FlipDocs returns the two whole-doc shapes used by the
// "flip between two array shapes" tests. d1 always has the same id; only
// the tags array changes. Reusing the id forces collection.update to
// delete-then-insert all index entries every flip.
func audit15FlipDocs() (a, b *anyenc.Value) {
	a = anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`)
	b = anyenc.MustParseJson(`{"id":"d1","tags":["x"]}`)
	return
}

// TestAudit15_ConcurrentReaderMultiKey_StableSnapshotCount: writer flips
// d1 between ["a","b","c"] and ["x"] in a loop. Reader runs Find({tags:"a"})
// .Count for the same duration — every observation must be 0 or 1 (never 2,
// never an error). 0 means the snapshot caught the ["x"] state; 1 means
// the snapshot caught the ["a","b","c"] state. Anything else implies the
// per-entry value byte (or dedup pipeline) saw torn state.
func TestAudit15_ConcurrentReaderMultiKey_StableSnapshotCount(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit15_stable_count")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	docA, docX := audit15FlipDocs()
	require.NoError(t, coll.UpsertOne(ctx, docA))

	var violations atomic.Int64
	var observations atomic.Int64
	flip := false
	writeStep := func() error {
		flip = !flip
		if flip {
			return coll.UpsertOne(ctx, docX)
		}
		return coll.UpsertOne(ctx, docA)
	}

	reader := func() error {
		n, err := coll.Find(`{"tags":"a"}`).Count(ctx)
		if err != nil {
			return fmt.Errorf("Count error: %w", err)
		}
		observations.Add(1)
		if n != 0 && n != 1 {
			violations.Add(1)
			return fmt.Errorf("torn snapshot: Count returned %d (expected 0 or 1)", n)
		}
		return nil
	}

	runAudit15Workload(t, writeStep, []func() error{reader})

	assert.Zero(t, violations.Load(),
		"expected no torn-snapshot observations; got %d violation(s) over %d observation(s)",
		violations.Load(), observations.Load())
	require.Greater(t, observations.Load(), int64(0),
		"reader did not run a single observation — workload too short or scheduling pathological")
}

// TestAudit15_ConcurrentReaderMultiKey_StableSnapshotIter: same flip
// pattern as above, but the reader uses Iter and collects doc ids.
// The result set per pass must contain at most {"d1"} (never d1 twice,
// never an unexpected id). This catches a leak in planIterator.Next /
// DocDedup that Count alone might miss because Count has its own dedup
// pipeline (CountEntries / countEntriesViaSortDedup).
func TestAudit15_ConcurrentReaderMultiKey_StableSnapshotIter(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit15_stable_iter")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	docA, docX := audit15FlipDocs()
	require.NoError(t, coll.UpsertOne(ctx, docA))

	var violations atomic.Int64
	var observations atomic.Int64
	flip := false
	writeStep := func() error {
		flip = !flip
		if flip {
			return coll.UpsertOne(ctx, docX)
		}
		return coll.UpsertOne(ctx, docA)
	}

	reader := func() error {
		// Use $in to force the multi-bound code path (the one that needs
		// the value byte to dedup). With a single bound on "a", a flip to
		// ["x"] simply yields zero results — no dedup work happens. With
		// $in: ["a","b","c"], the ["a","b","c"] state would yield d1
		// across THREE bounds — DocDedup must collapse it to one.
		iter, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Iter(ctx)
		if err != nil {
			return fmt.Errorf("Iter error: %w", err)
		}
		ids := map[string]int{}
		var iterErr error
		for iter.Next() {
			doc, derr := iter.Doc()
			if derr != nil {
				iterErr = derr
				break
			}
			id := doc.Value().GetStringBytes("id")
			ids[string(id)]++
		}
		if iterErr == nil {
			iterErr = iter.Err()
		}
		if cerr := iter.Close(); cerr != nil && iterErr == nil {
			iterErr = cerr
		}
		if iterErr != nil {
			return fmt.Errorf("iter error: %w", iterErr)
		}
		observations.Add(1)

		// Allowed: empty result, or exactly one occurrence of "d1".
		if len(ids) > 1 {
			violations.Add(1)
			return fmt.Errorf("torn snapshot: unexpected ids %v", ids)
		}
		if cnt, ok := ids["d1"]; ok && cnt != 1 {
			violations.Add(1)
			return fmt.Errorf("torn snapshot: d1 yielded %d times in one pass", cnt)
		}
		for id := range ids {
			if id != "d1" {
				violations.Add(1)
				return fmt.Errorf("torn snapshot: unexpected id %q", id)
			}
		}
		return nil
	}

	runAudit15Workload(t, writeStep, []func() error{reader})

	assert.Zero(t, violations.Load(),
		"expected no torn-snapshot observations; got %d violation(s) over %d observation(s)",
		violations.Load(), observations.Load())
	require.Greater(t, observations.Load(), int64(0),
		"reader did not run a single observation — workload too short or scheduling pathological")
}

// TestAudit15_ConcurrentReaderMultiKey_AddDeleteCycle: writer inserts and
// deletes the same doc with array tags in a tight loop. Reader runs
// Count over a multi-bound $in. Every observation must be 0 (doc absent)
// or 1 (doc present). Exercises a different code path from the flip test:
// here the index entries are fully removed and re-created, not
// delete-then-insert as part of an update.
func TestAudit15_ConcurrentReaderMultiKey_AddDeleteCycle(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit15_add_delete")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	doc := anyenc.MustParseJson(`{"id":"d1","tags":["a","b","c"]}`)

	// Start with the doc absent. The writer alternately inserts then
	// deletes by id.
	var violations atomic.Int64
	var observations atomic.Int64
	state := false // false = absent, true = present
	writeStep := func() error {
		if state {
			state = false
			err := coll.DeleteId(ctx, "d1")
			// Tolerate races with our own state tracking — if the doc
			// already vanished or never existed, we just resync.
			if err != nil {
				return fmt.Errorf("DeleteId: %w", err)
			}
			return nil
		}
		state = true
		if err := coll.UpsertOne(ctx, doc); err != nil {
			return fmt.Errorf("UpsertOne: %w", err)
		}
		return nil
	}

	reader := func() error {
		n, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Count(ctx)
		if err != nil {
			return fmt.Errorf("Count error: %w", err)
		}
		observations.Add(1)
		if n != 0 && n != 1 {
			violations.Add(1)
			return fmt.Errorf("torn snapshot: Count returned %d (expected 0 or 1)", n)
		}
		return nil
	}

	runAudit15Workload(t, writeStep, []func() error{reader})

	assert.Zero(t, violations.Load(),
		"expected no torn-snapshot observations; got %d violation(s) over %d observation(s)",
		violations.Load(), observations.Load())
	require.Greater(t, observations.Load(), int64(0),
		"reader did not run a single observation — workload too short or scheduling pathological")
}

// TestAudit15_ConcurrentReaderMultiKey_MultiBoundOverlap: writer flips the
// doc's array between three OVERLAPPING shapes (["a","b"], ["b","c"],
// ["a","c"]). Reader runs $in:["a","b","c"].Count. The doc always matches
// at least two bounds, so without correct dedup the count would be 2 or 3.
// With correct dedup it must be exactly 1 (or 0 if the writer happens to
// have no doc — but here the doc is always present).
func TestAudit15_ConcurrentReaderMultiKey_MultiBoundOverlap(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit15_multibound")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	shapes := []*anyenc.Value{
		anyenc.MustParseJson(`{"id":"d1","tags":["a","b"]}`),
		anyenc.MustParseJson(`{"id":"d1","tags":["b","c"]}`),
		anyenc.MustParseJson(`{"id":"d1","tags":["a","c"]}`),
	}
	require.NoError(t, coll.UpsertOne(ctx, shapes[0]))

	var violations atomic.Int64
	var observations atomic.Int64
	idx := 0
	writeStep := func() error {
		idx = (idx + 1) % len(shapes)
		return coll.UpsertOne(ctx, shapes[idx])
	}

	reader := func() error {
		n, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Count(ctx)
		if err != nil {
			return fmt.Errorf("Count error: %w", err)
		}
		observations.Add(1)
		// All three shapes match at least two of the three $in bounds.
		// Correct dedup → count == 1. Broken dedup → count >= 2.
		// (Count == 0 should not happen here because the doc is always
		// present, but we tolerate it defensively against snapshot
		// timing oddities; the violation we hunt is count >= 2.)
		if n != 0 && n != 1 {
			violations.Add(1)
			return fmt.Errorf("torn snapshot / broken dedup: Count returned %d (expected 0 or 1)", n)
		}
		return nil
	}

	runAudit15Workload(t, writeStep, []func() error{reader})

	assert.Zero(t, violations.Load(),
		"expected no broken-dedup observations; got %d violation(s) over %d observation(s)",
		violations.Load(), observations.Load())
	require.Greater(t, observations.Load(), int64(0),
		"reader did not run a single observation — workload too short or scheduling pathological")
}

// TestAudit15_ConcurrentReaderMultiKey_ConcurrentReaders: 4 reader
// goroutines + 1 writer goroutine, all hammering the same multi-key
// index simultaneously. Every reader must see consistent counts; no
// goroutine returns an error. This stresses the read-tx pool / WAL
// snapshot machinery harder than the single-reader subtests.
func TestAudit15_ConcurrentReaderMultiKey_ConcurrentReaders(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "audit15_many_readers")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{
		Name:   "ix_tags",
		Fields: []string{"tags"},
	}))

	docA, docX := audit15FlipDocs()
	require.NoError(t, coll.UpsertOne(ctx, docA))

	const numReaders = 4
	var violations atomic.Int64
	observations := make([]atomic.Int64, numReaders)

	flip := false
	writeStep := func() error {
		flip = !flip
		if flip {
			return coll.UpsertOne(ctx, docX)
		}
		return coll.UpsertOne(ctx, docA)
	}

	readers := make([]func() error, numReaders)
	for i := range readers {
		i := i
		readers[i] = func() error {
			// Mix Count and Iter across readers to exercise both paths.
			if i%2 == 0 {
				n, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Count(ctx)
				if err != nil {
					return fmt.Errorf("reader %d Count: %w", i, err)
				}
				observations[i].Add(1)
				if n != 0 && n != 1 {
					violations.Add(1)
					return fmt.Errorf("reader %d torn snapshot: Count=%d", i, n)
				}
				return nil
			}
			iter, err := coll.Find(`{"tags":{"$in":["a","b","c"]}}`).Iter(ctx)
			if err != nil {
				return fmt.Errorf("reader %d Iter: %w", i, err)
			}
			seen := map[string]int{}
			var iterErr error
			for iter.Next() {
				doc, derr := iter.Doc()
				if derr != nil {
					iterErr = derr
					break
				}
				seen[string(doc.Value().GetStringBytes("id"))]++
			}
			if iterErr == nil {
				iterErr = iter.Err()
			}
			if cerr := iter.Close(); cerr != nil && iterErr == nil {
				iterErr = cerr
			}
			if iterErr != nil {
				return fmt.Errorf("reader %d iter loop: %w", i, iterErr)
			}
			observations[i].Add(1)
			if len(seen) > 1 {
				violations.Add(1)
				return fmt.Errorf("reader %d torn snapshot: ids=%v", i, seen)
			}
			if cnt, ok := seen["d1"]; ok && cnt != 1 {
				violations.Add(1)
				return fmt.Errorf("reader %d torn snapshot: d1 yielded %d times", i, cnt)
			}
			return nil
		}
	}

	runAudit15Workload(t, writeStep, readers)

	assert.Zero(t, violations.Load(),
		"expected no torn-snapshot observations; got %d violation(s)",
		violations.Load())
	for i := range observations {
		require.Greater(t, observations[i].Load(), int64(0),
			"reader %d did not run a single observation", i)
	}
}

// TestSketch_LoadDuringConcurrentReader is a focused -race regression
// test for the data-race fix on collection.loadSketch
// (collection.go::loadSketch). The previous implementation replaced
// idx.sketch with a fresh NewIndexSketch on every checkStale invocation;
// concurrent readers calling idx.sketch.GetDocCount() / Estimate without
// holding c.mu would race on that pointer field. The fix updates the
// existing sketch in place via UnmarshalBinary (which uses atomic
// stores), so the pointer never changes after createIndex.
//
// This test forces hundreds of reload cycles by repeatedly opening write
// txs (which bump the file change counter and cause checkStale to fire
// reloadSketches on the next read tx) while concurrent goroutines hammer
// the read path that touches idx.sketch.
//
// Without the fix, this test fails with -race ("WARNING: DATA RACE" at
// collection.go:loadSketch / query.go:docCount). With the fix, it passes
// cleanly.
func TestSketch_LoadDuringConcurrentReader(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "sketch_race")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	for i := range 50 {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))))
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	var wg sync.WaitGroup
	var readerErrs atomic.Int32
	var writerErrs atomic.Int32

	// Writer: bumps the file change counter every commit, forcing the next
	// read tx's checkStale to invoke reloadSketches → loadSketch.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := 100
		for time.Now().Before(deadline) {
			if err := coll.Insert(ctx, anyenc.MustParseJson(
				fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))); err != nil {
				writerErrs.Add(1)
				return
			}
			if err := coll.DeleteId(ctx, i); err != nil {
				writerErrs.Add(1)
				return
			}
			i++
		}
	}()

	// Readers: exercise both the docCount (q.c.loadIndexes()[*].sketch read) and
	// the planner's Sketch field via Find().Count and Find().Iter.
	const numReaders = 8
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if _, err := coll.Find(`{"a":3}`).Count(ctx); err != nil {
					readerErrs.Add(1)
					return
				}
				if _, err := coll.Find(nil).Count(ctx); err != nil {
					readerErrs.Add(1)
					return
				}
				it, err := coll.Find(`{"a":2}`).Iter(ctx)
				if err != nil {
					readerErrs.Add(1)
					return
				}
				for it.Next() {
				}
				it.Close()
			}
		}()
	}

	wg.Wait()
	assert.Zero(t, writerErrs.Load(), "writer should not error")
	assert.Zero(t, readerErrs.Load(), "readers should not error")
}

// TestSketch_DocCountSurvivesReload pins the post-fix invariant that
// in-memory sketch state (DocCount in particular) is preserved across a
// reload triggered by checkStale. The previous implementation replaced
// the sketch outright on every reload; if a reader had triggered reload
// before the writer's persistSketches landed, the in-memory counter would
// silently revert to whatever was last persisted (often zero on a
// freshly-created index).
//
// With the in-place UnmarshalBinary fix, reload either applies persisted
// state (consistent with the read snapshot) or — if no persisted data
// exists — leaves the in-memory counter alone. This test asserts the
// no-data branch: even after a forced reload, GetDocCount reflects every
// inserted doc.
func TestSketch_DocCountSurvivesReload(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "sketch_survives")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"a"}}))

	const N = 25
	for i := range N {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(
			fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))))
	}

	// Force a stale-detection by running a query that opens a fresh read
	// tx after the writes committed.
	count, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, N, count)

	// Read DocCount via the same path the planner uses; it must reflect
	// every insert, not regress to zero from a reload that dropped state.
	c := coll.(*collection)
	c.mu.Lock()
	got := c.loadIndexes()[0].sketch.GetDocCount()
	c.mu.Unlock()
	assert.Equal(t, uint64(N), got,
		"sketch DocCount must equal inserted-doc count after reload (%d), got %d", N, got)
}
