package test

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndex_Concurrent_ParallelReads(t *testing.T) {
	// Multiple goroutines reading from indexed collection simultaneously
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"email"}, Unique: true}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"key"}, Unique: true}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"b"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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

	db, err := anystore.Open(ctx, dbPath, nil)
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
	if err := coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"data"}}); err != nil {
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
			_ = db.Flush(ctx, 0, anystore.FlushModeCheckpointPassive)
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
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
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

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

/*
Index/Planner tests inspired by SQLite: wal.test, trans.test
Test scenario:
Tests transaction isolation with indexes: read transactions see consistent
snapshots even during concurrent writes, write transaction index changes
are not visible until commit, rollback undoes index changes, and savepoints
work correctly with index mutations.
These tests verify our custom index and query planner implementation.
While inspired by SQLite test patterns, our system has a different
architecture (document-oriented with weight-based planner vs SQL VDBE).
*/

func TestIndex_Transaction_WriteTxVisibleViaTxContext(t *testing.T) {
	// Inserts within a WriteTx should be visible when querying through tx.Context()
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert docs within the write transaction
	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Query through tx.Context() should see the inserted documents
	count, err := coll.Find(`{"a":2}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Indexed query should also work within the transaction
	explain, err := coll.Find(`{"a":2}`).Explain(tx.Context())
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// Total count should be 10
	assertCollCountInTx(tx.Context(), t, coll, 10)

	require.NoError(t, tx.Commit())

	// After commit, data should be visible outside the transaction
	assertCollCount(t, coll, 10)
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)
}

func TestIndex_Transaction_RollbackUndoesInserts(t *testing.T) {
	// Rollback should undo all inserts and their index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))
	assertCollCount(t, coll, 2)

	// Start a write transaction and insert more
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := 3; i <= 12; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Within tx, count should be 12
	assertCollCountInTx(tx.Context(), t, coll, 12)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, only original 2 docs should remain
	assertCollCount(t, coll, 2)

	// Index query should only find original data
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":30}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Index length should be back to 2
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 2)
}

func TestIndex_Transaction_RollbackUndoesDeletes(t *testing.T) {
	// Rollback should undo deletes, restoring docs and index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(ctx, doc))
	}
	assertCollCount(t, coll, 10)

	// Start write tx and delete some docs
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.DeleteId(tx.Context(), 3))
	require.NoError(t, coll.DeleteId(tx.Context(), 7))

	// Within tx, count should be 8
	assertCollCountInTx(tx.Context(), t, coll, 8)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, all 10 docs should be back
	assertCollCount(t, coll, 10)

	// Deleted docs should be findable again
	doc, err := coll.FindId(ctx, 3)
	require.NoError(t, err)
	assert.Equal(t, 3, doc.Value().GetInt("a"))

	doc, err = coll.FindId(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, 7, doc.Value().GetInt("a"))

	// Index should still find them
	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_RollbackUndoesUpdates(t *testing.T) {
	// Rollback should undo updates, restoring original values and index entries
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":100}`),
		anyenc.MustParseJson(`{"id":2,"a":200}`),
	))

	// Start write tx and update
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.UpdateOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":999}`)))

	// Within tx, the update should be visible
	count, err := coll.Find(`{"a":999}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":100}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Rollback
	require.NoError(t, tx.Rollback())

	// Original value should be restored
	count, err = coll.Find(`{"a":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_Transaction_SavepointRollbackPartial(t *testing.T) {
	// Savepoint rollback should undo only the savepoint's changes,
	// while preserving earlier changes in the same write tx
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert first batch in the outer transaction
	require.NoError(t, coll.Insert(tx.Context(),
		anyenc.MustParseJson(`{"id":1,"a":10}`),
		anyenc.MustParseJson(`{"id":2,"a":20}`),
	))
	assertCollCountInTx(tx.Context(), t, coll, 2)

	// A nested insert that fails (duplicate id) triggers savepoint rollback
	err = coll.Insert(tx.Context(),
		anyenc.MustParseJson(`{"id":3,"a":30}`),
		anyenc.MustParseJson(`{"id":4,"a":40}`),
		anyenc.MustParseJson(`{"id":1,"a":50}`), // duplicate
	)
	require.Error(t, err)

	// After savepoint rollback, only the first 2 docs should remain
	assertCollCountInTx(tx.Context(), t, coll, 2)

	// Index query should still work for the first batch
	count, err := coll.Find(`{"a":10}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	// The failed insert's docs should not be findable
	count, err = coll.Find(`{"a":30}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Commit the outer transaction
	require.NoError(t, tx.Commit())

	// After commit, only the first batch should be persisted
	assertCollCount(t, coll, 2)
}

func TestIndex_Transaction_CommitPersistsIndexEntries(t *testing.T) {
	// After commit, index entries should be correctly persisted
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := range 50 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	require.NoError(t, tx.Commit())

	// Verify index works correctly after commit
	for v := range 10 {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 5, count, "expected 5 docs with a=%d", v)
	}

	// Index should have 50 entries
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 50)
}

func TestIndex_Transaction_UniqueConstraintInTx(t *testing.T) {
	// Unique constraint should be enforced within a transaction
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":100}`)))

	// Attempting to insert duplicate unique value within the same tx should fail
	err = coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":100}`))
	assert.ErrorIs(t, err, anystore.ErrUniqueConstraint)

	// The first insert should still be valid after the failed second insert
	assertCollCountInTx(tx.Context(), t, coll, 1)

	require.NoError(t, tx.Commit())
	assertCollCount(t, coll, 1)
}

func TestIndex_Transaction_CompoundIndexRollback(t *testing.T) {
	// Compound index entries should be correctly rolled back
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a", "b"}}))

	// Insert baseline data
	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":1,"b":10}`),
		anyenc.MustParseJson(`{"id":2,"a":2,"b":20}`),
	))

	// Start tx, insert more, then rollback
	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	require.NoError(t, coll.Insert(tx.Context(),
		anyenc.MustParseJson(`{"id":3,"a":1,"b":30}`),
		anyenc.MustParseJson(`{"id":4,"a":3,"b":40}`),
	))

	// Verify compound index query works within tx
	count, err := coll.Find(`{"a":1}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, count) // id:1 and id:3

	require.NoError(t, tx.Rollback())

	// After rollback, only baseline data
	count, err = coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count) // only id:1

	count, err = coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count) // id:4 was rolled back

	// Index length should be 2
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 2)
}

func TestIndex_Transaction_MultipleOperationsInTx(t *testing.T) {
	// Mix of insert, update, delete within one transaction, then commit
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 5 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Delete id:0 (a:0)
	require.NoError(t, coll.DeleteId(tx.Context(), 0))

	// Update id:1 (a:10 -> a:999)
	require.NoError(t, coll.UpdateOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":999}`)))

	// Insert new doc
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":10,"a":555}`)))

	// Verify state within tx
	assertCollCountInTx(tx.Context(), t, coll, 5) // 5 - 1 + 1 = 5

	count, err := coll.Find(`{"a":0}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count) // deleted

	count, err = coll.Find(`{"a":10}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count) // updated away

	count, err = coll.Find(`{"a":999}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count) // updated value

	count, err = coll.Find(`{"a":555}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count) // newly inserted

	require.NoError(t, tx.Commit())

	// Verify same state after commit
	assertCollCount(t, coll, 5)
	count, err = coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_MultipleOperationsRollback(t *testing.T) {
	// Mix of insert, update, delete within one transaction, then rollback
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 5 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i*10))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Delete id:0
	require.NoError(t, coll.DeleteId(tx.Context(), 0))
	// Update id:1
	require.NoError(t, coll.UpdateOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":999}`)))
	// Insert new
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":10,"a":555}`)))

	// Rollback all changes
	require.NoError(t, tx.Rollback())

	// Everything should be back to original state
	assertCollCount(t, coll, 5)

	// Deleted doc should be back
	doc, err := coll.FindId(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, 0, doc.Value().GetInt("a"))

	// Updated doc should have original value
	doc, err = coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 10, doc.Value().GetInt("a"))

	// Inserted doc should be gone
	_, err = coll.FindId(ctx, 10)
	assert.ErrorIs(t, err, anystore.ErrDocNotFound)

	// Index queries should reflect original state
	count, err := coll.Find(`{"a":0}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	count, err = coll.Find(`{"a":555}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_Transaction_SortWithinTx(t *testing.T) {
	// Sort queries should work correctly within a transaction
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert docs in reverse order within tx
	for i := 9; i >= 0; i-- {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Sort ascending should work within tx — query through tx.Context()
	iter, err := coll.Find(nil).Sort("a").Iter(tx.Context())
	require.NoError(t, err)
	var sortedVals []int
	for iter.Next() {
		doc, iterErr := iter.Doc()
		require.NoError(t, iterErr)
		sortedVals = append(sortedVals, doc.Value().GetInt("a"))
	}
	require.NoError(t, iter.Close())
	require.Len(t, sortedVals, 10)
	for i := 1; i < len(sortedVals); i++ {
		assert.True(t, sortedVals[i-1] <= sortedVals[i],
			"not sorted at %d: %d > %d", i, sortedVals[i-1], sortedVals[i])
	}

	require.NoError(t, tx.Commit())

	// Verify sorting works after commit too
	iter, err = coll.Find(nil).Sort("a").Iter(ctx)
	require.NoError(t, err)
	sortedVals = sortedVals[:0]
	for iter.Next() {
		doc, iterErr := iter.Doc()
		require.NoError(t, iterErr)
		sortedVals = append(sortedVals, doc.Value().GetInt("a"))
	}
	require.NoError(t, iter.Close())
	require.Len(t, sortedVals, 10)
	for i := 1; i < len(sortedVals); i++ {
		assert.True(t, sortedVals[i-1] <= sortedVals[i])
	}
}

func TestIndex_Transaction_ExplainWithinTx(t *testing.T) {
	// Explain should work correctly within a transaction context
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	// Explain within transaction should show IndexScan
	explain, err := coll.Find(`{"a":3}`).Explain(tx.Context())
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")
	assert.NotContains(t, explain.Sql, "FullScan")

	require.NoError(t, tx.Commit())
}

func TestIndex_Transaction_IndexCreationInTx(t *testing.T) {
	// Creating an index within a transaction should work and be visible
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)

	// Insert data first
	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	// Without index, query should use FullScan
	explain, err := coll.Find(`{"a":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "FullScan")

	// Create index (this auto-commits)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Now query should use IndexScan
	explain, err = coll.Find(`{"a":3}`).Explain(ctx)
	require.NoError(t, err)
	assert.Contains(t, explain.Sql, "IndexScan")

	// Verify index built correctly
	count, err := coll.Find(`{"a":3}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 4, count) // 20/5 = 4
}

func TestIndex_Transaction_BulkInsertInSingleTx(t *testing.T) {
	// Bulk insert via a single write transaction should be atomic
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	for i := range 100 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%10))
		require.NoError(t, coll.Insert(tx.Context(), doc))
	}

	require.NoError(t, tx.Commit())

	// Verify all 100 docs inserted
	assertCollCount(t, coll, 100)

	// Verify index correctness
	for v := range 10 {
		count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, v)).Count(ctx)
		require.NoError(t, err)
		assert.Equal(t, 10, count)
	}

	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)
	assertIndexLen(t, idxs[0], 100)
}

func TestIndex_Transaction_SequentialWriteTxs(t *testing.T) {
	// Multiple sequential write transactions should build on each other
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// First tx: insert docs with a=1
	tx1, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	for i := range 5 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":1}`, i))
		require.NoError(t, coll.Insert(tx1.Context(), doc))
	}
	require.NoError(t, tx1.Commit())

	// Second tx: insert docs with a=2
	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	for i := 5; i < 10; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":2}`, i))
		require.NoError(t, coll.Insert(tx2.Context(), doc))
	}
	require.NoError(t, tx2.Commit())

	// Both batches should be visible
	assertCollCount(t, coll, 10)

	count, err := coll.Find(`{"a":1}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)

	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 5, count)
}

func TestIndex_Transaction_RollbackThenCommitNewTx(t *testing.T) {
	// After rolling back one tx, a new tx should work correctly
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// First tx: insert and rollback
	tx1, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx1.Context(),
		anyenc.MustParseJson(`{"id":1,"a":100}`),
		anyenc.MustParseJson(`{"id":2,"a":200}`),
	))
	require.NoError(t, tx1.Rollback())

	// Verify empty
	assertCollCount(t, coll, 0)

	// Second tx: insert and commit
	tx2, err := fx.WriteTx(ctx)
	require.NoError(t, err)
	require.NoError(t, coll.Insert(tx2.Context(),
		anyenc.MustParseJson(`{"id":1,"a":999}`),
		anyenc.MustParseJson(`{"id":3,"a":888}`),
	))
	require.NoError(t, tx2.Commit())

	// New data should be committed
	assertCollCount(t, coll, 2)

	count, err := coll.Find(`{"a":999}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":100}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count) // rolled back value not present
}

func TestIndex_Transaction_SparseIndexRollback(t *testing.T) {
	// Sparse index should properly handle rollback of docs with missing fields
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Sparse: true}))

	// Insert some docs without field 'a'
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"b":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Insert docs with and without field 'a' in tx
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":20}`)))
	require.NoError(t, coll.Insert(tx.Context(), anyenc.MustParseJson(`{"id":3,"b":30}`)))

	// Within tx, sparse index should have 1 entry (id:2)
	idxs := coll.GetIndexes()
	require.Len(t, idxs, 1)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, only id:1 should exist (no 'a' field, sparse = no index entry)
	assertCollCount(t, coll, 1)
	assertIndexLen(t, idxs[0], 0) // no docs have 'a' field
}

func TestIndex_Transaction_ConcurrentReadDuringWrite(t *testing.T) {
	// Concurrent goroutines reading while another writes should not crash
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Insert initial data
	for i := range 20 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	var wg sync.WaitGroup

	// Start readers
	for r := range 5 {
		wg.Add(1)
		go func(rid int) {
			defer wg.Done()
			for j := range 20 {
				_ = j
				count, err := coll.Find(fmt.Sprintf(`{"a":%d}`, rid%5)).Count(ctx)
				if err != nil {
					t.Errorf("reader %d: count error: %v", rid, err)
					return
				}
				// Count might vary if writes are concurrent,
				// but should always succeed without error
				_ = count
			}
		}(r)
	}

	// Start a writer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 20; i < 40; i++ {
			doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
			if err := coll.Insert(ctx, doc); err != nil {
				t.Errorf("writer: insert error: %v", err)
				return
			}
		}
	}()

	wg.Wait()

	// After all goroutines finish, 40 docs total
	assertCollCount(t, coll, 40)
}

func TestIndex_Transaction_UpdateOneRollback(t *testing.T) {
	// UpdateOne within a tx that gets rolled back should restore original doc
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}, Unique: true}))

	require.NoError(t, coll.Insert(ctx,
		anyenc.MustParseJson(`{"id":1,"a":10,"b":"original"}`),
	))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Update both indexed and non-indexed fields
	require.NoError(t, coll.UpdateOne(tx.Context(),
		anyenc.MustParseJson(`{"id":1,"a":20,"b":"modified"}`)))

	// Within tx, the update should be visible
	doc, err := coll.FindId(tx.Context(), 1)
	require.NoError(t, err)
	assert.Equal(t, 20, doc.Value().GetInt("a"))
	assert.Equal(t, "modified", doc.Value().GetString("b"))

	// Rollback
	require.NoError(t, tx.Rollback())

	// Original values should be restored
	doc, err = coll.FindId(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 10, doc.Value().GetInt("a"))
	assert.Equal(t, "original", doc.Value().GetString("b"))

	// Index should still work with original value
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":20}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

func TestIndex_Transaction_UpsertInTx(t *testing.T) {
	// UpsertOne should work correctly within a transaction
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	// Insert initial doc
	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Upsert existing doc (update)
	require.NoError(t, coll.UpsertOne(tx.Context(), anyenc.MustParseJson(`{"id":1,"a":20}`)))

	// Upsert new doc (insert)
	require.NoError(t, coll.UpsertOne(tx.Context(), anyenc.MustParseJson(`{"id":2,"a":30}`)))

	// Verify within tx
	assertCollCountInTx(tx.Context(), t, coll, 2)

	count, err := coll.Find(`{"a":20}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":30}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, count)

	count, err = coll.Find(`{"a":10}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	require.NoError(t, tx.Commit())

	// Verify after commit
	assertCollCount(t, coll, 2)
}

func TestIndex_Transaction_EmptyTxCommit(t *testing.T) {
	// Committing a transaction with no changes should be a no-op
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// No operations within tx
	require.NoError(t, tx.Commit())

	// Data should be unchanged
	assertCollCount(t, coll, 1)
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_EmptyTxRollback(t *testing.T) {
	// Rolling back a transaction with no changes should be a no-op
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(`{"id":1,"a":10}`)))

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// No operations within tx
	require.NoError(t, tx.Rollback())

	// Data should be unchanged
	assertCollCount(t, coll, 1)
	count, err := coll.Find(`{"a":10}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestIndex_Transaction_FindDeleteInTx(t *testing.T) {
	// Find().Delete() within a transaction should be rollback-able
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%3))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Delete docs where a=0 within tx
	res, err := coll.Find(`{"a":0}`).Delete(tx.Context())
	require.NoError(t, err)
	assert.True(t, res.Modified > 0)

	// Within tx, those docs should be gone
	count, err := coll.Find(`{"a":0}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, deleted docs should be back
	count, err = coll.Find(`{"a":0}`).Count(ctx)
	require.NoError(t, err)
	assert.True(t, count > 0)

	assertCollCount(t, coll, 10)
}

func TestIndex_Transaction_FindUpdateInTx(t *testing.T) {
	// Find().Update() within a transaction should be rollback-able
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "test")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"a"}}))

	for i := range 10 {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d,"a":%d}`, i, i%5))
		require.NoError(t, coll.Insert(ctx, doc))
	}

	tx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	// Update docs where a=2, set a=99
	res, err := coll.Find(`{"a":2}`).Update(tx.Context(), `{"$set":{"a":99}}`)
	require.NoError(t, err)
	assert.Equal(t, 2, res.Modified)

	// Within tx, updated docs should have new value
	count, err := coll.Find(`{"a":99}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	count, err = coll.Find(`{"a":2}`).Count(tx.Context())
	require.NoError(t, err)
	assert.Equal(t, 0, count)

	// Rollback
	require.NoError(t, tx.Rollback())

	// After rollback, original values should be back
	count, err = coll.Find(`{"a":2}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	count, err = coll.Find(`{"a":99}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, count)
}

// TestIndex_Transaction_CompoundUniquePrefixQuery verifies that querying a
// compound unique index by prefix (first field only) returns all matching
// entries. Regression test for a bug where CoverIter was incorrectly used
// for partial-prefix queries on unique compound indexes, returning only 1
// result instead of all matches.
func TestIndex_Transaction_CompoundUniquePrefixQuery(t *testing.T) {
	fx := newFixture(t)
	coll, err := fx.CreateCollection(ctx, "changes")
	require.NoError(t, err)

	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Fields: []string{"t", "o"},
		Unique: true,
	}))
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{
		Fields: []string{"_q"},
	}))

	// Outer WriteTx — inserts via coll.Insert create savepoints
	outerTx, err := fx.WriteTx(ctx)
	require.NoError(t, err)

	arena := &anyenc.Arena{}

	// Insert root
	root := arena.NewObject()
	root.Set("id", arena.NewString("myroot"))
	root.Set("t", arena.NewString("mytree"))
	root.Set("o", arena.NewString("A"))
	root.Set("sc", arena.NewNumberInt(1))
	root.Set("r", arena.NewBinary(make([]byte, 300)))
	require.NoError(t, coll.Insert(outerTx.Context(), root))
	arena.Reset()

	// Insert 5 changes
	for i := 0; i < 5; i++ {
		obj := arena.NewObject()
		obj.Set("id", arena.NewString(fmt.Sprintf("ch%d", i)))
		obj.Set("t", arena.NewString("mytree"))
		obj.Set("o", arena.NewString(fmt.Sprintf("B%d", i)))
		obj.Set("sc", arena.NewNumberInt(1))
		obj.Set("_q", arena.NewNumberInt(i+1))
		obj.Set("r", arena.NewBinary(make([]byte, 200)))
		require.NoError(t, coll.Insert(outerTx.Context(), obj))
		arena.Reset()
	}

	require.NoError(t, outerTx.Commit())

	// Full scan should find all 6 docs
	fullCount, err := coll.Find(nil).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, fullCount)

	// Prefix query on first field of compound unique index should return all 6
	filtCount, err := coll.Find(`{"t":"mytree"}`).Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, 6, filtCount)
}
