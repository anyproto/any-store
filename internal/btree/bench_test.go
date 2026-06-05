package btree

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
)

// benchDB creates a temporary on-disk DB for benchmarks with default options.
func benchDB(b *testing.B) *DB {
	b.Helper()
	dir := b.TempDir()
	opts := DefaultOptions()
	opts.InProcess = true
	opts.NoCommitSync = true
	db, err := Open(filepath.Join(dir, "bench.db"), opts)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

// benchPopulate inserts n keys with the given value size into namespace "bench".
// Returns the namespace handle.
func benchPopulate(b *testing.B, db *DB, n int, valSize int) *Namespace {
	b.Helper()
	tx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := tx.CreateNamespace("bench")
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	val := make([]byte, valSize)
	const batchSize = 1000
	for start := 1; start <= n; start += batchSize {
		tx, err = db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		end := start + batchSize
		if end > n+1 {
			end = n + 1
		}
		for i := start; i < end; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i))
			if err := tx.Put(ns, key, val); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	return ns
}

// BenchmarkGet measures single-key read throughput.
func BenchmarkGet(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	// Pre-generate random keys within the populated range.
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		key := keys[i%numKeys]
		_, err = tx.Get(ns, key)
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetBatch measures batched read throughput (many reads per transaction).
func BenchmarkGetBatch(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	const batchSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginRead()
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < batchSize; j++ {
			key := keys[rand.Intn(numKeys)]
			_, err = tx.Get(ns, key)
			if err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCursorScan measures full cursor iteration throughput.
func BenchmarkCursorScan(b *testing.B) {
	for _, numKeys := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			db := benchDB(b)
			ns := benchPopulate(b, db, numKeys, 100)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				tx, err := db.BeginRead()
				if err != nil {
					b.Fatal(err)
				}
				cur := tx.NewCursor(ns)
				count := 0
				for err := cur.First(); err == nil && cur.Valid(); err = cur.Next() {
					_, _ = cur.Key()
					_, _ = cur.Value()
					count++
				}
				cur.Close()
				if err := tx.Rollback(); err != nil {
					b.Fatal(err)
				}
				if count != numKeys {
					b.Fatalf("expected %d keys, got %d", numKeys, count)
				}
			}
		})
	}
}

// BenchmarkPut measures single-key write throughput.
func BenchmarkPut(b *testing.B) {
	db := benchDB(b)
	tx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := tx.CreateNamespace("bench")
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	val := make([]byte, 100)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		key := binary.BigEndian.AppendUint32(nil, uint32(i+1))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkConcurrentReaders measures parallel read throughput.
func BenchmarkConcurrentReaders(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			tx, err := db.BeginRead()
			if err != nil {
				b.Fatal(err)
			}
			key := keys[r.Intn(numKeys)]
			_, err = tx.Get(ns, key)
			if err != nil {
				b.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkWriterWithReaders measures throughput when one writer runs
// concurrently with multiple readers.
func BenchmarkWriterWithReaders(b *testing.B) {
	const numKeys = 10000
	const valSize = 100
	db := benchDB(b)
	ns := benchPopulate(b, db, numKeys, valSize)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = binary.BigEndian.AppendUint32(nil, uint32(i+1))
	}

	// Start background readers.
	const numReaders = 4
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewSource(rand.Int63()))
			for {
				select {
				case <-stop:
					return
				default:
				}
				tx, err := db.BeginRead()
				if err != nil {
					return
				}
				key := keys[rng.Intn(numKeys)]
				_, _ = tx.Get(ns, key)
				_ = tx.Rollback()
			}
		}()
	}

	val := make([]byte, valSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		key := binary.BigEndian.AppendUint32(nil, uint32(rand.Intn(numKeys)+1))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	close(stop)
	wg.Wait()
}

// BenchmarkPutOverflow measures insert throughput with overflow-sized values.
// This exercises collectLeafCells + rebuildLeafPage during splits.
// With raw cell passthrough, splits skip overflow I/O and allocation entirely.
func BenchmarkPutOverflow(b *testing.B) {
	db := benchDB(b)
	tx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := tx.CreateNamespace("bench")
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Value larger than maxLocal to trigger overflow on every cell
	usable := 4096
	maxLocal := maxLocalPayload(usable)
	valSize := maxLocal + 100
	val := make([]byte, valSize)
	for i := range val {
		val[i] = byte(i)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Batch inserts to trigger many splits per commit
	const batchSize = 50
	for i := 0; i < b.N; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < batchSize; j++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(i*batchSize+j))
			if err := tx.Put(ns, key, val); err != nil {
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCollectRebuildOverflow directly benchmarks collectLeafCells + rebuildLeafPage
// on a page full of overflow cells. This isolates the allocation improvement.
func BenchmarkCollectRebuildOverflow(b *testing.B) {
	db := benchDB(b)

	tx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := tx.CreateNamespace("bench")
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	usable := 4096
	maxLocal := maxLocalPayload(usable)
	valSize := maxLocal + 100
	val := make([]byte, valSize)

	// Insert enough keys to have several full pages with overflow cells
	tx, err = db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	// Benchmark collect+rebuild on a leaf page with overflow cells
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		txw, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		bt := &btree{pager: txw.pager, rootPage: ns.rootPage, walMaxFrame: txw.walMaxFrame, writable: true}
		pg, err := bt.getPage(bt.rootPage)
		if err != nil {
			b.Fatal(err)
		}

		// Walk to a leaf page
		for pg.header.isInterior() {
			childOff := pg.getCellOffset(0)
			childPgno := binary.BigEndian.Uint32(pg.data[childOff:])
			bt.pager.releasePage(pg)
			pg, err = bt.getPage(childPgno)
			if err != nil {
				b.Fatal(err)
			}
		}

		wpg, err := bt.pager.getWritablePage(pg.pgno)
		bt.pager.releasePage(pg)
		if err != nil {
			b.Fatal(err)
		}

		cells, cellBuf, cerr := bt.collectLeafCells(wpg)
		if cerr != nil {
			b.Fatal(cerr)
		}
		_ = bt.rebuildLeafPage(wpg, cells)
		bt.pager.recycleCellBuf(cellBuf)
		bt.pager.releasePage(wpg)

		if err := txw.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertSepIntoInterior_DeepTree measures the cost of
// inserting a divider into an interior page. Before commit 2 of the
// balance_quick port, insertSepIntoInterior re-scanned the parent
// linearly (O(nCell)) to locate the insertion slot. With cellIdx
// threaded through pathEntry, the scan is eliminated; the improvement
// is proportional to parent fan-out.
//
// SQLite's equivalent: balance_nonroot takes iIdx as a parameter
// (btree.c:8230, 9213), populated from the cursor stack.
func BenchmarkInsertSepIntoInterior_DeepTree(b *testing.B) {
	resetPoolForTest(b)
	dir := b.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	_, err = tx.CreateNamespace("t1")
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	tx, err = db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := db.getNamespaceLocked("t1")
	if err != nil {
		b.Fatal(err)
	}
	val := make([]byte, 64)
	for i := 1; i <= 200_000; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tx, err := db.BeginWrite()
		if err != nil {
			b.Fatal(err)
		}
		ns, err := db.getNamespaceLocked("t1")
		if err != nil {
			b.Fatal(err)
		}
		key := binary.BigEndian.AppendUint32(nil, uint32(200_001+i))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBalanceQuick_MonotonicAppend measures per-row throughput of
// monotonic ObjectID-style appends — any-store's dominant write pattern.
// Before the balance_quick port, each overflow triggered a 2-way split
// copying ~1/3 of the page to a new sibling; after the port, rightmost
// appends leave the left page untouched and put one cell on the new
// sibling. Both leaf count and bytes-written-per-insert should drop.
func BenchmarkBalanceQuick_MonotonicAppend(b *testing.B) {
	resetPoolForTest(b)
	dir := b.TempDir()
	db, err := Open(filepath.Join(dir, "test.db"), Options{PageSize: 4096})
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	_, err = tx.CreateNamespace("t1")
	if err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	val := make([]byte, 128)
	tx, err = db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := db.getNamespaceLocked("t1")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := binary.BigEndian.AppendUint32(nil, uint32(i+1))
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	rtx, err := db.BeginRead()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = rtx.Rollback() }()
	ns2, err := db.getNamespaceLocked("t1")
	if err != nil {
		b.Fatal(err)
	}
	bt := &btree{pager: db.pager, rootPage: ns2.rootPage, walMaxFrame: rtx.walMaxFrame}
	leafCount := 0
	walkForLeaves(bt, bt.rootPage, &leafCount)
	b.ReportMetric(float64(leafCount), "leaves")
	if b.N > 0 {
		b.ReportMetric(float64(leafCount)/float64(b.N), "leaves/row")
	}
}

// walkForLeaves counts leaves in a btree without parsing cells.
func walkForLeaves(bt *btree, pgno uint32, count *int) {
	pg, err := bt.getPage(pgno)
	if err != nil {
		return
	}
	if pg.header.isLeaf() {
		*count++
		bt.pager.releasePage(pg)
		return
	}
	n := int(pg.header.cellCount)
	cpOff := pg.cellPointerOffset()
	children := make([]uint32, 0, n+1)
	for i := 0; i < n; i++ {
		off := int(binary.BigEndian.Uint16(pg.data[cpOff+i*2:]))
		children = append(children, binary.BigEndian.Uint32(pg.data[off:off+4]))
	}
	children = append(children, pg.header.rightChild)
	bt.pager.releasePage(pg)
	for _, c := range children {
		walkForLeaves(bt, c, count)
	}
}
