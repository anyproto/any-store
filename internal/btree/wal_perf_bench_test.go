package btree

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const (
	benchWALRows = 30000
	benchValSize = 128
)

func prepareBenchDBWithLargeWAL(b *testing.B, disableAutoCheckpoint bool) (*DB, *Namespace, [][]byte, string) {
	b.Helper()

	dir := b.TempDir()
	path := filepath.Join(dir, "bench.db")

	opts := DefaultOptions()
	opts.InProcess = true
	opts.NoCommitSync = true
	opts.DisableAutoCheckpoint = disableAutoCheckpoint
	if !disableAutoCheckpoint {
		opts.AutoCheckpointAfter = 64
	}

	db, err := Open(path, opts)
	requireNoErrorB(b, err)
	b.Cleanup(func() { _ = db.Close() })

	wtx, err := db.BeginWrite()
	requireNoErrorB(b, err)
	ns, err := wtx.CreateNamespace("bench")
	requireNoErrorB(b, err)
	requireNoErrorB(b, wtx.Commit())

	keys := make([][]byte, benchWALRows)
	val := make([]byte, benchValSize)
	for i := range benchWALRows {
		keys[i] = []byte(fmt.Sprintf("k-%08d", i))
		wtx, err = db.BeginWrite()
		requireNoErrorB(b, err)
		ns2, err := wtx.GetNamespace("bench")
		requireNoErrorB(b, err)
		requireNoErrorB(b, wtx.Put(ns2, keys[i], val))
		requireNoErrorB(b, wtx.Commit())
	}
	return db, ns, keys, path
}

func reportWALMetrics(b *testing.B, db *DB, path string) {
	b.Helper()
	info, err := os.Stat(path + "-wal")
	if err == nil {
		b.ReportMetric(float64(info.Size())/1024.0/1024.0, "wal_mb")
	}
	b.ReportMetric(float64(db.pager.wal.nFrame.Load()), "wal_frames")
}

func BenchmarkReadLargeWAL_NoAutoCheckpoint(b *testing.B) {
	db, ns, keys, path := prepareBenchDBWithLargeWAL(b, true)
	b.ResetTimer()

	var v []byte
	for i := 0; i < b.N; i++ {
		rtx, err := db.BeginRead()
		requireNoErrorB(b, err)
		v, err = rtx.Get(ns, keys[i%len(keys)])
		requireNoErrorB(b, err)
		requireNoErrorB(b, rtx.Rollback())
	}
	_ = v
	b.StopTimer()
	reportWALMetrics(b, db, path)
}

func BenchmarkReadLargeWAL_AutoCheckpoint(b *testing.B) {
	db, ns, keys, path := prepareBenchDBWithLargeWAL(b, false)
	b.ResetTimer()

	var v []byte
	for i := 0; i < b.N; i++ {
		rtx, err := db.BeginRead()
		requireNoErrorB(b, err)
		v, err = rtx.Get(ns, keys[i%len(keys)])
		requireNoErrorB(b, err)
		requireNoErrorB(b, rtx.Rollback())
	}
	_ = v
	b.StopTimer()
	reportWALMetrics(b, db, path)
}

func BenchmarkMixedLargeWAL_NoAutoCheckpoint(b *testing.B) {
	db, ns, keys, path := prepareBenchDBWithLargeWAL(b, true)
	b.ResetTimer()

	val := make([]byte, benchValSize)
	for i := 0; i < b.N; i++ {
		wtx, err := db.BeginWrite()
		requireNoErrorB(b, err)
		ns2, err := wtx.GetNamespace("bench")
		requireNoErrorB(b, err)
		k := []byte(fmt.Sprintf("w-%08d", i))
		requireNoErrorB(b, wtx.Put(ns2, k, val))
		requireNoErrorB(b, wtx.Commit())

		rtx, err := db.BeginRead()
		requireNoErrorB(b, err)
		_, err = rtx.Get(ns, keys[i%len(keys)])
		requireNoErrorB(b, err)
		requireNoErrorB(b, rtx.Rollback())
	}
	b.StopTimer()
	reportWALMetrics(b, db, path)
}

func BenchmarkOpenWithLargeWAL_NoAutoCheckpoint(b *testing.B) {
	dir := b.TempDir()
	path := filepath.Join(dir, "open-bench.db")
	opts := DefaultOptions()
	opts.InProcess = true
	opts.NoCommitSync = true
	opts.DisableAutoCheckpoint = true

	// Build a large WAL once and keep it uncheckpointed.
	db, err := Open(path, opts)
	requireNoErrorB(b, err)
	wtx, err := db.BeginWrite()
	requireNoErrorB(b, err)
	_, err = wtx.CreateNamespace("bench")
	requireNoErrorB(b, err)
	requireNoErrorB(b, wtx.Commit())
	val := make([]byte, benchValSize)
	for i := range benchWALRows {
		wtx, err = db.BeginWrite()
		requireNoErrorB(b, err)
		ns, err := wtx.GetNamespace("bench")
		requireNoErrorB(b, err)
		requireNoErrorB(b, wtx.Put(ns, []byte(fmt.Sprintf("k-%08d", i)), val))
		requireNoErrorB(b, wtx.Commit())
	}
	rawClose(db)

	walInfo, err := os.Stat(path + "-wal")
	requireNoErrorB(b, err)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dbi, openErr := Open(path, opts)
		requireNoErrorB(b, openErr)
		rawClose(dbi)
	}
	b.StopTimer()
	b.ReportMetric(float64(walInfo.Size())/1024.0/1024.0, "wal_mb")
}

func requireNoErrorB(b *testing.B, err error) {
	b.Helper()
	if err != nil {
		b.Fatal(err)
	}
}
