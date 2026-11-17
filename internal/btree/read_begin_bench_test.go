package btree

import (
	"path/filepath"
	"testing"
)

// BenchmarkReadBeginRollback is a read transaction that does nothing: the
// cost of a begin at the snapshot the last one had, per mode.
func BenchmarkReadBeginRollback(b *testing.B) {
	for _, mode := range []string{"multiprocess", "inprocess", "inmemory"} {
		b.Run(mode, func(b *testing.B) {
			opts := DefaultOptions()
			path := filepath.Join(b.TempDir(), "bench.db")
			switch mode {
			case "inprocess":
				opts.InProcess = true
			case "inmemory":
				opts.InMemory = true
				path = ""
			}
			db, err := testOpen(b, path, opts)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			wtx, err := db.BeginWrite()
			if err != nil {
				b.Fatal(err)
			}
			ns, err := wtx.CreateNamespace("bench")
			if err != nil {
				b.Fatal(err)
			}
			if err = wtx.Put(ns, []byte("k"), []byte("v")); err != nil {
				b.Fatal(err)
			}
			wtx.MarkDataChanged()
			wtx.MarkSchemaChanged()
			if err = wtx.Commit(); err != nil {
				b.Fatal(err)
			}
			// The test-build check reads page 1 at every begin: off here.
			SetCheckPage1ForTest(false)
			defer SetCheckPage1ForTest(true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rtx, err := db.BeginRead()
				if err != nil {
					b.Fatal(err)
				}
				if err = rtx.Rollback(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
