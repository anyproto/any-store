package btree

import (
	"encoding/binary"
	"path/filepath"
	"testing"
)

// numIVFCells approximates an IVF nlist for ~40k vectors (sqrt(n)-ish).
const numIVFCells = 196

// BenchmarkBalanceAlloc_IVFCellInterleaved reproduces the IVF-SQ :cell write
// path allocation profile. The :cell key is cellID‖label and the build loop
// iterates by label (build order), assigning each vector to its IVF cell — so
// inserts are INTERLEAVED across many cell prefixes, not globally ascending.
// That scatters inserts across many active rightmost-of-prefix leaves and drives
// the GENERAL balanceNonroot sibling-gather path (collectLeafCells x3 per balance
// + rewriteParentAfterBalance) — not the splitLeafRightmostAppend fast path a
// single ascending stream would hit. Records are ~772 bytes (4 + 768 int8, fits
// locally). Run with -benchmem / -memprofile to inspect the balance subtree.
func BenchmarkBalanceAlloc_IVFCellInterleaved(b *testing.B) {
	resetPageBufferPool()
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
	if _, err = tx.CreateNamespace("cell"); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}

	val := make([]byte, 772) // 4 + 768 int8, fits locally (no overflow)
	tx, err = db.BeginWrite()
	if err != nil {
		b.Fatal(err)
	}
	ns, err := db.getNamespaceLocked("cell")
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// key = cellID‖label: round-robin the cell prefix so consecutive inserts
		// land in different prefixes (interleaved), with the label ascending
		// within each prefix — the real IVF-SQ :cell insert pattern.
		cell := uint32(i % numIVFCells)
		label := uint32(i / numIVFCells)
		key := binary.BigEndian.AppendUint32(nil, cell)
		key = binary.BigEndian.AppendUint32(key, label)
		if err := tx.Put(ns, key, val); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	if err := tx.Commit(); err != nil {
		b.Fatal(err)
	}
}
