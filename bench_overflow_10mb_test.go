package anystore

import (
	"fmt"
	"testing"
	"time"

	"github.com/anyproto/any-store/anyenc"
	"github.com/stretchr/testify/require"
)

// setupLargeBlob inserts a single document with an ~sz-byte "payload"
// field containing a deterministic byte pattern. Returns the collection
// and the inserted doc id. The payload is written as a hex string so
// anyenc serialization stays stable; hex doubles the on-disk size, so
// pass sz/2 to get roughly sz bytes in the DB (close enough for a
// micro-bench — the goal is to span many overflow pages, not hit an
// exact size).
func setupLargeBlob(b *testing.B, sz int) (*collection, int) {
	b.Helper()
	fx := newFixture(b)
	coll, err := fx.CreateCollection(ctx, "blob")
	require.NoError(b, err)

	// Build a hex payload: 2 hex chars per input byte.
	raw := make([]byte, sz/2)
	for i := range raw {
		raw[i] = byte(i * 17) // deterministic, non-repetitive
	}
	payload := fmt.Sprintf("%x", raw)

	doc := anyenc.MustParseJson(fmt.Sprintf(`{"id": 1, "payload": "%s"}`, payload))
	require.NoError(b, coll.Insert(ctx, doc))

	// Force a checkpoint so the blob actually lives in the DB file
	// (overflow chain anchored there), not only in the WAL. Eliminates
	// WAL-vs-DB read path variance in the measurement.
	require.NoError(b, fx.Flush(ctx, 0*time.Second, FlushModeCheckpointFull))

	return coll.(*collection), 1
}

// BenchmarkOverflow10MB_FindId measures the cost of reading a ~10MB
// blob value back via the public FindId API. Includes SHM hash lookup,
// page reads, overflow chain walk, decompression (if any), anyenc
// parse, and the Doc wrapper. A cpuprofile of this bench tells us
// which part dominates.
//
// Run: go test -run='^$' -bench=BenchmarkOverflow10MB_FindId -benchmem -cpuprofile=/tmp/ovfl.prof
func BenchmarkOverflow10MB_FindId(b *testing.B) {
	coll, id := setupLargeBlob(b, 10<<20) // ~10MB

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := coll.FindId(ctx, id)
		if err != nil {
			b.Fatalf("FindId: %v", err)
		}
		_ = d.Value() // force materialization
	}
}

// BenchmarkOverflow10MB_FindId_WithParser uses a reusable parser to
// isolate parser-allocation cost from overflow-read cost. Delta
// against the previous bench shows how much parser churn contributes.
func BenchmarkOverflow10MB_FindId_WithParser(b *testing.B) {
	coll, id := setupLargeBlob(b, 10<<20)

	p := &anyenc.Parser{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := coll.FindIdWithParser(ctx, p, id)
		if err != nil {
			b.Fatalf("FindId: %v", err)
		}
		_ = d.Value()
	}
}

// setupLargeBlobMmap is like setupLargeBlob but opens the DB with
// MmapSize = 64 MiB. Used to measure the mmap-vs-ReadAt delta.
func setupLargeBlobMmap(b *testing.B, sz int) (*collection, int) {
	b.Helper()
	fx := newFixture(b, &Config{MmapSize: 64 << 20})
	coll, err := fx.CreateCollection(ctx, "blob")
	require.NoError(b, err)

	raw := make([]byte, sz/2)
	for i := range raw {
		raw[i] = byte(i * 17)
	}
	payload := fmt.Sprintf("%x", raw)

	doc := anyenc.MustParseJson(fmt.Sprintf(`{"id": 1, "payload": "%s"}`, payload))
	require.NoError(b, coll.Insert(ctx, doc))
	require.NoError(b, fx.Flush(ctx, 0*time.Second, FlushModeCheckpointFull))

	return coll.(*collection), 1
}

// BenchmarkOverflow10MB_FindId_Mmap is the mmap-enabled counterpart
// to BenchmarkOverflow10MB_FindId. Delta against the baseline is the
// measured value of the mmap change.
func BenchmarkOverflow10MB_FindId_Mmap(b *testing.B) {
	coll, id := setupLargeBlobMmap(b, 10<<20)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		d, err := coll.FindId(ctx, id)
		if err != nil {
			b.Fatalf("FindId: %v", err)
		}
		_ = d.Value()
	}
}

// BenchmarkOverflow_SizeSweep_FindId scans blob sizes to expose how
// per-op cost scales with overflow-chain length. Slope tells us
// whether we're bound by the walk (linear in chain length) or by
// payload size (linear in bytes copied) or by syscalls (linear in
// pages).
func BenchmarkOverflow_SizeSweep_FindId(b *testing.B) {
	sizes := []int{
		64 << 10,  // 64 KB — ~16 overflow pages
		512 << 10, // 512 KB — ~128
		2 << 20,   // 2 MB — ~512
		10 << 20,  // 10 MB — ~2560
	}
	for _, sz := range sizes {
		b.Run(fmt.Sprintf("sz=%dKB", sz>>10), func(b *testing.B) {
			coll, id := setupLargeBlob(b, sz)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				d, err := coll.FindId(ctx, id)
				if err != nil {
					b.Fatalf("FindId: %v", err)
				}
				_ = d.Value()
			}
		})
	}
}
