//go:build (linux || darwin) && (amd64 || arm64)

package btree

import (
	"path/filepath"
	"testing"
)

// BenchmarkShmHashGet is the shared-memory page lookup over two segments: a
// page found in the newest segment, one found in the oldest, one in neither.
func BenchmarkShmHashGet(b *testing.B) {
	wi, err := newWalIndex(filepath.Join(b.TempDir(), "test.shm"), false)
	requireNoErrorB(b, err)
	defer func() { _ = wi.close(true) }()
	const nFrames = htNPageOne + 2000
	for f := uint32(1); f <= nFrames; f++ {
		requireNoErrorB(b, wi.set(f+1, f))
	}
	for _, c := range []struct {
		name string
		pgno uint32
	}{{"newest", nFrames}, {"oldest", 100}, {"miss", nFrames + 100}} {
		b.Run(c.name, func(b *testing.B) {
			var sum uint32
			for i := 0; i < b.N; i++ {
				f, err := wi.shmHashGet(c.pgno, nFrames, 1)
				if err != nil {
					b.Fatal(err)
				}
				sum += f
			}
			_ = sum
		})
	}
}

// BenchmarkWalIndexAppend is the cost of indexing one appended frame
// (walIndex.set: the page number, the dead-writer guard, the hash slot), on a
// wal-index of each kind. The segment is started over every 4000 frames,
// which zeroes it once.
func BenchmarkWalIndexAppend(b *testing.B) {
	for _, c := range []struct {
		name      string
		inProcess bool
	}{{"shared-memory", false}, {"heap", true}} {
		b.Run(c.name, func(b *testing.B) {
			wi, err := newWalIndex(filepath.Join(b.TempDir(), "test.shm"), c.inProcess)
			requireNoErrorB(b, err)
			defer func() { _ = wi.close(true) }()
			const nFrames = 4000
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				frame := uint32(i%nFrames) + 1
				if frame == 1 {
					wi.rollbackToFrame(0) // the log is started over
				}
				if err := wi.set(frame+1, frame); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
