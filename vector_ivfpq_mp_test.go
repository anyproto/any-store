//go:build (linux || darwin) && (amd64 || arm64)

package anystore

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/stretchr/testify/require"
)

// TestIVFPQMultiprocessConsistency verifies cross-process consistency of the
// btree-resident IVF-PQ index. The IVF index caches the trained codebooks
// (coarse + PQ + precomputed table) in RAM, so the real hazard is: after a PEER
// process rebuilds the index (retraining the codebooks and recreating the
// namespaces), this process must reload that cached model — otherwise it would
// decode the rebuilt :cell codes with a stale model and return garbage.
//
// The test re-execs itself as a child process (anystore forbids two opens of one
// file within a single process; true cross-process needs separate OS processes,
// as in internal/btree/multiprocess_test.go). The PARENT builds the index and
// keeps its handle open; the CHILD opens the same file and rebuilds; the PARENT
// then searches on its still-open handle and must return correct results.
func TestIVFPQMultiprocessConsistency(t *testing.T) {
	if mode := os.Getenv("ASV_IVFMP_CHILD"); mode != "" {
		ivfmpChild(t, mode)
		return
	}

	const (
		dim = 24
		n   = 2000
		k   = 10
	)
	dir := t.TempDir()
	path := filepath.Join(dir, "mp.db")
	A := clusteredVecsAS(n, dim, 40, 1)

	// PARENT: build the index, keep the handle open.
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.CreateCollection(ctx, "docs")
	require.NoError(t, err)
	for i, v := range A {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(i, v))))
	}
	require.NoError(t, coll.CreateIndex(ctx, IndexInfo{
		Name: "emb", Kind: IndexKindVector,
		Vector: &VectorParams{Field: "v", Dim: dim, Metric: VectorL2, Mode: VectorModeIVFPQ, Closure: 4, NProbe: 16},
	}))

	queryIdx := []int{42, 100, 500, 1900}
	recallOf := func() float64 {
		var sum float64
		for _, qi := range queryIdx {
			truth := bruteIDs(A, A[qi], k)
			hh, err := vsearch(coll, "v", A[qi], k, 0)
			require.NoError(t, err) // a stale namespace handle would error here
			hit := 0
			for _, h := range hh {
				if truth[string(h.DocId)] {
					hit++
				}
			}
			sum += float64(hit) / float64(k)
		}
		return sum / float64(len(queryIdx))
	}

	findsSentinel := func(id int) bool {
		hits, err := vsearch(coll, "v", ivfmpSentinel(dim, id), 1, 0)
		require.NoError(t, err) // a stale namespace handle would error here
		return string(hits[0].DocId) == string(idBytesOf(id))
	}
	runChild := func(mode string) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestIVFPQMultiprocessConsistency$", "-test.v=true")
		cmd.Env = append(os.Environ(), "ASV_IVFMP_CHILD="+mode, "ASV_IVFMP_PATH="+path)
		done := make(chan struct{})
		var out []byte
		var cerr error
		go func() { out, cerr = cmd.CombinedOutput(); close(done) }()
		select {
		case <-done:
		case <-time.After(90 * time.Second):
			_ = cmd.Process.Kill()
			t.Fatalf("child (%s) timed out", mode)
		}
		require.NoError(t, cerr, "child (%s) failed:\n%s", mode, out)
	}

	// Baseline: this handle's own (gen-0) model; neither sentinel exists yet.
	require.GreaterOrEqual(t, recallOf(), 0.85, "baseline recall")
	require.False(t, findsSentinel(ivfmpInsertID), "insert sentinel absent pre-test")
	require.False(t, findsSentinel(ivfmpRebuildID), "rebuild sentinel absent pre-test")

	// Phase 1 — DATA staleness: child inserts a sentinel WITHOUT rebuilding (no
	// schema change, model unchanged). The parent must see it via MVCC on its next
	// read tx, scored with the still-valid cached model.
	runChild("insert")
	require.True(t, findsSentinel(ivfmpInsertID), "parent must see peer's plain insert (data staleness)")
	require.GreaterOrEqual(t, recallOf(), 0.85, "recall after peer insert")

	// Phase 2 — SCHEMA staleness: child inserts another sentinel AND rebuilds
	// (retrains codebooks with a different seed, recreates namespaces, marks schema
	// changed). The parent must reconcile and reload the gen-1 model + namespaces —
	// a stale cached model would error or return garbage.
	runChild("rebuild")
	require.True(t, findsSentinel(ivfmpRebuildID), "parent must find sentinel from peer's rebuilt index (model reload)")
	require.True(t, findsSentinel(ivfmpInsertID), "the phase-1 sentinel must survive the rebuild")
	require.GreaterOrEqual(t, recallOf(), 0.85, "recall after peer rebuild — stale model would collapse this")
}

const (
	ivfmpInsertID  = 99998
	ivfmpRebuildID = 99999
)

// ivfmpSentinel is a vector far outside the clustered data range ([-1,1]) — its
// only neighbour is itself — keyed by id so the two sentinels sit at distinct
// far-apart points. Both parent and child construct it identically.
func ivfmpSentinel(dim, id int) []float32 {
	v := make([]float32, dim)
	val := float32(50 + id%10) // distinct far points per sentinel
	for i := range v {
		v[i] = val
	}
	return v
}

// ivfmpChild is the child-process body. mode "insert" adds a sentinel without
// touching the schema; mode "rebuild" adds a sentinel and retrains the index.
func ivfmpChild(t *testing.T, mode string) {
	const dim = 24
	path := os.Getenv("ASV_IVFMP_PATH")
	require.NotEmpty(t, path)
	db, err := Open(ctx, path, nil)
	require.NoError(t, err)
	defer db.Close()
	coll, err := db.OpenCollection(ctx, "docs")
	require.NoError(t, err)
	switch mode {
	case "insert":
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(ivfmpInsertID, ivfmpSentinel(dim, ivfmpInsertID)))))
	case "rebuild":
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(vecDocJSON(ivfmpRebuildID, ivfmpSentinel(dim, ivfmpRebuildID)))))
		require.NoError(t, coll.CompactVectorIndex(ctx, "emb"))
	default:
		t.Fatalf("unknown child mode %q", mode)
	}
}
