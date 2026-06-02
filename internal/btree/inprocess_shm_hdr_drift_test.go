package btree

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInProcessSHMHeaderFrozenButReadsSeeCommits pins the by-design drift
// documented in docs/btree/NOTES.md#old-drift-inprocess-skips-shm-hdr-on-commit:
//
//	"In-process mode skips SHM hdr updates on commit (writeFrames !w.inProcess
//	 guard). db.BeginRead/BeginWrite synthesize a minimal walHdr{isInit:1,
//	 mxFrame:maxFrame} in that mode so read paths consuming tx.walHdr.mxFrame
//	 see the correct frame ceiling."
//
// The drift relies on TWO invariants that this test pins:
//
//  1. FROZEN HEADER: in in-process mode the heap-SHM region-0 WAL-index header
//     is written exactly once at open (initHeaderStateLocked -> writeHeader(0,
//     0,0,...), wal.go:1718, NOT gated by inProcess) and is NEVER refreshed on
//     commit (writeFrames gates w.index.writeHeader behind `if !w.inProcess`,
//     wal.go:2204). So for the DB's lifetime readHeader() returns the frozen
//     {isInit:1, mxFrame:0, nPage:0} regardless of how many commits land.
//
//  2. SYNTHESIZED CEILING: despite the frozen header, BeginRead (db.go:797) and
//     BeginWrite (db.go:930) synthesize walHdr{isInit:1, mxFrame:maxFrame} from
//     the live process-local mxCommitFrame atomic (beginReadHdr ->
//     tryBeginReadInProcessHdr, wal.go:2613), so reads observe every committed
//     frame.
//
// Why this fails loudly under a future refactor:
//
//   - If someone "fixes" the commit path to publish the SHM header in-process
//     (drops the `!w.inProcess` guard at wal.go:2204), readHeader().mxFrame
//     advances past 0 and the FROZEN HEADER assertion below fails. That guard
//     is load-bearing: the heap SHM hash slots are written without the
//     cross-process barrier (writeHeader's walShmBarrier is itself gated on
//     !inProcess), so publishing a non-zero mxFrame there would expose readers
//     to a header that promises frames the in-process get() path does not route
//     through. The test makes that silent re-coupling impossible.
//
//   - If someone breaks the synthesis (e.g. drops the else-branch at
//     db.go:797/930 so tx.walHdr stays the zero/frozen header), the SYNTHESIZED
//     CEILING assertions fail: WalMaxFrame() reads 0 and the post-commit reader
//     cannot see the committed rows.
func TestInProcessSHMHeaderFrozenButReadsSeeCommits(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	db, err := testOpen(t, dbPath, Options{PageSize: 4096, InProcess: true})
	require.NoError(t, err)
	defer db.Close()

	// Precondition: this DB really is in in-process (heap-SHM) mode, otherwise
	// the drift under test does not apply and the assertions would be vacuous.
	require.True(t, db.pager.inProcess, "test requires InProcess mode")
	require.True(t, db.pager.wal.inProcess, "wal must be in-process")
	require.True(t, db.pager.wal.index.inProcess, "wal index must be in-process")

	// readSHMHeader reads the RAW heap-SHM region-0 WAL-index header — the only
	// thing a cross-process peer (or any non-synthesizing reader) could observe.
	// This bypasses the BeginRead/BeginWrite synthesis entirely.
	readSHMHeader := func() WalIndexHdr {
		hdr, valid := db.pager.wal.index.readHeader()
		require.True(t, valid, "heap-SHM region-0 header must be valid (written once at open)")
		return hdr
	}

	// At open, initHeaderStateLocked has written the frozen header.
	openHdr := readSHMHeader()
	require.Equal(t, uint8(1), openHdr.isInit, "header initialized at open")
	require.Equal(t, uint32(0), openHdr.mxFrame, "open header mxFrame is 0")
	require.Equal(t, uint32(0), openHdr.nPage, "open header nPage is 0")

	// Perform several committed writes. Each commit appends WAL frames and
	// advances the process-local mxCommitFrame, but (in-process) must NOT touch
	// the SHM region-0 header.
	const nsName = "drift"
	const nRows = 64
	const valSize = 200 // forces multi-page growth so nPage would change if published

	want := make(map[uint32][]byte, nRows)
	for round := 0; round < 3; round++ {
		wtx, err := db.BeginWrite()
		require.NoError(t, err)

		// The writer's synthesized walHdr must already carry a non-zero ceiling
		// once frames are committed (invariant 2, BeginWrite path, db.go:930).
		if round > 0 {
			require.NotZero(t, wtx.WalMaxFrame(),
				"BeginWrite must synthesize a non-zero mxFrame after prior commits")
			require.Equal(t, db.pager.wal.index.mxCommitFrame.LoadLocal(), wtx.WalMaxFrame(),
				"synthesized writer mxFrame must equal live committed frame ceiling")
		}

		ns, err := wtx.CreateNamespace(nsName)
		if err != nil {
			// Namespace already exists after the first round; re-resolve it.
			ns, err = wtx.GetNamespace(nsName)
		}
		require.NoError(t, err)

		for i := 0; i < nRows; i++ {
			key := binary.BigEndian.AppendUint32(nil, uint32(round*nRows+i))
			val := []byte(fmt.Sprintf("r%d-row%d-", round, i))
			for len(val) < valSize {
				val = append(val, byte(i))
			}
			require.NoError(t, wtx.Put(ns, key, val))
			want[binary.BigEndian.Uint32(key)] = val
		}
		require.NoError(t, wtx.Commit())

		// INVARIANT 1 (FROZEN HEADER): the raw SHM header is unchanged by the
		// commit. mxCommitFrame has advanced, yet the published header is still
		// frozen at the open-time {isInit:1, mxFrame:0, nPage:0}.
		require.Positive(t, db.pager.wal.index.mxCommitFrame.LoadLocal(),
			"commit must advance the process-local committed-frame cursor")
		postHdr := readSHMHeader()
		require.Equal(t, uint8(1), postHdr.isInit, "header stays initialized")
		require.Equal(t, uint32(0), postHdr.mxFrame,
			"in-process commit must NOT publish mxFrame to the SHM header")
		require.Equal(t, uint32(0), postHdr.nPage,
			"in-process commit must NOT publish nPage to the SHM header")
	}

	committedFrame := db.pager.wal.index.mxCommitFrame.LoadLocal()
	require.Positive(t, committedFrame)

	// INVARIANT 2 (SYNTHESIZED CEILING): a fresh reader synthesizes a correct
	// minimal walHdr from the live committed-frame cursor (NOT from the frozen
	// SHM header), and therefore observes every committed row across the
	// commit->read cycle.
	rtx, err := db.BeginRead()
	require.NoError(t, err)
	defer func() { _ = rtx.Rollback() }()

	require.Equal(t, uint8(1), rtx.walHdr.isInit,
		"reader walHdr must be synthesized as initialized")
	require.Equal(t, committedFrame, rtx.WalMaxFrame(),
		"reader's synthesized mxFrame must equal the committed frame ceiling")
	// Sanity: the synthesized reader ceiling is NOT what the raw SHM header says.
	require.NotEqual(t, readSHMHeader().mxFrame, rtx.WalMaxFrame(),
		"reader ceiling must come from synthesis, not the frozen SHM header")

	ns, err := rtx.GetNamespace(nsName)
	require.NoError(t, err)
	for k, v := range want {
		key := binary.BigEndian.AppendUint32(nil, k)
		got, err := rtx.Get(ns, key)
		require.NoError(t, err, "committed key %d must be visible to a fresh reader", k)
		require.Equal(t, v, got, "value for key %d must match the committed write", k)
	}
}
