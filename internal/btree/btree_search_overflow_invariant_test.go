package btree

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSearchLeafOverflow_PrefixShortcutInvariant pins the by-design drift
// documented at
// docs/btree/NOTES.md#old-drift-binsearch-rawbytes-prefix-no-overflow-cache.
//
// searchLeafWithOverflow optimizes the binary search over overflow cells by
// comparing only the on-page local key prefix first (btree.go:632-655). The
// invariant a correct refactor MUST preserve:
//
//	A truncated prefix decision (prefixCmp != 0 over cmpLen = min(localKeyBytes,
//	len(key))) MAY short-circuit and skip the full overflow read, BUT whenever
//	the prefix compares EQUAL on cmpLen the code MUST fall through to a full
//	leafFullKey read so that NO ordering decision is ever made on truncated
//	bytes.
//
// We pin it via the test-only searchLeafOverflowProbe hook, which records
// whether each overflow cell was decided by the prefix shortcut
// (searchProbePrefixShortCircuit) or by a full key read (searchProbeFullKeyRead).
// A future refactor that decides ordering on equal-but-truncated bytes (e.g.
// dropping the leafFullKey fall-through) would change the recorded events and
// fail this test loudly. Production behavior is unchanged: the probe is nil
// outside tests.
func TestSearchLeafOverflow_PrefixShortcutInvariant(t *testing.T) {
	// Small page so a modest key overflows on-page.
	p := tempPagerWithPageSize(t, 512)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	// Build a single overflow leaf cell whose KEY spills to overflow pages.
	// The stored full key is distinct in every byte position so we can derive
	// case-specific search keys below. value is tiny and irrelevant.
	keyLen := maxLocal + 80 // > maxLocal => key overflows
	storedKey := make([]byte, keyLen)
	for i := range storedKey {
		// Keep bytes in the middle of the range so we can craft both a strictly
		// smaller and a strictly larger byte at any position.
		storedKey[i] = byte('m')
	}
	value := []byte("v")

	require.NoError(t, bt.rebuildLeafPage(pg, []cellData{{key: storedKey, value: value}}))

	// Derive the local-key split for the overflow cell exactly as the search
	// code does, so test cases target the prefix vs overflow regions precisely.
	totalPayload := keyLen + len(value)
	require.Greater(t, totalPayload, maxLocal, "stored key must overflow")
	nLocal := localPayloadSize(totalPayload, usableSize)
	localKeyBytes := min(nLocal, keyLen)
	require.Less(t, localKeyBytes, keyLen,
		"test requires the key itself to overflow (localKeyBytes < keyLen)")
	require.Greater(t, localKeyBytes, 1, "need at least 2 local prefix bytes")

	// Install the probe to record which decision path each cell takes.
	var events []int
	searchLeafOverflowProbe = func(event int) { events = append(events, event) }
	t.Cleanup(func() { searchLeafOverflowProbe = nil })

	// search runs one probe-recorded search and returns the recorded event
	// sequence, the matched index, and whether an exact match was found.
	search := func(key []byte) (gotEvents []int, idx int, found bool) {
		events = nil
		i, f, serr := searchLeafWithOverflow(pg, key, usableSize, p, 0, nil)
		require.NoError(t, serr)
		return events, i, f
	}

	t.Run("differing byte within local prefix short-circuits with no overflow read", func(t *testing.T) {
		// Key differs from stored at a position inside the local prefix.
		// The prefix alone determines ordering => short-circuit, no full read.
		smaller := bytes.Clone(storedKey)
		smaller[1] = 'a' // 'a' < 'm' within the local prefix
		ev, _, found := search(smaller)
		require.Equal(t, []int{searchProbePrefixShortCircuit}, ev,
			"differing byte in the local prefix must be decided WITHOUT a full overflow read")
		require.False(t, found)

		larger := bytes.Clone(storedKey)
		larger[1] = 'z' // 'z' > 'm' within the local prefix
		ev, _, found = search(larger)
		require.Equal(t, []int{searchProbePrefixShortCircuit}, ev,
			"differing byte in the local prefix must be decided WITHOUT a full overflow read")
		require.False(t, found)
	})

	t.Run("equal full keys: prefix equal falls through to full read => found", func(t *testing.T) {
		// Searching the exact stored key: prefix is equal on cmpLen, so the code
		// MUST take the full read and return EQUAL (found).
		ev, _, found := search(bytes.Clone(storedKey))
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"equal-on-prefix must fall through to a full key read, not a truncated decision")
		require.True(t, found, "exact overflow key must be found via the full read")
	})

	t.Run("differing byte beyond local prefix: prefix equal => full read disambiguates", func(t *testing.T) {
		// Search key equals stored on the whole local prefix but differs in the
		// overflow region. The prefix shortcut sees equality and MUST fall
		// through; only the full read can decide ordering correctly.
		largerInOverflow := bytes.Clone(storedKey)
		largerInOverflow[localKeyBytes+5] = 'z' // 'z' > 'm', beyond the local prefix
		ev, _, found := search(largerInOverflow)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"a difference beyond the local prefix must be decided by a full read")
		require.False(t, found, "search key > stored => no exact match")

		smallerInOverflow := bytes.Clone(storedKey)
		smallerInOverflow[localKeyBytes+5] = 'a' // 'a' < 'm', beyond the local prefix
		ev, _, found = search(smallerInOverflow)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"a difference beyond the local prefix must be decided by a full read")
		require.False(t, found)
	})

	t.Run("search key SHORTER than local prefix: cmpLen truncates to len(key), prefix equal => full read", func(t *testing.T) {
		// len(key) < localKeyBytes: cmpLen == len(key). The truncated compare is
		// equal, so the code MUST NOT decide on it; it falls through to a full
		// read where the longer stored key is correctly ordered as the greater.
		shortKey := bytes.Clone(storedKey[:localKeyBytes-2])
		ev, _, found := search(shortKey)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"a search key shorter than the local prefix must not be decided on truncated bytes")
		require.False(t, found, "shorter key is a strict prefix => not equal to the longer stored key")
	})

	t.Run("search key LONGER than localKeyBytes but equal on the local portion => full read", func(t *testing.T) {
		// len(key) > localKeyBytes, equal on the whole local prefix. cmpLen ==
		// localKeyBytes, prefix equal => full read disambiguates against the
		// overflow bytes.
		longerEqualLocal := bytes.Clone(storedKey)
		longerEqualLocal = append(longerEqualLocal, 'x') // strictly longer than stored
		ev, _, found := search(longerEqualLocal)
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"equal on the local portion must fall through to a full read")
		require.False(t, found, "stored key is a strict prefix of the search key => not equal")
	})

	t.Run("empty search key: cmpLen==0, no slice panic, full read taken", func(t *testing.T) {
		// len(key)==0 => cmpLen==0. bytes.Compare on empty slices is 0, so the
		// prefix is "equal"; the code must fall through to a full read (no
		// truncated decision) and must not panic on the empty-slice indexing.
		ev, _, found := search([]byte{})
		require.Equal(t, []int{searchProbeFullKeyRead}, ev,
			"empty key yields cmpLen==0 (equal); must fall through to a full read")
		require.False(t, found, "non-empty stored key is never equal to the empty key")
	})
}

// TestSearchLeafOverflow_StoredKeyExtendsSearchKey pins the specific case where
// the search key is a TRUE PREFIX of a longer stored overflow key: the prefix
// compares equal on cmpLen, the full read is taken, and full bytes.Compare must
// report stored > search. This is a focused complement to the table above so a
// regression in the "stored extends search" direction is unmissable.
func TestSearchLeafOverflow_StoredKeyExtendsSearchKey(t *testing.T) {
	p := tempPagerWithPageSize(t, 512)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	storedKey := bytes.Repeat([]byte("p"), maxLocal+80) // overflows
	require.NoError(t, bt.rebuildLeafPage(pg, []cellData{{key: storedKey, value: []byte("v")}}))

	nLocal := localPayloadSize(len(storedKey)+1, usableSize)
	localKeyBytes := min(nLocal, len(storedKey))

	var events []int
	searchLeafOverflowProbe = func(event int) { events = append(events, event) }
	t.Cleanup(func() { searchLeafOverflowProbe = nil })

	// Search key is the stored key truncated past the local prefix: a true
	// prefix of the longer overflow key. cmpLen == len(searchKey) == prefix is
	// equal, so the full read must run and order stored > search.
	searchKey := bytes.Clone(storedKey[:localKeyBytes+10])
	events = nil
	idx, found, serr := searchLeafWithOverflow(pg, searchKey, usableSize, p, 0, nil)
	require.NoError(t, serr)
	require.Equal(t, []int{searchProbeFullKeyRead}, events,
		"a true prefix of a longer stored key must be resolved by a full read, not a truncated decision")
	require.False(t, found, "search key is a strict prefix => not equal to the longer stored key")
	// stored > search => insertion point is before the cell (index 0).
	require.Equal(t, 0, idx)

	// Sanity: the full key the search path reconstructs equals the stored key,
	// confirming the full-read path (not a truncated guess) drives the decision.
	off := int(pg.data[pg.cellPointerOffset()])<<8 | int(pg.data[pg.cellPointerOffset()+1])
	full, ferr := leafFullKey(pg.data, off, usableSize, p, 0, nil)
	require.NoError(t, ferr)
	require.Equal(t, storedKey, full)
	require.Equal(t, 1, bytes.Compare(full, searchKey), "stored key must order after its strict prefix")
}

// TestSearchLeafOverflow_ProbeNilInProduction documents that the test hook is
// inert unless a test installs it: searchLeafWithOverflow must produce correct
// results with searchLeafOverflowProbe == nil (the production configuration).
func TestSearchLeafOverflow_ProbeNilInProduction(t *testing.T) {
	require.Nil(t, searchLeafOverflowProbe, "probe must be nil by default (production behavior)")

	p := tempPagerWithPageSize(t, 512)
	pg, err := p.allocatePage()
	require.NoError(t, err)
	bt := &btree{pager: p, rootPage: pg.pgno, writable: true}
	usableSize := bt.usablePageSize()
	maxLocal := maxLocalPayload(usableSize)

	storedKey := bytes.Repeat([]byte("q"), maxLocal+80)
	require.NoError(t, bt.rebuildLeafPage(pg, []cellData{{key: storedKey, value: []byte("v")}}))

	// Exact match still found with the probe nil.
	idx, found, serr := searchLeafWithOverflow(pg, bytes.Clone(storedKey), usableSize, p, 0, nil)
	require.NoError(t, serr)
	require.True(t, found)
	require.Equal(t, 0, idx)
}
