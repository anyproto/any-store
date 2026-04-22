package btree

import (
	"encoding/binary"
	"errors"
	"testing"
)

// buildWalHeader builds a serialized WAL header with configurable fields.
// Callers override the defaults to craft malformed inputs.
func buildWalHeader(t *testing.T, version, pageSize uint32) []byte {
	t.Helper()
	buf := make([]byte, walHeaderSize)
	binary.BigEndian.PutUint32(buf[0:4], walMagic)
	binary.BigEndian.PutUint32(buf[4:8], version)
	binary.BigEndian.PutUint32(buf[8:12], pageSize)
	binary.BigEndian.PutUint32(buf[12:16], 0)           // checkpoint
	binary.BigEndian.PutUint32(buf[16:20], 0xdeadbeef)  // salt1
	binary.BigEndian.PutUint32(buf[20:24], 0xcafef00d)  // salt2
	c1, c2 := walChecksum(buf[0:24], 0, 0)
	binary.BigEndian.PutUint32(buf[24:28], c1)
	binary.BigEndian.PutUint32(buf[28:32], c2)
	return buf
}

// TestWalHeaderDeserialize_GoodHeader sanity-checks the helper — a
// well-formed header of the current version and default page size must
// deserialize cleanly.
func TestWalHeaderDeserialize_GoodHeader(t *testing.T) {
	buf := buildWalHeader(t, walVersion, DefaultPageSize)
	var h walHeader
	if err := h.deserialize(buf); err != nil {
		t.Fatalf("good header rejected: %v", err)
	}
	if h.version != walVersion || h.pageSize != DefaultPageSize {
		t.Fatalf("fields mis-parsed: version=%d pageSize=%d", h.version, h.pageSize)
	}
}

// TestWalHeaderDeserialize_RejectsBadVersion mirrors SQLite's
// walIndexRecover rejection at wal.c:1406-1410.
func TestWalHeaderDeserialize_RejectsBadVersion(t *testing.T) {
	buf := buildWalHeader(t, walVersion+1, DefaultPageSize)
	var h walHeader
	err := h.deserialize(buf)
	if !errors.Is(err, ErrWALCorrupt) {
		t.Fatalf("bad version should be ErrWALCorrupt, got %v", err)
	}
}

// TestWalHeaderDeserialize_RejectsBadPageSize mirrors SQLite's
// walIndexRecover rejection at wal.c:1414-1419 (non-power-of-2, too
// small, too large, zero).
func TestWalHeaderDeserialize_RejectsBadPageSize(t *testing.T) {
	cases := []struct {
		name string
		ps   uint32
	}{
		{"zero", 0},
		{"non-power-of-2", 4097},
		{"too small", MinPageSize / 2},
		{"too large", MaxPageSize * 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := buildWalHeader(t, walVersion, tc.ps)
			var h walHeader
			err := h.deserialize(buf)
			if !errors.Is(err, ErrWALCorrupt) {
				t.Fatalf("bad pageSize %d should be ErrWALCorrupt, got %v", tc.ps, err)
			}
		})
	}
}
