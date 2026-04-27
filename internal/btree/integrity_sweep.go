package btree

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/zeebo/xxh3"
)

// IntegrityErrorKind discriminates per-page failure modes returned by
// VerifyIntegrity.
type IntegrityErrorKind int

const (
	IntegrityKindUnknown      IntegrityErrorKind = 0
	IntegrityChecksumMismatch IntegrityErrorKind = 1 // cksum codec
	IntegrityAEADAuthFail     IntegrityErrorKind = 2 // AES-GCM / ChaCha-Poly1305
)

// IntegrityMode is the codec mode currently installed on a DB.
type IntegrityMode int

const (
	IntegrityNone     IntegrityMode = 0
	IntegrityChecksum IntegrityMode = 1
	IntegrityAEAD     IntegrityMode = 2
)

// SweepError describes a single page that failed integrity verification.
type SweepError struct {
	PageNo uint32
	Kind   IntegrityErrorKind
	Inner  error
}

// SweepResult is the outcome of VerifyIntegrity.
type SweepResult struct {
	Mode   IntegrityMode
	Pages  int
	Errors []SweepError
}

// IntegrityMode returns the codec mode currently installed on the DB.
// Plain DBs (no codec) report IntegrityNone.
func (db *DB) IntegrityMode() IntegrityMode {
	if db.pager.codec == nil {
		return IntegrityNone
	}
	if _, ok := db.pager.codec.(*cksumCodec); ok {
		return IntegrityChecksum
	}
	return IntegrityAEAD
}

// CksumCodec returns the active *cksumCodec if one is installed, else nil.
// Used by anystore wrappers to expose the runtime PRAGMA-equivalent toggle
// (SetVerifyOnRead). No-op for AEAD codecs.
func (db *DB) CksumCodec() *cksumCodec {
	if c, ok := db.pager.codec.(*cksumCodec); ok {
		return c
	}
	return nil
}

// VerifyIntegrity walks every page in the database (1..DatabaseSize) and
// verifies the per-page integrity tag. For checksum-mode DBs this re-hashes
// the trailer and compares it; for AEAD-mode DBs it attempts a decrypt
// (the AEAD tag IS the integrity tag).
//
// Read-only operation that opens its own internal read transaction.
// Mismatches are collected into Result.Errors; non-integrity errors
// (file I/O, cancelled context) are returned as the function's error.
//
// On checksum DBs the sweep temporarily clears the verify-on-read flag so
// the read path doesn't abort on the first mismatch — the original flag
// is restored on return. On AEAD DBs there is no toggle (disabling AEAD
// verification would be a vulnerability); per-page Decrypt errors are
// caught and routed into the result instead of bubbling up.
//
// Returns IntegrityNone with zero pages scanned for plain DBs.
func (db *DB) VerifyIntegrity(ctx context.Context) (SweepResult, error) {
	mode := db.IntegrityMode()
	res := SweepResult{Mode: mode}
	if mode == IntegrityNone {
		return res, nil
	}

	if c := db.CksumCodec(); c != nil {
		prev := c.VerifyOn()
		c.SetVerify(false)
		defer c.SetVerify(prev)
	}

	tx, err := db.BeginRead()
	if err != nil {
		return res, err
	}
	defer tx.Rollback()

	total := db.pager.header.DatabaseSize
	maxFrame := tx.walMaxFrame
	for pgno := uint32(1); pgno <= total; pgno++ {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		switch mode {
		case IntegrityChecksum:
			if e := verifyCksumPage(db.pager, pgno, maxFrame); e != nil {
				res.Errors = append(res.Errors, *e)
			}
		case IntegrityAEAD:
			if e := verifyAEADPage(db.pager, pgno, maxFrame); e != nil {
				res.Errors = append(res.Errors, *e)
			}
		}
		res.Pages++
	}
	return res, nil
}

// verifyCksumPage reads pgno's raw on-disk bytes and re-hashes the body to
// compare against the trailer. Mirrors the codec's Encrypt/Decrypt range:
// pages 2+ hash [0..end-16], page 1 hashes [dbHeaderSize..end-16] because
// encryptPageWithCodec preserves the page-1 plaintext prefix.
func verifyCksumPage(p *pager, pgno, walMaxFrame uint32) *SweepError {
	raw, err := p.readRawPage(pgno, walMaxFrame)
	if err != nil {
		return &SweepError{PageNo: pgno, Inner: err}
	}
	start := 0
	if pgno == 1 {
		start = dbHeaderSize
	}
	bodyEnd := len(raw) - cksumOverhead
	if bodyEnd <= start {
		return &SweepError{PageNo: pgno, Kind: IntegrityChecksumMismatch, Inner: fmt.Errorf("page %d too small for trailer", pgno)}
	}
	want := xxh3.Hash128(raw[start:bodyEnd])
	var got xxh3.Uint128
	got.Lo = binary.LittleEndian.Uint64(raw[bodyEnd : bodyEnd+8])
	got.Hi = binary.LittleEndian.Uint64(raw[bodyEnd+8 : bodyEnd+16])
	if want != got {
		return &SweepError{PageNo: pgno, Kind: IntegrityChecksumMismatch}
	}
	return nil
}

// verifyAEADPage attempts a full decrypt of pgno's raw bytes through the
// installed codec. AEAD's auth tag is the integrity check — a successful
// Decrypt means the page is authentic. The plaintext output is discarded.
func verifyAEADPage(p *pager, pgno, walMaxFrame uint32) *SweepError {
	raw, err := p.readRawPage(pgno, walMaxFrame)
	if err != nil {
		return &SweepError{PageNo: pgno, Inner: err}
	}
	var s aeadScratch
	dst := make([]byte, len(raw))
	if _, derr := decryptPageWithCodec(p.codec, dst, raw, pgno, &s); derr != nil {
		return &SweepError{PageNo: pgno, Kind: IntegrityAEADAuthFail, Inner: derr}
	}
	return nil
}
