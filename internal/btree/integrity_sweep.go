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

// VerifyIntegrity walks every page of the snapshot its read transaction
// holds (1..ReadTx.DatabaseSize) and verifies the per-page integrity tag. For checksum-mode DBs this re-hashes
// the trailer and compares it; for AEAD-mode DBs it attempts a decrypt
// (the AEAD tag IS the integrity tag).
//
// Read-only operation that opens its own internal read transaction.
// Mismatches are collected into Result.Errors, and so is a page that cannot
// be read (Kind IntegrityKindUnknown, the read's error in Inner); the sweep
// goes on to the next page. Where the snapshot's WAL commit recorded the
// page count (sweepPageCount), page 1 is also listed, Kind
// IntegrityKindUnknown, when its header is not valid or counts differently.
// Inner is then the header's own error: ErrHeaderCorrupt, or ErrNewerFormat,
// ErrOldFormat or ErrSQLiteFormat for a header of another format. A page 1
// that cannot be read whole is listed with the read's error instead, the
// error lockBtree returns before it looks at the header (btree.c:3288-3289);
// its header, read alone, is listed as well, with ErrHeaderCorrupt, only
// when it counts fewer pages than the commit.
//
// The function's error is a read transaction that cannot begin, a cancelled
// context, and, when no WAL commit gives the page count, a page-1 header
// that cannot be read or is not valid. A read begin at a snapshot whose
// page-1 counters it does not hold yet reads them, so a page-1 header that
// cannot be read is then the function's error and there is no report; at a
// snapshot whose counters it holds, the sweep lists page 1 and goes on. A
// sqlite_dbpage scan cannot begin in the first case either: every
// transaction begins by reading page 1 (btree.c:3288-3289), and the pager
// cache keeps it only while the snapshot is unchanged (pager.c:3261-3264).
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

	tx, err := db.BeginReadContext(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback()

	if err := ctx.Err(); err != nil {
		return res, err
	}
	total, raw1, page1Errs, err := db.sweepPageCount(tx)
	if err != nil {
		return res, err
	}
	res.Errors = page1Errs

	// A 64-bit counter: the largest page count is the largest uint32, and a
	// uint32 counter would wrap to 0 below it and never end the loop.
	maxFrame := tx.walMaxFrame
	for i := uint64(1); i <= uint64(total); i++ {
		pgno := uint32(i)
		if err := ctx.Err(); err != nil {
			return res, err
		}
		res.Pages++
		// Page 1 was read for the count; a read that failed is listed.
		raw := raw1
		if pgno != 1 {
			if raw, err = db.pager.readRawPage(pgno, maxFrame, tx.cache); err != nil {
				res.Errors = append(res.Errors, SweepError{PageNo: pgno, Inner: err})
			}
		}
		var e *SweepError
		switch {
		case raw == nil:
		case mode == IntegrityChecksum:
			e = verifyCksumPage(pgno, raw)
		case mode == IntegrityAEAD:
			e = verifyAEADPage(db.pager.codec, pgno, raw)
		}
		if e != nil {
			res.Errors = append(res.Errors, *e)
		}
		if mode == IntegrityChecksum && (raw == nil || e != nil) {
			// Fire the codec's OnError hook so subscribers see sweep
			// mismatches the same way they see read-path mismatches.
			if c := db.CksumCodec(); c != nil {
				c.fire(pgno, errCksumMismatch)
			}
		}
	}
	return res, nil
}

// sweepPageCount returns the number of pages VerifyIntegrity sweeps under
// tx, page 1's raw bytes (nil when the page could not be read) and the
// errors found on the way, all of page 1.
//
// The sweep covers the pages of the snapshot it reads, by the rule a read
// transaction counts them (ReadTx.DatabaseSize, lockBtree's): page 1's size
// checked against the pager's count for this snapshot. The pager's in-memory
// header is the writer's, refreshed only at open, at a write begin and by a
// commit: it lags a peer process's growth and runs ahead of every snapshot
// while a commit is under way.
//
// Page 1 is read raw, as every page of the sweep is; its header is stored
// outside the codec. Decoding the page would return a damaged page 1 as the
// sweep's error instead of listing it. When the page cannot be read whole,
// its header is read on its own, so that a fault past the header costs the
// report page 1 and not every other page.
//
// Being outside the codec, the header's page count has no tag, and one
// damaged downwards with the change counters intact is a well-formed header:
// lockBtree accepts a size below the pager's count (btree.c:3294-3298,
// 3401-3408), since a file may hold pages past the database's end. Where the
// pager's count is the size a WAL commit of this snapshot recorded
// (sqlite3WalDbsize, wal.c:3669-3674), it is a second count of the same
// database, written by the commit that wrote page 1's (pager.commit keeps
// the two equal). The sweep then covers the pages the commit recorded.
// Page 1 is listed with the read's error when it cannot be read whole, and
// otherwise with the header's own error when its header is not valid or
// counts differently (ErrHeaderCorrupt, or deserialize's error for a header
// of another format): the read is judged first, as lockBtree returns a
// failed read of page 1 before it checks the header (btree.c:3288-3289).
// A header read alone that counts fewer pages than the commit is listed as
// well. Where the pager's count is the file's length, a smaller count in
// page 1 stands, as it does for every reader, and a header that cannot be
// read or gives no valid count is the sweep's error: there is then no count
// to sweep to.
func (db *DB) sweepPageCount(tx *ReadTx) (total uint32, raw1 []byte, page1Errs []SweepError, err error) {
	p := db.pager
	bound := p.readerDbSizeBound(tx.cache)
	// The size a WAL commit recorded, or 0. In every mode a snapshot's
	// header carries a size only once a commit has recorded one, and a WAL
	// reset keeps it, as in SQLite (effectiveReaderDbSize).
	var commitN uint32
	if n := tx.walHdr.nPage; n != 0 && n == bound {
		commitN = n
	}

	var hdr [dbHeaderSize]byte
	raw1, readErr := p.readRawPage(1, tx.walMaxFrame, tx.cache)
	hdrErr := readErr
	if readErr == nil {
		copy(hdr[:], raw1)
	} else {
		raw1 = nil
		page1Errs = append(page1Errs, SweepError{PageNo: 1, Inner: readErr})
		hdrErr = p.readRawPage1Header(tx.walMaxFrame, tx.cache, hdr[:])
	}
	if hdrErr == nil {
		var h dbHeader
		if hdrErr = h.deserialize(hdr[:]); hdrErr == nil {
			total, hdrErr = lockBtreePageCount(&h, bound)
		}
	}
	switch {
	case commitN == 0:
		if hdrErr != nil {
			return 0, nil, nil, hdrErr
		}
	case hdrErr != nil:
		// An unreadable page 1 is listed already, with the read's error.
		if readErr == nil {
			page1Errs = append(page1Errs, SweepError{PageNo: 1, Inner: hdrErr})
		}
		total = commitN
	case total != commitN:
		page1Errs = append(page1Errs, SweepError{PageNo: 1, Inner: fmt.Errorf(
			"%w: header DatabaseSize=%d, the WAL commit of the snapshot recorded %d pages", ErrHeaderCorrupt, total, commitN)})
		total = commitN
	}
	return total, raw1, page1Errs, nil
}

// verifyCksumPage re-hashes the body of pgno's raw on-disk bytes and
// compares it against the trailer. Mirrors the codec's Encrypt/Decrypt range:
// pages 2+ hash [0..end-16], page 1 hashes [dbHeaderSize..end-16] because
// encryptPageWithCodec preserves the page-1 plaintext prefix.
func verifyCksumPage(pgno uint32, raw []byte) *SweepError {
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
func verifyAEADPage(codec Codec, pgno uint32, raw []byte) *SweepError {
	var s aeadScratch
	dst := make([]byte, len(raw))
	if _, derr := decryptPageWithCodec(codec, dst, raw, pgno, &s); derr != nil {
		return &SweepError{PageNo: pgno, Kind: IntegrityAEADAuthFail, Inner: derr}
	}
	return nil
}
