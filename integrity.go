package anystore

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/internal/btree"
)

// Page-level integrity is enabled automatically for non-encrypted databases:
// every page carries an XXH3-128 trailer (16 bytes) that is verified on read.
// Encrypted databases derive integrity from the cipher's AEAD authentication
// tag instead. There is no opt-out — the cost is <1% on writes and effectively
// zero on reads (see bench-integrity.txt).
//
// File state is authoritative on reopen: existing plain databases stay plain,
// existing checksum databases auto-install the codec regardless of caller
// configuration. Conceptually mirrors SQLite's cksumvfs extension
// (https://sqlite.org/cksumvfs.html), generalized to also surface AEAD
// authentication failures via VerifyIntegrity / IntegrityMode.

// IntegrityErrorKind discriminates per-page failure types.
type IntegrityErrorKind int

const (
	// IntegrityKindUnknown is reserved.
	IntegrityKindUnknown IntegrityErrorKind = IntegrityErrorKind(btree.IntegrityKindUnknown)
	// IntegrityChecksumMismatch indicates an XXH3-128 trailer mismatch
	// (cksum mode).
	IntegrityChecksumMismatch IntegrityErrorKind = IntegrityErrorKind(btree.IntegrityChecksumMismatch)
	// IntegrityAEADAuthFail indicates an AEAD auth-tag failure (AES-GCM
	// or ChaCha20-Poly1305 mode).
	IntegrityAEADAuthFail IntegrityErrorKind = IntegrityErrorKind(btree.IntegrityAEADAuthFail)
)

// IntegrityError describes a single per-page verification failure.
type IntegrityError struct {
	PageNo uint32
	Kind   IntegrityErrorKind
	Inner  error
}

// IntegrityMode is the codec mode of a DB.
type IntegrityMode int

const (
	IntegrityNone     IntegrityMode = IntegrityMode(btree.IntegrityNone)
	IntegrityChecksum IntegrityMode = IntegrityMode(btree.IntegrityChecksum)
	IntegrityAEAD     IntegrityMode = IntegrityMode(btree.IntegrityAEAD)
)

// IntegrityReport is the outcome of VerifyIntegrity.
type IntegrityReport struct {
	Mode   IntegrityMode
	Pages  int
	Errors []IntegrityError
}

// VerifyPageChecksum returns true iff the trailing 16 bytes of `page` are
// the XXH3-128 of page[:len(page)-16]. Returns false for invalid sizes.
// Public; mirrors SQLite's verify_checksum() SQL function. Computes over
// the full page; for the codec-equivalent (which excludes the page-1 DB
// header for page 1), use (DB).VerifyIntegrity instead.
func VerifyPageChecksum(page []byte) bool {
	return btree.VerifyPageChecksum(page)
}

// StampPageChecksumForTest writes a valid XXH3-128 trailer into the last
// 16 bytes of `page`. Test/migration helper; production code never calls
// this directly — codec.Encrypt does it during normal page writes.
func StampPageChecksumForTest(page []byte) {
	btree.StampPageChecksum(page)
}

// ErrAEADIntegrityVerifyMandatory is returned by SetVerifyOnRead(false) on
// AEAD-encrypted databases. Disabling AEAD verification would return
// attacker-controlled plaintext, defeating the cipher.
var ErrAEADIntegrityVerifyMandatory = fmt.Errorf("anystore: cannot disable verify-on-read for AEAD-encrypted databases")

// ErrIntegrityVerifyUnsupported is returned by SetVerifyOnRead on a plain
// database (no integrity mode installed).
var ErrIntegrityVerifyUnsupported = fmt.Errorf("anystore: SetVerifyOnRead requires checksum or encryption mode")

// VerifyIntegrity walks every page in the database and verifies its
// per-page integrity tag. For checksum-mode DBs this re-hashes the trailer;
// for AEAD-mode DBs it attempts decryption (the AEAD tag IS the integrity
// tag). Plain DBs return IntegrityNone with zero pages scanned.
//
// Equivalent to running
//
//	SELECT count(*), verify_checksum(data)
//	  FROM sqlite_dbpage
//	 GROUP BY 2;
//
// against a cksumvfs-protected SQLite database, and works on AEAD-encrypted
// databases too.
//
// Returns an error only on I/O failures or context cancellation. Per-page
// integrity mismatches are returned in Report.Errors.
func (db *db) VerifyIntegrity(ctx context.Context) (IntegrityReport, error) {
	res, err := db.btreeDB.VerifyIntegrity(ctx)
	if err != nil {
		return IntegrityReport{}, err
	}
	rep := IntegrityReport{Mode: IntegrityMode(res.Mode), Pages: res.Pages}
	for _, e := range res.Errors {
		rep.Errors = append(rep.Errors, IntegrityError{
			PageNo: e.PageNo,
			Kind:   IntegrityErrorKind(e.Kind),
			Inner:  e.Inner,
		})
	}
	return rep, nil
}

// IntegrityMode reports the codec currently installed on the DB.
func (db *db) IntegrityMode() IntegrityMode {
	return IntegrityMode(db.btreeDB.IntegrityMode())
}

// VerifyOnRead reports whether read-path verification is currently enabled.
// Always true for AEAD-encrypted DBs (AEAD verification cannot be disabled).
// Always true for plain DBs (no-op).
func (db *db) VerifyOnRead() bool {
	switch db.IntegrityMode() {
	case IntegrityChecksum:
		c := db.btreeDB.CksumCodec()
		return c == nil || c.VerifyOn()
	default:
		return true
	}
}

// SetVerifyOnRead toggles read-path verification at runtime. Mirrors
// SQLite's PRAGMA checksum_verification.
//
// Returns ErrAEADIntegrityVerifyMandatory if called with on=false on an
// AEAD-encrypted DB; returns ErrIntegrityVerifyUnsupported on plain DBs.
// Calls with on=true are accepted (no-op) in any mode.
func (db *db) SetVerifyOnRead(on bool) error {
	switch db.IntegrityMode() {
	case IntegrityChecksum:
		if c := db.btreeDB.CksumCodec(); c != nil {
			c.SetVerify(on)
			return nil
		}
		return ErrIntegrityVerifyUnsupported
	case IntegrityAEAD:
		if !on {
			return ErrAEADIntegrityVerifyMandatory
		}
		return nil
	default:
		if on {
			return nil // benign no-op
		}
		return ErrIntegrityVerifyUnsupported
	}
}
