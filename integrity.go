package anystore

import (
	"context"
	"fmt"

	"github.com/anyproto/any-store/internal/btree"
)

// IntegrityConfig configures page-level integrity protection.
//
// Two opt-in modes are supported, mutually exclusive with EncryptionConfig:
//   - Plain (zero value): no integrity protection.
//   - Checksum (PageChecksums=true): XXH3-128 trailer per page.
//
// When Encryption is enabled, page integrity is automatically provided by
// the cipher's AEAD authentication tag (cryptographically authenticated).
// The OnError callback below works in both checksum and encryption modes.
//
// Conceptually mirrors SQLite's cksumvfs extension
// (https://sqlite.org/cksumvfs.html), generalized to also surface AEAD
// authentication failures.
type IntegrityConfig struct {
	// PageChecksums enables the no-encryption checksum codec at Open
	// time. Once a database is created with PageChecksums=true,
	// subsequent opens must also set true (and vice versa). Mutually
	// exclusive with EncryptionConfig.
	PageChecksums bool

	// OnError is invoked from the read path on every per-page integrity
	// failure, regardless of which mode is active. Runs on the I/O
	// goroutine; must not block. Use a buffered channel + drainer if
	// you need retention.
	OnError func(IntegrityError)

	// DisableVerifyOnRead suppresses ErrCodecTamper on checksum
	// mismatches at read time. Honored ONLY in PageChecksums mode.
	// OnError still fires; the read just doesn't return an error.
	// In Encryption mode this field is ignored — disabling AEAD
	// verification would return attacker-controlled plaintext.
	// Mirror of PRAGMA checksum_verification = OFF.
	DisableVerifyOnRead bool
}

// Enabled reports whether this config will install a non-encryption codec.
func (c IntegrityConfig) Enabled() bool { return c.PageChecksums }

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
// 16 bytes of `page`. Test/migration helper; production code goes through
// Open with IntegrityConfig.PageChecksums = true.
func StampPageChecksumForTest(page []byte) {
	btree.StampPageChecksum(page)
}

// ErrAEADIntegrityVerifyMandatory is returned by SetVerifyOnRead(false) on
// AEAD-encrypted databases. Disabling AEAD verification would return
// attacker-controlled plaintext, defeating the cipher.
var ErrAEADIntegrityVerifyMandatory = fmt.Errorf("anystore: cannot disable verify-on-read for AEAD-encrypted databases")

// ErrIntegrityVerifyUnsupported is returned by SetVerifyOnRead on a plain
// database (no integrity mode installed).
var ErrIntegrityVerifyUnsupported = fmt.Errorf("anystore: SetVerifyOnRead requires Integrity.PageChecksums=true or Encryption.Passphrase")

// integrityErrorFromInner builds an IntegrityError from a btree-level
// callback inner error and the current IntegrityMode. Used as the bridge
// between btree's untyped callback signature and the public typed event.
func integrityErrorFromInner(mode IntegrityMode, pgno uint32, inner error) IntegrityError {
	kind := IntegrityKindUnknown
	switch mode {
	case IntegrityChecksum:
		kind = IntegrityChecksumMismatch
	case IntegrityAEAD:
		kind = IntegrityAEADAuthFail
	}
	return IntegrityError{PageNo: pgno, Kind: kind, Inner: inner}
}

// integrityModeFromConfig returns the mode the codec will install given
// the supplied config. Used at Open to size the kind discriminator on
// the OnError adapter without inspecting the codec.
func integrityModeFromConfig(cfg *Config) IntegrityMode {
	if cfg.Encryption.Enabled() {
		return IntegrityAEAD
	}
	if cfg.Integrity.PageChecksums {
		return IntegrityChecksum
	}
	return IntegrityNone
}

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
// against a cksumvfs-protected SQLite database, but works on AEAD-encrypted
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
