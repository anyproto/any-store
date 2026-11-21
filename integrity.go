package anystore

import (
	"context"

	"github.com/anyproto/any-store/v2/internal/btree"
)

// Page-level integrity is enabled automatically for non-encrypted databases:
// every page carries an XXH3-128 trailer (16 bytes) that is verified on read.
// Encrypted databases derive integrity from the cipher's AEAD authentication
// tag instead. There is no opt-out — the cost is <1% on writes and effectively
// zero on reads.
//
// File state is authoritative on reopen: existing plain databases stay plain,
// existing checksum databases auto-install the codec regardless of caller
// configuration. Conceptually mirrors SQLite's cksumvfs extension
// (https://sqlite.org/cksumvfs.html), generalized to also surface AEAD
// authentication failures via VerifyIntegrity / IntegrityMode.

// IntegrityErrorKind discriminates per-page failure types.
type IntegrityErrorKind int

const (
	// IntegrityKindUnknown: not a tag failure. The page could not be read
	// and so was not verified, or page 1's header is not valid or disagrees
	// with the committed page count; Inner says which (VerifyIntegrity only).
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

// VerifyIntegrity walks every page in the database and verifies its
// per-page integrity tag. For checksum-mode DBs this re-hashes the trailer;
// for AEAD-mode DBs it attempts decryption (the AEAD tag IS the integrity
// tag). Plain DBs and in-memory DBs (whose pages are stored with no tag,
// whatever Config.Encryption says) return IntegrityNone with zero pages
// scanned.
//
// The analogue, for a cksumvfs-protected SQLite database, of scanning
// every page with
//
//	SELECT count(*), verify_checksum(data)
//	  FROM sqlite_dbpage
//	 GROUP BY 2;
//
// with two differences: it works on AEAD-encrypted databases too, and it
// goes on after a damaged page. With checksum verification on, the SQLite
// query fails at the first damaged page it reads from the file; the sweep
// lists the page and continues.
//
// A page that fails verification is listed in Report.Errors, and so is a
// page that cannot be read (Kind IntegrityKindUnknown, the read's error in
// Inner). When the last commit in the write-ahead log recorded the page
// count, page 1 is also listed (Kind IntegrityKindUnknown) when its header
// is not valid or disagrees with that count. Inner then wraps
// ErrHeaderCorrupt, or, for the header of another format (an older or newer
// any-store format, or SQLite's), is an error that names the format and
// does not wrap ErrCorrupt. A page 1 that cannot be read whole is listed
// with the read's error instead, and its header, read alone, is listed as
// well only when it counts fewer pages than the commit. The returned error
// is a sweep that could not start (the read transaction), a cancelled
// context, and, when the log holds no commit to count the pages by, a
// page-1 header that cannot be read or is not valid.
//
// OnIntegrityError fires once for each page whose tag fails, not for a
// page that could not be read.
//
// Reads and writes that run beside the sweep are not affected by it: they
// verify the pages they read exactly as configured
// (ContinueOnIntegrityError), as at any other time.
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
