package anystore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/anyproto/any-store/anyenc"
)

func TestVerifyPageChecksum_GoodPage(t *testing.T) {
	page := make([]byte, 4096)
	for i := range page[:4080] {
		page[i] = byte(i)
	}
	StampPageChecksumForTest(page)
	if !VerifyPageChecksum(page) {
		t.Fatal("good page reported as bad")
	}
}

func TestVerifyPageChecksum_BadPage(t *testing.T) {
	page := make([]byte, 4096)
	StampPageChecksumForTest(page)
	page[100] ^= 0xff
	if VerifyPageChecksum(page) {
		t.Fatal("corrupted page reported as good")
	}
}

func TestVerifyPageChecksum_RejectsBadSize(t *testing.T) {
	if VerifyPageChecksum(make([]byte, 100)) {
		t.Fatal("non-page-sized buffer should not pass")
	}
}

// writeAndCloseIntegrityDB creates a DB at path with the given config,
// writes a row, flushes, and closes. Returns the path. Used to set up
// fixture databases for corruption / sweep tests.
func writeAndCloseIntegrityDB(t *testing.T, path string, cfg *Config) {
	t.Helper()
	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	coll, err := db.CreateCollection(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":%d, "i":%d, "pad":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"}`, i, i))
		if err := coll.UpsertOne(context.Background(), doc); err != nil {
			t.Fatalf("upsert: %v", err)
		}
	}
	if err := db.Flush(context.Background(), 0, FlushModeCheckpointFull); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// TestIntegrity_Default_IsChecksum verifies that anystore.Open with a
// default config installs the cksum codec automatically (the user-level
// expectation: "checksums are on, you don't need to ask").
func TestIntegrity_Default_IsChecksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := &Config{}
	writeAndCloseIntegrityDB(t, path, cfg)

	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.IntegrityMode() != IntegrityChecksum {
		t.Fatalf("Mode = %v, want IntegrityChecksum (default)", db.IntegrityMode())
	}
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Pages == 0 {
		t.Fatal("Pages = 0")
	}
	if len(rep.Errors) != 0 {
		t.Fatalf("unexpected errors: %+v", rep.Errors)
	}
}

func TestIntegrity_AEAD_VerifyIntegrity_AllGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	writeAndCloseIntegrityDB(t, path, cfg)

	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != IntegrityAEAD {
		t.Fatalf("Mode = %v, want IntegrityAEAD", rep.Mode)
	}
	if len(rep.Errors) != 0 {
		t.Fatalf("unexpected errors: %+v", rep.Errors)
	}
}

func TestIntegrity_SetVerifyOnRead_Cksum(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	db, err := Open(context.Background(), filepath.Join(dir, "db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if !db.VerifyOnRead() {
		t.Fatal("default VerifyOnRead = false, want true")
	}
	if err := db.SetVerifyOnRead(false); err != nil {
		t.Fatalf("SetVerifyOnRead(false): %v", err)
	}
	if db.VerifyOnRead() {
		t.Fatal("after SetVerifyOnRead(false), still true")
	}
}

func TestIntegrity_SetVerifyOnRead_AEAD_Rejected(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	db, err := Open(context.Background(), filepath.Join(dir, "db"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	err = db.SetVerifyOnRead(false)
	if err == nil {
		t.Fatal("SetVerifyOnRead(false) on AEAD must error")
	}
	if err != ErrAEADIntegrityVerifyMandatory {
		t.Fatalf("want ErrAEADIntegrityVerifyMandatory, got %v", err)
	}
}

// TestIntegrity_VerifyIntegrity_DetectsCorruption_Cksum: the sweep is the
// public detection mechanism for cksum mode (no OnError config exposed at
// the anystore level — users either call VerifyIntegrity periodically or
// see ErrCodecTamper bubbling up from regular reads).
func TestIntegrity_VerifyIntegrity_DetectsCorruption_Cksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := &Config{}
	writeAndCloseIntegrityDB(t, path, cfg)

	pageSize := int64(4096)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[pageSize+200] ^= 0x01
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Errors) == 0 {
		t.Fatal("expected at least one cksum mismatch")
	}
	saw := false
	for _, e := range rep.Errors {
		if e.Kind == IntegrityChecksumMismatch && e.PageNo == 2 {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatalf("want IntegrityChecksumMismatch on page 2; got %+v", rep.Errors)
	}
}

func TestIntegrity_VerifyIntegrity_DetectsCorruption_AEAD(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	writeAndCloseIntegrityDB(t, path, cfg)

	pageSize := int64(4096)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[pageSize+200] ^= 0x01
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Errors) == 0 {
		t.Fatal("expected at least one AEAD auth fail")
	}
	saw := false
	for _, e := range rep.Errors {
		if e.Kind == IntegrityAEADAuthFail && e.PageNo == 2 {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatalf("want IntegrityAEADAuthFail on page 2; got %+v", rep.Errors)
	}
}
