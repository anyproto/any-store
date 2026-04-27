package anystore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

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

func TestIntegrity_Cksum_VerifyIntegrity_AllGood(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := &Config{}
	cfg.Integrity.PageChecksums = true
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
	if rep.Mode != IntegrityChecksum {
		t.Fatalf("Mode = %v, want IntegrityChecksum", rep.Mode)
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

func TestIntegrity_Plain_VerifyIntegrity_NoMode(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := &Config{}
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
	if rep.Mode != IntegrityNone {
		t.Fatalf("Mode = %v, want IntegrityNone", rep.Mode)
	}
}

func TestIntegrity_SetVerifyOnRead_Cksum(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{}
	cfg.Integrity.PageChecksums = true
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

func TestIntegrity_OnError_Cksum_FiresOnRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	cfg := &Config{}
	cfg.Integrity.PageChecksums = true
	writeAndCloseIntegrityDB(t, path, cfg)

	// Corrupt page 2 on disk.
	pageSize := int64(4096)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[pageSize+200] ^= 0x01
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}

	var fired atomic.Uint32
	cfg2 := &Config{}
	cfg2.Integrity.PageChecksums = true
	cfg2.Integrity.OnError = func(IntegrityError) { fired.Add(1) }

	db, err := Open(context.Background(), path, cfg2)
	if err != nil {
		// Open may already trip the codec depending on what page-1 lookup
		// triggers; that's fine — the callback fired.
		if fired.Load() == 0 {
			t.Fatalf("Open failed without firing OnError: %v", err)
		}
		return
	}
	defer db.Close()

	// Sweep — fires OnError per-mismatch via the codec hook because the
	// codec read path is what verifies on-disk pages.
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Errors) == 0 {
		t.Fatal("expected at least one mismatch")
	}
	// OnError fires from cksumCodec.Decrypt at read time, not from the
	// sweep's direct trailer recomputation. Trigger a normal read of the
	// corrupted page so the codec hook fires.
	rtx, err := db.ReadTx(context.Background())
	if err != nil {
		// rolling forward to a read may itself trip the codec; if it did,
		// the callback fired.
		if fired.Load() > 0 {
			return
		}
		t.Fatalf("ReadTx: %v", err)
	}
	_ = rtx.Commit()
	// Some implementations only fire on actual data reads; do a brief
	// sleep to let any pending callbacks settle (callbacks are sync
	// in our impl, so this is just defensive).
	time.Sleep(10 * time.Millisecond)
	if fired.Load() == 0 {
		t.Fatal("OnError did not fire for cksum mismatch")
	}
}

func TestIntegrity_OnError_AEAD_FiresOnRead(t *testing.T) {
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

	var fired atomic.Uint32
	cfg2 := &Config{}
	cfg2.Encryption.Passphrase = []byte("p")
	cfg2.Encryption.KDFIterations = 1000
	cfg2.Integrity.OnError = func(IntegrityError) { fired.Add(1) }

	db, err := Open(context.Background(), path, cfg2)
	if err != nil {
		if fired.Load() == 0 {
			t.Fatalf("Open failed without firing OnError: %v", err)
		}
		return
	}
	defer db.Close()

	// VerifyIntegrity on AEAD goes through decryptPageWithCodec which
	// fires OnError before returning ErrCodecTamper.
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Errors) == 0 {
		t.Fatal("expected at least one AEAD auth fail")
	}
	if fired.Load() == 0 {
		t.Fatal("OnError did not fire for AEAD failure")
	}
}
