package anystore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
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

// TestIntegrity_ContinueOnIntegrityError_Cksum verifies that the flag
// suppresses the read-path ErrPageIntegrity but still fires the callback.
// Models the forensic-dump use case: read past corruption, log every hit.
func TestIntegrity_ContinueOnIntegrityError_Cksum(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	writeAndCloseIntegrityDB(t, path, &Config{})

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
	cfg := &Config{
		ContinueOnIntegrityError: true,
		OnIntegrityError:         func(IntegrityError) { fired.Add(1) },
	}
	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatalf("Open with ContinueOnIntegrityError=true should succeed even with corrupt page: %v", err)
	}
	defer db.Close()

	// Sweep — reads pass through the codec without erroring; callback fires.
	rep, err := db.VerifyIntegrity(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Errors) == 0 {
		t.Fatal("expected at least one mismatch in report")
	}
	if fired.Load() == 0 {
		t.Fatal("OnIntegrityError did not fire")
	}
}

// TestIntegrity_OnIntegrityError_Cksum_FiresOnSweep verifies that a
// callback wired at Config.OnIntegrityError observes integrity failures
// discovered by VerifyIntegrity. Mirrors the production wiring path:
// callers configure the hook at Open time, never afterward.
func TestIntegrity_OnIntegrityError_Cksum_FiresOnSweep(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	writeAndCloseIntegrityDB(t, path, &Config{})

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
	var lastPgno atomic.Uint32
	var lastKind atomic.Int32
	cfg := &Config{
		OnIntegrityError: func(e IntegrityError) {
			fired.Add(1)
			lastPgno.Store(e.PageNo)
			lastKind.Store(int32(e.Kind))
		},
	}
	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.VerifyIntegrity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fired.Load() == 0 {
		t.Fatal("OnIntegrityError did not fire on cksum mismatch")
	}
	if lastPgno.Load() != 2 {
		t.Fatalf("lastPgno = %d, want 2", lastPgno.Load())
	}
	if IntegrityErrorKind(lastKind.Load()) != IntegrityChecksumMismatch {
		t.Fatalf("lastKind = %v, want IntegrityChecksumMismatch", lastKind.Load())
	}
}

// TestIntegrity_OnIntegrityError_AEAD_FiresOnSweep verifies the same wiring
// works for AEAD-encrypted databases — kind discriminator is set correctly
// based on config.
func TestIntegrity_OnIntegrityError_AEAD_FiresOnSweep(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "db")
	encCfg := &Config{}
	encCfg.Encryption.Passphrase = []byte("p")
	encCfg.Encryption.KDFIterations = 1000
	writeAndCloseIntegrityDB(t, path, encCfg)

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
	var lastKind atomic.Int32
	cfg := &Config{}
	cfg.Encryption.Passphrase = []byte("p")
	cfg.Encryption.KDFIterations = 1000
	cfg.OnIntegrityError = func(e IntegrityError) {
		fired.Add(1)
		lastKind.Store(int32(e.Kind))
	}
	db, err := Open(context.Background(), path, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.VerifyIntegrity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fired.Load() == 0 {
		t.Fatal("OnIntegrityError did not fire on AEAD failure")
	}
	if IntegrityErrorKind(lastKind.Load()) != IntegrityAEADAuthFail {
		t.Fatalf("lastKind = %v, want IntegrityAEADAuthFail", lastKind.Load())
	}
}

// TestIntegrity_VerifyIntegrity_DetectsCorruption_Cksum: the sweep is the
// public detection mechanism for cksum mode (no OnError config exposed at
// the anystore level — users either call VerifyIntegrity periodically or
// see ErrPageIntegrity bubbling up from regular reads).
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

// Regression test for the codec bypass in wal.writeFrames' in-tx
// frame-reuse branch. Building a multikey index past ~150K docs fills
// the writer cache enough that hot pages get re-spilled within one
// transaction. Before the fix the rewritten WAL frame held plaintext
// with the page's last 16 plaintext bytes as a junk trailer, and the
// next read surfaced as `decrypt page N: encryption: page
// authentication failed`. See docs/btree/mappings/sqlcipher_codec.json
// row "wal.c:4152-4156" for the SQLCipher reference and the Go fix.
func TestIntegrity_BulkMultikeyIndex_WALReuse(t *testing.T) {
	if testing.Short() {
		t.Skip("~2s; requires ~150K-doc multikey index build")
	}
	const n = 150_000
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "db")
	ctx := context.Background()
	db, err := Open(ctx, dbPath, &Config{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	coll, err := db.Collection(ctx, "golden")
	if err != nil {
		t.Fatalf("coll: %v", err)
	}
	arena := &anyenc.Arena{}
	const batch = 1000
	for i := 0; i < n; i += batch {
		end := i + batch
		if end > n {
			end = n
		}
		docs := make([]*anyenc.Value, 0, end-i)
		for j := i; j < end; j++ {
			doc := arena.NewObject()
			doc.Set("id", arena.NewNumberInt(j))
			tags := arena.NewArray()
			tags.SetArrayItem(0, arena.NewString(fmt.Sprintf("tag-%d", j%20)))
			tags.SetArrayItem(1, arena.NewString(fmt.Sprintf("cat-%d", j%10)))
			tags.SetArrayItem(2, arena.NewString(fmt.Sprintf("grp-%d", j%5)))
			doc.Set("tags", tags)
			docs = append(docs, doc)
		}
		if err := coll.Insert(ctx, docs...); err != nil {
			t.Fatalf("insert batch %d: %v", i, err)
		}
		arena.Reset()
	}
	if err := coll.EnsureIndex(ctx, IndexInfo{Fields: []string{"tags"}}); err != nil {
		t.Fatalf("ensure tags: %v", err)
	}
	// db.Collection on a new namespace walks the master btree from
	// page 1 to its child(ren) — that's the path that hits the
	// rewritten frame and surfaces the corruption.
	if _, err := db.Collection(ctx, "other"); err != nil {
		t.Fatalf("open other: %v", err)
	}
}
