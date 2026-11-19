package anystore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
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

	// Sweep — lists the mismatch; callback fires.
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

// sweepDamagedDoc is the document damagedSweepDB damages.
const sweepDamagedDoc = 150

func sweepDocID(i int) string { return fmt.Sprintf("d%04d", i) }

// sweepDocPad is document i's 300-byte field. No other document holds its
// text, so the test finds the document's page by it.
func sweepDocPad(i int) string {
	return strings.Repeat(fmt.Sprintf("<pad-%04d>", i), 30)
}

// damagedSweepDB writes a checksum database of 400 documents, about ten to
// a page, closes it and changes one byte of document sweepDamagedDoc in the
// file, leaving the page's checksum as it was. It returns the path and the
// damaged page.
func damagedSweepDB(t *testing.T) (path string, pgno uint32) {
	t.Helper()
	ctx := context.Background()
	path = filepath.Join(t.TempDir(), "db")
	db, err := Open(ctx, path, &Config{DisableCompression: true})
	require.NoError(t, err)
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 400; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s","pad":"%s"}`, sweepDocID(i), sweepDocPad(i)))))
	}
	require.NoError(t, db.Close())

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	needle := []byte(sweepDocPad(sweepDamagedDoc)[:40])
	at := bytes.Index(data, needle)
	require.Greater(t, at, 0, "setup: the document is in the database file")
	at += 15
	data[at] = '!'
	require.NoError(t, os.WriteFile(path, data, 0o644))
	return path, uint32(at/4096) + 1
}

// TestIntegrity_SweepKeepsVerificationOn: while VerifyIntegrity runs, every
// other read and write of the handle still verifies page checksums. A read
// of a damaged page fails, and so does a write to it: a write that read the
// page unverified would commit it with a fresh checksum over the damage,
// and no later sweep or read would find it.
func TestIntegrity_SweepKeepsVerificationOn(t *testing.T) {
	ctx := context.Background()
	path, damaged := damagedSweepDB(t)

	// The sweep is parked in the callback it makes for the damaged page.
	// The callback must not block (Config.OnIntegrityError); the test blocks
	// it only to hold the sweep open.
	var armed atomic.Bool
	var inSweep func()
	db, err := Open(ctx, path, &Config{OnIntegrityError: func(IntegrityError) {
		if armed.CompareAndSwap(true, false) {
			inSweep()
		}
	}})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)

	var readErr error
	var writeErrs [3]error
	inSweep = func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, readErr = coll.FindId(ctx, sweepDocID(sweepDamagedDoc))
			// The damaged document and its two neighbours in key order:
			// one of the two at least shares its leaf.
			for i := range writeErrs {
				_, writeErrs[i] = coll.UpdateId(ctx, sweepDocID(sweepDamagedDoc-1+i), query.MustParseModifier(`{"$set":{"x":1}}`))
			}
		}()
		<-done
	}
	armed.Store(true)
	rep, err := db.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.False(t, armed.Load(), "setup: the sweep reached the damaged page")
	require.Equal(t, []IntegrityError{{PageNo: damaged, Kind: IntegrityChecksumMismatch}}, rep.Errors)

	require.ErrorIs(t, readErr, ErrPageIntegrity, "a read of the damaged page during the sweep")
	require.ErrorIs(t, writeErrs[1], ErrPageIntegrity, "an update of the damaged document during the sweep")
	if writeErrs[0] == nil && writeErrs[2] == nil {
		t.Fatal("an update of a neighbour on the damaged page succeeded during the sweep")
	}
	for _, i := range []int{0, 2} {
		if writeErrs[i] != nil {
			require.ErrorIs(t, writeErrs[i], ErrPageIntegrity)
		}
	}

	// The damage is still there to be found.
	_, err = coll.FindId(ctx, sweepDocID(sweepDamagedDoc))
	require.ErrorIs(t, err, ErrPageIntegrity)
	rep, err = db.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.Equal(t, []IntegrityError{{PageNo: damaged, Kind: IntegrityChecksumMismatch}}, rep.Errors)
}

// TestIntegrity_OverlappingSweepsKeepVerificationOn: two sweeps that overlap
// leave checksum verification on. Each is parked in its callback for the
// damaged page: B begins while A is under way and ends after A has ended.
func TestIntegrity_OverlappingSweepsKeepVerificationOn(t *testing.T) {
	ctx := context.Background()
	path, damaged := damagedSweepDB(t)

	parked := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var calls atomic.Int32
	db, err := Open(ctx, path, &Config{OnIntegrityError: func(IntegrityError) {
		if n := calls.Add(1); n <= 2 {
			close(parked[n-1])
			<-release[n-1]
		}
	}})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()
	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)

	type result struct {
		rep IntegrityReport
		err error
	}
	sweep := func() chan result {
		ch := make(chan result, 1)
		go func() {
			rep, err := db.VerifyIntegrity(ctx)
			ch <- result{rep, err}
		}()
		return ch
	}
	a := sweep()
	<-parked[0]
	b := sweep()
	<-parked[1]
	close(release[0])
	resA := <-a
	close(release[1])
	resB := <-b
	for _, res := range []result{resA, resB} {
		require.NoError(t, res.err)
		require.Equal(t, []IntegrityError{{PageNo: damaged, Kind: IntegrityChecksumMismatch}}, res.rep.Errors)
	}

	_, err = coll.FindId(ctx, sweepDocID(sweepDamagedDoc))
	require.ErrorIs(t, err, ErrPageIntegrity, "a read of the damaged page after both sweeps")
}

// TestIntegrity_InMemoryHasNoIntegrityMode: an in-memory database stores
// its pages as they are, with no codec, whatever Config.Encryption says
// (EncryptionConfig: "InMemory databases ignore Encryption"). There is no
// page tag to verify: the mode is IntegrityNone and a sweep scans nothing,
// where it used to try the cipher's tag on plain pages and list every one.
func TestIntegrity_InMemoryHasNoIntegrityMode(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		cfg  Config
	}{
		{name: "plain", cfg: Config{InMemory: true}},
		{name: "passphrase", cfg: Config{InMemory: true, Encryption: EncryptionConfig{Passphrase: []byte("p"), KDFIterations: 1000}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var fired atomic.Uint32
			cfg := tc.cfg
			cfg.OnIntegrityError = func(IntegrityError) { fired.Add(1) }
			db, err := Open(ctx, ":memory:", &cfg)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()
			coll, err := db.CreateCollection(ctx, "c")
			require.NoError(t, err)
			for i := 0; i < 200; i++ {
				require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s","pad":"%s"}`, sweepDocID(i), sweepDocPad(i)))))
			}
			require.NoError(t, db.Flush(ctx, 0, FlushModeCheckpointFull))

			require.Equal(t, IntegrityNone, db.IntegrityMode())
			rep, err := db.VerifyIntegrity(ctx)
			require.NoError(t, err)
			require.Equal(t, IntegrityNone, rep.Mode)
			require.Zero(t, rep.Pages)
			require.Empty(t, rep.Errors)
			require.Zero(t, fired.Load())
		})
	}
}
