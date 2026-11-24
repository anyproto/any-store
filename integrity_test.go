package anystore

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/anyproto/any-store/v2/anyenc"
	"github.com/anyproto/any-store/v2/query"
)

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
// A sweep leaves the setting as configured: the first read of the damaged
// page comes after the sweep, so it goes to the file through the codec, and
// it still returns the damaged document and fires the callback.
func TestIntegrity_ContinueOnIntegrityError_Cksum(t *testing.T) {
	ctx := context.Background()
	path, damaged := damagedSweepDB(t)

	var fired atomic.Uint32
	var lastPgno atomic.Uint32
	cfg := &Config{
		ContinueOnIntegrityError: true,
		OnIntegrityError: func(e IntegrityError) {
			fired.Add(1)
			lastPgno.Store(e.PageNo)
		},
	}
	db, err := Open(ctx, path, cfg)
	require.NoError(t, err, "Open with ContinueOnIntegrityError=true succeeds with a corrupt page")
	defer func() { _ = db.Close() }()
	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)
	require.Zero(t, fired.Load(), "setup: nothing has read the damaged page")

	// The sweep lists the mismatch and fires the callback once for it.
	rep, err := db.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.Equal(t, []IntegrityError{{PageNo: damaged, Kind: IntegrityChecksumMismatch}}, rep.Errors)
	require.Equal(t, uint32(1), fired.Load(), "one callback for one damaged page")

	// The first read of the damaged page: no page cache holds it, so it is
	// read from the file and checked with the setting the sweep left.
	fired.Store(0)
	lastPgno.Store(0)
	doc, err := coll.FindId(ctx, sweepDocID(sweepDamagedDoc))
	require.NoError(t, err, "a forensic read after the sweep")
	pad := string(doc.Value().GetStringBytes("pad"))
	want := sweepDocPad(sweepDamagedDoc)
	require.Len(t, pad, len(want))
	require.NotEqual(t, want, pad, "the read returns the damaged bytes")
	require.NotZero(t, fired.Load(), "reading the damaged page fires the callback")
	require.Equal(t, damaged, lastPgno.Load())
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
	fired.Store(0)
	if _, err := db.VerifyIntegrity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := fired.Load(); n != 1 {
		t.Fatalf("OnIntegrityError fired %d times for one damaged page in one sweep, want 1", n)
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
	fired.Store(0)
	if _, err := db.VerifyIntegrity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := fired.Load(); n != 1 {
		t.Fatalf("OnIntegrityError fired %d times for one damaged page in one sweep, want 1", n)
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
	// A setup step that fails must not leave the database open.
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = db.Close()
		}
	})
	coll, err := db.CreateCollection(ctx, "c")
	require.NoError(t, err)
	for i := 0; i < 400; i++ {
		require.NoError(t, coll.Insert(ctx, anyenc.MustParseJson(fmt.Sprintf(`{"id":"%s","pad":"%s"}`, sweepDocID(i), sweepDocPad(i)))))
	}
	closed = true
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
	var released [2]sync.Once
	releaseSweep := func(i int) { released[i].Do(func() { close(release[i]) }) }
	var calls atomic.Int32
	db, err := Open(ctx, path, &Config{OnIntegrityError: func(IntegrityError) {
		if n := calls.Add(1); n <= 2 {
			close(parked[n-1])
			<-release[n-1]
		}
	}})
	require.NoError(t, err)
	// Whatever fails, the parked callbacks are released before the close,
	// which waits for the sweeps' read transactions.
	defer func() {
		releaseSweep(0)
		releaseSweep(1)
		_ = db.Close()
	}()
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
	// A sweep that returns before it reaches the damaged page (an error, a
	// callback that no longer fires) fails the test instead of hanging it.
	deadline := time.After(time.Minute)
	awaitParked := func(i int, done chan result) {
		t.Helper()
		select {
		case <-parked[i]:
		case res := <-done:
			t.Fatalf("sweep %d returned before its callback: err=%v, %d errors", i, res.err, len(res.rep.Errors))
		case <-deadline:
			t.Fatalf("sweep %d neither parked nor returned", i)
		}
	}
	awaitResult := func(i int, done chan result) result {
		t.Helper()
		select {
		case res := <-done:
			return res
		case <-deadline:
			t.Fatalf("sweep %d did not return", i)
			return result{}
		}
	}
	a := sweep()
	awaitParked(0, a)
	b := sweep()
	awaitParked(1, b)
	releaseSweep(0)
	resA := awaitResult(0, a)
	releaseSweep(1)
	resB := awaitResult(1, b)
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
// page tag to verify: the mode is IntegrityNone, and a sweep scans no page,
// lists none and fires no callback.
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
