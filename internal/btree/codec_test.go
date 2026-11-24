package btree

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCodec_SharedCodecConcurrentEncrypt drives one codec object from
// several goroutines at once, each with its own scratch, the way two
// databases opened with the same Options.Codec, or a database and its
// Backup destination, both write through it. Every page must carry a nonce that no
// other page has: a repeated nonce under one key breaks AES-GCM and
// ChaCha20-Poly1305 outright. Run it with -race too.
func TestCodec_SharedCodecConcurrentEncrypt(t *testing.T) {
	key := bytes.Repeat([]byte{7}, KeyLen)
	for _, tc := range []struct {
		name     string
		mk       func([]byte) (Codec, error)
		nonceLen int
		tagLen   int
	}{
		{"AES256GCM", NewAESCodec, aesNonceLen, aesTagLen},
		{"ChaCha20Poly1305", NewChaCha20Poly1305Codec, chacha20NonceLen, chachaTagLen},
		{"XChaCha20Poly1305", NewXChaCha20Poly1305Codec, xChaCha20NonceLen, chachaTagLen},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := tc.mk(key)
			if err != nil {
				t.Fatal(err)
			}
			const (
				workers  = 4
				pageSize = 4096
			)
			perWorker := 20000
			if testing.Short() {
				perWorker = 2000
			}
			// Layout [ct][tag][nonce][pad]: the nonce follows the tag.
			noncePos := pageSize - c.Overhead() + tc.tagLen

			got := make([][][24]byte, workers)
			errs := make([]error, workers)
			var wg sync.WaitGroup
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					var s aeadScratch // owned by this goroutine alone
					src := make([]byte, pageSize)
					dst := make([]byte, pageSize)
					out := make([][24]byte, 0, perWorker)
					for i := 0; i < perWorker; i++ {
						ct, err := c.Encrypt(dst, src, uint32(i+2), &s)
						if err != nil {
							errs[w] = err
							return
						}
						var n [24]byte
						copy(n[:], ct[noncePos:noncePos+tc.nonceLen])
						out = append(out, n)
					}
					got[w] = out
				}(w)
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					t.Fatalf("Encrypt: %v", err)
				}
			}

			seen := make(map[[24]byte]struct{}, workers*perWorker)
			dups := 0
			for _, ns := range got {
				for _, n := range ns {
					if _, ok := seen[n]; ok {
						dups++
						continue
					}
					seen[n] = struct{}{}
				}
			}
			if dups != 0 {
				t.Fatalf("%d of %d pages reuse a nonce under one key", dups, workers*perWorker)
			}
		})
	}
}

// The nonce pool belongs to the scratch, made on its first Encrypt: a
// scratch that only decrypts (every reader's) never allocates one, and the
// codec holds none for two scratches to share.
func TestCodec_NoncePoolOwnedByScratch(t *testing.T) {
	key := bytes.Repeat([]byte{9}, KeyLen)
	for _, mk := range []func([]byte) (Codec, error){NewAESCodec, NewChaCha20Poly1305Codec, NewXChaCha20Poly1305Codec} {
		c, err := mk(key)
		if err != nil {
			t.Fatal(err)
		}
		const pageSize = 4096
		src := bytes.Repeat([]byte{0xA5}, pageSize)
		ct := make([]byte, pageSize)
		pt := make([]byte, pageSize)

		var w, r aeadScratch
		if _, err := c.Encrypt(ct, src, 3, &w); err != nil {
			t.Fatal(err)
		}
		if w.nonces == nil {
			t.Fatalf("%T: Encrypt left the writer's scratch without a nonce pool", c)
		}
		for i := 0; i < 3; i++ {
			if _, err := c.Decrypt(pt, ct, 3, &r, nil); err != nil {
				t.Fatalf("%T: Decrypt: %v", c, err)
			}
		}
		if r.nonces != nil {
			t.Fatalf("%T: Decrypt allocated a nonce pool in a read-only scratch", c)
		}
		body := pageSize - c.Overhead()
		if !bytes.Equal(pt[:body], src[:body]) {
			t.Fatalf("%T: round-trip body mismatch", c)
		}

		// A second writer gets a pool of its own.
		var w2 aeadScratch
		if _, err := c.Encrypt(ct, src, 3, &w2); err != nil {
			t.Fatal(err)
		}
		if w2.nonces == nil || w2.nonces == w.nonces {
			t.Fatalf("%T: two scratches share one nonce pool", c)
		}
	}
}

// A new pool is exhausted, so its first draw reads crypto/rand: the whole
// first nonce is the start of a fresh fill, and no byte of the zero buffer
// behind it is handed out.
func TestNoncePool_FirstDrawIsRandom(t *testing.T) {
	var s aeadScratch
	if err := s.drawNonce(s.nonce[:]); err != nil {
		t.Fatal(err)
	}
	if s.nonce == ([24]byte{}) {
		t.Fatalf("first nonce drawn is all zero")
	}
	if s.nonces.off != len(s.nonce) || !bytes.Equal(s.nonce[:], s.nonces.buf[:len(s.nonce)]) {
		t.Fatalf("first nonce is not the start of a fresh fill (pool offset %d)", s.nonces.off)
	}
}

func TestCodec_NilIsZeroOverhead(t *testing.T) {
	var c Codec
	if got := overheadOrZero(c); got != 0 {
		t.Fatalf("overheadOrZero(nil) = %d, want 0", got)
	}
}

func TestCodec_EncryptPlainNil(t *testing.T) {
	var c Codec
	src := []byte("hello world")
	dst := make([]byte, 0, len(src))
	out, err := encryptWith(c, dst, src, 42, &aeadScratch{})
	if err != nil {
		t.Fatalf("encryptWith(nil) error: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("nil codec must pass-through: got %q want %q", out, src)
	}
}

func TestCodec_DecryptPlainNil(t *testing.T) {
	var c Codec
	src := []byte("hello world")
	dst := make([]byte, 0, len(src))
	out, err := decryptWith(c, dst, src, 42, &aeadScratch{}, nil)
	if err != nil {
		t.Fatalf("decryptWith(nil) error: %v", err)
	}
	if !bytes.Equal(out, src) {
		t.Fatalf("nil codec must pass-through")
	}
}

func TestPager_InstallCodec_ReservedSpace(t *testing.T) {
	p := &pager{pageSize: 4096}
	p.usableSize_ = int(p.pageSize) // pre-install: no reserve
	if p.usableSize() != 4096 {
		t.Fatalf("usableSize before install = %d, want 4096", p.usableSize())
	}

	key := make([]byte, 32)
	c, err := NewAESCodec(key)
	if err != nil {
		t.Fatal(err)
	}
	p.installCodec(c, nil)
	if p.header.ReservedSpace != uint8(c.Overhead()) {
		t.Fatalf("ReservedSpace = %d, want %d", p.header.ReservedSpace, c.Overhead())
	}
	if p.usableSize() != 4096-c.Overhead() {
		t.Fatalf("usableSize after install = %d, want %d", p.usableSize(), 4096-c.Overhead())
	}
	if p.codec != c {
		t.Fatalf("codec not stored")
	}
}

func TestPager_InstallCodec_NilIsNoop(t *testing.T) {
	p := &pager{pageSize: 4096}
	p.usableSize_ = int(p.pageSize)
	p.installCodec(nil, nil)
	if p.codec != nil {
		t.Fatalf("nil install set codec")
	}
	if p.header.ReservedSpace != 0 {
		t.Fatalf("nil install changed ReservedSpace")
	}
}

// One codec object in Options.Codec serving two databases whose writers
// commit at the same time, each from its own goroutine: every commit
// encrypts its frames through the shared codec, so a codec that kept state
// of its own across encrypts (a nonce source, a scratch buffer) would repeat
// a nonce under the key or mix two pages. Both files reopen with the codec,
// hold every row, and pass the integrity sweep. Run it with -race.
func TestCodec_SharedCodecTwoDatabasesWriting(t *testing.T) {
	codec, err := NewAESCodec(bytes.Repeat([]byte{0x3c}, KeyLen))
	require.NoError(t, err)
	dir := t.TempDir()
	paths := []string{filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db")}
	rows := 300
	if testing.Short() {
		rows = 60
	}
	key := func(j int) []byte { return fmt.Appendf(nil, "k%05d", j) }
	value := func(db, j int) []byte { return bytes.Repeat([]byte{byte(db*101 + j)}, 300) }

	dbs := make([]*DB, len(paths))
	for i, path := range paths {
		db, err := testOpen(t, path, Options{Codec: codec})
		require.NoError(t, err)
		dbs[i] = db
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		_, err = tx.CreateNamespace("data")
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}
	start := make(chan struct{})
	errs := make([]error, len(dbs))
	var wg sync.WaitGroup
	for i, db := range dbs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			// One commit per row: every commit encrypts its frames.
			for j := range rows {
				tx, err := db.BeginWrite()
				if err != nil {
					errs[i] = err
					return
				}
				ns, err := tx.GetNamespace("data")
				if err == nil {
					err = tx.Put(ns, key(j), value(i, j))
				}
				if err != nil {
					_ = tx.Rollback()
					errs[i] = err
					return
				}
				if err := tx.Commit(); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	for i, db := range dbs {
		require.NoError(t, errs[i])
		require.NoError(t, db.Close())
	}

	for i, path := range paths {
		db, err := testOpen(t, path, Options{Codec: codec})
		require.NoError(t, err)
		rtx, err := db.BeginRead()
		require.NoError(t, err)
		ns, err := rtx.GetNamespace("data")
		require.NoError(t, err)
		for j := range rows {
			got, err := rtx.Get(ns, key(j))
			require.NoError(t, err, "%s row %d", path, j)
			require.Equal(t, value(i, j), got, "%s row %d", path, j)
		}
		require.NoError(t, rtx.Rollback())
		rep, err := db.VerifyIntegrity(context.Background())
		require.NoError(t, err)
		require.Empty(t, rep.Errors, path)
		require.NoError(t, db.Close())
	}
}

// One codec object in Options.Codec serving three databases: A and B each
// with an OnIntegrityError of its own, C with none. A damaged page is
// reported to the callback of the database it belongs to and to no other,
// whatever order the databases were opened in: the callback is the
// database's (installCodec stores it on the pager), not the codec's, which
// is installed as given and so serves all three. The public API gives each
// database a codec of its own; Options.Codec is the one place one object can
// be shared.
func TestCodec_SharedCodecIntegrityCallbackPerDatabase(t *testing.T) {
	codec, err := NewAESCodec(bytes.Repeat([]byte{0x5a}, KeyLen))
	require.NoError(t, err)
	dir := t.TempDir()
	pathA, pathB, pathC := filepath.Join(dir, "a.db"), filepath.Join(dir, "b.db"), filepath.Join(dir, "c.db")
	for _, path := range []string{pathA, pathB, pathC} {
		db, err := testOpen(t, path, Options{Codec: codec})
		require.NoError(t, err)
		tx, err := db.BeginWrite()
		require.NoError(t, err)
		ns, err := tx.CreateNamespace("data")
		require.NoError(t, err)
		for i := range 200 {
			require.NoError(t, tx.Put(ns, fmt.Appendf(nil, "k%05d", i), bytes.Repeat([]byte{byte(i)}, 300)))
		}
		require.NoError(t, tx.Commit())
		require.NoError(t, db.Checkpoint(CheckpointTruncate))
		require.NoError(t, db.Close())
	}
	// Damage page 2 of A and of C; B stays intact.
	const pageSize, damaged = 4096, 2
	for _, path := range []string{pathA, pathC} {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		data[(damaged-1)*pageSize+200] ^= 0x01
		require.NoError(t, os.WriteFile(path, data, 0o644))
	}

	type hits struct {
		calls atomic.Int32
		page  atomic.Uint32
	}
	var hitA, hitB hits
	record := func(h *hits) func(uint32, error) {
		return func(pgno uint32, inner error) {
			h.page.Store(pgno)
			h.calls.Add(1)
		}
	}
	// Opened in the order A, B, C, all with the one codec: B's callback is
	// the last one given, and C gives none.
	dbA, err := testOpen(t, pathA, Options{Codec: codec, OnIntegrityError: record(&hitA)})
	require.NoError(t, err)
	defer dbA.Close()
	dbB, err := testOpen(t, pathB, Options{Codec: codec, OnIntegrityError: record(&hitB)})
	require.NoError(t, err)
	defer dbB.Close()
	dbC, err := testOpen(t, pathC, Options{Codec: codec})
	require.NoError(t, err)
	defer dbC.Close()

	// A's damaged page, read by the sweep, goes to A's callback only.
	rep, err := dbA.VerifyIntegrity(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, rep.Errors, "A's damaged page must fail verification")
	require.ErrorIs(t, rep.Errors[0].Inner, ErrPageIntegrity)
	assert.Positive(t, hitA.calls.Load(), "A's failure must reach A's callback")
	assert.Equal(t, uint32(damaged), hitA.page.Load())
	assert.Zero(t, hitB.calls.Load(), "A's failure must not reach B's callback")

	// C has no callback: its failures reach no one.
	before := hitA.calls.Load()
	rep, err = dbC.VerifyIntegrity(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, rep.Errors, "C's damaged page must fail verification")
	assert.Equal(t, before, hitA.calls.Load(), "C's failure must not reach A's callback")
	assert.Zero(t, hitB.calls.Load(), "C's failure must not reach B's callback")

	rep, err = dbB.VerifyIntegrity(context.Background())
	require.NoError(t, err)
	require.Empty(t, rep.Errors)
	assert.Zero(t, hitB.calls.Load())
}
