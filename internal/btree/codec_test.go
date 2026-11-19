package btree

import (
	"bytes"
	"sync"
	"testing"
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
			if _, err := c.Decrypt(pt, ct, 3, &r); err != nil {
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

// A new pool is exhausted, so its first draw reads crypto/rand: the zero
// buffer behind it is never handed out as nonces.
func TestNoncePool_FirstDrawIsRandom(t *testing.T) {
	var s aeadScratch
	if err := s.drawNonce(s.nonce[:]); err != nil {
		t.Fatal(err)
	}
	if s.nonce == ([24]byte{}) {
		t.Fatalf("first nonce drawn is all zero")
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
	out, err := decryptWith(c, dst, src, 42, &aeadScratch{})
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
	p.installCodec(c)
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
	p.installCodec(nil)
	if p.codec != nil {
		t.Fatalf("nil install set codec")
	}
	if p.header.ReservedSpace != 0 {
		t.Fatalf("nil install changed ReservedSpace")
	}
}
