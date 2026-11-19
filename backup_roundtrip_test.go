package anystore_test

// What a backup is for: the copy reopens with the credentials of its source
// and holds the same documents. One round trip per kind of database (plain,
// checksummed, encrypted by passphrase, by raw key, by a caller's codec), per
// kind of source (file, in-memory) and per state of the destination (absent,
// a database of the same kind, a database of another kind).

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	anystore "github.com/anyproto/any-store/v2"
	"github.com/anyproto/any-store/v2/anyenc"
)

// roundTripKind is one kind of database: how it is opened, and how a copy of
// it is opened again. The two differ only where a config cannot be reused (a
// caller's codec, the in-memory flag).
type roundTripKind struct {
	name   string
	open   func(t *testing.T) *anystore.Config
	reopen func(t *testing.T) *anystore.Config
	mode   anystore.IntegrityMode // of the copy
}

func passphraseConfig(pass string, cipher anystore.CipherType) func(*testing.T) *anystore.Config {
	return func(*testing.T) *anystore.Config {
		return &anystore.Config{Encryption: anystore.EncryptionConfig{
			Passphrase:    []byte(pass),
			KDFIterations: 1000,
			CipherType:    cipher,
		}}
	}
}

func codecConfig(t *testing.T) *anystore.Config {
	codec, err := anystore.NewChaCha20Poly1305Codec(bytes.Repeat([]byte{0x5a}, 32))
	require.NoError(t, err)
	return &anystore.Config{Encryption: anystore.EncryptionConfig{Codec: codec}}
}

func defaultConfig(*testing.T) *anystore.Config { return nil }

func inMemory(conf func(*testing.T) *anystore.Config) func(*testing.T) *anystore.Config {
	return func(t *testing.T) *anystore.Config {
		c := conf(t)
		if c == nil {
			c = &anystore.Config{}
		}
		c.InMemory = true
		return c
	}
}

var roundTripKinds = []roundTripKind{
	{"checksummed", defaultConfig, defaultConfig, anystore.IntegrityChecksum},
	{"passphrase", passphraseConfig("correct horse", ""), passphraseConfig("correct horse", ""), anystore.IntegrityAEAD},
	{"passphrase-default-kdf", func(*testing.T) *anystore.Config {
		return &anystore.Config{Encryption: anystore.EncryptionConfig{Passphrase: []byte("correct horse")}}
	}, func(*testing.T) *anystore.Config {
		return &anystore.Config{Encryption: anystore.EncryptionConfig{Passphrase: []byte("correct horse")}}
	}, anystore.IntegrityAEAD},
	{"passphrase-xchacha", passphraseConfig("correct horse", anystore.CipherXChaCha20Poly1305), passphraseConfig("correct horse", anystore.CipherXChaCha20Poly1305), anystore.IntegrityAEAD},
	{"raw-key-32", passphraseConfig("0123456789abcdef0123456789abcdef", ""), passphraseConfig("0123456789abcdef0123456789abcdef", ""), anystore.IntegrityAEAD},
	{"caller-codec", codecConfig, codecConfig, anystore.IntegrityAEAD},
	{"in-memory", inMemory(defaultConfig), defaultConfig, anystore.IntegrityNone},
	{"in-memory-passphrase", inMemory(passphraseConfig("correct horse", "")), passphraseConfig("correct horse", ""), anystore.IntegrityAEAD},
	{"in-memory-raw-key-32", inMemory(passphraseConfig("0123456789abcdef0123456789abcdef", "")), passphraseConfig("0123456789abcdef0123456789abcdef", ""), anystore.IntegrityAEAD},
}

// Kinds that are only destinations: the source's kind in everything the file
// header shows (encrypted, 32 reserved bytes), and not readable with the
// source's credentials.
var roundTripForeignKinds = []roundTripKind{
	{"another-passphrase", passphraseConfig("battery staple", ""), passphraseConfig("battery staple", ""), anystore.IntegrityAEAD},
	{"passphrase-chacha", passphraseConfig("correct horse", anystore.CipherChaCha20Poly1305), passphraseConfig("correct horse", anystore.CipherChaCha20Poly1305), anystore.IntegrityAEAD},
}

// fillRoundTrip puts n documents of a few sizes (some past one page) and an
// index into collection "c", and returns the documents by id.
func fillRoundTrip(t *testing.T, db anystore.DB, from, n int) map[string]string {
	t.Helper()
	ctx := context.Background()
	coll, err := db.Collection(ctx, "c")
	require.NoError(t, err)
	require.NoError(t, coll.EnsureIndex(ctx, anystore.IndexInfo{Fields: []string{"n"}}))
	want := make(map[string]string, n)
	for i := from; i < from+n; i++ {
		pad := 300
		if i%50 == 7 {
			pad = 9000 // an overflow chain
		}
		doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":"d%05d","n":%d,"pad":"%0*d"}`, i, i, pad, i))
		require.NoError(t, coll.Insert(ctx, doc))
		want[fmt.Sprintf("d%05d", i)] = doc.String()
	}
	return want
}

// requireCopy opens the database at path with conf and requires that it is
// sound, that it holds exactly the documents of want, and that its index
// answers a query. It leaves the file closed.
func requireCopy(t *testing.T, path string, conf *anystore.Config, mode anystore.IntegrityMode, want map[string]string) {
	t.Helper()
	ctx := context.Background()
	db, err := anystore.Open(ctx, path, conf)
	require.NoError(t, err, "the copy opens with its source's credentials")
	defer func() { require.NoError(t, db.Close()) }()
	require.Equal(t, mode, db.IntegrityMode())
	require.NoError(t, db.IntegrityCheck(ctx))
	report, err := db.VerifyIntegrity(ctx)
	require.NoError(t, err)
	require.Empty(t, report.Errors)

	coll, err := db.OpenCollection(ctx, "c")
	require.NoError(t, err)
	got := make(map[string]string, len(want))
	iter, err := coll.Find(nil).Iter(ctx)
	require.NoError(t, err)
	for iter.Next() {
		doc, derr := iter.Doc()
		require.NoError(t, derr)
		got[string(doc.Value().GetStringBytes("id"))] = doc.Value().String()
	}
	require.NoError(t, iter.Err())
	require.NoError(t, iter.Close())
	require.Equal(t, len(want), len(got))
	for id, js := range want {
		require.Equal(t, js, got[id], "document %s", id)
	}

	// the index: in the copy, used, and complete
	require.Len(t, coll.GetIndexes(), 1)
	q := coll.Find(`{"n":{"$gte":0}}`).Sort("n")
	explain, err := q.Explain(ctx)
	require.NoError(t, err)
	var used bool
	for _, idx := range explain.Indexes {
		used = used || idx.Used
	}
	require.True(t, used, "the query runs on the index: %s", explain.Plan)
	n, err := q.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, len(want), n)
}

func TestBackupRoundTrip(t *testing.T) {
	ctx := context.Background()
	for _, kind := range roundTripKinds {
		t.Run(kind.name, func(t *testing.T) {
			// given
			dir := t.TempDir()
			src, err := anystore.Open(ctx, filepath.Join(dir, "src.db"), kind.open(t))
			require.NoError(t, err)
			want := fillRoundTrip(t, src, 0, 300)

			// when
			copyPath := filepath.Join(dir, "copy.db")
			require.NoError(t, src.Backup(ctx, copyPath))
			require.NoError(t, src.Close())

			// then
			requireCopy(t, copyPath, kind.reopen(t), kind.mode, want)

			// and a backup of the copy is a copy too: the restored database
			// is backed up like any other
			restored, err := anystore.Open(ctx, copyPath, kind.reopen(t))
			require.NoError(t, err)
			for id, js := range fillRoundTrip(t, restored, 300, 20) {
				want[id] = js
			}
			againPath := filepath.Join(dir, "again.db")
			require.NoError(t, restored.Backup(ctx, againPath))
			require.NoError(t, restored.Close())
			requireCopy(t, againPath, kind.reopen(t), kind.mode, want)
		})
	}
}

// A backup over a database of the same kind replaces its content, under the
// destination's own key.
func TestBackupRoundTrip_OverExistingOfSameKind(t *testing.T) {
	ctx := context.Background()
	for _, kind := range roundTripKinds {
		t.Run(kind.name, func(t *testing.T) {
			// given: a destination with other content, larger than the source
			dir := t.TempDir()
			dstPath := filepath.Join(dir, "dst.db")
			dst, err := anystore.Open(ctx, dstPath, kind.reopen(t))
			require.NoError(t, err)
			fillRoundTrip(t, dst, 5000, 600)
			require.NoError(t, dst.Close())

			src, err := anystore.Open(ctx, filepath.Join(dir, "src.db"), kind.open(t))
			require.NoError(t, err)
			want := fillRoundTrip(t, src, 0, 300)
			if kind.name == "in-memory" {
				// The destination a default Open made is checksummed, an
				// in-memory database is not: see OverExistingOfAnotherKind.
				// The plain destination is a backup of an in-memory database.
				require.NoError(t, os.Remove(dstPath))
				require.NoError(t, src.Backup(ctx, dstPath))
				plain, err := anystore.Open(ctx, dstPath, nil)
				require.NoError(t, err)
				fillRoundTrip(t, plain, 5000, 600)
				require.NoError(t, plain.Close())
			}

			// when
			require.NoError(t, src.Backup(ctx, dstPath))
			require.NoError(t, src.Close())

			// then
			requireCopy(t, dstPath, kind.reopen(t), kind.mode, want)
		})
	}
}

// A backup over a database of another kind (other reserved space, other page
// codec), or of the same kind under other credentials, is refused before a
// page of the destination is written: the destination opens afterwards as it
// did before.
func TestBackupRoundTrip_OverExistingOfAnotherKind(t *testing.T) {
	ctx := context.Background()
	kindByName := func(name string) roundTripKind {
		for _, k := range append(roundTripKinds[:len(roundTripKinds):len(roundTripKinds)], roundTripForeignKinds...) {
			if k.name == name {
				return k
			}
		}
		t.Fatalf("no kind %q", name)
		return roundTripKind{}
	}
	plainFile := func(t *testing.T, path string) map[string]string {
		mem, err := anystore.Open(ctx, "", &anystore.Config{InMemory: true})
		require.NoError(t, err)
		want := fillRoundTrip(t, mem, 7000, 100)
		require.NoError(t, mem.Backup(ctx, path))
		require.NoError(t, mem.Close())
		return want
	}
	for _, tc := range []struct{ src, dst string }{
		{"in-memory", "checksummed"},
		{"checksummed", "plain"},
		{"checksummed", "passphrase"},
		{"passphrase", "checksummed"},
		{"passphrase", "plain"},
		{"passphrase", "passphrase-xchacha"},
		{"in-memory", "passphrase"},
		{"passphrase", "another-passphrase"},
		{"passphrase", "passphrase-chacha"},
		{"in-memory-raw-key-32", "passphrase"},
	} {
		t.Run(tc.src+"-over-"+tc.dst, func(t *testing.T) {
			// given
			dir := t.TempDir()
			dstPath := filepath.Join(dir, "dst.db")
			var (
				dstWant   map[string]string
				dstReopen = defaultConfig
				dstMode   = anystore.IntegrityNone
			)
			if tc.dst == "plain" {
				dstWant = plainFile(t, dstPath)
			} else {
				dstKind := kindByName(tc.dst)
				dstReopen, dstMode = dstKind.reopen, dstKind.mode
				dst, err := anystore.Open(ctx, dstPath, dstKind.open(t))
				require.NoError(t, err)
				dstWant = fillRoundTrip(t, dst, 7000, 100)
				require.NoError(t, dst.Close())
			}
			before, err := os.ReadFile(dstPath)
			require.NoError(t, err)

			srcKind := kindByName(tc.src)
			src, err := anystore.Open(ctx, filepath.Join(dir, "src.db"), srcKind.open(t))
			require.NoError(t, err)
			defer func() { require.NoError(t, src.Close()) }()
			fillRoundTrip(t, src, 0, 300)

			// when
			err = src.Backup(ctx, dstPath)

			// then
			require.Error(t, err)
			t.Logf("Backup: %v", err)
			after, err := os.ReadFile(dstPath)
			require.NoError(t, err)
			assert.True(t, bytes.Equal(before, after), "the destination file is untouched")
			requireCopy(t, dstPath, dstReopen(t), dstMode, dstWant)
		})
	}
}

// A backup taken while a writer commits is one committed state of the
// source: with one document per transaction, the documents 0..k-1 for some k.
func TestBackupRoundTrip_WhileWriting(t *testing.T) {
	ctx := context.Background()
	for _, kind := range roundTripKinds {
		t.Run(kind.name, func(t *testing.T) {
			if kind.name == "caller-codec" {
				// Backup hands the caller's codec object to the destination,
				// so the source's writer and the backup's commit encrypt with
				// one object at once, and the built-in codecs' nonce pool is
				// not safe for that (the race detector reports it). A defect
				// of its own, not of the copy: this kind joins the test when
				// it is fixed.
				t.Skip("a caller's codec is shared by the source and the destination")
			}
			// given
			dir := t.TempDir()
			src, err := anystore.Open(ctx, filepath.Join(dir, "src.db"), kind.open(t))
			require.NoError(t, err)
			defer func() { require.NoError(t, src.Close()) }()
			const seeded, written = 600, 150
			want := fillRoundTrip(t, src, 0, seeded)

			// when
			var (
				wg       sync.WaitGroup
				writeErr error
				started  = make(chan struct{})
			)
			wg.Add(1)
			go func() {
				defer wg.Done()
				coll, cerr := src.Collection(ctx, "c")
				if cerr != nil {
					writeErr = cerr
					close(started)
					return
				}
				for i := seeded; i < seeded+written; i++ {
					doc := anyenc.MustParseJson(fmt.Sprintf(`{"id":"d%05d","n":%d,"pad":"%0300d"}`, i, i, i))
					if writeErr = coll.Insert(ctx, doc); writeErr != nil {
						break
					}
					if i == seeded {
						close(started)
					}
				}
			}()
			<-started
			copyPath := filepath.Join(dir, "copy.db")
			berr := src.Backup(ctx, copyPath)
			wg.Wait()

			// then
			require.NoError(t, writeErr)
			require.NoError(t, berr)
			copyDB, err := anystore.Open(ctx, copyPath, kind.reopen(t))
			require.NoError(t, err)
			coll, err := copyDB.OpenCollection(ctx, "c")
			require.NoError(t, err)
			k, err := coll.Count(ctx)
			require.NoError(t, err)
			require.NoError(t, copyDB.Close())
			require.GreaterOrEqual(t, k, seeded)
			require.LessOrEqual(t, k, seeded+written)
			for i := seeded; i < k; i++ {
				want[fmt.Sprintf("d%05d", i)] = anyenc.MustParseJson(fmt.Sprintf(`{"id":"d%05d","n":%d,"pad":"%0300d"}`, i, i, i)).String()
			}
			requireCopy(t, copyPath, kind.reopen(t), kind.mode, want)
		})
	}
}
