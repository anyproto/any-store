# Upstream sync state

Last verified upstream versions whose code/behavior the btree port mirrors.
Bump this file when re-syncing against a newer upstream.

## SQLite

- **Version:** 3.53.0
- **Source:** https://sqlite.org/src/
- **Tracks:** core pager, WAL format, B-tree balance/split, integrity-check,
  SHM index, busy/recovery semantics, and the cksumvfs algorithm shape (we
  diverge on hash algorithm — see `specs/integrity.md`).

Most ported sites in this package cite SQLite source filenames inline
(e.g. `wal.c:3022-3056`, `pager.c:5507`). Ported test files cite their origin
the same way, rooted at the SQLite source tree (`test/wal.test`,
`src/backup.c`) — never at a local checkout path, so the citation means the
same thing on every machine. When updating to a newer SQLite release, grep for
those references and re-validate behavior.

## SQLCipher

- **Version:** v4.14.0
- **Commit:** 778ab890cfc30c3631212dcceb0295498abdcd3e
- **Date:** 2026-03-13
- **Based on SQLite:** 3.51.3 (per upstream changelog at the pinned commit)
- **Source:** https://github.com/sqlcipher/sqlcipher
- **Tracks:** AES-256-GCM page-encryption codec layout (per-page nonce + tag
  + padding to 16-byte alignment), reserve-bytes convention, KDF (PBKDF2-
  HMAC-SHA256) key derivation, and the page-1 plaintext-prefix rule used
  in `encryptPageWithCodec` / `decryptPageWithCodec`.
- **Note:** SQLCipher's own SQLite baseline (3.51.3) is older than the
  SQLite reference we pin above (3.53.0). The two upstreams are pinned
  separately because we don't ship SQLCipher's C code — we re-implement
  its codec shape in Go, and we track core SQLite changes ourselves.

## Re-sync workflow

1. Update the version + commit + date above for the upstream you're
   re-syncing against.
2. `diff` the relevant subsystem's upstream source against the prior
   pinned commit; for each meaningful change, evaluate whether the Go
   port needs a corresponding update.
3. When in doubt, search for the upstream filename:line reference in the
   Go code (`grep -rn "wal.c:3022"` etc.) — the cited line numbers shift
   with version, so update them too.
4. Bump the version in this file in the same commit as the porting work.
