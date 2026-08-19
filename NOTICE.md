# Third-Party Notices

Any Store is licensed under the MIT License (see [LICENSE.md](LICENSE.md)). Its
storage engine derives from two upstream projects, noted below.

Per-site provenance is tracked in
[`docs/btree/mappings/`](docs/btree/mappings/) and cited inline in
`internal/btree/`. Pinned upstream versions:
[`docs/btree/UPSTREAM.md`](docs/btree/UPSTREAM.md).

---

## SQLite — Public Domain

Any Store's btree, pager and WAL are a Go implementation aligned with SQLite's
design. The on-disk format is our own and is not SQLite-compatible.

SQLite's source is released into the public domain by its authors, so no
attribution is required. It is recorded here for transparency.

> The author disclaims copyright to this source code. In place of
> a legal notice, here is a blessing:
>
> * May you do good and not evil.
> * May you find forgiveness for yourself and forgive others.
> * May you share freely, never taking more than you give.

SQLite is a trademark of Hipp, Wyrick & Company, Inc. Any Store is not
affiliated with or endorsed by them.

---

## SQLCipher — BSD-3-Clause

Any Store's optional page-level encryption is an independent Go implementation
following design conventions established by SQLCipher. **No SQLCipher source
code is included, translated, or linked.**

**Reused** — conventions, re-implemented from scratch:

* reserved-tail page overhead, 16-byte aligned;
* the page-1 plaintext-prefix rule, keeping the DB header readable before key
  derivation;
* PBKDF2-HMAC-SHA256 key derivation with SQLCipher 4.x's default iteration
  count;
* placement of codec invocation points in the pager and WAL write paths — 5 of
  35 upstream codec hook sites; the other 30 are deliberately omitted.

**Not reused** — SQLCipher's cryptographic implementation (`sqlcipher.c`,
`crypto_*.c`) is not ported. Any Store uses Go standard-library and
`golang.org/x/crypto` primitives.

**Divergent** — Any Store uses AEAD ciphers (AES-256-GCM, ChaCha20-Poly1305,
XChaCha20-Poly1305) rather than SQLCipher's AES-256-CBC + HMAC-SHA512, with a
different per-page layout. Any Store cannot read or write SQLCipher databases.

SQLCipher is a trademark of Zetetic LLC. Any Store is not affiliated with,
endorsed by, or sponsored by Zetetic LLC; the name is used only to describe the
origin of these design conventions.

```
Copyright (c) 2025, ZETETIC LLC
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of the ZETETIC LLC nor the
      names of its contributors may be used to endorse or promote products
      derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY ZETETIC LLC ''AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL ZETETIC LLC BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
```

---

## Go dependencies

Every module in the shipped build graph is under MIT, BSD-2-Clause or
BSD-3-Clause. Test-only dependencies add ISC and Apache-2.0. No dependency,
shipped or test-only, is copyleft.

Regenerate the per-module list with `go-licenses`, or:

```sh
go list -deps -f '{{if .Module}}{{.Module.Path}}{{end}}' ./... | sort -u
```
