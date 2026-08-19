# Mappings

Cross-references between any-store's Go btree port and upstream SQLite C, used to track porting coverage and intentional drift.

For prose-level drift analysis (cell formats, page layouts, missing features, architectural differences), see [`internal/btree/NOTES.md`](../../internal/btree/NOTES.md). The mapping files here track function-level coverage; NOTES.md explains *why* the divergences exist.

## Files

Hand-edited (tracked):

- **`go_to_sqlite.json`** — forward map, the source of truth. Each Go function gets a list of SQLite cites + a free-form note for drift.
- **`sqlite_skip.json`** — grouped rationale for SQLite functions deliberately not ported (deprecated features, divergent designs, inlined helpers, etc.).
- **`sqlcipher_codec.json`** — block-level map of every SQLCipher `#if defined(SQLITE_HAS_CODEC)` patch in `../sqlcipher/src` to its any-store Go counterpart. Use when adding/auditing codec hooks; an empty `code` list means the hook is intentionally omitted, with the reason in the row's `note`. Sites without a Go counterpart land here as a deliberate-drift record so a future port doesn't silently re-introduce them. Origin: this file was created after the 2026-04-28 incident where the SQLCipher reuse-path codec patch (`wal.c:4152-4156`) had no Go mirror, producing `decrypt page 2: encryption: page authentication failed` at multikey-index scale.

Generated (`*.gen.json`, gitignored — never edit):

- **`any_store_funcs.gen.json`** — Go func list scanned from `internal/btree/`.
- **`sqlite_funcs.gen.json`** — SQLite func allowlist scanned from `../sqlitec/src/`.
- **`sqlite_to_go.gen.json`** — reverse map (SQLite → Go citers), derived from `go_to_sqlite.json`.
- **`sqlite_funcs_unmapped.gen.json`** — SQLite funcs with no Go citer; cross-reference against `sqlite_skip.json` to triage.
- **`sqlcipher_codec_blocks.gen.json`** — every `#if SQLITE_HAS_CODEC` block scanned from `../sqlcipher/src/`. Each record carries `{file, start, end, context}`, where `<file>:<start>-<end>` is the row key in `sqlcipher_codec.json` and `context` is the enclosing function (or `(file scope)`). Cross-reference against `sqlcipher_codec.json` via the `+ codec` / `- codec` buckets in `mappings_diff` to triage.

## Regenerate

```sh
go run ./docs/btree/mappings/scripts/extract_funcs          # rescan Go + C func names
go run ./docs/btree/mappings/scripts/extract_codec_blocks   # rescan SQLCipher codec blocks
go run ./docs/btree/mappings/scripts/mappings_diff          # triage report (optional)
go run ./docs/btree/mappings/scripts/build_mappings         # rebuild reverse map + unmapped list
```

`mappings_diff` reports six buckets — new/orphan Go funcs (vs `go_to_sqlite.json`), new/orphan SQLite funcs (vs cites + `sqlite_skip.json`), and new/orphan SQLCipher codec blocks (vs `sqlcipher_codec.json`) — plus a count of untriaged stub rows. Always exits 0 (triage, not gating).

`build_mappings` reformats `go_to_sqlite.json` in place (deterministic key order, sorted cites) and fails if any cite isn't in the SQLite allowlist or any Go func is missing a row.

`extract_codec_blocks` accepts a `-skip` flag (comma-separated basename globs) for files outside the btree port's scope. The default skip list excludes `crypto_*.c`, `sqlcipher.{c,h}`, `tclsqlite.c`, and `test*.c`; pass `-skip ""` to scan everything. Scan extensions are `.c`, `.h`, `.h.in`. Conditionals are matched on any `#if[def]` whose body mentions `SQLITE_HAS_CODEC` as a token, so compound expressions (`#if !defined(...) || defined(SQLITE_HAS_CODEC)`) are not silently dropped.

`sqlcipher.{c,h}` are excluded by design: those files are SQLCipher's own crypto/KDF/HMAC implementation (~3.9k lines, all under one file-scope `#ifdef SQLITE_HAS_CODEC`), not hooks into stock SQLite. Any-store's moral equivalent is the `Codec` interface in `internal/btree/codec.go` plus the bundled `codec_aes.go` / `codec_chacha.go` / `codec_kdf.go` / `codec_cksum.go` implementations. Treat `sqlcipher.c` as "replace, don't port"; this map only covers the integration points.

Limitation: this map only sees blocks SQLCipher delineates with a codec-conditional `#if`. Silent edits to stock SQLite source not wrapped in a codec guard would not appear; auditing those requires diffing `../sqlcipher/src` against `../sqlitec/src` on each upstream sync (not currently automated).
