# Mappings

Cross-references between any-store's Go btree port and upstream SQLite C, used to track porting coverage and intentional drift.

For prose-level drift analysis (cell formats, page layouts, missing features, architectural differences), see [`internal/btree/NOTES.md`](../../internal/btree/NOTES.md). The mapping files here track function-level coverage; NOTES.md explains *why* the divergences exist.

## Files

Hand-edited (tracked):

- **`go_to_sqlite.json`** — forward map, the source of truth. Each Go function gets a list of SQLite cites + a free-form note for drift.
- **`sqlite_skip.json`** — grouped rationale for SQLite functions deliberately not ported (deprecated features, divergent designs, inlined helpers, etc.).

Generated (`*.gen.json`, gitignored — never edit):

- **`any_store_funcs.gen.json`** — Go func list scanned from `internal/btree/`.
- **`sqlite_funcs.gen.json`** — SQLite func allowlist scanned from `../sqlitec/src/`.
- **`sqlite_to_go.gen.json`** — reverse map (SQLite → Go citers), derived from `go_to_sqlite.json`.
- **`sqlite_funcs_unmapped.gen.json`** — SQLite funcs with no Go citer; cross-reference against `sqlite_skip.json` to triage.

## Regenerate

```sh
go run ./docs/btree/mappings/scripts/extract_funcs    # rescan Go + C sources
go run ./docs/btree/mappings/scripts/mappings_diff    # triage report (optional)
go run ./docs/btree/mappings/scripts/build_mappings   # rebuild reverse map + unmapped list
```

`mappings_diff` reports four buckets — new Go funcs needing rows, orphan rows for removed Go funcs, new SQLite funcs needing a map-or-skip decision, and stale cites/skip entries for removed SQLite funcs — plus a count of untriaged stub rows. Always exits 0 (triage, not gating).

`build_mappings` reformats `go_to_sqlite.json` in place (deterministic key order, sorted cites) and fails if any cite isn't in the SQLite allowlist or any Go func is missing a row.
