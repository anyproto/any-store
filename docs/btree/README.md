# btree docs

Documentation for the embedded btree/pager/wal engine in [`internal/btree/`](../../internal/btree/) — a Go port of SQLite's storage layer (`btree.c`, `pager.c`, `wal.c`, `pcache.c`, `pcache1.c`, plus selected SHM helpers).

## Start here

- **[NOTES.md](NOTES.md)** — comprehensive drift catalog vs upstream SQLite. Section markers (e.g. "§20, drift 3") are the citation form used in source comments.
- **[UPSTREAM.md](UPSTREAM.md)** — last-verified SQLite + SQLCipher upstream versions; bump when re-syncing.

## Subdirectories

- **[mappings/](mappings/)** — function-level coverage matrix (Go ↔ SQLite C). Authoritative inputs: `go_to_sqlite.json`, `sqlite_skip.json`. Generated `*.gen.json` files are gitignored. See [mappings/README.md](mappings/README.md) for the regen workflow.
- **[specs/integrity.md](specs/integrity.md)** — page-integrity (cksumvfs) codec design.

Implementation plans and the remaining design docs live in the tests repo under `any-store-tests:docs/any-store/btree/{plans,specs}`.

## Conventions

Plan filenames are date-prefixed (`YYYY-MM-DD-name.md`); spec filenames are topic-named. Source comments cite docs as `docs/btree/NOTES.md §N` or `any-store-tests:docs/any-store/btree/plans/<file>.md Task N` so a reader can `grep` from either side.
