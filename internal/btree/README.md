# internal/btree

Embedded btree/pager/wal engine — Go port of SQLite's storage layer (`btree.c`, `pager.c`, `wal.c`, `pcache.c`, `pcache1.c`, plus selected SHM helpers).

## Documentation

All package documentation lives under [`docs/btree/`](../../docs/btree/):

- **[NOTES.md](../../docs/btree/NOTES.md)** — drift catalog vs upstream SQLite. Source comments cite it as `docs/btree/NOTES.md §N`.
- **[UPSTREAM.md](../../docs/btree/UPSTREAM.md)** — last-verified SQLite + SQLCipher upstream versions.
- **[mappings/](../../docs/btree/mappings/)** — function-level coverage matrix (Go ↔ SQLite C).
- **[plans/](../../docs/btree/plans/)** and **[specs/](../../docs/btree/specs/)** — implementation plans and design docs.

See [`docs/btree/README.md`](../../docs/btree/README.md) for the full overview.
