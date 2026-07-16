# Peer-visibility criticals grooming: FTS peer DDL, checkpoint read-mark clobber, master-cell overflow

**Repo:** `github.com/anyproto/any-store/v2`, branch `btree` @ `9a97c12`
**Worktree (read/edit code HERE):** `/home/che/projects/any-store/.claude/worktrees/peer-visibility-groom`
**Companion test repo:** `/home/che/projects/any-store-tests` (module `any-store-tests`, go.work → `../any-store`)
**Bug docs:** `BUG-40-fts-peer-ddl-not-reconciled.md`, `BUG-41-checkpoint-clobbers-peer-readmarks.md`, `BUG-42-master-cell-overflow-namespace-unresolvable.md` — all in the test repo
**Reference sources:** sqlitec pinned `version-3.52.0` at `~/projects/sqlitec` (HEAD `257fdf849`, verified this groom); all wal.c / btree.c line cites below are against that tag

**Goal (acceptance):** the three bug docs flip to FIXED with descriptive regression tests green: btree-layer long-name master-cell round-trip at ps=512 + IntegrityCheck; multiprocess FTS peer-drop (IntegrityCheck clean after churn) and peer-create ($text sees backfilled + new docs); two-process checkpoint test where a live peer's SHM read-mark survives PASSIVE checkpoints and a pinned reader never observes future bytes.

**Scope decisions this groom (2026-07-17, supersedes the 2026-07-12 per-doc decisions where noted):**
- BUG-42 gets the **full fix now** (overflow-following readers + rename read-back guard + name caps), superseding "cap now, reader fix later" — exploration showed the value-reconstruction machinery already exists and one funnel point (`resolveNamespace`) repairs all four read entry points.
- BUG-41's deferred "dedicated decision" is **this document**: align with wal.c's per-slot-lock invariant; no new drift is introduced, one filed follow-up is closed.
- Implementation order: **BUG-42 → BUG-40 → BUG-41** (self-contained btree read fix first, app-layer reconcile second, the delicate WAL/SHM protocol change last, in its own PR).

**Semantics oracle:** SQLite C sources for BUG-41/42 (btree/WAL layer — the alignment directive applies hard); BUG-40 is any-store app-layer design with no C counterpart (SQLite has no per-connection index handle snapshots), so its oracle is the repo's own vector-reconcile precedent.

Every empirical claim below was verified on this worktree at 9a97c12.

---

## Decision summary

| # | question | decision |
|---|---|---|
| 1 | BUG-42 scope | Full fix: overflow-following value read in `resolveNamespace` + the `masterLeaf` integrity hook, `RenameNamespace` read-back guard, and app-layer name caps. The caps prevent the public API from ever writing an overflowing master cell; the reader fix repairs the btree layer (any page size) and any already-written long-name data. |
| 2 | BUG-42 reader mechanism | Mirror `ReadTx.AppendValue`'s overflow branch (db.go:1575-1620): copy the local `cell.value` portion, then `pager.readOverflowAt(cell.overflowPg, keyOverflow, valOverflow, dst, walMaxFrame, cache)` (pager.go:3050). Works with nil cache, so the writer path `getNamespaceLocked` is covered too. Alignment source: SQLite reads sqlite_master exclusively through `accessPayload` (btree.c:5121), which reassembles local + overflow transparently — the local-only master reader was the drift, and this change removes it (no DRIFT entry; commit message cites accessPayload). |
| 3 | BUG-42 name caps | 255 bytes each for collection and index names, enforced in `validateCollectionName` (db.go:356) and after `createName()` defaulting (collection.go:977-978, 1037-1038). Budget: widest derived namespace is fts `len(C)+len(I)+11`; overflow needs `derived+4 > maxLocalPayload(4096)=1002`, so the binding constraint is `len(C)+len(I) ≤ 987`; 255+255 leaves 477 bytes of margin. New error var in errors.go beside `ErrInvalidCollectionName`. |
| 4 | BUG-42 rename guard | After `bt.Put(newName, …)` (db.go:1200-1202), re-resolve `newName` and require the root page to match the moved namespace; on mismatch return an error so the caller rolls back (contract at db.go:1166-1168). Cheap backstop; only meaningful with decision 2 in place. |
| 5 | BUG-40 mechanism | New `reconcileFtsIndexesLocked(tx, infos)` mirroring `reconcileVectorIndexesLocked` (vector_index.go:486-530), called beside it in `reconcileIndexes`; `Kind==IndexKindFulltext` infos are excluded from `rangeInfos` (fixing the misclassification at collection.go:1584-1590 that silently dropped them at `ix:` resolution). |
| 6 | BUG-40 adopt/reuse/evict | Adopt peer-created: construct `ftsIndex` + `bindNamespaces(tx.GetNamespace)` (resolve-only, fulltext_index.go:159-178). Reuse existing iff the `nsMeta` namespace root is unchanged (analog of vector `rootUnchanged`, vector_index.go:295-307 — detects peer drop+recreate). Keep-existing on transient resolution failure (never drop a working index on an error, matching vector_index.go:515-522). Evict peer-dropped by omission **+ `fx.pending.reset()`** (precedent: `invalidateCollection`, db.go:551-553). CoW `storeFtsIndexes` iff changed. Adoption trusts the peer's on-disk backfill (matches range/vector adoption; confirmed from 2026-07-12). |
| 7 | BUG-40 disk writes | None. Reconcile never calls `DeleteNamespace`/`removeIndex`/`MarkSchemaChanged` — the peer already performed the DDL, and reconcile may run inside a read tx. Eviction is purely local handle release. |
| 8 | BUG-41 mechanism | Restore wal.c's invariant (wal.c:367-370: aReadMark[K] is only written under exclusive WAL_READ_LOCK(K)). Add single-word publishers `shmWriteNBackfill` / `shmWriteNBackfillAttempted` (mirroring `shmWriteReadMark`, wal.go:1302); the two `checkpointWithMode` publishes (wal.go:3580, 3801) write **counters only** — read-marks leave the checkpoint publish path entirely (the reader loop at wal.go:3500-3542 already writes them per-slot-locked). C analogs: nBackfillAttempted at wal.c:2268, `AtomicStore(&nBackfill)` at wal.c:2331 — C never bulk-copies marks when publishing counters. |
| 9 | BUG-41 recovery/open paths | The `doResetWAL`→`reset()` path is **already lock-safe** (`tryResetWALWithBusy` wal.go:3864-3884 holds slots 1-4 exclusive — matches the range lock at wal.c:2361 before `walRestartHdr`). The unprotected sites are `recoverLocked` (wal.go:2049) and `reset()`'s open-path callers `initHeaderStateLocked` (wal.go:1785) / `writeHeader` (wal.go:1840): their SHM mark writes move to a per-slot try-lock loop that skips BUSY (live-reader) slots, mirroring walIndexRecover (wal.c:1573-1588). This closes the follow-up filed in NOTES `drift-2026-07-10-3` (NOTES.md:3325). Restructure so every SHM `aReadMark[i]` store is statically auditable as lock-bracketed. |
| 10 | BUG-41 non-scope | Mark-*value* drifts stay untouched: drift-88 (recovery sets all marks NOT_USED vs C pre-seeding slot 1), drift-99 (restart clobbers marks 0/1 to NOT_USED vs walRestartHdr values), drift-97/98 (salts / nCkpt). This increment changes locking and publication shape only. Process-local `aReadMark` mirrors stay (in-process heapShm mode has no region; wal.go:505-520). |
| 11 | NOTES/DRIFT output | No new DRIFT entries — all three fixes remove or avoid divergence. One NOTES edit: annotate the "Adjacent follow-up (separate issue)" sentence of `drift-2026-07-10-3` as resolved, citing the new per-slot-locked recovery mark writes. |
| 12 | increments | 1 = BUG-42 full fix (S+S+S, btree + app validation). 2 = BUG-40 reconcile (M, app layer + multiprocess tests in the test repo). 3 = BUG-41 (L, WAL/SHM protocol, own PR, two-process tests). Separable commits per scope discipline; no BUG-NN identifiers in code or test names. |

---

## BUG-42 — master-cell overflow (btree layer + app caps)

### Facts (verified)

- Cell contract (btree.go:162-164): for overflow cells `cell.key`/`cell.value` hold the **local portions only**; when the 4-byte rootPgno value spills wholly, `len(cell.value)==0` and `cell.overflowPg != 0` (assignment guarded at btree.go:217-219).
- All four namespace read entry points — `DB.GetNamespace` (db.go:1332), `getNamespaceLocked` (db.go:1351, writer path, nil cache), `getNamespaceAt` (db.go:1360), `ReadTx/WriteTx.GetNamespace` — funnel into `resolveNamespace` (db.go:1368-1420). The buggy read is db.go:1395-1399 (`len(cell.value) < 4 → ErrCorrupt`). Key lookup is already overflow-correct (`searchLeafWithOverflow`, db.go:1380) — only the value read is broken.
- The `masterLeaf` IntegrityCheck hook (integrity.go:646-669) has the identical local-only read at :650-654. Overflow-page **accounting** is already correct: `checkTreePage` computes `nOverflow` and `checkList`s the chain (integrity.go:404-419), so "page never used" does not false-positive; the reported `corrupt namespace root value` is purely the value read.
- Reconstruction machinery exists: `ReadTx.AppendValue`'s overflow branch (db.go:1575-1620) decodes keyLen/valLen, computes `nLocal`/`localValBytes`/`keyOverflow`/`valOverflow`, copies the local part, then `pager.readOverflowAt(firstPgno, skip, amt, dst, walMaxFrame, cache)` (pager.go:3050; branches internally on nil cache).
- `maxLocalPayload = ((usableSize-12)*64/255) - 23` (page.go:105): **102** @ ps=512, **1002** @ ps=4096. Master cell overflows when `len(name)+4 > maxLocalPayload`.
- Write side is already correct (`bt.Put` spills properly, db.go:1126-1127); `CreateNamespace` fails loudly only because the wrapper re-resolves; `RenameNamespace` (db.go:1171-1203) never re-resolves → silent durable brick.
- Namespace-name derivations and worst-case overhead over user names (C = collection, I = index): data ns = `C` (+0); range `ix:C:I` (+4, index.go:298-300); fulltext `ftx:C:I:part`, longest part `vocab` → **+11** (fulltext_index.go:24-34); vector `vix:C:I` + suffix, longest `:cell` → +10 (vector_index.go:43-50, vivf/store.go:220-222).
- Current validation has **no length rule anywhere**: `validateCollectionName` (db.go:356-369) rejects only empty/system/reserved prefixes; index names are unvalidated and `createName()` (index.go:285-287) can synthesize long ones from field lists.

### Design

1. **`resolveNamespace`**: on `cell.overflowPg != 0`, reconstruct the value exactly as db.go:1602-1618 does — local part first (the value may be split, not only wholly spilled), then `readOverflowAt` with `skip` = key-overflow bytes into a 4-byte buffer. `bt` already carries `pager`/`walMaxFrame`/`cache`.
2. **`masterLeaf` hook**: same reconstruction with `ic.pager/ic.cache/ic.walMaxFrame/ic.usableSize` (integrity.go:608-616); widen the `onLeafCell` callback or reconstruct inside `checkTreePage` (which already calls `leafFullKey` for overflow cells at integrity.go:383-389) — implementer's choice by readability. Refresh the stale `db.go:1253` cross-ref comment at integrity.go:647.
3. **`RenameNamespace` read-back guard** after the Put (db.go:1202): re-resolve, require `rootPage` match, error on mismatch (caller rolls back per db.go:1166-1168).
4. **Name caps (255 bytes each)** per decision 3, with a new sentinel error in errors.go.
5. **Tests**: btree layer — `tempDBWithPageSize(t, 512)` (helpers_test.go:13), ~120-byte name: Create→Get→Rename→Get round-trip + `IntegrityCheck` green + reopen persistence; place near `namespace_rename_test.go` / the `TestAppendValue_*Overflow` cluster (db_test.go:398/775/800). App layer — cap acceptance/rejection at the boundary (255 ok, 256 rejected) for collection and index names including a synthesized-long `createName()` case.

---

## BUG-40 — FTS peer DDL reconcile (app layer)

### Facts (verified)

- Trigger chain: tx begin → `db.checkStale` (db.go:426/402) → `tx.IsSchemaStale()` → `db.reconcileIndexSet` (db.go:478-510) → per open collection `c.reconcileIndexes(tx)` (db.go:508). `collectionVanished` (db.go:524-537) is false for peer FTS DDL (collection still exists) — whole-handle invalidation never fires, so the FTS-ignorant `reconcileIndexes` is the only chance to fix the handles.
- `reconcileIndexes` (collection.go:1566-1636): vector infos split off to `reconcileVectorIndexesLocked` (:1582); everything else lands in `rangeInfos` (:1584-1590) — **FTS infos included**, since the predicate is `Kind != IndexKindVector`. Each range info resolves `ix:C:I`; an FTS index has no `ix:` namespace (it has five `ftx:C:I:{map,meta,vocab,info,post}`), so resolution fails into the nsErr branch (:1597-1607) and the info is silently dropped. `c.ftsIndexes` is never touched.
- `ftsIndexes` = `atomic.Pointer[[]*ftsIndex]` (collection.go:152); mutation sites are all local-DDL/init/rollback (collection.go:284-319, 939-944, 1182-1189, 965-971).
- Eviction subtlety: each `ftsIndex` carries a per-tx `pending` write-back buffer (fulltext_pending.go:47-55); the flush/reset drivers enumerate the **snapshot** (`flushAllFtsPending` db.go:1262-1283 at commit, `resetAllFtsPending` db.go:1288-1301 at write-tx begin), so an evicted handle's buffered postings would be stranded. Precedent: `invalidateCollection` (db.go:539-560) resets pending before dropping; `db.orphanFtsPending` (db.go:263-269) exists for mid-tx closes. Reconcile runs at tx begin (after `resetAllFtsPending` on write-tx) so the buffer is normally empty — the evict-side `pending.reset()` is the safety belt. No background goroutines exist to join (FTS merging is synchronous at commit).
- Local `DropIndex` FTS teardown (collection.go:1172-1190) deletes the five namespaces + catalog rows — peer-drop eviction must replicate **only** the snapshot republish, never the disk writes.

### Design

Per decisions 5-7. Shape of `reconcileFtsIndexesLocked` (caller holds `c.mu`, mirroring vector_index.go:486-530):

- Build `byName` from `loadFtsIndexes()`; `want` = infos with `Kind==IndexKindFulltext`; `changed = want != len(cur)`.
- Per info: existing + `nsMeta` root unchanged → reuse the live object; else construct + `bindNamespaces(tx.GetNamespace)`; on resolution error with an existing object, keep the existing (transient-failure rule); on resolution error with no existing object, skip (peer's create not yet visible / mid-rollback) and set changed.
- Republish via `storeFtsIndexes` iff changed; for every prior object not carried forward, `fx.pending.reset()`.
- Root-identity note: `nsMeta` is stable across normal writes (namespace root moves only on drop+recreate), making it the drop+recreate detector, same as vector `MetaRoot()`.

### Tests (any-store-tests, storetest/multiprocess_fts_test.go)

Extend the existing harness (`runMultiProcessHelper` multiprocess_test.go:90-111, deterministic-corpus helpers at :47-82):

- **Peer drop (face 1, corruption)**: parent opens collection with FTS index warm (insert + search once); child helper drops the index and inserts ~500 docs into a second "churn" collection (recycling the freed `ftx:` pages); parent inserts more docs (must return nil), then: `IntegrityCheck` green, churn count exact, parent's `$text` returns `ErrNoFulltextIndex` (the index is gone — correct post-drop behavior).
- **Peer create (face 2, correctness)**: parent opens collection and inserts d1 (no index); child helper `EnsureIndex(fulltext)` (backfills d1); parent inserts d2; parent's `$text` (and a fresh reopen's `$text`) must return both d1 and d2.

---

## BUG-41 — checkpoint read-mark clobber (WAL/SHM protocol; own PR)

### Facts (verified against wal.go @ this worktree and wal.c @ version-3.52.0)

- **Structural root cause**: the Go port has no single-word SHM publisher for `nBackfill`/`nBackfillAttempted`; every publish funnels through `shmWriteCkptInfo()` (wal.go:1244-1257) which also stores `aReadMark[0..4]` from process-local mirrors — while holding at most slot-0's lock. C publishes the counters individually (wal.c:2268, wal.c:2331) and documents the invariant at wal.c:367-370: *"The value of aReadMark[K] may only be changed by a thread that is holding an exclusive lock on WAL_READ_LOCK(K)."*
- **Call-site lock audit** (sharper than the bug doc):

| site | wal.go | locks held on slots 1-4 | verdict |
|---|---|---|---|
| `checkpointWithMode` pre-backfill | 3580 | none (only `lockRead0` excl. @3566) | **the clobber** (C analog wal.c:2268 writes nBackfillAttempted only) |
| `checkpointWithMode` post-backfill | 3801 | none (only `lockRead0` excl.) | **the clobber** (C analog wal.c:2331 writes nBackfill only) |
| `doResetWAL` → `reset()` | 3902→832 | slots 1-4 exclusive (`tryResetWALWithBusy` 3864-3884) | lock-safe (matches wal.c:2361 range lock before walRestartHdr) |
| `recoverLocked` publish | 2049 | recovery exclusive range, but **not** per-reader-slot | unprotected — the F1 follow-up (C: wal.c:1573-1588 takes WAL_READ_LOCK(i) per slot, skips BUSY) |
| `initHeaderStateLocked` → `reset()` | 1785→832 | doResetWAL path: safe; open path: recovery context | unprotected on the open path |
| `writeHeader` → `reset()` | 1840→832 | reached from recoverLocked invalid-header branch (:1894) | unprotected (same recovery context) |

- The empirically-verified data-loss interleaving (bug doc) goes through the two `checkpointWithMode` sites: checkpoint #1's post-backfill publish overwrites a live peer's slot mark with local `NOT_USED`; checkpoint #2 then treats the slot as unused, never lowers `mxSafeFrame`, and backfills past the reader's pinned frame.
- Correct templates already in-repo: `shmWriteReadMark` (single slot, wal.go:1302-1310), the checkpoint reader loop (wal.go:3500-3542: per-slot `walBusyLock` exclusive, write, unlock; BUSY → lower mxSafeFrame), the read-tx slot claim (wal.go:2848-2979: exclusive claim, write, downgrade to shared, revalidate).
- Process-local mirrors must stay: in-process (heapShm) mode has no mmap region and the atomics are the source of truth (wal.go:505-520); `shmWriteCkptInfo`/`shmWriteReadMark` no-op harmlessly when `region()` errors.
- Fresh-SHM seeding: mark initialization into a zeroed region happens via the recovery/reset paths, which this design keeps (now correctly locked) — the checkpoint publishes never seeded anything a recovery hadn't already written.
- Existing NOTES entries touching this area (all stay): `drift-88` (recovery mark values), `drift-99` (restart mark values), `drift-97`/`drift-98` (salts/nCkpt), and the follow-up sentence in `drift-2026-07-10-3` (NOTES.md:3325) that this fix resolves.

### Design

Per decisions 8-10:

1. `shmWriteNBackfill()` / `shmWriteNBackfillAttempted()` single-word publishers; `checkpointWithMode` @3580 → attempted-only, @3801 → nBackfill-only.
2. Split the bulk writer: `reset()` keeps clearing local mirrors + hash + publishing counters; SHM mark writes become an explicit helper whose two legal call shapes are (a) under the slots-1-4 range lock (doResetWAL path — bulk write OK), (b) per-slot try-lock skip-BUSY loop (recovery/open paths — wal.c:1573-1588 analog). `shmWriteCkptInfo` in its mark-writing form disappears from unlocked contexts; `recoverLocked`'s publish (:2049) and the open-path `reset()` callers route through (b).
3. Audit `shmReadCkptInfo`'s remaining callers (test/single-goroutine contexts per its warning, wal.go:1259-1279) — read side unchanged.
4. NOTES.md: annotate the follow-up in `drift-2026-07-10-3` as resolved with the new function name; no new DRIFT entry.

### Tests (internal/btree, re-exec harness per multiprocess_reader_test.go)

- **Protocol test**: parent opens multi-process DB, pins a read tx on a slot ≥1, records `shmReadMark(slot)`; child process commits and runs ≥2 PASSIVE checkpoints; parent asserts its SHM mark is unchanged (today it flips to `readMarkNotUsed`) and `shmNBackfill()` never exceeds the pinned mark.
- **End-to-end snapshot test**: the bug doc's interleaving — reader pins a snapshot at frame N; writer commits past N and PASSIVE-checkpoints twice; reader re-reads pages whose only ≤N version was backfilled but rewritten later, and must see the pinned-generation bytes. Torn-read oracle: uniform-fill values + growing keyspace (stable overwrites hide mixed-generation reads).
- Both build-gated like the existing multiprocess tests and skipped on tmpfs (`requireRealFilesystem` convention in storetest; the btree-layer tests use their own TMPDIR guard).

---

## Increments & commit shape

1. **fix(btree): master-table cells follow the overflow chain** — resolveNamespace + masterLeaf + rename read-back guard + ps=512 tests. Commit cites `accessPayload` (sqlitec btree.c:5121).
2. **fix: cap collection and index name length** — validateCollectionName + post-`createName` index-name check + errors.go + tests. (Separable from 1; either order works, but 1 first so the cap is a hardening layer, not the fix.)
3. **fix: reconcile fulltext indexes on peer schema changes** — reconcileFtsIndexesLocked + routing fix; multiprocess tests land in any-store-tests in the same effort.
4. **fix(btree): publish checkpoint counters without rewriting read-marks** — single-word publishers + checkpointWithMode changes + protocol test. Own PR with 5.
5. **fix(btree): recovery resets read-marks under per-slot locks** — recovery/open-path mark writes + NOTES annotation + end-to-end test.

## Verification

- Per increment: `go test ./internal/btree/` + full `go test ./...` in the worktree (TMPDIR on a real filesystem for multiprocess tests); `-race` on changed packages.
- Test repo: storetest (new FTS peer-DDL tests + existing `TestMultiProcessFts_*`, `TestIVFPQMultiprocessConsistency`, `TestMultiProcessReaderScanCorruption`) against this worktree via a scratch GOWORK `use` block.
- BUG-41: `-tags debugtrace` + `BTREE_TRACE` two-process trace before/after — the SHM mark must no longer flip across a checkpoint and `nBackfill` must never pass a live reader's mark.
- Not hot paths (tx-begin reconcile, namespace resolution, checkpoint publish) → no benchstat gate; watch reconcile cost anecdotally since it runs on every stale tx begin.
