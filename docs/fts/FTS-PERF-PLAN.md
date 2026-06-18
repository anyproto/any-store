# FTS Perf — Top-k / Concurrency Plan (P1)

Status: **concurrency + alloc hygiene SHIPPED; remaining latency optimizations
deferred (TODO)** — see "Shipped vs TODO" below. Follows `FTS-V2-PLAN.md`
Phases 1–3 (PR #123).

## Shipped vs TODO

**Shipped** (PR #123):
- ✅ `ReadConcurrency = max(NumCPU−1, 4)` default — concurrency unblocked
  (3.9×→14.2× scaling; 5.3k→24.3k q/s). The actual ceiling was the reader cap.
- ✅ Per-query chunk-reader reuse + bounded accumulator pool (227→72 allocs/op).

**TODO (deferred — search is interactive within budget; do only if a real large
corpus or heavy-AND workload shows a problem):**
- ⏳ **Dense doc-length array** — O(1) length lookup replacing the per-posting
  `docinfo` B-tree Get; ~halves single-thread latency. Needs the generation-tied
  cross-process cache (see "New P1" section). Highest ROI of the remaining items.
- ⏳ **Lead-iterator AND** — drive `+required` queries from the lowest-DF term;
  insurance against pathological high-DF AND queries on large corpora.
- ❌ **Block-Max WAND — will not build.** For single-user local-first, the latency
  gain is sub-frame-invisible and concurrent throughput is already far beyond one
  user's needs; it's server-throughput tech and pure architectural debt here.
  (The P1c section is retained only as the record of this decision.)

Reviewed with an outside expert (Gemini 3.1 Pro) on 2026-06-18. The headline:
**staged, measure-gated — do the cheap fixes first and stop if they suffice.**
Do NOT start with WAND.

## UPDATE (measured 2026-06-18) — the diagnosis changed; profile beat hypothesis

We profiled before building, and the concurrency story was **not** GC:

- A **block profile** put 76% of blocked time in `btree.(*DB).beginRead` — the
  engine caps concurrent read transactions with a semaphore (each live reader
  holds its own page cache; a RAM bound), and the default cap was **4**. 32 worker
  goroutines were serializing 8:1 on it — that *was* the "~3.9× scaling".
- **Fix:** default `ReadConcurrency = max(NumCPU−1, 4)` (anystore `Config`, wired
  to `btree.Options.MaxReaders`); reader caches are lazily created, so RAM follows
  *actual* concurrency, not the cap. Result: 1→16-core scaling **3.9× → 14.2×**,
  chunk-profile aggregate **5.3k → 24.3k q/s**. Concurrency is solved, with zero
  FTS change.
- The alloc cleanup (per-query chunk-reader reuse, accumulator pool size-cap) was
  done too (high-DF query 227 → 72 allocs/op — we'd regressed to one heap reader
  per 128-doc chunk via an interface return). It's hygiene; it did **not** move
  latency or scaling.

**Reprioritized next steps (expert-confirmed):**
1. **Dense doc-length array** — highest ROI; see new section below.
2. **Lead-iterator AND** — insurance against pathological high-DF AND queries.
3. **Drop Block-Max WAND.** For a single-user local-first store, 1 ms vs 5 ms is
   sub-frame-invisible and concurrent throughput is already "functionally
   infinite" (24k q/s for one user). BMW is server-throughput tech → pure
   architectural debt here. The P1c section below is kept only as a record of the
   decision *not* to build it.
4. **Then ship.**

### New P1 (highest ROI) — dense doc-length array

The single-thread cost is "cost #1": the read path scores every matching posting,
and each scored posting does a B-tree point-`Get` on `docinfo` for the doc length
(BM25 length-norm) — a B-tree traversal + page pin + 128-item binary search,
millions of times per high-DF query. Replace with an O(1) lookup:

- A dense `[]uint32` keyed by IntDocID (dense, stable). ~4 MB at 1M docs — don't
  quantize (quantization is a billions-of-docs concern).
- **Cross-process snapshot trap:** don't keep a long-lived globally-synced array.
  Cache it at the DB level **tied to a btree commit generation**; on read-tx
  start, if the cached generation matches the tx snapshot, use it; else lazily
  clone + apply the `docinfo` delta (scan only keys changed since the cached
  generation), then atomically swap the cached pointer. 4 MB copy at a write
  boundary is negligible.
- No format change, no algorithm change; expected to ~halve single-thread latency.

## Measured bottleneck

Paragraph-chunk profile (38k small docs, avgDocLen ≈ 58, 26 MB index, warm,
Ryzen 9950X 16C/32T), realistic mixed query set:

- single-thread: ~1,360 q/s (~735 µs/query) — raw speed is fine.
- concurrent 32 workers: ~5,300 q/s — **only ~3.9× scaling**. The red flag.
- Cost is dominated by high-DF common terms; rare terms are tens of µs.

Root cause: the read path scores **every** matching posting into a flat
accumulator (cost scales with DF, not `Limit`), and each high-DF query allocates
~20 KB of scratch. The poor concurrent scaling is **GC-bound** (allocation
thrash), not lock-bound (reads use independent read-txs over a shared page cache).

These are two distinct problems with two distinct fixes.

## Staged plan (measure-gated)

### P1a — cut per-query allocation via a per-query arena (do first; no format change)

Goal: kill the GC thrash that caps concurrent scaling — **without** introducing
uncontrolled RAM.

**Core mechanism: reuse one arena for the lifetime of a single query** — not a
cross-query pool. The churn is the *count* of allocations per query (per-term
cursor key/prefix, per-chunk/position decode scratch, dense-TF scratch, etc.).
Collapse them into a small set of buffers held in the query-scoped `ftsExec`
(which already exists per query and already reuses `dlBuf`/`tokBuf`), reused
across the whole term/chunk loop and grown on demand. The arena lives exactly as
long as the query and is freed right after — so RAM is bounded by the number of
**in-flight** queries (≈ concurrency), sized to each query's own need, and never
retained between queries. This is the read-path analog of the writer-owned
reused buffers on the write path.

**Why not cross-query `sync.Pool` for the big buffers:** `sync.Pool` retains its
contents across every P with no size bound, so pooling the *large* per-query
buffers (the accumulator table can reach MBs on a high-DF query over a big corpus)
lets N concurrent Ps each pin a giant buffer → unbounded RAM, which is
unacceptable on a constrained device. Per-query arena reuse avoids retention
entirely.

**The accumulator (the one large buffer that is cross-query pooled today):**
keep `ftsScoreAccPool` but add a **size cap** — on return, if the table grew past
a cap, drop the oversized backing arrays (nil them) so GC reclaims them and the
next user reallocates small. So the pool retains only small/typical tables; large
ones are never pinned. This is the pattern the write path already uses
(`ftsPendingShrink` / "don't pin a giant arena after a bulk load" in
`fulltext_pending.go`). The existing pool has exactly this leak today (a huge-DF
query pins a multi-MB table) — fixing it is part of P1a.

**Small fixed-size scratch** (e.g. the analyzer — `searchCandidates` does
`fts.NewAnalyzer()` per query, allocating NFKC + case-fold transformers) can be
reused per query too; pooling those cross-query is low-risk (small, bounded by
concurrency) if profiling shows it matters.

**Method:** profile first (`-benchmem` + an alloc pprof on the concurrent bench)
to find the real per-query alloc sources — don't guess. Then route them through
the query arena, size-cap the accumulator pool, and re-bench concurrent scaling.

- Expected: concurrent throughput rises toward near-linear, with RAM bounded by
  in-flight queries (arena freed per query; pool retains only capped buffers).
- This is the fix for the **concurrency ceiling**.

### P1b — lead-iterator intersection for required (AND) queries (no format change)

Today `+a +b` scores all of `a`'s and all of `b`'s postings into the accumulator,
then drops docs missing a required bit — scanning every occurrence of a common
required term wastefully. Instead:

- Sort the required terms by DF ascending; take the **lowest-DF** term as the lead.
- Iterate the lead's postings; for each DocID, `Seek` the other required terms'
  chunks (`Tuple(term, IntDocID/128)` → jump straight to the chunk, binary-search
  its ≤128 docs) to confirm presence + gather TF.
- Work drops from O(matches of the high-DF term) to
  O(matches of the low-DF term · log(high-DF)). AND-query latency → single-digit µs.
- No storage change; complements the existing `requiredMask` (which still serves
  `$defaultOperator:"and"` over should-terms).

### Re-profile gate

After P1a+P1b, re-measure single-thread tail + concurrent scaling on the chunk
profile. **If acceptable for a local-first desktop/mobile app, STOP** — do not
build WAND. Top-k pruning is only worth its complexity if the pure-OR tail latency
is still too high after pooling.

### P1c — Block-Max WAND (conditional; only if pure-OR tail is still too high)

Dual-path, exact (returns the same top-k as full scoring):

- **Path 1 — exact accumulator** (today's): Count/Update/Delete, explicit
  real-field sorts, heavy residual filters. Unchanged.
- **Path 2 — pruned top-k**: default-relevance `$text` + `Limit` only. A top-k
  min-heap whose minimum is the live `threshold`; for each 128-doc block compute
  `blockMaxScore`; if `< threshold`, skip the whole block.
- **Format (free to change — alpha, no back-compat):** add per-block `MaxTF` +
  `MinDL` (varint) to the chunk. Do **not** bake a final BM25 score — `avgdl`
  drifts as docs change and would invalidate it (breaking the exact oracle).
  At query time, plug `MaxTF`/`MinDL` into the *live* BM25 formula
  (`idf·bm25(MaxTF, MinDL, avgdl_now)`) for a valid, drift-safe upper bound.
- **Residual filter + Limit:** over-fetch `k·c` candidates via Path 2, apply the
  filter; if ≥ k survive, yield; if the filter is too restrictive and survivors
  drop below k, fall back to Path 1.
- **Operators:** BMW covers pure-OR (should) scoring. Required(AND) uses P1b's
  intersection (better than WAND-over-OR). **Phrase and prefix stay on the exact
  path** — positional adjacency and expansion don't fit WAND's additive bounds.

BMW is preferred over MaxScore/plain-WAND here because postings are already
physically chunked into 128-doc B-tree blocks — the block boundaries BMW needs
are free.

## Decision record

Expert-confirmed (2026-06-18): pooling before pruning (fixes the GC-bound
concurrency ceiling cheaply); lead-iterator intersection for AND (large win, no
format change); keep dual paths (pruning can't serve Count/sort/heavy-filter);
per-block `MaxTF`+`MinDL` (not a baked score) for drift-safe BMW bounds; BMW over
MaxScore given the existing block structure; phrase/prefix off the pruned path;
**measure-gate** — stop after P1a+P1b unless the OR tail is still too high.
