# FTS: Production Assessment & TODOs

From the any-store-tests cross-machine runs on f414ff6 (r9950x, r3900x
Win+Linux, p14, hp; real air-crash corpus — 38k paragraph "chunks" /
1,459 full articles). Result files in any-store-tests `results/dist-runs/`,
allocation analysis in its PERF-1.

## What's solid (no concerns found)

- Correctness: engine ranking matches a brute-force BM25 oracle EXACTLY
  (same analyzer, same float64 summation order); cross-process match counts
  exact; no ghost/dangling postings in ~1,500+ SIGKILL crash iterations
  (vector-fts workloads) or any multiprocess run.
- Stability: peer-writer visibility on live handles, build-in-subprocess,
  crash recovery — all green on every machine.
- OS-neutral performance (unlike f32 HNSW — no Windows penalty).
- Rare-term search is excellent: 24 µs/q at 20k docs.
- Relevance baseline on the real corpus: MRR 0.73 / Recall@5 0.87 for
  distinctive known-item queries.

## Concerns / TODOs (impact order)

1. **Common-term search cost scales with document frequency, not Limit.**
   BM25 scores EVERY matching posting before the top-k is cut: 1.8 ms +
   8.5k allocs/op for "crash" at 20k chunks (vs 24 µs rare-term — a 75x
   spread); ~0.45 allocs + 23 B per matching doc, linear (18.4k allocs/op at
   the full 38k corpus). `Count()` pays the same. TODO: top-k pruning
   (MaxScore/WAND/block-max postings) for latency, and pooled postings/
   accumulator buffers for the GC churn (any-store-tests PERF-1).

2. **Big-document writes are heavy — chunking is the mitigation.**
   Whole-article profile: single-doc update 29.8 ms (27k allocs), one-token
   SmallEdit still 13.6 ms (the delta path re-tokenizes the whole doc to
   diff). Paragraph-chunk profile: 2.6 / 2.0 ms. For interactive editing,
   index block/paragraph-sized docs (which also matches the vector-chunk
   unit) and debounce per-keystroke updates. TODO (library): tokenize-diff
   that skips unchanged regions, or accept and document the chunking
   guidance.

3. **Query semantics — Phase 1 closed most of these** (see `FTS-V2-PLAN.md`):
   - ✅ phrase queries (`"..."`, positional merge) + positional CJK matching;
   - ✅ boolean AND / required + exclude — via the typed `$require` / `$exclude`
     sub-fields and `$defaultOperator:"and"` (deliberately NOT inline `+`/`-`,
     which is unsafe for raw user input);
   - ✅ prefix search (`term*`) for search-as-you-type;
   - ✅ configurable BM25 `b`/`k1` (`FulltextParams.B/K1`) — Phase 2;
   - ✅ per-field weights (BM25F) — Phase 3 (postings v2 + per-field TF; simplified
     BM25F with global length-norm; `FulltextParams.Weights`); v1 data drops/recreates;
   - ⏳ language stemming — still a single default analyzer (NFKC + fold +
     UAX#29 + CJK bigram); optional, Phase 4;
   - still: `$text` only top-level or inside `$and`, one per query.

4. Residual filters on high-df queries inherit cost #1 (the filter applies
   after full BM25 accumulation): 15.9 ms for selective-filter over the
   article profile. Falls out of fixing #1.
