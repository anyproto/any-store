# Full-Text Search (FTS) in any-store — Design

Status: **draft / in progress** (branch `btree-fts`).

This document describes the lexical full-text-search layer being added to
any-store. It captures the architecture, the on-disk format, and — importantly —
the decisions that are **expensive to reverse** so we get them right on the
first pass.

## Goals & constraints

any-store is an embedded, single-process, pure-Go document store (the storage
engine is a custom B-tree, a Go port of SQLite's pager/wal/btree). FTS must fit
that substrate, not fight it.

- **Scale**: local-first / personal. 10k–1M documents, tens to low-hundreds of
  MB of text per device. Not billions of docs.
- **Latency**: interactive, sub-100ms for typical multi-term queries.
- **Updates**: real-time, incremental. Documents change constantly as the user
  edits. No batch rebuilds, no background segment merges.
- **Queries**: ranked bag-of-words (BM25) is the priority; prefix /
  search-as-you-type is important; phrase queries matter (and are load-bearing
  for CJK, see below). Fuzzy/typo tolerance is a later nice-to-have.
- **Multilingual**: Latin, Cyrillic, CJK, etc. Tokenization must "just work"
  without per-field language configuration.
- **Pure Go, no CGO, single binary.** The index lives inside the B-tree pages so
  it automatically inherits the WAL, atomic commit, at-rest encryption, and
  per-page checksums. No separate index file with its own durability story.

## Core decision: the B-tree *is* the inverted index

Classic FTS engines (Lucene/Tantivy, and SQLite's own FTS5) use LSM-style
immutable segments with background merges. That design exists to tame random
B-tree writes on multi-GB corpora — a problem we **do not have** at local-first
scale (the working set fits in the page cache). Building a segment/merge engine
inside our B-tree would pollute the single-writer transaction model for no
benefit.

Instead we lean into the B-tree: FTS is just a set of ordinary B-tree
namespaces, maintained synchronously inside the same write transaction as the
document it indexes. This mirrors exactly how any-store's existing range indexes
work (`ix:<coll>:<field>` namespaces, key-encode → `Put` in the write tx).

## Namespaces

Following the existing `ix:` / `vix:` conventions, FTS adds (per indexed
collection+index) these namespaces under an `fts:` prefix:

| Namespace      | Key                              | Value                                   | Role |
|----------------|----------------------------------|-----------------------------------------|------|
| `fts:docmap`   | `StringDocID` / `IntDocID`       | the other direction                     | map user UUID ⇄ internal int id |
| `fts:meta`     | small fixed keys                 | counters                                | `N` (doc count), total tokens (→ avgdl), IntDocID sequence |
| `fts:vocab`    | `term`                           | `varint(docFreq)`                       | BM25 `df`; CBO selectivity; prefix dictionary |
| `fts:docinfo`  | `IntDocID`                       | `varint(docLenInTokens)`                | BM25 doc-length normalization |
| `fts:postings` | `Tuple(term, IntDocID/128)`      | packed delta-varint blob                | the inverted index (bounded chunks) |

`fts:docterms` (`IntDocID` → analyzed terms) is optional — see "Updates".

### The internal integer doc id (`IntDocID`)

Documents have string/UUID primary keys. Posting compression relies entirely on
delta-encoding **monotonically increasing integers**, so postings never use the
string id. `fts:docmap` allocates a fresh sequential `IntDocID` per inserted
document from a counter in `fts:meta`.

> **Hard invariant:** `IntDocID` is strictly increasing and **never reused**,
> even after a document is deleted. The delta-encoding inside a chunk assumes
> ascending ids; reusing an id can place a smaller id after a larger one and
> corrupt the delta chain. Deleted ids simply leave holes (harmless — a varint
> gap of 5 costs the same as a gap of 1).

## Postings layout: bounded chunks

Two obvious layouts are both traps:

- **one key per `(term, docId)`** — no read-modify-write, but B-tree per-cell
  framing on every word occurrence bloats the file 5–10×.
- **one key per `term` with a monolithic posting blob** — appending one doc to a
  frequent term ("the") is a read-modify-write of a 100KB+ value that spills to
  overflow pages: catastrophic write amplification.

We use **bounded chunks**: group a term's postings into fixed blocks of
`CHUNK = 128` documents.

```
key   = anyenc.Tuple(term, IntDocID / 128)
value = [ version, <per-doc records...> ]
        per-doc record = DocIdDelta(varint), TF(varint), PosDelta1(varint), ...
```

- DocIds within a chunk are delta-encoded (ascending). Positions within a doc
  are delta-encoded (ascending, so deltas are tiny).
- A chunk holds at most 128 docs, so its value blob effectively never spills to
  an overflow page. A doc insert is a read-modify-write of a single <4KB B-tree
  leaf value, contained entirely in the page cache; commit writes only the few
  dirtied pages to the WAL. Write amplification is localized and bounded.

### Positions are always stored (v1)

Positions roughly double index size and decode cost, but are load-bearing: CJK
search is bigram-phrase search (below), so phrases are not optional. v1 always
stores positions. A future `DOCS_AND_FREQS_ONLY` per-field option (gated by the
version byte) can drop them for large bodies that don't need phrase search — to
be decided by measuring real index size on a representative corpus.

## Analysis pipeline (tokenizer)

Implemented in `internal/fts/analyzer.go`. Deliberately ~200 lines, three small
dependencies, no dictionaries:

```
NFKC normalize  ->  Unicode case-fold  ->  UAX#29 word split  ->  CJK bigram
```

- **NFKC** (`golang.org/x/text/unicode/norm`): collapses full-width forms,
  compatibility ligatures, and NFC/NFD spelling differences so text typed on
  different platforms/IMEs matches.
- **Case folding** (`golang.org/x/text/cases`): locale-independent
  search-oriented case folding (ß→ss, Turkish I, final sigma). NOT
  `strings.ToLower`.
- **UAX#29 word boundaries** (`github.com/rivo/uniseg`): splits spaced scripts.
  UAX#29 emits each CJK ideograph/kana/Hangul syllable as its own token.
- **CJK bigrams**: consecutive CJK characters are re-assembled into overlapping
  bigrams (`東京都` → `東京`, `京都`) at contiguous positions. A lone CJK char is a
  unigram. A CJK query is matched as a **phrase** over its bigrams (positional
  adjacency) and scored as one synthetic term: TF = the adjacency-confirmed
  occurrence count, IDF = the sum of the constituent bigrams' IDFs.

Explicitly **not** done: stemming (we can't assume a per-field language; prefix
search covers most of its value) and diacritic stripping (NFKC already makes the
*same* word match across NFC/NFD; accent-insensitive matching is a later toggle).

Dependencies added: `github.com/rivo/uniseg`, `golang.org/x/text`.

## BM25 scoring & stats

BM25 needs `N` (doc count), `avgdl` (avg doc length), per-term `df`, per-doc
length, and per-(term,doc) `tf`. Where they live:

- `N`, total tokens → `fts:meta`; `df` → `fts:vocab`; doc length → `fts:docinfo`;
  `tf` and positions → the `fts:postings` chunk record.

There is **no contention** (single writer). The only risk is WAL bloat from
rewriting the global counters many times. Mitigation: during a write tx,
accumulate stat deltas (`±1` to `df`, doc-length changes, `N`) in an in-memory
Go map; flush them to `fts:meta`/`fts:vocab` exactly once just before commit, so
those pages are dirtied once per tx.

`fts:vocab` does double duty: a point lookup gives the exact `df` of a term, so
the cost-based optimizer can estimate FTS selectivity precisely.

The query path accumulates per-document BM25 scores in `ftsScoreAcc` — a flat
open-addressing `IntDocID → score` table with O(1) generation reset, pooled
across queries and pointer-free (no GC scan). It replaces a per-query
`map[uint64]float64` that would grow/rehash as a common term accumulates
thousands of documents (the dense-`IntDocID` analog of `internal/vivf`'s
`u32fmap`). This cut heavy-query memory ~40% with no change to ranking.

## Write path (write-first): per-tx buffer + delta updates

Writes (single + batch insert/update) are the priority. The on-disk format is
unchanged — the wins are in how writes are *staged*. The bottleneck is B-tree IO
and WAL dirty-page amplification, not varint CPU (<1µs/chunk), so we stage in
memory and hit the tree once per touched key. (Implemented in
`fulltext_pending.go`; the `internal/simd` package is float32/int8 distance
kernels for the vector index and offers FTS nothing — see the design notes.)

**Per-tx write-back buffer** (`ftsPending`, mirrors SQLite FTS5's `fts5_hash.c`):
instead of read-modify-writing a postings chunk and the vocab `df` for every
term of every document, the maintenance path accumulates postings adds/removes
and `df` deltas in memory for the whole write tx, then flushes each TOUCHED
chunk/term to the B-tree exactly once, **in sorted key order** (cursor glides
left-to-right; same-leaf updates coalesce while pinned). A batch of N docs that
all contain "the" rewrites the "the" chunk once, not N times. The buffer is
writer-owned (single writer holds the btree write lock for the whole tx), reset
at tx start, and flushed at commit — the same lifecycle as the range-index
sketch.

**RAM bound:** once the buffer exceeds `ftsSpillPostings` (~200k buffered posting
ops) it is flushed mid-tx (safe — same tx, just written earlier), so a single
huge batch can't grow it without bound. Oversized buffer maps are released after
a bulk load rather than retained.

**Strong cross-process consistency:** the buffer is purely a per-transaction,
in-memory staging area, flushed into the **same atomic btree commit** as the
documents. There is no cross-transaction or cross-process in-memory cache, so
another process opening a read tx after commit observes a complete, consistent
index, and a crash never leaves a document without its postings. `IntDocID` is
allocated by reading `fts:meta seq` fresh from committed state each tx (under the
cross-process WAL write lock), so two processes never collide. (Verified by
`TestFtsMultiprocessConsistency`, which drives a real child process.)

**Stable `IntDocID` delta updates.** An update keeps the document's `IntDocID`
and diffs old vs new token streams: removed terms drop the id from their chunk,
added terms insert it, unchanged terms do nothing. Editing one word in a long
note touches a couple of chunks, not all of them — and the `IntDocID` is reused,
so chunks never accrete tombstones (no vacuum is ever needed) and `maxIntDocID`
tracks the live-doc count, not the edit count. This is also the write-throughput
win: delete+insert would rewrite every term's chunk twice.

## Public API (locked)

**Declaration** mirrors the vector-index format: a `Kind` discriminator on the
existing `IndexInfo` plus a kind-specific params struct, declared through the
existing `CreateIndex`/`EnsureIndex`. (On the vector branch this is
`Kind IndexKindVector` + `Vector *VectorParams`; FTS adds the parallel
`Kind IndexKindFulltext` + `Fulltext *FulltextParams`.)

```go
type IndexInfo struct {
    Name     string
    Fields   []string
    Unique   bool
    Sparse   bool
    Kind     IndexKind       // IndexKindRange (default) | IndexKindFulltext
    Fulltext *FulltextParams // when Kind == IndexKindFulltext
}

coll.EnsureIndex(ctx, IndexInfo{
    Kind:   IndexKindFulltext,
    Fields: []string{"title", "body"},
})
```

**Query** uses MongoDB-compatible syntax: the `$text` operator matches, and the
relevance score is read/sorted via the `$meta: "textScore"` convention.

```go
coll.Find(`{"$text": {"$search": "east tokyo"}}`).
    Sort(`{"score": {"$meta": "textScore"}}`)
```

## Query / planner integration (first-class, mirrors the vector index)

FTS is a **first-class planner source**, integrated exactly like the vector
index. A `$text` predicate must drive the query (it's the only selective
source), so `BuildPlan` bypasses the CBO and builds:

```
FtsIter -> [FilterIter(residual)] -> [SortIter(real field)] -> [LimitIter]
```

- **`FtsIter`** (`internal/qplanner/fts_iter.go`) is the source. Its
  `FtsQuerySpec{Search FtsSearchFunc, Ordered}` carries a search **closure**
  (the anystore layer injects BM25 over the namespaces, so qplanner stays free of
  any fts/analyzer dependency — the same `VectorSearchFunc` trick). On first
  `Next()` it runs the search (all candidates, score-ranked), then streams them
  lazily, fetching+parsing each doc, injecting the `_score` virtual field, and
  recording `Plan.Scores`.
- **Residual filter**: everything except the `$text` clause runs as a downstream
  `FilterIter` over the ranked candidates (the `_distance`/residual analog).
- **Sort**: no sort (or `{$meta:"textScore"}`) ⇒ the `FtsIter` is `Ordered`
  (score-descending), so the planner emits no `SortIter` and streams straight to
  `LimitIter`. A sort on a **real field** inserts a `SortIter` that re-orders the
  candidates.
- **Score sidecar**: `Plan.Scores` (docId→BM25) + the `_score` virtual field are
  the `Plan.Distances`/`_distance` analog. The public iterator exposes
  `Score() float64` (mirrors `Distance()`); `{$meta:"textScore"}` maps to
  relevance order.
- `Iter`, `Count`, `Update`, `Delete`, and `Explain` all flow through this one
  plan (via `PlanParams.Fts`), so `$text` works uniformly across every operation
  — Explain shows the `FtsSearch` plan.

To guarantee a correct global top-k the search exhausts the query terms' chunks
(cheap at our scale: decoding ~100k varint postings is single-digit ms). No
index-intersection pushdown is built into the postings format.

Phrase / CJK matching (**implemented**, Phase 1 — see `FTS-V2-PLAN.md`) is a
zig-zag merge join on `IntDocID` across the phrase terms' chunks (`termStream`),
then a positional adjacency check; it scores the phrase as a synthetic term
(TF = adjacency count, IDF = Σ of constituent IDFs). A CJK run analyzes to
adjacent bigrams and is matched as an implicit phrase.

Prefix / search-as-you-type (**implemented**, Phase 1): prefix-scan `fts:vocab`,
take the top-M completions by `df` (`ftsPrefixMaxExpansions`), then OR those
terms' postings — bounds latency.

Boolean require/exclude (**implemented**, Phase 1) come from the typed
`$require`/`$exclude` sub-fields of `$text` (not inline `+`/`-`): required clauses
gate a per-doc `requiredMask` in the accumulator; excluded clauses tombstone
matching docs. `$defaultOperator:"and"` makes bare `$search` terms required.

## On-disk invariants that are expensive to change

Lock these three; everything else can evolve in later versions:

1. **Version byte** — the first byte of every `fts:postings` value is a format
   version. Without it we can't migrate blobs in place (e.g. to add a per-chunk
   max-TF for WAND skipping, or repack varints). Mandatory.
2. **Chunk divisor = 128** (power of two). Changing it rekeys the entire
   `fts:postings` namespace, forcing a full re-index. Pick 128 and never change.
3. **Stable, per-document `IntDocID`.** An `IntDocID` is allocated once when a
   document is first indexed (`seq` is monotonic for *new* documents) and stays
   bound to that document across all its edits — the delta-update reuses it. It
   is never reused for a *different* document. A chunk's postings are
   delta-encoded ascending by `IntDocID`; keeping the id stable preserves a
   document's position in its chunk across edits and prevents tombstone
   accretion (so no vacuum is ever needed).

Changing any of these requires a blocking, multi-minute re-index on upgrade.

## v1 cut line

Build exactly this, in order:

1. **Analyzer** — NFKC + fold + UAX#29 + CJK bigram. ✅ (`internal/fts/analyzer.go`)
2. **Postings codec** — pure-Go `pack`/`unpack` of the
   `[version, DocIdDelta, TF, PosDelta...]` chunk blob, with benchmarks proving
   microsecond decode.
3. **Namespaces + write path** — `docmap`/`meta`/`vocab`/`docinfo`/`postings`;
   IntDocID allocation; insert/update/delete maintenance. ✅
4. **Read path** — chunk fetch, BM25 over the `ftsScoreAcc` accumulator, top-k.
   ✅ (`fulltext_search.go`)
5. **Planner hook** — `$text` in the AST; `FtsScan` drives, residual filters
   evaluated post-yield; `{$meta:"textScore"}` relevance sort. ✅
6. **Write-first staging** — per-tx write-back buffer (sorted flush, RAM-spill
   cap), stable-`IntDocID` delta-update, reusable term maps. ✅
   (`fulltext_pending.go`). Measured: batch insert ~1.5–2×, interactive edit
   ~1.9×, heavy query memory −40%.

**SIMD:** rejected for FTS — the `internal/simd` kernels are float32/int8 vector
distances; the FTS write path is B-tree-bound (≈80% of allocs are btree page
ops) and the read loop is gather/scatter-shaped, neither of which SIMD helps.
The lever was data layout (write-back buffer, dense `ftsScoreAcc`), not
vectorization.

**Compression (anyenc S2):** rejected for the index data. The doc btree already
S2-compresses whole documents, but FTS data is the wrong shape for an LZ-family
byte compressor:
- Postings are delta-varints — dense small integers with no recurring 4-byte
  patterns — in <512-byte chunks (below S2's dictionary warm-up). S2 yields
  ~1.0× while adding compress-on-write / decompress-on-read CPU to the hot RMW
  path. The right tool is *integer* coding, which we already do.
- Vocab front-coding is the correct dictionary tool but lives in the B-tree node
  format (off-limits); doing it at the app layer (term-blocking) would break the
  B-tree's native binary search for prefix/range — not worth ~1MB.
The real footprint lever is structural, not compression: a positionless
(`DOCS_AND_FREQS_ONLY`) mode halves the postings (positions are ~50% of them).
At ~19MB index for 29MB text (with positions), the index is already well-coded.

Implemented since the original v1 cut (Phase 1, `FTS-V2-PLAN.md`): phrase /
positional CJK matching; prefix search; boolean require/exclude (`$require` /
`$exclude`) and `$defaultOperator`.

Explicitly **deferred to v2+**: per-field weights / BM25F (Phase 3 — postings v2
+ docinfo v2); configurable BM25 `b`/`k1` (Phase 2); top-k materialization in
`search` (it currently clones every matched id before `Limit` trims); replacing
the per-doc `docinfo` `Get` with an in-memory dense quantized doc-length array
(safe now that `IntDocID` is stable and dense); fuzzy/Levenshtein search over
`fts:vocab`; chunk-level (max-TF) WAND skipping; configurable analyzers /
stemming; `DOCS_AND_FREQS_ONLY` positionless fields. Stop words are intentionally
indexed (cheap under delta-varints; personal-notes users search "to do" etc.).
