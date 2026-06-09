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
  unigram. A CJK query is rewritten to a **phrase** over its bigrams and scored
  by summing the bigram BM25 contributions.

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

## Updates (the keystroke problem)

A naive "delete old doc, insert new doc" on every save touches every term's
chunk (~300 chunks for a 500-word note) — WAL thrash. Instead use **delta
updates**:

1. Get the document's previous analyzed terms — either re-analyze the old text,
   or read a forward `fts:docterms` (`IntDocID` → terms) index.
2. Analyze the new text. Diff old vs new term/position sets.
3. Read-modify-write only the chunks for terms that actually changed. Editing
   one word in a 500-word note touches ~2 chunks, not 300.

No tombstones, no background compaction: when an RMW empties a chunk, just
`Delete` the key. The page cache absorbs repeated saves to the same hot chunk.

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

## Query / planner integration

FTS surfaces to the cost-based planner as an `FtsScan` iterator producing
`(IntDocID, score)`.

Key gotcha: a scored iterator can't be cheaply *driven* by another index. If a
query is relevance-sorted, the planner **must** make `FtsScan` the driver:

- `FtsScan` fetches the query terms' chunks, computes BM25, and maintains a
  top-k max-heap.
- Any remaining (non-FTS) filter predicates are evaluated **post-yield**: before
  admitting a candidate to the heap, point-look-up the document and test the
  rest of the AST.
- To guarantee a correct global top-k, `FtsScan` exhausts the query terms'
  chunks (cheap at our scale: decoding ~100k varint postings is single-digit
  milliseconds). No index-intersection pushdown is built into the postings
  format.

Phrase / CJK matching is a zig-zag merge join on `IntDocID` across the involved
terms' chunks, then a positional adjacency check.

Prefix / search-as-you-type: prefix-scan `fts:vocab`, take the top-M completions
by `df`, then query only those terms' postings — bounds latency.

## On-disk invariants that are expensive to change

Lock these three; everything else can evolve in later versions:

1. **Version byte** — the first byte of every `fts:postings` value is a format
   version. Without it we can't migrate blobs in place (e.g. to add a per-chunk
   max-TF for WAND skipping, or repack varints). Mandatory.
2. **Chunk divisor = 128** (power of two). Changing it rekeys the entire
   `fts:postings` namespace, forcing a full re-index. Pick 128 and never change.
3. **Monotonic, never-reused `IntDocID`** (see above).

Changing any of these requires a blocking, multi-minute re-index on upgrade.

## v1 cut line

Build exactly this, in order:

1. **Analyzer** — NFKC + fold + UAX#29 + CJK bigram. ✅ (`internal/fts/analyzer.go`)
2. **Postings codec** — pure-Go `pack`/`unpack` of the
   `[version, DocIdDelta, TF, PosDelta...]` chunk blob, with benchmarks proving
   microsecond decode.
3. **Namespaces + write path** — `docmap`/`meta`/`vocab`/`docinfo`/`postings`;
   IntDocID allocation; delta-update on insert/update/delete; in-memory stat
   aggregation flushed at commit.
4. **Read path** — chunk fetch, zig-zag phrase merge, BM25, top-k heap.
5. **Planner hook** — wire `$text` into the AST; make `FtsScan` the driver with
   post-yield filter evaluation.

Explicitly **deferred to v2+**: fuzzy/Levenshtein-automaton search over
`fts:vocab`; chunk-level (max-TF) WAND skipping; configurable analyzers/
dictionaries; stop-word removal (we index stop words — delta-varints compress
them well, and personal-notes users search for "to do", "let it be", etc.);
`DOCS_AND_FREQS_ONLY` positionless fields.
